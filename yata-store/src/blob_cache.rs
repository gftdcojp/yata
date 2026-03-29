//! BlobCache: tiered blob storage abstraction.
//!
//! Provides metadata-blob separation backed by R2 (blobs) + in-process
//! HashMap (metadata), with optional disk and mmap tiers.
//!
//! ## Design
//!
//! Immutable "objects" identified by ObjectID:
//! - **Blob**: raw bytes (Arrow IPC, CSR topology, vector index)
//! - **Metadata**: JSON-like tree describing the object's schema/type
//!
//! Storage mapping:
//! - Blob → R2 object or local file
//! - Metadata → in-process HashMap (synced to DO SQLite for persistence)
//! - Local cache → LRU Vec<u8> cache for hot blobs
//!
//! ## Zero-copy semantics
//!
//! True zero-copy (mmap) isn't possible in CF Workers. Instead:
//! - Blobs are fetched once from R2 → cached in memory as `bytes::Bytes` (refcounted, no copy on slice)
//! - Arrow IPC slicing works on `Bytes` without copying
//! - yata-vex can wrap `Bytes` as Arrow RecordBatch

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

/// Object ID (uint64).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ObjectId(pub u64);

impl ObjectId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "o{:016x}", self.0)
    }
}

/// Blob type tag (what kind of data the blob contains).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BlobType {
    /// Arrow IPC vertex group (per-label).
    ArrowVertexGroup,
    /// Arrow IPC edge group (per-label).
    ArrowEdgeGroup,
    /// CSR topology segment (offsets + edge_ids).
    CsrTopology,
    /// Vector index (IVF_PQ or DiskANN).
    VectorIndex,
    /// Schema metadata.
    Schema,
    /// Raw bytes.
    Raw,
}

/// Object metadata (tree structure).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ObjectMeta {
    pub id: ObjectId,
    pub blob_type: BlobType,
    /// Human-readable label (e.g., "Person", "KNOWS").
    pub label: String,
    /// Partition ID (0 for single-partition).
    pub partition_id: u32,
    /// Size of the blob in bytes.
    pub size_bytes: u64,
    /// R2 key for the blob.
    pub r2_key: String,
    /// Arbitrary metadata fields.
    pub fields: HashMap<String, String>,
    /// Creation timestamp (ns).
    pub created_at: i64,
}

/// Blob cache trait — implemented by memory, disk, and mmap backends.
pub trait BlobCache: Send + Sync {
    /// Get object metadata by ID.
    fn get_meta(&self, id: ObjectId) -> Option<ObjectMeta>;

    /// Get blob data by ID. Returns refcounted bytes (no copy on slice).
    fn get_blob(&self, id: ObjectId) -> Option<bytes::Bytes>;

    /// Put a blob + metadata. Returns the assigned ObjectId.
    fn put(&self, meta: ObjectMeta, data: bytes::Bytes) -> ObjectId;

    /// List all objects matching a blob type.
    fn list(&self, blob_type: Option<&BlobType>) -> Vec<ObjectMeta>;

    /// Delete an object.
    fn delete(&self, id: ObjectId) -> bool;

    /// Total stored bytes.
    fn total_bytes(&self) -> u64;

    /// Memory cache bytes (for monitoring).
    fn cache_bytes(&self) -> u64 {
        0
    }

    /// List objects matching a label.
    fn list_by_label(&self, label: &str) -> Vec<ObjectMeta> {
        self.list(None)
            .into_iter()
            .filter(|m| m.label == label)
            .collect()
    }
}

// ── Memory Blob Cache (Cloudflare) ─────────────────────────────────

/// In-process blob cache. Blobs are cached in memory as `Bytes`.
/// Metadata is in a HashMap. Both can be synced to DO SQLite / R2 externally.
pub struct MemoryBlobCache {
    meta: Mutex<HashMap<ObjectId, ObjectMeta>>,
    blobs: Mutex<HashMap<ObjectId, bytes::Bytes>>,
    next_id: std::sync::atomic::AtomicU64,
    /// Memory budget for blob cache (bytes). 0 = unlimited.
    budget_bytes: u64,
}

impl MemoryBlobCache {
    /// Create a new in-memory blob cache with optional memory budget (MB).
    pub fn new(budget_mb: u64) -> Self {
        Self {
            meta: Mutex::new(HashMap::new()),
            blobs: Mutex::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
            budget_bytes: budget_mb * 1024 * 1024,
        }
    }

    /// Allocate a new ObjectId.
    fn alloc_id(&self) -> ObjectId {
        ObjectId(
            self.next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Export all metadata as JSON (for DO SQLite sync).
    pub fn export_meta(&self) -> Vec<ObjectMeta> {
        self.meta.lock().unwrap().values().cloned().collect()
    }

    /// Import metadata from external source (DO SQLite restore).
    pub fn import_meta(&self, metas: Vec<ObjectMeta>) {
        let mut m = self.meta.lock().unwrap();
        for meta in metas {
            let id = meta.id;
            if id.0 >= self.next_id.load(std::sync::atomic::Ordering::Relaxed) {
                self.next_id
                    .store(id.0 + 1, std::sync::atomic::Ordering::Relaxed);
            }
            m.insert(id, meta);
        }
    }

    /// Get blob by label and type (searches metadata for matching object).
    pub fn get_blob_by_label(&self, label: &str, blob_type: &BlobType) -> Option<bytes::Bytes> {
        let meta = self.meta.lock().unwrap();
        let id = meta
            .values()
            .find(|m| m.label == label && &m.blob_type == blob_type)?
            .id;
        drop(meta);
        self.blobs.lock().unwrap().get(&id).cloned()
    }

    /// Current cache size in bytes.
    pub fn cache_bytes(&self) -> u64 {
        self.blobs
            .lock()
            .unwrap()
            .values()
            .map(|b| b.len() as u64)
            .sum()
    }

    /// Evict blobs to fit within budget. LRU by ObjectId (lowest = oldest).
    fn maybe_evict(&self) {
        if self.budget_bytes == 0 {
            return;
        }
        let mut blobs = self.blobs.lock().unwrap();
        let mut total: u64 = blobs.values().map(|b| b.len() as u64).sum();
        if total <= self.budget_bytes {
            return;
        }

        let mut ids: Vec<ObjectId> = blobs.keys().copied().collect();
        ids.sort_by_key(|id| id.0);
        for id in ids {
            if total <= self.budget_bytes {
                break;
            }
            if let Some(blob) = blobs.remove(&id) {
                total -= blob.len() as u64;
            }
        }
    }
}

impl BlobCache for MemoryBlobCache {
    fn get_meta(&self, id: ObjectId) -> Option<ObjectMeta> {
        self.meta.lock().unwrap().get(&id).cloned()
    }

    fn get_blob(&self, id: ObjectId) -> Option<bytes::Bytes> {
        self.blobs.lock().unwrap().get(&id).cloned()
    }

    fn put(&self, mut meta: ObjectMeta, data: bytes::Bytes) -> ObjectId {
        let id = if meta.id.0 == 0 {
            self.alloc_id()
        } else {
            meta.id
        };
        meta.id = id;
        meta.size_bytes = data.len() as u64;
        self.meta.lock().unwrap().insert(id, meta);
        self.blobs.lock().unwrap().insert(id, data);
        self.maybe_evict();
        id
    }

    fn list(&self, blob_type: Option<&BlobType>) -> Vec<ObjectMeta> {
        let m = self.meta.lock().unwrap();
        match blob_type {
            Some(bt) => m.values().filter(|m| &m.blob_type == bt).cloned().collect(),
            None => m.values().cloned().collect(),
        }
    }

    fn delete(&self, id: ObjectId) -> bool {
        let meta_removed = self.meta.lock().unwrap().remove(&id).is_some();
        let blob_removed = self.blobs.lock().unwrap().remove(&id).is_some();
        meta_removed || blob_removed
    }

    fn total_bytes(&self) -> u64 {
        self.blobs
            .lock()
            .unwrap()
            .values()
            .map(|b| b.len() as u64)
            .sum()
    }
}

// ── Disk Blob Cache (Container) ─────────────────────────────────────

/// Disk-backed blob cache for CF Container (8GB disk).
/// Blobs stored as files: `{base_dir}/blobs/{object_id}`.
/// Metadata stored as JSON: `{base_dir}/meta/{object_id}.json`.
/// Page-in: file → read to Bytes. Page-out: drop from memory (LRU eviction).
pub struct DiskBlobCache {
    base_dir: std::path::PathBuf,
    meta_cache: Mutex<HashMap<ObjectId, ObjectMeta>>,
    blob_cache: Mutex<HashMap<ObjectId, bytes::Bytes>>,
    next_id: std::sync::atomic::AtomicU64,
    budget_bytes: u64,
}

impl DiskBlobCache {
    pub fn new(base_dir: impl Into<std::path::PathBuf>, budget_mb: u64) -> std::io::Result<Self> {
        let base_dir = base_dir.into();
        std::fs::create_dir_all(base_dir.join("blobs"))?;
        std::fs::create_dir_all(base_dir.join("meta"))?;

        // Scan existing metadata to find max ID
        let mut max_id = 0u64;
        if let Ok(entries) = std::fs::read_dir(base_dir.join("meta")) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if let Some(id_str) = name.strip_suffix(".json") {
                        if let Ok(id) = u64::from_str_radix(id_str, 16) {
                            if id > max_id {
                                max_id = id;
                            }
                        }
                    }
                }
            }
        }

        Ok(Self {
            base_dir,
            meta_cache: Mutex::new(HashMap::new()),
            blob_cache: Mutex::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(max_id + 1),
            budget_bytes: budget_mb * 1024 * 1024,
        })
    }

    fn blob_path(&self, id: ObjectId) -> std::path::PathBuf {
        self.base_dir.join("blobs").join(format!("{:016x}", id.0))
    }

    fn meta_path(&self, id: ObjectId) -> std::path::PathBuf {
        self.base_dir
            .join("meta")
            .join(format!("{:016x}.json", id.0))
    }

    fn alloc_id(&self) -> ObjectId {
        ObjectId(
            self.next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Page-in: load blob from disk into memory cache.
    pub fn page_in(&self, id: ObjectId) -> Option<bytes::Bytes> {
        // Check memory cache first
        if let Some(b) = self.blob_cache.lock().unwrap().get(&id) {
            return Some(b.clone());
        }
        // Read from disk
        let path = self.blob_path(id);
        let data = std::fs::read(&path).ok()?;
        let bytes = bytes::Bytes::from(data);
        self.blob_cache.lock().unwrap().insert(id, bytes.clone());
        self.maybe_evict();
        Some(bytes)
    }

    /// Page-out: evict blob from memory cache (stays on disk).
    pub fn page_out(&self, id: ObjectId) {
        self.blob_cache.lock().unwrap().remove(&id);
    }

    /// Evict blobs from memory to stay within budget.
    fn maybe_evict(&self) {
        if self.budget_bytes == 0 {
            return;
        }
        let mut cache = self.blob_cache.lock().unwrap();
        let mut total: u64 = cache.values().map(|b| b.len() as u64).sum();
        if total <= self.budget_bytes {
            return;
        }
        let mut ids: Vec<ObjectId> = cache.keys().copied().collect();
        ids.sort_by_key(|id| id.0);
        for id in ids {
            if total <= self.budget_bytes {
                break;
            }
            if let Some(blob) = cache.remove(&id) {
                total -= blob.len() as u64;
            }
        }
    }

    /// Memory cache usage.
    pub fn cache_bytes(&self) -> u64 {
        self.blob_cache
            .lock()
            .unwrap()
            .values()
            .map(|b| b.len() as u64)
            .sum()
    }

    /// Total disk usage (all blobs).
    pub fn disk_bytes(&self) -> u64 {
        let mut total = 0u64;
        if let Ok(entries) = std::fs::read_dir(self.base_dir.join("blobs")) {
            for entry in entries.flatten() {
                if let Ok(m) = entry.metadata() {
                    total += m.len();
                }
            }
        }
        total
    }

    /// Load all metadata from disk into cache.
    pub fn load_all_meta(&self) -> usize {
        let mut count = 0;
        let mut cache = self.meta_cache.lock().unwrap();
        if let Ok(entries) = std::fs::read_dir(self.base_dir.join("meta")) {
            for entry in entries.flatten() {
                if let Ok(data) = std::fs::read(entry.path()) {
                    if let Ok(meta) = serde_json::from_slice::<ObjectMeta>(&data) {
                        cache.insert(meta.id, meta);
                        count += 1;
                    }
                }
            }
        }
        count
    }

    /// Export all metadata (for R2 sync).
    pub fn export_meta(&self) -> Vec<ObjectMeta> {
        self.meta_cache.lock().unwrap().values().cloned().collect()
    }
}

impl BlobCache for DiskBlobCache {
    fn get_meta(&self, id: ObjectId) -> Option<ObjectMeta> {
        // Memory cache
        if let Some(m) = self.meta_cache.lock().unwrap().get(&id) {
            return Some(m.clone());
        }
        // Disk
        let data = std::fs::read(self.meta_path(id)).ok()?;
        let meta: ObjectMeta = serde_json::from_slice(&data).ok()?;
        self.meta_cache.lock().unwrap().insert(id, meta.clone());
        Some(meta)
    }

    fn get_blob(&self, id: ObjectId) -> Option<bytes::Bytes> {
        self.page_in(id)
    }

    fn put(&self, mut meta: ObjectMeta, data: bytes::Bytes) -> ObjectId {
        let id = if meta.id.0 == 0 {
            self.alloc_id()
        } else {
            meta.id
        };
        meta.id = id;
        meta.size_bytes = data.len() as u64;

        // Write blob to disk
        let _ = std::fs::write(self.blob_path(id), &data);
        // Write metadata to disk
        let _ = std::fs::write(
            self.meta_path(id),
            serde_json::to_vec(&meta).unwrap_or_default(),
        );
        // Update caches
        self.meta_cache.lock().unwrap().insert(id, meta);
        self.blob_cache.lock().unwrap().insert(id, data);
        self.maybe_evict();
        id
    }

    fn list(&self, blob_type: Option<&BlobType>) -> Vec<ObjectMeta> {
        let m = self.meta_cache.lock().unwrap();
        match blob_type {
            Some(bt) => m.values().filter(|m| &m.blob_type == bt).cloned().collect(),
            None => m.values().cloned().collect(),
        }
    }

    fn delete(&self, id: ObjectId) -> bool {
        let _ = std::fs::remove_file(self.blob_path(id));
        let _ = std::fs::remove_file(self.meta_path(id));
        self.meta_cache.lock().unwrap().remove(&id);
        self.blob_cache.lock().unwrap().remove(&id);
        true
    }

    fn total_bytes(&self) -> u64 {
        self.disk_bytes()
    }
}

// ── Mmap Blob Cache (Out-of-Core, trillion-scale) ───────────────────

/// Mmap-backed blob cache — zero-copy access to Arrow IPC blobs via OS page cache.
/// Capacity: 8GB disk = ~500M edges (vs DiskBlobCache's 4GB RAM limit).
///
/// Key difference from DiskBlobCache:
///   DiskBlobCache: `page_in()` reads entire file into `Bytes` (RAM copy) → LRU eviction by dropping `Bytes`
///   MmapBlobCache: `page_in()` creates `Mmap` handle → OS manages 4KB page granularity → no explicit RAM management
pub struct MmapBlobCache {
    base_dir: std::path::PathBuf,
    /// mmap handles keyed by ObjectId — OS manages page-in/out at 4KB granularity.
    mmaps: Mutex<HashMap<ObjectId, memmap2::Mmap>>,
    /// Metadata cache.
    meta_cache: Mutex<HashMap<ObjectId, ObjectMeta>>,
    next_id: std::sync::atomic::AtomicU64,
    /// Total mapped bytes (for stats/monitoring).
    mapped_bytes: std::sync::atomic::AtomicU64,
}

impl MmapBlobCache {
    /// Create a new mmap blob cache at the given directory.
    /// Scans existing metadata files to resume from prior state.
    pub fn new(base_dir: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        let base_dir = base_dir.into();
        std::fs::create_dir_all(base_dir.join("blobs"))?;
        std::fs::create_dir_all(base_dir.join("meta"))?;

        let mut max_id = 0u64;
        if let Ok(entries) = std::fs::read_dir(base_dir.join("meta")) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if let Some(id_str) = name.strip_suffix(".json") {
                        if let Ok(id) = u64::from_str_radix(id_str, 16) {
                            if id > max_id {
                                max_id = id;
                            }
                        }
                    }
                }
            }
        }

        Ok(Self {
            base_dir,
            mmaps: Mutex::new(HashMap::new()),
            meta_cache: Mutex::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(max_id + 1),
            mapped_bytes: std::sync::atomic::AtomicU64::new(0),
        })
    }

    fn blob_path(&self, id: ObjectId) -> std::path::PathBuf {
        self.base_dir.join("blobs").join(format!("{:016x}", id.0))
    }

    fn meta_path(&self, id: ObjectId) -> std::path::PathBuf {
        self.base_dir
            .join("meta")
            .join(format!("{:016x}.json", id.0))
    }

    fn alloc_id(&self) -> ObjectId {
        ObjectId(
            self.next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Page-in: create mmap handle for the blob file. OS manages actual page faults.
    /// Unlike DiskBlobCache, does NOT copy the file into RAM — the OS page cache handles it.
    pub fn page_in(&self, id: ObjectId) -> bool {
        let mut mmaps = self.mmaps.lock().unwrap();
        if mmaps.contains_key(&id) {
            return true;
        }
        let path = self.blob_path(id);
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => return false,
        };
        let mmap = match unsafe { memmap2::Mmap::map(&file) } {
            Ok(m) => m,
            Err(_) => return false,
        };
        let len = mmap.len() as u64;
        mmaps.insert(id, mmap);
        self.mapped_bytes
            .fetch_add(len, std::sync::atomic::Ordering::Relaxed);
        true
    }

    /// Page-out: drop the mmap handle. OS reclaims pages as needed.
    pub fn page_out(&self, id: ObjectId) {
        let mut mmaps = self.mmaps.lock().unwrap();
        if let Some(mmap) = mmaps.remove(&id) {
            let len = mmap.len() as u64;
            self.mapped_bytes
                .fetch_sub(len, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Total bytes currently mapped (for monitoring).
    pub fn mapped_bytes(&self) -> u64 {
        self.mapped_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Load all metadata from disk into cache.
    pub fn load_all_meta(&self) -> usize {
        let mut count = 0;
        let mut cache = self.meta_cache.lock().unwrap();
        if let Ok(entries) = std::fs::read_dir(self.base_dir.join("meta")) {
            for entry in entries.flatten() {
                if let Ok(data) = std::fs::read(entry.path()) {
                    if let Ok(meta) = serde_json::from_slice::<ObjectMeta>(&data) {
                        cache.insert(meta.id, meta);
                        count += 1;
                    }
                }
            }
        }
        count
    }

    /// Total disk usage (all blobs).
    pub fn disk_bytes(&self) -> u64 {
        let mut total = 0u64;
        if let Ok(entries) = std::fs::read_dir(self.base_dir.join("blobs")) {
            for entry in entries.flatten() {
                if let Ok(m) = entry.metadata() {
                    total += m.len();
                }
            }
        }
        total
    }

    /// Export all metadata (for R2 sync).
    pub fn export_meta(&self) -> Vec<ObjectMeta> {
        self.meta_cache.lock().unwrap().values().cloned().collect()
    }
}

impl BlobCache for MmapBlobCache {
    fn get_meta(&self, id: ObjectId) -> Option<ObjectMeta> {
        if let Some(m) = self.meta_cache.lock().unwrap().get(&id) {
            return Some(m.clone());
        }
        let data = std::fs::read(self.meta_path(id)).ok()?;
        let meta: ObjectMeta = serde_json::from_slice(&data).ok()?;
        self.meta_cache.lock().unwrap().insert(id, meta.clone());
        Some(meta)
    }

    fn get_blob(&self, id: ObjectId) -> Option<bytes::Bytes> {
        // Ensure mmap handle exists (page-in on demand).
        let mmaps = self.mmaps.lock().unwrap();
        if let Some(mmap) = mmaps.get(&id) {
            // Return Bytes backed by a copy of the mmap slice.
            // The mmap itself is zero-copy from disk via OS page cache;
            // we copy into Bytes here so the caller can hold it after the lock drops.
            return Some(bytes::Bytes::copy_from_slice(&mmap[..]));
        }
        drop(mmaps);

        // Auto page-in from disk.
        if !self.page_in(id) {
            return None;
        }
        let mmaps = self.mmaps.lock().unwrap();
        mmaps
            .get(&id)
            .map(|mmap| bytes::Bytes::copy_from_slice(&mmap[..]))
    }

    fn put(&self, mut meta: ObjectMeta, data: bytes::Bytes) -> ObjectId {
        let id = if meta.id.0 == 0 {
            self.alloc_id()
        } else {
            meta.id
        };
        meta.id = id;
        meta.size_bytes = data.len() as u64;

        // Write blob to disk.
        let _ = std::fs::write(self.blob_path(id), &data);
        // Write metadata to disk.
        let _ = std::fs::write(
            self.meta_path(id),
            serde_json::to_vec(&meta).unwrap_or_default(),
        );
        // Update meta cache.
        self.meta_cache.lock().unwrap().insert(id, meta);
        // Mmap the new blob immediately.
        self.page_in(id);
        id
    }

    fn list(&self, blob_type: Option<&BlobType>) -> Vec<ObjectMeta> {
        let m = self.meta_cache.lock().unwrap();
        match blob_type {
            Some(bt) => m.values().filter(|m| &m.blob_type == bt).cloned().collect(),
            None => m.values().cloned().collect(),
        }
    }

    fn delete(&self, id: ObjectId) -> bool {
        self.page_out(id);
        let _ = std::fs::remove_file(self.blob_path(id));
        let _ = std::fs::remove_file(self.meta_path(id));
        self.meta_cache.lock().unwrap().remove(&id);
        true
    }

    fn total_bytes(&self) -> u64 {
        self.disk_bytes()
    }

    fn cache_bytes(&self) -> u64 {
        self.mapped_bytes()
    }
}

// ── Graph fragment ──────────────────────────────────────────────────

/// A graph fragment stored as Arrow IPC blobs in a BlobCache.
pub struct GraphFragment {
    /// Blob cache backing this fragment.
    store: Arc<dyn BlobCache>,
    /// Vertex group objects by label.
    vertex_objects: HashMap<String, ObjectId>,
    /// Edge group objects by label.
    edge_objects: HashMap<String, ObjectId>,
    /// CSR topology object (optional).
    csr_object: Option<ObjectId>,
    /// Partition ID.
    pub partition_id: u32,
}

impl GraphFragment {
    pub fn new(store: Arc<dyn BlobCache>, partition_id: u32) -> Self {
        Self {
            store,
            vertex_objects: HashMap::new(),
            edge_objects: HashMap::new(),
            csr_object: None,
            partition_id,
        }
    }

    /// Register a vertex group blob.
    pub fn add_vertex_group(&mut self, label: &str, id: ObjectId) {
        self.vertex_objects.insert(label.to_string(), id);
    }

    /// Register an edge group blob.
    pub fn add_edge_group(&mut self, label: &str, id: ObjectId) {
        self.edge_objects.insert(label.to_string(), id);
    }

    /// Register CSR topology blob.
    pub fn set_csr(&mut self, id: ObjectId) {
        self.csr_object = Some(id);
    }

    /// Get vertex group Arrow IPC bytes for a label (lazy: fetched on demand).
    pub fn vertex_group_bytes(&self, label: &str) -> Option<bytes::Bytes> {
        let id = self.vertex_objects.get(label)?;
        self.store.get_blob(*id)
    }

    /// Get edge group Arrow IPC bytes for a label.
    pub fn edge_group_bytes(&self, label: &str) -> Option<bytes::Bytes> {
        let id = self.edge_objects.get(label)?;
        self.store.get_blob(*id)
    }

    /// Get CSR topology bytes.
    pub fn csr_bytes(&self) -> Option<bytes::Bytes> {
        let id = self.csr_object?;
        self.store.get_blob(id)
    }

    /// Available vertex labels.
    pub fn vertex_labels(&self) -> Vec<String> {
        self.vertex_objects.keys().cloned().collect()
    }

    /// Available edge labels.
    pub fn edge_labels(&self) -> Vec<String> {
        self.edge_objects.keys().cloned().collect()
    }

    /// Total bytes across all blobs.
    pub fn total_bytes(&self) -> u64 {
        let mut total = 0u64;
        for id in self
            .vertex_objects
            .values()
            .chain(self.edge_objects.values())
        {
            if let Some(meta) = self.store.get_meta(*id) {
                total += meta.size_bytes;
            }
        }
        if let Some(csr_id) = self.csr_object {
            if let Some(meta) = self.store.get_meta(csr_id) {
                total += meta.size_bytes;
            }
        }
        total
    }

    /// Export a manifest describing the current fragment state.
    pub fn to_manifest(&self) -> FragmentManifest {
        let mut vertex_count = 0u64;
        let mut edge_count = 0u64;
        for id in self.vertex_objects.values() {
            if let Some(meta) = self.store.get_meta(*id) {
                vertex_count += meta
                    .fields
                    .get("row_count")
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
            }
        }
        for id in self.edge_objects.values() {
            if let Some(meta) = self.store.get_meta(*id) {
                edge_count += meta
                    .fields
                    .get("row_count")
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
            }
        }
        FragmentManifest {
            vertex_labels: self.vertex_objects.clone(),
            edge_labels: self.edge_objects.clone(),
            csr_object: self.csr_object,
            schema_object: None,
            partition_id: self.partition_id,
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            vertex_count,
            edge_count,
        }
    }

    /// Reconstruct a fragment from a persisted manifest.
    pub fn from_manifest(store: Arc<dyn BlobCache>, manifest: &FragmentManifest) -> Self {
        Self {
            store,
            vertex_objects: manifest.vertex_labels.clone(),
            edge_objects: manifest.edge_labels.clone(),
            csr_object: manifest.csr_object,
            partition_id: manifest.partition_id,
        }
    }

    /// Read-only access to vertex label → ObjectId mapping.
    pub fn vertex_object_ids(&self) -> &HashMap<String, ObjectId> {
        &self.vertex_objects
    }

    /// Read-only access to edge label → ObjectId mapping.
    pub fn edge_object_ids(&self) -> &HashMap<String, ObjectId> {
        &self.edge_objects
    }

    /// Underlying blob cache.
    pub fn store(&self) -> &Arc<dyn BlobCache> {
        &self.store
    }
}

// ── Fragment manifest ───────────────────────────────────────────────

/// Serializable manifest for a graph fragment.
/// Maps label names → ObjectIds so the fragment can be
/// reconstructed without re-scanning the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentManifest {
    /// Vertex label → ObjectId (Arrow IPC blob).
    pub vertex_labels: HashMap<String, ObjectId>,
    /// Edge label → ObjectId (Arrow IPC blob).
    pub edge_labels: HashMap<String, ObjectId>,
    /// CSR topology blob (optional).
    pub csr_object: Option<ObjectId>,
    /// Schema metadata blob (optional).
    pub schema_object: Option<ObjectId>,
    /// Partition ID.
    pub partition_id: u32,
    /// Snapshot timestamp (ns since epoch).
    pub timestamp_ns: i64,
    /// Total vertex count across all labels.
    pub vertex_count: u64,
    /// Total edge count across all labels.
    pub edge_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(label: &str, bt: BlobType) -> ObjectMeta {
        ObjectMeta {
            id: ObjectId(0), // auto-assign
            blob_type: bt,
            label: label.to_string(),
            partition_id: 0,
            size_bytes: 0,
            r2_key: format!("vineyard/test/{}", label),
            fields: HashMap::new(),
            created_at: 0,
        }
    }

    #[test]
    fn test_memory_blob_cache_put_get() {
        let store = MemoryBlobCache::new(0);
        let data = bytes::Bytes::from_static(b"arrow-ipc-data-here");
        let meta = make_meta("Person", BlobType::ArrowVertexGroup);

        let id = store.put(meta, data.clone());
        assert!(id.0 > 0);

        let got = store.get_blob(id).unwrap();
        assert_eq!(got, data);

        let got_meta = store.get_meta(id).unwrap();
        assert_eq!(got_meta.label, "Person");
        assert_eq!(got_meta.size_bytes, 19);
    }

    #[test]
    fn test_memory_blob_cache_list() {
        let store = MemoryBlobCache::new(0);
        store.put(
            make_meta("Person", BlobType::ArrowVertexGroup),
            bytes::Bytes::from_static(b"a"),
        );
        store.put(
            make_meta("KNOWS", BlobType::ArrowEdgeGroup),
            bytes::Bytes::from_static(b"b"),
        );
        store.put(
            make_meta("Company", BlobType::ArrowVertexGroup),
            bytes::Bytes::from_static(b"c"),
        );

        let all = store.list(None);
        assert_eq!(all.len(), 3);

        let vertices = store.list(Some(&BlobType::ArrowVertexGroup));
        assert_eq!(vertices.len(), 2);

        let edges = store.list(Some(&BlobType::ArrowEdgeGroup));
        assert_eq!(edges.len(), 1);
    }

    #[test]
    fn test_memory_blob_cache_delete() {
        let store = MemoryBlobCache::new(0);
        let id = store.put(
            make_meta("X", BlobType::Raw),
            bytes::Bytes::from_static(b"data"),
        );
        assert!(store.get_blob(id).is_some());
        assert!(store.delete(id));
        assert!(store.get_blob(id).is_none());
    }

    #[test]
    fn test_memory_blob_cache_budget_eviction() {
        let store = MemoryBlobCache::new(1); // 1MB budget
        // Put 2MB of data — should evict oldest
        let big = bytes::Bytes::from(vec![0u8; 600_000]); // 600KB
        let id1 = store.put(make_meta("A", BlobType::Raw), big.clone());
        let id2 = store.put(make_meta("B", BlobType::Raw), big.clone());
        let id3 = store.put(make_meta("C", BlobType::Raw), big.clone());

        // id1 should be evicted (oldest)
        assert!(store.get_blob(id1).is_none());
        // id3 should still be present
        assert!(store.get_blob(id3).is_some());
        assert!(store.cache_bytes() <= 1_200_000);
    }

    #[test]
    fn test_graph_fragment() {
        let store = Arc::new(MemoryBlobCache::new(0));

        let v_id = store.put(
            make_meta("Person", BlobType::ArrowVertexGroup),
            bytes::Bytes::from_static(b"vertex-arrow-ipc"),
        );
        let e_id = store.put(
            make_meta("KNOWS", BlobType::ArrowEdgeGroup),
            bytes::Bytes::from_static(b"edge-arrow-ipc"),
        );

        let mut frag = GraphFragment::new(store.clone(), 0);
        frag.add_vertex_group("Person", v_id);
        frag.add_edge_group("KNOWS", e_id);

        assert_eq!(frag.vertex_labels(), vec!["Person"]);
        assert_eq!(frag.edge_labels(), vec!["KNOWS"]);
        assert_eq!(
            frag.vertex_group_bytes("Person").unwrap(),
            &b"vertex-arrow-ipc"[..]
        );
        assert_eq!(
            frag.edge_group_bytes("KNOWS").unwrap(),
            &b"edge-arrow-ipc"[..]
        );
        assert!(frag.total_bytes() > 0);
    }

    #[test]
    fn test_export_import_meta() {
        let store = MemoryBlobCache::new(0);
        store.put(
            make_meta("A", BlobType::Raw),
            bytes::Bytes::from_static(b"a"),
        );
        store.put(
            make_meta("B", BlobType::Raw),
            bytes::Bytes::from_static(b"b"),
        );

        let exported = store.export_meta();
        assert_eq!(exported.len(), 2);

        let store2 = MemoryBlobCache::new(0);
        store2.import_meta(exported);
        assert_eq!(store2.list(None).len(), 2);
    }

    #[test]
    fn test_object_id_display() {
        let id = ObjectId(255);
        assert_eq!(format!("{}", id), "o00000000000000ff");
    }

    // ── DiskBlobCache tests ───────────────────────────────────────

    #[test]
    fn test_disk_blob_cache_put_get() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskBlobCache::new(dir.path(), 0).unwrap();
        let data = bytes::Bytes::from_static(b"disk-arrow-ipc");
        let meta = make_meta("Person", BlobType::ArrowVertexGroup);

        let id = store.put(meta, data.clone());
        assert!(id.0 > 0);

        // Get from memory cache
        let got = store.get_blob(id).unwrap();
        assert_eq!(got, data);

        // Evict from cache, get from disk
        store.page_out(id);
        let got2 = store.get_blob(id).unwrap();
        assert_eq!(got2, data);

        // Metadata
        let meta = store.get_meta(id).unwrap();
        assert_eq!(meta.label, "Person");
    }

    #[test]
    fn test_disk_blob_cache_page_in_out() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskBlobCache::new(dir.path(), 1).unwrap(); // 1MB budget
        let big = bytes::Bytes::from(vec![42u8; 500_000]); // 500KB

        let id1 = store.put(make_meta("A", BlobType::Raw), big.clone());
        let id2 = store.put(make_meta("B", BlobType::Raw), big.clone());
        let id3 = store.put(make_meta("C", BlobType::Raw), big.clone());

        // id1 evicted from memory (budget exceeded)
        assert_eq!(store.cache_bytes() <= 1_100_000, true);

        // But still on disk — page_in works
        let got = store.page_in(id1);
        assert!(got.is_some());
        assert_eq!(got.unwrap().len(), 500_000);
    }

    #[test]
    fn test_disk_blob_cache_persist_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let id;
        {
            let store = DiskBlobCache::new(dir.path(), 0).unwrap();
            id = store.put(
                make_meta("X", BlobType::Raw),
                bytes::Bytes::from_static(b"persist"),
            );
        }
        // Reopen
        let store2 = DiskBlobCache::new(dir.path(), 0).unwrap();
        store2.load_all_meta();
        let meta = store2.get_meta(id).unwrap();
        assert_eq!(meta.label, "X");
        let blob = store2.get_blob(id).unwrap();
        assert_eq!(&blob[..], b"persist");
    }

    #[test]
    fn test_disk_blob_cache_fragment() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(DiskBlobCache::new(dir.path(), 0).unwrap());

        let v_id = store.put(
            make_meta("Person", BlobType::ArrowVertexGroup),
            bytes::Bytes::from_static(b"vertex-data"),
        );
        let e_id = store.put(
            make_meta("KNOWS", BlobType::ArrowEdgeGroup),
            bytes::Bytes::from_static(b"edge-data"),
        );

        let mut frag = GraphFragment::new(store.clone(), 0);
        frag.add_vertex_group("Person", v_id);
        frag.add_edge_group("KNOWS", e_id);

        assert_eq!(
            frag.vertex_group_bytes("Person").unwrap(),
            &b"vertex-data"[..]
        );
        assert_eq!(frag.edge_group_bytes("KNOWS").unwrap(), &b"edge-data"[..]);
        assert!(frag.total_bytes() > 0);
    }

    #[test]
    fn test_disk_blob_cache_disk_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = DiskBlobCache::new(dir.path(), 0).unwrap();
        store.put(
            make_meta("A", BlobType::Raw),
            bytes::Bytes::from(vec![0u8; 1000]),
        );
        store.put(
            make_meta("B", BlobType::Raw),
            bytes::Bytes::from(vec![0u8; 2000]),
        );
        assert_eq!(store.disk_bytes(), 3000);
    }

    #[test]
    fn test_bytes_zero_copy_slice() {
        // Demonstrate that Bytes slice doesn't copy
        let full = bytes::Bytes::from(vec![1u8, 2, 3, 4, 5, 6, 7, 8]);
        let slice = full.slice(2..6); // [3, 4, 5, 6]
        assert_eq!(&slice[..], &[3, 4, 5, 6]);
        // Both point to the same allocation (refcounted)
        assert_eq!(full.len(), 8);
        assert_eq!(slice.len(), 4);
    }

    // ── MmapBlobCache tests ───────────────────────────────────────

    #[test]
    fn test_mmap_put_get() {
        let dir = tempfile::tempdir().unwrap();
        let store = MmapBlobCache::new(dir.path()).unwrap();
        let data = bytes::Bytes::from_static(b"mmap-arrow-ipc-data");
        let meta = make_meta("Person", BlobType::ArrowVertexGroup);

        let id = store.put(meta, data.clone());
        assert!(id.0 > 0);

        let got = store.get_blob(id).unwrap();
        assert_eq!(got, data);

        let got_meta = store.get_meta(id).unwrap();
        assert_eq!(got_meta.label, "Person");
        assert_eq!(got_meta.size_bytes, 19);
    }

    #[test]
    fn test_mmap_page_in_out() {
        let dir = tempfile::tempdir().unwrap();
        let store = MmapBlobCache::new(dir.path()).unwrap();
        let data = bytes::Bytes::from(vec![42u8; 4096]);

        let id = store.put(make_meta("A", BlobType::Raw), data.clone());
        assert!(store.mapped_bytes() >= 4096);

        // Page out — drops mmap handle.
        store.page_out(id);
        assert_eq!(store.mapped_bytes(), 0);

        // Page in — re-creates mmap handle, data still accessible.
        assert!(store.page_in(id));
        assert!(store.mapped_bytes() >= 4096);
        let got = store.get_blob(id).unwrap();
        assert_eq!(got, data);
    }

    #[test]
    fn test_mmap_list() {
        let dir = tempfile::tempdir().unwrap();
        let store = MmapBlobCache::new(dir.path()).unwrap();
        store.put(
            make_meta("Person", BlobType::ArrowVertexGroup),
            bytes::Bytes::from_static(b"a"),
        );
        store.put(
            make_meta("KNOWS", BlobType::ArrowEdgeGroup),
            bytes::Bytes::from_static(b"b"),
        );
        store.put(
            make_meta("Company", BlobType::ArrowVertexGroup),
            bytes::Bytes::from_static(b"c"),
        );

        let all = store.list(None);
        assert_eq!(all.len(), 3);

        let vertices = store.list(Some(&BlobType::ArrowVertexGroup));
        assert_eq!(vertices.len(), 2);

        let edges = store.list(Some(&BlobType::ArrowEdgeGroup));
        assert_eq!(edges.len(), 1);
    }

    #[test]
    fn test_mmap_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = MmapBlobCache::new(dir.path()).unwrap();
        let id = store.put(
            make_meta("X", BlobType::Raw),
            bytes::Bytes::from_static(b"data"),
        );
        assert!(store.get_blob(id).is_some());
        assert!(store.delete(id));
        assert!(store.get_blob(id).is_none());
        assert!(store.get_meta(id).is_none());
        // File should be gone too.
        assert!(!store.blob_path(id).exists());
    }

    #[test]
    fn test_mmap_zero_copy_tracking() {
        let dir = tempfile::tempdir().unwrap();
        let store = MmapBlobCache::new(dir.path()).unwrap();

        let data1 = bytes::Bytes::from(vec![0u8; 1000]);
        let data2 = bytes::Bytes::from(vec![0u8; 2000]);

        let id1 = store.put(make_meta("A", BlobType::Raw), data1);
        let id2 = store.put(make_meta("B", BlobType::Raw), data2);

        // mapped_bytes tracks both mmaps.
        assert_eq!(store.mapped_bytes(), 3000);

        // Page out one — only that portion removed.
        store.page_out(id1);
        assert_eq!(store.mapped_bytes(), 2000);

        store.page_out(id2);
        assert_eq!(store.mapped_bytes(), 0);
    }

    #[test]
    fn test_mmap_large_blob() {
        let dir = tempfile::tempdir().unwrap();
        let store = MmapBlobCache::new(dir.path()).unwrap();
        let big = bytes::Bytes::from(vec![0xABu8; 1_000_000]); // 1MB

        let id = store.put(make_meta("Large", BlobType::Raw), big.clone());
        assert!(store.mapped_bytes() >= 1_000_000);

        let got = store.get_blob(id).unwrap();
        assert_eq!(got.len(), 1_000_000);
        assert_eq!(got[0], 0xAB);
        assert_eq!(got[999_999], 0xAB);
    }

    #[test]
    fn test_mmap_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let id1;
        let id2;
        {
            let store = MmapBlobCache::new(dir.path()).unwrap();
            id1 = store.put(
                make_meta("A", BlobType::Raw),
                bytes::Bytes::from_static(b"persist-a"),
            );
            id2 = store.put(
                make_meta("B", BlobType::ArrowVertexGroup),
                bytes::Bytes::from_static(b"persist-b"),
            );
        }
        // Reopen at same directory — blobs should still be on disk.
        let store2 = MmapBlobCache::new(dir.path()).unwrap();
        store2.load_all_meta();

        let meta1 = store2.get_meta(id1).unwrap();
        assert_eq!(meta1.label, "A");
        let blob1 = store2.get_blob(id1).unwrap();
        assert_eq!(&blob1[..], b"persist-a");

        let meta2 = store2.get_meta(id2).unwrap();
        assert_eq!(meta2.label, "B");
        let blob2 = store2.get_blob(id2).unwrap();
        assert_eq!(&blob2[..], b"persist-b");

        // Disk bytes should reflect both files.
        assert_eq!(store2.disk_bytes(), 9 + 9); // "persist-a" + "persist-b"
    }
}
