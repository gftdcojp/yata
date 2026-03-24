//! Lazy vector index: defers index build until first search.
//! Supports incremental append, automatic brute-force → IVF_PQ promotion,
//! disk persistence (Arrow IPC mmap), and S3/CAS restore on cold start.

use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::distance;
use crate::ivf_pq::{IvfPqConfig, IvfPqIndex};
use crate::quantize::PqConfig;
use crate::storage::FlatVectorStore;
use crate::{BRUTE_FORCE_THRESHOLD, DistanceMetric, Result, SearchResult, VectorIndex, VexError};

const VECTORS_FILE: &str = "vex_vectors.arrow";

/// Lazy vector index with automatic tiering:
/// - < BRUTE_FORCE_THRESHOLD vectors: brute-force scan (no index overhead)
/// - >= BRUTE_FORCE_THRESHOLD vectors: IVF_PQ (built on first search after dirty)
///
/// Persistence:
/// - `data_dir` set → vectors persisted to Arrow IPC file on disk
/// - `persist_to_cas()` → snapshot to CAS (Blake3-addressed, S3 write-through)
/// - `restore()` → try disk first, then CAS (S3 fallback)
pub struct LazyVectorIndex {
    store: RwLock<FlatVectorStore>,
    index: RwLock<Option<Box<dyn VectorIndex>>>,
    dirty: AtomicBool,
    /// Disk dirty: new vectors since last persist_to_disk.
    disk_dirty: AtomicBool,
    count: AtomicUsize,
    dim: AtomicUsize,
    metric: DistanceMetric,
    /// Local data directory for Arrow IPC persistence.
    data_dir: RwLock<Option<PathBuf>>,
    /// Whether store was restored from disk (skip re-read on search).
    restored: AtomicBool,
}

impl LazyVectorIndex {
    pub fn new(dim: usize, metric: DistanceMetric) -> Self {
        Self {
            store: RwLock::new(FlatVectorStore::new(dim)),
            index: RwLock::new(None),
            dirty: AtomicBool::new(false),
            disk_dirty: AtomicBool::new(false),
            count: AtomicUsize::new(0),
            dim: AtomicUsize::new(dim),
            metric,
            data_dir: RwLock::new(None),
            restored: AtomicBool::new(false),
        }
    }

    /// Create with a data directory for disk persistence.
    pub fn with_data_dir(dim: usize, metric: DistanceMetric, data_dir: impl Into<PathBuf>) -> Self {
        let dir = data_dir.into();
        let idx = Self::new(dim, metric);
        *idx.data_dir.write().unwrap() = Some(dir);
        idx
    }

    /// Set data directory (can be called after construction).
    pub fn set_data_dir(&self, dir: impl Into<PathBuf>) {
        *self.data_dir.write().unwrap() = Some(dir.into());
    }

    /// Append vectors. Index is NOT rebuilt — marked dirty for lazy rebuild on search.
    pub fn append(&self, vid: u64, vector: &[f32]) -> Result<()> {
        let dim = self.dim.load(Ordering::Relaxed);
        if dim == 0 {
            self.dim.store(vector.len(), Ordering::Relaxed);
            let mut store = self.store.write().unwrap();
            *store = FlatVectorStore::new(vector.len());
            store.push(vid, vector);
        } else {
            if vector.len() != dim {
                return Err(VexError::DimensionMismatch {
                    expected: dim,
                    got: vector.len(),
                });
            }
            self.store.write().unwrap().push(vid, vector);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
        self.disk_dirty.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Append a batch of vectors.
    pub fn append_batch(&self, vids: &[u64], vectors: &[f32], dim: usize) -> Result<()> {
        let n = vids.len();
        if vectors.len() != n * dim {
            return Err(VexError::DimensionMismatch {
                expected: n * dim,
                got: vectors.len(),
            });
        }
        let stored_dim = self.dim.load(Ordering::Relaxed);
        if stored_dim == 0 {
            self.dim.store(dim, Ordering::Relaxed);
            let mut store = self.store.write().unwrap();
            *store = FlatVectorStore::new(dim);
            for i in 0..n {
                store.push(vids[i], &vectors[i * dim..(i + 1) * dim]);
            }
        } else {
            if dim != stored_dim {
                return Err(VexError::DimensionMismatch {
                    expected: stored_dim,
                    got: dim,
                });
            }
            let mut store = self.store.write().unwrap();
            for i in 0..n {
                store.push(vids[i], &vectors[i * dim..(i + 1) * dim]);
            }
        }
        self.count.fetch_add(n, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
        self.disk_dirty.store(true, Ordering::Relaxed);
        Ok(())
    }

    // ---- Disk persistence ----

    /// Persist vector store to Arrow IPC file on local disk.
    /// Returns the file path written.
    pub fn persist_to_disk(&self) -> Result<Option<PathBuf>> {
        if !self.disk_dirty.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let dir = self.data_dir.read().unwrap();
        let dir = match dir.as_ref() {
            Some(d) => d.clone(),
            None => return Ok(None),
        };
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(VECTORS_FILE);

        let store = self.store.read().unwrap();
        if store.is_empty() {
            return Ok(None);
        }
        let ipc_bytes = store.to_ipc_file()?;
        std::fs::write(&path, &ipc_bytes)?;
        self.disk_dirty.store(false, Ordering::Relaxed);
        Ok(Some(path))
    }

    /// Persist to CAS (content-addressed). Returns Blake3 hash of the snapshot.
    /// If S3CasStore is used, this automatically writes through to R2.
    pub fn persist_to_cas_sync<F>(&self, cas_put: F) -> Result<Option<yata_core::Blake3Hash>>
    where
        F: Fn(bytes::Bytes) -> std::result::Result<yata_core::Blake3Hash, String>,
    {
        let store = self.store.read().unwrap();
        if store.is_empty() {
            return Ok(None);
        }
        let ipc_bytes = store.to_ipc_file()?;
        let hash = cas_put(bytes::Bytes::from(ipc_bytes.to_vec()))
            .map_err(|e| VexError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        Ok(Some(hash))
    }

    /// Restore from local disk (Arrow IPC file).
    /// Returns number of vectors loaded, or 0 if no file found.
    pub fn restore_from_disk(&self) -> Result<usize> {
        let dir = self.data_dir.read().unwrap();
        let dir = match dir.as_ref() {
            Some(d) => d.clone(),
            None => return Ok(0),
        };
        let path = dir.join(VECTORS_FILE);
        if !path.exists() {
            return Ok(0);
        }
        self.load_from_ipc_file(&path)
    }

    /// Restore from CAS (S3 fallback). Fetches by Blake3 hash, writes to local disk.
    pub fn restore_from_cas_sync<F>(&self, cid: &yata_core::Blake3Hash, cas_get: F) -> Result<usize>
    where
        F: Fn(&yata_core::Blake3Hash) -> std::result::Result<Option<bytes::Bytes>, String>,
    {
        let data = cas_get(cid)
            .map_err(|e| VexError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        let data = match data {
            Some(d) => d,
            None => return Ok(0),
        };

        // Write to local disk for future mmap reads
        if let Some(dir) = self.data_dir.read().unwrap().as_ref() {
            std::fs::create_dir_all(dir)?;
            let path = dir.join(VECTORS_FILE);
            std::fs::write(&path, &data)?;
        }

        // Load into store
        self.load_from_ipc_bytes(&data)
    }

    /// Try restore: disk first, then CAS fallback.
    pub fn restore_with_fallback<F>(
        &self,
        cas_cid: Option<&yata_core::Blake3Hash>,
        cas_get: F,
    ) -> Result<usize>
    where
        F: Fn(&yata_core::Blake3Hash) -> std::result::Result<Option<bytes::Bytes>, String>,
    {
        // Try disk first
        let n = self.restore_from_disk()?;
        if n > 0 {
            return Ok(n);
        }
        // Fallback to CAS (S3)
        if let Some(cid) = cas_cid {
            return self.restore_from_cas_sync(cid, cas_get);
        }
        Ok(0)
    }

    fn load_from_ipc_file(&self, path: &Path) -> Result<usize> {
        let data = std::fs::read(path)?;
        self.load_from_ipc_bytes(&data)
    }

    fn load_from_ipc_bytes(&self, data: &[u8]) -> Result<usize> {
        let batches = yata_arrow::ipc_file_to_batches(data)
            .map_err(|e| VexError::Arrow(arrow::error::ArrowError::ExternalError(Box::new(e))))?;
        if batches.is_empty() {
            return Ok(0);
        }

        // Infer dim from first batch
        let first = &batches[0];
        let emb_col = first.column_by_name("embedding").ok_or_else(|| {
            VexError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing embedding column",
            ))
        })?;
        let list = emb_col
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeListArray>()
            .ok_or_else(|| {
                VexError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "embedding not FixedSizeList",
                ))
            })?;
        let dim = list.value_length() as usize;

        let mut total = 0usize;
        let mut store = self.store.write().unwrap();
        if store.dim() != dim {
            *store = FlatVectorStore::new(dim);
        }

        for batch in &batches {
            let partial = FlatVectorStore::from_record_batch(batch, dim)?;
            for i in 0..partial.len() {
                store.push(partial.vid(i), partial.get(i));
            }
            total += partial.len();
        }

        self.dim.store(dim, Ordering::Relaxed);
        self.count.store(total, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed); // need index build
        self.disk_dirty.store(false, Ordering::Relaxed); // disk is up to date
        self.restored.store(true, Ordering::Relaxed);
        Ok(total)
    }

    // ---- Index build ----

    /// Force index rebuild (if dirty). Called automatically on search.
    pub fn ensure_index(&self) -> Result<()> {
        if !self.dirty.load(Ordering::Relaxed) {
            return Ok(());
        }
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            self.dirty.store(false, Ordering::Relaxed);
            return Ok(());
        }

        if count < BRUTE_FORCE_THRESHOLD {
            let mut idx = self.index.write().unwrap();
            *idx = None;
            self.dirty.store(false, Ordering::Relaxed);
            return Ok(());
        }

        // Build IVF_PQ
        let store = self.store.read().unwrap();
        let dim = store.dim();
        let config = IvfPqConfig {
            num_partitions: (count / 100).max(4).min(256),
            pq: PqConfig {
                num_sub_vectors: (dim / 4).max(1).min(32),
                num_codes: 256,
                max_iter: 20,
                metric: self.metric,
            },
            kmeans_max_iter: 20,
        };
        let built = IvfPqIndex::build(&store, &config)?;
        let mut idx = self.index.write().unwrap();
        *idx = Some(Box::new(built));
        self.dirty.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Brute-force search over FlatVectorStore.
    fn brute_force_search(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<&dyn Fn(u64) -> bool>,
    ) -> Result<Vec<SearchResult>> {
        let store = self.store.read().unwrap();
        if store.is_empty() {
            return Ok(Vec::new());
        }
        let mut scored: Vec<SearchResult> = (0..store.len())
            .filter(|&i| filter.map_or(true, |f| f(store.vid(i))))
            .map(|i| SearchResult {
                vid: store.vid(i),
                distance: distance::distance(query, store.get(i), self.metric),
            })
            .collect();
        scored.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        scored.truncate(k);
        Ok(scored)
    }

    pub fn vector_count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }

    pub fn is_disk_dirty(&self) -> bool {
        self.disk_dirty.load(Ordering::Relaxed)
    }

    pub fn has_ivf_index(&self) -> bool {
        self.index.read().unwrap().is_some()
    }

    pub fn is_restored(&self) -> bool {
        self.restored.load(Ordering::Relaxed)
    }
}

impl VectorIndex for LazyVectorIndex {
    fn search(&self, query: &[f32], k: usize, nprobes: usize) -> Result<Vec<SearchResult>> {
        self.search_with_filter(query, k, nprobes, &|_| true)
    }

    fn search_with_filter(
        &self,
        query: &[f32],
        k: usize,
        nprobes: usize,
        filter: &dyn Fn(u64) -> bool,
    ) -> Result<Vec<SearchResult>> {
        let dim = self.dim.load(Ordering::Relaxed);
        if dim == 0 || self.count.load(Ordering::Relaxed) == 0 {
            return Ok(Vec::new());
        }
        if query.len() != dim {
            return Err(VexError::DimensionMismatch {
                expected: dim,
                got: query.len(),
            });
        }

        // Lazy rebuild if dirty
        self.ensure_index()?;

        // Dispatch: IVF_PQ or brute-force
        let idx = self.index.read().unwrap();
        if let Some(ref index) = *idx {
            index.search_with_filter(query, k, nprobes, filter)
        } else {
            self.brute_force_search(query, k, Some(filter))
        }
    }

    fn dim(&self) -> usize {
        self.dim.load(Ordering::Relaxed)
    }

    fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    fn metric(&self) -> DistanceMetric {
        self.metric
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lazy_empty_search() {
        let idx = LazyVectorIndex::new(4, DistanceMetric::L2);
        let results = idx.search(&[0.0, 0.0, 0.0, 0.0], 5, 4).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_lazy_brute_force_under_threshold() {
        let idx = LazyVectorIndex::new(4, DistanceMetric::L2);
        for i in 0..100u64 {
            let v = [i as f32, 0.0, 0.0, 0.0];
            idx.append(i, &v).unwrap();
        }
        assert!(idx.is_dirty());
        assert!(!idx.has_ivf_index());

        let results = idx.search(&[0.0, 0.0, 0.0, 0.0], 3, 4).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].vid, 0);
        assert!(!idx.is_dirty());
        assert!(!idx.has_ivf_index());
    }

    #[test]
    fn test_lazy_append_search_append_search() {
        let idx = LazyVectorIndex::new(2, DistanceMetric::L2);
        idx.append(1, &[1.0, 0.0]).unwrap();
        idx.append(2, &[0.0, 1.0]).unwrap();

        let r1 = idx.search(&[1.0, 0.0], 1, 1).unwrap();
        assert_eq!(r1[0].vid, 1);

        idx.append(3, &[1.0, 0.01]).unwrap();
        assert!(idx.is_dirty());

        let r2 = idx.search(&[1.0, 0.0], 2, 1).unwrap();
        assert_eq!(r2.len(), 2);
        let vids: Vec<u64> = r2.iter().map(|r| r.vid).collect();
        assert!(vids.contains(&1));
        assert!(vids.contains(&3));
    }

    #[test]
    fn test_lazy_batch_append() {
        let idx = LazyVectorIndex::new(0, DistanceMetric::L2);
        let vids = [10, 20, 30];
        let vectors = [1.0f32, 0.0, 0.0, 1.0, 0.5, 0.5];
        idx.append_batch(&vids, &vectors, 2).unwrap();
        assert_eq!(idx.vector_count(), 3);
        assert_eq!(idx.dim(), 2);

        let r = idx.search(&[1.0, 0.0], 1, 1).unwrap();
        assert_eq!(r[0].vid, 10);
    }

    #[test]
    fn test_lazy_dimension_mismatch() {
        let idx = LazyVectorIndex::new(3, DistanceMetric::L2);
        idx.append(1, &[1.0, 2.0, 3.0]).unwrap();
        let err = idx.append(2, &[1.0, 2.0]);
        assert!(err.is_err());
    }

    #[test]
    fn test_lazy_with_filter() {
        let idx = LazyVectorIndex::new(2, DistanceMetric::L2);
        for i in 0..50u64 {
            idx.append(i, &[i as f32, 0.0]).unwrap();
        }
        let results = idx
            .search_with_filter(&[0.0, 0.0], 3, 4, &|vid| vid % 2 == 0)
            .unwrap();
        for r in &results {
            assert_eq!(r.vid % 2, 0, "filter violated: vid={}", r.vid);
        }
    }

    #[test]
    fn test_lazy_cosine_metric() {
        let idx = LazyVectorIndex::new(2, DistanceMetric::Cosine);
        idx.append(1, &[1.0, 0.0]).unwrap();
        idx.append(2, &[0.0, 1.0]).unwrap();
        idx.append(3, &[0.707, 0.707]).unwrap();

        let r = idx.search(&[1.0, 0.0], 1, 1).unwrap();
        assert_eq!(r[0].vid, 1);
    }

    #[test]
    fn test_persist_and_restore_disk() {
        let dir = tempfile::tempdir().unwrap();
        let idx = LazyVectorIndex::with_data_dir(3, DistanceMetric::L2, dir.path());
        idx.append(1, &[1.0, 0.0, 0.0]).unwrap();
        idx.append(2, &[0.0, 1.0, 0.0]).unwrap();
        idx.append(3, &[0.0, 0.0, 1.0]).unwrap();

        // Persist
        let path = idx.persist_to_disk().unwrap();
        assert!(path.is_some());
        assert!(!idx.is_disk_dirty());

        // New index, restore from disk
        let idx2 = LazyVectorIndex::with_data_dir(0, DistanceMetric::L2, dir.path());
        let n = idx2.restore_from_disk().unwrap();
        assert_eq!(n, 3);
        assert!(idx2.is_restored());
        assert_eq!(idx2.vector_count(), 3);
        assert_eq!(idx2.dim(), 3);

        // Search should work
        let r = idx2.search(&[1.0, 0.0, 0.0], 1, 1).unwrap();
        assert_eq!(r[0].vid, 1);
    }

    #[test]
    fn test_persist_to_cas_and_restore() {
        use std::collections::HashMap;
        use std::sync::Mutex;

        // Mock CAS
        let cas = Mutex::new(HashMap::<String, bytes::Bytes>::new());

        let idx = LazyVectorIndex::new(2, DistanceMetric::L2);
        idx.append(10, &[1.0, 0.0]).unwrap();
        idx.append(20, &[0.0, 1.0]).unwrap();

        // Persist to mock CAS
        let hash = idx
            .persist_to_cas_sync(|data| {
                let h = yata_core::Blake3Hash::of(&data);
                cas.lock().unwrap().insert(h.hex(), data);
                Ok(h)
            })
            .unwrap();
        assert!(hash.is_some());
        let cid = hash.unwrap();

        // Restore from mock CAS
        let idx2 = LazyVectorIndex::new(0, DistanceMetric::L2);
        let n = idx2
            .restore_from_cas_sync(&cid, |h| Ok(cas.lock().unwrap().get(&h.hex()).cloned()))
            .unwrap();
        assert_eq!(n, 2);

        let r = idx2.search(&[1.0, 0.0], 1, 1).unwrap();
        assert_eq!(r[0].vid, 10);
    }

    #[test]
    fn test_restore_with_fallback_disk_first() {
        let dir = tempfile::tempdir().unwrap();

        // Write data to disk
        let idx = LazyVectorIndex::with_data_dir(2, DistanceMetric::L2, dir.path());
        idx.append(1, &[1.0, 0.0]).unwrap();
        idx.persist_to_disk().unwrap();

        // Restore: should find on disk, never call CAS
        let idx2 = LazyVectorIndex::with_data_dir(0, DistanceMetric::L2, dir.path());
        let n = idx2
            .restore_with_fallback(None, |_| {
                panic!("should not call CAS when disk has data");
            })
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn test_no_persist_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let idx = LazyVectorIndex::with_data_dir(4, DistanceMetric::L2, dir.path());
        let path = idx.persist_to_disk().unwrap();
        assert!(path.is_none());
    }

    #[test]
    fn test_no_persist_when_clean() {
        let dir = tempfile::tempdir().unwrap();
        let idx = LazyVectorIndex::with_data_dir(2, DistanceMetric::L2, dir.path());
        idx.append(1, &[1.0, 0.0]).unwrap();
        idx.persist_to_disk().unwrap();

        // Second persist with no new data
        let path = idx.persist_to_disk().unwrap();
        assert!(path.is_none(), "should not persist again when clean");
    }
}
