//! MutableCSR: GraphScope Flex-inspired mutable compressed sparse row graph store.
//!
//! Design:
//! - Vertices: dense u32 VID array, label list, alive bitvec
//! - Edges: per-label CSR segments (offsets + edge_ids packed array)
//! - Properties: per-vertex HashMap (mutable) + columnar cache (read-optimized, rebuilt on commit)
//! - Columnar property cache: per-label, per-key Vec<Option<PropValue>> for cache-friendly scan
//! - Label bitmap: Vec<bool> per-label for O(V/64) bitwise scan
//! - Mutations: append-only within current version, CSR rebuilt on commit (incremental for dirty labels)

pub mod arrow_store;
pub mod graph_store_enum;
pub mod mirror;
pub mod partition;
pub mod partitioned;
pub mod vineyard;
pub use arrow_store::ArrowGraphStore;
pub use graph_store_enum::{GraphStoreEnum, MemoryBudget};
pub use mirror::{MirrorEdge, MirrorRegistry, MirrorVertex};
pub use partitioned::PartitionedGraphStore;
pub use vineyard::{
    BlobType, DiskVineyard, EdgeVineyard, FragmentManifest, GraphFragment, MmapVineyard, ObjectId,
    ObjectMeta, VineyardStore,
};

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use yata_core::{GlobalEid, GlobalVid, LocalEid, LocalVid, PartitionId};
use yata_cypher::Graph;
use yata_grin::*;

/// Orderable wrapper for PropValue (used as BTreeMap key in property indexes).
#[derive(Debug, Clone, PartialEq)]
pub struct PropValueOrd(pub PropValue);

impl Eq for PropValueOrd {}

impl Ord for PropValueOrd {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (&self.0, &other.0) {
            (PropValue::Int(a), PropValue::Int(b)) => a.cmp(b),
            (PropValue::Str(a), PropValue::Str(b)) => a.cmp(b),
            (PropValue::Bool(a), PropValue::Bool(b)) => a.cmp(b),
            (PropValue::Float(a), PropValue::Float(b)) => {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            }
            (PropValue::Null, PropValue::Null) => std::cmp::Ordering::Equal,
            // Different types: order by discriminant index
            (a, b) => discriminant_index(a).cmp(&discriminant_index(b)),
        }
    }
}

impl PartialOrd for PropValueOrd {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn discriminant_index(v: &PropValue) -> u8 {
    match v {
        PropValue::Null => 0,
        PropValue::Bool(_) => 1,
        PropValue::Int(_) => 2,
        PropValue::Float(_) => 3,
        PropValue::Str(_) => 4,
    }
}

// ── WAL (Write-Ahead Log) for graph mutations ────────────────────────

/// WAL entry for graph mutations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    AddVertex {
        vid: u32,
        global_vid: Option<GlobalVid>,
        label: String,
        props: Vec<(String, PropValue)>,
    },
    AddEdge {
        edge_id: u32,
        src: u32,
        dst: u32,
        label: String,
        props: Vec<(String, PropValue)>,
    },
    DeleteVertex {
        vid: u32,
    },
    DeleteEdge {
        edge_id: u32,
    },
    SetProperty {
        vid: u32,
        key: String,
        value: PropValue,
    },
    Commit {
        version: u64,
    },
}

/// In-memory WAL for graph mutations. Supports serialization for future file persistence.
pub struct GraphWal {
    entries: Vec<WalEntry>,
}

impl GraphWal {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn append(&mut self, entry: WalEntry) {
        self.entries.push(entry);
    }

    pub fn entries(&self) -> &[WalEntry] {
        &self.entries
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Replay WAL entries into a MutableCsrStore to recover state.
    pub fn replay(&self, store: &mut MutableCsrStore) {
        for entry in &self.entries {
            match entry {
                WalEntry::AddVertex {
                    label,
                    props,
                    global_vid,
                    ..
                } => {
                    let p: Vec<(&str, PropValue)> =
                        props.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                    if let Some(global_vid) = global_vid {
                        store.add_vertex_with_global_id(*global_vid, label, &p);
                    } else {
                        store.add_vertex(label, &p);
                    }
                }
                WalEntry::AddEdge {
                    src,
                    dst,
                    label,
                    props,
                    ..
                } => {
                    let p: Vec<(&str, PropValue)> =
                        props.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                    store.add_edge(*src, *dst, label, &p);
                }
                WalEntry::DeleteVertex { vid } => {
                    store.delete_vertex(*vid);
                }
                WalEntry::DeleteEdge { edge_id } => {
                    store.delete_edge(*edge_id);
                }
                WalEntry::SetProperty { vid, key, value } => {
                    store.set_vertex_prop(*vid, key, value.clone());
                }
                WalEntry::Commit { .. } => {
                    store.commit();
                }
            }
        }
    }

    /// Serialize WAL to bytes (JSON lines format).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for entry in &self.entries {
            let line = serde_json::to_string(entry).unwrap_or_default();
            buf.extend(line.as_bytes());
            buf.push(b'\n');
        }
        buf
    }

    /// Deserialize WAL from bytes.
    pub fn from_bytes(data: &[u8]) -> Self {
        let text = String::from_utf8_lossy(data);
        let entries: Vec<WalEntry> = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();
        Self { entries }
    }
}

impl Default for GraphWal {
    fn default() -> Self {
        Self::new()
    }
}

// ── MVCC Snapshot ────────────────────────────────────────────────────

/// Read-only snapshot of the graph at a specific version.
pub struct GraphSnapshot {
    version: u64,
    store: MutableCsrStore,
}

impl GraphSnapshot {
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn store(&self) -> &MutableCsrStore {
        &self.store
    }
}

/// Per-label columnar property cache (GraphScope Flex-inspired).
/// Dense Vec<Option<PropValue>> indexed by VID for cache-friendly sequential scan.
/// Rebuilt on commit() from vertex_props_map.
#[derive(Clone, Default)]
struct ColumnarPropCache {
    /// (label, prop_key) → dense column: column[vid] = Some(value) if vertex has this prop
    columns: HashMap<(String, String), Vec<Option<PropValue>>>,
}

impl ColumnarPropCache {
    fn new() -> Self {
        Self {
            columns: HashMap::new(),
        }
    }

    /// Scan a column with a predicate, returning matching VIDs.
    /// Sequential memory access for cache-friendly iteration.
    fn scan(&self, label: &str, key: &str, pred: &Predicate, alive: &[bool]) -> Option<Vec<u32>> {
        let col = self.columns.get(&(label.to_string(), key.to_string()))?;
        let mut result = Vec::new();
        for (vid, opt_val) in col.iter().enumerate() {
            if vid >= alive.len() || !alive[vid] {
                continue;
            }
            if let Some(val) = opt_val {
                let matches = match pred {
                    Predicate::Eq(_, target) => val == target,
                    Predicate::Neq(_, target) => val != target,
                    Predicate::Lt(_, target) => {
                        prop_cmp(val, target) == Some(std::cmp::Ordering::Less)
                    }
                    Predicate::Gt(_, target) => {
                        prop_cmp(val, target) == Some(std::cmp::Ordering::Greater)
                    }
                    Predicate::In(_, vals) => vals.contains(val),
                    Predicate::StartsWith(_, prefix) => {
                        if let PropValue::Str(s) = val {
                            s.starts_with(prefix.as_str())
                        } else {
                            false
                        }
                    }
                    Predicate::True => true,
                    _ => return None, // And/Or — fall back to HashMap path
                };
                if matches {
                    result.push(vid as u32);
                }
            }
        }
        Some(result)
    }
}

/// Per-label bitmap: dense Vec<bool> for O(V/64) scan.
#[derive(Clone, Default)]
struct LabelBitmap {
    /// label → bitmap: bitmap[vid] = true if vertex has this label and is alive
    bitmaps: HashMap<String, Vec<bool>>,
}

impl LabelBitmap {
    fn new() -> Self {
        Self {
            bitmaps: HashMap::new(),
        }
    }

    /// Get VIDs for a label using bitmap scan.
    fn scan(&self, label: &str) -> Vec<u32> {
        match self.bitmaps.get(label) {
            Some(bitmap) => bitmap
                .iter()
                .enumerate()
                .filter_map(|(vid, &alive)| if alive { Some(vid as u32) } else { None })
                .collect(),
            None => Vec::new(),
        }
    }
}

/// B-tree range index per (label, property).
/// Enables O(log N + K) range queries and O(log N + K) ORDER BY.
/// Rebuilt on commit() alongside prop_eq_index.
#[derive(Clone, Default)]
pub struct BTreeIndex {
    trees: HashMap<(String, String), std::collections::BTreeMap<PropValueOrd, Vec<u32>>>,
}

impl BTreeIndex {
    fn new() -> Self {
        Self {
            trees: HashMap::new(),
        }
    }

    /// Range scan: returns VIDs where min <= prop_value <= max.
    /// O(log N + K) where K = result size.
    pub fn range(
        &self,
        label: &str,
        prop: &str,
        min: Option<&PropValue>,
        max: Option<&PropValue>,
    ) -> Vec<u32> {
        let key = (label.to_string(), prop.to_string());
        let tree = match self.trees.get(&key) {
            Some(t) => t,
            None => return Vec::new(),
        };

        use std::ops::Bound;
        let lo = match min {
            Some(v) => Bound::Included(PropValueOrd(v.clone())),
            None => Bound::Unbounded,
        };
        let hi = match max {
            Some(v) => Bound::Included(PropValueOrd(v.clone())),
            None => Bound::Unbounded,
        };

        tree.range((lo, hi))
            .flat_map(|(_, vids)| vids.iter().copied())
            .collect()
    }

    /// Top-K with ordering. Returns up to `limit` VIDs ordered by property value.
    /// O(log N + K).
    pub fn order_by(
        &self,
        label: &str,
        prop: &str,
        ascending: bool,
        limit: usize,
    ) -> Vec<u32> {
        let key = (label.to_string(), prop.to_string());
        let tree = match self.trees.get(&key) {
            Some(t) => t,
            None => return Vec::new(),
        };

        let mut result = Vec::with_capacity(limit);
        if ascending {
            for (_, vids) in tree.iter() {
                for &vid in vids {
                    result.push(vid);
                    if result.len() >= limit {
                        return result;
                    }
                }
            }
        } else {
            for (_, vids) in tree.iter().rev() {
                for &vid in vids {
                    result.push(vid);
                    if result.len() >= limit {
                        return result;
                    }
                }
            }
        }
        result
    }

    /// Rebuild all B-tree indexes from vertex data.
    fn rebuild(
        &mut self,
        label_index: &HashMap<String, Vec<u32>>,
        vertex_props_map: &[HashMap<String, PropValue>],
        vertex_alive: &[bool],
    ) {
        // Rebuild for all registered (label, prop) keys
        let keys: Vec<(String, String)> = self.trees.keys().cloned().collect();
        for (label, prop) in keys {
            self.rebuild_single(label_index, vertex_props_map, vertex_alive, &label, &prop);
        }
    }

    fn rebuild_single(
        &mut self,
        label_index: &HashMap<String, Vec<u32>>,
        vertex_props_map: &[HashMap<String, PropValue>],
        vertex_alive: &[bool],
        label: &str,
        prop: &str,
    ) {
        let mut tree = std::collections::BTreeMap::<PropValueOrd, Vec<u32>>::new();
        if let Some(vids) = label_index.get(label) {
            for &vid in vids {
                if (vid as usize) < vertex_alive.len() && vertex_alive[vid as usize] {
                    if let Some(props) = vertex_props_map.get(vid as usize) {
                        if let Some(val) = props.get(prop) {
                            tree.entry(PropValueOrd(val.clone()))
                                .or_default()
                                .push(vid);
                        }
                    }
                }
            }
        }
        self.trees
            .insert((label.to_string(), prop.to_string()), tree);
    }

    /// Register a (label, prop) pair for B-tree indexing.
    /// Index will be built/rebuilt on next commit().
    pub fn register(&mut self, label: &str, prop: &str) {
        let key = (label.to_string(), prop.to_string());
        self.trees
            .entry(key)
            .or_insert_with(std::collections::BTreeMap::new);
    }

    /// Check if a (label, prop) pair is registered.
    pub fn is_registered(&self, label: &str, prop: &str) -> bool {
        self.trees
            .contains_key(&(label.to_string(), prop.to_string()))
    }
}

/// Per-label compressed sparse row segment.
#[derive(Clone)]
pub struct CsrSegment {
    /// offsets[vid] = start index in edge_ids array. len = max_vid + 2.
    pub offsets: Vec<u32>,
    /// Packed edge IDs grouped by source (out) or destination (in) vertex.
    pub edge_ids: Vec<u32>,
}

impl CsrSegment {
    fn neighbors(&self, vid: u32) -> &[u32] {
        let start = self.offsets.get(vid as usize).copied().unwrap_or(0) as usize;
        let end = self.offsets.get(vid as usize + 1).copied().unwrap_or(0) as usize;
        if start >= self.edge_ids.len() || end > self.edge_ids.len() || start >= end {
            return &[];
        }
        &self.edge_ids[start..end]
    }
}

/// Pending edge mutation not yet committed to CSR.
/// Fields retained for future incremental CSR rebuild.
#[allow(dead_code)]
#[derive(Clone)]
struct PendingEdge {
    src: u32,
    dst: u32,
    label: String,
    edge_id: u32,
}

// ── Global → Local ID map (M1: ID separation) ──────────────────────

/// Bidirectional map between GlobalVid and local u32 VID within a partition.
/// Enables cross-partition vertex reference while keeping CSR local.
#[derive(Clone, Default)]
pub struct GlobalToLocalMap {
    /// GlobalVid → local u32 VID
    g2l: HashMap<u64, u32>,
    /// local u32 VID → GlobalVid (dense: indexed by local VID)
    l2g: Vec<u64>,
}

impl GlobalToLocalMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a mapping. Returns the local VID.
    pub fn insert(&mut self, global: yata_core::GlobalVid, local: u32) {
        self.g2l.insert(global.0, local);
        while self.l2g.len() <= local as usize {
            self.l2g.push(0);
        }
        self.l2g[local as usize] = global.0;
    }

    /// Lookup local VID from global.
    pub fn to_local(&self, global: yata_core::GlobalVid) -> Option<u32> {
        self.g2l.get(&global.0).copied()
    }

    /// Lookup global VID from local.
    pub fn to_global(&self, local: u32) -> Option<yata_core::GlobalVid> {
        self.l2g
            .get(local as usize)
            .map(|&v| yata_core::GlobalVid(v))
    }

    /// Number of mapped vertices.
    pub fn len(&self) -> usize {
        self.g2l.len()
    }

    pub fn is_empty(&self) -> bool {
        self.g2l.is_empty()
    }

    /// Clear all mappings.
    pub fn clear(&mut self) {
        self.g2l.clear();
        self.l2g.clear();
    }
}

/// MutableCSR graph store implementing all GRIN traits.
pub struct MutableCsrStore {
    // Vertex storage
    vertex_labels: Vec<Vec<String>>,
    vertex_alive: Vec<bool>,
    vertex_props_map: Vec<HashMap<String, PropValue>>,
    vertex_global_ids: Vec<Option<GlobalVid>>,
    vertex_count: u32,
    vid_alloc: AtomicU32,
    global_to_local_vid: HashMap<GlobalVid, LocalVid>,

    // Edge storage: label -> CsrSegment
    out_csr: HashMap<String, CsrSegment>,
    in_csr: HashMap<String, CsrSegment>,

    // Edge metadata indexed by edge_id
    edge_props: Vec<HashMap<String, PropValue>>,
    edge_labels: Vec<String>,
    edge_src: Vec<u32>,
    edge_dst: Vec<u32>,
    edge_global_ids: Vec<Option<GlobalEid>>,
    edge_alive: Vec<bool>,
    edge_count: u32,
    global_to_local_eid: HashMap<GlobalEid, LocalEid>,

    // Pending mutations (not yet in CSR)
    pending_edges: Vec<PendingEdge>,

    // Version
    version: AtomicU64,

    // Schema
    known_vertex_labels: Vec<String>,
    known_edge_labels: Vec<String>,
    vertex_pk: HashMap<String, String>,

    // Partition
    partition_id: PartitionId,

    // WAL (optional, enabled via enable_wal())
    wal: Option<GraphWal>,

    /// Label bitmap index: label -> list of alive VIDs with that label.
    /// Rebuilt on commit(). O(popcount) scan instead of O(V).
    label_index: HashMap<String, Vec<u32>>,

    /// Property equality index: (label, prop_key) -> HashMap<debug_value_string, Vec<u32>>
    /// Enables O(1) lookup for Eq predicates. Rebuilt on commit().
    prop_eq_index: HashMap<(String, String), HashMap<String, Vec<u32>>>,

    /// Dirty tracking: vertex labels touched since last drain.
    dirty_vertex_labels: HashSet<String>,
    /// Dirty tracking: edge labels touched since last drain.
    dirty_edge_labels: HashSet<String>,

    /// Columnar property cache: per-(label, key) dense Vec for cache-friendly scan.
    /// Rebuilt on commit(). Read path uses this for predicate evaluation.
    columnar_cache: ColumnarPropCache,
    /// Label bitmap: per-label dense Vec<bool> for O(V/64) scan.
    /// Rebuilt on commit(). Replaces label_index for count/scan operations.
    label_bitmap: LabelBitmap,

    /// B-tree range index: (label, prop_key) → BTreeMap<PropValueOrd, Vec<u32>>.
    /// Enables O(log N + K) range queries and ORDER BY. Rebuilt on commit().
    btree_index: BTreeIndex,

    /// Global→Local vertex ID map (M1: ID separation).
    /// Optional: only populated when `add_vertex_with_global_id()` is used.
    global_map: GlobalToLocalMap,
}

impl MutableCsrStore {
    /// Create an empty store (partition 0).
    pub fn new() -> Self {
        Self::new_with_partition_id(PartitionId::from(0))
    }

    /// Create an empty store for a specific partition.
    pub fn new_partition(partition_id: u32) -> Self {
        Self::new_with_partition_id(PartitionId::from(partition_id))
    }

    /// Create an empty store for a specific typed partition.
    pub fn new_with_partition_id(partition_id: PartitionId) -> Self {
        Self {
            vertex_labels: Vec::new(),
            vertex_alive: Vec::new(),
            vertex_props_map: Vec::new(),
            vertex_global_ids: Vec::new(),
            vertex_count: 0,
            vid_alloc: AtomicU32::new(0),
            global_to_local_vid: HashMap::new(),
            out_csr: HashMap::new(),
            in_csr: HashMap::new(),
            edge_props: Vec::new(),
            edge_labels: Vec::new(),
            edge_src: Vec::new(),
            edge_dst: Vec::new(),
            edge_global_ids: Vec::new(),
            edge_alive: Vec::new(),
            edge_count: 0,
            global_to_local_eid: HashMap::new(),
            pending_edges: Vec::new(),
            version: AtomicU64::new(0),
            known_vertex_labels: Vec::new(),
            known_edge_labels: Vec::new(),
            vertex_pk: HashMap::new(),
            partition_id,
            wal: None,
            label_index: HashMap::new(),
            prop_eq_index: HashMap::new(),
            dirty_vertex_labels: HashSet::new(),
            dirty_edge_labels: HashSet::new(),
            columnar_cache: ColumnarPropCache::new(),
            label_bitmap: LabelBitmap::new(),
            btree_index: BTreeIndex::new(),
            global_map: GlobalToLocalMap::new(),
        }
    }

    /// Drain dirty vertex labels since last drain. Returns labels that were mutated.
    pub fn drain_dirty_vertex_labels(&mut self) -> Vec<String> {
        self.dirty_vertex_labels.drain().collect()
    }

    /// Drain dirty edge labels since last drain. Returns labels that were mutated.
    pub fn drain_dirty_edge_labels(&mut self) -> Vec<String> {
        self.dirty_edge_labels.drain().collect()
    }

    /// Set the primary key column for a vertex label.
    pub fn set_vertex_primary_key(&mut self, label: &str, key: &str) {
        self.vertex_pk.insert(label.to_string(), key.to_string());
    }

    /// MERGE by primary key: O(1) lookup via prop_eq_index.
    /// If vertex with matching (label, pk_key=pk_value) exists → update props, return vid.
    /// If not → create vertex with label + props, return vid.
    /// Requires commit() to rebuild indexes after mutation.
    ///
    /// GraphScope Groot parity: get_vertex_by_primary_key → O(1) upsert.
    pub fn merge_by_pk(
        &mut self,
        label: &str,
        pk_key: &str,
        pk_value: &PropValue,
        props: &[(&str, PropValue)],
    ) -> u32 {
        // O(1) lookup via prop_eq_index
        let lookup_key = format!("{:?}", pk_value);
        let existing_vid = self
            .prop_eq_index
            .get(&(label.to_string(), pk_key.to_string()))
            .and_then(|idx| idx.get(&lookup_key))
            .and_then(|vids| vids.first().copied());

        if let Some(vid) = existing_vid {
            // UPDATE: set properties on existing vertex
            if let Some(pmap) = self.vertex_props_map.get_mut(vid as usize) {
                for (k, v) in props {
                    pmap.insert(k.to_string(), v.clone());
                }
            }
            self.dirty_vertex_labels.insert(label.to_string());
            vid
        } else {
            // CREATE: add new vertex
            let mut all_props: Vec<(&str, PropValue)> = vec![(pk_key, pk_value.clone())];
            all_props.extend(props.iter().map(|(k, v)| (*k, v.clone())));
            let vid = self.add_vertex_with_labels(&[label.to_string()], &all_props);
            vid
        }
    }

    /// Delete vertex by primary key: O(1) lookup via prop_eq_index.
    /// Returns true if vertex was found and deleted.
    pub fn delete_by_pk(&mut self, label: &str, pk_key: &str, pk_value: &PropValue) -> bool {
        let lookup_key = format!("{:?}", pk_value);
        let vid = self
            .prop_eq_index
            .get(&(label.to_string(), pk_key.to_string()))
            .and_then(|idx| idx.get(&lookup_key))
            .and_then(|vids| vids.first().copied());

        if let Some(vid) = vid {
            if let Some(alive) = self.vertex_alive.get_mut(vid as usize) {
                *alive = false;
                self.vertex_count = self.vertex_count.saturating_sub(1);
                self.dirty_vertex_labels.insert(label.to_string());
                return true;
            }
        }
        false
    }

    /// Current snapshot version.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    // ── Snapshot accessors (GraphScope Flex-style persistence) ──

    /// Borrow vertex data for snapshot serialization.
    pub fn vertex_labels_raw(&self) -> &[Vec<String>] {
        &self.vertex_labels
    }
    pub fn vertex_alive_raw(&self) -> &[bool] {
        &self.vertex_alive
    }
    pub fn vertex_props_raw(&self) -> &[HashMap<String, PropValue>] {
        &self.vertex_props_map
    }
    pub fn vertex_global_ids_raw(&self) -> &[Option<GlobalVid>] {
        &self.vertex_global_ids
    }
    pub fn vertex_count_raw(&self) -> u32 {
        self.vertex_count
    }

    /// Borrow edge data for snapshot serialization.
    pub fn edge_labels_raw(&self) -> &[String] {
        &self.edge_labels
    }
    pub fn edge_src_raw(&self) -> &[u32] {
        &self.edge_src
    }
    pub fn edge_dst_raw(&self) -> &[u32] {
        &self.edge_dst
    }
    pub fn edge_global_ids_raw(&self) -> &[Option<GlobalEid>] {
        &self.edge_global_ids
    }
    pub fn edge_alive_raw(&self) -> &[bool] {
        &self.edge_alive
    }
    pub fn edge_props_raw(&self) -> &[HashMap<String, PropValue>] {
        &self.edge_props
    }
    pub fn edge_count_raw(&self) -> u32 {
        self.edge_count
    }

    /// Get edge label by edge ID.
    pub fn edge_label(&self, eid: u32) -> Option<&str> {
        self.edge_labels.get(eid as usize).map(|s| s.as_str())
    }

    /// Check if edge is alive.
    pub fn edge_alive(&self, eid: u32) -> bool {
        self.edge_alive.get(eid as usize).copied().unwrap_or(false)
    }

    /// Get edge source vertex ID.
    pub fn edge_src(&self, eid: u32) -> Option<u32> {
        self.edge_src.get(eid as usize).copied()
    }

    /// Get edge destination vertex ID.
    pub fn edge_dst(&self, eid: u32) -> Option<u32> {
        self.edge_dst.get(eid as usize).copied()
    }

    /// Borrow CSR topology for snapshot serialization (GraphScope .adj equivalent).
    pub fn out_csr_raw(&self) -> &HashMap<String, CsrSegment> {
        &self.out_csr
    }
    pub fn in_csr_raw(&self) -> &HashMap<String, CsrSegment> {
        &self.in_csr
    }

    /// Known schema labels.
    pub fn known_vertex_labels_raw(&self) -> &[String] {
        &self.known_vertex_labels
    }
    pub fn known_edge_labels_raw(&self) -> &[String] {
        &self.known_edge_labels
    }
    pub fn vertex_pk_raw(&self) -> &HashMap<String, String> {
        &self.vertex_pk
    }
    pub fn partition_id_raw(&self) -> PartitionId {
        self.partition_id
    }

    fn allocate_vertex_slot(&mut self) -> (u32, usize) {
        let vid = self.vid_alloc.fetch_add(1, Ordering::Relaxed);
        let vid_usize = vid as usize;
        while self.vertex_labels.len() <= vid_usize {
            self.vertex_labels.push(Vec::new());
            self.vertex_alive.push(false);
            self.vertex_props_map.push(HashMap::new());
            self.vertex_global_ids.push(None);
        }
        (vid, vid_usize)
    }

    fn store_vertex_record(
        &mut self,
        vid: u32,
        vid_usize: usize,
        labels: &[String],
        props: &[(&str, PropValue)],
        global_vid: Option<GlobalVid>,
    ) {
        self.vertex_labels[vid_usize] = labels.to_vec();
        self.vertex_alive[vid_usize] = true;
        let mut prop_map = HashMap::new();
        for &(k, ref v) in props {
            prop_map.insert(k.to_string(), v.clone());
        }
        self.vertex_props_map[vid_usize] = prop_map;
        self.vertex_global_ids[vid_usize] = global_vid;
        if let Some(gid) = global_vid {
            self.global_to_local_vid.insert(gid, LocalVid::from(vid));
            self.global_map.insert(gid, vid);
        }
        self.vertex_count = self.vid_alloc.load(Ordering::Relaxed);
    }

    pub fn local_vid(&self, global_vid: GlobalVid) -> Option<LocalVid> {
        self.global_to_local_vid.get(&global_vid).copied()
    }

    pub fn global_vid(&self, local_vid: u32) -> Option<GlobalVid> {
        self.vertex_global_ids
            .get(local_vid as usize)
            .copied()
            .flatten()
    }

    pub fn local_eid(&self, global_eid: GlobalEid) -> Option<LocalEid> {
        self.global_to_local_eid.get(&global_eid).copied()
    }

    pub fn global_eid(&self, local_eid: u32) -> Option<GlobalEid> {
        self.edge_global_ids
            .get(local_eid as usize)
            .copied()
            .flatten()
    }

    pub fn add_vertex_with_global_id(
        &mut self,
        global_vid: GlobalVid,
        label: &str,
        props: &[(&str, PropValue)],
    ) -> u32 {
        self.add_vertex_with_labels_and_optional_global_id(
            Some(global_vid),
            &[label.to_string()],
            props,
        )
    }

    pub fn add_vertex_with_optional_global_id(
        &mut self,
        global_vid: Option<GlobalVid>,
        label: &str,
        props: &[(&str, PropValue)],
    ) -> u32 {
        self.add_vertex_with_labels_and_optional_global_id(global_vid, &[label.to_string()], props)
    }

    pub fn add_vertex_with_labels_and_global_id(
        &mut self,
        global_vid: GlobalVid,
        labels: &[String],
        props: &[(&str, PropValue)],
    ) -> u32 {
        self.add_vertex_with_labels_and_optional_global_id(Some(global_vid), labels, props)
    }

    pub fn add_vertex_with_labels_and_optional_global_id(
        &mut self,
        global_vid: Option<GlobalVid>,
        labels: &[String],
        props: &[(&str, PropValue)],
    ) -> u32 {
        if let Some(global_vid) = global_vid {
            if let Some(existing) = self.local_vid(global_vid) {
                return existing.get();
            }
        }
        let (vid, vid_usize) = self.allocate_vertex_slot();
        self.store_vertex_record(vid, vid_usize, labels, props, global_vid);
        for label in labels {
            self.register_vertex_label(label);
            self.dirty_vertex_labels.insert(label.clone());
        }
        if let Some(ref mut wal) = self.wal {
            wal.append(WalEntry::AddVertex {
                vid,
                global_vid,
                label: labels.first().cloned().unwrap_or_default(),
                props: props
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect(),
            });
        }
        vid
    }

    pub fn add_edge_with_global_id(
        &mut self,
        global_eid: GlobalEid,
        src: u32,
        dst: u32,
        label: &str,
        props: &[(&str, PropValue)],
    ) -> u32 {
        self.add_edge_with_optional_global_id(Some(global_eid), src, dst, label, props)
    }

    pub fn add_edge_with_optional_global_id(
        &mut self,
        global_eid: Option<GlobalEid>,
        src: u32,
        dst: u32,
        label: &str,
        props: &[(&str, PropValue)],
    ) -> u32 {
        if let Some(global_eid) = global_eid {
            if let Some(existing) = self.local_eid(global_eid) {
                return existing.get();
            }
        }
        let eid = self.edge_count;
        self.edge_count += 1;

        let mut prop_map = HashMap::new();
        for &(k, ref v) in props {
            prop_map.insert(k.to_string(), v.clone());
        }

        self.edge_props.push(prop_map);
        self.edge_labels.push(label.to_string());
        self.edge_src.push(src);
        self.edge_dst.push(dst);
        self.edge_global_ids.push(global_eid);
        self.edge_alive.push(true);
        if let Some(global_eid) = global_eid {
            self.global_to_local_eid
                .insert(global_eid, LocalEid::from(eid));
        }

        self.pending_edges.push(PendingEdge {
            src,
            dst,
            label: label.to_string(),
            edge_id: eid,
        });

        self.register_edge_label(label);
        self.dirty_edge_labels.insert(label.to_string());
        eid
    }

    /// Rebuild CSR segments — incremental if only specific labels are dirty.
    fn rebuild_csr(&mut self) {
        let dirty = &self.dirty_edge_labels;
        let max_vid = self.vertex_count;
        if max_vid == 0 {
            self.out_csr.clear();
            self.in_csr.clear();
            self.pending_edges.clear();
            return;
        }

        // Incremental: only rebuild dirty label segments (or all if no tracking)
        let rebuild_all = dirty.is_empty() && !self.pending_edges.is_empty();

        // Collect edges for labels that need rebuild
        let mut edges_by_label: HashMap<String, Vec<(u32, u32, u32)>> = HashMap::new();
        for eid in 0..self.edge_count as usize {
            if eid >= self.edge_labels.len() {
                continue;
            }
            if !self.edge_alive[eid] {
                continue;
            }
            let label = &self.edge_labels[eid];
            if rebuild_all || dirty.contains(label) {
                edges_by_label.entry(label.clone()).or_default().push((
                    self.edge_src[eid],
                    self.edge_dst[eid],
                    eid as u32,
                ));
            }
        }

        // Remove dirty labels that now have zero edges
        if !rebuild_all {
            for label in dirty.iter() {
                if !edges_by_label.contains_key(label) {
                    self.out_csr.remove(label);
                    self.in_csr.remove(label);
                }
            }
        } else {
            self.out_csr.clear();
            self.in_csr.clear();
        }

        // Build CSR for each label
        for (label, edges) in &edges_by_label {
            self.build_csr_segment(label, edges, max_vid);
        }

        self.pending_edges.clear();
    }

    /// Build a single label's outgoing + incoming CSR segments via prefix-sum.
    fn build_csr_segment(&mut self, label: &str, edges: &[(u32, u32, u32)], max_vid: u32) {
        let n = max_vid as usize + 2;

        // Outgoing CSR
        let mut out_offsets = vec![0u32; n];
        for &(src, _, _) in edges {
            if (src as usize + 1) < n {
                out_offsets[src as usize + 1] += 1;
            }
        }
        for i in 1..n {
            out_offsets[i] += out_offsets[i - 1];
        }
        let mut out_edge_ids = vec![0u32; edges.len()];
        let mut cursor = out_offsets.clone();
        for &(src, _, eid) in edges {
            let pos = cursor[src as usize] as usize;
            if pos < out_edge_ids.len() {
                out_edge_ids[pos] = eid;
                cursor[src as usize] += 1;
            }
        }
        self.out_csr.insert(
            label.to_string(),
            CsrSegment {
                offsets: out_offsets,
                edge_ids: out_edge_ids,
            },
        );

        // Incoming CSR
        let mut in_offsets = vec![0u32; n];
        for &(_, dst, _) in edges {
            if (dst as usize + 1) < n {
                in_offsets[dst as usize + 1] += 1;
            }
        }
        for i in 1..n {
            in_offsets[i] += in_offsets[i - 1];
        }
        let mut in_edge_ids = vec![0u32; edges.len()];
        let mut cursor = in_offsets.clone();
        for &(_, dst, eid) in edges {
            let pos = cursor[dst as usize] as usize;
            if pos < in_edge_ids.len() {
                in_edge_ids[pos] = eid;
                cursor[dst as usize] += 1;
            }
        }
        self.in_csr.insert(
            label.to_string(),
            CsrSegment {
                offsets: in_offsets,
                edge_ids: in_edge_ids,
            },
        );
    }

    /// Check if a predicate matches a vertex's properties.
    fn predicate_matches(&self, vid: u32, predicate: &Predicate) -> bool {
        match predicate {
            Predicate::True => true,
            Predicate::Eq(key, val) => self
                .vertex_prop_value(vid, key)
                .map_or(false, |v| &v == val),
            Predicate::Neq(key, val) => {
                self.vertex_prop_value(vid, key).map_or(true, |v| &v != val)
            }
            Predicate::Lt(key, val) => self.vertex_prop_value(vid, key).map_or(false, |v| {
                prop_cmp(&v, val) == Some(std::cmp::Ordering::Less)
            }),
            Predicate::Gt(key, val) => self.vertex_prop_value(vid, key).map_or(false, |v| {
                prop_cmp(&v, val) == Some(std::cmp::Ordering::Greater)
            }),
            Predicate::In(key, vals) => self
                .vertex_prop_value(vid, key)
                .map_or(false, |v| vals.contains(&v)),
            Predicate::StartsWith(key, prefix) => {
                self.vertex_prop_value(vid, key).map_or(false, |v| {
                    if let PropValue::Str(s) = &v {
                        s.starts_with(prefix.as_str())
                    } else {
                        false
                    }
                })
            }
            Predicate::And(a, b) => {
                self.predicate_matches(vid, a) && self.predicate_matches(vid, b)
            }
            Predicate::Or(a, b) => self.predicate_matches(vid, a) || self.predicate_matches(vid, b),
        }
    }

    /// Get a vertex property value.
    fn vertex_prop_value(&self, vid: u32, key: &str) -> Option<PropValue> {
        self.vertex_props_map
            .get(vid as usize)
            .and_then(|m| m.get(key).cloned())
    }

    fn register_vertex_label(&mut self, label: &str) {
        if !self.known_vertex_labels.contains(&label.to_string()) {
            self.known_vertex_labels.push(label.to_string());
        }
    }

    fn register_edge_label(&mut self, label: &str) {
        if !self.known_edge_labels.contains(&label.to_string()) {
            self.known_edge_labels.push(label.to_string());
        }
    }

    // ── Index methods ───────────────────────────────────────────────

    /// Rebuild label index — incremental for dirty labels only.
    fn rebuild_label_index(&mut self) {
        let dirty = &self.dirty_vertex_labels;
        if dirty.is_empty() {
            // Full rebuild (first commit or no dirty tracking)
            self.label_index.clear();
            for vid in 0..self.vertex_count {
                if !self.vertex_alive[vid as usize] {
                    continue;
                }
                for label in &self.vertex_labels[vid as usize] {
                    self.label_index.entry(label.clone()).or_default().push(vid);
                }
            }
        } else {
            // Incremental: only rebuild dirty label entries
            for label in dirty.iter() {
                let mut vids = Vec::new();
                for vid in 0..self.vertex_count {
                    if !self.vertex_alive[vid as usize] {
                        continue;
                    }
                    if self.vertex_labels[vid as usize].contains(label) {
                        vids.push(vid);
                    }
                }
                if vids.is_empty() {
                    self.label_index.remove(label);
                } else {
                    self.label_index.insert(label.clone(), vids);
                }
            }
        }
    }

    /// Rebuild all registered property equality indexes.
    fn rebuild_prop_indexes(&mut self) {
        let keys: Vec<(String, String)> = self.prop_eq_index.keys().cloned().collect();
        for (label, prop) in keys {
            self.rebuild_single_prop_index(&label, &prop);
        }
    }

    /// Rebuild a single property equality index.
    fn rebuild_single_prop_index(&mut self, label: &str, prop_key: &str) {
        let mut idx: HashMap<String, Vec<u32>> = HashMap::new();
        let vids = self.label_index.get(label).cloned().unwrap_or_default();
        for vid in vids {
            if let Some(val) = self.vertex_prop_value(vid, prop_key) {
                let key = format!("{:?}", val);
                idx.entry(key).or_default().push(vid);
            }
        }
        self.prop_eq_index
            .insert((label.to_string(), prop_key.to_string()), idx);
    }

    /// Rebuild B-tree range indexes for all registered (label, prop) pairs.
    fn rebuild_btree_indexes(&mut self) {
        self.btree_index.rebuild(
            &self.label_index,
            &self.vertex_props_map,
            &self.vertex_alive,
        );
    }

    /// Register a (label, prop) pair for B-tree range indexing.
    /// Index is built on next commit().
    pub fn register_btree_index(&mut self, label: &str, prop: &str) {
        self.btree_index.register(label, prop);
    }

    /// B-tree range scan: returns VIDs where min <= prop_value <= max.
    pub fn btree_range(
        &self,
        label: &str,
        prop: &str,
        min: Option<&PropValue>,
        max: Option<&PropValue>,
    ) -> Vec<u32> {
        self.btree_index.range(label, prop, min, max)
    }

    /// B-tree ORDER BY: returns up to `limit` VIDs ordered by property value.
    pub fn btree_order_by(
        &self,
        label: &str,
        prop: &str,
        ascending: bool,
        limit: usize,
    ) -> Vec<u32> {
        self.btree_index.order_by(label, prop, ascending, limit)
    }

    /// Access the B-tree index directly.
    pub fn btree_index(&self) -> &BTreeIndex {
        &self.btree_index
    }

    /// Rebuild columnar property cache for all labels.
    /// Creates dense Vec<Option<PropValue>> per (label, prop_key) for cache-friendly scan.
    fn rebuild_columnar_cache(&mut self) {
        self.columnar_cache.columns.clear();
        let n = self.vertex_count as usize;

        for label in &self.known_vertex_labels {
            // Collect all prop keys for this label
            let mut all_keys: HashSet<String> = HashSet::new();
            if let Some(vids) = self.label_index.get(label) {
                for &vid in vids {
                    if let Some(props) = self.vertex_props_map.get(vid as usize) {
                        for k in props.keys() {
                            all_keys.insert(k.clone());
                        }
                    }
                }
            }

            // Build dense column per key
            for key in all_keys {
                let mut col = vec![None; n];
                if let Some(vids) = self.label_index.get(label) {
                    for &vid in vids {
                        if let Some(props) = self.vertex_props_map.get(vid as usize) {
                            if let Some(val) = props.get(&key) {
                                col[vid as usize] = Some(val.clone());
                            }
                        }
                    }
                }
                self.columnar_cache
                    .columns
                    .insert((label.clone(), key), col);
            }
        }
    }

    /// Rebuild label bitmaps for all labels.
    fn rebuild_label_bitmap(&mut self) {
        self.label_bitmap.bitmaps.clear();
        let n = self.vertex_count as usize;
        for label in &self.known_vertex_labels {
            let mut bitmap = vec![false; n];
            if let Some(vids) = self.label_index.get(label) {
                for &vid in vids {
                    bitmap[vid as usize] = true;
                }
            }
            self.label_bitmap.bitmaps.insert(label.clone(), bitmap);
        }
    }

    /// Create an equality index on a (label, property) pair for faster Eq lookups.
    /// The index is rebuilt automatically on each commit().
    pub fn create_index(&mut self, label: &str, prop_key: &str) {
        // Insert empty entry to register; rebuild immediately if label_index exists
        self.prop_eq_index
            .entry((label.to_string(), prop_key.to_string()))
            .or_default();
        // If label_index is populated, rebuild now
        if !self.label_index.is_empty() {
            self.rebuild_single_prop_index(label, prop_key);
        }
    }

    // ── WAL methods ──────────────────────────────────────────────────

    /// Enable WAL tracking. After this, all mutations are recorded.
    pub fn enable_wal(&mut self) {
        self.wal = Some(GraphWal::new());
    }

    /// Get WAL reference (if enabled).
    pub fn wal(&self) -> Option<&GraphWal> {
        self.wal.as_ref()
    }

    /// Take WAL (for persistence), replacing with a fresh empty WAL.
    pub fn take_wal(&mut self) -> Option<GraphWal> {
        self.wal.take().map(|w| {
            self.wal = Some(GraphWal::new());
            w
        })
    }

    // ── MVCC snapshot ────────────────────────────────────────────────

    /// Create a read-only snapshot at the current version.
    pub fn snapshot(&self) -> GraphSnapshot {
        GraphSnapshot {
            version: self.version.load(Ordering::Relaxed),
            store: self.clone(),
        }
    }
}

/// Manual Clone impl for MutableCsrStore (atomics are copied by value).
impl Clone for MutableCsrStore {
    fn clone(&self) -> Self {
        Self {
            vertex_labels: self.vertex_labels.clone(),
            vertex_alive: self.vertex_alive.clone(),
            vertex_props_map: self.vertex_props_map.clone(),
            vertex_global_ids: self.vertex_global_ids.clone(),
            vertex_count: self.vertex_count,
            vid_alloc: AtomicU32::new(self.vid_alloc.load(Ordering::Relaxed)),
            global_to_local_vid: self.global_to_local_vid.clone(),
            out_csr: self.out_csr.clone(),
            in_csr: self.in_csr.clone(),
            edge_props: self.edge_props.clone(),
            edge_labels: self.edge_labels.clone(),
            edge_src: self.edge_src.clone(),
            edge_dst: self.edge_dst.clone(),
            edge_global_ids: self.edge_global_ids.clone(),
            edge_alive: self.edge_alive.clone(),
            edge_count: self.edge_count,
            global_to_local_eid: self.global_to_local_eid.clone(),
            pending_edges: self.pending_edges.clone(),
            version: AtomicU64::new(self.version.load(Ordering::Relaxed)),
            known_vertex_labels: self.known_vertex_labels.clone(),
            known_edge_labels: self.known_edge_labels.clone(),
            vertex_pk: self.vertex_pk.clone(),
            partition_id: self.partition_id,
            wal: None, // WAL is not cloned into snapshots
            label_index: self.label_index.clone(),
            prop_eq_index: self.prop_eq_index.clone(),
            dirty_vertex_labels: HashSet::new(),
            dirty_edge_labels: HashSet::new(),
            columnar_cache: self.columnar_cache.clone(),
            label_bitmap: self.label_bitmap.clone(),
            btree_index: self.btree_index.clone(),
            global_map: self.global_map.clone(),
        }
    }
}

// ── Global ID compatibility API ───────────────────────────────────────

impl MutableCsrStore {
    /// Lookup local VID from a global vertex ID.
    pub fn lookup_local_vid(&self, global: yata_core::GlobalVid) -> Option<u32> {
        self.global_map.to_local(global)
    }

    /// Lookup global VID from a local vertex ID.
    pub fn lookup_global_vid(&self, local: u32) -> Option<yata_core::GlobalVid> {
        self.global_map.to_global(local)
    }

    /// Borrow the global→local map.
    pub fn global_map(&self) -> &GlobalToLocalMap {
        &self.global_map
    }

    /// Mutable borrow of the global→local map (for bulk import).
    pub fn global_map_mut(&mut self) -> &mut GlobalToLocalMap {
        &mut self.global_map
    }
}

// ── Cypher bridge: MutableCsrStore → MemoryGraph ─────────────────────

impl MutableCsrStore {
    /// Add a vertex with multiple labels.
    pub fn add_vertex_with_labels(
        &mut self,
        labels: &[String],
        props: &[(&str, PropValue)],
    ) -> u32 {
        let (vid, vid_usize) = self.allocate_vertex_slot();
        self.store_vertex_record(vid, vid_usize, labels, props, None);
        for l in labels {
            self.register_vertex_label(l);
            self.dirty_vertex_labels.insert(l.clone());
        }
        vid
    }

    /// Soft-delete all vertices with the given label (for LRU eviction).
    pub fn remove_vertices_by_label(&mut self, label: &str) {
        let vids: Vec<u32> = self
            .label_index
            .get(label)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        for vid in &vids {
            if let Some(alive) = self.vertex_alive.get_mut(*vid as usize) {
                *alive = false;
            }
        }
        self.label_index.remove(label);
    }

    /// Soft-delete all edges with the given relationship type (for LRU eviction).
    pub fn remove_edges_by_label(&mut self, rel_type: &str) {
        for eid in 0..self.edge_alive.len() {
            if self.edge_alive[eid] && self.edge_labels[eid] == rel_type {
                self.edge_alive[eid] = false;
            }
        }
    }

    /// Build a MemoryGraph from ALL data in the CSR store (no disk I/O).
    pub fn to_full_memory_graph(&self) -> yata_cypher::MemoryGraph {
        let mut g = yata_cypher::MemoryGraph::new();
        // Add all alive vertices
        for vid in 0..self.vertex_alive.len() {
            if !self.vertex_alive[vid] {
                continue;
            }
            let labels = self.vertex_labels.get(vid).cloned().unwrap_or_default();
            let props = self
                .vertex_props_map
                .get(vid)
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| (k.clone(), prop_to_cypher(v)))
                        .collect()
                })
                .unwrap_or_default();
            // Use vid as string ID for round-trip fidelity
            let id = self.vertex_string_id(vid as u32);
            g.add_node(yata_cypher::NodeRef { id, labels, props });
        }
        // Add all alive edges
        for eid in 0..self.edge_alive.len() {
            if !self.edge_alive[eid] {
                continue;
            }
            let src = self.vertex_string_id(self.edge_src[eid]);
            let dst = self.vertex_string_id(self.edge_dst[eid]);
            let rel_type = self.edge_labels[eid].clone();
            let props = self
                .edge_props
                .get(eid)
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| (k.clone(), prop_to_cypher(v)))
                        .collect()
                })
                .unwrap_or_default();
            let id = format!("e{}", eid);
            g.add_rel(yata_cypher::RelRef {
                id,
                rel_type,
                src,
                dst,
                props,
            });
        }
        g.build_csr();
        g
    }

    /// Build a MemoryGraph from a filtered subset of the CSR store.
    /// Only includes vertices matching `labels` and their incident edges
    /// (optionally filtered by `rel_types`), plus 1-hop neighbor vertices.
    pub fn to_filtered_memory_graph(
        &self,
        labels: &[String],
        rel_types: &[String],
    ) -> yata_cypher::MemoryGraph {
        let mut g = yata_cypher::MemoryGraph::new();

        // Step 1: Collect matching vertex IDs
        let mut seed_vids: Vec<u32> = Vec::new();
        for vid in 0..self.vertex_alive.len() {
            if !self.vertex_alive[vid] {
                continue;
            }
            let vlabels = match self.vertex_labels.get(vid) {
                Some(l) => l,
                None => continue,
            };
            if labels.is_empty() || labels.iter().any(|l| vlabels.contains(l)) {
                seed_vids.push(vid as u32);
            }
        }

        // Step 2: Add seed vertices to graph
        let mut added_vids = std::collections::HashSet::new();
        for &vid in &seed_vids {
            let id = self.vertex_string_id(vid);
            let vlabels = self
                .vertex_labels
                .get(vid as usize)
                .cloned()
                .unwrap_or_default();
            let props = self
                .vertex_props_map
                .get(vid as usize)
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| (k.clone(), prop_to_cypher(v)))
                        .collect()
                })
                .unwrap_or_default();
            g.add_node(yata_cypher::NodeRef {
                id,
                labels: vlabels,
                props,
            });
            added_vids.insert(vid);
        }

        // Step 3: Collect edges incident to seed vertices, optionally filtered by rel_types
        let mut neighbor_vids = std::collections::HashSet::new();
        for eid in 0..self.edge_alive.len() {
            if !self.edge_alive[eid] {
                continue;
            }
            let src = self.edge_src[eid];
            let dst = self.edge_dst[eid];
            let touches_seed = added_vids.contains(&src) || added_vids.contains(&dst);
            if !touches_seed {
                continue;
            }
            if !rel_types.is_empty() && !rel_types.contains(&self.edge_labels[eid]) {
                continue;
            }

            let src_id = self.vertex_string_id(src);
            let dst_id = self.vertex_string_id(dst);
            let rel_type = self.edge_labels[eid].clone();
            let props = self
                .edge_props
                .get(eid)
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| (k.clone(), prop_to_cypher(v)))
                        .collect()
                })
                .unwrap_or_default();
            g.add_rel(yata_cypher::RelRef {
                id: format!("e{}", eid),
                rel_type,
                src: src_id,
                dst: dst_id,
                props,
            });
            if !added_vids.contains(&src) {
                neighbor_vids.insert(src);
            }
            if !added_vids.contains(&dst) {
                neighbor_vids.insert(dst);
            }
        }

        // Step 4: Add 1-hop neighbor vertices
        for &vid in &neighbor_vids {
            let id = self.vertex_string_id(vid);
            let vlabels = self
                .vertex_labels
                .get(vid as usize)
                .cloned()
                .unwrap_or_default();
            let props = self
                .vertex_props_map
                .get(vid as usize)
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| (k.clone(), prop_to_cypher(v)))
                        .collect()
                })
                .unwrap_or_default();
            g.add_node(yata_cypher::NodeRef {
                id,
                labels: vlabels,
                props,
            });
        }

        g.build_csr();
        g
    }

    /// Get the string ID for a vertex. Uses the first property that looks like
    /// a primary key (vid/id/eid), falling back to "v{vid}".
    fn vertex_string_id(&self, vid: u32) -> String {
        if let Some(global_vid) = self.global_vid(vid) {
            return format!("g{}", global_vid.get());
        }
        if let Some(props) = self.vertex_props_map.get(vid as usize) {
            // Check for explicit string IDs used by graph_host
            for key in &["vid", "id", "_vid"] {
                if let Some(PropValue::Str(s)) = props.get(*key) {
                    return s.clone();
                }
            }
        }
        format!("v{}", vid)
    }
}

/// Convert PropValue (yata-grin) → Value (yata-cypher).
pub fn prop_to_cypher(v: &PropValue) -> yata_cypher::types::Value {
    match v {
        PropValue::Null => yata_cypher::types::Value::Null,
        PropValue::Bool(b) => yata_cypher::types::Value::Bool(*b),
        PropValue::Int(i) => yata_cypher::types::Value::Int(*i),
        PropValue::Float(f) => yata_cypher::types::Value::Float(*f),
        PropValue::Str(s) => yata_cypher::types::Value::Str(s.clone()),
    }
}

/// Convert Value (yata-cypher) → PropValue (yata-grin).
pub fn cypher_to_prop(v: &yata_cypher::types::Value) -> PropValue {
    match v {
        yata_cypher::types::Value::Null => PropValue::Null,
        yata_cypher::types::Value::Bool(b) => PropValue::Bool(*b),
        yata_cypher::types::Value::Int(i) => PropValue::Int(*i),
        yata_cypher::types::Value::Float(f) => PropValue::Float(*f),
        yata_cypher::types::Value::Str(s) => PropValue::Str(s.clone()),
        // Complex types degrade to JSON string
        other => PropValue::Str(format!("{}", other)),
    }
}

impl Default for MutableCsrStore {
    fn default() -> Self {
        Self::new()
    }
}

fn prop_cmp(a: &PropValue, b: &PropValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (PropValue::Int(x), PropValue::Int(y)) => Some(x.cmp(y)),
        (PropValue::Float(x), PropValue::Float(y)) => x.partial_cmp(y),
        (PropValue::Str(x), PropValue::Str(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

// SAFETY: AtomicU32/AtomicU64 are Send+Sync. All other fields are owned.
unsafe impl Send for MutableCsrStore {}
unsafe impl Sync for MutableCsrStore {}

impl Topology for MutableCsrStore {
    fn vertex_count(&self) -> usize {
        self.vertex_alive.iter().filter(|&&a| a).count()
    }

    fn edge_count(&self) -> usize {
        self.edge_alive.iter().filter(|&&a| a).count()
    }

    fn has_vertex(&self, vid: u32) -> bool {
        self.vertex_alive
            .get(vid as usize)
            .copied()
            .unwrap_or(false)
    }

    fn out_degree(&self, vid: u32) -> usize {
        if !self.has_vertex(vid) {
            return 0;
        }
        self.out_csr
            .values()
            .map(|csr| csr.neighbors(vid).len())
            .sum()
    }

    fn in_degree(&self, vid: u32) -> usize {
        if !self.has_vertex(vid) {
            return 0;
        }
        self.in_csr
            .values()
            .map(|csr| csr.neighbors(vid).len())
            .sum()
    }

    fn out_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        if !self.has_vertex(vid) {
            return Vec::new();
        }
        let mut result = Vec::new();
        for (label, csr) in &self.out_csr {
            for &eid in csr.neighbors(vid) {
                let eid_usize = eid as usize;
                if eid_usize < self.edge_dst.len() && self.edge_alive[eid_usize] {
                    result.push(Neighbor {
                        vid: self.edge_dst[eid_usize],
                        edge_id: eid,
                        edge_label: label.clone(),
                    });
                }
            }
        }
        result
    }

    fn in_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        if !self.has_vertex(vid) {
            return Vec::new();
        }
        let mut result = Vec::new();
        for (label, csr) in &self.in_csr {
            for &eid in csr.neighbors(vid) {
                let eid_usize = eid as usize;
                if eid_usize < self.edge_src.len() && self.edge_alive[eid_usize] {
                    result.push(Neighbor {
                        vid: self.edge_src[eid_usize],
                        edge_id: eid,
                        edge_label: label.clone(),
                    });
                }
            }
        }
        result
    }

    fn out_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        if !self.has_vertex(vid) {
            return Vec::new();
        }
        let csr = match self.out_csr.get(edge_label) {
            Some(c) => c,
            None => return Vec::new(),
        };
        let mut result = Vec::new();
        for &eid in csr.neighbors(vid) {
            let eid_usize = eid as usize;
            if eid_usize < self.edge_dst.len() && self.edge_alive[eid_usize] {
                result.push(Neighbor {
                    vid: self.edge_dst[eid_usize],
                    edge_id: eid,
                    edge_label: edge_label.to_string(),
                });
            }
        }
        result
    }

    fn in_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        if !self.has_vertex(vid) {
            return Vec::new();
        }
        let csr = match self.in_csr.get(edge_label) {
            Some(c) => c,
            None => return Vec::new(),
        };
        let mut result = Vec::new();
        for &eid in csr.neighbors(vid) {
            let eid_usize = eid as usize;
            if eid_usize < self.edge_src.len() && self.edge_alive[eid_usize] {
                result.push(Neighbor {
                    vid: self.edge_src[eid_usize],
                    edge_id: eid,
                    edge_label: edge_label.to_string(),
                });
            }
        }
        result
    }
}

impl Property for MutableCsrStore {
    fn vertex_labels(&self, vid: u32) -> Vec<String> {
        self.vertex_labels
            .get(vid as usize)
            .cloned()
            .unwrap_or_default()
    }

    fn vertex_prop(&self, vid: u32, key: &str) -> Option<PropValue> {
        if !self.has_vertex(vid) {
            return None;
        }
        self.vertex_prop_value(vid, key)
    }

    fn vertex_all_props(&self, vid: u32) -> HashMap<String, PropValue> {
        if !self.has_vertex(vid) {
            return HashMap::new();
        }
        self.vertex_props_map
            .get(vid as usize)
            .cloned()
            .unwrap_or_default()
    }

    fn edge_prop(&self, edge_id: u32, key: &str) -> Option<PropValue> {
        let eid = edge_id as usize;
        if eid >= self.edge_alive.len() || !self.edge_alive[eid] {
            return None;
        }
        self.edge_props.get(eid).and_then(|m| m.get(key).cloned())
    }

    fn vertex_prop_keys(&self, label: &str) -> Vec<String> {
        let mut keys = std::collections::HashSet::new();
        for (vid, labels) in self.vertex_labels.iter().enumerate() {
            if labels.contains(&label.to_string()) && self.vertex_alive[vid] {
                if let Some(props) = self.vertex_props_map.get(vid) {
                    for k in props.keys() {
                        keys.insert(k.clone());
                    }
                }
            }
        }
        keys.into_iter().collect()
    }

    fn edge_prop_keys(&self, label: &str) -> Vec<String> {
        let mut keys = std::collections::HashSet::new();
        for (eid, el) in self.edge_labels.iter().enumerate() {
            if el == label && self.edge_alive[eid] {
                if let Some(props) = self.edge_props.get(eid) {
                    for k in props.keys() {
                        keys.insert(k.clone());
                    }
                }
            }
        }
        keys.into_iter().collect()
    }
}

impl Schema for MutableCsrStore {
    fn vertex_labels(&self) -> Vec<String> {
        self.known_vertex_labels.clone()
    }

    fn edge_labels(&self) -> Vec<String> {
        self.known_edge_labels.clone()
    }

    fn vertex_primary_key(&self, label: &str) -> Option<String> {
        self.vertex_pk.get(label).cloned()
    }
}

impl Scannable for MutableCsrStore {
    fn scan_vertices(&self, label: &str, predicate: &Predicate) -> Vec<u32> {
        // Fast path 1: property equality index (O(1) hash lookup)
        if let Predicate::Eq(key, value) = predicate {
            if let Some(idx) = self.prop_eq_index.get(&(label.to_string(), key.clone())) {
                let lookup_key = format!("{:?}", value);
                return idx.get(&lookup_key).cloned().unwrap_or_default();
            }
        }

        // Fast path 2: columnar cache scan (sequential memory, cache-friendly)
        match predicate {
            Predicate::Eq(key, _)
            | Predicate::Neq(key, _)
            | Predicate::Lt(key, _)
            | Predicate::Gt(key, _)
            | Predicate::In(key, _)
            | Predicate::StartsWith(key, _) => {
                if let Some(result) =
                    self.columnar_cache
                        .scan(label, key, predicate, &self.vertex_alive)
                {
                    return result;
                }
            }
            Predicate::True => {
                // Use bitmap for pure label scan (only if bitmap exists)
                if self.label_bitmap.bitmaps.contains_key(label) {
                    return self.label_bitmap.scan(label);
                }
            }
            _ => {} // And/Or — fall through to HashMap path
        }

        // Fall back: label index + HashMap predicate
        let candidates = if let Some(vids) = self.label_index.get(label) {
            vids.clone()
        } else {
            let mut vids = Vec::new();
            for (vid, labels) in self.vertex_labels.iter().enumerate() {
                if self.vertex_alive[vid] && labels.contains(&label.to_string()) {
                    vids.push(vid as u32);
                }
            }
            vids
        };
        candidates
            .into_iter()
            .filter(|&vid| self.predicate_matches(vid, predicate))
            .collect()
    }

    fn scan_vertices_by_label(&self, label: &str) -> Vec<u32> {
        // Fast path: bitmap scan (sequential memory)
        if self.label_bitmap.bitmaps.contains_key(label) {
            return self.label_bitmap.scan(label);
        }
        // Fallback: label index
        if let Some(vids) = self.label_index.get(label) {
            return vids.clone();
        }
        self.scan_vertices(label, &Predicate::True)
    }

    fn scan_all_vertices(&self) -> Vec<u32> {
        (0..self.vertex_alive.len() as u32)
            .filter(|&vid| self.vertex_alive[vid as usize])
            .collect()
    }
}

impl Mutable for MutableCsrStore {
    fn add_vertex(&mut self, label: &str, props: &[(&str, PropValue)]) -> u32 {
        let (vid, vid_usize) = self.allocate_vertex_slot();
        self.store_vertex_record(vid, vid_usize, &[label.to_string()], props, None);
        self.register_vertex_label(label);
        self.dirty_vertex_labels.insert(label.to_string());

        // WAL
        if let Some(ref mut wal) = self.wal {
            wal.append(WalEntry::AddVertex {
                vid,
                global_vid: None,
                label: label.to_string(),
                props: props
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect(),
            });
        }

        vid
    }

    fn add_edge(&mut self, src: u32, dst: u32, label: &str, props: &[(&str, PropValue)]) -> u32 {
        let eid = self.edge_count;
        self.edge_count += 1;

        let mut prop_map = HashMap::new();
        for &(k, ref v) in props {
            prop_map.insert(k.to_string(), v.clone());
        }

        self.edge_props.push(prop_map);
        self.edge_labels.push(label.to_string());
        self.edge_src.push(src);
        self.edge_dst.push(dst);
        self.edge_global_ids.push(None);
        self.edge_alive.push(true);

        self.pending_edges.push(PendingEdge {
            src,
            dst,
            label: label.to_string(),
            edge_id: eid,
        });

        self.register_edge_label(label);
        self.dirty_edge_labels.insert(label.to_string());

        // WAL
        if let Some(ref mut wal) = self.wal {
            wal.append(WalEntry::AddEdge {
                edge_id: eid,
                src,
                dst,
                label: label.to_string(),
                props: props
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect(),
            });
        }

        eid
    }

    fn set_vertex_prop(&mut self, vid: u32, key: &str, value: PropValue) {
        if let Some(props) = self.vertex_props_map.get_mut(vid as usize) {
            props.insert(key.to_string(), value.clone());
        }
        // Mark dirty
        if let Some(labels) = self.vertex_labels.get(vid as usize) {
            for l in labels {
                self.dirty_vertex_labels.insert(l.clone());
            }
        }
        // WAL
        if let Some(ref mut wal) = self.wal {
            wal.append(WalEntry::SetProperty {
                vid,
                key: key.to_string(),
                value,
            });
        }
    }

    fn delete_vertex(&mut self, vid: u32) {
        // Mark dirty before clearing alive flag
        if let Some(labels) = self.vertex_labels.get(vid as usize) {
            for l in labels {
                self.dirty_vertex_labels.insert(l.clone());
            }
        }
        if let Some(Some(global_vid)) = self.vertex_global_ids.get(vid as usize) {
            self.global_to_local_vid.remove(global_vid);
        }
        if let Some(alive) = self.vertex_alive.get_mut(vid as usize) {
            *alive = false;
        }
        // Soft-delete all incident edges
        for eid in 0..self.edge_count as usize {
            if self.edge_src[eid] == vid || self.edge_dst[eid] == vid {
                if self.edge_alive[eid] {
                    if let Some(label) = self.edge_labels.get(eid) {
                        self.dirty_edge_labels.insert(label.clone());
                    }
                }
                self.edge_alive[eid] = false;
            }
        }
        // WAL
        if let Some(ref mut wal) = self.wal {
            wal.append(WalEntry::DeleteVertex { vid });
        }
    }

    fn delete_edge(&mut self, edge_id: u32) {
        // Mark edge label dirty before delete
        if let Some(label) = self.edge_labels.get(edge_id as usize) {
            self.dirty_edge_labels.insert(label.clone());
        }
        if let Some(alive) = self.edge_alive.get_mut(edge_id as usize) {
            *alive = false;
        }
        if let Some(Some(global_eid)) = self.edge_global_ids.get(edge_id as usize) {
            self.global_to_local_eid.remove(global_eid);
        }
        // WAL
        if let Some(ref mut wal) = self.wal {
            wal.append(WalEntry::DeleteEdge { edge_id });
        }
    }

    fn commit(&mut self) -> u64 {
        self.rebuild_csr();
        self.rebuild_label_index();
        self.rebuild_prop_indexes();
        self.rebuild_columnar_cache();
        self.rebuild_label_bitmap();
        self.rebuild_btree_indexes();
        // Reset dirty tracking after rebuild
        self.dirty_vertex_labels.clear();
        self.dirty_edge_labels.clear();
        let ver = self.version.fetch_add(1, Ordering::Relaxed) + 1;
        // WAL
        if let Some(ref mut wal) = self.wal {
            wal.append(WalEntry::Commit { version: ver });
        }
        ver
    }
}

impl Partitioned for MutableCsrStore {
    fn partition_id(&self) -> u32 {
        self.partition_id.get()
    }

    fn partition_count(&self) -> u32 {
        1 // single-store: overridden by coordinator
    }

    fn vertex_partition(&self, _vid: u32) -> u32 {
        self.partition_id.get()
    }

    fn is_master(&self, _vid: u32) -> bool {
        true // single-store: all vertices are master
    }
}

impl Tiered for MutableCsrStore {
    fn tier(&self) -> Tier {
        Tier::Hot
    }

    fn can_serve(&self, labels: &[String]) -> bool {
        if labels.is_empty() {
            return self.vertex_count() > 0;
        }
        labels.iter().any(|l| self.label_index.contains_key(l))
    }
}

impl Versioned for MutableCsrStore {
    fn current_version(&self) -> VersionId {
        self.version.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn versions(&self, _limit: usize) -> Vec<VersionId> {
        vec![self.current_version()]
    }

    fn has_version(&self, version: VersionId) -> bool {
        version == self.current_version()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_core::{GlobalVid, LocalVid, PartitionId};

    #[test]
    fn test_empty_store() {
        let store = MutableCsrStore::new();
        assert_eq!(store.vertex_count(), 0);
        assert_eq!(store.edge_count(), 0);
        assert!(!store.has_vertex(0));
        assert_eq!(store.version(), 0);
        assert_eq!(store.partition_id_raw(), PartitionId::from(0));
    }

    #[test]
    fn test_partition_constructors() {
        let typed = MutableCsrStore::new_with_partition_id(PartitionId::from(7));
        let compat = MutableCsrStore::new_partition(7);

        assert_eq!(typed.partition_id_raw(), PartitionId::from(7));
        assert_eq!(compat.partition_id_raw(), PartitionId::from(7));
        assert_eq!(
            MutableCsrStore::default().partition_id_raw(),
            PartitionId::from(0)
        );
    }

    #[test]
    fn test_add_vertex() {
        let mut store = MutableCsrStore::new();
        let vid = store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        assert_eq!(vid, 0);
        assert!(store.has_vertex(0));
        assert_eq!(store.vertex_count(), 1);
        assert_eq!(
            store.vertex_prop(0, "name"),
            Some(PropValue::Str("Alice".into()))
        );
        assert_eq!(store.vertex_prop(0, "age"), Some(PropValue::Int(30)));
        assert_eq!(store.vertex_prop(0, "missing"), None);

        let labels: Vec<String> = Property::vertex_labels(&store, 0);
        assert_eq!(labels, vec!["Person".to_string()]);
    }

    #[test]
    fn test_add_vertex_with_global_id() {
        let mut store = MutableCsrStore::new();
        let gid = GlobalVid::from(42u64);
        let vid = store.add_vertex_with_global_id(
            gid,
            "Person",
            &[("name", PropValue::Str("Alice".into()))],
        );
        assert_eq!(vid, 0);
        assert_eq!(store.local_vid(gid), Some(LocalVid::from(0u32)));
        assert_eq!(store.global_vid(0), Some(gid));
    }

    #[test]
    fn test_add_vertex_with_global_id_is_idempotent() {
        let mut store = MutableCsrStore::new();
        let gid = GlobalVid::from(7u64);
        let first = store.add_vertex_with_global_id(gid, "Person", &[]);
        let second = store.add_vertex_with_global_id(gid, "Person", &[]);
        assert_eq!(first, second);
        assert_eq!(store.vertex_count(), 1);
    }

    #[test]
    fn test_add_edge_with_global_id() {
        let mut store = MutableCsrStore::new();
        let a = store.add_vertex("Person", &[]);
        let b = store.add_vertex("Person", &[]);
        let eid = store.add_edge_with_global_id(77u64.into(), a, b, "KNOWS", &[]);
        assert_eq!(eid, 0);
        assert_eq!(store.local_eid(77u64.into()), Some(LocalEid::from(0u32)));
        assert_eq!(store.global_eid(0), Some(77u64.into()));
    }

    #[test]
    fn test_optional_global_id_wrappers() {
        let mut store = MutableCsrStore::new();
        let labels = vec!["Person".to_string(), "Actor".to_string()];
        let alice = store.add_vertex_with_labels_and_optional_global_id(
            Some(100u64.into()),
            &labels,
            &[("name", PropValue::Str("Alice".into()))],
        );
        let bob = store.add_vertex_with_optional_global_id(None, "Person", &[]);
        let knows =
            store.add_edge_with_optional_global_id(Some(200u64.into()), alice, bob, "KNOWS", &[]);
        let likes = store.add_edge_with_optional_global_id(None, bob, alice, "LIKES", &[]);

        assert_eq!(store.global_vid(alice), Some(100u64.into()));
        assert_eq!(store.global_vid(bob), None);
        assert_eq!(store.global_eid(knows), Some(200u64.into()));
        assert_eq!(store.global_eid(likes), None);
    }

    #[test]
    fn test_add_edge_and_traverse() {
        let mut store = MutableCsrStore::new();
        let alice = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        let bob = store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        let eid = store.add_edge(alice, bob, "KNOWS", &[("since", PropValue::Int(2020))]);

        // Before commit, CSR is empty
        assert_eq!(store.out_degree(alice), 0);

        // Commit rebuilds CSR
        let ver = store.commit();
        assert_eq!(ver, 1);

        // Now traversal works
        assert_eq!(store.out_degree(alice), 1);
        assert_eq!(store.in_degree(bob), 1);
        assert_eq!(store.in_degree(alice), 0);
        assert_eq!(store.out_degree(bob), 0);

        let out = store.out_neighbors(alice);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].vid, bob);
        assert_eq!(out[0].edge_label, "KNOWS");
        assert_eq!(out[0].edge_id, eid);

        let inc = store.in_neighbors(bob);
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].vid, alice);

        // Edge properties
        assert_eq!(store.edge_prop(eid, "since"), Some(PropValue::Int(2020)));
    }

    #[test]
    fn test_multi_label() {
        let mut store = MutableCsrStore::new();
        let a = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        let b = store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        let c = store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);

        store.add_edge(a, b, "WORKS_AT", &[]);
        store.add_edge(a, c, "KNOWS", &[]);
        store.commit();

        // By label traversal
        let works = store.out_neighbors_by_label(a, "WORKS_AT");
        assert_eq!(works.len(), 1);
        assert_eq!(works[0].vid, b);

        let knows = store.out_neighbors_by_label(a, "KNOWS");
        assert_eq!(knows.len(), 1);
        assert_eq!(knows[0].vid, c);

        // Schema
        let vlabels = Schema::vertex_labels(&store);
        assert!(vlabels.contains(&"Person".to_string()));
        assert!(vlabels.contains(&"Company".to_string()));

        let elabels = Schema::edge_labels(&store);
        assert!(elabels.contains(&"WORKS_AT".to_string()));
        assert!(elabels.contains(&"KNOWS".to_string()));
    }

    #[test]
    fn test_predicate_scan() {
        let mut store = MutableCsrStore::new();
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Bob".into())),
                ("age", PropValue::Int(25)),
            ],
        );
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Charlie".into())),
                ("age", PropValue::Int(35)),
            ],
        );

        // Gt age > 28
        let result =
            store.scan_vertices("Person", &Predicate::Gt("age".into(), PropValue::Int(28)));
        assert_eq!(result.len(), 2); // Alice(30), Charlie(35)
        assert!(result.contains(&0));
        assert!(result.contains(&2));

        // Eq name = "Bob"
        let result = store.scan_vertices(
            "Person",
            &Predicate::Eq("name".into(), PropValue::Str("Bob".into())),
        );
        assert_eq!(result, vec![1]);

        // StartsWith name starts with "Al"
        let result =
            store.scan_vertices("Person", &Predicate::StartsWith("name".into(), "Al".into()));
        assert_eq!(result, vec![0]);

        // And: age > 28 AND name starts with "C"
        let result = store.scan_vertices(
            "Person",
            &Predicate::And(
                Box::new(Predicate::Gt("age".into(), PropValue::Int(28))),
                Box::new(Predicate::StartsWith("name".into(), "C".into())),
            ),
        );
        assert_eq!(result, vec![2]);

        // scan_vertices_by_label
        let all_persons = store.scan_vertices_by_label("Person");
        assert_eq!(all_persons.len(), 3);

        // scan_all_vertices
        let all = store.scan_all_vertices();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_star_topology() {
        let mut store = MutableCsrStore::new();
        let hub = store.add_vertex("Hub", &[("name", PropValue::Str("center".into()))]);
        let mut spokes = Vec::new();
        for i in 0..100 {
            let v = store.add_vertex("Spoke", &[("idx", PropValue::Int(i))]);
            spokes.push(v);
        }

        for &spoke in &spokes {
            store.add_edge(hub, spoke, "CONNECTS", &[]);
        }
        store.commit();

        assert_eq!(store.out_degree(hub), 100);
        assert_eq!(store.in_degree(hub), 0);

        let neighbors = store.out_neighbors(hub);
        assert_eq!(neighbors.len(), 100);

        // Each spoke has in-degree 1
        for &spoke in &spokes {
            assert_eq!(store.in_degree(spoke), 1);
            assert_eq!(store.out_degree(spoke), 0);
        }
    }

    #[test]
    fn test_commit_rebuilds_csr() {
        let mut store = MutableCsrStore::new();
        let a = store.add_vertex("V", &[]);
        let b = store.add_vertex("V", &[]);
        let c = store.add_vertex("V", &[]);

        // First batch
        store.add_edge(a, b, "E", &[]);
        store.commit();
        assert_eq!(store.out_degree(a), 1);

        // Second batch
        store.add_edge(a, c, "E", &[]);
        store.commit();
        assert_eq!(store.out_degree(a), 2);
        assert_eq!(store.version(), 2);
    }

    #[test]
    fn test_delete_vertex() {
        let mut store = MutableCsrStore::new();
        let a = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        let b = store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_edge(a, b, "KNOWS", &[]);
        store.commit();

        assert_eq!(store.vertex_count(), 2);
        assert_eq!(store.edge_count(), 1);

        // Delete vertex a
        store.delete_vertex(a);
        store.commit();

        assert!(!store.has_vertex(a));
        assert!(store.has_vertex(b));
        assert_eq!(store.vertex_count(), 1);
        assert_eq!(store.edge_count(), 0); // incident edges soft-deleted

        // Scans should not return deleted vertex
        let scanned = store.scan_vertices_by_label("Person");
        assert_eq!(scanned, vec![b]);

        // Properties of deleted vertex return None
        assert_eq!(store.vertex_prop(a, "name"), None);
    }

    #[test]
    fn test_delete_edge() {
        let mut store = MutableCsrStore::new();
        let a = store.add_vertex("V", &[]);
        let b = store.add_vertex("V", &[]);
        let eid = store.add_edge(a, b, "E", &[("w", PropValue::Float(1.5))]);
        store.commit();

        assert_eq!(store.out_degree(a), 1);
        assert_eq!(store.edge_prop(eid, "w"), Some(PropValue::Float(1.5)));

        store.delete_edge(eid);
        store.commit();

        assert_eq!(store.out_degree(a), 0);
        assert_eq!(store.edge_count(), 0);
        assert_eq!(store.edge_prop(eid, "w"), None);
    }

    #[test]
    fn test_set_vertex_prop() {
        let mut store = MutableCsrStore::new();
        let vid = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);

        assert_eq!(
            store.vertex_prop(vid, "name"),
            Some(PropValue::Str("Alice".into()))
        );

        store.set_vertex_prop(vid, "name", PropValue::Str("Alicia".into()));
        assert_eq!(
            store.vertex_prop(vid, "name"),
            Some(PropValue::Str("Alicia".into()))
        );

        store.set_vertex_prop(vid, "age", PropValue::Int(31));
        assert_eq!(store.vertex_prop(vid, "age"), Some(PropValue::Int(31)));
    }

    #[test]
    fn test_vertex_primary_key() {
        let mut store = MutableCsrStore::new();
        store.set_vertex_primary_key("Person", "name");
        assert_eq!(store.vertex_primary_key("Person"), Some("name".to_string()));
        assert_eq!(store.vertex_primary_key("Unknown"), None);
    }

    #[test]
    fn test_in_neighbors_by_label() {
        let mut store = MutableCsrStore::new();
        let a = store.add_vertex("V", &[]);
        let b = store.add_vertex("V", &[]);
        let c = store.add_vertex("V", &[]);

        store.add_edge(a, c, "X", &[]);
        store.add_edge(b, c, "Y", &[]);
        store.commit();

        let x_in = store.in_neighbors_by_label(c, "X");
        assert_eq!(x_in.len(), 1);
        assert_eq!(x_in[0].vid, a);

        let y_in = store.in_neighbors_by_label(c, "Y");
        assert_eq!(y_in.len(), 1);
        assert_eq!(y_in[0].vid, b);

        let z_in = store.in_neighbors_by_label(c, "Z");
        assert!(z_in.is_empty());
    }

    #[test]
    fn test_prop_keys() {
        let mut store = MutableCsrStore::new();
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Bob".into())),
                ("city", PropValue::Str("Tokyo".into())),
            ],
        );
        store.add_edge(0, 1, "KNOWS", &[("since", PropValue::Int(2020))]);
        store.commit();

        let vkeys = store.vertex_prop_keys("Person");
        assert!(vkeys.contains(&"name".to_string()));
        assert!(vkeys.contains(&"age".to_string()));
        assert!(vkeys.contains(&"city".to_string()));

        let ekeys = store.edge_prop_keys("KNOWS");
        assert!(ekeys.contains(&"since".to_string()));
    }

    #[test]
    fn test_large_graph() {
        let mut store = MutableCsrStore::new();

        // 10K vertices
        for i in 0..10_000u32 {
            store.add_vertex("Node", &[("id", PropValue::Int(i as i64))]);
        }

        // 50K edges (random-ish pattern: v -> (v*7+13) % 10000)
        for i in 0..10_000u32 {
            for j in 0..5 {
                let dst = (i.wrapping_mul(7).wrapping_add(13).wrapping_add(j * 997)) % 10_000;
                store.add_edge(i, dst, "LINK", &[]);
            }
        }

        store.commit();

        assert_eq!(store.vertex_count(), 10_000);
        assert_eq!(store.edge_count(), 50_000);

        // Spot-check traversal
        let out = store.out_neighbors(0);
        assert_eq!(out.len(), 5);

        // Scan with predicate
        let found = store.scan_vertices("Node", &Predicate::Eq("id".into(), PropValue::Int(42)));
        assert_eq!(found, vec![42]);
    }

    #[test]
    fn test_or_predicate() {
        let mut store = MutableCsrStore::new();
        store.add_vertex("P", &[("x", PropValue::Int(1))]);
        store.add_vertex("P", &[("x", PropValue::Int(2))]);
        store.add_vertex("P", &[("x", PropValue::Int(3))]);

        let result = store.scan_vertices(
            "P",
            &Predicate::Or(
                Box::new(Predicate::Eq("x".into(), PropValue::Int(1))),
                Box::new(Predicate::Eq("x".into(), PropValue::Int(3))),
            ),
        );
        assert_eq!(result.len(), 2);
        assert!(result.contains(&0));
        assert!(result.contains(&2));
    }

    #[test]
    fn test_neq_predicate() {
        let mut store = MutableCsrStore::new();
        store.add_vertex("P", &[("x", PropValue::Int(1))]);
        store.add_vertex("P", &[("x", PropValue::Int(2))]);

        let result = store.scan_vertices("P", &Predicate::Neq("x".into(), PropValue::Int(1)));
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_in_predicate() {
        let mut store = MutableCsrStore::new();
        store.add_vertex("P", &[("x", PropValue::Int(1))]);
        store.add_vertex("P", &[("x", PropValue::Int(2))]);
        store.add_vertex("P", &[("x", PropValue::Int(3))]);

        let result = store.scan_vertices(
            "P",
            &Predicate::In("x".into(), vec![PropValue::Int(1), PropValue::Int(3)]),
        );
        assert_eq!(result.len(), 2);
        assert!(result.contains(&0));
        assert!(result.contains(&2));
    }

    #[test]
    fn test_no_neighbors_for_deleted_vertex() {
        let mut store = MutableCsrStore::new();
        let a = store.add_vertex("V", &[]);
        let b = store.add_vertex("V", &[]);
        store.add_edge(a, b, "E", &[]);
        store.commit();

        store.delete_vertex(a);
        // Even before re-commit, out_neighbors checks alive
        assert!(store.out_neighbors(a).is_empty());
        assert_eq!(store.out_degree(a), 0);
    }

    #[test]
    fn test_set_vertex_prop_japanese_string() {
        let mut store = MutableCsrStore::new();
        let vid = store.add_vertex("人物", &[("名前", PropValue::Str("山田花子".into()))]);

        // Verify initial Japanese property
        assert_eq!(
            store.vertex_prop(vid, "名前"),
            Some(PropValue::Str("山田花子".into()))
        );

        // set_vertex_prop with Japanese value
        store.set_vertex_prop(vid, "職業", PropValue::Str("ソフトウェアエンジニア".into()));
        assert_eq!(
            store.vertex_prop(vid, "職業"),
            Some(PropValue::Str("ソフトウェアエンジニア".into()))
        );

        // Overwrite existing Japanese property
        store.set_vertex_prop(vid, "名前", PropValue::Str("田中太郎".into()));
        assert_eq!(
            store.vertex_prop(vid, "名前"),
            Some(PropValue::Str("田中太郎".into()))
        );

        // Japanese key with non-Japanese value and vice versa
        store.set_vertex_prop(vid, "住所", PropValue::Str("Tokyo, Japan".into()));
        assert_eq!(
            store.vertex_prop(vid, "住所"),
            Some(PropValue::Str("Tokyo, Japan".into()))
        );
    }

    #[test]
    fn test_csr_property_cjk_emoji_mixed() {
        let mut store = MutableCsrStore::new();

        // CJK: Japanese + Chinese + Korean + emoji in a single value
        let mixed = "日本語 中文 한국어 🎌🇯🇵✨ emoji 🚀";
        let vid = store.add_vertex(
            "多言語",
            &[
                ("content", PropValue::Str(mixed.into())),
                ("タグ", PropValue::Str("テスト🧪".into())),
            ],
        );

        assert_eq!(
            store.vertex_prop(vid, "content"),
            Some(PropValue::Str(mixed.into()))
        );
        assert_eq!(
            store.vertex_prop(vid, "タグ"),
            Some(PropValue::Str("テスト🧪".into()))
        );

        // Verify byte-level integrity of CJK+emoji mix
        if let Some(PropValue::Str(ref s)) = store.vertex_prop(vid, "content") {
            assert_eq!(s.as_bytes(), mixed.as_bytes());
        } else {
            panic!("expected Str prop");
        }

        // set_vertex_prop with emoji overwrite
        store.set_vertex_prop(vid, "content", PropValue::Str("更新済み ✅🎉".into()));
        assert_eq!(
            store.vertex_prop(vid, "content"),
            Some(PropValue::Str("更新済み ✅🎉".into()))
        );

        // Edge with CJK label and emoji props
        let vid2 = store.add_vertex("ノード", &[]);
        let eid = store.add_edge(
            vid,
            vid2,
            "関連",
            &[("種類", PropValue::Str("参照🔗".into()))],
        );
        assert_eq!(
            store.edge_prop(eid, "種類"),
            Some(PropValue::Str("参照🔗".into()))
        );
    }

    #[test]
    fn test_untyped_out_neighbors() {
        let mut store = MutableCsrStore::new();
        let a = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        let b = store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        let c = store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);

        store.add_edge(a, b, "KNOWS", &[]);
        store.add_edge(a, c, "WORKS_AT", &[]);
        store.add_edge(b, a, "FOLLOWS", &[]);
        store.commit();

        // out_neighbors returns ALL neighbors across all edge labels
        let out = store.out_neighbors(a);
        assert_eq!(out.len(), 2);
        let out_vids: Vec<u32> = out.iter().map(|n| n.vid).collect();
        assert!(out_vids.contains(&b));
        assert!(out_vids.contains(&c));

        // in_neighbors returns ALL incoming neighbors across all edge labels
        let inc = store.in_neighbors(a);
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].vid, b);
        assert_eq!(inc[0].edge_label, "FOLLOWS");

        // Vertex b: out = FOLLOWS->a, in = KNOWS from a
        let out_b = store.out_neighbors(b);
        assert_eq!(out_b.len(), 1);
        assert_eq!(out_b[0].vid, a);

        let in_b = store.in_neighbors(b);
        assert_eq!(in_b.len(), 1);
        assert_eq!(in_b[0].vid, a);
        assert_eq!(in_b[0].edge_label, "KNOWS");
    }

    #[test]
    fn test_delete_edge_consistency() {
        let mut store = MutableCsrStore::new();
        let a = store.add_vertex("V", &[]);
        let b = store.add_vertex("V", &[]);
        let eid = store.add_edge(a, b, "REL", &[("w", PropValue::Float(1.0))]);
        store.commit();

        // Verify edge exists
        assert_eq!(store.out_neighbors_by_label(a, "REL").len(), 1);
        assert_eq!(store.in_neighbors_by_label(b, "REL").len(), 1);

        // Delete edge and recommit
        store.delete_edge(eid);
        store.commit();

        // out_neighbors_by_label returns empty
        assert!(store.out_neighbors_by_label(a, "REL").is_empty());
        // in_neighbors_by_label returns empty
        assert!(store.in_neighbors_by_label(b, "REL").is_empty());
        // edge_alive is false (edge_prop returns None for dead edges)
        assert_eq!(store.edge_prop(eid, "w"), None);
        // edge_count is 0
        assert_eq!(store.edge_count(), 0);
    }

    #[test]
    fn test_scan_all_vertices() {
        let mut store = MutableCsrStore::new();
        let p = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        let co = store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        let ci = store.add_vertex("City", &[("name", PropValue::Str("Tokyo".into()))]);
        store.commit();

        let all = store.scan_all_vertices();
        assert_eq!(all.len(), 3);
        assert!(all.contains(&p));
        assert!(all.contains(&co));
        assert!(all.contains(&ci));
    }

    #[test]
    fn test_delete_vertex_cascading_edges() {
        let mut store = MutableCsrStore::new();
        let a = store.add_vertex("V", &[("name", PropValue::Str("A".into()))]);
        let b = store.add_vertex("V", &[("name", PropValue::Str("B".into()))]);
        let c = store.add_vertex("V", &[("name", PropValue::Str("C".into()))]);

        store.add_edge(a, b, "E", &[]);
        store.add_edge(b, c, "E", &[]);
        store.commit();

        // Verify initial topology
        assert_eq!(store.out_neighbors(a).len(), 1);
        assert_eq!(store.out_neighbors(b).len(), 1);
        assert_eq!(store.in_neighbors(b).len(), 1);
        assert_eq!(store.in_neighbors(c).len(), 1);

        // Delete vertex B
        store.delete_vertex(b);
        store.commit();

        // Edges to/from B are gone
        assert!(!store.has_vertex(b));
        assert!(store.out_neighbors(b).is_empty());
        assert!(store.in_neighbors(b).is_empty());

        // A has no outgoing edges (A->B was deleted)
        assert!(store.out_neighbors(a).is_empty());
        // C has no incoming edges (B->C was deleted)
        assert!(store.in_neighbors(c).is_empty());

        // A and C still exist
        assert!(store.has_vertex(a));
        assert!(store.has_vertex(c));
        assert_eq!(store.edge_count(), 0);
    }

    #[test]
    fn test_property_update_roundtrip() {
        let mut store = MutableCsrStore::new();
        let vid = store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        store.commit();

        // Verify initial values
        assert_eq!(
            store.vertex_prop(vid, "name"),
            Some(PropValue::Str("Alice".into()))
        );
        assert_eq!(store.vertex_prop(vid, "age"), Some(PropValue::Int(30)));

        // Update name — old value replaced
        store.set_vertex_prop(vid, "name", PropValue::Str("Alicia".into()));
        assert_eq!(
            store.vertex_prop(vid, "name"),
            Some(PropValue::Str("Alicia".into()))
        );

        // Update age
        store.set_vertex_prop(vid, "age", PropValue::Int(31));
        assert_eq!(store.vertex_prop(vid, "age"), Some(PropValue::Int(31)));

        // After commit, updated values persist
        store.commit();
        assert_eq!(
            store.vertex_prop(vid, "name"),
            Some(PropValue::Str("Alicia".into()))
        );
        assert_eq!(store.vertex_prop(vid, "age"), Some(PropValue::Int(31)));
    }

    #[test]
    fn test_empty_store_operations() {
        let store = MutableCsrStore::new();

        // scan_all_vertices on empty store returns empty
        assert!(store.scan_all_vertices().is_empty());

        // out_neighbors on nonexistent vertex returns empty
        assert!(store.out_neighbors(0).is_empty());

        // vertex_count returns 0
        assert_eq!(store.vertex_count(), 0);

        // edge_count returns 0
        assert_eq!(store.edge_count(), 0);

        // in_neighbors on nonexistent vertex returns empty
        assert!(store.in_neighbors(0).is_empty());

        // has_vertex returns false
        assert!(!store.has_vertex(0));

        // scan_vertices_by_label returns empty
        assert!(store.scan_vertices_by_label("Person").is_empty());
    }

    #[test]
    fn test_multi_label_vertex_scan() {
        let mut store = MutableCsrStore::new();
        let vid = store.add_vertex_with_labels(
            &["Person".into(), "Employee".into()],
            &[("name", PropValue::Str("Alice".into()))],
        );
        store.commit();

        // Both labels find the vertex
        let persons = store.scan_vertices_by_label("Person");
        assert_eq!(persons.len(), 1);
        assert!(persons.contains(&vid));

        let employees = store.scan_vertices_by_label("Employee");
        assert_eq!(employees.len(), 1);
        assert!(employees.contains(&vid));

        // Labels are both reported
        let labels: Vec<String> = Property::vertex_labels(&store, vid);
        assert!(labels.contains(&"Person".to_string()));
        assert!(labels.contains(&"Employee".to_string()));
    }

    #[test]
    fn test_self_loop_edge() {
        let mut store = MutableCsrStore::new();
        let v = store.add_vertex("Node", &[("name", PropValue::Str("self".into()))]);
        store.add_edge(v, v, "LOOP", &[]);
        store.commit();

        // out_neighbors_by_label returns the vertex itself
        let out = store.out_neighbors_by_label(v, "LOOP");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].vid, v);

        // in_neighbors_by_label returns the vertex itself
        let inc = store.in_neighbors_by_label(v, "LOOP");
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].vid, v);

        // Degrees reflect the self-loop
        assert_eq!(store.out_degree(v), 1);
        assert_eq!(store.in_degree(v), 1);
    }
}

#[cfg(test)]
mod wal_tests {
    use super::*;

    #[test]
    fn test_wal_records_mutations() {
        let mut store = MutableCsrStore::new();
        store.enable_wal();

        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_edge(0, 0, "SELF", &[("w", PropValue::Int(1))]);

        let wal = store.wal().unwrap();
        assert_eq!(wal.entries().len(), 2);
        match &wal.entries()[0] {
            WalEntry::AddVertex {
                vid,
                global_vid,
                label,
                props,
            } => {
                assert_eq!(*vid, 0);
                assert_eq!(*global_vid, None);
                assert_eq!(label, "Person");
                assert_eq!(props.len(), 1);
            }
            other => panic!("expected AddVertex, got {:?}", other),
        }
        match &wal.entries()[1] {
            WalEntry::AddEdge {
                edge_id,
                src,
                dst,
                label,
                props,
            } => {
                assert_eq!(*edge_id, 0);
                assert_eq!(*src, 0);
                assert_eq!(*dst, 0);
                assert_eq!(label, "SELF");
                assert_eq!(props.len(), 1);
            }
            other => panic!("expected AddEdge, got {:?}", other),
        }
    }

    #[test]
    fn test_wal_replay() {
        // Build a store with WAL enabled and populate it
        let mut src_store = MutableCsrStore::new();
        src_store.enable_wal();

        src_store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        src_store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Bob".into())),
                ("age", PropValue::Int(25)),
            ],
        );
        src_store.add_edge(0, 1, "KNOWS", &[("since", PropValue::Int(2020))]);
        src_store.commit();

        // Take the WAL and replay into a fresh store
        let wal = src_store.take_wal().unwrap();
        let mut dst_store = MutableCsrStore::new();
        wal.replay(&mut dst_store);

        // Verify the replayed store matches
        assert_eq!(dst_store.vertex_count(), 2);
        assert_eq!(dst_store.edge_count(), 1);
        assert_eq!(
            dst_store.vertex_prop(0, "name"),
            Some(PropValue::Str("Alice".into()))
        );
        assert_eq!(
            dst_store.vertex_prop(1, "name"),
            Some(PropValue::Str("Bob".into()))
        );
        assert_eq!(dst_store.out_degree(0), 1);
    }

    #[test]
    fn test_wal_serialization() {
        let mut wal = GraphWal::new();
        wal.append(WalEntry::AddVertex {
            vid: 0,
            global_vid: Some(GlobalVid::from(9u64)),
            label: "Person".into(),
            props: vec![("name".into(), PropValue::Str("Alice".into()))],
        });
        wal.append(WalEntry::AddEdge {
            edge_id: 0,
            src: 0,
            dst: 1,
            label: "KNOWS".into(),
            props: vec![],
        });
        wal.append(WalEntry::DeleteVertex { vid: 42 });
        wal.append(WalEntry::Commit { version: 1 });

        let bytes = wal.to_bytes();
        let restored = GraphWal::from_bytes(&bytes);

        assert_eq!(restored.entries().len(), 4);
        match &restored.entries()[0] {
            WalEntry::AddVertex {
                vid,
                global_vid,
                label,
                ..
            } => {
                assert_eq!(*vid, 0);
                assert_eq!(*global_vid, Some(GlobalVid::from(9u64)));
                assert_eq!(label, "Person");
            }
            other => panic!("expected AddVertex, got {:?}", other),
        }
        match &restored.entries()[3] {
            WalEntry::Commit { version } => assert_eq!(*version, 1),
            other => panic!("expected Commit, got {:?}", other),
        }
    }

    #[test]
    fn test_wal_commit_entry() {
        let mut store = MutableCsrStore::new();
        store.enable_wal();

        store.add_vertex("V", &[]);
        store.commit();

        let wal = store.wal().unwrap();
        let last = wal.entries().last().unwrap();
        match last {
            WalEntry::Commit { version } => assert_eq!(*version, 1),
            other => panic!("expected Commit, got {:?}", other),
        }
    }

    #[test]
    fn test_wal_replay_with_deletes() {
        let mut src_store = MutableCsrStore::new();
        src_store.enable_wal();

        src_store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        src_store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        src_store.add_edge(0, 1, "KNOWS", &[]);
        src_store.commit();
        src_store.delete_vertex(0);
        src_store.commit();

        let wal = src_store.take_wal().unwrap();
        let mut dst_store = MutableCsrStore::new();
        wal.replay(&mut dst_store);

        // Alice deleted, Bob alive
        assert!(!dst_store.has_vertex(0));
        assert!(dst_store.has_vertex(1));
        assert_eq!(dst_store.vertex_count(), 1);
        // Incident edge also soft-deleted
        assert_eq!(dst_store.edge_count(), 0);
    }
}

#[cfg(test)]
mod mvcc_tests {
    use super::*;

    #[test]
    fn test_snapshot_isolation() {
        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.commit();

        // Take snapshot at version 1
        let snap = store.snapshot();
        assert_eq!(snap.version(), 1);
        assert_eq!(snap.store().vertex_count(), 1);

        // Mutate after snapshot
        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.commit();

        // Snapshot still sees only Alice
        assert_eq!(snap.store().vertex_count(), 1);
        assert_eq!(
            snap.store().vertex_prop(0, "name"),
            Some(PropValue::Str("Alice".into()))
        );

        // Live store sees both
        assert_eq!(store.vertex_count(), 2);
    }

    #[test]
    fn test_snapshot_version() {
        let mut store = MutableCsrStore::new();
        assert_eq!(store.version(), 0);

        store.add_vertex("V", &[]);
        store.commit(); // version 1

        let snap1 = store.snapshot();
        assert_eq!(snap1.version(), 1);

        store.add_vertex("V", &[]);
        store.commit(); // version 2

        let snap2 = store.snapshot();
        assert_eq!(snap2.version(), 2);

        // snap1 still at version 1
        assert_eq!(snap1.version(), 1);
    }

    #[test]
    fn test_multiple_snapshots() {
        let mut store = MutableCsrStore::new();

        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.commit();
        let snap1 = store.snapshot();

        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.commit();
        let snap2 = store.snapshot();

        store.add_vertex("Person", &[("name", PropValue::Str("Charlie".into()))]);
        store.commit();
        let snap3 = store.snapshot();

        assert_eq!(snap1.store().vertex_count(), 1);
        assert_eq!(snap2.store().vertex_count(), 2);
        assert_eq!(snap3.store().vertex_count(), 3);

        assert_eq!(snap1.version(), 1);
        assert_eq!(snap2.version(), 2);
        assert_eq!(snap3.version(), 3);
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use std::time::Instant;

    #[test]
    fn bench_csr_traversal_10k() {
        let mut store = MutableCsrStore::new();
        let n = 10_000;
        let e = 50_000;
        // Add vertices
        for i in 0..n {
            store.add_vertex("Person", &[("name", PropValue::Str(format!("p{}", i)))]);
        }
        // Add edges
        for i in 0..e {
            let src = (i % n) as u32;
            let dst = ((i * 7 + 3) % n) as u32;
            if src != dst {
                store.add_edge(src, dst, "KNOWS", &[]);
            }
        }
        store.commit();

        // Benchmark: 1-hop traversal via CSR
        let iters = 10_000;
        let start = Instant::now();
        let mut total_neighbors = 0usize;
        for i in 0..iters {
            let vid = (i % n) as u32;
            let neighbors = store.out_neighbors(vid);
            total_neighbors += neighbors.len();
        }
        let elapsed = start.elapsed();
        println!("\n=== BENCH: CSR 1-hop traversal (10K nodes, 50K edges) ===");
        println!("Iters: {}, Total neighbors: {}", iters, total_neighbors);
        println!("Total: {:?}", elapsed);
        println!("Per lookup: {:?}", elapsed / iters as u32);
        println!("Lookups/sec: {:.0}", iters as f64 / elapsed.as_secs_f64());
    }

    #[test]
    fn bench_csr_scan_by_label() {
        let mut store = MutableCsrStore::new();
        for i in 0..10_000 {
            let label = if i % 3 == 0 { "Person" } else { "Company" };
            store.add_vertex(label, &[("name", PropValue::Str(format!("v{}", i)))]);
        }
        store.commit();

        let iters = 100;
        let start = Instant::now();
        let mut total = 0;
        for _ in 0..iters {
            total += store.scan_vertices_by_label("Person").len();
        }
        let elapsed = start.elapsed();
        println!("\n=== BENCH: Label scan (10K vertices, ~3.3K Person) ===");
        println!("Iters: {}, Total found: {}", iters, total);
        println!("Total: {:?}", elapsed);
        println!("Per scan: {:?}", elapsed / iters as u32);
        println!("Scans/sec: {:.0}", iters as f64 / elapsed.as_secs_f64());
    }

    #[test]
    fn bench_property_lookup() {
        let mut store = MutableCsrStore::new();
        for i in 0..10_000 {
            store.add_vertex(
                "Person",
                &[
                    ("name", PropValue::Str(format!("Person{}", i))),
                    ("age", PropValue::Int(20 + (i as i64 % 50))),
                ],
            );
        }
        store.commit();

        let iters = 100_000;
        let start = Instant::now();
        let mut found = 0;
        for i in 0..iters {
            let vid = (i % 10_000) as u32;
            if store.vertex_prop(vid, "name").is_some() {
                found += 1;
            }
        }
        let elapsed = start.elapsed();
        println!("\n=== BENCH: Property lookup (10K vertices) ===");
        println!("Iters: {}, Found: {}", iters, found);
        println!("Total: {:?}", elapsed);
        println!("Per lookup: {:?}", elapsed / iters as u32);
        println!("Lookups/sec: {:.0}", iters as f64 / elapsed.as_secs_f64());
    }

    #[test]
    fn bench_write_throughput() {
        let start = Instant::now();
        let mut store = MutableCsrStore::new();
        let n = 100_000;
        for i in 0..n {
            store.add_vertex("Person", &[("name", PropValue::Str(format!("p{}", i)))]);
        }
        let vertex_elapsed = start.elapsed();

        let edge_start = Instant::now();
        let e = 500_000;
        for i in 0..e {
            let src = (i % n) as u32;
            let dst = ((i * 7 + 3) % n) as u32;
            if src != dst {
                store.add_edge(src, dst, "KNOWS", &[]);
            }
        }
        let edge_elapsed = edge_start.elapsed();

        let commit_start = Instant::now();
        store.commit();
        let commit_elapsed = commit_start.elapsed();

        println!("\n=== BENCH: Write throughput ===");
        println!(
            "Add {} vertices: {:?} ({:.0} vertices/sec)",
            n,
            vertex_elapsed,
            n as f64 / vertex_elapsed.as_secs_f64()
        );
        println!(
            "Add {} edges: {:?} ({:.0} edges/sec)",
            e,
            edge_elapsed,
            e as f64 / edge_elapsed.as_secs_f64()
        );
        println!("Commit (CSR rebuild): {:?}", commit_elapsed);
    }
}

#[cfg(test)]
mod index_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_label_index_scan() {
        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.commit();

        let persons = store.scan_vertices_by_label("Person");
        assert_eq!(persons.len(), 2);
        assert!(persons.contains(&0));
        assert!(persons.contains(&1));

        let companies = store.scan_vertices_by_label("Company");
        assert_eq!(companies.len(), 1);
        assert!(companies.contains(&2));

        let unknown = store.scan_vertices_by_label("Unknown");
        assert!(unknown.is_empty());
    }

    #[test]
    fn test_label_index_after_delete() {
        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Charlie".into()))]);
        store.commit();

        assert_eq!(store.scan_vertices_by_label("Person").len(), 3);

        // Delete Bob (vid=1)
        store.delete_vertex(1);
        store.commit();

        let persons = store.scan_vertices_by_label("Person");
        assert_eq!(persons.len(), 2);
        assert!(persons.contains(&0)); // Alice
        assert!(!persons.contains(&1)); // Bob deleted
        assert!(persons.contains(&2)); // Charlie
    }

    #[test]
    fn test_prop_eq_index() {
        let mut store = MutableCsrStore::new();
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Bob".into())),
                ("age", PropValue::Int(25)),
            ],
        );
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Charlie".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        store.create_index("Person", "name");
        store.create_index("Person", "age");
        store.commit();

        // Eq lookup via index
        let result = store.scan_vertices(
            "Person",
            &Predicate::Eq("name".into(), PropValue::Str("Bob".into())),
        );
        assert_eq!(result, vec![1]);

        // Eq lookup for age=30 via index
        let result =
            store.scan_vertices("Person", &Predicate::Eq("age".into(), PropValue::Int(30)));
        assert_eq!(result.len(), 2);
        assert!(result.contains(&0));
        assert!(result.contains(&2));

        // Non-existent value
        let result = store.scan_vertices(
            "Person",
            &Predicate::Eq("name".into(), PropValue::Str("Nobody".into())),
        );
        assert!(result.is_empty());
    }

    #[test]
    fn test_prop_index_after_mutation() {
        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.create_index("Person", "name");
        store.commit();

        let result = store.scan_vertices(
            "Person",
            &Predicate::Eq("name".into(), PropValue::Str("Alice".into())),
        );
        assert_eq!(result, vec![0]);

        // Add more vertices and recommit
        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.commit();

        // Index should now include new vertices
        let result = store.scan_vertices(
            "Person",
            &Predicate::Eq("name".into(), PropValue::Str("Alice".into())),
        );
        assert_eq!(result.len(), 2);
        assert!(result.contains(&0));
        assert!(result.contains(&2));

        let result = store.scan_vertices(
            "Person",
            &Predicate::Eq("name".into(), PropValue::Str("Bob".into())),
        );
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_scan_uses_label_index() {
        let mut store = MutableCsrStore::new();
        let n = 10_000;
        for i in 0..n {
            let label = if i % 10 == 0 { "Target" } else { "Other" };
            store.add_vertex(label, &[("id", PropValue::Int(i))]);
        }
        store.commit();

        // Measure indexed scan
        let start = Instant::now();
        let iters = 1_000;
        let mut total = 0;
        for _ in 0..iters {
            total += store.scan_vertices_by_label("Target").len();
        }
        let indexed_elapsed = start.elapsed();

        // Verify correctness
        assert_eq!(total / iters, 1_000); // 10K / 10 = 1K Target vertices

        println!(
            "\n=== INDEX BENCH: Label index scan ({} vertices, {} iters) ===",
            n, iters
        );
        println!(
            "Total: {:?}, Per scan: {:?}",
            indexed_elapsed,
            indexed_elapsed / iters as u32
        );
    }

    // ── Edge property tests ─────────────────────────────────────────

    #[test]
    fn test_edge_properties() {
        let mut s = MutableCsrStore::new();
        let a = s.add_vertex("N", &[("name".into(), PropValue::Str("A".into()))]);
        let b = s.add_vertex("N", &[("name".into(), PropValue::Str("B".into()))]);
        s.add_edge(a, b, "REL", &[("weight".into(), PropValue::Float(0.75))]);
        s.commit();
        // Edge property access
        let prop = s.edge_prop(0, "weight");
        assert_eq!(prop, Some(PropValue::Float(0.75)));
        assert_eq!(s.edge_prop(0, "nonexistent"), None);
    }

    // ── Delete vertex/edge ──────────────────────────────────────────

    #[test]
    fn test_delete_vertex_makes_invisible() {
        let mut s = MutableCsrStore::new();
        s.add_vertex("X", &[("v".into(), PropValue::Int(1))]);
        s.add_vertex("X", &[("v".into(), PropValue::Int(2))]);
        s.commit();
        assert_eq!(s.scan_vertices_by_label("X").len(), 2);
        s.delete_vertex(0);
        s.commit();
        assert_eq!(s.scan_vertices_by_label("X").len(), 1);
    }

    #[test]
    fn test_delete_edge_makes_invisible() {
        let mut s = MutableCsrStore::new();
        let a = s.add_vertex("N", &[]);
        let b = s.add_vertex("N", &[]);
        s.add_edge(a, b, "E", &[]);
        s.commit();
        assert_eq!(s.edge_count(), 1);
        s.delete_edge(0);
        s.commit();
        assert_eq!(s.edge_count(), 0);
    }

    // ── Set property ────────────────────────────────────────────────

    #[test]
    fn test_set_vertex_prop() {
        let mut s = MutableCsrStore::new();
        s.add_vertex("N", &[("x".into(), PropValue::Int(1))]);
        s.commit();
        assert_eq!(s.vertex_prop(0, "x"), Some(PropValue::Int(1)));
        s.set_vertex_prop(0, "x", PropValue::Int(99));
        assert_eq!(s.vertex_prop(0, "x"), Some(PropValue::Int(99)));
    }

    #[test]
    fn test_set_vertex_prop_new_key() {
        let mut s = MutableCsrStore::new();
        s.add_vertex("N", &[]);
        s.commit();
        assert_eq!(s.vertex_prop(0, "y"), None);
        s.set_vertex_prop(0, "y", PropValue::Str("hello".into()));
        assert_eq!(s.vertex_prop(0, "y"), Some(PropValue::Str("hello".into())));
    }

    // ── Multi-label vertex ──────────────────────────────────────────

    #[test]
    fn test_add_vertex_with_multiple_labels() {
        let mut s = MutableCsrStore::new();
        s.add_vertex_with_labels(
            &["Person".into(), "Employee".into()],
            &[("name".into(), PropValue::Str("Alice".into()))],
        );
        s.commit();
        assert_eq!(s.scan_vertices_by_label("Person").len(), 1);
        assert_eq!(s.scan_vertices_by_label("Employee").len(), 1);
        assert_eq!(Property::vertex_labels(&s, 0), vec!["Person", "Employee"]);
    }

    // ── vertex_all_props ────────────────────────────────────────────

    #[test]
    fn test_vertex_all_props() {
        let mut s = MutableCsrStore::new();
        s.add_vertex(
            "N",
            &[
                ("a".into(), PropValue::Int(1)),
                ("b".into(), PropValue::Str("x".into())),
            ],
        );
        s.commit();
        let props = s.vertex_all_props(0);
        assert!(props.len() >= 2); // a, b (+ possibly _vid)
        assert_eq!(props.get("a"), Some(&PropValue::Int(1)));
        assert_eq!(props.get("b"), Some(&PropValue::Str("x".into())));
    }

    #[test]
    fn test_vertex_all_props_deleted_vertex() {
        let mut s = MutableCsrStore::new();
        s.add_vertex("N", &[("x".into(), PropValue::Int(1))]);
        s.commit();
        s.delete_vertex(0);
        s.commit();
        let props = s.vertex_all_props(0);
        assert!(props.is_empty(), "deleted vertex should have no props");
    }

    // ── to_filtered_memory_graph ────────────────────────────────────

    #[test]
    fn test_filtered_memory_graph_all() {
        let mut s = MutableCsrStore::new();
        s.add_vertex("A", &[("v".into(), PropValue::Int(1))]);
        s.add_vertex("B", &[("v".into(), PropValue::Int(2))]);
        s.commit();
        let g = s.to_filtered_memory_graph(&[], &[]);
        assert_eq!(g.nodes().len(), 2, "empty filter = all vertices");
    }

    #[test]
    fn test_filtered_memory_graph_label_filter() {
        let mut s = MutableCsrStore::new();
        s.add_vertex("A", &[("v".into(), PropValue::Int(1))]);
        s.add_vertex("B", &[("v".into(), PropValue::Int(2))]);
        s.add_vertex("A", &[("v".into(), PropValue::Int(3))]);
        s.commit();
        let g = s.to_filtered_memory_graph(&["A".into()], &[]);
        assert_eq!(
            g.nodes().len(),
            2,
            "label filter should return only A vertices"
        );
    }

    // ── Clone ───────────────────────────────────────────────────────

    #[test]
    fn test_store_clone_independence() {
        let mut s = MutableCsrStore::new();
        s.add_vertex("N", &[("x".into(), PropValue::Int(1))]);
        s.commit();
        let mut s2 = s.clone();
        s2.add_vertex("N", &[("x".into(), PropValue::Int(2))]);
        s2.commit();
        assert_eq!(
            s.scan_vertices_by_label("N").len(),
            1,
            "original unaffected by clone mutation"
        );
        assert_eq!(s2.scan_vertices_by_label("N").len(), 2);
    }

    // ── GlobalToLocalMap tests ──────────────────────────────────────

    #[test]
    fn test_global_to_local_map_basic() {
        let mut map = GlobalToLocalMap::new();
        let gv = yata_core::GlobalVid::encode(
            yata_core::PartitionId::new(0),
            yata_core::LocalVid::new(42),
        );
        map.insert(gv, 0);
        assert_eq!(map.to_local(gv), Some(0));
        assert_eq!(map.to_global(0), Some(gv));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_global_to_local_map_missing() {
        let map = GlobalToLocalMap::new();
        let gv = yata_core::GlobalVid::new(999);
        assert_eq!(map.to_local(gv), None);
        assert_eq!(map.to_global(0), None);
        assert!(map.is_empty());
    }

    #[test]
    fn test_add_vertex_with_global_id() {
        let mut store = MutableCsrStore::new();
        let gv = yata_core::GlobalVid::encode(
            yata_core::PartitionId::new(1),
            yata_core::LocalVid::new(100),
        );
        let local = store.add_vertex_with_global_id(
            gv,
            "Person",
            &[("name", PropValue::Str("Alice".into()))],
        );
        assert_eq!(local, 0);
        assert_eq!(store.lookup_local_vid(gv), Some(0));
        assert_eq!(store.lookup_global_vid(0), Some(gv));
        assert!(store.has_vertex(0));
        assert_eq!(
            store.vertex_prop(0, "name"),
            Some(PropValue::Str("Alice".into()))
        );
    }

    #[test]
    fn test_global_map_survives_clone() {
        let mut store = MutableCsrStore::new();
        let gv = yata_core::GlobalVid::from_local(0);
        store.add_vertex_with_global_id(gv, "X", &[]);
        let clone = store.clone();
        assert_eq!(clone.lookup_local_vid(gv), Some(0));
        assert_eq!(clone.global_map().len(), 1);
    }

    // ── B-tree index tests ──────────────────────────────────────────

    #[test]
    fn test_btree_range_query() {
        let mut store = MutableCsrStore::new();
        store.register_btree_index("Person", "age");
        for age in [20, 25, 30, 35, 40, 45] {
            store.add_vertex("Person", &[("age", PropValue::Int(age))]);
        }
        store.commit();

        // Range: 25 <= age <= 35
        let result = store.btree_range(
            "Person",
            "age",
            Some(&PropValue::Int(25)),
            Some(&PropValue::Int(35)),
        );
        assert_eq!(result.len(), 3);
        // VIDs 1,2,3 have ages 25,30,35
        assert!(result.contains(&1));
        assert!(result.contains(&2));
        assert!(result.contains(&3));
    }

    #[test]
    fn test_btree_range_unbounded() {
        let mut store = MutableCsrStore::new();
        store.register_btree_index("P", "val");
        store.add_vertex("P", &[("val", PropValue::Int(10))]);
        store.add_vertex("P", &[("val", PropValue::Int(20))]);
        store.add_vertex("P", &[("val", PropValue::Int(30))]);
        store.commit();

        // val >= 20 (no upper bound)
        let result = store.btree_range("P", "val", Some(&PropValue::Int(20)), None);
        assert_eq!(result.len(), 2); // 20, 30

        // val <= 20 (no lower bound)
        let result = store.btree_range("P", "val", None, Some(&PropValue::Int(20)));
        assert_eq!(result.len(), 2); // 10, 20

        // All values (both unbounded)
        let result = store.btree_range("P", "val", None, None);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_btree_order_by_ascending() {
        let mut store = MutableCsrStore::new();
        store.register_btree_index("P", "score");
        store.add_vertex("P", &[("score", PropValue::Int(30))]);
        store.add_vertex("P", &[("score", PropValue::Int(10))]);
        store.add_vertex("P", &[("score", PropValue::Int(20))]);
        store.commit();

        let result = store.btree_order_by("P", "score", true, 10);
        assert_eq!(result, vec![1, 2, 0]); // 10, 20, 30
    }

    #[test]
    fn test_btree_order_by_descending() {
        let mut store = MutableCsrStore::new();
        store.register_btree_index("P", "score");
        store.add_vertex("P", &[("score", PropValue::Int(30))]);
        store.add_vertex("P", &[("score", PropValue::Int(10))]);
        store.add_vertex("P", &[("score", PropValue::Int(20))]);
        store.commit();

        let result = store.btree_order_by("P", "score", false, 2);
        assert_eq!(result, vec![0, 2]); // top-2: 30, 20
    }

    #[test]
    fn test_btree_order_by_with_limit() {
        let mut store = MutableCsrStore::new();
        store.register_btree_index("P", "val");
        for i in 0..100 {
            store.add_vertex("P", &[("val", PropValue::Int(i))]);
        }
        store.commit();

        let result = store.btree_order_by("P", "val", true, 5);
        assert_eq!(result.len(), 5);
        assert_eq!(result, vec![0, 1, 2, 3, 4]);

        let result = store.btree_order_by("P", "val", false, 3);
        assert_eq!(result.len(), 3);
        assert_eq!(result, vec![99, 98, 97]);
    }

    #[test]
    fn test_btree_string_range() {
        let mut store = MutableCsrStore::new();
        store.register_btree_index("P", "name");
        store.add_vertex("P", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("P", &[("name", PropValue::Str("Bob".into()))]);
        store.add_vertex("P", &[("name", PropValue::Str("Carol".into()))]);
        store.add_vertex("P", &[("name", PropValue::Str("Dave".into()))]);
        store.commit();

        // "Bob" <= name <= "Dave"
        let result = store.btree_range(
            "P",
            "name",
            Some(&PropValue::Str("Bob".into())),
            Some(&PropValue::Str("Dave".into())),
        );
        assert_eq!(result.len(), 3); // Bob, Carol, Dave
    }

    #[test]
    fn test_btree_unregistered_returns_empty() {
        let store = MutableCsrStore::new();
        let result = store.btree_range("P", "val", None, None);
        assert!(result.is_empty());

        let result = store.btree_order_by("P", "val", true, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn test_btree_respects_deleted_vertices() {
        let mut store = MutableCsrStore::new();
        store.register_btree_index("P", "val");
        store.add_vertex("P", &[("val", PropValue::Int(1))]);
        store.add_vertex("P", &[("val", PropValue::Int(2))]);
        store.add_vertex("P", &[("val", PropValue::Int(3))]);
        store.delete_vertex(1); // delete vid=1 (val=2)
        store.commit();

        let result = store.btree_range("P", "val", None, None);
        assert_eq!(result.len(), 2);
        assert!(!result.contains(&1));
    }
}
