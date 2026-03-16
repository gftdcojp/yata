//! MutableCSR: GraphScope Flex-inspired mutable compressed sparse row graph store.
//!
//! Design:
//! - Vertices: dense u32 VID array, label list, alive bitvec
//! - Edges: per-label CSR segments (offsets + edge_ids packed array)
//! - Properties: per-vertex/edge HashMap storage
//! - Mutations: append-only within current version, CSR rebuilt on commit

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use serde::{Deserialize, Serialize};
use yata_grin::*;
use yata_cypher::Graph;

// ── WAL (Write-Ahead Log) for graph mutations ────────────────────────

/// WAL entry for graph mutations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    AddVertex { vid: u32, label: String, props: Vec<(String, PropValue)> },
    AddEdge { edge_id: u32, src: u32, dst: u32, label: String, props: Vec<(String, PropValue)> },
    DeleteVertex { vid: u32 },
    DeleteEdge { edge_id: u32 },
    SetProperty { vid: u32, key: String, value: PropValue },
    Commit { version: u64 },
}

/// In-memory WAL for graph mutations. Supports serialization for future file persistence.
pub struct GraphWal {
    entries: Vec<WalEntry>,
}

impl GraphWal {
    pub fn new() -> Self { Self { entries: Vec::new() } }

    pub fn append(&mut self, entry: WalEntry) {
        self.entries.push(entry);
    }

    pub fn entries(&self) -> &[WalEntry] { &self.entries }

    pub fn clear(&mut self) { self.entries.clear(); }

    /// Replay WAL entries into a MutableCsrStore to recover state.
    pub fn replay(&self, store: &mut MutableCsrStore) {
        for entry in &self.entries {
            match entry {
                WalEntry::AddVertex { label, props, .. } => {
                    let p: Vec<(&str, PropValue)> = props.iter()
                        .map(|(k, v)| (k.as_str(), v.clone())).collect();
                    store.add_vertex(label, &p);
                }
                WalEntry::AddEdge { src, dst, label, props, .. } => {
                    let p: Vec<(&str, PropValue)> = props.iter()
                        .map(|(k, v)| (k.as_str(), v.clone())).collect();
                    store.add_edge(*src, *dst, label, &p);
                }
                WalEntry::DeleteVertex { vid } => { store.delete_vertex(*vid); }
                WalEntry::DeleteEdge { edge_id } => { store.delete_edge(*edge_id); }
                WalEntry::SetProperty { vid, key, value } => {
                    store.set_vertex_prop(*vid, key, value.clone());
                }
                WalEntry::Commit { .. } => { store.commit(); }
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
        let entries: Vec<WalEntry> = text.lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();
        Self { entries }
    }
}

impl Default for GraphWal {
    fn default() -> Self { Self::new() }
}

// ── MVCC Snapshot ────────────────────────────────────────────────────

/// Read-only snapshot of the graph at a specific version.
pub struct GraphSnapshot {
    version: u64,
    store: MutableCsrStore,
}

impl GraphSnapshot {
    pub fn version(&self) -> u64 { self.version }
    pub fn store(&self) -> &MutableCsrStore { &self.store }
}

/// Per-label compressed sparse row segment.
#[derive(Clone)]
struct CsrSegment {
    /// offsets[vid] = start index in edge_ids array. len = max_vid + 2.
    offsets: Vec<u32>,
    /// Packed edge IDs grouped by source (out) or destination (in) vertex.
    edge_ids: Vec<u32>,
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

/// MutableCSR graph store implementing all GRIN traits.
pub struct MutableCsrStore {
    // Vertex storage
    vertex_labels: Vec<Vec<String>>,
    vertex_alive: Vec<bool>,
    vertex_props_map: Vec<HashMap<String, PropValue>>,
    vertex_count: u32,
    vid_alloc: AtomicU32,

    // Edge storage: label -> CsrSegment
    out_csr: HashMap<String, CsrSegment>,
    in_csr: HashMap<String, CsrSegment>,

    // Edge metadata indexed by edge_id
    edge_props: Vec<HashMap<String, PropValue>>,
    edge_labels: Vec<String>,
    edge_src: Vec<u32>,
    edge_dst: Vec<u32>,
    edge_alive: Vec<bool>,
    edge_count: u32,

    // Pending mutations (not yet in CSR)
    pending_edges: Vec<PendingEdge>,

    // Version
    version: AtomicU64,

    // Schema
    known_vertex_labels: Vec<String>,
    known_edge_labels: Vec<String>,
    vertex_pk: HashMap<String, String>,

    // Partition
    partition_id: u32,

    // WAL (optional, enabled via enable_wal())
    wal: Option<GraphWal>,
}

impl MutableCsrStore {
    /// Create an empty store (partition 0).
    pub fn new() -> Self {
        Self::new_partition(0)
    }

    /// Create an empty store for a specific partition.
    pub fn new_partition(partition_id: u32) -> Self {
        Self {
            vertex_labels: Vec::new(),
            vertex_alive: Vec::new(),
            vertex_props_map: Vec::new(),
            vertex_count: 0,
            vid_alloc: AtomicU32::new(0),
            out_csr: HashMap::new(),
            in_csr: HashMap::new(),
            edge_props: Vec::new(),
            edge_labels: Vec::new(),
            edge_src: Vec::new(),
            edge_dst: Vec::new(),
            edge_alive: Vec::new(),
            edge_count: 0,
            pending_edges: Vec::new(),
            version: AtomicU64::new(0),
            known_vertex_labels: Vec::new(),
            known_edge_labels: Vec::new(),
            vertex_pk: HashMap::new(),
            partition_id,
            wal: None,
        }
    }

    /// Set the primary key column for a vertex label.
    pub fn set_vertex_primary_key(&mut self, label: &str, key: &str) {
        self.vertex_pk.insert(label.to_string(), key.to_string());
    }

    /// Current snapshot version.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    /// Rebuild all CSR segments from the full edge list.
    fn rebuild_csr(&mut self) {
        self.out_csr.clear();
        self.in_csr.clear();

        let max_vid = self.vertex_count;
        if max_vid == 0 {
            self.pending_edges.clear();
            return;
        }

        // Group edges by label
        let mut edges_by_label: HashMap<String, Vec<(u32, u32, u32)>> = HashMap::new();
        for eid in 0..self.edge_count as usize {
            if eid >= self.edge_labels.len() {
                continue;
            }
            if !self.edge_alive[eid] {
                continue;
            }
            let label = &self.edge_labels[eid];
            let src = self.edge_src[eid];
            let dst = self.edge_dst[eid];
            edges_by_label
                .entry(label.clone())
                .or_default()
                .push((src, dst, eid as u32));
        }

        for (label, edges) in &edges_by_label {
            // Build outgoing CSR
            let mut out_offsets = vec![0u32; max_vid as usize + 2];
            for &(src, _, _) in edges {
                if (src as usize + 1) < out_offsets.len() {
                    out_offsets[src as usize + 1] += 1;
                }
            }
            for i in 1..out_offsets.len() {
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
                label.clone(),
                CsrSegment {
                    offsets: out_offsets,
                    edge_ids: out_edge_ids,
                },
            );

            // Build incoming CSR
            let mut in_offsets = vec![0u32; max_vid as usize + 2];
            for &(_, dst, _) in edges {
                if (dst as usize + 1) < in_offsets.len() {
                    in_offsets[dst as usize + 1] += 1;
                }
            }
            for i in 1..in_offsets.len() {
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
                label.clone(),
                CsrSegment {
                    offsets: in_offsets,
                    edge_ids: in_edge_ids,
                },
            );
        }

        self.pending_edges.clear();
    }

    /// Check if a predicate matches a vertex's properties.
    fn predicate_matches(&self, vid: u32, predicate: &Predicate) -> bool {
        match predicate {
            Predicate::True => true,
            Predicate::Eq(key, val) => {
                self.vertex_prop_value(vid, key)
                    .map_or(false, |v| &v == val)
            }
            Predicate::Neq(key, val) => {
                self.vertex_prop_value(vid, key)
                    .map_or(true, |v| &v != val)
            }
            Predicate::Lt(key, val) => {
                self.vertex_prop_value(vid, key)
                    .map_or(false, |v| prop_cmp(&v, val) == Some(std::cmp::Ordering::Less))
            }
            Predicate::Gt(key, val) => {
                self.vertex_prop_value(vid, key).map_or(false, |v| {
                    prop_cmp(&v, val) == Some(std::cmp::Ordering::Greater)
                })
            }
            Predicate::In(key, vals) => {
                self.vertex_prop_value(vid, key)
                    .map_or(false, |v| vals.contains(&v))
            }
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
            Predicate::Or(a, b) => {
                self.predicate_matches(vid, a) || self.predicate_matches(vid, b)
            }
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

    // ── WAL methods ──────────────────────────────────────────────────

    /// Enable WAL tracking. After this, all mutations are recorded.
    pub fn enable_wal(&mut self) {
        self.wal = Some(GraphWal::new());
    }

    /// Get WAL reference (if enabled).
    pub fn wal(&self) -> Option<&GraphWal> { self.wal.as_ref() }

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
            vertex_count: self.vertex_count,
            vid_alloc: AtomicU32::new(self.vid_alloc.load(Ordering::Relaxed)),
            out_csr: self.out_csr.clone(),
            in_csr: self.in_csr.clone(),
            edge_props: self.edge_props.clone(),
            edge_labels: self.edge_labels.clone(),
            edge_src: self.edge_src.clone(),
            edge_dst: self.edge_dst.clone(),
            edge_alive: self.edge_alive.clone(),
            edge_count: self.edge_count,
            pending_edges: self.pending_edges.clone(),
            version: AtomicU64::new(self.version.load(Ordering::Relaxed)),
            known_vertex_labels: self.known_vertex_labels.clone(),
            known_edge_labels: self.known_edge_labels.clone(),
            vertex_pk: self.vertex_pk.clone(),
            partition_id: self.partition_id,
            wal: None, // WAL is not cloned into snapshots
        }
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
        let vid = self.vid_alloc.fetch_add(1, Ordering::Relaxed);
        let vid_usize = vid as usize;
        while self.vertex_labels.len() <= vid_usize {
            self.vertex_labels.push(Vec::new());
            self.vertex_alive.push(false);
            self.vertex_props_map.push(HashMap::new());
        }
        self.vertex_labels[vid_usize] = labels.to_vec();
        self.vertex_alive[vid_usize] = true;
        let mut prop_map = HashMap::new();
        for &(k, ref v) in props {
            prop_map.insert(k.to_string(), v.clone());
        }
        self.vertex_props_map[vid_usize] = prop_map;
        self.vertex_count = self.vid_alloc.load(Ordering::Relaxed);
        for l in labels {
            self.register_vertex_label(l);
        }
        vid
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
            let props = self.vertex_props_map.get(vid)
                .map(|m| m.iter().map(|(k, v)| (k.clone(), prop_to_cypher(v))).collect())
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
            let props = self.edge_props.get(eid)
                .map(|m| m.iter().map(|(k, v)| (k.clone(), prop_to_cypher(v))).collect())
                .unwrap_or_default();
            let id = format!("e{}", eid);
            g.add_rel(yata_cypher::RelRef { id, rel_type, src, dst, props });
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
            if !self.vertex_alive[vid] { continue; }
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
            let vlabels = self.vertex_labels.get(vid as usize).cloned().unwrap_or_default();
            let props = self.vertex_props_map.get(vid as usize)
                .map(|m| m.iter().map(|(k, v)| (k.clone(), prop_to_cypher(v))).collect())
                .unwrap_or_default();
            g.add_node(yata_cypher::NodeRef { id, labels: vlabels, props });
            added_vids.insert(vid);
        }

        // Step 3: Collect edges incident to seed vertices, optionally filtered by rel_types
        let mut neighbor_vids = std::collections::HashSet::new();
        for eid in 0..self.edge_alive.len() {
            if !self.edge_alive[eid] { continue; }
            let src = self.edge_src[eid];
            let dst = self.edge_dst[eid];
            let touches_seed = added_vids.contains(&src) || added_vids.contains(&dst);
            if !touches_seed { continue; }
            if !rel_types.is_empty() && !rel_types.contains(&self.edge_labels[eid]) { continue; }

            let src_id = self.vertex_string_id(src);
            let dst_id = self.vertex_string_id(dst);
            let rel_type = self.edge_labels[eid].clone();
            let props = self.edge_props.get(eid)
                .map(|m| m.iter().map(|(k, v)| (k.clone(), prop_to_cypher(v))).collect())
                .unwrap_or_default();
            g.add_rel(yata_cypher::RelRef {
                id: format!("e{}", eid), rel_type, src: src_id, dst: dst_id, props,
            });
            if !added_vids.contains(&src) { neighbor_vids.insert(src); }
            if !added_vids.contains(&dst) { neighbor_vids.insert(dst); }
        }

        // Step 4: Add 1-hop neighbor vertices
        for &vid in &neighbor_vids {
            let id = self.vertex_string_id(vid);
            let vlabels = self.vertex_labels.get(vid as usize).cloned().unwrap_or_default();
            let props = self.vertex_props_map.get(vid as usize)
                .map(|m| m.iter().map(|(k, v)| (k.clone(), prop_to_cypher(v))).collect())
                .unwrap_or_default();
            g.add_node(yata_cypher::NodeRef { id, labels: vlabels, props });
        }

        g.build_csr();
        g
    }

    /// Get the string ID for a vertex. Uses the first property that looks like
    /// a primary key (vid/id/eid), falling back to "v{vid}".
    fn vertex_string_id(&self, vid: u32) -> String {
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
        let mut result = Vec::new();
        for (vid, labels) in self.vertex_labels.iter().enumerate() {
            if !self.vertex_alive[vid] {
                continue;
            }
            if !labels.contains(&label.to_string()) {
                continue;
            }
            if self.predicate_matches(vid as u32, predicate) {
                result.push(vid as u32);
            }
        }
        result
    }

    fn scan_vertices_by_label(&self, label: &str) -> Vec<u32> {
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
        let vid = self.vid_alloc.fetch_add(1, Ordering::Relaxed);
        let vid_usize = vid as usize;

        // Extend storage if needed
        while self.vertex_labels.len() <= vid_usize {
            self.vertex_labels.push(Vec::new());
            self.vertex_alive.push(false);
            self.vertex_props_map.push(HashMap::new());
        }

        self.vertex_labels[vid_usize] = vec![label.to_string()];
        self.vertex_alive[vid_usize] = true;

        let mut prop_map = HashMap::new();
        for &(k, ref v) in props {
            prop_map.insert(k.to_string(), v.clone());
        }
        self.vertex_props_map[vid_usize] = prop_map;

        self.vertex_count = self.vid_alloc.load(Ordering::Relaxed);
        self.register_vertex_label(label);

        // WAL
        if let Some(ref mut wal) = self.wal {
            wal.append(WalEntry::AddVertex {
                vid,
                label: label.to_string(),
                props: props.iter().map(|(k, v)| (k.to_string(), v.clone())).collect(),
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
        self.edge_alive.push(true);

        self.pending_edges.push(PendingEdge {
            src,
            dst,
            label: label.to_string(),
            edge_id: eid,
        });

        self.register_edge_label(label);

        // WAL
        if let Some(ref mut wal) = self.wal {
            wal.append(WalEntry::AddEdge {
                edge_id: eid,
                src,
                dst,
                label: label.to_string(),
                props: props.iter().map(|(k, v)| (k.to_string(), v.clone())).collect(),
            });
        }

        eid
    }

    fn set_vertex_prop(&mut self, vid: u32, key: &str, value: PropValue) {
        if let Some(props) = self.vertex_props_map.get_mut(vid as usize) {
            props.insert(key.to_string(), value.clone());
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
        if let Some(alive) = self.vertex_alive.get_mut(vid as usize) {
            *alive = false;
        }
        // Soft-delete all incident edges
        for eid in 0..self.edge_count as usize {
            if self.edge_src[eid] == vid || self.edge_dst[eid] == vid {
                self.edge_alive[eid] = false;
            }
        }
        // WAL
        if let Some(ref mut wal) = self.wal {
            wal.append(WalEntry::DeleteVertex { vid });
        }
    }

    fn delete_edge(&mut self, edge_id: u32) {
        if let Some(alive) = self.edge_alive.get_mut(edge_id as usize) {
            *alive = false;
        }
        // WAL
        if let Some(ref mut wal) = self.wal {
            wal.append(WalEntry::DeleteEdge { edge_id });
        }
    }

    fn commit(&mut self) -> u64 {
        self.rebuild_csr();
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
        self.partition_id
    }

    fn partition_count(&self) -> u32 {
        1 // single-store: overridden by coordinator
    }

    fn vertex_partition(&self, _vid: u32) -> u32 {
        self.partition_id
    }

    fn is_master(&self, _vid: u32) -> bool {
        true // single-store: all vertices are master
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_store() {
        let store = MutableCsrStore::new();
        assert_eq!(store.vertex_count(), 0);
        assert_eq!(store.edge_count(), 0);
        assert!(!store.has_vertex(0));
        assert_eq!(store.version(), 0);
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
    fn test_add_edge_and_traverse() {
        let mut store = MutableCsrStore::new();
        let alice = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        let bob = store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        let eid = store.add_edge(
            alice,
            bob,
            "KNOWS",
            &[("since", PropValue::Int(2020))],
        );

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
        assert_eq!(
            store.edge_prop(eid, "since"),
            Some(PropValue::Int(2020))
        );
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
        let result = store.scan_vertices("Person", &Predicate::Gt("age".into(), PropValue::Int(28)));
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
            let v = store.add_vertex(
                "Spoke",
                &[("idx", PropValue::Int(i))],
            );
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
        assert_eq!(
            store.vertex_primary_key("Person"),
            Some("name".to_string())
        );
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
        let found = store.scan_vertices(
            "Node",
            &Predicate::Eq("id".into(), PropValue::Int(42)),
        );
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

        let result = store.scan_vertices(
            "P",
            &Predicate::Neq("x".into(), PropValue::Int(1)),
        );
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
            &Predicate::In(
                "x".into(),
                vec![PropValue::Int(1), PropValue::Int(3)],
            ),
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
            WalEntry::AddVertex { vid, label, props } => {
                assert_eq!(*vid, 0);
                assert_eq!(label, "Person");
                assert_eq!(props.len(), 1);
            }
            other => panic!("expected AddVertex, got {:?}", other),
        }
        match &wal.entries()[1] {
            WalEntry::AddEdge { edge_id, src, dst, label, props } => {
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

        src_store.add_vertex("Person", &[("name", PropValue::Str("Alice".into())), ("age", PropValue::Int(30))]);
        src_store.add_vertex("Person", &[("name", PropValue::Str("Bob".into())), ("age", PropValue::Int(25))]);
        src_store.add_edge(0, 1, "KNOWS", &[("since", PropValue::Int(2020))]);
        src_store.commit();

        // Take the WAL and replay into a fresh store
        let wal = src_store.take_wal().unwrap();
        let mut dst_store = MutableCsrStore::new();
        wal.replay(&mut dst_store);

        // Verify the replayed store matches
        assert_eq!(dst_store.vertex_count(), 2);
        assert_eq!(dst_store.edge_count(), 1);
        assert_eq!(dst_store.vertex_prop(0, "name"), Some(PropValue::Str("Alice".into())));
        assert_eq!(dst_store.vertex_prop(1, "name"), Some(PropValue::Str("Bob".into())));
        assert_eq!(dst_store.out_degree(0), 1);
    }

    #[test]
    fn test_wal_serialization() {
        let mut wal = GraphWal::new();
        wal.append(WalEntry::AddVertex {
            vid: 0,
            label: "Person".into(),
            props: vec![("name".into(), PropValue::Str("Alice".into()))],
        });
        wal.append(WalEntry::AddEdge {
            edge_id: 0, src: 0, dst: 1,
            label: "KNOWS".into(),
            props: vec![],
        });
        wal.append(WalEntry::DeleteVertex { vid: 42 });
        wal.append(WalEntry::Commit { version: 1 });

        let bytes = wal.to_bytes();
        let restored = GraphWal::from_bytes(&bytes);

        assert_eq!(restored.entries().len(), 4);
        match &restored.entries()[0] {
            WalEntry::AddVertex { vid, label, .. } => {
                assert_eq!(*vid, 0);
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
        assert_eq!(snap.store().vertex_prop(0, "name"), Some(PropValue::Str("Alice".into())));

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
            store.add_vertex("Person", &[
                ("name", PropValue::Str(format!("Person{}", i))),
                ("age", PropValue::Int(20 + (i as i64 % 50))),
            ]);
        }
        store.commit();

        let iters = 100_000;
        let start = Instant::now();
        let mut found = 0;
        for i in 0..iters {
            let vid = (i % 10_000) as u32;
            if store.vertex_prop(vid, "name").is_some() { found += 1; }
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
        println!("Add {} vertices: {:?} ({:.0} vertices/sec)", n, vertex_elapsed, n as f64 / vertex_elapsed.as_secs_f64());
        println!("Add {} edges: {:?} ({:.0} edges/sec)", e, edge_elapsed, e as f64 / edge_elapsed.as_secs_f64());
        println!("Commit (CSR rebuild): {:?}", commit_elapsed);
    }
}
