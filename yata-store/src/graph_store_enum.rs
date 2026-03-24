//! GraphStoreEnum: unified enum for single vs partitioned graph store.
//!
//! Wraps MutableCsrStore (single partition) and PartitionedGraphStore (multi-partition)
//! behind a common API surface. Provides memory budget tracking for OOM protection.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use yata_core::PartitionId;
use yata_grin::*;

use yata_cypher::Graph;

use crate::MutableCsrStore;
use crate::arrow_store::ArrowGraphStore;
use crate::partitioned::PartitionedGraphStore;

/// Memory budget for OOM protection. Tracks estimated bytes used.
pub struct MemoryBudget {
    /// Max allowed bytes (0 = unlimited).
    max_bytes: usize,
    /// Current estimated bytes used.
    used_bytes: AtomicUsize,
    /// Estimated bytes per vertex (props + labels + overhead).
    bytes_per_vertex: usize,
    /// Estimated bytes per edge (src + dst + label + props + CSR entry).
    bytes_per_edge: usize,
}

impl MemoryBudget {
    /// Create a new budget. `max_mb` = 0 means unlimited.
    pub fn new(max_mb: usize) -> Self {
        Self {
            max_bytes: max_mb * 1024 * 1024,
            used_bytes: AtomicUsize::new(0),
            bytes_per_vertex: 200, // ~200 bytes per vertex with 2 props
            bytes_per_edge: 100,   // ~100 bytes per edge
        }
    }

    /// Check if adding `n_vertices` and `n_edges` would exceed budget.
    pub fn can_add(&self, n_vertices: usize, n_edges: usize) -> bool {
        if self.max_bytes == 0 {
            return true;
        }
        let additional = n_vertices * self.bytes_per_vertex + n_edges * self.bytes_per_edge;
        self.used_bytes.load(Ordering::Relaxed) + additional <= self.max_bytes
    }

    /// Record that vertices/edges were added.
    pub fn record_add(&self, n_vertices: usize, n_edges: usize) {
        let additional = n_vertices * self.bytes_per_vertex + n_edges * self.bytes_per_edge;
        self.used_bytes.fetch_add(additional, Ordering::Relaxed);
    }

    /// Record that vertices/edges were removed.
    pub fn record_remove(&self, n_vertices: usize, n_edges: usize) {
        let reduction = n_vertices * self.bytes_per_vertex + n_edges * self.bytes_per_edge;
        self.used_bytes.fetch_sub(
            reduction.min(self.used_bytes.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
    }

    /// Current estimated usage in MB.
    pub fn used_mb(&self) -> f64 {
        self.used_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0)
    }

    /// Max budget in MB (0 = unlimited).
    pub fn max_mb(&self) -> usize {
        self.max_bytes / (1024 * 1024).max(1)
    }

    /// Usage ratio (0.0 - 1.0). Returns 0.0 if unlimited.
    pub fn usage_ratio(&self) -> f64 {
        if self.max_bytes == 0 {
            return 0.0;
        }
        self.used_bytes.load(Ordering::Relaxed) as f64 / self.max_bytes as f64
    }

    /// Sync budget from actual store counts.
    pub fn sync_from_counts(&self, vertex_count: usize, edge_count: usize) {
        let bytes = vertex_count * self.bytes_per_vertex + edge_count * self.bytes_per_edge;
        self.used_bytes.store(bytes, Ordering::Relaxed);
    }
}

impl Default for MemoryBudget {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Unified graph store: single partition or multi-partition.
pub enum GraphStoreEnum {
    Single(MutableCsrStore),
    Partitioned(PartitionedGraphStore),
    /// Vineyard-native Arrow store (replaces CSR for primary query path).
    Arrow(ArrowGraphStore),
}

impl GraphStoreEnum {
    /// Create based on partition count.
    pub fn new(partition_count: u32, hot_partition_id: PartitionId) -> Self {
        if partition_count <= 1 {
            GraphStoreEnum::Single(MutableCsrStore::new_with_partition_id(hot_partition_id))
        } else {
            GraphStoreEnum::Partitioned(PartitionedGraphStore::new(partition_count))
        }
    }

    /// Whether this is partitioned mode.
    pub fn is_partitioned(&self) -> bool {
        matches!(self, GraphStoreEnum::Partitioned(_))
    }

    /// Get as single store (for GIE fast path).
    pub fn as_single(&self) -> Option<&MutableCsrStore> {
        match self {
            GraphStoreEnum::Single(s) => Some(s),
            _ => None,
        }
    }

    /// Get as single store mutably.
    pub fn as_single_mut(&mut self) -> Option<&mut MutableCsrStore> {
        match self {
            GraphStoreEnum::Single(s) => Some(s),
            _ => None,
        }
    }

    /// Get as partitioned store.
    pub fn as_partitioned(&self) -> Option<&PartitionedGraphStore> {
        match self {
            GraphStoreEnum::Partitioned(p) => Some(p),
            _ => None,
        }
    }

    /// Get as partitioned store mutably.
    pub fn as_partitioned_mut(&mut self) -> Option<&mut PartitionedGraphStore> {
        match self {
            GraphStoreEnum::Partitioned(p) => Some(p),
            _ => None,
        }
    }

    /// Create as Arrow store (Vineyard-native).
    pub fn new_arrow(partition_id: PartitionId) -> Self {
        GraphStoreEnum::Arrow(ArrowGraphStore::new(partition_id))
    }

    /// Get as Arrow store.
    pub fn as_arrow(&self) -> Option<&ArrowGraphStore> {
        match self {
            GraphStoreEnum::Arrow(a) => Some(a),
            _ => None,
        }
    }

    /// Get as Arrow store mutably.
    pub fn as_arrow_mut(&mut self) -> Option<&mut ArrowGraphStore> {
        match self {
            GraphStoreEnum::Arrow(a) => Some(a),
            _ => None,
        }
    }

    /// Get partition count.
    pub fn partition_count(&self) -> u32 {
        match self {
            GraphStoreEnum::Single(_) => 1,
            GraphStoreEnum::Partitioned(p) => p.partition_count(),
            GraphStoreEnum::Arrow(_) => 1,
        }
    }
}

// ── GRIN trait delegation ──

impl Topology for GraphStoreEnum {
    fn vertex_count(&self) -> usize {
        match self {
            GraphStoreEnum::Single(s) => s.vertex_count(),
            GraphStoreEnum::Partitioned(p) => p.vertex_count(),
            GraphStoreEnum::Arrow(a) => a.vertex_count(),
        }
    }
    fn edge_count(&self) -> usize {
        match self {
            GraphStoreEnum::Single(s) => s.edge_count(),
            GraphStoreEnum::Partitioned(p) => p.edge_count(),
            GraphStoreEnum::Arrow(a) => a.edge_count(),
        }
    }
    fn has_vertex(&self, vid: u32) -> bool {
        match self {
            GraphStoreEnum::Single(s) => s.has_vertex(vid),
            GraphStoreEnum::Partitioned(p) => p.has_vertex(vid),
            GraphStoreEnum::Arrow(a) => a.has_vertex(vid),
        }
    }
    fn out_degree(&self, vid: u32) -> usize {
        match self {
            GraphStoreEnum::Single(s) => s.out_degree(vid),
            GraphStoreEnum::Partitioned(p) => p.out_degree(vid),
            GraphStoreEnum::Arrow(a) => a.out_degree(vid),
        }
    }
    fn in_degree(&self, vid: u32) -> usize {
        match self {
            GraphStoreEnum::Single(s) => s.in_degree(vid),
            GraphStoreEnum::Partitioned(p) => p.in_degree(vid),
            GraphStoreEnum::Arrow(a) => a.in_degree(vid),
        }
    }
    fn out_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        match self {
            GraphStoreEnum::Single(s) => s.out_neighbors(vid),
            GraphStoreEnum::Partitioned(p) => p.out_neighbors(vid),
            GraphStoreEnum::Arrow(a) => a.out_neighbors(vid),
        }
    }
    fn in_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        match self {
            GraphStoreEnum::Single(s) => s.in_neighbors(vid),
            GraphStoreEnum::Partitioned(p) => p.in_neighbors(vid),
            GraphStoreEnum::Arrow(a) => a.in_neighbors(vid),
        }
    }
    fn out_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        match self {
            GraphStoreEnum::Single(s) => s.out_neighbors_by_label(vid, edge_label),
            GraphStoreEnum::Partitioned(p) => p.out_neighbors_by_label(vid, edge_label),
            GraphStoreEnum::Arrow(a) => a.out_neighbors_by_label(vid, edge_label),
        }
    }
    fn in_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        match self {
            GraphStoreEnum::Single(s) => s.in_neighbors_by_label(vid, edge_label),
            GraphStoreEnum::Partitioned(p) => p.in_neighbors_by_label(vid, edge_label),
            GraphStoreEnum::Arrow(a) => a.in_neighbors_by_label(vid, edge_label),
        }
    }
}

impl Property for GraphStoreEnum {
    fn vertex_labels(&self, vid: u32) -> Vec<String> {
        match self {
            GraphStoreEnum::Single(s) => Property::vertex_labels(s, vid),
            GraphStoreEnum::Partitioned(p) => Property::vertex_labels(p, vid),
            GraphStoreEnum::Arrow(a) => Property::vertex_labels(a, vid)
        }
    }
    fn vertex_prop(&self, vid: u32, key: &str) -> Option<PropValue> {
        match self {
            GraphStoreEnum::Single(s) => s.vertex_prop(vid, key),
            GraphStoreEnum::Partitioned(p) => p.vertex_prop(vid, key),
            GraphStoreEnum::Arrow(a) => a.vertex_prop(vid, key),
        }
    }
    fn edge_prop(&self, edge_id: u32, key: &str) -> Option<PropValue> {
        match self {
            GraphStoreEnum::Single(s) => s.edge_prop(edge_id, key),
            GraphStoreEnum::Partitioned(p) => p.edge_prop(edge_id, key),
            GraphStoreEnum::Arrow(a) => a.edge_prop(edge_id, key),
        }
    }
    fn vertex_prop_keys(&self, label: &str) -> Vec<String> {
        match self {
            GraphStoreEnum::Single(s) => s.vertex_prop_keys(label),
            GraphStoreEnum::Partitioned(p) => p.vertex_prop_keys(label),
            GraphStoreEnum::Arrow(a) => a.vertex_prop_keys(label),
        }
    }
    fn edge_prop_keys(&self, label: &str) -> Vec<String> {
        match self {
            GraphStoreEnum::Single(s) => s.edge_prop_keys(label),
            GraphStoreEnum::Partitioned(p) => p.edge_prop_keys(label),
            GraphStoreEnum::Arrow(a) => a.edge_prop_keys(label),
        }
    }
    fn vertex_all_props(&self, vid: u32) -> HashMap<String, PropValue> {
        match self {
            GraphStoreEnum::Single(s) => s.vertex_all_props(vid),
            GraphStoreEnum::Partitioned(p) => p.vertex_all_props(vid),
            GraphStoreEnum::Arrow(a) => a.vertex_all_props(vid),
        }
    }
}

impl Schema for GraphStoreEnum {
    fn vertex_labels(&self) -> Vec<String> {
        match self {
            GraphStoreEnum::Single(s) => Schema::vertex_labels(s),
            GraphStoreEnum::Partitioned(p) => Schema::vertex_labels(p),
            GraphStoreEnum::Arrow(a) => Schema::vertex_labels(a)
        }
    }
    fn edge_labels(&self) -> Vec<String> {
        match self {
            GraphStoreEnum::Single(s) => Schema::edge_labels(s),
            GraphStoreEnum::Partitioned(p) => Schema::edge_labels(p),
            GraphStoreEnum::Arrow(a) => Schema::edge_labels(a)
        }
    }
    fn vertex_primary_key(&self, label: &str) -> Option<String> {
        match self {
            GraphStoreEnum::Single(s) => s.vertex_primary_key(label),
            GraphStoreEnum::Partitioned(p) => p.vertex_primary_key(label),
            GraphStoreEnum::Arrow(a) => a.vertex_primary_key(label),
        }
    }
}

impl Scannable for GraphStoreEnum {
    fn scan_vertices(&self, label: &str, predicate: &Predicate) -> Vec<u32> {
        match self {
            GraphStoreEnum::Single(s) => s.scan_vertices(label, predicate),
            GraphStoreEnum::Partitioned(p) => p.scan_vertices(label, predicate),
            GraphStoreEnum::Arrow(a) => a.scan_vertices(label, predicate),
        }
    }
    fn scan_vertices_by_label(&self, label: &str) -> Vec<u32> {
        match self {
            GraphStoreEnum::Single(s) => s.scan_vertices_by_label(label),
            GraphStoreEnum::Partitioned(p) => p.scan_vertices_by_label(label),
            GraphStoreEnum::Arrow(a) => a.scan_vertices_by_label(label),
        }
    }
    fn scan_all_vertices(&self) -> Vec<u32> {
        match self {
            GraphStoreEnum::Single(s) => s.scan_all_vertices(),
            GraphStoreEnum::Partitioned(p) => p.scan_all_vertices(),
            GraphStoreEnum::Arrow(a) => a.scan_all_vertices(),
        }
    }
}

impl Mutable for GraphStoreEnum {
    fn add_vertex(&mut self, label: &str, props: &[(&str, PropValue)]) -> u32 {
        match self {
            GraphStoreEnum::Single(s) => s.add_vertex(label, props),
            GraphStoreEnum::Partitioned(p) => p.add_vertex(label, props),
            GraphStoreEnum::Arrow(a) => a.add_vertex(label, props),
        }
    }
    fn add_edge(&mut self, src: u32, dst: u32, label: &str, props: &[(&str, PropValue)]) -> u32 {
        match self {
            GraphStoreEnum::Single(s) => s.add_edge(src, dst, label, props),
            GraphStoreEnum::Partitioned(p) => p.add_edge(src, dst, label, props),
            GraphStoreEnum::Arrow(a) => a.add_edge(src, dst, label, props),
        }
    }
    fn set_vertex_prop(&mut self, vid: u32, key: &str, value: PropValue) {
        match self {
            GraphStoreEnum::Single(s) => s.set_vertex_prop(vid, key, value),
            GraphStoreEnum::Partitioned(p) => p.set_vertex_prop(vid, key, value),
            GraphStoreEnum::Arrow(a) => a.set_vertex_prop(vid, key, value),
        }
    }
    fn delete_vertex(&mut self, vid: u32) {
        match self {
            GraphStoreEnum::Single(s) => s.delete_vertex(vid),
            GraphStoreEnum::Partitioned(p) => p.delete_vertex(vid),
            GraphStoreEnum::Arrow(a) => a.delete_vertex(vid),
        }
    }
    fn delete_edge(&mut self, edge_id: u32) {
        match self {
            GraphStoreEnum::Single(s) => s.delete_edge(edge_id),
            GraphStoreEnum::Partitioned(p) => p.delete_edge(edge_id),
            GraphStoreEnum::Arrow(a) => a.delete_edge(edge_id),
        }
    }
    fn commit(&mut self) -> u64 {
        match self {
            GraphStoreEnum::Single(s) => s.commit(),
            GraphStoreEnum::Partitioned(p) => p.commit(),
            GraphStoreEnum::Arrow(a) => Mutable::commit(a),
        }
    }
}

// ── MutableCsrStore-specific methods (delegated for single mode) ──

impl GraphStoreEnum {
    /// Build a MemoryGraph for Cypher execution.
    pub fn to_filtered_memory_graph(
        &self,
        labels: &[String],
        rel_types: &[String],
    ) -> yata_cypher::MemoryGraph {
        match self {
            GraphStoreEnum::Single(s) => s.to_filtered_memory_graph(labels, rel_types),
            GraphStoreEnum::Arrow(_) => yata_cypher::MemoryGraph::new(),
            GraphStoreEnum::Partitioned(p) => {
                // For partitioned mode, build from first partition that has data
                // TODO: merge across partitions for full graph
                for part in p.partitions() {
                    if part.vertex_count() > 0 {
                        return part.to_filtered_memory_graph(labels, rel_types);
                    }
                }
                yata_cypher::MemoryGraph::new()
            }
        }
    }

    /// Build a full MemoryGraph.
    pub fn to_full_memory_graph(&self) -> yata_cypher::MemoryGraph {
        match self {
            GraphStoreEnum::Single(s) => s.to_full_memory_graph(),
            GraphStoreEnum::Partitioned(_) => {
                // Build merged MemoryGraph from all partitions
                let mut g = yata_cypher::MemoryGraph::new();
                if let GraphStoreEnum::Partitioned(p) = self {
                    for (pid_idx, part) in p.partitions().iter().enumerate() {
                        let part_g = part.to_full_memory_graph();
                        // Merge nodes with partition-prefixed IDs
                        for node in part_g.nodes() {
                            let mut n = node.clone();
                            n.id = format!("p{}_{}", pid_idx, n.id);
                            g.add_node(n);
                        }
                        for rel in part_g.rels() {
                            let mut r = rel.clone();
                            r.src = format!("p{}_{}", pid_idx, r.src);
                            r.dst = format!("p{}_{}", pid_idx, r.dst);
                            r.id = format!("p{}_{}", pid_idx, r.id);
                            g.add_rel(r);
                        }
                    }
                    g.build_csr();
                }
                g
            }
            GraphStoreEnum::Arrow(_) => yata_cypher::MemoryGraph::new(),
        }
    }

    /// Add vertex with multiple labels (snapshot restore).
    pub fn add_vertex_with_labels(
        &mut self,
        labels: &[String],
        props: &[(&str, PropValue)],
    ) -> u32 {
        match self {
            GraphStoreEnum::Single(s) => s.add_vertex_with_labels(labels, props),
            GraphStoreEnum::Arrow(a) => {
                let label = labels.first().map(|s| s.as_str()).unwrap_or("_default");
                a.add_vertex(label, props)
            }
            GraphStoreEnum::Partitioned(p) => {
                let label = labels.first().map(|s| s.as_str()).unwrap_or("_default");
                p.add_vertex(label, props)
            }
        }
    }

    /// Add vertex with labels and optional global VID (snapshot restore with identity).
    pub fn add_vertex_with_labels_and_optional_global_id(
        &mut self,
        global_vid: Option<yata_core::GlobalVid>,
        labels: &[String],
        props: &[(&str, PropValue)],
    ) -> u32 {
        match self {
            GraphStoreEnum::Single(s) => {
                s.add_vertex_with_labels_and_optional_global_id(global_vid, labels, props)
            }
            GraphStoreEnum::Arrow(a) => {
                let label = labels.first().map(|s| s.as_str()).unwrap_or("_default");
                a.add_vertex(label, props)
            }
            GraphStoreEnum::Partitioned(p) => {
                let label = labels.first().map(|s| s.as_str()).unwrap_or("_default");
                p.add_vertex(label, props)
            }
        }
    }

    /// Add edge with optional global EID (snapshot restore).
    pub fn add_edge_with_optional_global_id(
        &mut self,
        _global_eid: Option<yata_core::GlobalEid>,
        src: u32,
        dst: u32,
        label: &str,
        props: &[(&str, PropValue)],
    ) -> u32 {
        match self {
            GraphStoreEnum::Single(s) => {
                s.add_edge_with_optional_global_id(_global_eid, src, dst, label, props)
            }
            GraphStoreEnum::Partitioned(p) => p.add_edge(src, dst, label, props),
            GraphStoreEnum::Arrow(a) => a.add_edge(src, dst, label, props),
        }
    }

    /// Remove vertices by label (LRU eviction).
    pub fn remove_vertices_by_label(&mut self, label: &str) {
        match self {
            GraphStoreEnum::Single(s) => s.remove_vertices_by_label(label),
            GraphStoreEnum::Arrow(_) => {} // Arrow store: no LRU eviction
            GraphStoreEnum::Partitioned(p) => {
                for part in p.partitions_mut() {
                    part.remove_vertices_by_label(label);
                }
            }
        }
    }

    /// Remove edges by label (LRU eviction).
    pub fn remove_edges_by_label(&mut self, rel_type: &str) {
        match self {
            GraphStoreEnum::Single(s) => s.remove_edges_by_label(rel_type),
            GraphStoreEnum::Arrow(_) => {}
            GraphStoreEnum::Partitioned(p) => {
                for part in p.partitions_mut() {
                    part.remove_edges_by_label(rel_type);
                }
            }
        }
    }

    /// Set vertex primary key.
    pub fn set_vertex_primary_key(&mut self, label: &str, key: &str) {
        match self {
            GraphStoreEnum::Single(s) => s.set_vertex_primary_key(label, key),
            GraphStoreEnum::Arrow(_) => {} // Arrow: PK is always "rkey"
            GraphStoreEnum::Partitioned(p) => {
                for part in p.partitions_mut() {
                    part.set_vertex_primary_key(label, key);
                }
            }
        }
    }

    /// Raw accessors delegation (snapshot serialization).
    pub fn vertex_count_raw(&self) -> u32 {
        match self {
            GraphStoreEnum::Single(s) => s.vertex_count_raw(),
            GraphStoreEnum::Partitioned(p) => p.vertex_count() as u32,
            GraphStoreEnum::Arrow(a) => a.vertex_count() as u32,
        }
    }

    pub fn edge_count_raw(&self) -> u32 {
        match self {
            GraphStoreEnum::Single(s) => s.edge_count_raw(),
            GraphStoreEnum::Partitioned(p) => p.edge_count() as u32,
            GraphStoreEnum::Arrow(a) => a.edge_count() as u32,
        }
    }

    pub fn vertex_alive_raw(&self) -> &[bool] {
        match self {
            GraphStoreEnum::Single(s) => s.vertex_alive_raw(),
            GraphStoreEnum::Partitioned(_) => &[], // Not applicable for partitioned
            GraphStoreEnum::Arrow(_) => &[], // Not applicable for partitioned
        }
    }

    pub fn edge_alive_raw(&self) -> &[bool] {
        match self {
            GraphStoreEnum::Single(s) => s.edge_alive_raw(),
            GraphStoreEnum::Partitioned(_) => &[],
            GraphStoreEnum::Arrow(_) => &[],
        }
    }

    pub fn edge_labels_raw(&self) -> &[String] {
        match self {
            GraphStoreEnum::Single(s) => s.edge_labels_raw(),
            GraphStoreEnum::Partitioned(_) => &[],
            GraphStoreEnum::Arrow(_) => &[],
        }
    }

    pub fn edge_src_raw(&self) -> &[u32] {
        match self {
            GraphStoreEnum::Single(s) => s.edge_src_raw(),
            GraphStoreEnum::Partitioned(_) => &[],
            GraphStoreEnum::Arrow(_) => &[],
        }
    }

    pub fn edge_dst_raw(&self) -> &[u32] {
        match self {
            GraphStoreEnum::Single(s) => s.edge_dst_raw(),
            GraphStoreEnum::Partitioned(_) => &[],
            GraphStoreEnum::Arrow(_) => &[],
        }
    }

    pub fn edge_props_raw(&self) -> &[HashMap<String, PropValue>] {
        match self {
            GraphStoreEnum::Single(s) => s.edge_props_raw(),
            GraphStoreEnum::Partitioned(_) => &[],
            GraphStoreEnum::Arrow(_) => &[],
        }
    }

    pub fn out_csr_raw(&self) -> &HashMap<String, crate::CsrSegment> {
        static EMPTY: std::sync::LazyLock<HashMap<String, crate::CsrSegment>> =
            std::sync::LazyLock::new(HashMap::new);
        match self {
            GraphStoreEnum::Single(s) => s.out_csr_raw(),
            GraphStoreEnum::Partitioned(_) => &EMPTY,
            GraphStoreEnum::Arrow(_) => &EMPTY,
        }
    }

    pub fn in_csr_raw(&self) -> &HashMap<String, crate::CsrSegment> {
        static EMPTY: std::sync::LazyLock<HashMap<String, crate::CsrSegment>> =
            std::sync::LazyLock::new(HashMap::new);
        match self {
            GraphStoreEnum::Single(s) => s.in_csr_raw(),
            GraphStoreEnum::Partitioned(_) => &EMPTY,
            GraphStoreEnum::Arrow(_) => &EMPTY,
        }
    }

    pub fn vertex_pk_raw(&self) -> &HashMap<String, String> {
        static EMPTY: std::sync::LazyLock<HashMap<String, String>> =
            std::sync::LazyLock::new(HashMap::new);
        match self {
            GraphStoreEnum::Single(s) => s.vertex_pk_raw(),
            GraphStoreEnum::Partitioned(_) => &EMPTY,
            GraphStoreEnum::Arrow(_) => &EMPTY,
        }
    }

    pub fn partition_id_raw(&self) -> PartitionId {
        match self {
            GraphStoreEnum::Single(s) => s.partition_id_raw(),
            GraphStoreEnum::Partitioned(_) => PartitionId::new(0),
            GraphStoreEnum::Arrow(_) => PartitionId::new(0),
        }
    }

    pub fn global_map(&self) -> &crate::GlobalToLocalMap {
        static EMPTY: std::sync::LazyLock<crate::GlobalToLocalMap> =
            std::sync::LazyLock::new(crate::GlobalToLocalMap::new);
        match self {
            GraphStoreEnum::Single(s) => s.global_map(),
            GraphStoreEnum::Partitioned(_) => &EMPTY,
            GraphStoreEnum::Arrow(_) => &EMPTY,
        }
    }

    pub fn global_vid(&self, local: u32) -> Option<yata_core::GlobalVid> {
        match self {
            GraphStoreEnum::Single(s) => s.global_vid(local),
            GraphStoreEnum::Partitioned(_) => None,
            GraphStoreEnum::Arrow(_) => None,
        }
    }

    pub fn global_eid(&self, local: u32) -> Option<yata_core::GlobalEid> {
        match self {
            GraphStoreEnum::Single(s) => s.global_eid(local),
            GraphStoreEnum::Partitioned(_) => None,
            GraphStoreEnum::Arrow(_) => None,
        }
    }

    pub fn version(&self) -> u64 {
        match self {
            GraphStoreEnum::Single(s) => s.version(),
            GraphStoreEnum::Partitioned(_) => 0,
            GraphStoreEnum::Arrow(_) => 0,
        }
    }

    /// Drain dirty vertex labels.
    pub fn drain_dirty_vertex_labels(&mut self) -> Vec<String> {
        match self {
            GraphStoreEnum::Single(s) => s.drain_dirty_vertex_labels(),
            GraphStoreEnum::Partitioned(_) => Vec::new(),
            GraphStoreEnum::Arrow(_) => Vec::new(),
        }
    }

    /// Drain dirty edge labels.
    pub fn drain_dirty_edge_labels(&mut self) -> Vec<String> {
        match self {
            GraphStoreEnum::Single(s) => s.drain_dirty_edge_labels(),
            GraphStoreEnum::Partitioned(_) => Vec::new(),
            GraphStoreEnum::Arrow(_) => Vec::new(),
        }
    }
}

unsafe impl Send for GraphStoreEnum {}
unsafe impl Sync for GraphStoreEnum {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_mode() {
        let mut store = GraphStoreEnum::new(1, PartitionId::new(0));
        assert!(!store.is_partitioned());
        assert!(store.as_single().is_some());

        let v = store.add_vertex("A", &[("x", PropValue::Int(1))]);
        store.commit();
        assert_eq!(store.vertex_count(), 1);
        assert_eq!(store.vertex_prop(v, "x"), Some(PropValue::Int(1)));
    }

    #[test]
    fn test_partitioned_mode() {
        let mut store = GraphStoreEnum::new(4, PartitionId::new(0));
        assert!(store.is_partitioned());
        assert!(store.as_partitioned().is_some());

        let v0 = store.add_vertex("A", &[("x", PropValue::Int(1))]);
        let v1 = store.add_vertex("B", &[("x", PropValue::Int(2))]);
        store.add_edge(v0, v1, "R", &[]);
        store.commit();

        assert_eq!(store.vertex_count(), 2);
        assert_eq!(store.edge_count(), 1);
    }

    #[test]
    fn test_memory_budget() {
        let budget = MemoryBudget::new(10); // 10 MB
        assert!(budget.can_add(1000, 1000));
        budget.record_add(1000, 1000);
        assert!(budget.used_mb() > 0.0);

        // Try to add way too many
        assert!(!budget.can_add(100_000, 100_000));
    }

    #[test]
    fn test_memory_budget_unlimited() {
        let budget = MemoryBudget::new(0);
        assert!(budget.can_add(1_000_000, 1_000_000));
    }

    #[test]
    fn test_memory_budget_sync() {
        let budget = MemoryBudget::new(100);
        budget.sync_from_counts(10_000, 30_000);
        assert!(budget.used_mb() > 0.0);
    }

    #[test]
    fn test_memory_budget_basic() {
        let budget = MemoryBudget::new(50); // 50 MB
        assert_eq!(budget.max_mb(), 50);
        assert_eq!(budget.used_mb(), 0.0);
        assert_eq!(budget.usage_ratio(), 0.0);

        // Sync from counts: 1000 vertices (200B each) + 2000 edges (100B each)
        // = 200_000 + 200_000 = 400_000 bytes ≈ 0.38 MB
        budget.sync_from_counts(1000, 2000);
        assert!(budget.used_mb() > 0.3);
        assert!(budget.used_mb() < 0.5);
        assert!(budget.usage_ratio() > 0.0);
        assert!(budget.usage_ratio() < 1.0);
    }

    #[test]
    fn test_memory_budget_zero() {
        let budget = MemoryBudget::new(0); // unlimited
        // can_add should always return true for unlimited budget
        assert!(budget.can_add(0, 0));
        assert!(budget.can_add(1_000_000, 1_000_000));
        assert!(budget.can_add(usize::MAX / 1000, 0));
        // usage_ratio returns 0.0 for unlimited
        assert_eq!(budget.usage_ratio(), 0.0);
    }

    #[test]
    fn test_memory_budget_over_limit() {
        let budget = MemoryBudget::new(1); // 1 MB = 1_048_576 bytes
        // 200 bytes/vertex * 6000 = 1_200_000 > 1_048_576
        assert!(!budget.can_add(6000, 0));
        // Under limit should be fine
        assert!(budget.can_add(100, 100));

        // Record some usage, then check can_add again
        budget.record_add(5000, 0); // 5000 * 200 = 1_000_000 bytes
        // Now adding 1000 more vertices (200_000 bytes) would exceed 1MB
        assert!(!budget.can_add(1000, 0));
        // But a small add is still possible
        assert!(budget.can_add(1, 0));
    }

    #[test]
    fn test_graph_store_enum_single() {
        let mut store = GraphStoreEnum::new(1, PartitionId::new(0));
        assert!(!store.is_partitioned());
        assert!(store.as_single().is_some());
        assert!(store.as_partitioned().is_none());

        // Add a vertex and verify via trait methods
        let vid = Mutable::add_vertex(
            &mut store,
            "Person",
            &[("name", PropValue::Str("Alice".into()))],
        );
        Mutable::commit(&mut store);

        assert_eq!(Topology::vertex_count(&store), 1);
        assert!(Topology::has_vertex(&store, vid));
        assert_eq!(
            Property::vertex_labels(&store, vid),
            vec!["Person".to_string()]
        );
        assert_eq!(
            Property::vertex_prop(&store, vid, "name"),
            Some(PropValue::Str("Alice".into()))
        );
    }

    #[test]
    fn test_graph_store_enum_delegation() {
        let mut store = GraphStoreEnum::new(1, PartitionId::new(0));

        // Empty store
        assert_eq!(Topology::vertex_count(&store), 0);
        assert_eq!(Topology::edge_count(&store), 0);
        assert!(!Topology::has_vertex(&store, 0));

        // Add vertices and edges
        let v0 = Mutable::add_vertex(&mut store, "A", &[("val", PropValue::Int(10))]);
        let v1 = Mutable::add_vertex(&mut store, "B", &[("val", PropValue::Int(20))]);
        let _e = Mutable::add_edge(&mut store, v0, v1, "REL", &[]);
        Mutable::commit(&mut store);

        // Delegation: vertex_count, edge_count, has_vertex
        assert_eq!(Topology::vertex_count(&store), 2);
        assert_eq!(Topology::edge_count(&store), 1);
        assert!(Topology::has_vertex(&store, v0));
        assert!(Topology::has_vertex(&store, v1));
        assert!(!Topology::has_vertex(&store, 999));

        // Delegation: out_degree, in_degree
        assert_eq!(Topology::out_degree(&store, v0), 1);
        assert_eq!(Topology::in_degree(&store, v1), 1);

        // Delegation: schema
        let vlabels = Schema::vertex_labels(&store);
        assert!(vlabels.contains(&"A".to_string()));
        assert!(vlabels.contains(&"B".to_string()));
        let elabels = Schema::edge_labels(&store);
        assert!(elabels.contains(&"REL".to_string()));
    }
}

