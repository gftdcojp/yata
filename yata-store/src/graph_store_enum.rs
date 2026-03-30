//! GraphStoreEnum: unified enum for single vs partitioned graph store.
//!
//! Wraps MutableCsrStore (single partition) and PartitionedGraphStore (multi-partition)
//! behind a common API surface. Provides memory budget tracking for OOM protection.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use yata_core::PartitionId;
use yata_cypher::Graph;
use yata_grin::*;

use crate::MutableCsrStore;
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
            bytes_per_vertex: 200,
            bytes_per_edge: 100,
        }
    }

    pub fn can_add(&self, n_vertices: usize, n_edges: usize) -> bool {
        if self.max_bytes == 0 {
            return true;
        }
        let additional = n_vertices * self.bytes_per_vertex + n_edges * self.bytes_per_edge;
        self.used_bytes.load(Ordering::Relaxed) + additional <= self.max_bytes
    }

    pub fn record_add(&self, n_vertices: usize, n_edges: usize) {
        let additional = n_vertices * self.bytes_per_vertex + n_edges * self.bytes_per_edge;
        self.used_bytes.fetch_add(additional, Ordering::Relaxed);
    }

    pub fn record_remove(&self, n_vertices: usize, n_edges: usize) {
        let reduction = n_vertices * self.bytes_per_vertex + n_edges * self.bytes_per_edge;
        self.used_bytes.fetch_sub(
            reduction.min(self.used_bytes.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
    }

    pub fn used_mb(&self) -> f64 {
        self.used_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0)
    }

    pub fn max_mb(&self) -> usize {
        self.max_bytes / (1024 * 1024).max(1)
    }

    pub fn usage_ratio(&self) -> f64 {
        if self.max_bytes == 0 {
            return 0.0;
        }
        self.used_bytes.load(Ordering::Relaxed) as f64 / self.max_bytes as f64
    }

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
}

impl GraphStoreEnum {
    pub fn new(partition_count: u32, hot_partition_id: PartitionId) -> Self {
        if partition_count <= 1 {
            Self::Single(MutableCsrStore::new_with_partition_id(hot_partition_id))
        } else {
            Self::Partitioned(PartitionedGraphStore::new(partition_count))
        }
    }

    pub fn is_partitioned(&self) -> bool {
        matches!(self, Self::Partitioned(_))
    }

    pub fn as_single(&self) -> Option<&MutableCsrStore> {
        match self {
            Self::Single(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_single_mut(&mut self) -> Option<&mut MutableCsrStore> {
        match self {
            Self::Single(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_partitioned(&self) -> Option<&PartitionedGraphStore> {
        match self {
            Self::Partitioned(p) => Some(p),
            _ => None,
        }
    }

    pub fn as_partitioned_mut(&mut self) -> Option<&mut PartitionedGraphStore> {
        match self {
            Self::Partitioned(p) => Some(p),
            _ => None,
        }
    }

    pub fn partition_count(&self) -> u32 {
        match self {
            Self::Single(_) => 1,
            Self::Partitioned(p) => p.partition_count(),
        }
    }

    pub fn to_filtered_memory_graph(
        &self,
        labels: &[String],
        rel_types: &[String],
    ) -> yata_cypher::MemoryGraph {
        match self {
            Self::Single(s) => s.to_filtered_memory_graph(labels, rel_types),
            Self::Partitioned(p) => {
                for part in p.partitions() {
                    if part.vertex_count() > 0 {
                        return part.to_filtered_memory_graph(labels, rel_types);
                    }
                }
                yata_cypher::MemoryGraph::new()
            }
        }
    }

    pub fn to_full_memory_graph(&self) -> yata_cypher::MemoryGraph {
        match self {
            Self::Single(s) => s.to_full_memory_graph(),
            Self::Partitioned(p) => {
                let mut g = yata_cypher::MemoryGraph::new();
                for (pid_idx, part) in p.partitions().iter().enumerate() {
                    let part_g = part.to_full_memory_graph();
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
                g
            }
        }
    }

    pub fn add_vertex_with_labels(
        &mut self,
        labels: &[String],
        props: &[(&str, PropValue)],
    ) -> u32 {
        match self {
            Self::Single(s) => s.add_vertex_with_labels(labels, props),
            Self::Partitioned(p) => {
                let label = labels.first().map(|s| s.as_str()).unwrap_or("_default");
                p.add_vertex(label, props)
            }
        }
    }

    pub fn add_vertex_with_labels_and_optional_global_id(
        &mut self,
        global_vid: Option<yata_core::GlobalVid>,
        labels: &[String],
        props: &[(&str, PropValue)],
    ) -> u32 {
        match self {
            Self::Single(s) => s.add_vertex_with_labels_and_optional_global_id(global_vid, labels, props),
            Self::Partitioned(p) => {
                let label = labels.first().map(|s| s.as_str()).unwrap_or("_default");
                p.add_vertex(label, props)
            }
        }
    }

    pub fn add_edge_with_optional_global_id(
        &mut self,
        global_eid: Option<yata_core::GlobalEid>,
        src: u32,
        dst: u32,
        label: &str,
        props: &[(&str, PropValue)],
    ) -> u32 {
        match self {
            Self::Single(s) => s.add_edge_with_optional_global_id(global_eid, src, dst, label, props),
            Self::Partitioned(p) => p.add_edge(src, dst, label, props),
        }
    }

    pub fn remove_vertices_by_label(&mut self, label: &str) {
        match self {
            Self::Single(s) => s.remove_vertices_by_label(label),
            Self::Partitioned(p) => {
                for part in p.partitions_mut() {
                    part.remove_vertices_by_label(label);
                }
            }
        }
    }

    pub fn remove_edges_by_label(&mut self, rel_type: &str) {
        match self {
            Self::Single(s) => s.remove_edges_by_label(rel_type),
            Self::Partitioned(p) => {
                for part in p.partitions_mut() {
                    part.remove_edges_by_label(rel_type);
                }
            }
        }
    }

    pub fn set_vertex_primary_key(&mut self, label: &str, key: &str) {
        match self {
            Self::Single(s) => s.set_vertex_primary_key(label, key),
            Self::Partitioned(p) => {
                for part in p.partitions_mut() {
                    part.set_vertex_primary_key(label, key);
                }
            }
        }
    }

    pub fn vertex_count_raw(&self) -> u32 {
        match self {
            Self::Single(s) => s.vertex_count_raw(),
            Self::Partitioned(p) => p.vertex_count() as u32,
        }
    }

    pub fn edge_count_raw(&self) -> u32 {
        match self {
            Self::Single(s) => s.edge_count_raw(),
            Self::Partitioned(p) => p.edge_count() as u32,
        }
    }

    pub fn vertex_alive_raw(&self) -> &[bool] {
        match self {
            Self::Single(s) => s.vertex_alive_raw(),
            Self::Partitioned(_) => &[],
        }
    }

    pub fn edge_alive_raw(&self) -> &[bool] {
        match self {
            Self::Single(s) => s.edge_alive_raw(),
            Self::Partitioned(_) => &[],
        }
    }

    pub fn edge_labels_raw(&self) -> &[String] {
        match self {
            Self::Single(s) => s.edge_labels_raw(),
            Self::Partitioned(_) => &[],
        }
    }

    pub fn edge_src_raw(&self) -> &[u32] {
        match self {
            Self::Single(s) => s.edge_src_raw(),
            Self::Partitioned(_) => &[],
        }
    }

    pub fn edge_dst_raw(&self) -> &[u32] {
        match self {
            Self::Single(s) => s.edge_dst_raw(),
            Self::Partitioned(_) => &[],
        }
    }

    pub fn edge_props_raw(&self) -> &[HashMap<String, PropValue>] {
        match self {
            Self::Single(s) => s.edge_props_raw(),
            Self::Partitioned(_) => &[],
        }
    }

    pub fn out_csr_raw(&self) -> HashMap<String, crate::CsrSegment> {
        match self {
            Self::Single(s) => s.out_csr_raw(),
            Self::Partitioned(_) => HashMap::new(),
        }
    }

    pub fn in_csr_raw(&self) -> HashMap<String, crate::CsrSegment> {
        match self {
            Self::Single(s) => s.in_csr_raw(),
            Self::Partitioned(_) => HashMap::new(),
        }
    }

    pub fn vertex_pk_raw(&self) -> &HashMap<String, String> {
        static EMPTY: std::sync::LazyLock<HashMap<String, String>> =
            std::sync::LazyLock::new(HashMap::new);
        match self {
            Self::Single(s) => s.vertex_pk_raw(),
            Self::Partitioned(_) => &EMPTY,
        }
    }

    pub fn partition_id_raw(&self) -> PartitionId {
        match self {
            Self::Single(s) => s.partition_id_raw(),
            Self::Partitioned(_) => PartitionId::new(0),
        }
    }

    pub fn global_map(&self) -> &crate::GlobalToLocalMap {
        static EMPTY: std::sync::LazyLock<crate::GlobalToLocalMap> =
            std::sync::LazyLock::new(crate::GlobalToLocalMap::new);
        match self {
            Self::Single(s) => s.global_map(),
            Self::Partitioned(_) => &EMPTY,
        }
    }

    pub fn global_vid(&self, local: u32) -> Option<yata_core::GlobalVid> {
        match self {
            Self::Single(s) => s.global_vid(local),
            Self::Partitioned(_) => None,
        }
    }

    pub fn global_eid(&self, local: u32) -> Option<yata_core::GlobalEid> {
        match self {
            Self::Single(s) => s.global_eid(local),
            Self::Partitioned(_) => None,
        }
    }

    pub fn version(&self) -> u64 {
        match self {
            Self::Single(s) => s.version(),
            Self::Partitioned(_) => 0,
        }
    }

    pub fn drain_dirty_vertex_labels(&mut self) -> Vec<String> {
        match self {
            Self::Single(s) => s.drain_dirty_vertex_labels(),
            Self::Partitioned(_) => Vec::new(),
        }
    }

    pub fn drain_dirty_edge_labels(&mut self) -> Vec<String> {
        match self {
            Self::Single(s) => s.drain_dirty_edge_labels(),
            Self::Partitioned(_) => Vec::new(),
        }
    }
}

impl Topology for GraphStoreEnum {
    fn vertex_count(&self) -> usize {
        match self {
            Self::Single(s) => s.vertex_count(),
            Self::Partitioned(p) => p.vertex_count(),
        }
    }
    fn edge_count(&self) -> usize {
        match self {
            Self::Single(s) => s.edge_count(),
            Self::Partitioned(p) => p.edge_count(),
        }
    }
    fn has_vertex(&self, vid: u32) -> bool {
        match self {
            Self::Single(s) => s.has_vertex(vid),
            Self::Partitioned(p) => p.has_vertex(vid),
        }
    }
    fn out_degree(&self, vid: u32) -> usize {
        match self {
            Self::Single(s) => s.out_degree(vid),
            Self::Partitioned(p) => p.out_degree(vid),
        }
    }
    fn in_degree(&self, vid: u32) -> usize {
        match self {
            Self::Single(s) => s.in_degree(vid),
            Self::Partitioned(p) => p.in_degree(vid),
        }
    }
    fn out_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        match self {
            Self::Single(s) => s.out_neighbors(vid),
            Self::Partitioned(p) => p.out_neighbors(vid),
        }
    }
    fn in_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        match self {
            Self::Single(s) => s.in_neighbors(vid),
            Self::Partitioned(p) => p.in_neighbors(vid),
        }
    }
    fn out_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        match self {
            Self::Single(s) => s.out_neighbors_by_label(vid, edge_label),
            Self::Partitioned(p) => p.out_neighbors_by_label(vid, edge_label),
        }
    }
    fn in_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        match self {
            Self::Single(s) => s.in_neighbors_by_label(vid, edge_label),
            Self::Partitioned(p) => p.in_neighbors_by_label(vid, edge_label),
        }
    }
}

impl Property for GraphStoreEnum {
    fn vertex_labels(&self, vid: u32) -> Vec<String> {
        match self {
            Self::Single(s) => Property::vertex_labels(s, vid),
            Self::Partitioned(p) => Property::vertex_labels(p, vid),
        }
    }
    fn vertex_prop(&self, vid: u32, key: &str) -> Option<PropValue> {
        match self {
            Self::Single(s) => s.vertex_prop(vid, key),
            Self::Partitioned(p) => p.vertex_prop(vid, key),
        }
    }
    fn edge_prop(&self, edge_id: u32, key: &str) -> Option<PropValue> {
        match self {
            Self::Single(s) => s.edge_prop(edge_id, key),
            Self::Partitioned(p) => p.edge_prop(edge_id, key),
        }
    }
    fn vertex_prop_keys(&self, label: &str) -> Vec<String> {
        match self {
            Self::Single(s) => s.vertex_prop_keys(label),
            Self::Partitioned(p) => p.vertex_prop_keys(label),
        }
    }
    fn edge_prop_keys(&self, label: &str) -> Vec<String> {
        match self {
            Self::Single(s) => s.edge_prop_keys(label),
            Self::Partitioned(p) => p.edge_prop_keys(label),
        }
    }
    fn vertex_all_props(&self, vid: u32) -> HashMap<String, PropValue> {
        match self {
            Self::Single(s) => s.vertex_all_props(vid),
            Self::Partitioned(p) => p.vertex_all_props(vid),
        }
    }
}

impl Schema for GraphStoreEnum {
    fn vertex_labels(&self) -> Vec<String> {
        match self {
            Self::Single(s) => Schema::vertex_labels(s),
            Self::Partitioned(p) => Schema::vertex_labels(p),
        }
    }
    fn edge_labels(&self) -> Vec<String> {
        match self {
            Self::Single(s) => Schema::edge_labels(s),
            Self::Partitioned(p) => Schema::edge_labels(p),
        }
    }
    fn vertex_primary_key(&self, label: &str) -> Option<String> {
        match self {
            Self::Single(s) => s.vertex_primary_key(label),
            Self::Partitioned(p) => p.vertex_primary_key(label),
        }
    }
}

impl Scannable for GraphStoreEnum {
    fn scan_vertices(&self, label: &str, predicate: &Predicate) -> Vec<u32> {
        match self {
            Self::Single(s) => s.scan_vertices(label, predicate),
            Self::Partitioned(p) => p.scan_vertices(label, predicate),
        }
    }
    fn scan_vertices_by_label(&self, label: &str) -> Vec<u32> {
        match self {
            Self::Single(s) => s.scan_vertices_by_label(label),
            Self::Partitioned(p) => p.scan_vertices_by_label(label),
        }
    }
    fn scan_all_vertices(&self) -> Vec<u32> {
        match self {
            Self::Single(s) => s.scan_all_vertices(),
            Self::Partitioned(p) => p.scan_all_vertices(),
        }
    }
}

impl Mutable for GraphStoreEnum {
    fn add_vertex(&mut self, label: &str, props: &[(&str, PropValue)]) -> u32 {
        match self {
            Self::Single(s) => s.add_vertex(label, props),
            Self::Partitioned(p) => p.add_vertex(label, props),
        }
    }
    fn add_edge(&mut self, src: u32, dst: u32, label: &str, props: &[(&str, PropValue)]) -> u32 {
        match self {
            Self::Single(s) => s.add_edge(src, dst, label, props),
            Self::Partitioned(p) => p.add_edge(src, dst, label, props),
        }
    }
    fn set_vertex_prop(&mut self, vid: u32, key: &str, value: PropValue) {
        match self {
            Self::Single(s) => s.set_vertex_prop(vid, key, value),
            Self::Partitioned(p) => p.set_vertex_prop(vid, key, value),
        }
    }
    fn delete_vertex(&mut self, vid: u32) {
        match self {
            Self::Single(s) => s.delete_vertex(vid),
            Self::Partitioned(p) => p.delete_vertex(vid),
        }
    }
    fn delete_edge(&mut self, edge_id: u32) {
        match self {
            Self::Single(s) => s.delete_edge(edge_id),
            Self::Partitioned(p) => p.delete_edge(edge_id),
        }
    }
    fn commit(&mut self) -> u64 {
        match self {
            Self::Single(s) => s.commit(),
            Self::Partitioned(p) => p.commit(),
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
        let budget = MemoryBudget::new(10);
        assert!(budget.can_add(1000, 1000));
        budget.record_add(1000, 1000);
        assert!(budget.used_mb() > 0.0);
        assert!(!budget.can_add(100_000, 100_000));
    }
}
