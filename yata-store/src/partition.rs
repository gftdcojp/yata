//! Partition-aware graph storage (M2: partition-local store + selective checkpoint).
//!
//! Key abstractions:
//! - `PartitionAssignment`: strategy for routing vertices to partitions (hash, range, label)
//! - `PartitionStoreSet`: collection of `MutableCsrStore` instances forming a global graph
//! - Selective checkpoint: only dirty partitions are serialized/uploaded
//!
//! Serialization is handled externally (yata-engine/snapshot) to avoid circular deps.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use yata_core::{GlobalVid, LocalVid, PartitionId};
use yata_grin::*;

use crate::{GlobalToLocalMap, MutableCsrStore};

// ── Partition assignment ────────────────────────────────────────────

/// Strategy for assigning vertices to partitions.
#[derive(Debug, Clone)]
pub enum PartitionAssignment {
    /// Hash-based: partition = hash(global_vid) % partition_count.
    Hash { partition_count: u32 },
    /// Label-based: specific labels map to specific partitions.
    Label {
        label_map: HashMap<String, u32>,
        default_partition: u32,
    },
    /// Single partition (backward compat).
    Single,
}

impl PartitionAssignment {
    /// Determine which partition a vertex belongs to.
    pub fn assign_vertex(&self, global_vid: GlobalVid, label: &str) -> PartitionId {
        match self {
            PartitionAssignment::Hash { partition_count } => {
                let hash = (global_vid.0.wrapping_mul(0x9E3779B97F4A7C15)) >> 48;
                PartitionId::new((hash as u32) % partition_count)
            }
            PartitionAssignment::Label {
                label_map,
                default_partition,
            } => {
                let pid = label_map.get(label).copied().unwrap_or(*default_partition);
                PartitionId::new(pid)
            }
            PartitionAssignment::Single => PartitionId::new(0),
        }
    }

    /// Number of partitions.
    pub fn partition_count(&self) -> u32 {
        match self {
            PartitionAssignment::Hash { partition_count } => *partition_count,
            PartitionAssignment::Label { label_map, .. } => label_map
                .values()
                .copied()
                .max()
                .map(|m| m + 1)
                .unwrap_or(1),
            PartitionAssignment::Single => 1,
        }
    }
}

impl Default for PartitionAssignment {
    fn default() -> Self {
        Self::Single
    }
}

// ── Partition store set ─────────────────────────────────────────────

/// A collection of `MutableCsrStore` instances, one per partition.
/// Provides a global graph facade with partition-local storage.
pub struct PartitionStoreSet {
    /// Per-partition stores. Key = partition_id.
    stores: HashMap<u32, MutableCsrStore>,
    /// Global → (partition, local_vid) routing map.
    global_map: GlobalToLocalMap,
    /// Assignment strategy.
    assignment: PartitionAssignment,
    /// Dirty partition tracking (partitions modified since last checkpoint).
    dirty_partitions: HashSet<u32>,
    /// Global version counter.
    version: AtomicU64,
}

impl PartitionStoreSet {
    /// Create a new partition store set with the given assignment strategy.
    pub fn new(assignment: PartitionAssignment) -> Self {
        let count = assignment.partition_count();
        let mut stores = HashMap::with_capacity(count as usize);
        for pid in 0..count {
            stores.insert(pid, MutableCsrStore::new_partition(pid));
        }
        Self {
            stores,
            global_map: GlobalToLocalMap::new(),
            assignment,
            dirty_partitions: HashSet::new(),
            version: AtomicU64::new(0),
        }
    }

    /// Create a single-partition store set (backward compat).
    pub fn single() -> Self {
        Self::new(PartitionAssignment::Single)
    }

    /// Number of partitions.
    pub fn partition_count(&self) -> u32 {
        self.assignment.partition_count()
    }

    /// Get the assignment strategy.
    pub fn assignment(&self) -> &PartitionAssignment {
        &self.assignment
    }

    /// Get a partition store by ID.
    pub fn partition(&self, pid: u32) -> Option<&MutableCsrStore> {
        self.stores.get(&pid)
    }

    /// Get a mutable partition store by ID.
    pub fn partition_mut(&mut self, pid: u32) -> Option<&mut MutableCsrStore> {
        self.stores.get_mut(&pid)
    }

    /// Iterate over all partition stores.
    pub fn partitions(&self) -> impl Iterator<Item = (u32, &MutableCsrStore)> {
        self.stores.iter().map(|(&pid, store)| (pid, store))
    }

    /// Borrow the global map.
    pub fn global_map(&self) -> &GlobalToLocalMap {
        &self.global_map
    }

    /// Replace a partition store (used by external restore logic).
    pub fn set_partition(&mut self, pid: u32, store: MutableCsrStore) {
        self.stores.insert(pid, store);
    }

    // ── Mutation API ─────────────────────────────────────────────

    /// Add a vertex, routing to the correct partition based on assignment strategy.
    /// Returns (partition_id, local_vid).
    pub fn add_vertex(
        &mut self,
        global: GlobalVid,
        label: &str,
        props: &[(&str, PropValue)],
    ) -> (PartitionId, u32) {
        let pid = self.assignment.assign_vertex(global, label);
        let store = self
            .stores
            .get_mut(&pid.0)
            .expect("partition store missing");
        let local_vid = store.add_vertex(label, props);
        self.global_map.insert(global, local_vid);
        store.global_map_mut().insert(global, local_vid);
        self.dirty_partitions.insert(pid.0);
        (pid, local_vid)
    }

    /// Add an edge between two global vertices. Resolves partitions and local VIDs.
    /// The edge is stored in the source vertex's partition.
    /// If dst is on a different partition, a ghost vertex is created on src's partition.
    /// Returns (partition_id, local_edge_id) or None if src/dst not found.
    pub fn add_edge(
        &mut self,
        src_global: GlobalVid,
        dst_global: GlobalVid,
        label: &str,
        props: &[(&str, PropValue)],
    ) -> Option<(PartitionId, u32)> {
        // Find which partition actually owns the src vertex
        let src_pid = self.find_vertex_partition(src_global)?;

        let store = self.stores.get_mut(&src_pid)?;

        // Resolve src local VID on src's partition
        let src_local = store.global_map().to_local(src_global)?;

        // Resolve dst local VID — may need ghost vertex on src's partition
        let dst_local = match store.global_map().to_local(dst_global) {
            Some(local) => local,
            None => {
                // dst is on a different partition or not yet registered locally.
                // Create ghost vertex on src's partition.
                let ghost_vid = store.add_vertex("_ghost", &[]);
                store.global_map_mut().insert(dst_global, ghost_vid);
                ghost_vid
            }
        };

        let local_eid = store.add_edge(src_local, dst_local, label, props);
        self.dirty_partitions.insert(src_pid);
        Some((PartitionId::new(src_pid), local_eid))
    }

    /// Find which partition a global vertex's HOME is (non-ghost).
    fn find_vertex_partition(&self, gvid: GlobalVid) -> Option<u32> {
        // Prefer non-ghost entries
        for (&pid, store) in &self.stores {
            if let Some(local) = store.global_map().to_local(gvid) {
                let labels = Property::vertex_labels(store, local);
                if labels.first().map(|l| l.as_str()) != Some("_ghost") {
                    return Some(pid);
                }
            }
        }
        // Fallback: any partition that has it (even ghost)
        for (&pid, store) in &self.stores {
            if store.global_map().to_local(gvid).is_some() {
                return Some(pid);
            }
        }
        None
    }

    /// Commit all dirty partitions.
    pub fn commit(&mut self) -> u64 {
        for &pid in &self.dirty_partitions.clone() {
            if let Some(store) = self.stores.get_mut(&pid) {
                store.commit();
            }
        }
        self.version.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Commit only a specific partition.
    pub fn commit_partition(&mut self, pid: u32) -> Option<u64> {
        self.stores.get_mut(&pid).map(|store| store.commit())
    }

    // ── Dirty partition tracking ─────────────────────────────────

    /// Get the set of dirty partition IDs (modified since last drain).
    pub fn dirty_partitions(&self) -> &HashSet<u32> {
        &self.dirty_partitions
    }

    /// Drain dirty partitions, returning the set and clearing the tracking.
    pub fn drain_dirty_partitions(&mut self) -> HashSet<u32> {
        std::mem::take(&mut self.dirty_partitions)
    }

    /// Check if any partition is dirty.
    pub fn has_dirty_partitions(&self) -> bool {
        !self.dirty_partitions.is_empty()
    }

    // ── Global read API ──────────────────────────────────────────

    /// Total vertex count across all partitions.
    pub fn vertex_count(&self) -> usize {
        self.stores.values().map(|s| s.vertex_count()).sum()
    }

    /// Total edge count across all partitions.
    pub fn edge_count(&self) -> usize {
        self.stores.values().map(|s| s.edge_count()).sum()
    }

    /// All vertex labels across all partitions.
    pub fn vertex_labels(&self) -> Vec<String> {
        let mut labels = HashSet::new();
        for store in self.stores.values() {
            for l in Schema::vertex_labels(store) {
                labels.insert(l);
            }
        }
        labels.into_iter().collect()
    }

    /// All edge labels across all partitions.
    pub fn edge_labels(&self) -> Vec<String> {
        let mut labels = HashSet::new();
        for store in self.stores.values() {
            for l in Schema::edge_labels(store) {
                labels.insert(l);
            }
        }
        labels.into_iter().collect()
    }

    /// Scan vertices by label across all partitions.
    /// Returns (partition_id, local_vid) pairs.
    pub fn scan_vertices_by_label(&self, label: &str) -> Vec<(u32, u32)> {
        let mut result = Vec::new();
        for (&pid, store) in &self.stores {
            for vid in store.scan_vertices_by_label(label) {
                result.push((pid, vid));
            }
        }
        result
    }

    /// Scan vertices with predicate across all partitions.
    pub fn scan_vertices(&self, label: &str, predicate: &Predicate) -> Vec<(u32, u32)> {
        let mut result = Vec::new();
        for (&pid, store) in &self.stores {
            for vid in store.scan_vertices(label, predicate) {
                result.push((pid, vid));
            }
        }
        result
    }

    /// Out-neighbors of a vertex identified by global ID.
    pub fn out_neighbors(&self, global: GlobalVid) -> Vec<(GlobalVid, u32, String)> {
        let (pid, store, local) = match self.resolve_vertex(global) {
            Some(r) => r,
            None => return Vec::new(),
        };
        store
            .out_neighbors(local)
            .into_iter()
            .map(|n| {
                let neighbor_global =
                    store
                        .global_map()
                        .to_global(n.vid)
                        .unwrap_or(GlobalVid::encode(
                            PartitionId::new(pid),
                            LocalVid::new(n.vid),
                        ));
                (neighbor_global, n.edge_id, n.edge_label)
            })
            .collect()
    }

    /// Get vertex property by global ID.
    pub fn vertex_prop(&self, global: GlobalVid, key: &str) -> Option<PropValue> {
        let (_, store, local) = self.resolve_vertex(global)?;
        store.vertex_prop(local, key)
    }

    /// Get vertex labels by global ID.
    pub fn vertex_labels_of(&self, global: GlobalVid) -> Vec<String> {
        match self.resolve_vertex(global) {
            Some((_, store, local)) => Property::vertex_labels(store, local),
            None => Vec::new(),
        }
    }

    /// Resolve a global vertex to (partition_id, store_ref, local_vid).
    fn resolve_vertex(&self, gvid: GlobalVid) -> Option<(u32, &MutableCsrStore, u32)> {
        for (&pid, store) in &self.stores {
            if let Some(local) = store.global_map().to_local(gvid) {
                // Skip ghost vertices (label = "_ghost")
                let labels = Property::vertex_labels(store, local);
                if labels.first().map(|l| l.as_str()) == Some("_ghost") {
                    continue;
                }
                return Some((pid, store, local));
            }
        }
        None
    }

    /// Find the home partition of a global vertex (non-ghost).
    pub fn find_vertex_home(&self, gvid: GlobalVid) -> Option<u32> {
        for (&pid, store) in &self.stores {
            if let Some(local) = store.global_map().to_local(gvid) {
                let labels = Property::vertex_labels(store, local);
                if labels.first().map(|l| l.as_str()) != Some("_ghost") {
                    return Some(pid);
                }
            }
        }
        None
    }

    /// Current global version.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }
}

// ── Partitioned manifest (no serialization deps) ────────────────────

/// Manifest for a partitioned graph snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionedManifest {
    pub version: u32,
    pub partition_count: u32,
    pub partitions: Vec<PartitionManifestEntry>,
    pub timestamp_ns: i64,
    pub total_vertex_count: u64,
    pub total_edge_count: u64,
}

/// Per-partition manifest entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionManifestEntry {
    pub partition_id: u32,
    pub vertex_count: u64,
    pub edge_count: u64,
    pub vertex_labels: Vec<String>,
    pub edge_labels: Vec<String>,
    pub wal_lsn: u64,
    pub checkpoint_version: u64,
}

impl PartitionStoreSet {
    /// Build a PartitionedManifest from current state.
    pub fn build_manifest(&self, checkpointed: &[u32]) -> PartitionedManifest {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        let partitions: Vec<PartitionManifestEntry> = checkpointed
            .iter()
            .filter_map(|&pid| {
                let store = self.stores.get(&pid)?;
                Some(PartitionManifestEntry {
                    partition_id: pid,
                    vertex_count: store.vertex_count() as u64,
                    edge_count: store.edge_count() as u64,
                    vertex_labels: Schema::vertex_labels(store),
                    edge_labels: Schema::edge_labels(store),
                    wal_lsn: 0,
                    checkpoint_version: store.version(),
                })
            })
            .collect();

        PartitionedManifest {
            version: 1,
            partition_count: self.partition_count(),
            partitions,
            timestamp_ns: now_ns,
            total_vertex_count: self.vertex_count() as u64,
            total_edge_count: self.edge_count() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_partition_store_set() {
        let mut pss = PartitionStoreSet::single();
        assert_eq!(pss.partition_count(), 1);
        assert_eq!(pss.vertex_count(), 0);

        let gv0 = GlobalVid::from_local(0);
        let gv1 = GlobalVid::from_local(1);
        pss.add_vertex(gv0, "Person", &[("name", PropValue::Str("Alice".into()))]);
        pss.add_vertex(gv1, "Person", &[("name", PropValue::Str("Bob".into()))]);
        pss.add_edge(gv0, gv1, "KNOWS", &[]);
        pss.commit();

        assert_eq!(pss.vertex_count(), 2);
        assert_eq!(pss.edge_count(), 1);
        assert_eq!(
            pss.vertex_prop(gv0, "name"),
            Some(PropValue::Str("Alice".into()))
        );
    }

    #[test]
    fn test_hash_partition_distribution() {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 4 });
        assert_eq!(pss.partition_count(), 4);

        for i in 0..100u64 {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            pss.add_vertex(gv, "Node", &[("idx", PropValue::Int(i as i64))]);
        }
        pss.commit();

        assert_eq!(pss.vertex_count(), 100);

        // Verify distribution: each partition should have some vertices
        let mut counts = Vec::new();
        for pid in 0..4 {
            let count = pss.partition(pid).map(|s| s.vertex_count()).unwrap_or(0);
            counts.push(count);
        }
        for (pid, &count) in counts.iter().enumerate() {
            assert!(count > 0, "partition {} has 0 vertices", pid);
        }
        assert_eq!(counts.iter().sum::<usize>(), 100);
    }

    #[test]
    fn test_label_partition_assignment() {
        let mut label_map = HashMap::new();
        label_map.insert("Person".to_string(), 0);
        label_map.insert("Company".to_string(), 1);
        let assignment = PartitionAssignment::Label {
            label_map,
            default_partition: 0,
        };

        let mut pss = PartitionStoreSet::new(assignment);
        let gv0 = GlobalVid::from_local(0);
        let gv1 = GlobalVid::from_local(1);
        let gv2 = GlobalVid::from_local(2);

        pss.add_vertex(gv0, "Person", &[("name", PropValue::Str("Alice".into()))]);
        pss.add_vertex(gv1, "Company", &[("name", PropValue::Str("GFTD".into()))]);
        pss.add_vertex(gv2, "Person", &[("name", PropValue::Str("Bob".into()))]);
        pss.commit();

        assert_eq!(pss.partition(0).unwrap().vertex_count(), 2); // Alice + Bob
        assert_eq!(pss.partition(1).unwrap().vertex_count(), 1); // GFTD
    }

    #[test]
    fn test_dirty_partition_tracking() {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 4 });

        let gv0 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        pss.add_vertex(gv0, "A", &[]);

        assert!(pss.has_dirty_partitions());
        let dirty_count = pss.dirty_partitions().len();
        assert_eq!(dirty_count, 1);

        // Drain clears dirty state
        let drained = pss.drain_dirty_partitions();
        assert_eq!(drained.len(), 1);
        assert!(!pss.has_dirty_partitions());
    }

    #[test]
    fn test_selective_dirty_tracking() {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 4 });

        for i in 0..10u64 {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            pss.add_vertex(gv, "Node", &[("i", PropValue::Int(i as i64))]);
        }
        pss.commit();

        let dirty_before = pss.dirty_partitions().len();
        assert!(dirty_before > 0);
        assert!(dirty_before <= 4); // at most 4 partitions

        let drained = pss.drain_dirty_partitions();
        let clean = 4 - drained.len() as u32;
        assert!(clean < 4, "not all partitions should be dirty");
        assert!(!pss.has_dirty_partitions());
    }

    #[test]
    fn test_cross_partition_edge() {
        let mut label_map = HashMap::new();
        label_map.insert("Person".to_string(), 0);
        label_map.insert("Company".to_string(), 1);
        let assignment = PartitionAssignment::Label {
            label_map,
            default_partition: 0,
        };

        let mut pss = PartitionStoreSet::new(assignment);
        let alice = GlobalVid::from_local(0);
        let gftd = GlobalVid::from_local(1);

        pss.add_vertex(alice, "Person", &[("name", PropValue::Str("Alice".into()))]);
        pss.add_vertex(gftd, "Company", &[("name", PropValue::Str("GFTD".into()))]);
        let edge_result = pss.add_edge(alice, gftd, "WORKS_AT", &[]);
        pss.commit();

        assert!(edge_result.is_some());
        let (edge_pid, _eid) = edge_result.unwrap();
        assert_eq!(edge_pid.0, 0); // edge stored in source's partition
    }

    #[test]
    fn test_out_neighbors() {
        let mut pss = PartitionStoreSet::single();
        let gv0 = GlobalVid::from_local(0);
        let gv1 = GlobalVid::from_local(1);
        let gv2 = GlobalVid::from_local(2);

        pss.add_vertex(gv0, "A", &[]);
        pss.add_vertex(gv1, "B", &[]);
        pss.add_vertex(gv2, "C", &[]);
        pss.add_edge(gv0, gv1, "R1", &[]);
        pss.add_edge(gv0, gv2, "R2", &[]);
        pss.commit();

        let neighbors = pss.out_neighbors(gv0);
        assert_eq!(neighbors.len(), 2);
    }

    #[test]
    fn test_global_scan_across_partitions() {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 4 });

        for i in 0..40u64 {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            let label = if i % 2 == 0 { "Even" } else { "Odd" };
            pss.add_vertex(gv, label, &[]);
        }
        pss.commit();

        let even = pss.scan_vertices_by_label("Even");
        let odd = pss.scan_vertices_by_label("Odd");
        assert_eq!(even.len(), 20);
        assert_eq!(odd.len(), 20);
    }

    #[test]
    fn test_manifest_build() {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 2 });

        for i in 0..10u64 {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            pss.add_vertex(gv, "Node", &[("v", PropValue::Int(i as i64))]);
        }
        pss.commit();

        let manifest = pss.build_manifest(&[0, 1]);
        assert_eq!(manifest.partition_count, 2);
        assert_eq!(manifest.total_vertex_count, 10);
        assert_eq!(manifest.partitions.len(), 2);
    }

    #[test]
    fn test_partition_commit_selective() {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 2 });

        let gv0 = GlobalVid::from_local(0);
        pss.add_vertex(gv0, "A", &[]);
        // Only commit partition 0
        let ver = pss.commit_partition(0);
        assert!(ver.is_some());
    }

    #[test]
    fn test_set_partition_replace() {
        let mut pss = PartitionStoreSet::single();
        let gv0 = GlobalVid::from_local(0);
        pss.add_vertex(gv0, "A", &[]);
        pss.commit();
        assert_eq!(pss.vertex_count(), 1);

        // Replace with empty store
        pss.set_partition(0, MutableCsrStore::new_partition(0));
        assert_eq!(pss.vertex_count(), 0);
    }
}
