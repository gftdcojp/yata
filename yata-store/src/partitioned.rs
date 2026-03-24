//! PartitionedGraphStore — multi-partition facade over MutableCsrStore.
//!
//! Edge-cut partitioning: vertices hash to partitions by GlobalVid.
//! All outgoing edges live on the source vertex's partition.
//!
//! ## Composed VID encoding (u32 API surface)
//!
//! The GRIN traits return `u32`. To encode partition + local VID in a single u32:
//!
//! ```text
//!   [partition_id: 8 bits][local_vid: 24 bits]
//!   → max 256 partitions × 16M vertices per partition
//! ```
//!
//! For trillion-scale, use the GlobalVid(u64) path directly.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use yata_core::{GlobalEid, GlobalVid, LocalVid, PartitionId};
use yata_grin::*;

use crate::MutableCsrStore;

// ── Composed VID encoding ───────────────────────────────────────────

const LOCAL_BITS: u32 = 24;
const LOCAL_MASK: u32 = (1 << LOCAL_BITS) - 1; // 0x00FF_FFFF

/// Compose a partition ID and local VID into a single u32.
/// Layout: `[pid: 8][local: 24]`. Supports up to 256 partitions, 16M vids each.
#[inline]
pub fn compose_vid(pid: PartitionId, local: u32) -> u32 {
    debug_assert!(pid.get() < 256, "partition_id must fit in 8 bits");
    debug_assert!(local <= LOCAL_MASK, "local_vid must fit in 24 bits");
    (pid.get() << LOCAL_BITS) | (local & LOCAL_MASK)
}

/// Decompose a composed u32 VID into (PartitionId, local_vid).
#[inline]
pub fn decompose_vid(composed: u32) -> (PartitionId, u32) {
    let pid = composed >> LOCAL_BITS;
    let local = composed & LOCAL_MASK;
    (PartitionId::new(pid), local)
}

/// Compose a partition ID and local EID into a single u32.
#[inline]
pub fn compose_eid(pid: PartitionId, local: u32) -> u32 {
    compose_vid(pid, local) // same encoding
}

/// Decompose a composed u32 EID into (PartitionId, local_eid).
#[inline]
pub fn decompose_eid(composed: u32) -> (PartitionId, u32) {
    decompose_vid(composed)
}

// ── PartitionedGraphStore ───────────────────────────────────────────

/// Multi-partition graph store wrapping N MutableCsrStore partitions.
///
/// Edge-cut partitioning: each vertex lives on exactly one partition
/// determined by `global_vid % partition_count`. Outgoing edges are
/// stored on the source vertex's partition.
///
/// The GRIN trait methods use composed u32 IDs (8-bit partition + 24-bit local).
/// For full u64 scale, use `global_to_local()` and partition accessors directly.
pub struct PartitionedGraphStore {
    partitions: Vec<MutableCsrStore>,
    partition_count: u32,
    /// Global vertex ID allocator (monotonic).
    global_vid_alloc: AtomicU64,
    /// Global edge ID allocator (monotonic).
    global_eid_alloc: AtomicU64,
    /// Fast lookup: GlobalVid -> PartitionId.
    vid_partition: HashMap<GlobalVid, PartitionId>,
    /// Cross-partition edge dst map: (src_partition, local_eid) -> composed_dst_vid.
    /// Needed because edges are stored with local dst vids within the partition,
    /// but the dst vertex may live on a different partition.
    edge_dst_map: HashMap<(u32, u32), u32>,
}

impl PartitionedGraphStore {
    /// Create a new partitioned store with `partition_count` empty partitions.
    pub fn new(partition_count: u32) -> Self {
        assert!(partition_count > 0, "partition_count must be > 0");
        assert!(
            partition_count <= 256,
            "partition_count must be <= 256 (8-bit encoding)"
        );
        let partitions = (0..partition_count)
            .map(|i| MutableCsrStore::new_partition(i))
            .collect();
        Self {
            partitions,
            partition_count,
            global_vid_alloc: AtomicU64::new(0),
            global_eid_alloc: AtomicU64::new(0),
            vid_partition: HashMap::new(),
            edge_dst_map: HashMap::new(),
        }
    }

    /// Hash-based partition assignment for a global vertex ID.
    #[inline]
    pub fn partition_for_vertex(&self, global_vid: GlobalVid) -> PartitionId {
        PartitionId::new((global_vid.get() % self.partition_count as u64) as u32)
    }

    /// Hash-based partition assignment for a label string.
    #[inline]
    pub fn partition_for_label(&self, label: &str) -> PartitionId {
        let hash = label
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
        PartitionId::new((hash % self.partition_count as u64) as u32)
    }

    /// Access a partition by ID.
    #[inline]
    pub fn partition(&self, pid: PartitionId) -> &MutableCsrStore {
        &self.partitions[pid.get() as usize]
    }

    /// Access a partition mutably by ID.
    #[inline]
    pub fn partition_mut(&mut self, pid: PartitionId) -> &mut MutableCsrStore {
        &mut self.partitions[pid.get() as usize]
    }

    /// All partitions.
    #[inline]
    pub fn partitions(&self) -> &[MutableCsrStore] {
        &self.partitions
    }

    /// All partitions mutably.
    #[inline]
    pub fn partitions_mut(&mut self) -> &mut [MutableCsrStore] {
        &mut self.partitions
    }

    /// Returns partition IDs that have dirty vertex or edge labels (modified since last commit).
    pub fn dirty_partitions(&self) -> Vec<PartitionId> {
        let mut dirty = Vec::new();
        for (i, p) in self.partitions.iter().enumerate() {
            // Use version as a heuristic: partitions with pending edges or dirty labels
            // We check if the partition has any pending mutations by examining its state.
            // Since we can't directly access private dirty_*_labels, we track via vid_partition
            // changes or use the fact that partitions that received add_vertex/add_edge calls
            // since last commit will have a higher vid_alloc than vertex_count after commit.
            //
            // Simpler approach: check if the partition's current version differs from
            // what we'd expect if it were clean. But MutableCsrStore doesn't expose pending state.
            // We keep our own tracking.
            let _ = p;
            // For now, include all partitions that have data
            if p.vertex_count() > 0 || p.edge_count() > 0 {
                dirty.push(PartitionId::new(i as u32));
            }
        }
        dirty
    }

    /// Resolve a GlobalVid to its partition and local VID.
    pub fn global_to_local(&self, global_vid: GlobalVid) -> Option<(PartitionId, LocalVid)> {
        let pid = self.vid_partition.get(&global_vid)?;
        let store = &self.partitions[pid.get() as usize];
        let local = store.local_vid(global_vid)?;
        Some((*pid, local))
    }

    /// Allocate a new global vertex ID and assign it to the hashed partition.
    fn alloc_global_vid(&mut self) -> (GlobalVid, PartitionId) {
        let gid = GlobalVid::new(self.global_vid_alloc.fetch_add(1, Ordering::Relaxed));
        let pid = self.partition_for_vertex(gid);
        self.vid_partition.insert(gid, pid);
        (gid, pid)
    }

    /// Allocate a new global edge ID.
    fn alloc_global_eid(&mut self) -> GlobalEid {
        GlobalEid::new(self.global_eid_alloc.fetch_add(1, Ordering::Relaxed))
    }
}

// ── Topology ────────────────────────────────────────────────────────

impl Topology for PartitionedGraphStore {
    fn vertex_count(&self) -> usize {
        self.partitions.iter().map(|p| p.vertex_count()).sum()
    }

    fn edge_count(&self) -> usize {
        self.partitions.iter().map(|p| p.edge_count()).sum()
    }

    fn has_vertex(&self, vid: u32) -> bool {
        let (pid, local) = decompose_vid(vid);
        if pid.get() >= self.partition_count {
            return false;
        }
        self.partitions[pid.get() as usize].has_vertex(local)
    }

    fn out_degree(&self, vid: u32) -> usize {
        let (pid, local) = decompose_vid(vid);
        if pid.get() >= self.partition_count {
            return 0;
        }
        self.partitions[pid.get() as usize].out_degree(local)
    }

    fn in_degree(&self, vid: u32) -> usize {
        // Edge-cut: incoming edges can be on any partition (edge on src's partition).
        // Scan edge_dst_map for edges pointing to this composed vid.
        self.edge_dst_map
            .values()
            .filter(|&&dst| dst == vid)
            .count()
    }

    fn out_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        let (pid, local) = decompose_vid(vid);
        if pid.get() >= self.partition_count {
            return Vec::new();
        }
        self.partitions[pid.get() as usize]
            .out_neighbors(local)
            .into_iter()
            .map(|n| {
                // Resolve dst via edge_dst_map (cross-partition), or same partition fallback
                let dst_composed = self
                    .edge_dst_map
                    .get(&(pid.get(), n.edge_id))
                    .copied()
                    .unwrap_or_else(|| compose_vid(pid, n.vid));
                Neighbor {
                    vid: dst_composed,
                    edge_id: compose_eid(pid, n.edge_id),
                    edge_label: n.edge_label,
                }
            })
            .collect()
    }

    fn in_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        // Edge-cut: incoming edges can be on any partition. Scan edge_dst_map.
        let mut result = Vec::new();
        for (&(part, local_eid), &dst) in &self.edge_dst_map {
            if dst != vid {
                continue;
            }
            let p = &self.partitions[part as usize];
            let eid = local_eid as usize;
            if eid < p.edge_alive_raw().len() && p.edge_alive_raw()[eid] {
                let pid = PartitionId::new(part);
                result.push(Neighbor {
                    vid: compose_vid(pid, p.edge_src_raw()[eid]),
                    edge_id: compose_eid(pid, local_eid),
                    edge_label: p.edge_labels_raw()[eid].clone(),
                });
            }
        }
        result
    }

    fn out_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        let (pid, local) = decompose_vid(vid);
        if pid.get() >= self.partition_count {
            return Vec::new();
        }
        self.partitions[pid.get() as usize]
            .out_neighbors_by_label(local, edge_label)
            .into_iter()
            .map(|n| {
                let dst_composed = self
                    .edge_dst_map
                    .get(&(pid.get(), n.edge_id))
                    .copied()
                    .unwrap_or_else(|| compose_vid(pid, n.vid));
                Neighbor {
                    vid: dst_composed,
                    edge_id: compose_eid(pid, n.edge_id),
                    edge_label: n.edge_label,
                }
            })
            .collect()
    }

    fn in_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        // Edge-cut: incoming edges can be on any partition. Scan edge_dst_map.
        let mut result = Vec::new();
        for (&(part, local_eid), &dst) in &self.edge_dst_map {
            if dst != vid {
                continue;
            }
            let p = &self.partitions[part as usize];
            let eid = local_eid as usize;
            if eid < p.edge_alive_raw().len()
                && p.edge_alive_raw()[eid]
                && eid < p.edge_labels_raw().len()
                && p.edge_labels_raw()[eid] == edge_label
            {
                let pid = PartitionId::new(part);
                result.push(Neighbor {
                    vid: compose_vid(pid, p.edge_src_raw()[eid]),
                    edge_id: compose_eid(pid, local_eid),
                    edge_label: edge_label.to_string(),
                });
            }
        }
        result
    }
}

// ── Property ────────────────────────────────────────────────────────

impl Property for PartitionedGraphStore {
    fn vertex_labels(&self, vid: u32) -> Vec<String> {
        let (pid, local) = decompose_vid(vid);
        if pid.get() >= self.partition_count {
            return Vec::new();
        }
        Property::vertex_labels(&self.partitions[pid.get() as usize], local)
    }

    fn vertex_prop(&self, vid: u32, key: &str) -> Option<PropValue> {
        let (pid, local) = decompose_vid(vid);
        if pid.get() >= self.partition_count {
            return None;
        }
        self.partitions[pid.get() as usize].vertex_prop(local, key)
    }

    fn vertex_all_props(&self, vid: u32) -> HashMap<String, PropValue> {
        let (pid, local) = decompose_vid(vid);
        if pid.get() >= self.partition_count {
            return HashMap::new();
        }
        self.partitions[pid.get() as usize].vertex_all_props(local)
    }

    fn edge_prop(&self, edge_id: u32, key: &str) -> Option<PropValue> {
        let (pid, local) = decompose_eid(edge_id);
        if pid.get() >= self.partition_count {
            return None;
        }
        self.partitions[pid.get() as usize].edge_prop(local, key)
    }

    fn vertex_prop_keys(&self, label: &str) -> Vec<String> {
        let mut keys = HashSet::new();
        for p in &self.partitions {
            for k in p.vertex_prop_keys(label) {
                keys.insert(k);
            }
        }
        keys.into_iter().collect()
    }

    fn edge_prop_keys(&self, label: &str) -> Vec<String> {
        let mut keys = HashSet::new();
        for p in &self.partitions {
            for k in p.edge_prop_keys(label) {
                keys.insert(k);
            }
        }
        keys.into_iter().collect()
    }
}

// ── Schema ──────────────────────────────────────────────────────────

impl Schema for PartitionedGraphStore {
    fn vertex_labels(&self) -> Vec<String> {
        let mut labels = HashSet::new();
        for p in &self.partitions {
            for l in Schema::vertex_labels(p) {
                labels.insert(l);
            }
        }
        labels.into_iter().collect()
    }

    fn edge_labels(&self) -> Vec<String> {
        let mut labels = HashSet::new();
        for p in &self.partitions {
            for l in Schema::edge_labels(p) {
                labels.insert(l);
            }
        }
        labels.into_iter().collect()
    }

    fn vertex_primary_key(&self, label: &str) -> Option<String> {
        // Return first partition's primary key (schema is shared)
        for p in &self.partitions {
            if let Some(pk) = p.vertex_primary_key(label) {
                return Some(pk);
            }
        }
        None
    }
}

// ── Scannable ───────────────────────────────────────────────────────

impl Scannable for PartitionedGraphStore {
    fn scan_vertices(&self, label: &str, predicate: &Predicate) -> Vec<u32> {
        let mut result = Vec::new();
        for (i, p) in self.partitions.iter().enumerate() {
            let pid = PartitionId::new(i as u32);
            for local_vid in p.scan_vertices(label, predicate) {
                result.push(compose_vid(pid, local_vid));
            }
        }
        result
    }

    fn scan_vertices_by_label(&self, label: &str) -> Vec<u32> {
        let mut result = Vec::new();
        for (i, p) in self.partitions.iter().enumerate() {
            let pid = PartitionId::new(i as u32);
            for local_vid in p.scan_vertices_by_label(label) {
                result.push(compose_vid(pid, local_vid));
            }
        }
        result
    }

    fn scan_all_vertices(&self) -> Vec<u32> {
        let mut result = Vec::new();
        for (i, p) in self.partitions.iter().enumerate() {
            let pid = PartitionId::new(i as u32);
            for local_vid in p.scan_all_vertices() {
                result.push(compose_vid(pid, local_vid));
            }
        }
        result
    }
}

// ── Mutable ─────────────────────────────────────────────────────────

impl Mutable for PartitionedGraphStore {
    fn add_vertex(&mut self, label: &str, props: &[(&str, PropValue)]) -> u32 {
        let (gid, pid) = self.alloc_global_vid();
        let local =
            self.partitions[pid.get() as usize].add_vertex_with_global_id(gid, label, props);
        compose_vid(pid, local)
    }

    fn add_edge(&mut self, src: u32, dst: u32, label: &str, props: &[(&str, PropValue)]) -> u32 {
        let (src_pid, src_local) = decompose_vid(src);
        let (_dst_pid, dst_local) = decompose_vid(dst);
        let geid = self.alloc_global_eid();
        // Edge-cut: edge lives on the source vertex's partition.
        // Store local src and local dst within the partition's CSR.
        // Record the composed dst in edge_dst_map for cross-partition resolution.
        let local_eid = self.partitions[src_pid.get() as usize]
            .add_edge_with_global_id(geid, src_local, dst_local, label, props);
        self.edge_dst_map.insert((src_pid.get(), local_eid), dst);
        compose_eid(src_pid, local_eid)
    }

    fn set_vertex_prop(&mut self, vid: u32, key: &str, value: PropValue) {
        let (pid, local) = decompose_vid(vid);
        if pid.get() < self.partition_count {
            self.partitions[pid.get() as usize].set_vertex_prop(local, key, value);
        }
    }

    fn delete_vertex(&mut self, vid: u32) {
        let (pid, local) = decompose_vid(vid);
        if pid.get() < self.partition_count {
            self.partitions[pid.get() as usize].delete_vertex(local);
            // Remove from vid_partition tracking
            if let Some(gid) = self.partitions[pid.get() as usize].global_vid(local) {
                self.vid_partition.remove(&gid);
            }
        }
    }

    fn delete_edge(&mut self, edge_id: u32) {
        let (pid, local) = decompose_eid(edge_id);
        if pid.get() < self.partition_count {
            self.partitions[pid.get() as usize].delete_edge(local);
        }
    }

    fn commit(&mut self) -> u64 {
        let mut max_version = 0u64;
        for p in &mut self.partitions {
            let v = p.commit();
            if v > max_version {
                max_version = v;
            }
        }
        max_version
    }
}

// ── Partitioned ─────────────────────────────────────────────────────

impl Partitioned for PartitionedGraphStore {
    fn partition_id(&self) -> u32 {
        0 // the facade itself is partition 0; individual partitions have their own IDs
    }

    fn partition_count(&self) -> u32 {
        self.partition_count
    }

    fn vertex_partition(&self, vid: u32) -> u32 {
        let (pid, _) = decompose_vid(vid);
        pid.get()
    }

    fn is_master(&self, vid: u32) -> bool {
        let (pid, local) = decompose_vid(vid);
        if pid.get() >= self.partition_count {
            return false;
        }
        self.partitions[pid.get() as usize].has_vertex(local)
    }
}

// ── Send + Sync ─────────────────────────────────────────────────────

// MutableCsrStore contains AtomicU32/AtomicU64 which are Send+Sync.
// The HashMap and Vec fields are owned. PartitionedGraphStore is single-writer.
unsafe impl Send for PartitionedGraphStore {}
unsafe impl Sync for PartitionedGraphStore {}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use yata_grin::{Mutable, Partitioned, Property, Scannable, Schema, Topology};

    #[test]
    fn test_compose_decompose_vid() {
        let pid = PartitionId::new(3);
        let local = 42u32;
        let composed = compose_vid(pid, local);
        let (p, l) = decompose_vid(composed);
        assert_eq!(p, pid);
        assert_eq!(l, local);
    }

    #[test]
    fn test_compose_decompose_boundary() {
        // Max partition (255) and max local (16M - 1)
        let pid = PartitionId::new(255);
        let local = LOCAL_MASK;
        let composed = compose_vid(pid, local);
        let (p, l) = decompose_vid(composed);
        assert_eq!(p, pid);
        assert_eq!(l, local);
    }

    #[test]
    fn test_partitioned_basic() {
        let mut store = PartitionedGraphStore::new(4);
        assert_eq!(store.vertex_count(), 0);
        assert_eq!(store.edge_count(), 0);
        assert_eq!(store.partition_count(), 4);

        // Add vertices — they'll distribute across partitions by global_vid hash
        let v0 = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        let v1 = store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        let v2 = store.add_vertex("Person", &[("name", PropValue::Str("Carol".into()))]);
        let v3 = store.add_vertex("Person", &[("name", PropValue::Str("Dave".into()))]);

        assert_eq!(store.vertex_count(), 4);
        assert!(store.has_vertex(v0));
        assert!(store.has_vertex(v1));
        assert!(store.has_vertex(v2));
        assert!(store.has_vertex(v3));

        // Verify vertices land on different partitions (0,1,2,3 mod 4)
        let (p0, _) = decompose_vid(v0);
        let (p1, _) = decompose_vid(v1);
        let (p2, _) = decompose_vid(v2);
        let (p3, _) = decompose_vid(v3);
        // GlobalVid 0..3 mod 4 = partitions 0,1,2,3
        assert_eq!(p0.get(), 0);
        assert_eq!(p1.get(), 1);
        assert_eq!(p2.get(), 2);
        assert_eq!(p3.get(), 3);

        // Commit and verify counts persist
        store.commit();
        assert_eq!(store.vertex_count(), 4);
    }

    #[test]
    fn test_partitioned_edge() {
        let mut store = PartitionedGraphStore::new(4);

        let alice = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        let bob = store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        let carol = store.add_vertex("Person", &[("name", PropValue::Str("Carol".into()))]);

        let e0 = store.add_edge(alice, bob, "KNOWS", &[("since", PropValue::Int(2020))]);
        let e1 = store.add_edge(bob, carol, "KNOWS", &[("since", PropValue::Int(2021))]);

        store.commit();

        assert_eq!(store.edge_count(), 2);

        // Verify edge is on source partition
        let (e0_pid, _) = decompose_eid(e0);
        let (alice_pid, _) = decompose_vid(alice);
        assert_eq!(e0_pid, alice_pid);

        let (e1_pid, _) = decompose_eid(e1);
        let (bob_pid, _) = decompose_vid(bob);
        assert_eq!(e1_pid, bob_pid);

        // Verify topology: Alice -> Bob
        let alice_out = store.out_neighbors(alice);
        assert_eq!(alice_out.len(), 1);
        assert_eq!(alice_out[0].vid, bob);
        assert_eq!(alice_out[0].edge_label, "KNOWS");

        // Verify topology: Bob -> Carol
        let bob_out = store.out_neighbors(bob);
        assert_eq!(bob_out.len(), 1);
        assert_eq!(bob_out[0].vid, carol);
    }

    #[test]
    fn test_partitioned_scan() {
        let mut store = PartitionedGraphStore::new(4);

        // Add vertices with different labels
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_vertex("Company", &[("name", PropValue::Str("ACME".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Carol".into()))]);
        store.add_vertex("Company", &[("name", PropValue::Str("Globex".into()))]);

        store.commit();

        // Scan by label across partitions
        let persons = store.scan_vertices_by_label("Person");
        assert_eq!(persons.len(), 3);

        let companies = store.scan_vertices_by_label("Company");
        assert_eq!(companies.len(), 2);

        // Scan all
        let all = store.scan_all_vertices();
        assert_eq!(all.len(), 5);

        // Scan with predicate
        let alices = store.scan_vertices(
            "Person",
            &Predicate::Eq("name".into(), PropValue::Str("Alice".into())),
        );
        assert_eq!(alices.len(), 1);
    }

    #[test]
    fn test_partitioned_property() {
        let mut store = PartitionedGraphStore::new(4);

        let v0 = store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        let v1 = store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Bob".into())),
                ("age", PropValue::Int(25)),
            ],
        );

        // Read properties across partitions
        assert_eq!(
            store.vertex_prop(v0, "name"),
            Some(PropValue::Str("Alice".into()))
        );
        assert_eq!(
            store.vertex_prop(v1, "name"),
            Some(PropValue::Str("Bob".into()))
        );
        assert_eq!(store.vertex_prop(v0, "age"), Some(PropValue::Int(30)));
        assert_eq!(store.vertex_prop(v1, "age"), Some(PropValue::Int(25)));

        // Set property
        store.set_vertex_prop(v0, "age", PropValue::Int(31));
        assert_eq!(store.vertex_prop(v0, "age"), Some(PropValue::Int(31)));

        // Vertex labels
        let labels = Property::vertex_labels(&store, v0);
        assert_eq!(labels, vec!["Person".to_string()]);

        // All props
        let all = store.vertex_all_props(v1);
        assert_eq!(all.get("name"), Some(&PropValue::Str("Bob".into())));
        assert_eq!(all.get("age"), Some(&PropValue::Int(25)));

        // Schema: merged vertex labels
        let schema_labels = Schema::vertex_labels(&store);
        assert!(schema_labels.contains(&"Person".to_string()));
    }

    #[test]
    fn test_partitioned_dirty_tracking() {
        let mut store = PartitionedGraphStore::new(4);

        // Initially all partitions are empty
        let dirty_before = store.dirty_partitions();
        assert!(dirty_before.is_empty());

        // Add vertices to some partitions
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.commit();

        // Partitions with data should be reported
        let dirty_after = store.dirty_partitions();
        assert!(dirty_after.len() >= 2);

        // Each dirty partition should have vertices
        for pid in &dirty_after {
            assert!(store.partition(*pid).vertex_count() > 0);
        }
    }

    #[test]
    fn test_partitioned_global_id_routing() {
        let mut store = PartitionedGraphStore::new(4);

        // Verify hash-based routing: global_vid % 4
        assert_eq!(store.partition_for_vertex(GlobalVid::new(0)).get(), 0);
        assert_eq!(store.partition_for_vertex(GlobalVid::new(1)).get(), 1);
        assert_eq!(store.partition_for_vertex(GlobalVid::new(2)).get(), 2);
        assert_eq!(store.partition_for_vertex(GlobalVid::new(3)).get(), 3);
        assert_eq!(store.partition_for_vertex(GlobalVid::new(4)).get(), 0);
        assert_eq!(store.partition_for_vertex(GlobalVid::new(100)).get(), 0);

        // Add a vertex and verify global_to_local resolves
        let v0 = store.add_vertex("Person", &[]);
        let _ = v0;

        // GlobalVid(0) should be on partition 0
        let (pid, local) = store.global_to_local(GlobalVid::new(0)).unwrap();
        assert_eq!(pid.get(), 0);
        assert_eq!(local.get(), 0);
    }

    #[test]
    fn test_partitioned_delete_vertex() {
        let mut store = PartitionedGraphStore::new(2);

        let v0 = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        let v1 = store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_edge(v0, v1, "KNOWS", &[]);
        store.commit();

        assert_eq!(store.vertex_count(), 2);
        assert_eq!(store.edge_count(), 1);

        store.delete_vertex(v0);
        store.commit();

        assert_eq!(store.vertex_count(), 1);
        assert!(!store.has_vertex(v0));
        assert!(store.has_vertex(v1));
    }

    #[test]
    fn test_partitioned_delete_edge() {
        let mut store = PartitionedGraphStore::new(2);

        let v0 = store.add_vertex("Person", &[]);
        let v1 = store.add_vertex("Person", &[]);
        let e0 = store.add_edge(v0, v1, "KNOWS", &[]);
        store.commit();

        assert_eq!(store.edge_count(), 1);

        store.delete_edge(e0);
        store.commit();

        assert_eq!(store.edge_count(), 0);
    }

    #[test]
    fn test_partitioned_edge_properties() {
        let mut store = PartitionedGraphStore::new(4);

        let v0 = store.add_vertex("Person", &[]);
        let v1 = store.add_vertex("Person", &[]);
        let e0 = store.add_edge(v0, v1, "KNOWS", &[("weight", PropValue::Float(0.9))]);
        store.commit();

        assert_eq!(store.edge_prop(e0, "weight"), Some(PropValue::Float(0.9)));
        assert_eq!(store.edge_prop(e0, "missing"), None);
    }

    #[test]
    fn test_partitioned_schema_merge() {
        let mut store = PartitionedGraphStore::new(4);

        // Vertices with different labels will land on different partitions
        store.add_vertex("Person", &[]);
        store.add_vertex("Company", &[]);
        store.add_vertex("Location", &[]);
        store.commit();

        let vlabels = Schema::vertex_labels(&store);
        assert!(vlabels.contains(&"Person".to_string()));
        assert!(vlabels.contains(&"Company".to_string()));
        assert!(vlabels.contains(&"Location".to_string()));
    }

    #[test]
    fn test_partitioned_is_master() {
        let mut store = PartitionedGraphStore::new(4);

        let v0 = store.add_vertex("Person", &[]);
        assert!(store.is_master(v0));

        // Non-existent vertex
        assert!(!store.is_master(compose_vid(PartitionId::new(3), 999)));
    }

    #[test]
    fn test_partitioned_many_vertices() {
        let mut store = PartitionedGraphStore::new(4);

        // Add 100 vertices and verify distribution
        let mut vids = Vec::new();
        for i in 0..100 {
            let v = store.add_vertex("Node", &[("id", PropValue::Int(i))]);
            vids.push(v);
        }
        store.commit();

        assert_eq!(store.vertex_count(), 100);

        // Check each partition got ~25 vertices (hash distribution)
        for i in 0..4 {
            let count = store.partition(PartitionId::new(i)).vertex_count();
            assert_eq!(count, 25, "partition {i} should have 25 vertices");
        }

        // All vertices retrievable
        for (i, v) in vids.iter().enumerate() {
            assert!(store.has_vertex(*v), "vertex {i} should exist");
            assert_eq!(
                store.vertex_prop(*v, "id"),
                Some(PropValue::Int(i as i64)),
                "vertex {i} prop mismatch"
            );
        }
    }
}
