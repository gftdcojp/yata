//! Partition-scoped snapshot persistence for trillion-scale graphs.
//!
//! Extends the base snapshot system with partition-aware layout:
//!   {prefix}snap/global_manifest.json    — global metadata (partition list, schema version)
//!   {prefix}snap/partitions/{pid}/manifest.json
//!   {prefix}snap/partitions/{pid}/v/{label}.arrow
//!   {prefix}snap/partitions/{pid}/e/{label}.arrow
//!   {prefix}snap/partitions/{pid}/topo.bin
//!   {prefix}snap/partitions/{pid}/schema.json
//!
//! Only dirty partitions are checkpointed (selective checkpoint).
//! Restore can load individual partitions on demand (selective restore).

use std::collections::HashSet;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use yata_core::PartitionId;
use yata_store::MutableCsrStore;

use crate::snapshot::{self, SnapshotBundle};

/// Global manifest tracking all partitions in a partitioned graph snapshot.
#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalSnapshotManifest {
    /// Format version.
    pub version: u32,
    /// Total partition count.
    pub partition_count: u32,
    /// Partitions that have data (may be sparse).
    pub active_partitions: Vec<u32>,
    /// Checkpoint version (monotonically increasing).
    pub checkpoint_version: u64,
    /// Timestamp of this checkpoint (ns since epoch).
    pub timestamp_ns: i64,
    /// Aggregate vertex count across all partitions.
    pub total_vertex_count: u64,
    /// Aggregate edge count across all partitions.
    pub total_edge_count: u64,
}

/// Tracks which partitions have been modified since last checkpoint.
#[derive(Debug, Default)]
pub struct DirtyPartitionTracker {
    dirty: HashSet<PartitionId>,
    checkpoint_version: u64,
}

impl DirtyPartitionTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark a partition as dirty (needs checkpoint).
    pub fn mark_dirty(&mut self, pid: PartitionId) {
        self.dirty.insert(pid);
    }

    /// Get all dirty partitions.
    pub fn dirty_partitions(&self) -> Vec<PartitionId> {
        self.dirty.iter().copied().collect()
    }

    /// Check if a specific partition is dirty.
    pub fn is_dirty(&self, pid: PartitionId) -> bool {
        self.dirty.contains(&pid)
    }

    /// Clear dirty tracking after successful checkpoint.
    pub fn clear(&mut self) {
        self.dirty.clear();
        self.checkpoint_version += 1;
    }

    /// Current checkpoint version.
    pub fn checkpoint_version(&self) -> u64 {
        self.checkpoint_version
    }

    /// Number of dirty partitions.
    pub fn dirty_count(&self) -> usize {
        self.dirty.len()
    }
}

/// Result of a selective checkpoint operation.
#[derive(Debug)]
pub struct SelectiveCheckpointResult {
    /// Partitions that were checkpointed.
    pub checkpointed: Vec<PartitionId>,
    /// Partitions that were skipped (clean).
    pub skipped: Vec<PartitionId>,
    /// Per-partition snapshot bundles (for upload).
    pub bundles: Vec<(PartitionId, SnapshotBundle)>,
    /// Global manifest.
    pub global_manifest: GlobalSnapshotManifest,
}

/// Serialize only dirty partitions into snapshot bundles.
pub fn selective_checkpoint(
    partitions: &[MutableCsrStore],
    tracker: &DirtyPartitionTracker,
    wal_lsn: u64,
) -> Result<SelectiveCheckpointResult, String> {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    let mut checkpointed = Vec::new();
    let mut skipped = Vec::new();
    let mut bundles = Vec::new();
    let mut total_vertices: u64 = 0;
    let mut total_edges: u64 = 0;
    let mut active_partitions = Vec::new();

    for (i, store) in partitions.iter().enumerate() {
        let pid = PartitionId::from(i as u32);
        let vc = store.vertex_count_raw() as u64;
        let ec = store.edge_count_raw() as u64;

        if vc > 0 || ec > 0 {
            active_partitions.push(i as u32);
        }
        total_vertices += vc;
        total_edges += ec;

        if tracker.is_dirty(pid) {
            let bundle = snapshot::serialize_snapshot(store, wal_lsn)?;
            bundles.push((pid, bundle));
            checkpointed.push(pid);
        } else {
            skipped.push(pid);
        }
    }

    let global_manifest = GlobalSnapshotManifest {
        version: 1,
        partition_count: partitions.len() as u32,
        active_partitions,
        checkpoint_version: tracker.checkpoint_version() + 1,
        timestamp_ns: now_ns,
        total_vertex_count: total_vertices,
        total_edge_count: total_edges,
    };

    Ok(SelectiveCheckpointResult {
        checkpointed,
        skipped,
        bundles,
        global_manifest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_grin::{Mutable, PropValue};

    fn make_partitioned_stores() -> Vec<MutableCsrStore> {
        let mut stores = Vec::new();
        for i in 0..4u32 {
            let mut s = MutableCsrStore::new_partition(i);
            s.add_vertex("Person", &[("name", PropValue::Str(format!("P{i}")))]);
            if i % 2 == 0 {
                s.add_vertex("Company", &[("name", PropValue::Str(format!("C{i}")))]);
            }
            s.commit();
            stores.push(s);
        }
        stores
    }

    #[test]
    fn test_dirty_partition_tracker() {
        let mut tracker = DirtyPartitionTracker::new();
        assert_eq!(tracker.dirty_count(), 0);
        assert_eq!(tracker.checkpoint_version(), 0);

        tracker.mark_dirty(PartitionId::from(0));
        tracker.mark_dirty(PartitionId::from(2));
        assert_eq!(tracker.dirty_count(), 2);
        assert!(tracker.is_dirty(PartitionId::from(0)));
        assert!(!tracker.is_dirty(PartitionId::from(1)));
        assert!(tracker.is_dirty(PartitionId::from(2)));

        tracker.clear();
        assert_eq!(tracker.dirty_count(), 0);
        assert_eq!(tracker.checkpoint_version(), 1);
    }

    #[test]
    fn test_selective_checkpoint_only_dirty() {
        let stores = make_partitioned_stores();
        let mut tracker = DirtyPartitionTracker::new();

        // Only partitions 1 and 3 are dirty
        tracker.mark_dirty(PartitionId::from(1));
        tracker.mark_dirty(PartitionId::from(3));

        let result = selective_checkpoint(&stores, &tracker, 0).unwrap();

        assert_eq!(result.checkpointed.len(), 2);
        assert_eq!(result.skipped.len(), 2);
        assert_eq!(result.bundles.len(), 2);
        assert_eq!(result.global_manifest.partition_count, 4);
        assert_eq!(result.global_manifest.checkpoint_version, 1);
        // All partitions have data
        assert_eq!(result.global_manifest.active_partitions.len(), 4);
    }

    #[test]
    fn test_selective_checkpoint_all_dirty() {
        let stores = make_partitioned_stores();
        let mut tracker = DirtyPartitionTracker::new();
        for i in 0..4 {
            tracker.mark_dirty(PartitionId::from(i));
        }

        let result = selective_checkpoint(&stores, &tracker, 42).unwrap();
        assert_eq!(result.checkpointed.len(), 4);
        assert_eq!(result.skipped.len(), 0);
        assert_eq!(result.bundles.len(), 4);
    }

    #[test]
    fn test_selective_checkpoint_none_dirty() {
        let stores = make_partitioned_stores();
        let tracker = DirtyPartitionTracker::new();

        let result = selective_checkpoint(&stores, &tracker, 0).unwrap();
        assert_eq!(result.checkpointed.len(), 0);
        assert_eq!(result.skipped.len(), 4);
        assert_eq!(result.bundles.len(), 0);
    }

    #[test]
    fn test_global_manifest_aggregate_counts() {
        let stores = make_partitioned_stores();
        let mut tracker = DirtyPartitionTracker::new();
        for i in 0..4 {
            tracker.mark_dirty(PartitionId::from(i));
        }

        let result = selective_checkpoint(&stores, &tracker, 0).unwrap();
        // 4 Person + 2 Company = 6 vertices
        assert_eq!(result.global_manifest.total_vertex_count, 6);
    }

    #[test]
    fn test_checkpoint_version_increments() {
        let stores = make_partitioned_stores();
        let mut tracker = DirtyPartitionTracker::new();
        tracker.mark_dirty(PartitionId::from(0));

        let r1 = selective_checkpoint(&stores, &tracker, 0).unwrap();
        assert_eq!(r1.global_manifest.checkpoint_version, 1);

        tracker.clear();
        tracker.mark_dirty(PartitionId::from(1));

        let r2 = selective_checkpoint(&stores, &tracker, 1).unwrap();
        assert_eq!(r2.global_manifest.checkpoint_version, 2);
    }
}
