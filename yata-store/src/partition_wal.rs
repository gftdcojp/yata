//! Partitioned WAL: per-partition DurableWal files (M4: partition writer).
//!
//! Each partition gets its own WAL file under `{wal_dir}/partitions/{pid}/graph.wal`.
//! The shard_id field in DurableWalEntry is used as partition_id.
//!
//! This enables:
//! - Per-partition fsync (only dirty partition's WAL is written)
//! - Per-partition truncate (only checkpoint'd partition's WAL is truncated)
//! - Per-partition replay on restart (only relevant partition is replayed)

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use yata_core::{DurableWalEntry, DurableWalOp};

use yata_grin::Mutable;

use crate::MutableCsrStore;
use crate::durable_wal::DurableWal;
use crate::partition::PartitionStoreSet;

/// Per-partition WAL manager.
pub struct PartitionedWal {
    /// Base directory for WAL files.
    base_dir: PathBuf,
    /// Per-partition WAL instances. Lazily opened.
    wals: HashMap<u32, DurableWal>,
    /// Partition count.
    partition_count: u32,
}

impl PartitionedWal {
    /// Open or create partitioned WAL files under `base_dir/partitions/{pid}/`.
    pub fn open(base_dir: impl AsRef<Path>, partition_count: u32) -> io::Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let mut wals = HashMap::new();

        for pid in 0..partition_count {
            let dir = base_dir.join("partitions").join(pid.to_string());
            let wal = DurableWal::open(&dir)?;
            wals.insert(pid, wal);
        }

        Ok(Self {
            base_dir,
            wals,
            partition_count,
        })
    }

    /// Append an operation to a specific partition's WAL.
    pub fn append(&mut self, partition_id: u32, op: DurableWalOp) -> io::Result<u64> {
        let wal = self.wals.get_mut(&partition_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("partition {} WAL not found", partition_id),
            )
        })?;
        wal.append(op)
    }

    /// Append a batch of operations to a specific partition's WAL.
    pub fn append_batch(&mut self, partition_id: u32, ops: Vec<DurableWalOp>) -> io::Result<u64> {
        let wal = self.wals.get_mut(&partition_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("partition {} WAL not found", partition_id),
            )
        })?;
        wal.append_batch(ops)
    }

    /// Mark a partition's WAL as committed up to the given LSN.
    pub fn mark_committed(&mut self, partition_id: u32, lsn: u64) {
        if let Some(wal) = self.wals.get_mut(&partition_id) {
            wal.mark_committed(lsn);
        }
    }

    /// Truncate a specific partition's WAL.
    pub fn truncate(&mut self, partition_id: u32) -> io::Result<()> {
        let wal = self.wals.get_mut(&partition_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("partition {} WAL not found", partition_id),
            )
        })?;
        wal.truncate()
    }

    /// Truncate all partitions' WALs.
    pub fn truncate_all(&mut self) -> io::Result<()> {
        for wal in self.wals.values_mut() {
            wal.truncate()?;
        }
        Ok(())
    }

    /// Get the current LSN for a partition.
    pub fn lsn(&self, partition_id: u32) -> u64 {
        self.wals.get(&partition_id).map(|w| w.lsn()).unwrap_or(0)
    }

    /// Get the committed LSN for a partition.
    pub fn committed_lsn(&self, partition_id: u32) -> u64 {
        self.wals
            .get(&partition_id)
            .map(|w| w.committed_lsn())
            .unwrap_or(0)
    }

    /// Scan a partition's WAL for entries after the given LSN.
    pub fn scan_partition(
        &self,
        partition_id: u32,
        after_lsn: u64,
    ) -> io::Result<Vec<DurableWalEntry>> {
        let dir = self
            .base_dir
            .join("partitions")
            .join(partition_id.to_string());
        let wal_path = dir.join("graph.wal");
        if !wal_path.exists() {
            return Ok(Vec::new());
        }
        DurableWal::scan_after(&wal_path, after_lsn)
    }

    /// Replay a partition's WAL into the corresponding MutableCsrStore.
    pub fn replay_partition(
        &self,
        partition_id: u32,
        csr: &mut MutableCsrStore,
        after_lsn: u64,
    ) -> io::Result<usize> {
        let entries = self.scan_partition(partition_id, after_lsn)?;
        let count = entries.len();
        DurableWal::replay_into(&entries, csr);
        if count > 0 {
            csr.commit();
        }
        Ok(count)
    }

    /// Replay all partitions' WALs into a PartitionStoreSet.
    pub fn replay_all(&self, pss: &mut PartitionStoreSet, after_lsn: u64) -> io::Result<usize> {
        let mut total = 0;
        for pid in 0..self.partition_count {
            if let Some(store) = pss.partition_mut(pid) {
                total += self.replay_partition(pid, store, after_lsn)?;
            }
        }
        Ok(total)
    }

    /// Get WAL directory for a partition.
    pub fn partition_dir(&self, partition_id: u32) -> PathBuf {
        self.base_dir
            .join("partitions")
            .join(partition_id.to_string())
    }

    /// Number of partitions.
    pub fn partition_count(&self) -> u32 {
        self.partition_count
    }

    /// Total uncommitted entries across all partitions.
    pub fn total_uncommitted(&self) -> u64 {
        self.wals
            .values()
            .map(|w| w.lsn().saturating_sub(w.committed_lsn()))
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_core::DurablePropValue;
    use yata_grin::{Mutable, PropValue, Topology};

    #[test]
    fn test_partitioned_wal_open() {
        let dir = tempfile::tempdir().unwrap();
        let pwal = PartitionedWal::open(dir.path(), 4).unwrap();
        assert_eq!(pwal.partition_count(), 4);
        for pid in 0..4 {
            assert_eq!(pwal.lsn(pid), 0);
        }
    }

    #[test]
    fn test_append_to_specific_partition() {
        let dir = tempfile::tempdir().unwrap();
        let mut pwal = PartitionedWal::open(dir.path(), 4).unwrap();

        // Append to partition 2
        let lsn = pwal
            .append(
                2,
                DurableWalOp::CreateVertex {
                    node_id: "v1".into(),
                    global_vid: None,
                    labels: vec!["A".into()],
                    props: vec![("x".into(), DurablePropValue::Int(42))],
                },
            )
            .unwrap();
        assert_eq!(lsn, 1);
        assert_eq!(pwal.lsn(2), 1);

        // Other partitions unaffected
        assert_eq!(pwal.lsn(0), 0);
        assert_eq!(pwal.lsn(1), 0);
        assert_eq!(pwal.lsn(3), 0);
    }

    #[test]
    fn test_batch_append() {
        let dir = tempfile::tempdir().unwrap();
        let mut pwal = PartitionedWal::open(dir.path(), 2).unwrap();

        let ops = vec![
            DurableWalOp::CreateVertex {
                node_id: "a".into(),
                global_vid: None,
                labels: vec!["X".into()],
                props: vec![],
            },
            DurableWalOp::CreateVertex {
                node_id: "b".into(),
                global_vid: None,
                labels: vec!["X".into()],
                props: vec![],
            },
        ];
        let lsn = pwal.append_batch(0, ops).unwrap();
        assert_eq!(lsn, 2);
        assert_eq!(pwal.lsn(0), 2);
        assert_eq!(pwal.lsn(1), 0);
    }

    #[test]
    fn test_scan_partition() {
        let dir = tempfile::tempdir().unwrap();
        let mut pwal = PartitionedWal::open(dir.path(), 2).unwrap();

        pwal.append(
            0,
            DurableWalOp::CreateVertex {
                node_id: "p0_v1".into(),
                global_vid: None,
                labels: vec![],
                props: vec![],
            },
        )
        .unwrap();
        pwal.append(
            1,
            DurableWalOp::CreateVertex {
                node_id: "p1_v1".into(),
                global_vid: None,
                labels: vec![],
                props: vec![],
            },
        )
        .unwrap();
        pwal.append(
            0,
            DurableWalOp::CreateVertex {
                node_id: "p0_v2".into(),
                global_vid: None,
                labels: vec![],
                props: vec![],
            },
        )
        .unwrap();

        let p0_entries = pwal.scan_partition(0, 0).unwrap();
        assert_eq!(p0_entries.len(), 2);

        let p1_entries = pwal.scan_partition(1, 0).unwrap();
        assert_eq!(p1_entries.len(), 1);
    }

    #[test]
    fn test_truncate_partition() {
        let dir = tempfile::tempdir().unwrap();
        let mut pwal = PartitionedWal::open(dir.path(), 2).unwrap();

        for i in 0..5 {
            pwal.append(
                0,
                DurableWalOp::CreateVertex {
                    node_id: format!("v{}", i),
                    global_vid: None,
                    labels: vec![],
                    props: vec![],
                },
            )
            .unwrap();
        }
        pwal.append(
            1,
            DurableWalOp::CreateVertex {
                node_id: "p1".into(),
                global_vid: None,
                labels: vec![],
                props: vec![],
            },
        )
        .unwrap();

        // Truncate only partition 0 after LSN 3
        pwal.mark_committed(0, 3);
        pwal.truncate(0).unwrap();

        let p0_entries = pwal.scan_partition(0, 0).unwrap();
        assert_eq!(p0_entries.len(), 2); // LSN 4 and 5

        // Partition 1 unaffected
        let p1_entries = pwal.scan_partition(1, 0).unwrap();
        assert_eq!(p1_entries.len(), 1);
    }

    #[test]
    fn test_replay_partition() {
        let dir = tempfile::tempdir().unwrap();
        let mut pwal = PartitionedWal::open(dir.path(), 2).unwrap();

        pwal.append(
            0,
            DurableWalOp::CreateVertex {
                node_id: "alice".into(),
                global_vid: None,
                labels: vec!["Person".into()],
                props: vec![("name".into(), DurablePropValue::Str("Alice".into()))],
            },
        )
        .unwrap();
        pwal.append(
            0,
            DurableWalOp::CreateVertex {
                node_id: "bob".into(),
                global_vid: None,
                labels: vec!["Person".into()],
                props: vec![("name".into(), DurablePropValue::Str("Bob".into()))],
            },
        )
        .unwrap();

        let mut csr = MutableCsrStore::new_partition(0);
        let count = pwal.replay_partition(0, &mut csr, 0).unwrap();
        assert_eq!(count, 2);
        assert_eq!(csr.vertex_count(), 2);
    }

    #[test]
    fn test_replay_all() {
        let dir = tempfile::tempdir().unwrap();
        let mut pwal = PartitionedWal::open(dir.path(), 2).unwrap();

        pwal.append(
            0,
            DurableWalOp::CreateVertex {
                node_id: "p0_v1".into(),
                global_vid: None,
                labels: vec!["A".into()],
                props: vec![],
            },
        )
        .unwrap();
        pwal.append(
            1,
            DurableWalOp::CreateVertex {
                node_id: "p1_v1".into(),
                global_vid: None,
                labels: vec!["B".into()],
                props: vec![],
            },
        )
        .unwrap();

        use crate::partition::PartitionAssignment;
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 2 });
        let total = pwal.replay_all(&mut pss, 0).unwrap();
        assert_eq!(total, 2);

        // Each partition should have 1 vertex
        assert_eq!(pss.partition(0).unwrap().vertex_count(), 1);
        assert_eq!(pss.partition(1).unwrap().vertex_count(), 1);
    }

    #[test]
    fn test_total_uncommitted() {
        let dir = tempfile::tempdir().unwrap();
        let mut pwal = PartitionedWal::open(dir.path(), 2).unwrap();

        pwal.append(
            0,
            DurableWalOp::Commit {
                message: "a".into(),
            },
        )
        .unwrap();
        pwal.append(
            0,
            DurableWalOp::Commit {
                message: "b".into(),
            },
        )
        .unwrap();
        pwal.append(
            1,
            DurableWalOp::Commit {
                message: "c".into(),
            },
        )
        .unwrap();

        assert_eq!(pwal.total_uncommitted(), 3);

        pwal.mark_committed(0, 1);
        assert_eq!(pwal.total_uncommitted(), 2); // p0: 1 uncommitted, p1: 1
    }

    #[test]
    fn test_reopen_continues() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut pwal = PartitionedWal::open(dir.path(), 2).unwrap();
            pwal.append(
                0,
                DurableWalOp::Commit {
                    message: "first".into(),
                },
            )
            .unwrap();
            pwal.append(
                1,
                DurableWalOp::Commit {
                    message: "second".into(),
                },
            )
            .unwrap();
        }

        // Reopen
        let mut pwal = PartitionedWal::open(dir.path(), 2).unwrap();
        assert_eq!(pwal.lsn(0), 1);
        assert_eq!(pwal.lsn(1), 1);

        let lsn = pwal
            .append(
                0,
                DurableWalOp::Commit {
                    message: "third".into(),
                },
            )
            .unwrap();
        assert_eq!(lsn, 2);
    }

    #[test]
    fn test_nonexistent_partition_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut pwal = PartitionedWal::open(dir.path(), 2).unwrap();
        let result = pwal.append(
            99,
            DurableWalOp::Commit {
                message: "x".into(),
            },
        );
        assert!(result.is_err());
    }
}
