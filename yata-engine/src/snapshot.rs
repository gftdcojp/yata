//! Snapshot manifest types for R2 persistence.
//!
//! Legacy SnapshotBundle / serialize_snapshot / restore_snapshot_bundle are removed.
//! ArrowFragment (yata-vineyard) is the canonical snapshot format.
//! This module retains only SnapshotManifest for backward-compat manifest.json parsing.

use serde::{Deserialize, Serialize};
use yata_core::PartitionId;

/// Snapshot manifest stored as `{prefix}snap/manifest.json`.
/// Used for backward-compat parsing of existing R2 manifests.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub version: u32,
    #[serde(default)]
    pub format: String,
    pub partition_id: PartitionId,
    #[serde(default)]
    pub wal_lsn: u64,
    #[serde(default)]
    pub timestamp_ns: i64,
    pub vertex_count: u64,
    pub edge_count: u64,
    #[serde(default)]
    pub vertex_labels: Vec<String>,
    #[serde(default)]
    pub edge_labels: Vec<String>,
    #[serde(default = "default_partition_count")]
    pub partition_count: u32,
    #[serde(default)]
    pub global_map_count: u64,
    #[serde(default)]
    pub blob_count: u32,
}

fn default_partition_count() -> u32 {
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_manifest_serialize_roundtrip() {
        let manifest = SnapshotManifest {
            version: 3,
            format: "arrow_fragment".into(),
            partition_id: PartitionId::from(0),
            wal_lsn: 100,
            timestamp_ns: 1711300000_000_000_000,
            vertex_count: 5000,
            edge_count: 12000,
            vertex_labels: vec!["Person".into(), "Company".into()],
            edge_labels: vec!["WORKS_AT".into()],
            partition_count: 1,
            global_map_count: 0,
            blob_count: 8,
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: SnapshotManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.version, 3);
        assert_eq!(parsed.format, "arrow_fragment");
        assert_eq!(parsed.vertex_count, 5000);
        assert_eq!(parsed.edge_count, 12000);
        assert_eq!(parsed.vertex_labels.len(), 2);
        assert_eq!(parsed.edge_labels.len(), 1);
        assert_eq!(parsed.blob_count, 8);
    }

    #[test]
    fn test_snapshot_manifest_defaults_on_missing_fields() {
        let json = r#"{"version": 1, "partition_id": 0, "vertex_count": 10, "edge_count": 5}"#;
        let parsed: SnapshotManifest = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.version, 1);
        assert_eq!(parsed.format, ""); // default empty
        assert_eq!(parsed.wal_lsn, 0);
        assert_eq!(parsed.timestamp_ns, 0);
        assert_eq!(parsed.partition_count, 1); // default_partition_count
        assert!(parsed.vertex_labels.is_empty());
        assert!(parsed.edge_labels.is_empty());
        assert_eq!(parsed.global_map_count, 0);
        assert_eq!(parsed.blob_count, 0);
    }

    #[test]
    fn test_snapshot_manifest_with_partition_count() {
        let json = r#"{"version": 2, "partition_id": 3, "vertex_count": 100, "edge_count": 200, "partition_count": 4}"#;
        let parsed: SnapshotManifest = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.partition_count, 4);
        assert_eq!(parsed.partition_id, PartitionId::from(3));
    }
}
