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
