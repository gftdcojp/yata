use std::collections::HashMap;
use yata_core::PartitionId;
use yata_store::partition::PartitionAssignment;

/// WAL segment serialization format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalFormat {
    /// NDJSON (legacy, default for backward compatibility).
    Ndjson,
    /// Arrow IPC File format (zero-copy mmap, Phase 0 of Shannon-optimal architecture).
    Arrow,
}

impl WalFormat {
    pub fn from_env() -> Self {
        match std::env::var("YATA_WAL_FORMAT").as_deref() {
            Ok("ndjson") => WalFormat::Ndjson,
            _ => WalFormat::Arrow, // Default: Arrow IPC (Phase 1)
        }
    }
}

/// Configuration for the tiered graph engine (WAL Projection only).
#[derive(Debug, Clone)]
pub struct TieredEngineConfig {
    pub hot_max_vertices: usize,
    pub hot_max_edges: usize,
    pub cache_max_entries: usize,
    pub cache_ttl_secs: u64,
    pub adj_expansion_limit: usize,
    pub runtime_workers: usize,
    pub batch_commit_threshold: u32,
    pub batch_commit_timeout_ms: u64,
    pub enable_ocel_events: bool,
    pub app_id: String,
    pub org_id: String,
    pub lazy_label_max_loaded: usize,
    pub hot_partition_id: PartitionId,
    pub partition_count: u32,
    pub blob_cache_budget_mb: u64,
    /// WAL ring buffer capacity (entries). Write Container only.
    pub wal_ring_capacity: usize,
    /// WAL segment max bytes before R2 flush.
    pub wal_segment_max_bytes: usize,
    /// WAL segment max age (seconds) before R2 flush.
    pub wal_segment_max_age_secs: u64,
    /// WAL checkpoint interval (seconds). ArrowFragment for cold start.
    pub wal_checkpoint_interval_secs: u64,
    /// WAL segment format (ndjson or arrow). Arrow enables zero-copy mmap reads.
    pub wal_format: WalFormat,
    /// Partition assignment strategy (derived from YATA_PARTITION_LABELS env var).
    /// Format: "Label1=0,Label2=1,Label3=1" (label=partition_id pairs).
    /// When set, enables label-based partitioning for dedicated label isolation.
    pub partition_assignment: PartitionAssignment,
}

impl Default for TieredEngineConfig {
    fn default() -> Self {
        let app_id = std::env::var("PERFORMER_ID")
            .or_else(|_| std::env::var("SPIN_VARIABLE_PERFORMER_ID"))
            .or_else(|_| std::env::var("APP_ID"))
            .unwrap_or_default();
        let org_id = std::env::var("YATA_ORG_ID")
            .or_else(|_| {
                std::env::var("YATA_S3_PREFIX").map(|p| p.trim_end_matches('/').to_string())
            })
            .unwrap_or_default();
        tracing::info!(
            app_id = %app_id,
            org_id = %org_id,
            "TieredEngineConfig::default()"
        );
        Self {
            hot_max_vertices: 50_000,
            hot_max_edges: 200_000,
            cache_max_entries: 256,
            cache_ttl_secs: 30,
            adj_expansion_limit: 5_000,
            runtime_workers: 2,
            batch_commit_threshold: std::env::var("YATA_BATCH_COMMIT_THRESHOLD")
                .ok().and_then(|s| s.parse().ok()).unwrap_or(1),
            batch_commit_timeout_ms: std::env::var("YATA_BATCH_COMMIT_TIMEOUT_MS")
                .ok().and_then(|s| s.parse().ok()).unwrap_or(2_000),
            enable_ocel_events: std::env::var("YATA_OCEL_EVENTS")
                .map(|s| s == "true" || s == "1").unwrap_or(false),
            app_id,
            org_id,
            lazy_label_max_loaded: std::env::var("YATA_LAZY_LABEL_MAX")
                .ok().and_then(|s| s.parse().ok()).unwrap_or(12),
            hot_partition_id: std::env::var("YATA_HOT_PARTITION_ID")
                .ok().and_then(|s| s.parse::<u32>().ok())
                .map(PartitionId::from).unwrap_or_else(|| PartitionId::from(0)),
            partition_count: std::env::var("YATA_PARTITION_COUNT")
                .ok().and_then(|s| s.parse().ok()).unwrap_or(1),
            blob_cache_budget_mb: std::env::var("YATA_VINEYARD_BUDGET_MB")
                .ok().and_then(|s| s.parse().ok()).unwrap_or(256),
            wal_ring_capacity: std::env::var("YATA_WAL_RING_CAPACITY")
                .ok().and_then(|s| s.parse().ok()).unwrap_or(100_000),
            wal_segment_max_bytes: std::env::var("YATA_WAL_SEGMENT_MAX_BYTES")
                .ok().and_then(|s| s.parse().ok()).unwrap_or(1_048_576),
            wal_segment_max_age_secs: std::env::var("YATA_WAL_SEGMENT_MAX_AGE_SECS")
                .ok().and_then(|s| s.parse().ok()).unwrap_or(10),
            wal_checkpoint_interval_secs: std::env::var("YATA_WAL_CHECKPOINT_INTERVAL_SECS")
                .ok().and_then(|s| s.parse().ok()).unwrap_or(300),
            wal_format: WalFormat::from_env(),
            partition_assignment: parse_partition_assignment(),
        }
    }
}

/// Parse YATA_PARTITION_LABELS env var into PartitionAssignment.
///
/// Format: "Expert=1,Key=1" → Label { label_map: {Expert→1, Key→1}, default_partition: 0 }
/// Unset or empty → uses YATA_PARTITION_COUNT for Hash or Single.
fn parse_partition_assignment() -> PartitionAssignment {
    if let Ok(labels_str) = std::env::var("YATA_PARTITION_LABELS") {
        let labels_str = labels_str.trim();
        if labels_str.is_empty() {
            return fallback_assignment();
        }
        let mut label_map = HashMap::new();
        for pair in labels_str.split(',') {
            let pair = pair.trim();
            if let Some((label, pid_str)) = pair.split_once('=') {
                if let Ok(pid) = pid_str.trim().parse::<u32>() {
                    label_map.insert(label.trim().to_string(), pid);
                }
            }
        }
        if label_map.is_empty() {
            return fallback_assignment();
        }
        let default_partition = std::env::var("YATA_PARTITION_DEFAULT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        tracing::info!(
            labels = ?label_map,
            default_partition,
            "label-based partitioning enabled"
        );
        PartitionAssignment::Label {
            label_map,
            default_partition,
        }
    } else {
        fallback_assignment()
    }
}

fn fallback_assignment() -> PartitionAssignment {
    let count: u32 = std::env::var("YATA_PARTITION_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    if count <= 1 {
        PartitionAssignment::Single
    } else {
        PartitionAssignment::Hash {
            partition_count: count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_values() {
        let cfg = TieredEngineConfig::default();
        assert_eq!(cfg.hot_max_vertices, 50_000);
        assert_eq!(cfg.cache_max_entries, 256);
        assert_eq!(cfg.partition_count, 1);
        assert_eq!(cfg.wal_ring_capacity, 100_000);
        assert_eq!(cfg.wal_segment_max_bytes, 1_048_576);
        assert_eq!(cfg.wal_segment_max_age_secs, 10);
        assert_eq!(cfg.wal_checkpoint_interval_secs, 300);
    }

    #[test]
    fn test_default_config_partition_id_is_zero() {
        let cfg = TieredEngineConfig::default();
        assert_eq!(cfg.hot_partition_id, PartitionId::from(0));
    }

    #[test]
    fn test_parse_partition_assignment_single() {
        // No env vars set → Single
        let assignment = fallback_assignment();
        assert!(matches!(assignment, PartitionAssignment::Single));
    }

    #[test]
    fn test_parse_partition_labels_format() {
        // Simulate parsing logic directly
        let labels_str = "Expert=1,Key=1";
        let mut label_map = HashMap::new();
        for pair in labels_str.split(',') {
            if let Some((label, pid_str)) = pair.split_once('=') {
                if let Ok(pid) = pid_str.trim().parse::<u32>() {
                    label_map.insert(label.trim().to_string(), pid);
                }
            }
        }
        assert_eq!(label_map.len(), 2);
        assert_eq!(label_map["Expert"], 1);
        assert_eq!(label_map["Key"], 1);

        let assignment = PartitionAssignment::Label {
            label_map,
            default_partition: 0,
        };
        assert_eq!(assignment.partition_count(), 2);
    }
}
