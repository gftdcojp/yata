use yata_core::PartitionId;

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
    /// WAL segment format (ndjson or arrow). Arrow enables zero-copy mmap reads.
    pub wal_format: WalFormat,
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
            wal_format: WalFormat::from_env(),
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
    }

    #[test]
    fn test_default_config_partition_id_is_zero() {
        let cfg = TieredEngineConfig::default();
        assert_eq!(cfg.hot_partition_id, PartitionId::from(0));
    }

}
