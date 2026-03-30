use yata_core::PartitionId;

/// Configuration for the tiered graph engine (LanceDB persistence).
#[derive(Debug, Clone)]
pub struct TieredEngineConfig {
    pub hot_max_vertices: usize,
    pub hot_max_edges: usize,
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
        assert_eq!(cfg.partition_count, 1);
        assert_eq!(cfg.wal_ring_capacity, 100_000);
    }

    #[test]
    fn test_default_config_partition_id_is_zero() {
        let cfg = TieredEngineConfig::default();
        assert_eq!(cfg.hot_partition_id, PartitionId::from(0));
    }
}
