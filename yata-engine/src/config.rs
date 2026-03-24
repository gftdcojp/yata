use yata_core::PartitionId;

/// Persistence mode for the graph engine.
/// Snapshot-only architecture. MDAG mode removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistenceMode {
    /// GraphScope Flex-style: periodic Arrow IPC snapshot to R2. ONLY mode.
    Snapshot,
}

/// Configuration for the tiered graph engine.
#[derive(Debug, Clone)]
pub struct TieredEngineConfig {
    /// Max vertices in HOT tier before eviction.
    pub hot_max_vertices: usize,
    /// Max edges in HOT tier.
    pub hot_max_edges: usize,
    /// Query cache max entries.
    pub cache_max_entries: usize,
    /// Query cache TTL in seconds.
    pub cache_ttl_secs: u64,
    /// Max vertex set size for adjacency expansion.
    pub adj_expansion_limit: usize,
    /// Tokio runtime worker threads for async I/O.
    pub runtime_workers: usize,
    /// Batch commit: accumulate N mutations before triggering snapshot commit.
    /// 1 = immediate (legacy), >1 = batched.
    pub batch_commit_threshold: u32,
    /// Batch commit: max time (ms) before flushing even if threshold not reached.
    pub batch_commit_timeout_ms: u64,
    /// Auto-create :OcelEvent nodes for every mutation (OCEL v2 audit trail).
    pub enable_ocel_events: bool,
    /// App ID for mutation metadata injection (e.g. "intel"). Resolved at engine creation.
    pub app_id: String,
    /// Org ID for mutation metadata injection (e.g. "gftd"). Resolved at engine creation.
    pub org_id: String,
    /// Max labels to keep loaded in HOT CSR. LRU eviction when exceeded. 0 = unlimited.
    pub lazy_label_max_loaded: usize,
    /// Directory for DurableWal files. Defaults to `{data_dir}/wal/`.
    pub wal_dir: Option<String>,
    /// Persistence mode: Snapshot only.
    pub persistence_mode: PersistenceMode,
    /// Partition ID for the HOT in-memory store. Default is partition 0.
    pub hot_partition_id: PartitionId,
    /// Number of partitions for the graph store. 1 = single partition (legacy).
    /// >1 enables partition-aware storage, selective checkpoint, and partition pruning.
    pub partition_count: u32,
    /// Vineyard blob cache budget in MB. 0 = unlimited. Default: 256.
    pub vineyard_budget_mb: u64,
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
            performer_raw = ?std::env::var("PERFORMER_ID"),
            org_raw = ?std::env::var("YATA_ORG_ID"),
            s3_prefix_raw = ?std::env::var("YATA_S3_PREFIX"),
            "TieredEngineConfig::default() env resolution"
        );
        Self {
            hot_max_vertices: 50_000,
            hot_max_edges: 200_000,
            cache_max_entries: 256,
            cache_ttl_secs: 30,
            adj_expansion_limit: 5_000,
            runtime_workers: 2,
            batch_commit_threshold: std::env::var("YATA_BATCH_COMMIT_THRESHOLD")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1),
            batch_commit_timeout_ms: std::env::var("YATA_BATCH_COMMIT_TIMEOUT_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(2_000),
            enable_ocel_events: std::env::var("YATA_OCEL_EVENTS")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(false),
            app_id,
            org_id,
            lazy_label_max_loaded: std::env::var("YATA_LAZY_LABEL_MAX")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(12),
            wal_dir: std::env::var("YATA_WAL_DIR").ok(),
            persistence_mode: PersistenceMode::Snapshot,
            hot_partition_id: std::env::var("YATA_HOT_PARTITION_ID")
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
                .map(PartitionId::from)
                .unwrap_or_else(|| PartitionId::from(0)),
            partition_count: std::env::var("YATA_PARTITION_COUNT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1),
            vineyard_budget_mb: std::env::var("YATA_VINEYARD_BUDGET_MB")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(256),
        }
    }
}
