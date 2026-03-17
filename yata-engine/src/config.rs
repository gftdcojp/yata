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
    /// Max vertex set size for adjacency expansion in Lance pushdown.
    pub adj_expansion_limit: usize,
    /// Tokio runtime worker threads for async Lance I/O.
    pub runtime_workers: usize,
}

impl Default for TieredEngineConfig {
    fn default() -> Self {
        Self {
            hot_max_vertices: 50_000,
            hot_max_edges: 200_000,
            cache_max_entries: 256,
            cache_ttl_secs: 30,
            adj_expansion_limit: 5_000,
            runtime_workers: 2,
        }
    }
}
