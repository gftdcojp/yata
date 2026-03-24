//! R2 streaming read — CSR topology loaded on-demand from R2 without full materialization.
//! Long-term goal: eliminate the need to load entire Arrow IPC files into CSR.
//! Instead, stream adjacency lists from R2 as queries traverse the graph.

/// Trait for streaming adjacency list access from external storage.
pub trait StreamingAdjacency: Send + Sync {
    /// Get outgoing neighbors for a vertex, fetching from R2 if not cached.
    fn stream_out_neighbors(&self, vid: u32, label: &str) -> Vec<u32>;

    /// Get outgoing neighbors across all labels.
    fn stream_out_neighbors_all(&self, vid: u32) -> Vec<(u32, String)>;

    /// Prefetch adjacency data for a set of vertices (batch optimization).
    fn prefetch(&self, vids: &[u32]) -> Result<(), String>;

    /// Cache statistics.
    fn cache_stats(&self) -> StreamingCacheStats;
}

#[derive(Debug, Clone, Default)]
pub struct StreamingCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub prefetches: u64,
    pub bytes_fetched: u64,
}

/// Stub implementation that returns empty results (no streaming, all in-memory).
pub struct NullStreaming;

impl StreamingAdjacency for NullStreaming {
    fn stream_out_neighbors(&self, _vid: u32, _label: &str) -> Vec<u32> {
        vec![]
    }

    fn stream_out_neighbors_all(&self, _vid: u32) -> Vec<(u32, String)> {
        vec![]
    }

    fn prefetch(&self, _vids: &[u32]) -> Result<(), String> {
        Ok(())
    }

    fn cache_stats(&self) -> StreamingCacheStats {
        StreamingCacheStats::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_streaming_returns_empty() {
        let s = NullStreaming;
        assert!(s.stream_out_neighbors(0, "knows").is_empty());
        assert!(s.stream_out_neighbors_all(0).is_empty());
        assert!(s.prefetch(&[0, 1, 2]).is_ok());
    }

    #[test]
    fn test_cache_stats_default() {
        let stats = StreamingCacheStats::default();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.prefetches, 0);
        assert_eq!(stats.bytes_fetched, 0);

        let s = NullStreaming;
        let stats = s.cache_stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
    }
}
