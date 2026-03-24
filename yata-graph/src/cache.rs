//! Graph-level cache: CSR store + query result LRU.
//!
//! Lives inside `GraphStore` so all consumers (gateway, bolt,
//! magatama-server) get caching for free.
//!
//! - CSR hit (ns) or query cache hit (us)
//! - Writes: update in-memory graph + bump generation

use std::collections::HashMap;
use std::time::Instant;

use yata_cypher::MemoryGraph;

/// Cache configuration.
#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// Max query result cache entries.
    pub max_query_entries: usize,
    /// Query result TTL in seconds.
    pub query_ttl_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_query_entries: 256,
            query_ttl_secs: 30,
        }
    }
}

/// Cached query result entry.
struct QueryEntry {
    rows: Vec<Vec<(String, String)>>,
    created: Instant,
    generation: u64,
}

/// Graph cache: CSR store + query result LRU.
pub struct GraphCache {
    config: CacheConfig,
    /// Cached CSR-indexed MemoryGraph. `None` = cold (needs external restore).
    csr: Option<MemoryGraph>,
    /// Generation counter. Bumped on every write. Query cache entries with
    /// older generation are stale.
    generation: u64,
    /// Query result cache. Key = blake3(cypher + params).
    query_results: HashMap<[u8; 32], QueryEntry>,
    /// LRU order (oldest first).
    lru_order: Vec<[u8; 32]>,
}

impl GraphCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config,
            csr: None,
            generation: 0,
            query_results: HashMap::new(),
            lru_order: Vec::new(),
        }
    }

    /// Get the cached CSR MemoryGraph if available.
    pub fn get_csr(&self) -> Option<&MemoryGraph> {
        self.csr.as_ref()
    }

    /// Store a freshly loaded MemoryGraph as the CSR cache.
    pub fn set_csr(&mut self, graph: MemoryGraph) {
        self.csr = Some(graph);
    }

    /// Current generation.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Invalidate the CSR cache and bump generation (called after writes).
    pub fn invalidate(&mut self) {
        self.csr = None;
        self.generation += 1;
        tracing::debug!(generation = self.generation, "graph cache invalidated");
    }

    /// Look up a cached query result.
    pub fn get_query(&self, key: &[u8; 32]) -> Option<&Vec<Vec<(String, String)>>> {
        let entry = self.query_results.get(key)?;
        // Check generation (stale if written since cached)
        if entry.generation != self.generation {
            return None;
        }
        // Check TTL
        if entry.created.elapsed().as_secs() > self.config.query_ttl_secs {
            return None;
        }
        Some(&entry.rows)
    }

    /// Store a query result.
    pub fn put_query(&mut self, key: [u8; 32], rows: Vec<Vec<(String, String)>>) {
        // Evict oldest if at capacity
        while self.query_results.len() >= self.config.max_query_entries {
            if let Some(oldest) = self.lru_order.first().copied() {
                self.lru_order.remove(0);
                self.query_results.remove(&oldest);
            } else {
                break;
            }
        }
        // Remove existing entry from LRU if present
        self.lru_order.retain(|k| k != &key);
        self.lru_order.push(key);
        self.query_results.insert(
            key,
            QueryEntry {
                rows,
                created: Instant::now(),
                generation: self.generation,
            },
        );
    }

    /// Compute cache key from cypher + params.
    pub fn cache_key(cypher: &str, params: &[(String, String)]) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(cypher.as_bytes());
        for (k, v) in params {
            hasher.update(k.as_bytes());
            hasher.update(v.as_bytes());
        }
        *hasher.finalize().as_bytes()
    }

    /// Stats for observability.
    pub fn stats(&self) -> CacheStats {
        use yata_cypher::Graph;
        CacheStats {
            csr_loaded: self.csr.is_some(),
            csr_nodes: self.csr.as_ref().map(|g| g.nodes().len()).unwrap_or(0),
            csr_edges: self.csr.as_ref().map(|g| g.rels().len()).unwrap_or(0),
            generation: self.generation,
            query_entries: self.query_results.len(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CacheStats {
    pub csr_loaded: bool,
    pub csr_nodes: usize,
    pub csr_edges: usize,
    pub generation: u64,
    pub query_entries: usize,
}
