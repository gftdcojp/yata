use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use yata_cypher::Graph;
use yata_graph::{LanceGraphStore, QueryableGraph};
use yata_store::MutableCsrStore;

use crate::cache::{cache_key, QueryCache};
use crate::config::TieredEngineConfig;
use crate::loader;
use crate::rls;
use crate::router;
use crate::writer;

/// Shared tokio runtime for all engine instances (avoids nested runtime issues).
static ENGINE_RT: std::sync::LazyLock<tokio::runtime::Runtime> =
    std::sync::LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("yata-engine")
            .build()
            .expect("yata-engine tokio runtime")
    });

/// Tiered graph engine: HOT (MutableCSR) → WARM (Lance local) → COLD (Lance B2).
///
/// This struct absorbs all logic previously in magatama-host/graph_host.rs.
pub struct TieredGraphEngine {
    config: TieredEngineConfig,
    /// HOT tier: in-memory CSR store.
    hot: Mutex<MutableCsrStore>,
    /// WARM tier: Lance store (local PVC).
    warm: LanceGraphStore,
    /// Query result cache.
    cache: Mutex<QueryCache>,
    /// Whether the HOT tier has been initialized from WARM.
    hot_initialized: AtomicBool,
}

impl TieredGraphEngine {
    /// Create a new tiered engine backed by a Lance store at `lance_uri`.
    pub fn new(config: TieredEngineConfig, lance_uri: &str) -> Self {
        let init_lance = LanceGraphStore::new(lance_uri);
        let warm = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(init_lance))
        } else {
            ENGINE_RT.block_on(init_lance)
        }
        .expect("failed to init Lance graph store");

        let cache = QueryCache::new(config.cache_max_entries, config.cache_ttl_secs);

        Self {
            config,
            hot: Mutex::new(MutableCsrStore::new()),
            warm,
            cache: Mutex::new(cache),
            hot_initialized: AtomicBool::new(false),
        }
    }

    /// Run an async future, handling both inside-runtime and outside-runtime contexts.
    fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Inside a runtime — use block_in_place to avoid nesting panic
            tokio::task::block_in_place(|| handle.block_on(f))
        } else {
            // Outside a runtime — use the shared static runtime
            ENGINE_RT.block_on(f)
        }
    }

    /// Ensure HOT tier is loaded from WARM (Lance). Idempotent.
    fn ensure_hot(&self) {
        if self.hot_initialized.load(Ordering::Relaxed) {
            return;
        }
        let result = self.block_on(loader::load_csr_from_lance(&self.warm));
        match result {
            Ok(csr) => {
                if let Ok(mut hot) = self.hot.lock() {
                    *hot = csr;
                }
                self.hot_initialized.store(true, Ordering::Relaxed);
            }
            Err(e) => {
                tracing::warn!("engine: failed to load CSR from Lance: {e}");
            }
        }
    }

    /// Main query entry point (sync, for WIT host compatibility).
    pub fn query(
        &self,
        cypher: &str,
        params: &[(String, String)],
        rls_org_id: Option<&str>,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        let is_mutation = router::is_cypher_mutation(cypher);

        // Cache lookup (reads only)
        if !is_mutation {
            let k = cache_key(cypher, params, rls_org_id);
            if let Ok(c) = self.cache.lock() {
                if let Some(rows) = c.get(&k) {
                    tracing::trace!("engine: cache hit");
                    return Ok(rows.clone());
                }
            }
        }

        if !is_mutation {
            // Read path: use HOT CSR (no Lance I/O)
            self.ensure_hot();
            if let Ok(csr) = self.hot.lock() {
                let mut g = if let Some((labels, rel_types)) =
                    router::extract_pushdown_hints(cypher)
                {
                    QueryableGraph(csr.to_filtered_memory_graph(&labels, &rel_types))
                } else {
                    QueryableGraph(csr.to_full_memory_graph())
                };

                if let Some(org_id) = rls_org_id {
                    rls::apply_rls_filter(&mut g.0, org_id);
                }

                let rows = g.query(cypher, params).map_err(|e| e.to_string())?;

                let k = cache_key(cypher, params, rls_org_id);
                if let Ok(mut c) = self.cache.lock() {
                    c.put(k, rows.clone());
                }

                return Ok(rows);
            }
            // CSR lock failed — fall through to Lance path
        }

        // Mutation path (or CSR lock failure): Lance + CSR update
        let max_n = self.config.hot_max_vertices;
        let max_e = self.config.hot_max_edges;
        let rls_owned = rls_org_id.map(String::from);

        self.block_on(async {
            // Build MemoryGraph from CSR for mutations
            self.ensure_hot();
            let mut g = if let Ok(csr) = self.hot.lock() {
                QueryableGraph(csr.to_full_memory_graph())
            } else {
                self.warm
                    .to_memory_graph_bounded(max_n, max_e)
                    .await
                    .map_err(|e| format!("graph load: {e}"))?
            };

            if let Some(ref org_id) = rls_owned {
                rls::apply_rls_filter(&mut g.0, org_id);
            }

            // Mutation tracking: before snapshot
            let before_vids: HashSet<String> =
                g.0.nodes().iter().map(|n| n.id.clone()).collect();
            let before_eids: HashSet<String> =
                g.0.rels().iter().map(|r| r.id.clone()).collect();
            let before_snapshot: HashMap<String, String> = g
                .0
                .nodes()
                .iter()
                .map(|n| (n.id.clone(), format!("{:?}{:?}", n.labels, n.props)))
                .collect();

            // Execute Cypher
            let rows = g.query(cypher, params).map_err(|e| e.to_string())?;

            // Delta persist to Lance
            let stats = writer::write_delta(
                &self.warm,
                &before_vids,
                &before_eids,
                &before_snapshot,
                &g,
                rls_owned.as_deref(),
            )
            .await?;

            // Rebuild HOT CSR from post-mutation state
            if stats.new_vertices > 0 || stats.modified_vertices > 0 || stats.new_edges > 0 {
                let new_csr = loader::rebuild_csr_from_graph(&g);
                if let Ok(mut hot) = self.hot.lock() {
                    *hot = new_csr;
                    self.hot_initialized.store(true, Ordering::Relaxed);
                    tracing::debug!("engine: CSR rebuilt after mutation");
                }
            }

            // Invalidate cache on mutation
            if let Ok(mut c) = self.cache.lock() {
                c.invalidate();
            }

            Ok(rows)
        })
    }

    /// Force reload HOT tier from WARM (Lance).
    pub fn reload_hot(&self) -> Result<(), String> {
        self.hot_initialized.store(false, Ordering::Relaxed);
        self.ensure_hot();
        if let Ok(mut c) = self.cache.lock() {
            c.invalidate();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_engine(dir: &tempfile::TempDir) -> TieredGraphEngine {
        TieredGraphEngine::new(TieredEngineConfig::default(), dir.path().to_str().unwrap())
    }

    fn run_query(
        engine: &TieredGraphEngine,
        cypher: &str,
        params: &[(String, String)],
        rls: Option<&str>,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        engine.query(cypher, params, rls)
    }

    #[test]
    fn test_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let engine = make_engine(&dir);
        run_query(&engine, "CREATE (a:Person {name: 'Alice', age: 30})", &[], None).unwrap();
        let rows = run_query(&engine, "MATCH (n:Person) RETURN n.name AS name", &[], None).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_write_read_consistency() {
        let dir = tempfile::tempdir().unwrap();
        let engine = make_engine(&dir);
        run_query(&engine, "CREATE (e:Entity {eid: 'e1', name: 'Test'})", &[], None).unwrap();
        run_query(&engine, "CREATE (ev:Evidence {evid: 'ev1', eid: 'e1', cat: 'Fraud'})", &[], None).unwrap();
        let rows = run_query(&engine, "MATCH (ev:Evidence {eid: 'e1'}) RETURN ev.evid AS id, ev.cat AS cat", &[], None).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_rls() {
        let dir = tempfile::tempdir().unwrap();
        let engine = make_engine(&dir);
        run_query(&engine, "CREATE (n:Item {name: 'A', org_id: 'org_1'})", &[], None).unwrap();
        run_query(&engine, "CREATE (n:Item {name: 'B', org_id: 'org_2'})", &[], None).unwrap();
        run_query(&engine, "CREATE (n:Schema {name: 'System'})", &[], None).unwrap();
        let rows = run_query(&engine, "MATCH (n) RETURN n.name AS name", &[], Some("org_1")).unwrap();
        let names: Vec<&str> = rows
            .iter()
            .flat_map(|r| r.iter().find(|(c, _)| c == "name").map(|(_, v)| v.as_str()))
            .collect();
        assert!(names.contains(&"\"A\""));
        assert!(names.contains(&"\"System\""));
        assert!(!names.contains(&"\"B\""));
    }

    #[test]
    fn test_cache_hit() {
        let dir = tempfile::tempdir().unwrap();
        let engine = make_engine(&dir);
        run_query(&engine, "CREATE (p:Person {name: 'Alice'})", &[], None).unwrap();
        run_query(&engine, "MATCH (n:Person) RETURN n.name", &[], None).unwrap();
        let rows = run_query(&engine, "MATCH (n:Person) RETURN n.name", &[], None).unwrap();
        assert_eq!(rows.len(), 1);
    }
}
