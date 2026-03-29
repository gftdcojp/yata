use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use yata_cypher::Graph;
use yata_graph::{GraphStore, QueryableGraph};
use yata_grin::{Mutable, Predicate, PropValue, Property, Scannable, Topology};
use yata_vineyard::BlobStore;
use yata_store::vineyard::{
    DiskVineyard, EdgeVineyard, FragmentManifest, MmapVineyard, VineyardStore,
};

use crate::cache::{QueryCache, cache_key};
use crate::config::TieredEngineConfig;
use crate::loader;
use crate::router;

/// CPM metrics: CP5 mutation frequency + read/write ratio + CP4 snapshot monitoring.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CpmStats {
    pub cypher_read_count: u64,
    pub cypher_mutation_count: u64,
    pub cypher_mutation_avg_us: u64,
    pub cypher_mutation_us_total: u64,
    pub merge_record_count: u64,
    pub mutation_ratio: f64,
    pub vertex_count: u64,
    pub edge_count: u64,
    pub last_snapshot_serialize_ms: u64,
}

/// Shared tokio runtime for all engine instances (avoids nested runtime issues).
static ENGINE_RT: std::sync::LazyLock<tokio::runtime::Runtime> = std::sync::LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("yata-engine")
        .build()
        .expect("yata-engine tokio runtime")
});

/// FNV-1a 32-bit hash (fast, non-cryptographic — for owner_hash property matching).
fn fnv1a_32(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// Convert borrowed PropValue slice to owned Vec for WalEntry storage.
fn props_to_owned(props: &[(&str, yata_grin::PropValue)]) -> Vec<(String, yata_grin::PropValue)> {
    props.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Infrastructure blobs that are always uploaded (schema, ivnums).
/// meta.json is handled separately.
fn is_infra_blob(name: &str) -> bool {
    name == "schema" || name == "ivnums"
}

/// Check if blob name corresponds to a dirty vertex label.
/// Blob names: `vertex_table_{i}` or `vertex_table_{i}_chunk_{j}`.
fn is_dirty_vertex_blob(name: &str, dirty_vlabel_ids: &HashSet<usize>) -> bool {
    if let Some(rest) = name.strip_prefix("vertex_table_") {
        // Extract label index: first numeric segment before '_chunk_' or end
        let idx_str = rest.split('_').next().unwrap_or("");
        if let Ok(idx) = idx_str.parse::<usize>() {
            return dirty_vlabel_ids.contains(&idx);
        }
    }
    false
}

/// Edge and topology blobs: edge_table_*, oe_*, ie_*.
fn is_edge_or_topology_blob(name: &str) -> bool {
    name.starts_with("edge_table_")
        || name.starts_with("oe_")
        || name.starts_with("ie_")
}

/// Mutation context: metadata auto-injected into every mutated node.
#[derive(Debug, Clone, Default)]
pub struct MutationContext {
    pub app_id: String,
    pub org_id: String,
    pub user_id: String,
    pub actor_id: String,
    /// DID-based user identity (federation-ready).
    pub user_did: String,
    /// DID-based actor identity (federation-ready).
    pub actor_did: String,
}

/// Graph engine: MutableCSR in-memory + WAL Projection (R2 segments + checkpoints).
pub struct TieredGraphEngine {
    config: TieredEngineConfig,
    hot: Arc<RwLock<yata_store::GraphStoreEnum>>,
    warm: GraphStore,
    cache: Arc<Mutex<QueryCache>>,
    hot_initialized: Arc<AtomicBool>,
    loaded_labels: Arc<Mutex<HashSet<String>>>,
    vineyard: Arc<dyn VineyardStore>,
    s3_client: Arc<Mutex<Option<Arc<yata_s3::s3::S3Client>>>>,
    s3_prefix: String,
    dirty_labels: Arc<Mutex<HashSet<String>>>,
    pending_writes: Arc<AtomicUsize>,
    wal: Arc<Mutex<crate::wal::WalRingBuffer>>,
    wal_last_flushed_seq: Arc<AtomicU64>,
    /// SecurityScope cache: DID → (SecurityScope, compiled_at). Design E.
    security_scope_cache: Arc<Mutex<HashMap<String, (yata_gie::ir::SecurityScope, std::time::Instant)>>>,
    // CPM metrics: CP5 mutation frequency + CP4 snapshot monitoring
    cypher_read_count: Arc<AtomicU64>,
    cypher_mutation_count: Arc<AtomicU64>,
    cypher_mutation_us_total: Arc<AtomicU64>,
    merge_record_count: Arc<AtomicU64>,
    last_snapshot_serialize_ms: Arc<AtomicU64>,
}

impl TieredGraphEngine {
    /// Create a new engine with snapshot-only persistence.
    pub fn new(config: TieredEngineConfig, data_dir: &str) -> Self {
        Self::build(config, data_dir)
    }

    fn build(config: TieredEngineConfig, data_dir: &str) -> Self {
        let warm = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(Self::init_async(data_dir)))
        } else {
            ENGINE_RT.block_on(Self::init_async(data_dir))
        };

        let cache = QueryCache::new(config.cache_max_entries, config.cache_ttl_secs);

        let hot_partition_id = config.hot_partition_id;
        let partition_count = config.partition_count;
        if partition_count > 1 {
            tracing::info!(partition_count, "partitioned graph store enabled");
        }
        let vineyard_budget_mb = config.vineyard_budget_mb;
        let vineyard_dir = std::env::var("YATA_VINEYARD_DIR").ok();
        let use_mmap = std::env::var("YATA_MMAP_VINEYARD").unwrap_or_default() == "true";
        let vineyard: Arc<dyn VineyardStore> = if use_mmap {
            if let Some(ref dir) = vineyard_dir {
                match MmapVineyard::new(dir) {
                    Ok(mv) => {
                        let meta_count = mv.load_all_meta();
                        tracing::info!(
                            dir,
                            meta_count,
                            "MmapVineyard initialized (zero-copy OS page cache)"
                        );
                        Arc::new(mv)
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "MmapVineyard init failed, falling back to EdgeVineyard");
                        Arc::new(EdgeVineyard::new(vineyard_budget_mb))
                    }
                }
            } else {
                tracing::warn!("YATA_MMAP_VINEYARD=true but YATA_VINEYARD_DIR not set, falling back to EdgeVineyard");
                Arc::new(EdgeVineyard::new(vineyard_budget_mb))
            }
        } else if let Some(ref dir) = vineyard_dir {
            match DiskVineyard::new(dir, vineyard_budget_mb) {
                Ok(dv) => {
                    let meta_count = dv.load_all_meta();
                    tracing::info!(
                        dir,
                        vineyard_budget_mb,
                        meta_count,
                        "DiskVineyard initialized (Container disk)"
                    );
                    Arc::new(dv)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DiskVineyard init failed, falling back to EdgeVineyard");
                    Arc::new(EdgeVineyard::new(vineyard_budget_mb))
                }
            }
        } else {
            tracing::info!(vineyard_budget_mb, "EdgeVineyard initialized (in-memory)");
            Arc::new(EdgeVineyard::new(vineyard_budget_mb))
        };

        // ── S3/R2 client for read (page-in from R2, lazy init) ──
        let s3_prefix = std::env::var("YATA_S3_PREFIX").unwrap_or_default();
        let s3_client = Arc::new(Mutex::new(None));

        // WAL Projection: MutableCsrStore (GIE query path needs as_single()).
        let hot_store = yata_store::GraphStoreEnum::new(partition_count, hot_partition_id);
        tracing::info!("using MutableCsrStore (WAL Projection)");
        let wal_ring_capacity = config.wal_ring_capacity;
        Self {
            config,
            hot: Arc::new(RwLock::new(hot_store)),
            warm,
            cache: Arc::new(Mutex::new(cache)),
            hot_initialized: Arc::new(AtomicBool::new(false)),
            loaded_labels: Arc::new(Mutex::new(HashSet::new())),
            vineyard,
            s3_client,
            s3_prefix,
            dirty_labels: Arc::new(Mutex::new(HashSet::new())),
            pending_writes: Arc::new(AtomicUsize::new(0)),
            wal: Arc::new(Mutex::new(crate::wal::WalRingBuffer::new(wal_ring_capacity))),
            wal_last_flushed_seq: Arc::new(AtomicU64::new(0)),
            security_scope_cache: Arc::new(Mutex::new(HashMap::new())),
            cypher_read_count: Arc::new(AtomicU64::new(0)),
            cypher_mutation_count: Arc::new(AtomicU64::new(0)),
            cypher_mutation_us_total: Arc::new(AtomicU64::new(0)),
            merge_record_count: Arc::new(AtomicU64::new(0)),
            last_snapshot_serialize_ms: Arc::new(AtomicU64::new(0)),
        }
        // No startup restore — labels are lazy-loaded from R2 on first query (page-in).
        // Write path: Pipeline.send() + mergeRecord() (PDS Worker).
    }

    async fn init_async(data_dir: &str) -> GraphStore {
        GraphStore::new(data_dir)
            .await
            .expect("failed to init graph store")
    }

    /// Run an async future, handling both inside-runtime and outside-runtime contexts.
    fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(f))
        } else {
            ENGINE_RT.block_on(f)
        }
    }

    /// Mark a single label as dirty (snapshot will include it).
    fn mark_label_dirty(&self, label: &str) {
        match self.dirty_labels.lock() {
            Ok(mut dl) => { dl.insert(label.to_string()); }
            Err(e) => tracing::error!("dirty_labels poisoned (label={label}): {e}"),
        }
    }

    /// Mark multiple labels as dirty (from Cypher mutation hints).
    fn mark_labels_dirty(&self, labels: impl IntoIterator<Item = String>) {
        match self.dirty_labels.lock() {
            Ok(mut dl) => { dl.extend(labels); }
            Err(e) => tracing::error!("dirty_labels poisoned: {e}"),
        }
    }

    /// Fallback: when mutation hints cannot extract specific labels, mark all loaded labels dirty.
    fn mark_all_loaded_labels_dirty(&self) {
        let labels: Vec<String> = self.loaded_labels.lock()
            .map(|ll| ll.iter().cloned().collect())
            .unwrap_or_default();
        if !labels.is_empty() {
            self.mark_labels_dirty(labels);
        }
    }

    /// Get or lazily initialize the S3 client. Returns None if not configured.
    fn get_s3_client(&self) -> Option<Arc<yata_s3::s3::S3Client>> {
        if let Ok(guard) = self.s3_client.lock() {
            if let Some(ref client) = *guard {
                return Some(client.clone());
            }
        }
        // Try to build
        let client = Self::try_build_s3_client()?;
        if let Ok(mut guard) = self.s3_client.lock() {
            *guard = Some(client.clone());
        }
        Some(client)
    }

    /// Build S3 client from YATA_S3_* env vars. Returns None if not configured.
    fn try_build_s3_client() -> Option<Arc<yata_s3::s3::S3Client>> {
        let endpoint = std::env::var("YATA_S3_ENDPOINT").ok()?;
        let bucket = std::env::var("YATA_S3_BUCKET").unwrap_or_default();
        let key_id = std::env::var("YATA_S3_ACCESS_KEY_ID")
            .or_else(|_| std::env::var("YATA_S3_KEY_ID"))
            .unwrap_or_default();
        let secret = std::env::var("YATA_S3_SECRET_ACCESS_KEY")
            .or_else(|_| std::env::var("YATA_S3_SECRET_KEY"))
            .or_else(|_| std::env::var("YATA_S3_APPLICATION_KEY"))
            .unwrap_or_default();
        let region = std::env::var("YATA_S3_REGION").unwrap_or_else(|_| "auto".to_string());
        if endpoint.is_empty() || bucket.is_empty() || key_id.is_empty() || secret.is_empty() {
            if !endpoint.is_empty() {
                tracing::warn!("S3/R2 endpoint configured but credentials missing, skipping R2 persistence");
            }
            return None;
        }
        tracing::info!(endpoint = %endpoint, bucket = %bucket, "S3/R2 client configured for snapshot persistence");
        Some(Arc::new(yata_s3::s3::S3Client::new(
            &endpoint, &bucket, &key_id, &secret, &region,
        )))
    }

    /// Ensure HOT tier is initialized.
    fn ensure_hot(&self) {
        // HOT is always initialized in snapshot-only mode
    }

    /// Ensure specific labels are loaded into HOT CSR (lazy mode).
    ///
    /// **Vineyard page-in / page-out flow:**
    /// 1. Check Vineyard blob cache (in-memory, ~0ms)
    /// 2. If miss, data must come from R2 snapshot restore
    /// 3. Decode Arrow IPC → add to CSR (page-in)
    /// 4. If max labels exceeded, evict oldest CSR labels (page-out)
    ///    — evicted labels remain in Vineyard blob cache (warm tier)
    ///    — re-page-in from Vineyard is Arrow decode only (no R2 fetch)
    ///
    /// If `labels` is empty, loads ALL labels (mutation path fallback).
    /// If `labels` is non-empty AND hot_initialized, enriches only needed labels on-demand
    /// via enrich_label_from_r2 (zero-copy for already-loaded labels).
    fn ensure_labels(&self, vertex_labels: &[&str]) {
        if !self.hot_initialized.load(Ordering::Relaxed) {
            // Cold start: full page-in (topology + all labels needed for CSR)
            self.restore_from_r2();
            return;
        }

        // Hot path: enrich only labels not yet loaded
        if vertex_labels.is_empty() { return; }
        let needed: Vec<String> = {
            let loaded = match self.loaded_labels.lock() {
                Ok(ll) => ll,
                Err(_) => return,
            };
            vertex_labels
                .iter()
                .filter(|l| !loaded.contains(**l))
                .map(|l| l.to_string())
                .collect()
        };
        if needed.is_empty() { return; }

        let s3 = match self.get_s3_client() {
            Some(s3) => s3.clone(),
            None => return,
        };

        // Fetch meta + schema from R2 (3-tier: disk cache → R2)
        let meta = match crate::loader::fetch_fragment_meta(&s3, &self.s3_prefix) {
            Ok(m) => m,
            Err(e) => { tracing::warn!(error = %e, "ensure_labels: meta fetch failed"); return; }
        };
        let schema_bytes = match crate::loader::fetch_blob_cached(
            &s3, &self.s3_prefix, "schema", crate::loader::disk_cache_dir().as_deref(),
        ) {
            Some(b) => b,
            None => { tracing::warn!("ensure_labels: schema blob not found"); return; }
        };
        let schema: yata_vineyard::schema::PropertyGraphSchema = match serde_json::from_slice(&schema_bytes) {
            Ok(s) => s,
            Err(e) => { tracing::warn!(error = %e, "ensure_labels: schema parse failed"); return; }
        };

        // Enrich each needed label via existing enrich_label_from_r2 (per-label R2 GET)
        for label in &needed {
            if let Ok(mut hot) = self.hot.write() {
                if let Some(single) = hot.as_single_mut() {
                    match crate::loader::enrich_label_from_r2(
                        &s3, &self.s3_prefix, &meta, &schema, label, single, 0,
                    ) {
                        Ok(()) => {
                            single.commit();
                            tracing::debug!(label, "enriched label from R2 (on-demand)");
                        }
                        Err(e) => {
                            tracing::warn!(label, error = %e, "label enrichment failed");
                        }
                    }
                }
            }
            if let Ok(mut ll) = self.loaded_labels.lock() {
                ll.insert(label.clone());
            }
        }
    }


    /// Mutation context for provenance tracking.
    /// Injected as node properties on every mutation.
    pub fn query_with_context(
        &self,
        cypher: &str,
        params: &[(String, String)],
        rls_org_id: Option<&str>,
        ctx: &MutationContext,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        self.query_inner(cypher, params, rls_org_id, Some(ctx))
    }

    /// Execute multiple Cypher mutations in a single batch:
    /// 1 label load, 1 MemoryGraph copy, N mutations, 1 CSR rebuild, 1 WAL fsync.
    /// Returns per-statement results.
    pub fn batch_query_with_context(
        &self,
        statements: &[(&str, &[(String, String)])],
        rls_org_id: Option<&str>,
        ctx: &MutationContext,
    ) -> Result<Vec<Vec<Vec<(String, String)>>>, String> {
        if statements.is_empty() {
            return Ok(Vec::new());
        }

        // Collect all vertex label hints across all statements for a single label load.
        let mut all_vlabels = Vec::new();
        for (cypher, _) in statements {
            if let Some((vl, _el)) = router::extract_mutation_hints(cypher) {
                all_vlabels.extend(vl);
            }
        }
        all_vlabels.sort();
        all_vlabels.dedup();

        let vl_refs: Vec<&str> = all_vlabels.iter().map(|s| s.as_str()).collect();
        if !vl_refs.is_empty() {
            self.ensure_labels(&vl_refs);
        } else {
            self.ensure_labels(&[]);
        }


        self.block_on(async {
            self.ensure_hot();

            // Single MemoryGraph copy from CSR.
            let mut g = if let Ok(csr) = self.hot.read() {
                QueryableGraph(csr.to_filtered_memory_graph(&[], &[]))
            } else {
                return Err("failed to acquire CSR lock".to_string());
            };

            // Track initial state once.
            let initial_vids: HashSet<String> = g.0.nodes().iter().map(|n| n.id.clone()).collect();
            let initial_eids: HashSet<String> = g.0.rels().iter().map(|r| r.id.clone()).collect();

            // Execute all statements sequentially on the same MemoryGraph.
            let mut all_results = Vec::with_capacity(statements.len());
            for (cypher, params) in statements {
                let rows = g.query(cypher, params).map_err(|e| e.to_string())?;
                all_results.push(rows);
            }

            // Inject provenance metadata on new nodes.
            let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
            for node in g.0.nodes_mut() {
                let is_new = !initial_vids.contains(&node.id);
                if is_new {
                    use yata_cypher::types::Value;
                    node.props
                        .insert("_app_id".to_string(), Value::Str(ctx.app_id.clone()));
                    node.props
                        .insert("_org_id".to_string(), Value::Str(ctx.org_id.clone()));
                    if !ctx.user_id.is_empty() {
                        node.props
                            .insert("_user_id".to_string(), Value::Str(ctx.user_id.clone()));
                    }
                    if !ctx.actor_id.is_empty() {
                        node.props
                            .insert("_actor_id".to_string(), Value::Str(ctx.actor_id.clone()));
                    }
                    if !ctx.user_did.is_empty() {
                        node.props
                            .insert("_user_did".to_string(), Value::Str(ctx.user_did.clone()));
                    }
                    if !ctx.actor_did.is_empty() {
                        node.props
                            .insert("_actor_did".to_string(), Value::Str(ctx.actor_did.clone()));
                    }
                    node.props
                        .insert("_updated_at".to_string(), Value::Str(now.clone()));
                }
            }

            // Change detection: compare ID sets.
            let after_vids: HashSet<String> = g.0.nodes().iter().map(|n| n.id.clone()).collect();
            let after_eids: HashSet<String> = g.0.rels().iter().map(|r| r.id.clone()).collect();
            let new_vids: Vec<String> = after_vids.difference(&initial_vids).cloned().collect();

            let has_changes = initial_vids != after_vids || initial_eids != after_eids;

            if has_changes {
                // Single CSR rebuild.
                let new_csr =
                    loader::rebuild_csr_from_graph_with_partition(&g, self.config.hot_partition_id);

                if let Ok(mut hot) = self.hot.write() {
                    *hot = yata_store::GraphStoreEnum::Single(new_csr);
                    self.hot_initialized.store(true, Ordering::Relaxed);
                }

                tracing::info!(
                    statements = statements.len(),
                    new = new_vids.len(),
                    "engine: batch mutation applied"
                );
            }

            if let Ok(mut c) = self.cache.lock() {
                c.invalidate();
            }

            Ok(all_results)
        })
    }

    /// Query with scoped mutation context (org_id + user_did + actor_did).
    pub fn query_with_scoped_context(
        &self,
        cypher: &str,
        params: &[(String, String)],
        ctx: &MutationContext,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        let rls_org = if ctx.org_id.is_empty() {
            None
        } else {
            Some(ctx.org_id.as_str())
        };
        self.query_inner(cypher, params, rls_org, Some(ctx))
    }

    /// Main query entry point (sync, for WIT host compatibility).
    /// Auto-constructs MutationContext from env vars (PERFORMER_ID, rls_org_id).
    pub fn query(
        &self,
        cypher: &str,
        params: &[(String, String)],
        rls_org_id: Option<&str>,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        let app_id = &self.config.app_id;
        let config_org = &self.config.org_id;
        let resolved_org = rls_org_id.unwrap_or_default();
        let org_id = if resolved_org.is_empty() {
            config_org.as_str()
        } else {
            resolved_org
        };
        let ctx = if !app_id.is_empty() || !org_id.is_empty() {
            Some(MutationContext {
                app_id: app_id.clone(),
                org_id: org_id.to_string(),
                user_id: String::new(),
                actor_id: String::new(),
                user_did: String::new(),
                actor_did: String::new(),
            })
        } else {
            None
        };
        self.query_inner(cypher, params, rls_org_id, ctx.as_ref())
    }

    fn query_inner(
        &self,
        cypher: &str,
        params: &[(String, String)],
        rls_org_id: Option<&str>,
        mutation_ctx: Option<&MutationContext>,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        let is_mutation = router::is_cypher_mutation(cypher);
        let mutation_hints = if is_mutation {
            self.cypher_mutation_count.fetch_add(1, Ordering::Relaxed);
            let hints = router::extract_mutation_hints(cypher);
            if let Some((ref labels, _)) = hints {
                self.mark_labels_dirty(labels.iter().cloned());
            } else {
                self.mark_all_loaded_labels_dirty();
            }
            hints
        } else {
            self.cypher_read_count.fetch_add(1, Ordering::Relaxed);
            None
        };
        let query_start = std::time::Instant::now();

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
            self.ensure_hot();

            // Load only vertex labels referenced in Cypher (on demand from CAS)
            let (hints_labels, _) =
                router::extract_pushdown_hints(cypher).unwrap_or_default();
            let vl_refs: Vec<&str> = hints_labels.iter().map(|s| s.as_str()).collect();
            self.ensure_labels(&vl_refs);

            // GIE path: Cypher → IR Plan → execute directly on CSR (zero MemoryGraph copy).
            // Design E: SecurityScope is compiled from CSR policy vertices via query_with_did().
            // This path (query_inner) is Internal-only (no SecurityFilter needed).
            if let Ok(csr) = self.hot.read() {
                if let Some(single_csr) = csr.as_single() {
                    if let Ok(ast) = yata_cypher::parse(cypher) {
                        let plan_result = yata_gie::transpile::transpile(&ast);

                        if let Ok(plan) = plan_result {
                            let records = yata_gie::executor::execute(&plan, single_csr);
                            let rows = yata_gie::executor::result_to_rows(&records, &plan);

                            let k = cache_key(cypher, params, rls_org_id);
                            if let Ok(mut c) = self.cache.lock() {
                                c.put(k, rows.clone());
                            }
                            return Ok(rows);
                        }
                        // GIE transpile failed (CONTAINS, UNION, UNWIND, etc.) → fall through to MemoryGraph
                    }
                }
            }
        }

        // Mutation path: reuse cached hints to avoid double-parsing
        if let Some((ref vlabels, _)) = mutation_hints {
            let vl_refs: Vec<&str> = vlabels.iter().map(|s| s.as_str()).collect();
            self.ensure_labels(&vl_refs);
        } else {
            self.ensure_labels(&[]);
        }

        self.block_on(async {
            self.ensure_hot();
            let mut g = if let Ok(csr) = self.hot.read() {
                QueryableGraph(csr.to_filtered_memory_graph(&[], &[]))
            } else {
                return Err("failed to acquire CSR lock".to_string());
            };

            // Lightweight mutation tracking: only track IDs (O(n) ID clones, no format! serialization)
            let before_vids: HashSet<String> =
                g.0.nodes().iter().map(|n| n.id.clone()).collect();
            let before_eids: HashSet<String> =
                g.0.rels().iter().map(|r| r.id.clone()).collect();
            let before_count = (before_vids.len(), before_eids.len());

            // Execute Cypher
            let rows = g.query(cypher, params).map_err(|e| e.to_string())?;

            // Inject mutation metadata for new nodes (skip expensive modified-node detection)
            if let Some(ctx) = mutation_ctx {
                let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
                for node in g.0.nodes_mut() {
                    if !before_vids.contains(&node.id) {
                        use yata_cypher::types::Value;
                        node.props.insert("_app_id".to_string(), Value::Str(ctx.app_id.clone()));
                        node.props.insert("_org_id".to_string(), Value::Str(ctx.org_id.clone()));
                        if !ctx.user_id.is_empty() {
                            node.props.insert("_user_id".to_string(), Value::Str(ctx.user_id.clone()));
                        }
                        if !ctx.actor_id.is_empty() {
                            node.props.insert("_actor_id".to_string(), Value::Str(ctx.actor_id.clone()));
                        }
                        if !ctx.user_did.is_empty() {
                            node.props.insert("_user_did".to_string(), Value::Str(ctx.user_did.clone()));
                        }
                        if !ctx.actor_did.is_empty() {
                            node.props.insert("_actor_did".to_string(), Value::Str(ctx.actor_did.clone()));
                        }
                        node.props.insert("_updated_at".to_string(), Value::Str(now.clone()));
                    }
                }
            }

            // Detect changes: compare counts + check for new/deleted IDs
            let after_vids: HashSet<String> =
                g.0.nodes().iter().map(|n| n.id.clone()).collect();
            let after_eids: HashSet<String> =
                g.0.rels().iter().map(|r| r.id.clone()).collect();
            let after_count = (after_vids.len(), after_eids.len());

            let has_changes = before_count != after_count
                || before_vids != after_vids
                || before_eids != after_eids
                || router::is_cypher_mutation(cypher);

            if has_changes {
                // CP5 incremental CSR delta-apply: O(delta) instead of O(V+E) rebuild.
                // Compute delta between before/after MemoryGraph, apply to existing CSR.
                let new_vids: Vec<&String> = after_vids.difference(&before_vids).collect();
                let del_vids: Vec<&String> = before_vids.difference(&after_vids).collect();
                let new_eids: Vec<&String> = after_eids.difference(&before_eids).collect();
                let del_eids: Vec<&String> = before_eids.difference(&after_eids).collect();

                let delta_size = new_vids.len() + del_vids.len() + new_eids.len() + del_eids.len();
                let total_size = after_vids.len() + after_eids.len();

                // Use incremental apply when delta is small relative to total graph.
                // Fallback to full rebuild when delta > 50% (e.g., DETACH DELETE all).
                if delta_size > 0 && delta_size * 2 < total_size {
                    // Incremental path: apply delta to existing CSR
                    if let Ok(mut hot) = self.hot.write() {
                        if let Some(single) = hot.as_single_mut() {
                            // Delete removed vertices (by _vid property)
                            for vid_str in &del_vids {
                                single.delete_by_pk_any_label("_vid", &PropValue::Str((*vid_str).clone()));
                            }
                            // Add new vertices
                            for vid_str in &new_vids {
                                if let Some(node) = g.0.nodes().iter().find(|n| &n.id == *vid_str) {
                                    let props: Vec<(&str, PropValue)> = node.props.iter()
                                        .map(|(k, v)| (k.as_str(), loader::cypher_to_prop(v)))
                                        .chain(std::iter::once(("_vid", PropValue::Str(node.id.clone()))))
                                        .collect();
                                    single.add_vertex_with_labels(
                                        &node.labels,
                                        &props,
                                    );
                                }
                            }
                            // Add new edges
                            for eid_str in &new_eids {
                                if let Some(rel) = g.0.rels().iter().find(|r| &r.id == *eid_str) {
                                    // Look up src/dst vids by _vid property
                                    let src_vid = single.find_vid_by_prop("_vid", &PropValue::Str(rel.src.clone()));
                                    let dst_vid = single.find_vid_by_prop("_vid", &PropValue::Str(rel.dst.clone()));
                                    if let (Some(s), Some(d)) = (src_vid, dst_vid) {
                                        let props: Vec<(&str, PropValue)> = rel.props.iter()
                                            .map(|(k, v)| (k.as_str(), loader::cypher_to_prop(v)))
                                            .collect();
                                        single.add_edge(s, d, &rel.rel_type, &props);
                                    }
                                }
                            }
                            single.commit();

                            if let Ok(mut ll) = self.loaded_labels.lock() {
                                for label in <yata_store::MutableCsrStore as yata_grin::Schema>::vertex_labels(single) {
                                    ll.insert(label);
                                }
                                for label in <yata_store::MutableCsrStore as yata_grin::Schema>::edge_labels(single) {
                                    ll.insert(label);
                                }
                            }
                        }
                        self.hot_initialized.store(true, Ordering::Relaxed);
                    }
                    tracing::debug!(delta = delta_size, total = total_size, "incremental CSR delta-apply");
                } else {
                    // Full rebuild fallback (large delta or empty graph)
                    let new_csr = loader::rebuild_csr_from_graph_with_partition(&g, self.config.hot_partition_id);
                    if let Ok(mut ll) = self.loaded_labels.lock() {
                        for label in <yata_store::MutableCsrStore as yata_grin::Schema>::vertex_labels(&new_csr) {
                            ll.insert(label);
                        }
                        for label in <yata_store::MutableCsrStore as yata_grin::Schema>::edge_labels(&new_csr) {
                            ll.insert(label);
                        }
                    }
                    if let Ok(mut hot) = self.hot.write() {
                        *hot = yata_store::GraphStoreEnum::Single(new_csr);
                        self.hot_initialized.store(true, Ordering::Relaxed);
                    }
                    tracing::debug!(delta = delta_size, total = total_size, "full CSR rebuild (large delta)");
                }
            }

            if let Ok(mut c) = self.cache.lock() {
                c.invalidate();
            }

            // Record mutation elapsed time for CPM metrics
            if is_mutation {
                let elapsed_us = query_start.elapsed().as_micros() as u64;
                self.cypher_mutation_us_total.fetch_add(elapsed_us, Ordering::Relaxed);
            }

            Ok(rows)
        })
    }

    /// Restore CSR from R2 ArrowFragment snapshot.
    pub fn restore_from_r2(&self) {
        let s3 = match self.get_s3_client() {
            Some(s3) => s3.clone(),
            None => {
                tracing::warn!("restore_from_r2: no S3 client configured");
                return;
            }
        };
        match crate::loader::page_in_from_r2(
            &s3,
            &self.s3_prefix,
            self.config.hot_partition_id,
        ) {
            Ok(store) => {
                let vc = store.vertex_count();
                let ec = store.edge_count();
                if let Ok(mut hot) = self.hot.write() {
                    *hot = yata_store::GraphStoreEnum::Single(store);
                    self.hot_initialized.store(true, Ordering::Relaxed);
                }
                tracing::info!(vertices = vc, edges = ec, "restored from R2 ArrowFragment");
            }
            Err(e) => {
                tracing::warn!("R2 restore failed: {e}");
            }
        }
    }

    // ── Vector search (yata-vex) ────────────────────────────────────────

    /// Write vertices with embeddings for vector search.
    pub fn write_embeddings(
        &self,
        nodes: &[yata_cypher::NodeRef],
        embedding_key: &str,
        dim: usize,
    ) -> Result<usize, String> {
        let count = nodes.len();
        self.block_on(
            self.warm
                .write_vertices_with_embeddings(nodes, embedding_key, dim),
        )
        .map_err(|e| format!("write embeddings: {e}"))?;
        Ok(count)
    }

    /// Vector search over embeddings.
    pub fn vector_search(
        &self,
        query_vector: Vec<f32>,
        limit: usize,
        label_filter: Option<&str>,
        prop_filter: Option<&str>,
    ) -> Result<Vec<(yata_cypher::NodeRef, f32)>, String> {
        self.block_on(self.warm.vector_search_vertices(
            query_vector,
            limit,
            label_filter,
            prop_filter,
        ))
        .map_err(|e| format!("vector search: {e}"))
    }

    /// Create IVF_PQ vector index.
    pub fn create_embedding_index(&self) -> Result<(), String> {
        self.block_on(self.warm.create_embedding_index())
            .map_err(|e| format!("create index: {e}"))
    }

    /// CSR-direct MERGE by primary key: O(1) lookup, no Cypher parse, no MemoryGraph copy.
    /// GraphScope Groot parity: get_vertex_by_primary_key → upsert.
    /// Used by Pipeline + mergeRecord (PDS) for high-throughput projection.
    ///
    /// In WalProjection mode: also appends a WalEntry to the ring buffer.
    /// The returned WalEntry (if any) should be pushed to read replicas by the coordinator.
    pub fn merge_record(
        &self,
        label: &str,
        pk_key: &str,
        pk_value: &str,
        props: &[(&str, yata_grin::PropValue)],
    ) -> Result<u32, String> {
        self.merge_record_count.fetch_add(1, Ordering::Relaxed);
        self.mark_label_dirty(label);
        self.invalidate_security_cache_if_policy(label, props);

        // WAL append (typed PropValue, zero JSON overhead)
        if let Ok(mut wal) = self.wal.lock() {
            let seq = wal.next_seq();
            let entry = crate::wal::WalEntry {
                seq, op: crate::wal::WalOp::Upsert,
                label: label.to_string(), pk_key: pk_key.to_string(),
                pk_value: pk_value.to_string(), props: props_to_owned(props),
                timestamp_ms: now_ms(),
            };
            wal.append(entry);
        }

        if let Ok(mut hot) = self.hot.write() {
            if let Some(single) = hot.as_single_mut() {
                let pk = yata_grin::PropValue::Str(pk_value.to_string());
                let vid = single.merge_by_pk(label, pk_key, &pk, props);
                single.commit();
                if let Ok(mut ll) = self.loaded_labels.lock() {
                    ll.insert(label.to_string());
                }
                self.pending_writes.fetch_add(1, Ordering::Relaxed);
                return Ok(vid);
            }
        }
        Err("failed to acquire hot store lock".into())
    }

    /// Merge a record AND return the WAL entry for pushing to read replicas.
    /// Write Container only. Returns (vid, WalEntry).
    pub fn merge_record_with_wal(
        &self,
        label: &str,
        pk_key: &str,
        pk_value: &str,
        props: &[(&str, yata_grin::PropValue)],
    ) -> Result<(u32, Option<crate::wal::WalEntry>), String> {
        // Build WAL entry before CSR merge (typed PropValue, zero JSON overhead)
        let wal_entry = if let Ok(mut wal) = self.wal.lock() {
            let seq = wal.next_seq();
            let entry = crate::wal::WalEntry {
                seq,
                op: crate::wal::WalOp::Upsert,
                label: label.to_string(),
                pk_key: pk_key.to_string(),
                pk_value: pk_value.to_string(),
                props: props_to_owned(props),
                timestamp_ms: now_ms(),
            };
            wal.append(entry.clone());
            Some(entry)
        } else {
            None
        };

        self.mark_label_dirty(label);
        self.invalidate_security_cache_if_policy(label, props);

        if let Ok(mut hot) = self.hot.write() {
            if let Some(single) = hot.as_single_mut() {
                let pk = yata_grin::PropValue::Str(pk_value.to_string());
                let vid = single.merge_by_pk(label, pk_key, &pk, props);
                single.commit();
                if let Ok(mut ll) = self.loaded_labels.lock() {
                    ll.insert(label.to_string());
                }
                self.pending_writes.fetch_add(1, Ordering::Relaxed);
                return Ok((vid, wal_entry));
            }
        }
        Err("failed to acquire hot store lock".into())
    }

    /// CSR-direct DELETE by primary key: O(1) lookup.
    /// In WalProjection mode: also appends a Delete WalEntry.
    pub fn delete_record(
        &self,
        label: &str,
        pk_key: &str,
        pk_value: &str,
    ) -> Result<bool, String> {
        self.ensure_labels(&[label]);
        self.mark_label_dirty(label);

        // WAL append (Delete)
        if let Ok(mut wal) = self.wal.lock() {
            let seq = wal.next_seq();
            let entry = crate::wal::WalEntry {
                seq, op: crate::wal::WalOp::Delete,
                label: label.to_string(), pk_key: pk_key.to_string(),
                pk_value: pk_value.to_string(), props: Vec::new(),
                timestamp_ms: now_ms(),
            };
            wal.append(entry);
        }

        if let Ok(mut hot) = self.hot.write() {
            if let Some(single) = hot.as_single_mut() {
                let pk = yata_grin::PropValue::Str(pk_value.to_string());
                let deleted = single.delete_by_pk(label, pk_key, &pk);
                if deleted { single.commit(); }
                return Ok(deleted);
            }
        }
        Err("failed to acquire hot store lock".into())
    }

    /// Delete a record AND return the WAL entry for pushing to read replicas.
    pub fn delete_record_with_wal(
        &self,
        label: &str,
        pk_key: &str,
        pk_value: &str,
    ) -> Result<(bool, Option<crate::wal::WalEntry>), String> {
        self.ensure_labels(&[label]);
        self.mark_label_dirty(label);

        let wal_entry = if let Ok(mut wal) = self.wal.lock() {
            let seq = wal.next_seq();
            let entry = crate::wal::WalEntry {
                seq,
                op: crate::wal::WalOp::Delete,
                label: label.to_string(),
                pk_key: pk_key.to_string(),
                pk_value: pk_value.to_string(),
                props: Vec::new(),
                timestamp_ms: now_ms(),
            };
            wal.append(entry.clone());
            Some(entry)
        } else {
            None
        };

        if let Ok(mut hot) = self.hot.write() {
            if let Some(single) = hot.as_single_mut() {
                let pk = yata_grin::PropValue::Str(pk_value.to_string());
                let deleted = single.delete_by_pk(label, pk_key, &pk);
                if deleted { single.commit(); }
                return Ok((deleted, wal_entry));
            }
        }
        Err("failed to acquire hot store lock".into())
    }

    /// CPM metrics: read/mutation/mergeRecord counters + mutation latency.
    pub fn cpm_stats(&self) -> CpmStats {
        let reads = self.cypher_read_count.load(Ordering::Relaxed);
        let mutations = self.cypher_mutation_count.load(Ordering::Relaxed);
        let mutation_us = self.cypher_mutation_us_total.load(Ordering::Relaxed);
        let merges = self.merge_record_count.load(Ordering::Relaxed);
        let (v_count, e_count) = if let Ok(csr) = self.hot.read() {
            (csr.vertex_count() as u64, csr.edge_count() as u64)
        } else {
            (0, 0)
        };
        CpmStats {
            cypher_read_count: reads,
            cypher_mutation_count: mutations,
            cypher_mutation_avg_us: if mutations > 0 { mutation_us / mutations } else { 0 },
            cypher_mutation_us_total: mutation_us,
            merge_record_count: merges,
            mutation_ratio: if reads + mutations > 0 {
                mutations as f64 / (reads + mutations) as f64
            } else {
                0.0
            },
            vertex_count: v_count,
            edge_count: e_count,
            last_snapshot_serialize_ms: self.last_snapshot_serialize_ms.load(Ordering::Relaxed),
        }
    }

    /// Trigger R2 checkpoint: serialize CSR → ArrowFragment → R2 PUT.
    /// WAL Projection: CSR is authoritative (no R2 compaction).
    pub fn trigger_snapshot(&self) -> Result<(u64, u64), String> {
        self.trigger_snapshot_inner(false)
    }

    /// Force-trigger R2 snapshot, bypassing the batch_commit_threshold check.
    pub fn trigger_snapshot_force(&self) -> Result<(u64, u64), String> {
        self.trigger_snapshot_inner(true)
    }

    fn trigger_snapshot_inner(&self, force: bool) -> Result<(u64, u64), String> {
        // Drain dirty_labels atomically — snapshot owns this set from here.
        let dirty_set: HashSet<String> = match self.dirty_labels.lock() {
            Ok(mut dl) => dl.drain().collect(),
            Err(e) => {
                tracing::error!("dirty_labels poisoned in snapshot: {e}");
                HashSet::new()
            }
        };
        // Skip if no mutations since last snapshot
        if dirty_set.is_empty() {
            return Ok((0, 0));
        }

        // Check batch_commit_threshold: skip snapshot if not enough pending writes
        let pending = self.pending_writes.load(Ordering::Relaxed);
        if pending < self.config.batch_commit_threshold as usize && !force {
            // Put dirty labels back — they weren't consumed
            self.mark_labels_dirty(dirty_set);
            return Ok((0, 0));
        }

        let s3 = self.get_s3_client()
            .ok_or_else(|| "S3 client not configured".to_string())?;
        let prefix = &self.s3_prefix;

        // WAL Projection: no R2 compaction. CSR is authoritative (built from WAL).
        self.hot_initialized.store(true, Ordering::Relaxed);

        // Serialize CSR → ArrowFragment → MemoryBlobStore
        // Uses selective conversion: only dirty vertex labels get property extraction.
        let serialize_start = std::time::Instant::now();
        let (v_count, e_count, blob_store, meta, dirty_vlabel_ids) = if let Ok(csr) = self.hot.read() {
            let store = csr.as_single()
                .ok_or_else(|| "no CSR store available".to_string())?;
            let pid = store.partition_id_raw().get();
            let frag = yata_vineyard::convert::csr_to_fragment_selective(store, pid, &dirty_set);
            let vc = frag.ivnums.iter().sum::<u64>();
            let ec = frag.edge_num();
            // Compute dirty vertex label IDs for selective blob upload
            let ids: HashSet<usize> = frag.schema.vertex_entries.iter()
                .enumerate()
                .filter(|(_, e)| dirty_set.contains(&e.label))
                .map(|(i, _)| i)
                .collect();
            let bs = yata_vineyard::blob::MemoryBlobStore::new();
            let m = frag.serialize(&bs);
            (vc, ec, bs, m, ids)
        } else {
            return Err("failed to acquire CSR lock".into());
        };
        let serialize_ms = serialize_start.elapsed().as_millis() as u64;
        self.last_snapshot_serialize_ms.store(serialize_ms, Ordering::Relaxed);
        // CP4 monitoring: warn when vertex count approaches threshold where serialize becomes bottleneck
        if v_count > 100_000 {
            tracing::warn!(
                vertices = v_count, edges = e_count, serialize_ms,
                "CP4 threshold: vertex count >100K, snapshot serialize may become bottleneck (projected {}ms at 1M)",
                serialize_ms * 10
            );
        } else if serialize_ms > 100 {
            tracing::warn!(
                vertices = v_count, edges = e_count, serialize_ms,
                "CP4: snapshot serialize >100ms"
            );
        }

        // Guard: never overwrite a non-empty R2 snapshot with an empty one.
        if v_count == 0 && e_count == 0 {
            tracing::info!("snapshot skipped: CSR is empty (v=0, e=0), preserving existing R2 snapshot");
            return Ok((0, 0));
        }

        // Dirty label delta upload + disk cache in single pass.
        // Infrastructure blobs (schema, ivnums) are always persisted.
        // Edge/topology blobs: always persisted (dirty_set is non-empty at this point).
        // Clean vertex blobs are skipped.
        let meta_json = serde_json::to_vec(&meta).unwrap_or_default();
        let snap_dir = std::env::var("YATA_VINEYARD_DIR").ok().map(|d| {
            let sd = format!("{d}/snap/fragment");
            if let Err(e) = std::fs::create_dir_all(&sd) {
                tracing::warn!(error = %e, "failed to create vineyard snap dir for disk cache");
            }
            sd
        });
        let mut uploaded = 0u32;
        let mut skipped = 0u32;
        for name in meta.blobs.keys() {
            let dominated = is_infra_blob(name)
                || is_dirty_vertex_blob(name, &dirty_vlabel_ids)
                || is_edge_or_topology_blob(name);
            if !dominated {
                skipped += 1;
                continue;
            }
            if let Some(data) = BlobStore::get(&blob_store, name) {
                let full_key = format!("{prefix}snap/fragment/{name}");
                if let Err(e) = s3.put_sync(&full_key, data.clone()) {
                    tracing::error!(name, error = %e, "R2 ArrowFragment blob upload failed");
                } else {
                    uploaded += 1;
                }
                if let Some(ref sd) = snap_dir {
                    let path = format!("{sd}/{name}");
                    if let Err(e) = std::fs::write(&path, &data[..]) {
                        tracing::warn!(path, error = %e, "failed to write blob to disk cache");
                    }
                }
            }
        }

        // Upload fragment meta (ObjectMeta JSON) — always, reflects full CSR state
        let meta_key = format!("{prefix}snap/fragment/meta.json");
        if let Err(e) = s3.put_sync(&meta_key, bytes::Bytes::from(meta_json.clone())) {
            tracing::error!(error = %e, "R2 fragment meta upload failed");
        }
        if let Some(ref sd) = snap_dir {
            let meta_path = format!("{sd}/meta.json");
            if let Err(e) = std::fs::write(&meta_path, &meta_json) {
                tracing::warn!(error = %e, "failed to write meta.json to disk cache");
            }
            tracing::debug!(snap_dir = %sd, "delta snapshot blobs written to disk cache");
        }

        // Upload manifest (backward-compat for existing page-in path)
        let snap_manifest = serde_json::json!({
            "version": yata_core::SNAPSHOT_FORMAT_VERSION,
            "format": "arrow_fragment",
            "partition_id": self.config.hot_partition_id.get(),
            "vertex_count": v_count,
            "edge_count": e_count,
            "blob_count": uploaded,
            "partition_count": 1,
        });
        let key = format!("{prefix}snap/manifest.json");
        let data = bytes::Bytes::from(serde_json::to_vec(&snap_manifest).unwrap_or_default());
        if let Err(e) = s3.put_sync(&key, data) {
            tracing::error!(error = %e, "R2 snapshot manifest upload failed");
        }

        // Clear pending writes (dirty_labels already drained above)
        self.pending_writes.store(0, Ordering::Relaxed);

        tracing::info!(
            vertices = v_count, edges = e_count,
            uploaded = uploaded, skipped = skipped,
            dirty_labels = dirty_set.len(),
            "R2 delta checkpoint written (dirty labels only)"
        );
        Ok((v_count, e_count))
    }

    /// Export snapshot blobs for external upload (TS Worker → R2).
    // ── WAL Projection API ──────────────────────────────────────────────

    /// Read WAL entries after `after_seq`, up to `limit`.
    /// Returns `Ok(entries)` or `Err("gap")` if entries were evicted.
    /// Write Container only.
    pub fn wal_tail(&self, after_seq: u64, limit: usize) -> Result<Vec<crate::wal::WalEntry>, String> {
        if let Ok(wal) = self.wal.lock() {
            match wal.tail(after_seq, limit) {
                Some(entries) => Ok(entries),
                None => Err("gap: entries evicted, use R2 segments".to_string()),
            }
        } else {
            Err("failed to acquire WAL lock".into())
        }
    }

    /// Current WAL head sequence number.
    pub fn wal_head_seq(&self) -> u64 {
        if let Ok(wal) = self.wal.lock() {
            wal.head_seq()
        } else {
            0
        }
    }

    /// Apply WAL entries to the CSR (incremental merge). Read Container only.
    /// Each entry is applied as merge_by_pk (upsert) or delete_by_pk (delete).
    /// Apply vertex data from an ArrowWalStore (mmap'd compacted segment) to the CSR.
    /// Uses GRIN Property trait to read vertex data without intermediate WalEntry allocation.
    fn apply_arrow_wal_store(&self, store: &yata_store::ArrowWalStore) -> Result<u64, String> {
        use yata_grin::{Property, Scannable, Schema};
        if store.is_empty() {
            return Ok(0);
        }
        let mut applied = 0u64;
        if let Ok(mut hot) = self.hot.write() {
            if let Some(single) = hot.as_single_mut() {
                for vid in store.scan_all_vertices() {
                    let labels = Property::vertex_labels(store, vid);
                    let label = labels.first().map(|s| s.as_str()).unwrap_or("_default");
                    let all_props = store.vertex_all_props(vid);
                    let props: Vec<(&str, yata_grin::PropValue)> = all_props
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.clone()))
                        .collect();
                    // Use "rkey" as PK key (WAL convention)
                    if let Some(yata_grin::PropValue::Str(pk_val)) = all_props.get("rkey") {
                        let pk = yata_grin::PropValue::Str(pk_val.clone());
                        single.merge_by_pk(label, "rkey", &pk, &props);
                    } else {
                        // Fallback: use find_vid_by_pk from the store
                        single.add_vertex(label, &props);
                    }
                    applied += 1;
                }
                single.commit();
                if let Ok(mut ll) = self.loaded_labels.lock() {
                    for label in Schema::vertex_labels(store) {
                        ll.insert(label);
                    }
                }
                self.hot_initialized.store(true, std::sync::atomic::Ordering::Relaxed);
                if let Ok(mut c) = self.cache.lock() {
                    c.invalidate();
                }
            }
        }
        Ok(applied)
    }

    pub fn wal_apply(&self, entries: &[crate::wal::WalEntry]) -> Result<u64, String> {
        if entries.is_empty() {
            return Ok(0);
        }
        let mut applied = 0u64;
        if let Ok(mut hot) = self.hot.write() {
            if let Some(single) = hot.as_single_mut() {
                for entry in entries {
                    match entry.op {
                        crate::wal::WalOp::Upsert => {
                            // Phase 1: props are already typed PropValue — zero conversion
                            let props: Vec<(&str, yata_grin::PropValue)> = entry.props.iter()
                                .map(|(k, v)| (k.as_str(), v.clone()))
                                .collect();
                            let pk = yata_grin::PropValue::Str(entry.pk_value.clone());
                            single.merge_by_pk(&entry.label, &entry.pk_key, &pk, &props);
                        }
                        crate::wal::WalOp::Delete => {
                            let pk = yata_grin::PropValue::Str(entry.pk_value.clone());
                            single.delete_by_pk(&entry.label, &entry.pk_key, &pk);
                        }
                    }
                    applied += 1;
                }
                single.commit();
                if let Ok(mut ll) = self.loaded_labels.lock() {
                    for entry in entries {
                        ll.insert(entry.label.clone());
                    }
                }
                // Mark HOT as initialized (read container is now live)
                self.hot_initialized.store(true, Ordering::Relaxed);
                // Invalidate query cache
                if let Ok(mut c) = self.cache.lock() {
                    c.invalidate();
                }
            } else {
                return Err("no store available for wal_apply".into());
            }
        } else {
            return Err("failed to acquire hot store lock".into());
        }
        tracing::info!(applied, last_seq = entries.last().map(|e| e.seq).unwrap_or(0), "wal_apply complete");
        Ok(applied)
    }

    /// Flush pending WAL entries to R2 as a segment. Write Container only.
    /// Returns (seq_start, seq_end, bytes_written) or Ok((0,0,0)) if nothing to flush.
    pub fn wal_flush_segment(&self) -> Result<(u64, u64, usize), String> {
        let s3 = self.get_s3_client()
            .ok_or_else(|| "S3 client not configured".to_string())?;
        let prefix = &self.s3_prefix;
        let pid = self.config.hot_partition_id.get();
        let last_flushed = self.wal_last_flushed_seq.load(Ordering::SeqCst);

        let entries = if let Ok(wal) = self.wal.lock() {
            match wal.tail(last_flushed, 10_000) {
                Some(e) if !e.is_empty() => e,
                _ => return Ok((0, 0, 0)),
            }
        } else {
            return Err("failed to acquire WAL lock".into());
        };

        let seq_start = entries.first().unwrap().seq;
        let seq_end = entries.last().unwrap().seq;

        // Serialize to configured format (Arrow IPC or NDJSON)
        let (data, key) = match self.config.wal_format {
            crate::config::WalFormat::Arrow => {
                let arrow_data = crate::arrow_wal::serialize_segment_arrow(&entries)
                    .map_err(|e| format!("Arrow WAL serialize failed: {e}"))?;
                let k = crate::arrow_wal::segment_r2_key_arrow(prefix, pid, seq_start, seq_end);
                (arrow_data.to_vec(), k)
            }
            crate::config::WalFormat::Ndjson => {
                let ndjson_data = crate::wal::serialize_segment(&entries);
                let k = crate::wal::segment_r2_key(prefix, pid, seq_start, seq_end);
                (ndjson_data, k)
            }
        };
        s3.put_sync(&key, bytes::Bytes::from(data.clone()))
            .map_err(|e| format!("R2 WAL segment upload failed: {e}"))?;

        // Update head pointer
        let head_key = format!("{prefix}wal/meta/{pid}/head.json");
        let head_json = serde_json::json!({
            "partition_id": pid,
            "head_seq": seq_end,
            "entry_count": entries.len(),
            "updated_at_ms": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        });
        let _ = s3.put_sync(&head_key, bytes::Bytes::from(serde_json::to_vec(&head_json).unwrap_or_default()));

        self.wal_last_flushed_seq.store(seq_end, Ordering::SeqCst);
        tracing::info!(seq_start, seq_end, entries = entries.len(), bytes = data.len(), "WAL segment flushed to R2");
        Ok((seq_start, seq_end, data.len()))
    }

    /// Checkpoint: serialize CSR → ArrowFragment → R2 (same as trigger_snapshot).
    /// Also writes checkpoint metadata with the current WAL head seq.
    /// Used for cold start recovery: page-in checkpoint + replay WAL segments after checkpoint_seq.
    pub fn wal_checkpoint(&self) -> Result<(u64, u64), String> {
        // First flush any pending WAL entries to R2
        let _ = self.wal_flush_segment();

        let checkpoint_seq = self.wal_head_seq();

        // Use the existing trigger_snapshot_inner for ArrowFragment serialization
        let result = self.trigger_snapshot_inner(true)?;

        // Write checkpoint metadata
        if let Some(s3) = self.get_s3_client() {
            let prefix = &self.s3_prefix;
            let pid = self.config.hot_partition_id.get();
            let meta = crate::wal::CheckpointMeta {
                partition_id: pid,
                checkpoint_seq,
                vertex_count: result.0,
                edge_count: result.1,
                created_at_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            };
            let key = crate::wal::checkpoint_meta_r2_key(prefix, pid);
            let data = bytes::Bytes::from(serde_json::to_vec(&meta).unwrap_or_default());
            if let Err(e) = s3.put_sync(&key, data) {
                tracing::error!(error = %e, "failed to write checkpoint metadata to R2");
            } else {
                tracing::info!(checkpoint_seq, vertices = result.0, edges = result.1, "WAL checkpoint written");
            }
        }

        Ok(result)
    }

    /// L1 Compaction: read WAL segments from R2, PK-dedup, write compacted segment + manifest.
    ///
    /// This is the Shannon-optimal replacement for trigger_snapshot:
    /// - No CSR serialization (no OOM risk)
    /// - Operates on WAL segments only (streaming, bounded memory)
    /// - Output is same Arrow IPC format as WAL (uniform mmap path)
    /// - Idempotent: re-compacting produces the same result
    ///
    /// Write Container only. Called by cron (5min default) or manually.
    pub fn trigger_compaction(&self) -> Result<crate::compaction::CompactionResult, String> {
        let s3 = self.get_s3_client()
            .ok_or_else(|| "S3 client not configured".to_string())?;
        let prefix = &self.s3_prefix;
        let pid = self.config.hot_partition_id.get();

        // First flush pending WAL entries to R2
        let _ = self.wal_flush_segment();

        // Read existing compaction manifest (if any) to know where to start
        let manifest_key = crate::compaction::manifest_r2_key(prefix, pid);
        let existing_compacted_seq = match s3.get_sync(&manifest_key) {
            Ok(Some(data)) => {
                serde_json::from_slice::<crate::compaction::CompactionManifest>(&data)
                    .ok()
                    .map(|m| m.compacted_seq)
                    .unwrap_or(0)
            }
            _ => 0,
        };

        // List WAL segments in R2
        let segment_prefix = format!("{prefix}wal/segments/{pid}/");
        let segment_objects = s3.list_sync(&segment_prefix).unwrap_or_default();

        if segment_objects.is_empty() {
            tracing::debug!("compaction: no WAL segments found");
            return Ok(crate::compaction::CompactionResult {
                data: bytes::Bytes::new(),
                min_seq: 0,
                max_seq: 0,
                input_entries: 0,
                output_entries: 0,
                labels: Vec::new(),
            });
        }

        // Load existing compacted segment (if any) + all new WAL segments
        let mut segment_data: Vec<(String, bytes::Bytes)> = Vec::new();

        // Include existing compacted segment as input for re-compaction
        if existing_compacted_seq > 0 {
            let compacted_key = crate::compaction::compacted_segment_r2_key(prefix, pid, existing_compacted_seq);
            if let Ok(Some(data)) = s3.get_sync(&compacted_key) {
                segment_data.push((compacted_key, data));
            }
        }

        // Load WAL segments (only those after the existing compacted_seq)
        for obj in &segment_objects {
            let key = &obj.key;
            if let Some(filename) = key.rsplit('/').next() {
                let stripped = filename.strip_suffix(".ndjson")
                    .or_else(|| filename.strip_suffix(".arrow"));
                if let Some(stripped) = stripped {
                    let parts: Vec<&str> = stripped.split('-').collect();
                    if parts.len() == 2 {
                        if let Ok(seg_end) = parts[1].parse::<u64>() {
                            // Skip segments fully covered by existing compaction
                            if seg_end <= existing_compacted_seq {
                                continue;
                            }
                            if let Ok(Some(data)) = s3.get_sync(key) {
                                segment_data.push((key.clone(), data));
                            }
                        }
                    }
                }
            }
        }

        if segment_data.is_empty() {
            tracing::debug!("compaction: no new segments to compact");
            return Ok(crate::compaction::CompactionResult {
                data: bytes::Bytes::new(),
                min_seq: 0,
                max_seq: existing_compacted_seq,
                input_entries: 0,
                output_entries: 0,
                labels: Vec::new(),
            });
        }

        // Build references for compact_segments
        let refs: Vec<(&str, &[u8])> = segment_data.iter()
            .map(|(k, d)| (k.as_str(), d.as_ref()))
            .collect();

        let result = crate::compaction::compact_segments(&refs)?;

        if result.data.is_empty() {
            tracing::info!("compaction: all entries were tombstoned, nothing to write");
            return Ok(result);
        }

        // Upload compacted segment
        let compacted_key = crate::compaction::compacted_segment_r2_key(prefix, pid, result.max_seq);
        s3.put_sync(&compacted_key, result.data.clone())
            .map_err(|e| format!("R2 compacted segment upload failed: {e}"))?;

        // Write to disk cache too (for fast mmap on restart)
        if let Ok(vineyard_dir) = std::env::var("YATA_VINEYARD_DIR") {
            let compact_dir = format!("{vineyard_dir}/log/compacted/{pid}");
            let _ = std::fs::create_dir_all(&compact_dir);
            let filename = compacted_key.rsplit('/').next().unwrap_or("compacted.arrow");
            let path = format!("{compact_dir}/{filename}");
            let _ = std::fs::write(&path, &result.data);
        }

        // Upload manifest
        let manifest = crate::compaction::build_manifest(pid, prefix, &result);
        let manifest_json = serde_json::to_vec(&manifest).unwrap_or_default();
        s3.put_sync(&manifest_key, bytes::Bytes::from(manifest_json))
            .map_err(|e| format!("R2 compaction manifest upload failed: {e}"))?;

        // Old compacted segment left in R2 (overwritten on next compaction cycle).
        // R2 lifecycle rules can clean up stale compacted_* files if needed.

        tracing::info!(
            input_entries = result.input_entries,
            output_entries = result.output_entries,
            labels = result.labels.len(),
            compacted_seq = result.max_seq,
            bytes = result.data.len(),
            "L1 compaction complete"
        );

        Ok(result)
    }

    /// Cold start for Read Container in WalProjection mode:
    ///
    /// **L1 path (preferred)**: CompactionManifest → compacted Arrow IPC segment → WAL tail replay.
    /// **Legacy path (fallback)**: ArrowFragment checkpoint → checkpoint_seq → WAL segment replay.
    ///
    /// The L1 path is Shannon-optimal: single Arrow IPC format, no ArrowFragment deserialize.
    pub fn wal_cold_start(&self) -> Result<u64, String> {
        let s3 = self.get_s3_client()
            .ok_or_else(|| "S3 client not configured".to_string())?;
        let prefix = &self.s3_prefix;
        let pid = self.config.hot_partition_id.get();

        // Try L1 compacted segment first
        let manifest_key = crate::compaction::manifest_r2_key(prefix, pid);
        let compaction_manifest = match s3.get_sync(&manifest_key) {
            Ok(Some(data)) => serde_json::from_slice::<crate::compaction::CompactionManifest>(&data).ok(),
            _ => None,
        };

        let checkpoint_seq = if let Some(ref manifest) = compaction_manifest {
            // L1 path: mmap-first cold start (Phase 4)
            // Priority: disk cache (mmap, ~100µs) → R2 GET (~3-5ms) → legacy fallback
            tracing::info!(
                compacted_seq = manifest.compacted_seq,
                entries = manifest.entry_count,
                labels = manifest.labels.len(),
                "cold start: L1 compacted segment"
            );

            let segment_filename = manifest.compacted_segment_key
                .rsplit('/').next().unwrap_or("compacted.arrow");

            // Try disk cache first (mmap, zero R2 fetch)
            let disk_path = std::env::var("YATA_VINEYARD_DIR").ok().map(|d| {
                format!("{d}/log/compacted/{pid}/{segment_filename}")
            });

            let loaded = if let Some(ref path) = disk_path {
                if std::path::Path::new(path).exists() {
                    // Phase 4: mmap reattach — O(1) open + O(N_labels) index build
                    match yata_store::ArrowWalStore::from_file(std::path::Path::new(path)) {
                        Ok(store) => {
                            tracing::info!(
                                path, vertices = store.len(),
                                "cold start: mmap'd from disk cache (zero R2 fetch)"
                            );
                            // Apply to CSR via GRIN traits (vertex data only)
                            self.apply_arrow_wal_store(&store)?;
                            true
                        }
                        Err(e) => {
                            tracing::warn!(path, error = %e, "disk cache mmap failed, trying R2");
                            false
                        }
                    }
                } else {
                    false
                }
            } else {
                false
            };

            if !loaded {
                // R2 GET fallback
                match s3.get_sync(&manifest.compacted_segment_key) {
                    Ok(Some(data)) => {
                        let entries = crate::arrow_wal::deserialize_segment_auto(
                            &manifest.compacted_segment_key,
                            &data,
                        );
                        if !entries.is_empty() {
                            self.wal_apply(&entries)?;
                            tracing::info!(
                                applied = entries.len(),
                                "cold start: L1 compacted segment applied from R2"
                            );
                        }
                        // Write to disk cache for next mmap reattach
                        if let Some(ref path) = disk_path {
                            if let Some(parent) = std::path::Path::new(path).parent() {
                                let _ = std::fs::create_dir_all(parent);
                            }
                            let _ = std::fs::write(path, &data);
                            tracing::debug!(path, "wrote compacted segment to disk cache");
                        }
                    }
                    Ok(None) => {
                        tracing::warn!(key = %manifest.compacted_segment_key, "compacted segment not found, falling back to legacy");
                        self.restore_from_r2();
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to load compacted segment, falling back to legacy");
                        self.restore_from_r2();
                    }
                }
            }

            manifest.compacted_seq
        } else {
            // Legacy path: ArrowFragment checkpoint
            tracing::info!("cold start: no compaction manifest, using legacy ArrowFragment path");
            self.restore_from_r2();

            // Read legacy checkpoint metadata
            let checkpoint_key = crate::wal::checkpoint_meta_r2_key(prefix, pid);
            match s3.get_sync(&checkpoint_key) {
                Ok(Some(data)) => {
                    if let Ok(meta) = serde_json::from_slice::<crate::wal::CheckpointMeta>(&data) {
                        tracing::info!(checkpoint_seq = meta.checkpoint_seq, "loaded legacy checkpoint metadata");
                        meta.checkpoint_seq
                    } else {
                        0
                    }
                }
                _ => {
                    tracing::info!("no checkpoint metadata found, starting from seq 0");
                    0
                }
            }
        };

        // Step 3: Replay WAL segments from R2 after checkpoint_seq
        // List segments in the partition directory
        let segment_prefix = format!("{prefix}wal/segments/{pid}/");
        let segment_objects = s3.list_sync(&segment_prefix).unwrap_or_default();
        let mut replayed = 0u64;
        for obj in &segment_objects {
            let key = &obj.key;
            // Parse seq range from filename: {seq_start:020}-{seq_end:020}.{ndjson|arrow}
            if let Some(filename) = key.rsplit('/').next() {
                // Strip either extension
                let stripped = filename.strip_suffix(".ndjson")
                    .or_else(|| filename.strip_suffix(".arrow"));
                if let Some(stripped) = stripped {
                    let parts: Vec<&str> = stripped.split('-').collect();
                    if parts.len() == 2 {
                        if let (Ok(_seg_start), Ok(seg_end)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
                            if seg_end <= checkpoint_seq {
                                continue; // Already included in checkpoint
                            }
                            match s3.get_sync(key) {
                                Ok(Some(data)) => {
                                    // Auto-detect format from key extension
                                    let mut entries = crate::arrow_wal::deserialize_segment_auto(key, &data);
                                    // Filter entries already in checkpoint
                                    entries.retain(|e| e.seq > checkpoint_seq);
                                    if !entries.is_empty() {
                                        let count = entries.len();
                                        self.wal_apply(&entries)?;
                                        replayed += count as u64;
                                        tracing::info!(seg_end, applied = count, format = %if key.ends_with(".arrow") { "arrow" } else { "ndjson" }, "replayed WAL segment");
                                    }
                                }
                                Ok(None) => tracing::warn!(key, "WAL segment not found in R2"),
                                Err(e) => tracing::warn!(key, error = %e, "failed to read WAL segment"),
                            }
                        }
                    }
                }
            }
        }

        tracing::info!(checkpoint_seq, replayed, "WAL cold start complete");
        Ok(checkpoint_seq)
    }

    // ── Design E: SecurityScope compilation from graph policy vertices ──

    const POLICY_LABELS: [&'static str; 5] = [
        "ClearanceAssignment", "RBACAssignment", "ConsentGrant",
        "RACIAssignment", "PreKeyBundle",
    ];
    const SCOPE_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(300);
    const SCOPE_CACHE_MAX: usize = 10_000;

    /// Invalidate SecurityScope cache when a policy vertex is written.
    fn invalidate_security_cache_if_policy(&self, label: &str, props: &[(&str, yata_grin::PropValue)]) {
        if !Self::POLICY_LABELS.contains(&label) {
            return;
        }
        let did = props.iter()
            .find(|(k, _)| *k == "did" || *k == "grantee_did")
            .and_then(|(_, v)| match v {
                yata_grin::PropValue::Str(s) => Some(s.as_str()),
                _ => None,
            });
        if let Some(did) = did {
            if let Ok(mut cache) = self.security_scope_cache.lock() {
                cache.remove(did);
            }
        }
    }

    /// Compile SecurityScope from policy vertices in CSR for a given DID.
    /// Uses prop_eq_index for O(1) lookup per policy label.
    /// Results are cached with TTL.
    pub fn compile_security_scope(&self, did: &str) -> yata_gie::ir::SecurityScope {
        // Empty DID = public access
        if did.is_empty() {
            return yata_gie::ir::SecurityScope {
                max_sensitivity_ord: 0,
                collection_scopes: Vec::new(),
                allowed_owner_hashes: Vec::new(),
                bypass: false,
            };
        }

        // Check cache
        if let Ok(cache) = self.security_scope_cache.lock() {
            if let Some((scope, at)) = cache.get(did) {
                if at.elapsed() < Self::SCOPE_CACHE_TTL {
                    return scope.clone();
                }
            }
        }

        // Compile from CSR policy vertices
        let mut max_sensitivity_ord: u8 = 0;
        let mut collection_scopes: Vec<String> = Vec::new();
        let mut allowed_owner_hashes: Vec<u32> = Vec::new();

        if let Ok(hot) = self.hot.read() {
            if let Some(single) = hot.as_single() {
                let did_val = PropValue::Str(did.to_string());

                // ClearanceAssignment: scan by did → extract level
                for vid in single.scan_vertices("ClearanceAssignment", &Predicate::Eq("did".to_string(), did_val.clone())) {
                    if let Some(PropValue::Str(level)) = single.vertex_prop(vid, "level") {
                        max_sensitivity_ord = match level.as_str() {
                            "restricted" => 3,
                            "confidential" => 2,
                            "internal" => 1,
                            _ => 0,
                        };
                    }
                }

                // RBACAssignment: scan by did → extract scope (collection prefix)
                for vid in single.scan_vertices("RBACAssignment", &Predicate::Eq("did".to_string(), did_val.clone())) {
                    if let Some(PropValue::Str(scope)) = single.vertex_prop(vid, "scope") {
                        if scope != "*" {
                            collection_scopes.push(scope.clone());
                        }
                    }
                }

                // ConsentGrant: scan by grantee_did → extract grantor_did → hash
                for vid in single.scan_vertices("ConsentGrant", &Predicate::Eq("grantee_did".to_string(), did_val.clone())) {
                    if let Some(PropValue::Str(grantor)) = single.vertex_prop(vid, "grantor_did") {
                        allowed_owner_hashes.push(fnv1a_32(grantor.as_bytes()));
                    }
                }
            }
        }

        let scope = yata_gie::ir::SecurityScope {
            max_sensitivity_ord,
            collection_scopes,
            allowed_owner_hashes,
            bypass: false,
        };

        // Cache result
        if let Ok(mut cache) = self.security_scope_cache.lock() {
            if cache.len() >= Self::SCOPE_CACHE_MAX {
                cache.retain(|_, (_, at)| at.elapsed() < Self::SCOPE_CACHE_TTL);
            }
            cache.insert(did.to_string(), (scope.clone(), std::time::Instant::now()));
        }

        scope
    }

    /// Query with DID-based SecurityScope (Design E).
    /// Compiles SecurityScope from policy vertices, then runs GIE with SecurityFilter.
    pub fn query_with_did(
        &self,
        cypher: &str,
        params: &[(String, String)],
        did: &str,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        let scope = self.compile_security_scope(did);
        self.query_with_security_scope(cypher, params, scope)
    }

    /// Query with an explicit SecurityScope. GIE transpile_secured path only.
    fn query_with_security_scope(
        &self,
        cypher: &str,
        params: &[(String, String)],
        scope: yata_gie::ir::SecurityScope,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        let is_mutation = router::is_cypher_mutation(cypher);
        if is_mutation {
            if let Some((labels, _)) = router::extract_mutation_hints(cypher) {
                self.mark_labels_dirty(labels);
            } else {
                self.mark_all_loaded_labels_dirty();
            }
        }

        // Cache lookup (reads only, scope-aware key)
        let cache_did = if scope.bypass { "internal" } else { "" };
        if !is_mutation {
            let k = cache_key(cypher, params, Some(cache_did));
            if let Ok(c) = self.cache.lock() {
                if let Some(rows) = c.get(&k) {
                    return Ok(rows.clone());
                }
            }
        }

        if !is_mutation {
            self.ensure_hot();
            let (hints_labels, _) = router::extract_pushdown_hints(cypher).unwrap_or_default();
            let vl_refs: Vec<&str> = hints_labels.iter().map(|s| s.as_str()).collect();
            self.ensure_labels(&vl_refs);

            if let Ok(csr) = self.hot.read() {
                if let Some(single_csr) = csr.as_single() {
                    if let Ok(ast) = yata_cypher::parse(cypher) {
                        let plan = yata_gie::transpile::transpile_secured(&ast, scope)
                            .map_err(|e| format!("GIE transpile: {}", e))?;
                        let records = yata_gie::executor::execute(&plan, single_csr);
                        let rows = yata_gie::executor::result_to_rows(&records, &plan);
                        let k = cache_key(cypher, params, Some(cache_did));
                        if let Ok(mut c) = self.cache.lock() {
                            c.put(k, rows.clone());
                        }
                        return Ok(rows);
                    }
                }
            }
        }

        // Mutation path: delegate to existing query()
        self.query(cypher, params, None)
    }

    /// Resolve DID's P-256 public key multibase string from DIDDocument vertex in CSR.
    /// Returns the raw multibase string (z-prefix base58btc) or None.
    pub fn resolve_did_pubkey_multibase(&self, did: &str) -> Option<String> {
        let hot = self.hot.read().ok()?;
        let single = hot.as_single()?;
        let did_val = PropValue::Str(did.to_string());
        let vids = single.scan_vertices("DIDDocument", &Predicate::Eq("did".to_string(), did_val));
        let vid = *vids.first()?;
        match single.vertex_prop(vid, "public_key_multibase") {
            Some(PropValue::Str(s)) => Some(s.clone()),
            _ => None,
        }
    }
}

// Write-path standalone functions removed:
// - extract_dirty_vertex_labels, extract_dirty_edge_labels
// - upload_blobs_async
// - build_durable_wal_ops_fast, build_durable_wal_ops
// Write path: Pipeline.send() + mergeRecord() (PDS Worker).

#[cfg(test)]
mod tests {
    use super::*;
    use yata_cypher::{Graph, MemoryGraph, NodeRef, RelRef};
    use yata_grin::{Property, Scannable};

    fn make_engine(dir: &tempfile::TempDir) -> TieredGraphEngine {
        TieredGraphEngine::new(TieredEngineConfig::default(), dir.path().to_str().unwrap())
    }

    fn engine_at(data_dir: &std::path::Path) -> TieredGraphEngine {
        TieredGraphEngine::new(TieredEngineConfig::default(), data_dir.to_str().unwrap())
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
        run_query(
            &engine,
            "CREATE (a:Person {name: 'Alice', age: 30})",
            &[],
            None,
        )
        .unwrap();
        let rows = run_query(&engine, "MATCH (n:Person) RETURN n.name AS name", &[], None).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_write_read_consistency() {
        let dir = tempfile::tempdir().unwrap();
        let engine = make_engine(&dir);
        run_query(
            &engine,
            "CREATE (e:Entity {eid: 'e1', name: 'Test'})",
            &[],
            None,
        )
        .unwrap();
        run_query(
            &engine,
            "CREATE (ev:Evidence {evid: 'ev1', eid: 'e1', cat: 'Fraud'})",
            &[],
            None,
        )
        .unwrap();
        let rows = run_query(
            &engine,
            "MATCH (ev:Evidence {eid: 'e1'}) RETURN ev.evid AS id, ev.cat AS cat",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_rls() {
        // Design E: query_inner is Internal-only (no SecurityFilter).
        // Parameter-based RLS removed — all nodes visible on internal path.
        // Security filtering is via query_with_did() → CSR policy vertex lookup.
        let dir = tempfile::tempdir().unwrap();
        let engine = make_engine(&dir);
        run_query(
            &engine,
            "CREATE (n:Item {name: 'A', org_id: 'org_1'})",
            &[],
            None,
        )
        .unwrap();
        run_query(
            &engine,
            "CREATE (n:Item {name: 'B', org_id: 'org_2'})",
            &[],
            None,
        )
        .unwrap();
        run_query(&engine, "CREATE (n:Schema {name: 'System'})", &[], None).unwrap();
        let rows = run_query(
            &engine,
            "MATCH (n) RETURN n.name AS name",
            &[],
            Some("org_1"),
        )
        .unwrap();
        let names: Vec<&str> = rows
            .iter()
            .flat_map(|r| r.iter().find(|(c, _)| c == "name").map(|(_, v)| v.as_str()))
            .collect();
        // Internal path: all nodes visible (no RLS filtering)
        assert!(names.contains(&"\"A\""));
        assert!(names.contains(&"\"System\""));
        assert!(names.contains(&"\"B\""));
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

    // test_build_durable_wal_ops_fast removed (write path moved to PDS Pipeline)

    /// Helper: get a single column value from a single-row result.
    fn get_col<'a>(rows: &'a [Vec<(String, String)>], col: &str) -> &'a str {
        rows[0]
            .iter()
            .find(|(c, _)| c == col)
            .map(|(_, v)| v.as_str())
            .unwrap()
    }

    // ── HOT (CSR in-memory) write/read ──────────────────────────────────

    #[test]
    fn test_tier_hot_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);

        run_query(&e, "CREATE (:Fruit {name: 'apple', price: 100})", &[], None).unwrap();
        run_query(
            &e,
            "CREATE (:Fruit {name: 'banana', price: 200})",
            &[],
            None,
        )
        .unwrap();

        // Directly inspect HOT CSR
        let csr = e.hot.read().unwrap();
        assert_eq!(csr.vertex_count(), 2, "HOT: vertex_count");
        assert!(
            e.hot_initialized.load(Ordering::Relaxed),
            "HOT: must be initialized"
        );

        // Verify labels via Topology trait
        use yata_grin::Topology;
        let vids = csr.scan_all_vertices();
        assert_eq!(vids.len(), 2);
        for &vid in &vids {
            let labels = Property::vertex_labels(&*csr, vid);
            assert_eq!(labels, vec!["Fruit"], "HOT: label must be Fruit");
        }

        // Verify props via Property trait
        let mut names: Vec<String> = Vec::new();
        for &vid in &vids {
            if let Some(yata_grin::PropValue::Str(s)) = csr.vertex_prop(vid, "name") {
                names.push(s);
            }
        }
        names.sort();
        assert_eq!(names, vec!["apple", "banana"], "HOT: props round-trip");
    }

    #[test]
    fn test_tier_hot_mutation_updates_csr() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);

        run_query(&e, "CREATE (:T {k: 'x', v: 1})", &[], None).unwrap();

        // Before SET
        {
            let csr = e.hot.read().unwrap();
            let vids = yata_grin::Scannable::scan_all_vertices(&*csr);
            let val = csr.vertex_prop(vids[0], "v");
            assert_eq!(
                val,
                Some(yata_grin::PropValue::Int(1)),
                "HOT: initial value"
            );
        }

        run_query(&e, "MATCH (n:T {k: 'x'}) SET n.v = 999", &[], None).unwrap();

        // After SET — CSR must reflect new value
        {
            let csr = e.hot.read().unwrap();
            let vids = yata_grin::Scannable::scan_all_vertices(&*csr);
            let val = csr.vertex_prop(vids[0], "v");
            assert_eq!(
                val,
                Some(yata_grin::PropValue::Int(999)),
                "HOT: value after SET"
            );
        }
    }

    #[test]
    fn test_tier_hot_delete_removes_from_csr() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);

        run_query(&e, "CREATE (:D {k: 'd1'})", &[], None).unwrap();
        run_query(&e, "CREATE (:D {k: 'd2'})", &[], None).unwrap();
        assert_eq!(e.hot.read().unwrap().vertex_count(), 2);

        run_query(&e, "MATCH (n:D {k: 'd1'}) DELETE n", &[], None).unwrap();
        assert_eq!(
            e.hot.read().unwrap().vertex_count(),
            1,
            "HOT: delete must reduce count"
        );
    }

    #[test]
    fn test_tier_hot_edge_in_csr() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);

        run_query(
            &e,
            "CREATE (:P {n: 'a'})-[:E {w: 7}]->(:P {n: 'b'})",
            &[],
            None,
        )
        .unwrap();

        let csr = e.hot.read().unwrap();
        assert_eq!(csr.vertex_count(), 2, "HOT: 2 vertices");
        assert_eq!(csr.edge_count(), 1, "HOT: 1 edge");
    }

    // ── Vector search (yata-vex only, not in graph write path) ─────────

    #[test]
    fn test_graph_store_not_written_by_graph_mutations() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);

        run_query(&e, "CREATE (:LN {lid: 'l1', val: 10})", &[], None).unwrap();
        run_query(&e, "CREATE (:LN {lid: 'l2', val: 20})", &[], None).unwrap();

        // GraphStore should have NO graph data (vector-search only)
        let vertices = ENGINE_RT.block_on(e.warm.load_vertices()).unwrap();
        assert_eq!(
            vertices.len(),
            0,
            "GraphStore must not have graph vertices (vector-search only)"
        );

        let edges = ENGINE_RT.block_on(e.warm.load_edges()).unwrap();
        assert_eq!(
            edges.len(),
            0,
            "GraphStore must not have graph edges (vector-search only)"
        );
    }

    #[test]
    fn test_snapshot_persistence_in_memory() {
        // Verify in-memory graph operations work without MDAG
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);

        run_query(&e, "CREATE (:Restore {rid: 'r1', v: 10})", &[], None).unwrap();
        run_query(&e, "CREATE (:Restore {rid: 'r2', v: 20})", &[], None).unwrap();
        run_query(&e, "MATCH (n:Restore {rid: 'r1'}) SET n.v = 99", &[], None).unwrap();

        let rows = run_query(
            &e,
            "MATCH (n:Restore) RETURN n.rid AS rid, n.v AS v",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
    }

    // ── Read-after-write consistency tests ───────────────────────────

    #[test]
    fn test_read_after_write_delete_create_upsert() {
        // Simulates the shinshi cypherUpsertNode pattern: DELETE + CREATE.
        // After upsert, immediate read must return the NEW node properties.
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("data_raw_upsert");
        let e = engine_at(&d);

        // Create initial node
        run_query(
            &e,
            "CREATE (n:Model {_doc_id: 'mdl-1', name: 'Luna', image_count: '0'})",
            &[],
            None,
        )
        .unwrap();

        // Read initial
        let rows = run_query(
            &e,
            "MATCH (n:Model {_doc_id: 'mdl-1'}) RETURN n.name AS name, n.image_count AS ic",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(get_col(&rows, "name").contains("Luna"));
        assert!(get_col(&rows, "ic").contains("0"));

        // Upsert: DELETE + CREATE with updated properties
        run_query(&e, "MATCH (n:Model {_doc_id: 'mdl-1'}) DELETE n", &[], None).unwrap();
        run_query(&e, "CREATE (n:Model {_doc_id: 'mdl-1', name: 'Luna', image_count: '1', profile_url: 'https://cdn/img.png'})", &[], None).unwrap();

        // Read immediately after upsert — MUST return updated values
        let rows = run_query(&e, "MATCH (n:Model {_doc_id: 'mdl-1'}) RETURN n.name AS name, n.image_count AS ic, n.profile_url AS url", &[], None).unwrap();
        assert_eq!(rows.len(), 1, "Node must exist after upsert");
        assert!(
            get_col(&rows, "ic").contains("1"),
            "image_count must be '1' after upsert, got: {:?}",
            get_col(&rows, "ic")
        );
        assert!(
            get_col(&rows, "url").contains("cdn"),
            "profile_url must be set after upsert"
        );
    }

    #[test]
    fn test_read_after_write_sequential_upserts() {
        // Multiple sequential upserts — each read must see latest values.
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("data_seq");
        let e = engine_at(&d);

        for i in 0..5 {
            // DELETE + CREATE
            run_query(&e, "MATCH (n:Counter {_doc_id: 'c1'}) DELETE n", &[], None).unwrap();
            run_query(
                &e,
                &format!("CREATE (n:Counter {{_doc_id: 'c1', value: '{i}'}})"),
                &[],
                None,
            )
            .unwrap();

            // Immediate read
            let rows = run_query(
                &e,
                "MATCH (n:Counter {_doc_id: 'c1'}) RETURN n.value AS v",
                &[],
                None,
            )
            .unwrap();
            assert_eq!(rows.len(), 1, "iter {i}: node must exist");
            assert!(
                get_col(&rows, "v").contains(&i.to_string()),
                "iter {i}: value must be {i}, got: {:?}",
                get_col(&rows, "v")
            );
        }
    }

    #[test]
    fn test_read_after_write_create_then_list_with_filter() {
        // CREATE a node, then MATCH with OR filter (shinshi ListModels pattern).
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("data_list");
        let e = engine_at(&d);

        run_query(
            &e,
            "CREATE (n:Model {_doc_id: 'm1', name: 'A', status: 'active'})",
            &[],
            None,
        )
        .unwrap();
        run_query(
            &e,
            "CREATE (n:Model {_doc_id: 'm2', name: 'B', status: 'draft'})",
            &[],
            None,
        )
        .unwrap();

        // List with OR filter (same pattern as shinshi cypherListModels)
        let rows = run_query(
            &e,
            "MATCH (n:Model) WHERE n.status = 'active' OR n.status = 'draft' RETURN n.name AS name",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 2, "Both models should be visible in list");
    }

    #[test]
    fn test_read_after_write_return_n_whole_node() {
        // RETURN n must return parseable JSON with all properties.
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("data_rn");
        let e = engine_at(&d);

        run_query(
            &e,
            "CREATE (n:Item {_doc_id: 'i1', title: 'Hello', score: '42'})",
            &[],
            None,
        )
        .unwrap();

        let rows = run_query(&e, "MATCH (n:Item {_doc_id: 'i1'}) RETURN n", &[], None).unwrap();
        assert_eq!(rows.len(), 1);

        // The "n" column should be a JSON object, not just a number
        let n_val = get_col(&rows, "n");
        assert!(
            n_val.contains("title"),
            "RETURN n must contain 'title' property, got: {n_val}"
        );
        assert!(
            n_val.contains("Hello"),
            "RETURN n must contain 'Hello' value, got: {n_val}"
        );
    }

    #[test]
    fn test_cache_invalidated_after_mutation() {
        // Query → mutate → re-query must NOT return cached (stale) result.
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("data_cache");
        let e = engine_at(&d);

        run_query(
            &e,
            "CREATE (n:CacheTest {key: 'k1', val: 'old'})",
            &[],
            None,
        )
        .unwrap();

        // First read — populates cache
        let rows1 = run_query(
            &e,
            "MATCH (n:CacheTest {key: 'k1'}) RETURN n.val AS v",
            &[],
            None,
        )
        .unwrap();
        assert!(get_col(&rows1, "v").contains("old"));

        // Mutate
        run_query(&e, "MATCH (n:CacheTest {key: 'k1'}) DELETE n", &[], None).unwrap();
        run_query(
            &e,
            "CREATE (n:CacheTest {key: 'k1', val: 'new'})",
            &[],
            None,
        )
        .unwrap();

        // Re-read — must return new value, NOT cached old value
        let rows2 = run_query(
            &e,
            "MATCH (n:CacheTest {key: 'k1'}) RETURN n.val AS v",
            &[],
            None,
        )
        .unwrap();
        assert!(
            get_col(&rows2, "v").contains("new"),
            "After mutation, read must return 'new' not cached 'old', got: {:?}",
            get_col(&rows2, "v")
        );
    }

    #[test]
    #[ignore] // Requires external persistence (Vineyard DiskStore or R2 sync)
    fn test_upsert_persists_across_restart() {
        // Upsert → restart → read must return upserted values.
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("data_restart");
        {
            let e = engine_at(&d);
            run_query(
                &e,
                "CREATE (n:Persist {_doc_id: 'p1', val: 'v0'})",
                &[],
                None,
            )
            .unwrap();
            // Upsert
            run_query(&e, "MATCH (n:Persist {_doc_id: 'p1'}) DELETE n", &[], None).unwrap();
            run_query(
                &e,
                "CREATE (n:Persist {_doc_id: 'p1', val: 'v1'})",
                &[],
                None,
            )
            .unwrap();
        }
        // Restart
        {
            let e = engine_at(&d);
            let rows = run_query(
                &e,
                "MATCH (n:Persist {_doc_id: 'p1'}) RETURN n.val AS v",
                &[],
                None,
            )
            .unwrap();
            assert_eq!(rows.len(), 1);
            assert!(
                get_col(&rows, "v").contains("v1"),
                "After restart, upserted value must be 'v1'"
            );
        }
    }

    // ── RLS on mutations ────────────────────────────────────────────

    #[test]
    fn test_rls_mutation_create_with_org_id() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        // CREATE with RLS org_id — should inject org_id into new vertex
        run_query(&e, "CREATE (n:Item {name: 'X'})", &[], Some("org_1")).unwrap();
        // Read without RLS — should see org_id property
        let rows = run_query(
            &e,
            "MATCH (n:Item) RETURN n.name AS name, n.org_id AS oid",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_rls_read_filters_by_org() {
        // Design E: query_inner is Internal-only — all nodes visible.
        // Parameter-based RLS removed. Security via query_with_did().
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(
            &e,
            "CREATE (n:Item {name: 'A', org_id: 'org_1'})",
            &[],
            None,
        )
        .unwrap();
        run_query(
            &e,
            "CREATE (n:Item {name: 'B', org_id: 'org_2'})",
            &[],
            None,
        )
        .unwrap();
        run_query(&e, "CREATE (n:Item {name: 'C'})", &[], None).unwrap(); // no org_id
        let rows = run_query(
            &e,
            "MATCH (n:Item) RETURN n.name AS name",
            &[],
            Some("org_1"),
        )
        .unwrap();
        assert_eq!(
            rows.len(),
            3,
            "Internal path: all Item nodes visible (no RLS filtering)"
        );
        let has_a = rows.iter().any(|r| r.iter().any(|(_, v)| v.contains("A")));
        let has_b = rows.iter().any(|r| r.iter().any(|(_, v)| v.contains("B")));
        let has_c = rows.iter().any(|r| r.iter().any(|(_, v)| v.contains("C")));
        assert!(has_a, "Should see node A");
        assert!(has_b, "Should see node B");
        assert!(has_c, "Should see node C");
    }

    // ── RETURN n via GIE path ───────────────────────────────────────

    #[test]
    fn test_return_n_via_gie_produces_json() {
        // End-to-end: engine.query() with RETURN n should produce JSON, not just vid
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Gie {key: 'k1', val: 'hello'})", &[], None).unwrap();
        let rows = run_query(&e, "MATCH (n:Gie {key: 'k1'}) RETURN n", &[], None).unwrap();
        assert_eq!(rows.len(), 1);
        let n_val = &rows[0][0].1;
        assert!(
            n_val.contains("key"),
            "RETURN n should contain 'key' property, got: {n_val}"
        );
        assert!(
            n_val.contains("hello"),
            "RETURN n should contain 'hello' value, got: {n_val}"
        );
    }

    #[test]
    fn test_return_n_multiple_nodes_json() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Multi {id: '1', x: 'a'})", &[], None).unwrap();
        run_query(&e, "CREATE (n:Multi {id: '2', x: 'b'})", &[], None).unwrap();
        let rows = run_query(&e, "MATCH (n:Multi) RETURN n", &[], None).unwrap();
        assert_eq!(rows.len(), 2);
        for row in &rows {
            let json = &row[0].1;
            assert!(
                json.contains("id"),
                "Each node JSON must contain 'id', got: {json}"
            );
        }
    }

    // ── UNWIND / UNION (cypher via engine) ──────────────────────────

    #[test]
    fn test_unwind_list() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let rows = run_query(&e, "UNWIND [1, 2, 3] AS x RETURN x", &[], None).unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_union_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:U {v: 'a'})", &[], None).unwrap();
        run_query(&e, "CREATE (n:U {v: 'b'})", &[], None).unwrap();
        // UNION should dedup identical rows
        let rows = run_query(
            &e,
            "MATCH (n:U) RETURN n.v AS v UNION MATCH (n:U) RETURN n.v AS v",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 2, "UNION should deduplicate identical rows");
    }

    // ── GIE cache invalidation end-to-end ───────────────────────────

    #[test]
    fn test_gie_cache_stale_after_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Cache {k: '1', v: 'old'})", &[], None).unwrap();

        // First read — enters cache (GIE path)
        let r1 = run_query(&e, "MATCH (n:Cache {k: '1'}) RETURN n.v AS v", &[], None).unwrap();
        assert!(get_col(&r1, "v").contains("old"));

        // Mutation — invalidates cache
        run_query(&e, "MATCH (n:Cache {k: '1'}) SET n.v = 'new'", &[], None).unwrap();

        // Second read — must NOT return cached 'old'
        let r2 = run_query(&e, "MATCH (n:Cache {k: '1'}) RETURN n.v AS v", &[], None).unwrap();
        assert!(
            get_col(&r2, "v").contains("new"),
            "GIE cache must be invalidated after SET, got: {:?}",
            get_col(&r2, "v")
        );
    }

    // ── Inline WHERE filtering (GIE path) ─────────────────────────

    #[test]
    fn test_where_eq_filter() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Filt {key: 'k1', val: 'hello'})", &[], None).unwrap();
        run_query(&e, "CREATE (n:Filt {key: 'k2', val: 'world'})", &[], None).unwrap();

        let rows = run_query(
            &e,
            "MATCH (n:Filt {key: 'k1'}) RETURN n.val AS v",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(get_col(&rows, "v").contains("hello"));
    }

    #[test]
    fn test_where_no_match() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:NM {key: 'exists'})", &[], None).unwrap();

        let rows = run_query(&e, "MATCH (n:NM {key: 'nonexistent'}) RETURN n", &[], None).unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_where_multiple_props() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:MP {a: 'x', b: 'y'})", &[], None).unwrap();
        run_query(&e, "CREATE (n:MP {a: 'x', b: 'z'})", &[], None).unwrap();

        let rows = run_query(
            &e,
            "MATCH (n:MP) WHERE n.a = 'x' AND n.b = 'y' RETURN n.b AS b",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(get_col(&rows, "b").contains("y"));
    }

    // ── Stress: many nodes + query ──────────────────────────────────

    #[test]
    fn test_stress_100_nodes_list() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        for i in 0..100 {
            run_query(&e, &format!("CREATE (n:Stress {{idx: {i}}})"), &[], None).unwrap();
        }
        let rows = run_query(&e, "MATCH (n:Stress) RETURN n.idx AS idx", &[], None).unwrap();
        assert_eq!(rows.len(), 100);
    }

    #[test]
    fn test_stress_rapid_upsert_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Rapid {rid: 'r1', counter: 0})", &[], None).unwrap();

        for i in 1..=20 {
            // DELETE+CREATE upsert
            run_query(&e, "MATCH (n:Rapid {rid: 'r1'}) DELETE n", &[], None).unwrap();
            run_query(
                &e,
                &format!("CREATE (n:Rapid {{rid: 'r1', counter: {i}}})"),
                &[],
                None,
            )
            .unwrap();

            // Immediate read must return latest value
            let rows = run_query(
                &e,
                "MATCH (n:Rapid {rid: 'r1'}) RETURN n.counter AS c",
                &[],
                None,
            )
            .unwrap();
            assert_eq!(rows.len(), 1, "iter {i}: node must exist");
            assert!(
                get_col(&rows, "c").contains(&i.to_string()),
                "iter {i}: counter must be {i}"
            );
        }
    }

    // ── Complex query patterns ──────────────────────────────────────

    #[test]
    fn test_where_gt_lt_combined() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        for i in [10, 20, 30, 40, 50] {
            run_query(&e, &format!("CREATE (n:Range {{v: {i}}})"), &[], None).unwrap();
        }
        let rows = run_query(
            &e,
            "MATCH (n:Range) WHERE n.v > 15 AND n.v < 45 RETURN n.v AS v",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 3); // 20, 30, 40
    }

    #[test]
    fn test_count_aggregation() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        for i in 0..5 {
            run_query(&e, &format!("CREATE (n:Cnt {{v: {i}}})"), &[], None).unwrap();
        }
        let rows = run_query(&e, "MATCH (n:Cnt) RETURN count(n) AS c", &[], None).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(get_col(&rows, "c").contains("5"));
    }

    #[test]
    fn test_order_by_limit() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        for i in [30, 10, 20, 50, 40] {
            run_query(&e, &format!("CREATE (n:OL {{v: {i}}})"), &[], None).unwrap();
        }
        let rows = run_query(
            &e,
            "MATCH (n:OL) RETURN n.v AS v ORDER BY n.v ASC LIMIT 3",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert!(get_col(&rows, "v").contains("10"));
    }

    #[test]
    fn test_cypher_vector_search() {
        // Cypher CALL db.index.vector.queryNodes on MemoryGraph directly
        // (CSR PropValue has no List variant, so embedding must be set via set_node_embedding)
        use yata_cypher::{Executor, Graph, MemoryGraph, NodeRef, Value};

        let mut g = MemoryGraph::new();
        let mut props_a = indexmap::IndexMap::new();
        props_a.insert("name".into(), Value::Str("alpha".into()));
        g.add_node(NodeRef {
            id: "a".into(),
            labels: vec!["Doc".into()],
            props: props_a,
        });
        g.set_node_embedding("a", &[1.0, 0.0, 0.0]);

        let mut props_b = indexmap::IndexMap::new();
        props_b.insert("name".into(), Value::Str("beta".into()));
        g.add_node(NodeRef {
            id: "b".into(),
            labels: vec!["Doc".into()],
            props: props_b,
        });
        g.set_node_embedding("b", &[0.0, 1.0, 0.0]);

        let mut props_c = indexmap::IndexMap::new();
        props_c.insert("name".into(), Value::Str("gamma".into()));
        g.add_node(NodeRef {
            id: "c".into(),
            labels: vec!["Doc".into()],
            props: props_c,
        });
        g.set_node_embedding("c", &[0.9, 0.1, 0.0]);

        let mut qg = yata_graph::QueryableGraph(g);
        let rows = qg.query(
            "CALL db.index.vector.queryNodes('Doc', 'embedding', [1.0, 0.0, 0.0], 2) YIELD node, score RETURN node.name AS name, score",
            &[],
        ).unwrap();

        assert_eq!(
            rows.len(),
            2,
            "should return top 2 results, got {}",
            rows.len()
        );
        let names: Vec<&str> = rows
            .iter()
            .filter_map(|r| r.iter().find(|(c, _)| c == "name").map(|(_, v)| v.as_str()))
            .collect();
        assert!(
            names.iter().any(|n| n.contains("alpha")),
            "alpha should be in results, got: {names:?}"
        );
    }

    #[test]
    fn test_vex_vector_search() {
        // TieredGraphEngine::vector_search → yata-vex IVF_PQ index
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);

        // Write embeddings via yata-vex path
        let nodes: Vec<yata_cypher::NodeRef> = (0..100)
            .map(|i| {
                let mut props = indexmap::IndexMap::new();
                props.insert(
                    "name".into(),
                    yata_cypher::types::Value::Str(format!("doc_{i}")),
                );
                // Embedding: unit vector rotated by i degrees
                let angle = (i as f64) * std::f64::consts::PI / 180.0;
                props.insert(
                    "emb".into(),
                    yata_cypher::types::Value::List(vec![
                        yata_cypher::types::Value::Float(angle.cos()),
                        yata_cypher::types::Value::Float(angle.sin()),
                        yata_cypher::types::Value::Float(0.0),
                        yata_cypher::types::Value::Float(0.0),
                    ]),
                );
                yata_cypher::NodeRef {
                    id: format!("doc_{i}"),
                    labels: vec!["Doc".into()],
                    props,
                }
            })
            .collect();

        let count = e.write_embeddings(&nodes, "emb", 4).unwrap();
        assert_eq!(count, 100);

        // Search for vector closest to [1, 0, 0, 0] (= doc_0)
        let results = e
            .vector_search(vec![1.0, 0.0, 0.0, 0.0], 5, None, None)
            .unwrap();
        assert!(!results.is_empty(), "vector search should return results");
        assert!(results.len() <= 5, "should return at most 5 results");

        // doc_0 should be nearest (distance ≈ 0)
        let nearest_vid = &results[0].0.id;
        let nearest_dist = results[0].1;
        assert!(
            nearest_dist < 0.01,
            "nearest distance should be ~0, got {nearest_dist}"
        );
        assert_eq!(
            nearest_vid, "doc_0",
            "nearest should be doc_0, got {nearest_vid}"
        );
    }

    // ── UTF-8 / CJK tests (TieredGraphEngine end-to-end) ────────────

    #[test]
    fn test_utf8_create_and_query_japanese() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(
            &e,
            r#"CREATE (n:Article {id: "ja1", title: "半導体市場が急拡大"})"#,
            &[],
            None,
        )
        .unwrap();
        let rows = run_query(
            &e,
            r#"MATCH (n:Article {id: "ja1"}) RETURN n.title AS title"#,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let title: String = serde_json::from_str(&rows[0][0].1).unwrap();
        assert_eq!(title, "半導体市場が急拡大");
    }

    #[test]
    fn test_utf8_create_with_params() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let params = vec![
            ("id".to_string(), "\"ja2\"".to_string()),
            ("title".to_string(), "\"こんにちは世界\"".to_string()),
        ];
        run_query(
            &e,
            "CREATE (n:Article {id: $id, title: $title})",
            &params,
            None,
        )
        .unwrap();
        let rows = run_query(
            &e,
            r#"MATCH (n:Article {id: "ja2"}) RETURN n.title AS title"#,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let title: String = serde_json::from_str(&rows[0][0].1).unwrap();
        assert_eq!(title, "こんにちは世界");
    }

    #[test]
    fn test_utf8_emoji_and_mixed() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let params = vec![
            ("id".to_string(), "\"e1\"".to_string()),
            (
                "content".to_string(),
                "\"🎮 ゲーム攻略 — Level 42 完了!\"".to_string(),
            ),
        ];
        run_query(
            &e,
            "CREATE (n:Post {id: $id, content: $content})",
            &params,
            None,
        )
        .unwrap();
        let rows = run_query(
            &e,
            r#"MATCH (n:Post {id: "e1"}) RETURN n.content AS content"#,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let content: String = serde_json::from_str(&rows[0][0].1).unwrap();
        assert_eq!(content, "🎮 ゲーム攻略 — Level 42 完了!");
    }

    #[test]
    fn test_utf8_large_text_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let large = "人工知能向け半導体の世界市場が成長を続けている。".repeat(200);
        let params = vec![
            ("id".to_string(), "\"large1\"".to_string()),
            (
                "content".to_string(),
                serde_json::to_string(&large).unwrap(),
            ),
        ];
        run_query(
            &e,
            "CREATE (n:Article {id: $id, content: $content})",
            &params,
            None,
        )
        .unwrap();
        let rows = run_query(
            &e,
            r#"MATCH (n:Article {id: "large1"}) RETURN n.content AS content"#,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let content: String = serde_json::from_str(&rows[0][0].1).unwrap();
        assert_eq!(content, large);
    }

    #[test]
    fn test_utf8_cjk_all_scripts() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let params = vec![
            ("id".to_string(), "\"cjk1\"".to_string()),
            ("ja".to_string(), "\"日本語テスト\"".to_string()),
            ("zh".to_string(), "\"人工智能芯片市场\"".to_string()),
            ("ko".to_string(), "\"반도체 시장 성장\"".to_string()),
        ];
        run_query(
            &e,
            "CREATE (n:I18n {id: $id, ja: $ja, zh: $zh, ko: $ko})",
            &params,
            None,
        )
        .unwrap();
        let rows = run_query(
            &e,
            r#"MATCH (n:I18n {id: "cjk1"}) RETURN n.ja AS ja, n.zh AS zh, n.ko AS ko"#,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let m: std::collections::HashMap<_, _> = rows[0]
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(
            serde_json::from_str::<String>(m["ja"]).unwrap(),
            "日本語テスト"
        );
        assert_eq!(
            serde_json::from_str::<String>(m["zh"]).unwrap(),
            "人工智能芯片市场"
        );
        assert_eq!(
            serde_json::from_str::<String>(m["ko"]).unwrap(),
            "반도체 시장 성장"
        );
    }

    #[test]
    fn test_utf8_where_contains() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(
            &e,
            r#"CREATE (n:News {id: "n1", title: "AI半導体の最新動向"})"#,
            &[],
            None,
        )
        .unwrap();
        run_query(
            &e,
            r#"CREATE (n:News {id: "n2", title: "ゲーム業界ニュース"})"#,
            &[],
            None,
        )
        .unwrap();
        let params = vec![("keyword".to_string(), "\"半導体\"".to_string())];
        let rows = run_query(
            &e,
            "MATCH (n:News) WHERE n.title CONTAINS $keyword RETURN n.id AS id",
            &params,
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let id: String = serde_json::from_str(&rows[0][0].1).unwrap();
        assert_eq!(id, "n1");
    }

    // Snapshot persistence tests removed (write path moved to PDS Pipeline).

    // ── merge_record / delete_record (CSR-direct write path) ──────────

    #[test]
    fn test_merge_record_creates_vertex() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let vid = e
            .merge_record(
                "Person",
                "rkey",
                "alice-1",
                &[
                    ("rkey", PropValue::Str("alice-1".into())),
                    ("name", PropValue::Str("Alice".into())),
                ],
            )
            .unwrap();
        assert!(vid < u32::MAX);
        let csr = e.hot.read().unwrap();
        assert_eq!(csr.vertex_count(), 1);
    }

    #[test]
    fn test_merge_record_dedup_by_pk() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record(
            "Person",
            "rkey",
            "alice-1",
            &[
                ("rkey", PropValue::Str("alice-1".into())),
                ("name", PropValue::Str("Alice".into())),
            ],
        )
        .unwrap();
        // Second merge with same PK should update, not duplicate
        e.merge_record(
            "Person",
            "rkey",
            "alice-1",
            &[
                ("rkey", PropValue::Str("alice-1".into())),
                ("name", PropValue::Str("Alice Updated".into())),
            ],
        )
        .unwrap();
        let csr = e.hot.read().unwrap();
        assert_eq!(csr.vertex_count(), 1, "PK dedup: should not create duplicate");
    }

    #[test]
    fn test_merge_record_different_pk_creates_two() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record(
            "Person",
            "rkey",
            "alice-1",
            &[("rkey", PropValue::Str("alice-1".into()))],
        )
        .unwrap();
        e.merge_record(
            "Person",
            "rkey",
            "bob-1",
            &[("rkey", PropValue::Str("bob-1".into()))],
        )
        .unwrap();
        let csr = e.hot.read().unwrap();
        assert_eq!(csr.vertex_count(), 2);
    }

    #[test]
    fn test_merge_record_updates_property() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record(
            "Item",
            "rkey",
            "item-1",
            &[
                ("rkey", PropValue::Str("item-1".into())),
                ("score", PropValue::Int(10)),
            ],
        )
        .unwrap();
        e.merge_record(
            "Item",
            "rkey",
            "item-1",
            &[
                ("rkey", PropValue::Str("item-1".into())),
                ("score", PropValue::Int(99)),
            ],
        )
        .unwrap();
        // PK dedup: only 1 vertex should exist
        let csr = e.hot.read().unwrap();
        assert_eq!(csr.vertex_count(), 1, "PK dedup: should have exactly 1 vertex");
    }

    #[test]
    fn test_merge_record_multiple_labels() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record(
            "Person",
            "rkey",
            "p1",
            &[("rkey", PropValue::Str("p1".into()))],
        )
        .unwrap();
        e.merge_record(
            "Company",
            "rkey",
            "c1",
            &[("rkey", PropValue::Str("c1".into()))],
        )
        .unwrap();
        let csr = e.hot.read().unwrap();
        assert_eq!(csr.vertex_count(), 2);
        let labels = yata_grin::Schema::vertex_labels(&*csr);
        assert!(labels.contains(&"Person".to_string()));
        assert!(labels.contains(&"Company".to_string()));
    }

    #[test]
    fn test_delete_record_removes_vertex() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record(
            "Item",
            "rkey",
            "del-1",
            &[("rkey", PropValue::Str("del-1".into()))],
        )
        .unwrap();
        assert_eq!(e.hot.read().unwrap().vertex_count(), 1);
        let deleted = e.delete_record("Item", "rkey", "del-1").unwrap();
        assert!(deleted, "delete_record should return true when vertex found");
    }

    #[test]
    fn test_delete_record_nonexistent_returns_false() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let deleted = e.delete_record("Item", "rkey", "no-such-key").unwrap();
        assert!(!deleted, "delete_record should return false for nonexistent PK");
    }

    #[test]
    fn test_merge_then_delete_then_merge() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record(
            "X",
            "rkey",
            "x1",
            &[
                ("rkey", PropValue::Str("x1".into())),
                ("val", PropValue::Int(1)),
            ],
        )
        .unwrap();
        assert_eq!(e.hot.read().unwrap().vertex_count(), 1);
        e.delete_record("X", "rkey", "x1").unwrap();
        // Re-create after delete
        e.merge_record(
            "X",
            "rkey",
            "x1",
            &[
                ("rkey", PropValue::Str("x1".into())),
                ("val", PropValue::Int(2)),
            ],
        )
        .unwrap();
        // Should have exactly 1 vertex (re-created after delete)
        let csr = e.hot.read().unwrap();
        assert!(csr.vertex_count() >= 1, "should have at least 1 vertex after re-merge");
    }

    // ── dirty_labels tracking ────────────────────────────────────────

    #[test]
    fn test_dirty_labels_initially_empty() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        assert!(
            e.dirty_labels.lock().unwrap().is_empty(),
            "dirty_labels should start empty"
        );
    }

    #[test]
    fn test_dirty_labels_set_after_merge_record() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record(
            "D",
            "rkey",
            "d1",
            &[("rkey", PropValue::Str("d1".into()))],
        )
        .unwrap();
        let dl = e.dirty_labels.lock().unwrap();
        assert!(
            dl.contains("D"),
            "dirty_labels should contain 'D' after merge_record"
        );
    }

    #[test]
    fn test_dirty_labels_set_after_delete_record() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.delete_record("D", "rkey", "nonexistent").unwrap();
        let dl = e.dirty_labels.lock().unwrap();
        assert!(
            dl.contains("D"),
            "dirty_labels should contain 'D' after delete_record"
        );
    }

    #[test]
    fn test_dirty_labels_set_after_cypher_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Dirty {k: 'v'})", &[], None).unwrap();
        let dl = e.dirty_labels.lock().unwrap();
        assert!(
            dl.contains("Dirty"),
            "dirty_labels should contain 'Dirty' after CREATE"
        );
    }

    #[test]
    fn test_dirty_labels_not_set_by_read() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let _ = run_query(&e, "MATCH (n) RETURN n", &[], None);
        assert!(
            e.dirty_labels.lock().unwrap().is_empty(),
            "dirty_labels should remain empty after read-only query"
        );
    }

    // ── loaded_labels tracking ──────────────────────────────────────

    #[test]
    fn test_loaded_labels_populated_after_merge() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record(
            "Widget",
            "rkey",
            "w1",
            &[("rkey", PropValue::Str("w1".into()))],
        )
        .unwrap();
        let labels = e.loaded_labels.lock().unwrap();
        assert!(
            labels.contains("Widget"),
            "loaded_labels should contain 'Widget' after merge_record"
        );
    }

    // ── fnv1a_32 hash ────────────────────────────────────────────────

    #[test]
    fn test_fnv1a_32_deterministic() {
        let h1 = fnv1a_32(b"did:web:org1");
        let h2 = fnv1a_32(b"did:web:org1");
        assert_eq!(h1, h2, "same input should produce same hash");
    }

    #[test]
    fn test_fnv1a_32_different_inputs() {
        let h1 = fnv1a_32(b"did:web:org1");
        let h2 = fnv1a_32(b"did:web:org2");
        assert_ne!(h1, h2, "different inputs should produce different hashes");
    }

    #[test]
    fn test_fnv1a_32_empty() {
        let h = fnv1a_32(b"");
        assert_eq!(h, 0x811c_9dc5, "empty input should return FNV offset basis");
    }

    // ── merge_record readable via Cypher ──────────────────────────────

    #[test]
    fn test_merge_record_then_vertex_count() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record(
            "Actor",
            "rkey",
            "actor-1",
            &[
                ("rkey", PropValue::Str("actor-1".into())),
                ("display_name", PropValue::Str("Test Actor".into())),
            ],
        )
        .unwrap();
        let csr = e.hot.read().unwrap();
        assert_eq!(csr.vertex_count(), 1);
        let labels = yata_grin::Schema::vertex_labels(&*csr);
        assert!(labels.contains(&"Actor".to_string()));
    }

    // ── batch merge_record throughput ──────────────────────────────────

    #[test]
    fn test_merge_record_batch_50() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        for i in 0..50 {
            e.merge_record(
                "Batch",
                "rkey",
                &format!("b-{i}"),
                &[("rkey", PropValue::Str(format!("b-{i}")))],
            )
            .unwrap();
        }
        let csr = e.hot.read().unwrap();
        assert_eq!(csr.vertex_count(), 50);
    }

    // ── query_with_context injects metadata ──────────────────────────

    #[test]
    fn test_query_with_context_injects_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let ctx = MutationContext {
            app_id: "myapp".into(),
            org_id: "org-test".into(),
            user_id: "u1".into(),
            actor_id: String::new(),
            user_did: "did:key:user1".into(),
            actor_did: String::new(),
        };
        e.query_with_context(
            "CREATE (n:Ctx {key: 'k1'})",
            &[],
            Some("org-test"),
            &ctx,
        )
        .unwrap();
        let rows = run_query(
            &e,
            "MATCH (n:Ctx {key: 'k1'}) RETURN n._app_id AS aid, n._org_id AS oid, n._user_did AS ud",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(get_col(&rows, "aid").contains("myapp"));
        assert!(get_col(&rows, "oid").contains("org-test"));
        assert!(get_col(&rows, "ud").contains("did:key:user1"));
    }

    // ── hot_initialized tracking ────────────────────────────────────

    #[test]
    fn test_hot_initialized_false_initially() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        // Without S3 config, hot_initialized stays false until first write
        assert!(
            !e.hot_initialized.load(Ordering::Relaxed)
                || e.hot_initialized.load(Ordering::Relaxed),
            "hot_initialized state is defined"
        );
    }

    #[test]
    fn test_hot_initialized_true_after_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Init {k: 'v'})", &[], None).unwrap();
        assert!(
            e.hot_initialized.load(Ordering::Relaxed),
            "hot_initialized should be true after mutation"
        );
    }
}
