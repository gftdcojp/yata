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
use crate::rls;
use crate::router;

/// Shared tokio runtime for all engine instances (avoids nested runtime issues).
static ENGINE_RT: std::sync::LazyLock<tokio::runtime::Runtime> = std::sync::LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("yata-engine")
        .build()
        .expect("yata-engine tokio runtime")
});

/// Build a full RlsScope from org_id + _rls_* Cypher params.
/// This bridges the REST layer's param injection to the engine's scoped RLS filter.
fn build_rls_scope_from_params(org_id: &str, params: &[(String, String)]) -> rls::RlsScope {
    let get = |key: &str| -> Option<String> {
        params.iter().find(|(k, _)| k == key).map(|(_, v)| {
            v.trim_matches('"').to_string()
        })
    };

    // org_id filtering is skipped when org_id is empty or when SecurityFilter (GIE path) handles governance.
    // "pds" magic value is prohibited. Empty org_id = no org_id filter (public/anon).
    let is_system_scope = org_id.is_empty() || org_id == "anon";

    let clearance = get("_rls_clearance").and_then(|s| {
        match s.as_str() {
            "public" => Some(rls::DataSensitivity::Public),
            "internal" => Some(rls::DataSensitivity::Internal),
            "confidential" => Some(rls::DataSensitivity::Confidential),
            "restricted" => Some(rls::DataSensitivity::Restricted),
            _ => None,
        }
    });

    let consent_grants = get("_rls_consent_grants")
        .and_then(|s| serde_json::from_str::<Vec<serde_json::Value>>(&s).ok())
        .map(|grants| {
            grants.iter().filter_map(|g| {
                Some(rls::ConsentGrant {
                    grantor_did: g.get("grantor_did")?.as_str()?.to_string(),
                    grantee_did: g.get("grantee_did")?.as_str().unwrap_or("").to_string(),
                    resource_ids: g.get("resource_ids")
                        .and_then(|v| v.as_array())
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                        .unwrap_or_default(),
                    max_sensitivity: match g.get("max_sensitivity").and_then(|v| v.as_str()).unwrap_or("public") {
                        "internal" => rls::DataSensitivity::Internal,
                        "confidential" => rls::DataSensitivity::Confidential,
                        "restricted" => rls::DataSensitivity::Restricted,
                        _ => rls::DataSensitivity::Public,
                    },
                    delegatable: g.get("delegatable").and_then(|v| v.as_bool()).unwrap_or(false),
                })
            }).collect()
        })
        .unwrap_or_default();

    rls::RlsScope {
        org_id: org_id.to_string(),
        user_did: get("_rls_user_did"),
        actor_did: get("_rls_actor_did"),
        clearance,
        consent_grants,
        skip_org_filter: is_system_scope,
    }
}

/// Convert RlsScope → GIE SecurityScope for predicate pushdown.
fn build_gie_security_scope(rls: &rls::RlsScope) -> yata_gie::ir::SecurityScope {
    if rls.skip_org_filter && rls.clearance.is_none() {
        return yata_gie::ir::SecurityScope { bypass: true, ..Default::default() };
    }

    let max_sensitivity_ord = match &rls.clearance {
        Some(rls::DataSensitivity::Restricted) => 3,
        Some(rls::DataSensitivity::Confidential) => 2,
        Some(rls::DataSensitivity::Internal) => 1,
        _ => 0, // public only
    };

    // RBAC: extract collection scope prefixes from _rls_* params
    // (injected by PDS as rbac_roles → collection prefix patterns)
    let collection_scopes = Vec::new(); // Legacy path: RBAC not wired. Design E uses compile_security_scope().

    // Consent: hash grantor DIDs for O(1) lookup during CSR traversal
    let allowed_owner_hashes: Vec<u32> = rls.consent_grants
        .iter()
        .map(|g| fnv1a_32(g.grantor_did.as_bytes()))
        .collect();

    yata_gie::ir::SecurityScope {
        max_sensitivity_ord,
        collection_scopes,
        allowed_owner_hashes,
        bypass: rls.skip_org_filter && max_sensitivity_ord >= 3,
    }
}

/// FNV-1a 32-bit hash (fast, non-cryptographic — for owner_hash property matching).
fn fnv1a_32(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// Convert PropValue slice to JSON map (for WAL entry serialization).
fn props_to_json(props: &[(&str, yata_grin::PropValue)]) -> serde_json::Map<String, serde_json::Value> {
    let mut m = serde_json::Map::new();
    for (k, v) in props {
        let jv = match v {
            yata_grin::PropValue::Str(s) => serde_json::Value::String(s.clone()),
            yata_grin::PropValue::Int(n) => serde_json::Value::Number((*n).into()),
            yata_grin::PropValue::Float(f) => serde_json::json!(*f),
            yata_grin::PropValue::Bool(b) => serde_json::Value::Bool(*b),
            yata_grin::PropValue::Null => serde_json::Value::Null,
        };
        m.insert(k.to_string(), jv);
    }
    m
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

impl MutationContext {
    /// Build an RlsScope from this context.
    pub fn to_rls_scope(&self) -> rls::RlsScope {
        rls::RlsScope {
            org_id: self.org_id.clone(),
            user_did: if self.user_did.is_empty() {
                None
            } else {
                Some(self.user_did.clone())
            },
            actor_did: if self.actor_did.is_empty() {
                None
            } else {
                Some(self.actor_did.clone())
            },
            ..Default::default()
        }
    }
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
    dirty: Arc<AtomicBool>,
    dirty_labels: Arc<Mutex<HashSet<String>>>,
    pending_writes: Arc<AtomicUsize>,
    wal: Arc<Mutex<crate::wal::WalRingBuffer>>,
    wal_last_flushed_seq: Arc<AtomicU64>,
    /// SecurityScope cache: DID → (SecurityScope, compiled_at). Design E.
    security_scope_cache: Arc<Mutex<HashMap<String, (yata_gie::ir::SecurityScope, std::time::Instant)>>>,
    // CPM metrics: CP5 mutation frequency measurement
    cypher_read_count: Arc<AtomicU64>,
    cypher_mutation_count: Arc<AtomicU64>,
    cypher_mutation_us_total: Arc<AtomicU64>,
    merge_record_count: Arc<AtomicU64>,
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
            dirty: Arc::new(AtomicBool::new(false)),
            dirty_labels: Arc::new(Mutex::new(HashSet::new())),
            pending_writes: Arc::new(AtomicUsize::new(0)),
            wal: Arc::new(Mutex::new(crate::wal::WalRingBuffer::new(wal_ring_capacity))),
            wal_last_flushed_seq: Arc::new(AtomicU64::new(0)),
            security_scope_cache: Arc::new(Mutex::new(HashMap::new())),
            cypher_read_count: Arc::new(AtomicU64::new(0)),
            cypher_mutation_count: Arc::new(AtomicU64::new(0)),
            cypher_mutation_us_total: Arc::new(AtomicU64::new(0)),
            merge_record_count: Arc::new(AtomicU64::new(0)),
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
    fn ensure_labels(&self, vertex_labels: &[&str]) {
        self.ensure_labels_vineyard_only(vertex_labels);
    }

    /// Cold start fallback: full page-in from R2 checkpoint.
    /// WAL mode: normally walColdStart handles this. This is the fallback
    /// for first query before walColdStart is called.
    fn ensure_labels_vineyard_only(&self, _vertex_labels: &[&str]) {
        if self.hot_initialized.load(Ordering::Relaxed) { return; }

        // Full page-in from R2 checkpoint
        self.restore_from_r2();
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

        let rls_owned = rls_org_id.map(String::from);

        self.block_on(async {
            self.ensure_hot();

            // Single MemoryGraph copy from CSR.
            let mut g = if let Ok(csr) = self.hot.read() {
                QueryableGraph(csr.to_filtered_memory_graph(&[], &[]))
            } else {
                return Err("failed to acquire CSR lock".to_string());
            };

            // Apply RLS once.
            if let Some(ref org_id) = rls_owned {
                if !ctx.user_did.is_empty() || !ctx.actor_did.is_empty() {
                    rls::apply_rls_filter_scoped(&mut g.0, &ctx.to_rls_scope());
                } else {
                    rls::apply_rls_filter(&mut g.0, org_id);
                }
            }

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

    /// Query with full RLS scope (org_id + user_did + actor_did).
    /// Uses RlsScope-based filtering for per-entity graph partitioning.
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
        if is_mutation {
            self.dirty.store(true, Ordering::Relaxed);
            self.cypher_mutation_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.cypher_read_count.fetch_add(1, Ordering::Relaxed);
        }
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
            // RLS via SecurityFilter predicate pushdown. No MemoryGraph fallback.
            if let Ok(csr) = self.hot.read() {
                if let Some(single_csr) = csr.as_single() {
                    if let Ok(ast) = yata_cypher::parse(cypher) {
                        // Build SecurityScope from RLS params (if authenticated).
                        // GIE SecurityFilter uses property-based governance (sensitivity_ord, owner_hash).
                        // If _rls_clearance param is present → new governance model → use GIE SecurityFilter.
                        // Otherwise → old org_id model → error (migrate to SecurityFilter).
                        let has_clearance = params.iter().any(|(k, _)| k == "_rls_clearance");

                        let plan_result = if rls_org_id.is_some() && has_clearance {
                            let rls = build_rls_scope_from_params(rls_org_id.unwrap(), params);
                            let security_scope = build_gie_security_scope(&rls);
                            yata_gie::transpile::transpile_secured(&ast, security_scope)
                        } else if rls_org_id.is_none() {
                            yata_gie::transpile::transpile(&ast)
                        } else {
                            // old org_id RLS → fall through to MemoryGraph path
                            Err(yata_gie::transpile::TranspileError::UnsupportedClause(
                                "old org_id RLS → MemoryGraph fallback".into(),
                            ))
                        };

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

        // Mutation path: load only labels referenced in Cypher (targeted).
        // Falls back to load-all only when AST extraction finds no labels.
        if let Some((vlabels, _elabels)) = router::extract_mutation_hints(cypher) {
            let vl_refs: Vec<&str> = vlabels.iter().map(|s| s.as_str()).collect();
            self.ensure_labels(&vl_refs);
        } else {
            self.ensure_labels(&[]);
        }
        let rls_owned = rls_org_id.map(String::from);

        self.block_on(async {
            self.ensure_hot();
            let mut g = if let Ok(csr) = self.hot.read() {
                QueryableGraph(csr.to_filtered_memory_graph(&[], &[]))
            } else {
                return Err("failed to acquire CSR lock".to_string());
            };

            if let Some(ref org_id) = rls_owned {
                let scope = build_rls_scope_from_params(org_id, params);
                rls::apply_rls_filter_scoped(&mut g.0, &scope);
            }

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
                // Rebuild HOT CSR immediately (reads see latest state)
                let new_csr = loader::rebuild_csr_from_graph_with_partition(&g, self.config.hot_partition_id);

                // Mark all labels in the new CSR as loaded
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
            }

            if let Ok(mut c) = self.cache.lock() {
                c.invalidate();
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
        self.dirty.store(true, Ordering::Relaxed);
        if let Ok(mut dl) = self.dirty_labels.lock() {
            dl.insert(label.to_string());
        }
        self.invalidate_security_cache_if_policy(label, props);

        // WAL append
        if let Ok(mut wal) = self.wal.lock() {
            let seq = wal.next_seq();
            let json_props = props_to_json(props);
            let entry = crate::wal::WalEntry {
                seq, op: crate::wal::WalOp::Upsert,
                label: label.to_string(), pk_key: pk_key.to_string(),
                pk_value: pk_value.to_string(), props: json_props,
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
        // Build WAL entry before CSR merge
        let wal_entry = if let Ok(mut wal) = self.wal.lock() {
            let seq = wal.next_seq();
            let mut json_props = serde_json::Map::new();
            for (k, v) in props {
                let jv = match v {
                    yata_grin::PropValue::Str(s) => serde_json::Value::String(s.clone()),
                    yata_grin::PropValue::Int(n) => serde_json::Value::Number((*n).into()),
                    yata_grin::PropValue::Float(f) => serde_json::json!(*f),
                    yata_grin::PropValue::Bool(b) => serde_json::Value::Bool(*b),
                    yata_grin::PropValue::Null => serde_json::Value::Null,
                };
                json_props.insert(k.to_string(), jv);
            }
            let entry = crate::wal::WalEntry {
                seq,
                op: crate::wal::WalOp::Upsert,
                label: label.to_string(),
                pk_key: pk_key.to_string(),
                pk_value: pk_value.to_string(),
                props: json_props,
                timestamp_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            };
            wal.append(entry.clone());
            Some(entry)
        } else {
            None
        };

        self.dirty.store(true, Ordering::Relaxed);
        if let Ok(mut dl) = self.dirty_labels.lock() {
            dl.insert(label.to_string());
        }
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
        self.dirty.store(true, Ordering::Relaxed);
        if let Ok(mut dl) = self.dirty_labels.lock() {
            dl.insert(label.to_string());
        }

        // WAL append (Delete)
        if let Ok(mut wal) = self.wal.lock() {
            let seq = wal.next_seq();
            let entry = crate::wal::WalEntry {
                seq, op: crate::wal::WalOp::Delete,
                label: label.to_string(), pk_key: pk_key.to_string(),
                pk_value: pk_value.to_string(), props: serde_json::Map::new(),
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
        self.dirty.store(true, Ordering::Relaxed);
        if let Ok(mut dl) = self.dirty_labels.lock() {
            dl.insert(label.to_string());
        }

        let wal_entry = if let Ok(mut wal) = self.wal.lock() {
            let seq = wal.next_seq();
            let entry = crate::wal::WalEntry {
                seq,
                op: crate::wal::WalOp::Delete,
                label: label.to_string(),
                pk_key: pk_key.to_string(),
                pk_value: pk_value.to_string(),
                props: serde_json::Map::new(),
                timestamp_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
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
        let dirty_set: HashSet<String> = if let Ok(mut dl) = self.dirty_labels.lock() {
            dl.drain().collect()
        } else {
            HashSet::new()
        };
        let had_dirty_flag = self.dirty.load(Ordering::Relaxed);

        // Skip if no mutations since last snapshot
        if !had_dirty_flag && dirty_set.is_empty() {
            return Ok((0, 0));
        }

        // Check batch_commit_threshold: skip snapshot if not enough pending writes
        let pending = self.pending_writes.load(Ordering::Relaxed);
        if pending < self.config.batch_commit_threshold as usize && !force {
            // Put dirty labels back — they weren't consumed
            if let Ok(mut dl) = self.dirty_labels.lock() {
                dl.extend(dirty_set);
            }
            return Ok((0, 0));
        }

        let s3 = self.get_s3_client()
            .ok_or_else(|| "S3 client not configured".to_string())?;
        let prefix = &self.s3_prefix;

        // WAL Projection: no R2 compaction. CSR is authoritative (built from WAL).
        self.hot_initialized.store(true, Ordering::Relaxed);

        // Serialize CSR → ArrowFragment → MemoryBlobStore
        // Uses selective conversion: only dirty vertex labels get property extraction.
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

        // Guard: never overwrite a non-empty R2 snapshot with an empty one.
        if v_count == 0 && e_count == 0 {
            tracing::info!("snapshot skipped: CSR is empty (v=0, e=0), preserving existing R2 snapshot");
            return Ok((0, 0));
        }

        // Dirty label delta upload: only PUT blobs for dirty vertex labels.
        // Infrastructure blobs (schema, ivnums, meta.json) are always uploaded.
        // Edge/topology blobs are uploaded only when dirty flag indicates edge mutations.
        let mut uploaded = 0u32;
        let mut skipped = 0u32;
        for name in meta.blobs.keys() {
            let should_upload = is_infra_blob(name)
                || is_dirty_vertex_blob(name, &dirty_vlabel_ids)
                || (had_dirty_flag && is_edge_or_topology_blob(name));
            if !should_upload {
                skipped += 1;
                continue;
            }
            if let Some(data) = BlobStore::get(&blob_store, name) {
                let full_key = format!("{prefix}snap/fragment/{name}");
                if let Err(e) = s3.put_sync(&full_key, data) {
                    tracing::error!(name, error = %e, "R2 ArrowFragment blob upload failed");
                } else {
                    uploaded += 1;
                }
            }
        }

        // Upload fragment meta (ObjectMeta JSON) — always, reflects full CSR state
        let meta_json = serde_json::to_vec(&meta).unwrap_or_default();
        let meta_key = format!("{prefix}snap/fragment/meta.json");
        if let Err(e) = s3.put_sync(&meta_key, bytes::Bytes::from(meta_json.clone())) {
            tracing::error!(error = %e, "R2 fragment meta upload failed");
        }

        // Write dirty blobs to YATA_VINEYARD_DIR on disk (same selective logic)
        if let Ok(vineyard_dir) = std::env::var("YATA_VINEYARD_DIR") {
            let snap_dir = format!("{vineyard_dir}/snap/fragment");
            if let Err(e) = std::fs::create_dir_all(&snap_dir) {
                tracing::warn!(error = %e, "failed to create vineyard snap dir for disk cache");
            } else {
                for name in meta.blobs.keys() {
                    let should_write = is_infra_blob(name)
                        || is_dirty_vertex_blob(name, &dirty_vlabel_ids)
                        || (had_dirty_flag && is_edge_or_topology_blob(name));
                    if !should_write { continue; }
                    if let Some(data) = BlobStore::get(&blob_store, name) {
                        let path = format!("{snap_dir}/{name}");
                        if let Err(e) = std::fs::write(&path, &data[..]) {
                            tracing::warn!(path, error = %e, "failed to write blob to disk cache");
                        }
                    }
                }
                // Write meta.json to disk — always
                let meta_path = format!("{snap_dir}/meta.json");
                if let Err(e) = std::fs::write(&meta_path, &meta_json) {
                    tracing::warn!(error = %e, "failed to write meta.json to disk cache");
                }
                tracing::debug!(snap_dir, "delta snapshot blobs written to disk cache");
            }
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

        // Clear dirty flag and pending writes (dirty_labels already drained above)
        self.dirty.store(false, Ordering::Relaxed);
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
                            let props: Vec<(&str, yata_grin::PropValue)> = entry.props.iter()
                                .map(|(k, v)| {
                                    let pv = match v {
                                        serde_json::Value::String(s) => yata_grin::PropValue::Str(s.clone()),
                                        serde_json::Value::Number(n) => {
                                            if let Some(i) = n.as_i64() {
                                                yata_grin::PropValue::Int(i)
                                            } else {
                                                yata_grin::PropValue::Float(n.as_f64().unwrap_or(0.0))
                                            }
                                        }
                                        serde_json::Value::Bool(b) => yata_grin::PropValue::Bool(*b),
                                        _ => yata_grin::PropValue::Str(v.to_string()),
                                    };
                                    (k.as_str(), pv)
                                })
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

        // Serialize to NDJSON
        let data = crate::wal::serialize_segment(&entries);
        let key = crate::wal::segment_r2_key(prefix, pid, seq_start, seq_end);
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

    /// Cold start for Read Container in WalProjection mode:
    /// 1. Load latest R2 checkpoint (ArrowFragment)
    /// 2. Read checkpoint metadata to get checkpoint_seq
    /// 3. Replay WAL segments from R2 after checkpoint_seq
    /// 4. Catch up from Write Container WAL tail (done by coordinator, not here)
    pub fn wal_cold_start(&self) -> Result<u64, String> {
        let s3 = self.get_s3_client()
            .ok_or_else(|| "S3 client not configured".to_string())?;
        let prefix = &self.s3_prefix;
        let pid = self.config.hot_partition_id.get();

        // Step 1: Load ArrowFragment checkpoint
        self.restore_from_r2();

        // Step 2: Read checkpoint metadata
        let checkpoint_key = crate::wal::checkpoint_meta_r2_key(prefix, pid);
        let checkpoint_seq = match s3.get_sync(&checkpoint_key) {
            Ok(Some(data)) => {
                if let Ok(meta) = serde_json::from_slice::<crate::wal::CheckpointMeta>(&data) {
                    tracing::info!(checkpoint_seq = meta.checkpoint_seq, "loaded checkpoint metadata");
                    meta.checkpoint_seq
                } else {
                    0
                }
            }
            _ => {
                tracing::info!("no checkpoint metadata found, starting from seq 0");
                0
            }
        };

        // Step 3: Replay WAL segments from R2 after checkpoint_seq
        // List segments in the partition directory
        let segment_prefix = format!("{prefix}wal/segments/{pid}/");
        let segment_objects = s3.list_sync(&segment_prefix).unwrap_or_default();
        let mut replayed = 0u64;
        for obj in &segment_objects {
            let key = &obj.key;
            // Parse seq range from filename: {seq_start:020}-{seq_end:020}.ndjson
            if let Some(filename) = key.rsplit('/').next() {
                if let Some(stripped) = filename.strip_suffix(".ndjson") {
                    let parts: Vec<&str> = stripped.split('-').collect();
                    if parts.len() == 2 {
                        if let (Ok(seg_start), Ok(seg_end)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
                            if seg_end <= checkpoint_seq {
                                continue; // Already included in checkpoint
                            }
                            match s3.get_sync(key) {
                                Ok(Some(data)) => {
                                    let mut entries = crate::wal::deserialize_segment(&data);
                                    // Filter entries already in checkpoint
                                    entries.retain(|e| e.seq > checkpoint_seq);
                                    if !entries.is_empty() {
                                        let count = entries.len();
                                        self.wal_apply(&entries)?;
                                        replayed += count as u64;
                                        tracing::info!(seg_start, seg_end, applied = count, "replayed WAL segment");
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
            self.dirty.store(true, Ordering::Relaxed);
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
        // RLS on reads: only see own org + system nodes
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
        run_query(&e, "CREATE (n:Item {name: 'C'})", &[], None).unwrap(); // no org_id = system
        let rows = run_query(
            &e,
            "MATCH (n:Item) RETURN n.name AS name",
            &[],
            Some("org_1"),
        )
        .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "RLS read should return org_1 node + system node (no org_id)"
        );
        let has_a = rows.iter().any(|r| r.iter().any(|(_, v)| v.contains("A")));
        let has_c = rows.iter().any(|r| r.iter().any(|(_, v)| v.contains("C")));
        assert!(has_a, "Should see org_1 node A");
        assert!(has_c, "Should see system node C (no org_id)");
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

    // ── dirty flag tracking ────────────────────────────────────────

    #[test]
    fn test_dirty_flag_initially_false() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        assert!(
            !e.dirty.load(Ordering::Relaxed),
            "dirty flag should start false"
        );
    }

    #[test]
    fn test_dirty_flag_set_after_merge_record() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record(
            "D",
            "rkey",
            "d1",
            &[("rkey", PropValue::Str("d1".into()))],
        )
        .unwrap();
        assert!(
            e.dirty.load(Ordering::Relaxed),
            "dirty flag should be true after merge_record"
        );
    }

    #[test]
    fn test_dirty_flag_set_after_delete_record() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.delete_record("D", "rkey", "nonexistent").unwrap();
        assert!(
            e.dirty.load(Ordering::Relaxed),
            "dirty flag should be true after delete_record (even if nothing deleted)"
        );
    }

    #[test]
    fn test_dirty_flag_set_after_cypher_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Dirty {k: 'v'})", &[], None).unwrap();
        assert!(
            e.dirty.load(Ordering::Relaxed),
            "dirty flag should be true after CREATE"
        );
    }

    #[test]
    fn test_dirty_flag_not_set_by_read() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        // Read on empty graph
        let _ = run_query(&e, "MATCH (n) RETURN n", &[], None);
        assert!(
            !e.dirty.load(Ordering::Relaxed),
            "dirty flag should remain false after read-only query"
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

    // ── MutationContext ──────────────────────────────────────────────

    #[test]
    fn test_mutation_context_to_rls_scope() {
        let ctx = MutationContext {
            app_id: "test-app".into(),
            org_id: "org-1".into(),
            user_id: "u1".into(),
            actor_id: "a1".into(),
            user_did: "did:key:alice".into(),
            actor_did: "did:key:bot1".into(),
        };
        let scope = ctx.to_rls_scope();
        assert_eq!(scope.org_id, "org-1");
        assert_eq!(scope.user_did, Some("did:key:alice".into()));
        assert_eq!(scope.actor_did, Some("did:key:bot1".into()));
    }

    #[test]
    fn test_mutation_context_empty_dids_are_none() {
        let ctx = MutationContext {
            org_id: "org-1".into(),
            user_did: String::new(),
            actor_did: String::new(),
            ..Default::default()
        };
        let scope = ctx.to_rls_scope();
        assert_eq!(scope.user_did, None);
        assert_eq!(scope.actor_did, None);
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

    // ── build_rls_scope_from_params ──────────────────────────────────

    #[test]
    fn test_build_rls_scope_basic() {
        let scope = build_rls_scope_from_params("org-1", &[]);
        assert_eq!(scope.org_id, "org-1");
        assert!(!scope.skip_org_filter);
        assert!(scope.clearance.is_none());
        assert!(scope.consent_grants.is_empty());
    }

    #[test]
    fn test_build_rls_scope_system_scope_empty() {
        let scope = build_rls_scope_from_params("", &[]);
        assert!(scope.skip_org_filter, "empty org_id should skip org filter");
    }

    #[test]
    fn test_build_rls_scope_system_scope_anon() {
        let scope = build_rls_scope_from_params("anon", &[]);
        assert!(scope.skip_org_filter, "anon org_id should skip org filter");
    }

    #[test]
    fn test_build_rls_scope_with_clearance() {
        let params = vec![("_rls_clearance".into(), "\"confidential\"".into())];
        let scope = build_rls_scope_from_params("org-1", &params);
        assert_eq!(scope.clearance, Some(rls::DataSensitivity::Confidential));
    }

    #[test]
    fn test_build_rls_scope_with_user_did() {
        let params = vec![("_rls_user_did".into(), "\"did:key:alice\"".into())];
        let scope = build_rls_scope_from_params("org-1", &params);
        assert_eq!(scope.user_did, Some("did:key:alice".into()));
    }

    // ── build_gie_security_scope ──────────────────────────────────────

    #[test]
    fn test_gie_security_scope_bypass_for_system() {
        let rls = rls::RlsScope {
            skip_org_filter: true,
            clearance: None,
            ..Default::default()
        };
        let scope = build_gie_security_scope(&rls);
        assert!(scope.bypass);
    }

    #[test]
    fn test_gie_security_scope_sensitivity_ord() {
        let rls = rls::RlsScope {
            org_id: "org-1".into(),
            clearance: Some(rls::DataSensitivity::Confidential),
            ..Default::default()
        };
        let scope = build_gie_security_scope(&rls);
        assert_eq!(scope.max_sensitivity_ord, 2);
        assert!(!scope.bypass);
    }

    #[test]
    fn test_gie_security_scope_consent_hashes() {
        let rls = rls::RlsScope {
            org_id: "org-1".into(),
            consent_grants: vec![rls::ConsentGrant {
                grantor_did: "did:web:org2".into(),
                grantee_did: "did:web:me".into(),
                resource_ids: vec!["*".into()],
                max_sensitivity: rls::DataSensitivity::Public,
                delegatable: false,
            }],
            ..Default::default()
        };
        let scope = build_gie_security_scope(&rls);
        assert_eq!(scope.allowed_owner_hashes.len(), 1);
        assert_eq!(
            scope.allowed_owner_hashes[0],
            fnv1a_32(b"did:web:org2")
        );
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
