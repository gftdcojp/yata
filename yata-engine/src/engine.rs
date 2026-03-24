use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use yata_cypher::Graph;
use yata_graph::{GraphStore, QueryableGraph};
use yata_grin::{Mutable, PropValue, Topology};
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
/// This bridges the MemoryGraph-based RLS to the GIE CSR-direct path.
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
    let collection_scopes = Vec::new(); // TODO: pass from PDS rbac_roles

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

/// Graph engine: HOT (MutableCSR in-memory) + Snapshot (R2 persistent).
///
/// Integrates: query routing, cache, RLS, snapshot persistence, vector search passthrough.
pub struct TieredGraphEngine {
    config: TieredEngineConfig,
    /// HOT tier: in-memory CSR store (single or partitioned).
    hot: Arc<Mutex<yata_store::GraphStoreEnum>>,
    /// GraphStore — vector search (yata-vex) only. NOT graph persistence.
    warm: GraphStore,
    /// Query result cache.
    cache: Arc<Mutex<QueryCache>>,
    /// Whether the HOT tier has been initialized.
    hot_initialized: Arc<AtomicBool>,
    /// Labels already loaded into HOT CSR.
    loaded_labels: Arc<Mutex<HashSet<String>>>,
    /// Vineyard blob store: DiskVineyard (Container disk) or EdgeVineyard (in-memory fallback).
    vineyard: Arc<dyn VineyardStore>,
    /// Last known fragment manifest (from snapshot or restore).
    fragment_manifest: Arc<Mutex<Option<FragmentManifest>>>,
    /// S3/R2 client for R2 read (page-in). Lazy-initialized on first use.
    s3_client: Arc<Mutex<Option<Arc<yata_s3::s3::S3Client>>>>,
    /// S3 key prefix for R2 read (e.g. "yata/").
    s3_prefix: String,
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

        // Arrow store is default (Vineyard-native, no CSR conversion).
        // Legacy CSR: set YATA_USE_CSR=true to fall back.
        let use_csr = std::env::var("YATA_USE_CSR").unwrap_or_default() == "true";
        let hot_store = if use_csr {
            tracing::info!("using legacy MutableCsrStore (YATA_USE_CSR=true)");
            yata_store::GraphStoreEnum::new(partition_count, hot_partition_id)
        } else {
            tracing::info!("using ArrowGraphStore (Vineyard-native, default)");
            yata_store::GraphStoreEnum::new_arrow(hot_partition_id)
        };
        Self {
            config,
            hot: Arc::new(Mutex::new(hot_store)),
            warm,
            cache: Arc::new(Mutex::new(cache)),
            hot_initialized: Arc::new(AtomicBool::new(true)),
            loaded_labels: Arc::new(Mutex::new(HashSet::new())),
            vineyard,
            fragment_manifest: Arc::new(Mutex::new(None)),
            s3_client,
            s3_prefix,
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
    fn ensure_labels(&self, vertex_labels: &[&str], edge_labels: &[&str]) {
        self.ensure_labels_vineyard_only(vertex_labels, edge_labels);
    }

    /// Page-in from R2 ArrowFragment snapshot.
    /// Fetches `snap/fragment/meta.json` + blobs → ArrowFragment::deserialize → CSR restore.
    /// Full graph is loaded atomically (no per-label partial page-in).
    fn ensure_labels_vineyard_only(&self, _vertex_labels: &[&str], _edge_labels: &[&str]) {
        // If already initialized with data, skip
        if self.hot_initialized.load(Ordering::Relaxed) {
            return;
        }

        let s3 = match self.get_s3_client() {
            Some(s3) => s3.clone(),
            None => return,
        };

        match crate::loader::page_in_from_r2(
            &s3,
            &self.s3_prefix,
            self.config.hot_partition_id,
        ) {
            Ok(store) => {
                let vc = store.vertex_count();
                let ec = store.edge_count();
                if vc == 0 && ec == 0 {
                    tracing::debug!("R2 page-in: empty fragment, skipping");
                    return;
                }
                if let Ok(mut hot) = self.hot.lock() {
                    *hot = yata_store::GraphStoreEnum::Single(store);
                    self.hot_initialized.store(true, Ordering::Relaxed);
                }
                tracing::info!(vertices = vc, edges = ec, "R2 ArrowFragment page-in complete");
            }
            Err(e) => {
                tracing::debug!("R2 ArrowFragment page-in failed (may not exist yet): {e}");
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

        // Collect all label hints across all statements for a single label load.
        let mut all_vlabels = Vec::new();
        let mut all_elabels = Vec::new();
        for (cypher, _) in statements {
            if let Some((vl, el)) = router::extract_mutation_hints(cypher) {
                all_vlabels.extend(vl);
                all_elabels.extend(el);
            }
        }
        all_vlabels.sort();
        all_vlabels.dedup();
        all_elabels.sort();
        all_elabels.dedup();

        let vl_refs: Vec<&str> = all_vlabels.iter().map(|s| s.as_str()).collect();
        let el_refs: Vec<&str> = all_elabels.iter().map(|s| s.as_str()).collect();
        if !vl_refs.is_empty() || !el_refs.is_empty() {
            self.ensure_labels(&vl_refs, &el_refs);
        } else {
            self.ensure_labels(&[], &[]);
        }

        let rls_owned = rls_org_id.map(String::from);

        self.block_on(async {
            self.ensure_hot();

            // Single MemoryGraph copy from CSR.
            let mut g = if let Ok(csr) = self.hot.lock() {
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

                if let Ok(mut hot) = self.hot.lock() {
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

            // Load only labels referenced in Cypher (on demand from CAS)
            let (hints_labels, hints_rels) =
                router::extract_pushdown_hints(cypher).unwrap_or_default();
            let vl_refs: Vec<&str> = hints_labels.iter().map(|s| s.as_str()).collect();
            let el_refs: Vec<&str> = hints_rels.iter().map(|s| s.as_str()).collect();
            self.ensure_labels(&vl_refs, &el_refs);

            // GIE path: Cypher → IR Plan → execute directly on CSR (zero MemoryGraph copy).
            // NOW supports RLS via SecurityFilter predicate pushdown (no MemoryGraph fallback).
            if let Ok(csr) = self.hot.lock() {
                if let Some(single_csr) = csr.as_single() {
                    if let Ok(ast) = yata_cypher::parse(cypher) {
                        // Build SecurityScope from RLS params (if authenticated).
                        // GIE SecurityFilter uses property-based governance (sensitivity_ord, owner_hash).
                        // If _rls_clearance param is present → new governance model → use GIE SecurityFilter.
                        // Otherwise → old org_id model → fall through to MemoryGraph path.
                        let has_clearance = params.iter().any(|(k, _)| k == "_rls_clearance");
                        let _use_gie_security = rls_org_id.is_none() || has_clearance;

                        let plan_result = if rls_org_id.is_some() && has_clearance {
                            let rls = build_rls_scope_from_params(rls_org_id.unwrap(), params);
                            let security_scope = build_gie_security_scope(&rls);
                            yata_gie::transpile::transpile_secured(&ast, security_scope)
                        } else if rls_org_id.is_none() {
                            yata_gie::transpile::transpile(&ast)
                        } else {
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
                        // GIE transpile failed (mutation or unsupported) → fall through to MemoryGraph
                    }
                }

                // Fallback: unsupported Cypher or partitioned mode → MemoryGraph path
                let mut g =
                    QueryableGraph(csr.to_filtered_memory_graph(&hints_labels, &hints_rels));

                if let Some(org_id) = rls_org_id {
                    let scope = build_rls_scope_from_params(org_id, params);
                    rls::apply_rls_filter_scoped(&mut g.0, &scope);
                }

                let rows = g.query(cypher, params).map_err(|e| e.to_string())?;

                let k = cache_key(cypher, params, rls_org_id);
                if let Ok(mut c) = self.cache.lock() {
                    c.put(k, rows.clone());
                }

                return Ok(rows);
            }
        }

        // Mutation path: load only labels referenced in Cypher (targeted).
        // Falls back to load-all only when AST extraction finds no labels.
        if let Some((vlabels, elabels)) = router::extract_mutation_hints(cypher) {
            let vl_refs: Vec<&str> = vlabels.iter().map(|s| s.as_str()).collect();
            let el_refs: Vec<&str> = elabels.iter().map(|s| s.as_str()).collect();
            self.ensure_labels(&vl_refs, &el_refs);
        } else {
            self.ensure_labels(&[], &[]);
        }
        let rls_owned = rls_org_id.map(String::from);

        self.block_on(async {
            self.ensure_hot();
            let mut g = if let Ok(csr) = self.hot.lock() {
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

                if let Ok(mut hot) = self.hot.lock() {
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
                if let Ok(mut hot) = self.hot.lock() {
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
    pub fn merge_record(
        &self,
        label: &str,
        pk_key: &str,
        pk_value: &str,
        props: &[(&str, yata_grin::PropValue)],
    ) -> Result<u32, String> {
        // Ensure label is loaded from R2 (page-in)
        self.ensure_labels(&[label], &[]);

        if let Ok(mut hot) = self.hot.lock() {
            // Arrow store path (Vineyard-native, no CSR conversion)
            if let Some(arrow) = hot.as_arrow_mut() {
                let vid = arrow.merge_by_pk(label, pk_key, pk_value, props);
                arrow.commit();
                if let Ok(mut ll) = self.loaded_labels.lock() {
                    ll.insert(label.to_string());
                }
                return Ok(vid);
            }
            // Legacy CSR path
            if let Some(single) = hot.as_single_mut() {
                let pk = yata_grin::PropValue::Str(pk_value.to_string());
                let vid = single.merge_by_pk(label, pk_key, &pk, props);
                single.commit();
                if let Ok(mut ll) = self.loaded_labels.lock() {
                    ll.insert(label.to_string());
                }
                return Ok(vid);
            }
        }
        Err("failed to acquire hot store lock".into())
    }

    /// CSR-direct DELETE by primary key: O(1) lookup.
    pub fn delete_record(
        &self,
        label: &str,
        pk_key: &str,
        pk_value: &str,
    ) -> Result<bool, String> {
        self.ensure_labels(&[label], &[]);

        if let Ok(mut hot) = self.hot.lock() {
            if let Some(arrow) = hot.as_arrow_mut() {
                let deleted = arrow.delete_by_pk(label, pk_key, pk_value);
                return Ok(deleted);
            }
            if let Some(single) = hot.as_single_mut() {
                let pk = yata_grin::PropValue::Str(pk_value.to_string());
                let deleted = single.delete_by_pk(label, pk_key, &pk);
                if deleted { single.commit(); }
                return Ok(deleted);
            }
        }
        Err("failed to acquire hot store lock".into())
    }

    /// Trigger R2 snapshot: CSR → ArrowFragment → BlobStore → R2 PUT.
    ///
    /// Uses yata-vineyard ArrowFragment format with NbrUnit zero-copy CSR.
    /// ~2000x faster serialize, ~50% smaller blobs vs legacy SnapshotBundle.
    pub fn trigger_snapshot(&self) -> Result<(u64, u64), String> {
        let s3 = self.get_s3_client()
            .ok_or_else(|| "S3 client not configured".to_string())?;
        let prefix = &self.s3_prefix;

        // Serialize CSR → ArrowFragment → MemoryBlobStore
        // Each partition Container snapshots independently (prefix = yata/partitions/{N}/)
        let (v_count, e_count, blob_store, meta) = if let Ok(csr) = self.hot.lock() {
            let store = csr.as_single()
                .ok_or_else(|| "no CSR store available".to_string())?;
            let pid = store.partition_id_raw().get();
            let frag = yata_vineyard::convert::csr_to_fragment(store, pid);
            let vc = frag.ivnums.iter().sum::<u64>();
            let ec = frag.edge_num();
            let bs = yata_vineyard::blob::MemoryBlobStore::new();
            let m = frag.serialize(&bs);
            (vc, ec, bs, m)
        } else {
            return Err("failed to acquire CSR lock".into());
        };

        // Guard: never overwrite a non-empty R2 snapshot with an empty one.
        if v_count == 0 && e_count == 0 {
            tracing::info!("snapshot skipped: CSR is empty (v=0, e=0), preserving existing R2 snapshot");
            return Ok((0, 0));
        }

        // Upload ArrowFragment blobs to R2
        let mut uploaded = 0u32;
        for (name, blob_id) in &meta.blobs {
            if let Some(data) = BlobStore::get(&blob_store, blob_id) {
                let full_key = format!("{prefix}snap/fragment/{name}");
                if let Err(e) = s3.put_sync(&full_key, data) {
                    tracing::error!(name, error = %e, "R2 ArrowFragment blob upload failed");
                } else {
                    uploaded += 1;
                }
            }
        }

        // Upload fragment meta (ObjectMeta JSON)
        let meta_json = serde_json::to_vec(&meta).unwrap_or_default();
        let meta_key = format!("{prefix}snap/fragment/meta.json");
        if let Err(e) = s3.put_sync(&meta_key, bytes::Bytes::from(meta_json)) {
            tracing::error!(error = %e, "R2 fragment meta upload failed");
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

        tracing::info!(vertices = v_count, edges = e_count, blobs = uploaded, "R2 ArrowFragment snapshot triggered");
        Ok((v_count, e_count))
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
        let csr = e.hot.lock().unwrap();
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
            let csr = e.hot.lock().unwrap();
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
            let csr = e.hot.lock().unwrap();
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
        assert_eq!(e.hot.lock().unwrap().vertex_count(), 2);

        run_query(&e, "MATCH (n:D {k: 'd1'}) DELETE n", &[], None).unwrap();
        assert_eq!(
            e.hot.lock().unwrap().vertex_count(),
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

        let csr = e.hot.lock().unwrap();
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
}
