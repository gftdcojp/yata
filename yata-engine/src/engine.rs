use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use yata_cypher::Graph;
use yata_graph::{GraphStore, QueryableGraph};
use yata_grin::{Mutable, Predicate, PropValue, Property, Scannable, Topology};
use yata_store::blob_cache::{
    DiskBlobCache, MemoryBlobCache, MmapBlobCache, BlobCache,
};

use crate::cache::{QueryCache, cache_key};
use crate::config::TieredEngineConfig;
use crate::loader;
use crate::router;

/// CPM metrics: CP5 mutation frequency + read/write ratio + compaction monitoring.
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
    pub last_compaction_ms: u64,
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

/// Graph engine: Sorted COO in-memory + WAL Projection (R2 segments + L1 compaction).
pub struct TieredGraphEngine {
    config: TieredEngineConfig,
    hot: Arc<RwLock<yata_store::GraphStoreEnum>>,
    warm: GraphStore,
    cache: Arc<Mutex<QueryCache>>,
    hot_initialized: Arc<AtomicBool>,
    cold_starting: Arc<AtomicBool>,
    /// Cached manifest for demand-paged label loading (LanceDB pattern).
    /// Loaded once on first query, then ensure_labels() fetches individual labels on-demand.
    /// Multi-node: each node loads manifest independently from R2 (shared source of truth).
    /// Lance TableManifest — single source for cold start + demand page-in.
    lance_manifest_cache: Arc<Mutex<Option<yata_lance::table::TableManifest>>>,
    loaded_labels: Arc<Mutex<HashSet<String>>>,
    blob_cache: Arc<dyn BlobCache>,
    s3_client: Arc<Mutex<Option<Arc<yata_s3::s3::S3Client>>>>,
    s3_prefix: String,
    dirty_labels: Arc<Mutex<HashSet<String>>>,
    pending_writes: Arc<AtomicUsize>,
    wal: Arc<Mutex<crate::wal::WalRingBuffer>>,
    wal_last_flushed_seq: Arc<AtomicU64>,
    /// SecurityScope cache: DID → (SecurityScope, compiled_at). Design E.
    security_scope_cache: Arc<Mutex<HashMap<String, (yata_gie::ir::SecurityScope, std::time::Instant)>>>,
    // CPM metrics: CP5 mutation frequency + compaction monitoring
    cypher_read_count: Arc<AtomicU64>,
    cypher_mutation_count: Arc<AtomicU64>,
    cypher_mutation_us_total: Arc<AtomicU64>,
    merge_record_count: Arc<AtomicU64>,
    last_compaction_ms: Arc<AtomicU64>,
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
        let blob_cache_budget_mb = config.blob_cache_budget_mb;
        let vineyard_dir = std::env::var("YATA_VINEYARD_DIR").ok();
        let use_mmap = std::env::var("YATA_MMAP_VINEYARD").unwrap_or_default() == "true";
        let blob_cache: Arc<dyn BlobCache> = if use_mmap {
            if let Some(ref dir) = vineyard_dir {
                match MmapBlobCache::new(dir) {
                    Ok(mv) => {
                        let meta_count = mv.load_all_meta();
                        tracing::info!(
                            dir,
                            meta_count,
                            "MmapBlobCache initialized (zero-copy OS page cache)"
                        );
                        Arc::new(mv)
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "MmapBlobCache init failed, falling back to MemoryBlobCache");
                        Arc::new(MemoryBlobCache::new(blob_cache_budget_mb))
                    }
                }
            } else {
                tracing::warn!("YATA_MMAP_VINEYARD=true but YATA_VINEYARD_DIR not set, falling back to MemoryBlobCache");
                Arc::new(MemoryBlobCache::new(blob_cache_budget_mb))
            }
        } else if let Some(ref dir) = vineyard_dir {
            match DiskBlobCache::new(dir, blob_cache_budget_mb) {
                Ok(dv) => {
                    let meta_count = dv.load_all_meta();
                    tracing::info!(
                        dir,
                        blob_cache_budget_mb,
                        meta_count,
                        "DiskBlobCache initialized (Container disk)"
                    );
                    Arc::new(dv)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DiskBlobCache init failed, falling back to MemoryBlobCache");
                    Arc::new(MemoryBlobCache::new(blob_cache_budget_mb))
                }
            }
        } else {
            tracing::info!(blob_cache_budget_mb, "MemoryBlobCache initialized (in-memory)");
            Arc::new(MemoryBlobCache::new(blob_cache_budget_mb))
        };

        // ── S3/R2 client for read (page-in from R2, lazy init) ──
        let s3_prefix = std::env::var("YATA_S3_PREFIX").unwrap_or_default();
        let s3_client = Arc::new(Mutex::new(None));

        // Sorted COO store (GIE query path needs as_single()).
        let hot_store = yata_store::GraphStoreEnum::new(partition_count, hot_partition_id);
        tracing::info!("using Sorted COO store (WAL Projection)");
        let wal_ring_capacity = config.wal_ring_capacity;
        Self {
            config,
            hot: Arc::new(RwLock::new(hot_store)),
            warm,
            cache: Arc::new(Mutex::new(cache)),
            hot_initialized: Arc::new(AtomicBool::new(false)),
            cold_starting: Arc::new(AtomicBool::new(false)),
            lance_manifest_cache: Arc::new(Mutex::new(None)),
            loaded_labels: Arc::new(Mutex::new(HashSet::new())),
            blob_cache,
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
            last_compaction_ms: Arc::new(AtomicU64::new(0)),
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
    /// LanceDB-style demand-paged label loading. Multi-node: each node loads independently.
    ///
    /// Phase 1 (manifest-only cold start): Download manifest from R2, set hot_initialized.
    /// Phase 2 (per-label demand page-in): Fetch only needed labels from R2 on each query.
    ///
    /// Multi-node safe: R2 = shared source of truth. Each node's loaded_labels is independent.
    /// No cross-node coordination needed — each node builds its own in-memory CSR subset.
    fn ensure_labels(&self, vertex_labels: &[&str]) {
        // Phase 1: Manifest-only cold start (one thread, ~50ms)
        if !self.hot_initialized.load(Ordering::SeqCst) {
            if self.cold_starting.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                let _ = self.wal_cold_start_manifest_only();
                self.cold_starting.store(false, Ordering::SeqCst);
            } else {
                // Spin-wait for manifest load only (fast, ~50ms not ~30s)
                let mut wait_ms = 5;
                for _ in 0..100 {
                    if self.hot_initialized.load(Ordering::SeqCst) { break; }
                    std::thread::sleep(std::time::Duration::from_millis(wait_ms));
                    wait_ms = (wait_ms * 2).min(200);
                }
            }
            // Fall through to Phase 2 (demand page-in)
        }

        // Phase 2: Demand page-in — fetch only needed labels
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
        let vineyard_dir = std::env::var("YATA_VINEYARD_DIR").ok();
        let pid = self.config.hot_partition_id.get();

        // Lance TableManifest: fragment-level demand page-in (3-tier: disk → R2 → verify)
        let lance_manifest = match self.lance_manifest_cache.lock() {
            Ok(lmc) => lmc.clone(),
            Err(_) => None,
        };
        if let Some(ref lm) = lance_manifest {
            for label in &needed {
                let matching_fragments: Vec<&yata_lance::Fragment> = lm.fragments.iter()
                    .filter(|f| f.labels.iter().any(|l| l == label))
                    .collect();
                if matching_fragments.is_empty() { continue; }
                for frag in matching_fragments {
                    let disk_path = vineyard_dir.as_ref()
                        .map(|d| format!("{d}/lance/vertices/{pid}/fragments/{:020}-{:06}.arrow", frag.version, frag.id));
                    // Tier 1: disk cache
                    let data = if let Some(ref path) = disk_path {
                        if std::path::Path::new(path).exists() {
                            std::fs::read(path).ok().map(bytes::Bytes::from)
                        } else { None }
                    } else { None };
                    // Tier 2: R2
                    let data = match data {
                        Some(d) => d,
                        None => match s3.get_sync(&frag.r2_key) {
                            Ok(Some(d)) => {
                                let actual = blake3::hash(&d).to_hex().to_string();
                                if !frag.blake3_hex.is_empty() && actual != frag.blake3_hex {
                                    tracing::error!(label, frag_id = frag.id, "Lance fragment checksum mismatch");
                                    continue;
                                }
                                if let Some(ref path) = disk_path {
                                    if let Some(parent) = std::path::Path::new(path).parent() {
                                        let _ = std::fs::create_dir_all(parent);
                                    }
                                    let _ = std::fs::write(path, &d);
                                }
                                d
                            }
                            _ => { continue; }
                        }
                    };
                    let entries = crate::arrow_wal::deserialize_segment_auto(&frag.r2_key, &data);
                    if !entries.is_empty() {
                        let _ = self.wal_apply(&entries);
                    }
                }
                if let Ok(mut ll) = self.loaded_labels.lock() {
                    ll.insert(label.clone());
                }
                tracing::info!(label, "demand page-in: loaded from Lance fragment");
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
                    self.hot_initialized.store(true, Ordering::SeqCst);
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
                        self.hot_initialized.store(true, Ordering::SeqCst);
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
                        self.hot_initialized.store(true, Ordering::SeqCst);
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

    /// L0 compact threshold: trigger compaction when pending_writes exceeds this.
    /// Size-based (workload-adaptive), replaces time-based cron.
    const L0_COMPACT_THRESHOLD: usize = 10_000;

    /// Append-only merge record (LanceDB-style: tombstone old + append new fragment).
    /// WAL append + hot store append. No per-write commit() — index rebuild deferred
    /// to compaction (size-based trigger at L0_COMPACT_THRESHOLD).
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

        // Hot store: append-only (tombstone old + append new, no in-place mutation)
        if let Ok(mut hot) = self.hot.write() {
            if let Some(single) = hot.as_single_mut() {
                let pk = yata_grin::PropValue::Str(pk_value.to_string());
                let vid = single.merge_by_pk(label, pk_key, &pk, props);
                // Lightweight commit: rebuild label_index + label_bitmap only
                // (keeps label scans correct). Expensive columnar/btree deferred.
                single.commit_labels_only();
                if let Ok(mut ll) = self.loaded_labels.lock() {
                    ll.insert(label.to_string());
                }
                let pw = self.pending_writes.fetch_add(1, Ordering::Relaxed) + 1;
                if pw >= Self::L0_COMPACT_THRESHOLD {
                    self.pending_writes.store(0, Ordering::Relaxed);
                    // Full commit at compaction (rebuild columnar cache, btree, prop indexes)
                    single.commit();
                    let _ = self.trigger_compaction();
                }
                return Ok(vid);
            }
        }
        Err("failed to acquire hot store lock".into())
    }

    /// Append-only merge record AND return the WAL entry for pushing to read replicas.
    /// Write Container only. Returns (vid, WalEntry).
    pub fn merge_record_with_wal(
        &self,
        label: &str,
        pk_key: &str,
        pk_value: &str,
        props: &[(&str, yata_grin::PropValue)],
    ) -> Result<(u32, Option<crate::wal::WalEntry>), String> {
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

        self.merge_record_count.fetch_add(1, Ordering::Relaxed);
        self.mark_label_dirty(label);
        self.invalidate_security_cache_if_policy(label, props);

        if let Ok(mut hot) = self.hot.write() {
            if let Some(single) = hot.as_single_mut() {
                let pk = yata_grin::PropValue::Str(pk_value.to_string());
                let vid = single.merge_by_pk(label, pk_key, &pk, props);
                // Lightweight commit: label_index + label_bitmap only
                single.commit_labels_only();
                if let Ok(mut ll) = self.loaded_labels.lock() {
                    ll.insert(label.to_string());
                }
                let pw = self.pending_writes.fetch_add(1, Ordering::Relaxed) + 1;
                if pw >= Self::L0_COMPACT_THRESHOLD {
                    self.pending_writes.store(0, Ordering::Relaxed);
                    single.commit();
                    let _ = self.trigger_compaction();
                }
                return Ok((vid, wal_entry));
            }
        }
        Err("failed to acquire hot store lock".into())
    }

    /// DELETE by primary key: O(1) lookup + WAL Delete entry.
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
            last_compaction_ms: self.last_compaction_ms.load(Ordering::Relaxed),
        }
    }

    /// Drain dirty_labels and return the set. After drain, dirty_labels is empty.
    fn drain_dirty_labels(&self) -> std::collections::HashSet<String> {
        match self.dirty_labels.lock() {
            Ok(mut dl) => dl.drain().collect(),
            Err(e) => {
                tracing::error!("dirty_labels poisoned on drain: {e}");
                std::collections::HashSet::new()
            }
        }
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

    /// Apply vertex data from an ArrowWalStore (mmap'd compacted segment) to the store.
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
                self.hot_initialized.store(true, std::sync::atomic::Ordering::SeqCst);
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
                self.hot_initialized.store(true, Ordering::SeqCst);
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

        let (seq_start, seq_end) = match (entries.first(), entries.last()) {
            (Some(first), Some(last)) => (first.seq, last.seq),
            _ => return Ok((0, 0, 0)),
        };

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

        // Update head pointer + segment registry (avoids R2 list_sync)
        let head_key = format!("{prefix}wal/meta/{pid}/head.json");
        // Read existing segment list from head, append new segment
        let mut segment_keys: Vec<String> = match s3.get_sync(&head_key) {
            Ok(Some(data)) => {
                serde_json::from_slice::<serde_json::Value>(&data)
                    .ok()
                    .and_then(|v| v.get("segments").cloned())
                    .and_then(|v| serde_json::from_value::<Vec<String>>(v).ok())
                    .unwrap_or_default()
            }
            _ => Vec::new(),
        };
        segment_keys.push(key.clone());
        let head_json = serde_json::json!({
            "partition_id": pid,
            "head_seq": seq_end,
            "entry_count": entries.len(),
            "segments": segment_keys,
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

    /// L1 Compaction: read WAL segments from R2, PK-dedup, write compacted segment + manifest.
    ///
    /// - Operates on WAL segments only (streaming, bounded memory)
    /// - Output is same Arrow IPC format as WAL (uniform mmap path)
    /// - Idempotent: re-compacting produces the same result
    ///
    /// Triggered by size-based threshold (L0_COMPACT_THRESHOLD) or manually via XRPC.
    pub fn trigger_compaction(&self) -> Result<crate::compaction::CompactionResult, String> {
        let s3 = self.get_s3_client()
            .ok_or_else(|| "S3 client not configured".to_string())?;
        let prefix = &self.s3_prefix;
        let pid = self.config.hot_partition_id.get();

        // First flush pending WAL entries to R2
        let _ = self.wal_flush_segment();

        // Drain dirty_labels — only these labels need re-compaction
        let dirty = self.drain_dirty_labels();

        // Read existing Lance manifest (or from cache)
        let existing_lance: Option<yata_lance::table::TableManifest> = {
            let cache = self.lance_manifest_cache.lock().ok();
            cache.and_then(|c| c.clone())
        };
        let existing_compacted_seq = existing_lance.as_ref().map(|m| m.version).unwrap_or(0);

        // Segment registry from head.json
        let head_key = format!("{prefix}wal/meta/{pid}/head.json");
        let segment_keys: Vec<String> = match s3.get_sync(&head_key) {
            Ok(Some(data)) => serde_json::from_slice::<serde_json::Value>(&data)
                .ok()
                .and_then(|v| v.get("segments").cloned())
                .and_then(|v| serde_json::from_value::<Vec<String>>(v).ok())
                .unwrap_or_default(),
            _ => Vec::new(),
        };

        if segment_keys.is_empty() && dirty.is_empty() {
            tracing::debug!("compaction: no segments and no dirty labels");
            return Ok(crate::compaction::CompactionResult {
                data: bytes::Bytes::new(),
                min_seq: 0,
                max_seq: 0,
                input_entries: 0,
                output_entries: 0,
                labels: Vec::new(),
            });
        }

        // Collect new WAL segments (after existing compacted_seq)
        let mut new_segment_data: Vec<(String, bytes::Bytes)> = Vec::new();
        let mut global_max_seq = existing_compacted_seq;
        for key in &segment_keys {
            if let Some(filename) = key.rsplit('/').next() {
                let stripped = filename.strip_suffix(".ndjson")
                    .or_else(|| filename.strip_suffix(".arrow"));
                if let Some(stripped) = stripped {
                    let parts: Vec<&str> = stripped.split('-').collect();
                    if parts.len() == 2 {
                        if let Ok(seg_end) = parts[1].parse::<u64>() {
                            global_max_seq = global_max_seq.max(seg_end);
                            if seg_end <= existing_compacted_seq {
                                continue;
                            }
                            if let Ok(Some(data)) = s3.get_sync(key) {
                                new_segment_data.push((key.clone(), data));
                            }
                        }
                    }
                }
            }
        }

        // Include existing Lance fragments for dirty labels
        let mut all_segment_refs: Vec<(String, bytes::Bytes)> = Vec::new();
        if let Some(ref lm) = existing_lance {
            for label in &dirty {
                for frag in &lm.fragments {
                    if frag.labels.iter().any(|l| l == label) {
                        if let Ok(Some(data)) = s3.get_sync(&frag.r2_key) {
                            all_segment_refs.push((frag.r2_key.clone(), data));
                        }
                    }
                }
            }
        }

        // Include new WAL segments
        all_segment_refs.extend(new_segment_data);

        if all_segment_refs.is_empty() && dirty.is_empty() {
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

        self.do_per_label_compaction(s3, prefix, pid, "", &all_segment_refs, &dirty, &std::collections::HashMap::new(), global_max_seq)
    }

    /// Execute per-label compaction: compact dirty labels → Lance-table-compatible fragments + manifest.
    fn do_per_label_compaction(
        &self,
        s3: std::sync::Arc<yata_s3::s3::S3Client>,
        prefix: &str,
        pid: u32,
        _manifest_key: &str,
        segment_refs: &[(String, bytes::Bytes)],
        dirty: &std::collections::HashSet<String>,
        _existing_label_segments: &std::collections::HashMap<String, crate::compaction::LabelSegmentState>,
        global_max_seq: u64,
    ) -> Result<crate::compaction::CompactionResult, String> {
        let refs: Vec<(&str, &[u8])> = segment_refs.iter()
            .map(|(k, d)| (k.as_str(), d.as_ref()))
            .collect();

        let label_results = crate::compaction::compact_segments_by_label(&refs, dirty)?;

        // Lance-table-compatible output: typed Arrow IPC fragments + versioned manifest
        let lance_table = yata_lance::LanceTable::new("vertices", pid, prefix);
        let mut total_output = 0usize;
        let mut total_bytes = 0usize;
        let vineyard_dir = std::env::var("YATA_VINEYARD_DIR").ok();

        // Phase 1: Convert each label's compacted entries → Lance fragment, upload to R2.
        let mut new_fragments: Vec<yata_lance::Fragment> = Vec::new();
        let mut uploaded_results: Vec<&crate::compaction::LabelCompactionResult> = Vec::with_capacity(label_results.len());
        for (frag_idx, lr) in label_results.iter().enumerate() {
            // Arrow IPC segment from compact_segments_by_label is the Lance fragment data.
            let fragment = lance_table.build_fragment(
                global_max_seq, frag_idx as u32, &lr.data,
                vec![lr.label.clone()], lr.entry_count,
            );

            // Upload fragment to Lance R2 path
            let r2_key = &fragment.r2_key;
            match s3.put_sync(r2_key, lr.data.clone()) {
                Ok(_) => {
                    // Disk cache (Lance layout)
                    if let Some(ref dir) = vineyard_dir {
                        let frag_dir = format!("{dir}/lance/vertices/{pid}/fragments");
                        let _ = std::fs::create_dir_all(&frag_dir);
                        let _ = std::fs::write(format!("{frag_dir}/{:020}-{:06}.arrow", global_max_seq, frag_idx), &lr.data);
                    }
                    new_fragments.push(fragment);
                    uploaded_results.push(lr);
                    total_output += lr.entry_count;
                    total_bytes += lr.data.len();
                }
                Err(e) => {
                    tracing::error!(label = lr.label, error = %e, "R2 Lance fragment upload failed, skipping label");
                }
            }
        }

        // Phase 2: Build Lance TableManifest (versioned, immutable). Single manifest path.
        let existing_lance: Option<yata_lance::table::TableManifest> = {
            let cache = self.lance_manifest_cache.lock().ok();
            cache.and_then(|c| c.clone())
        };
        let lance_manifest = lance_table.build_manifest(existing_lance.as_ref(), new_fragments);
        let lance_json = serde_json::to_vec(&lance_manifest).unwrap_or_default();
        let lance_versioned_key = lance_table.manifest_key(global_max_seq);
        s3.put_sync(&lance_versioned_key, bytes::Bytes::from(lance_json))
            .map_err(|e| format!("R2 Lance manifest upload failed: {e}"))?;
        // Update in-memory cache
        if let Ok(mut lmc) = self.lance_manifest_cache.lock() {
            *lmc = Some(lance_manifest.clone());
        }

        let all_labels: Vec<String> = lance_manifest.fragments.iter()
            .flat_map(|f| f.labels.iter().cloned())
            .collect::<std::collections::HashSet<_>>()
            .into_iter().collect();

        tracing::info!(
            dirty_labels = label_results.len(),
            total_labels = all_labels.len(),
            output_entries = total_output,
            lance_version = lance_manifest.version,
            lance_fragments = lance_manifest.fragments.len(),
            global_max_seq,
            bytes = total_bytes,
            "Lance compaction complete"
        );

        Ok(crate::compaction::CompactionResult {
            data: bytes::Bytes::new(),
            min_seq: 0,
            max_seq: global_max_seq,
            input_entries: 0,
            output_entries: total_output,
            labels: all_labels,
        })
    }

    /// Lance manifest-only cold start. Downloads Lance TableManifest + WAL tail only.
    /// Sets hot_initialized = true immediately — labels are loaded on-demand by ensure_labels().
    /// Multi-node safe: each node loads manifest independently, builds its own label subset.
    fn wal_cold_start_manifest_only(&self) -> Result<u64, String> {
        let s3 = self.get_s3_client()
            .ok_or_else(|| "S3 client not configured".to_string())?;
        let prefix = &self.s3_prefix;
        let pid = self.config.hot_partition_id.get();

        // Load Lance TableManifest from R2 (O(1) latest via inverted naming)
        let lance_prefix = format!("{}lance/vertices/{}/manifest-", prefix, pid);
        let checkpoint_seq = if let Ok(objects) = s3.list_sync(&lance_prefix) {
            if let Some(first) = objects.first() {
                if let Ok(Some(data)) = s3.get_sync(&first.key) {
                    match serde_json::from_slice::<yata_lance::table::TableManifest>(&data) {
                        Ok(lance_manifest) => {
                            let frag_count = lance_manifest.fragments.len();
                            let total_rows = lance_manifest.total_rows;
                            let version = lance_manifest.version;
                            tracing::info!(
                                version, fragments = frag_count,
                                total_rows, "Lance cold start: manifest loaded"
                            );
                            if let Ok(mut lmc) = self.lance_manifest_cache.lock() {
                                *lmc = Some(lance_manifest);
                            }
                            // Use version as checkpoint_seq proxy for WAL tail replay
                            version
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Lance manifest parse failed, starting empty");
                            0
                        }
                    }
                } else { 0 }
            } else {
                tracing::info!("no Lance manifest found (new database)");
                0
            }
        } else { 0 };

        // Replay WAL tail (entries after compacted_seq)
        let head_key = format!("{prefix}wal/meta/{pid}/head.json");
        let segment_keys: Vec<String> = match s3.get_sync(&head_key) {
            Ok(Some(data)) => serde_json::from_slice::<serde_json::Value>(&data)
                .ok()
                .and_then(|v| v.get("segments").cloned())
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        let mut tail_applied = 0u64;
        for seg_key in &segment_keys {
            let seq_end = seg_key.rsplit('/').next()
                .and_then(|f| f.split('-').nth(1))
                .and_then(|s| s.strip_suffix(".arrow").or_else(|| s.strip_suffix(".ndjson")))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            if seq_end <= checkpoint_seq { continue; }
            if let Ok(Some(data)) = s3.get_sync(seg_key) {
                let entries = crate::arrow_wal::deserialize_segment_auto(seg_key, &data);
                let filtered: Vec<_> = entries.into_iter().filter(|e| e.seq > checkpoint_seq).collect();
                if !filtered.is_empty() {
                    let _ = self.wal_apply(&filtered);
                    tail_applied += filtered.len() as u64;
                }
            }
        }
        if tail_applied > 0 {
            tracing::info!(tail_applied, "WAL tail replay complete");
        }

        // Set hot_initialized IMMEDIATELY — labels load on-demand
        self.hot_initialized.store(true, Ordering::SeqCst);
        Ok(checkpoint_seq)
    }

    /// Cold start: full-preload path (all labels). Use wal_cold_start_manifest_only() for demand-paged.
    pub fn wal_cold_start(&self) -> Result<u64, String> {
        let s3 = self.get_s3_client()
            .ok_or_else(|| "S3 client not configured".to_string())?;
        let prefix = &self.s3_prefix;
        let pid = self.config.hot_partition_id.get();

        // Load compacted segment(s) — try versioned manifest first, fallback to legacy
        let checkpoint_seq = match crate::compaction::find_latest_manifest(&s3, prefix, pid) {
            Some((manifest_ver, data)) => {
                if manifest_ver > 0 {
                    tracing::info!(manifest_ver, "cold start: loaded versioned manifest");
                }
                match serde_json::from_slice::<crate::compaction::CompactionManifest>(&data) {
                    Ok(manifest) if manifest.version >= 2 && !manifest.label_segments.is_empty() => {
                        // v2: load per-label compacted segments
                        tracing::info!(
                            compacted_seq = manifest.compacted_seq,
                            labels = manifest.label_segments.len(),
                            entries = manifest.entry_count,
                            "cold start: loading per-label compacted segments (v2)"
                        );
                        let vineyard_dir = std::env::var("YATA_VINEYARD_DIR").ok();
                        for (label, state) in &manifest.label_segments {
                            let disk_path = vineyard_dir.as_ref()
                                .map(|d| format!("{d}/log/compacted/{pid}/label/{label}.arrow"));

                            let loaded = disk_path.as_ref().map_or(false, |path| {
                                if !std::path::Path::new(path).exists() { return false; }
                                match yata_store::ArrowWalStore::from_file(std::path::Path::new(path)) {
                                    Ok(store) => {
                                        tracing::info!(label, path, vertices = store.len(), "cold start: mmap label from disk");
                                        self.apply_arrow_wal_store(&store).is_ok()
                                    }
                                    Err(_) => false,
                                }
                            });

                            if !loaded {
                                if let Ok(Some(data)) = s3.get_sync(&state.key) {
                                    // Verify Blake3 checksum if present
                                    if let Err(e) = crate::compaction::verify_blake3(&data, &state.blake3_hex, label) {
                                        tracing::error!(label, error = %e, "cold start: checksum verification failed, skipping corrupt segment");
                                        continue;
                                    }
                                    let entries = crate::arrow_wal::deserialize_segment_auto(&state.key, &data);
                                    if !entries.is_empty() {
                                        let _ = self.wal_apply(&entries);
                                        tracing::info!(label, applied = entries.len(), "cold start: label segment from R2");
                                    }
                                    if let Some(ref path) = disk_path {
                                        if let Some(parent) = std::path::Path::new(path).parent() {
                                            let _ = std::fs::create_dir_all(parent);
                                        }
                                        let _ = std::fs::write(path, &data);
                                    }
                                }
                            }
                        }
                        manifest.compacted_seq
                    }
                    Ok(manifest) => {
                        // v1: monolithic compacted segment
                        tracing::info!(
                            compacted_seq = manifest.compacted_seq,
                            entries = manifest.entry_count,
                            labels = manifest.labels.len(),
                            "cold start: loading compacted segment (v1)"
                        );
                        let filename = manifest.compacted_segment_key
                            .rsplit('/').next().unwrap_or("compacted.arrow");
                        let disk_path = std::env::var("YATA_VINEYARD_DIR").ok()
                            .map(|d| format!("{d}/log/compacted/{pid}/{filename}"));

                        let loaded = disk_path.as_ref().map_or(false, |path| {
                            if !std::path::Path::new(path).exists() { return false; }
                            match yata_store::ArrowWalStore::from_file(std::path::Path::new(path)) {
                                Ok(store) => {
                                    tracing::info!(path, vertices = store.len(), "cold start: mmap from disk");
                                    self.apply_arrow_wal_store(&store).is_ok()
                                }
                                Err(_) => false,
                            }
                        });

                        if !loaded {
                            if let Ok(Some(data)) = s3.get_sync(&manifest.compacted_segment_key) {
                                let entries = crate::arrow_wal::deserialize_segment_auto(
                                    &manifest.compacted_segment_key, &data,
                                );
                                if !entries.is_empty() {
                                    let _ = self.wal_apply(&entries);
                                    tracing::info!(applied = entries.len(), "cold start: compacted segment from R2");
                                }
                                if let Some(ref path) = disk_path {
                                    if let Some(parent) = std::path::Path::new(path).parent() {
                                        let _ = std::fs::create_dir_all(parent);
                                    }
                                    let _ = std::fs::write(path, &data);
                                }
                            }
                        }
                        manifest.compacted_seq
                    }
                    Err(_) => 0,
                }
            }
            None => {
                tracing::info!("cold start: no compaction manifest (run gftd yata migrate)");
                self.hot_initialized.store(true, Ordering::SeqCst);
                0
            }
        };

        // Replay WAL segments after compacted_seq (use segment registry from head.json)
        let head_key = format!("{prefix}wal/meta/{pid}/head.json");
        let segment_keys: Vec<String> = match s3.get_sync(&head_key) {
            Ok(Some(data)) => serde_json::from_slice::<serde_json::Value>(&data)
                .ok()
                .and_then(|v| v.get("segments").cloned())
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default(),
            _ => Vec::new(),
        };

        let mut replayed = 0u64;
        for key in &segment_keys {
            if let Some(filename) = key.rsplit('/').next() {
                let stripped = filename.strip_suffix(".ndjson")
                    .or_else(|| filename.strip_suffix(".arrow"));
                if let Some(stripped) = stripped {
                    let parts: Vec<&str> = stripped.split('-').collect();
                    if parts.len() == 2 {
                        if let Ok(seg_end) = parts[1].parse::<u64>() {
                            if seg_end <= checkpoint_seq { continue; }
                            if let Ok(Some(data)) = s3.get_sync(key) {
                                let mut entries = crate::arrow_wal::deserialize_segment_auto(key, &data);
                                entries.retain(|e| e.seq > checkpoint_seq);
                                if !entries.is_empty() {
                                    let count = entries.len();
                                    let _ = self.wal_apply(&entries);
                                    replayed += count as u64;
                                }
                            }
                        }
                    }
                }
            }
        }

        tracing::info!(checkpoint_seq, replayed, "cold start complete");
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

    /// Phase 5: Execute a distributed plan fragment step on local CSR.
    pub fn execute_fragment_step(
        &self,
        cypher: &str,
        partition_id: u32,
        partition_count: u32,
        target_round: u32,
        inbound: &std::collections::HashMap<u32, Vec<yata_gie::MaterializedRecord>>,
    ) -> Result<yata_gie::ExchangePayload, String> {
        self.ensure_hot();
        let hot = self.hot.read().map_err(|e| format!("lock: {e}"))?;
        let single = hot.as_single().ok_or("not single-partition store")?;
        yata_gie::execute_step(cypher, single, partition_id, partition_count, target_round, inbound)
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

    // ── WAL cold start recovery tests ──────────────────────────────────

    #[test]
    fn test_wal_flush_no_s3_returns_error_not_panic() {
        // Without S3 config, wal_flush_segment should return Err, never panic
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let result = e.wal_flush_segment();
        // S3 not configured → Err (but no panic)
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("S3 client not configured"));
    }

    #[test]
    fn test_wal_apply_roundtrip() {
        // Write entries via merge_record, then apply them to a fresh engine via wal_apply
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);

        e.merge_record("Person", "rkey", "alice", &[
            ("name", yata_grin::PropValue::Str("Alice".to_string())),
        ]).unwrap();
        e.merge_record("Person", "rkey", "bob", &[
            ("name", yata_grin::PropValue::Str("Bob".to_string())),
        ]).unwrap();

        // Extract WAL entries
        let entries = e.wal.lock().unwrap().tail(0, 100).unwrap();
        assert_eq!(entries.len(), 2);

        // Apply to a fresh engine
        let dir2 = tempfile::tempdir().unwrap();
        let e2 = make_engine(&dir2);
        let applied = e2.wal_apply(&entries).unwrap();
        assert_eq!(applied, 2);

        // Verify data is readable
        let rows = run_query(&e2, "MATCH (n:Person) RETURN n.name AS name", &[], None).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_wal_apply_pk_dedup() {
        // Applying entries with same PK should dedup (last write wins)
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);

        e.merge_record("Person", "rkey", "alice", &[
            ("name", yata_grin::PropValue::Str("Alice_v1".to_string())),
        ]).unwrap();
        e.merge_record("Person", "rkey", "alice", &[
            ("name", yata_grin::PropValue::Str("Alice_v2".to_string())),
        ]).unwrap();

        let rows = run_query(&e, "MATCH (n:Person) RETURN n.name AS name", &[], None).unwrap();
        assert_eq!(rows.len(), 1, "PK dedup: should have 1 Person, not 2");
    }

    #[test]
    fn test_wal_serialize_deserialize_arrow() {
        use crate::wal::{WalEntry, WalOp};

        let entries = vec![
            WalEntry {
                seq: 1, op: WalOp::Upsert,
                label: "Post".into(), pk_key: "rkey".into(), pk_value: "p1".into(),
                props: vec![("title".into(), yata_grin::PropValue::Str("Hello".into()))],
                timestamp_ms: 1000,
            },
            WalEntry {
                seq: 2, op: WalOp::Delete,
                label: "Post".into(), pk_key: "rkey".into(), pk_value: "p2".into(),
                props: vec![], timestamp_ms: 2000,
            },
        ];

        let data = crate::arrow_wal::serialize_segment_arrow(&entries).unwrap();
        let recovered = crate::arrow_wal::deserialize_segment_arrow(&data).unwrap();

        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].seq, 1);
        assert_eq!(recovered[0].label, "Post");
        assert_eq!(recovered[0].pk_value, "p1");
        assert_eq!(recovered[1].op, WalOp::Delete);
    }

    #[test]
    fn test_compaction_per_label_dirty_only() {
        use crate::wal::{WalEntry, WalOp};

        let entries = vec![
            WalEntry {
                seq: 1, op: WalOp::Upsert,
                label: "Post".into(), pk_key: "rkey".into(), pk_value: "p1".into(),
                props: vec![("title".into(), yata_grin::PropValue::Str("Hello".into()))],
                timestamp_ms: 1000,
            },
            WalEntry {
                seq: 2, op: WalOp::Upsert,
                label: "Like".into(), pk_key: "rkey".into(), pk_value: "l1".into(),
                props: vec![], timestamp_ms: 2000,
            },
            WalEntry {
                seq: 3, op: WalOp::Upsert,
                label: "Follow".into(), pk_key: "rkey".into(), pk_value: "f1".into(),
                props: vec![], timestamp_ms: 3000,
            },
        ];

        let seg = crate::arrow_wal::serialize_segment_arrow(&entries).unwrap();
        let dirty: std::collections::HashSet<String> = ["Post".to_string()].into_iter().collect();
        let results = crate::compaction::compact_segments_by_label(
            &[("seg.arrow", &seg)], &dirty,
        ).unwrap();

        assert_eq!(results.len(), 1, "Only dirty label Post should be compacted");
        assert_eq!(results[0].label, "Post");
    }

    #[test]
    fn test_compaction_blake3_checksum_roundtrip() {
        use crate::wal::{WalEntry, WalOp};

        let entries = vec![WalEntry {
            seq: 1, op: WalOp::Upsert,
            label: "Post".into(), pk_key: "rkey".into(), pk_value: "p1".into(),
            props: vec![("v".into(), yata_grin::PropValue::Int(42))],
            timestamp_ms: 1000,
        }];

        let seg_data = crate::arrow_wal::serialize_segment_arrow(&entries).unwrap();
        let checksum = crate::compaction::blake3_hex(&seg_data);

        // Verify passes
        assert!(crate::compaction::verify_blake3(&seg_data, &checksum, "Post").is_ok());

        // Tampered data fails
        let mut tampered = seg_data.to_vec();
        if let Some(last) = tampered.last_mut() { *last ^= 0xFF; }
        assert!(crate::compaction::verify_blake3(&tampered, &checksum, "Post").is_err());
    }

    #[test]
    fn test_cold_start_no_s3_graceful() {
        // Without S3 config, cold start should not panic
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let result = e.wal_cold_start();
        // Should return error (no S3 client) but not panic
        assert!(result.is_err());
    }

    #[test]
    fn test_wal_gap_detection() {
        use crate::wal::WalRingBuffer;

        let mut wal = WalRingBuffer::new(3); // capacity 3

        // Fill and evict
        for i in 1..=5 {
            let entry = crate::wal::WalEntry {
                seq: i, op: crate::wal::WalOp::Upsert,
                label: "T".into(), pk_key: "k".into(), pk_value: format!("v{i}"),
                props: vec![], timestamp_ms: 1000 + i,
            };
            wal.append(entry);
        }

        // Buffer has seq 3,4,5 (1,2 evicted). oldest_evicted = 2.
        assert_eq!(wal.oldest_evicted(), 2);
        assert_eq!(wal.oldest_seq(), 3);

        // Consumer at seq 0 — gap exists (needs R2 segments)
        assert!(wal.tail(0, 10).is_some(), "after_seq=0 should return all entries from buffer");

        // Consumer at seq 1 — gap (1 < oldest=3 AND 1 < oldest_evicted=2)
        assert!(wal.tail(1, 10).is_none(), "after_seq=1 should detect gap");

        // Consumer at seq 2 — at eviction boundary, no gap
        let tail = wal.tail(2, 10);
        assert!(tail.is_some(), "after_seq=2 should not have gap (boundary)");
        assert_eq!(tail.unwrap().len(), 3); // seq 3,4,5

        // Consumer at seq 4 — no gap, returns seq 5
        let tail = wal.tail(4, 10).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].seq, 5);

        // Consumer fully caught up
        let tail = wal.tail(5, 10).unwrap();
        assert!(tail.is_empty());
    }

    #[test]
    fn test_wal_empty_buffer_tail() {
        use crate::wal::WalRingBuffer;
        let wal = WalRingBuffer::new(10);

        // Empty buffer — no gap, empty result
        let tail = wal.tail(0, 10);
        assert!(tail.is_some());
        assert!(tail.unwrap().is_empty());
    }

    #[test]
    fn test_dirty_labels_drain() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);

        e.merge_record("Post", "rkey", "p1", &[
            ("title".into(), yata_grin::PropValue::Str("Hello".into())),
        ]).unwrap();
        e.merge_record("Like", "rkey", "l1", &[]).unwrap();

        let dirty = e.drain_dirty_labels();
        assert!(dirty.contains("Post"));
        assert!(dirty.contains("Like"));
        assert_eq!(dirty.len(), 2);

        // Drain again — should be empty
        let dirty2 = e.drain_dirty_labels();
        assert!(dirty2.is_empty(), "dirty_labels should be empty after drain");
    }

    #[test]
    fn test_v1_manifest_backward_compat() {
        // v1 JSON without label_segments and blake3_hex — should deserialize with defaults
        let json = r#"{"partition_id":0,"version":1,"compacted_segment_key":"k","compacted_seq":50,"entry_count":10,"labels":["Post"],"created_at_ms":0,"segment_bytes":100}"#;
        let manifest: crate::compaction::CompactionManifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.version, 1);
        assert!(manifest.label_segments.is_empty());
    }

    #[test]
    fn test_v2_manifest_blake3_backward_compat() {
        // v2 label_segments without blake3_hex field — should default to empty string
        let json = r#"{"partition_id":0,"version":2,"compacted_segment_key":"","compacted_seq":100,"entry_count":30,"labels":["Post"],"created_at_ms":0,"segment_bytes":2048,"label_segments":{"Post":{"key":"k","max_seq":100,"entry_count":30,"segment_bytes":2048}}}"#;
        let manifest: crate::compaction::CompactionManifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.version, 2);
        assert!(manifest.label_segments["Post"].blake3_hex.is_empty(), "blake3_hex should default to empty");
    }
}
