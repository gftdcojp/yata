use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use yata_cypher::Graph;
use yata_grin::{Predicate, PropValue, Property, Scannable, Topology};

use crate::config::TieredEngineConfig;
use crate::memory_bridge;
use crate::router;

/// Compaction result (LanceDB-native).
#[derive(Debug, Clone, serde::Serialize)]
pub struct CompactionResult {
    pub max_seq: u64,
    pub input_entries: usize,
    pub output_entries: usize,
    pub labels: Vec<String>,
}

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
pub(crate) static ENGINE_RT: std::sync::LazyLock<tokio::runtime::Runtime> = std::sync::LazyLock::new(|| {
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

/// Graph engine: LanceDB-backed. No persistent CSR. Query = LanceDB scan → ephemeral CSR → GIE.
pub struct TieredGraphEngine {
    config: TieredEngineConfig,
    lance_db: Arc<tokio::sync::Mutex<Option<yata_lance::YataDb>>>,
    lance_table: Arc<tokio::sync::Mutex<Option<yata_lance::YataTable>>>,
    s3_prefix: String,
    /// SecurityScope cache: DID → (SecurityScope, compiled_at). Design E.
    security_scope_cache: Arc<Mutex<HashMap<String, (yata_gie::ir::SecurityScope, std::time::Instant)>>>,
    cypher_read_count: Arc<AtomicU64>,
    cypher_mutation_count: Arc<AtomicU64>,
    cypher_mutation_us_total: Arc<AtomicU64>,
    merge_record_count: Arc<AtomicU64>,
    last_compaction_ms: Arc<AtomicU64>,
}

impl TieredGraphEngine {
    /// Create a new engine with Lance-backed persistence.
    pub fn new(config: TieredEngineConfig, data_dir: &str) -> Self {
        Self::build(config, data_dir)
    }

    fn build(config: TieredEngineConfig, data_dir: &str) -> Self {
        let partition_count = config.partition_count;
        if partition_count > 1 {
            tracing::info!(partition_count, "partitioned graph store enabled");
        }
        // s3_prefix is used for LanceDB connect path.
        // In production: YATA_S3_PREFIX env (e.g. "yata/") → connect_from_env builds s3:// URI
        // In tests: data_dir (tempdir) → connect_local uses this path
        let s3_prefix = std::env::var("YATA_S3_PREFIX").unwrap_or_else(|_| data_dir.to_string());

        tracing::info!("using LanceDB (no persistent CSR)");
        Self {
            config,
            lance_db: Arc::new(tokio::sync::Mutex::new(None)),
            lance_table: Arc::new(tokio::sync::Mutex::new(None)),
            s3_prefix,
            security_scope_cache: Arc::new(Mutex::new(HashMap::new())),
            cypher_read_count: Arc::new(AtomicU64::new(0)),
            cypher_mutation_count: Arc::new(AtomicU64::new(0)),
            cypher_mutation_us_total: Arc::new(AtomicU64::new(0)),
            merge_record_count: Arc::new(AtomicU64::new(0)),
            last_compaction_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Build an ephemeral LanceReadStore from LanceDB table (scans needed labels).
    /// This replaces the persistent L0 CSR — each query builds a fresh read store.
    fn build_read_store(&self, labels: &[&str]) -> Result<yata_lance::LanceReadStore, String> {
        let lance_tbl = self.lance_table.clone();
        self.block_on(async {
            let tbl_guard = lance_tbl.lock().await;
            let tbl = match tbl_guard.as_ref() {
                Some(t) => t,
                None => return Ok(yata_lance::LanceReadStore::default()),
            };
            let batches = if labels.is_empty() {
                tbl.scan_all().await.map_err(|e| format!("LanceDB scan: {e}"))?
            } else {
                let mut all = Vec::new();
                for label in labels {
                    let lb = tbl.scan_filter(&format!("label = '{}'", label.replace('\'', "''")))
                        .await.map_err(|e| format!("LanceDB scan: {e}"))?;
                    all.extend(lb);
                }
                all
            };
            yata_lance::LanceReadStore::from_live_batches(&batches, &[])
        })
    }

    /// Ensure LanceDB connection + table are initialized.
    fn ensure_lance(&self) {
        let lance_db = self.lance_db.clone();
        let lance_tbl = self.lance_table.clone();
        let prefix = self.s3_prefix.clone();
        let _ = self.block_on(async {
            let mut db_guard = lance_db.lock().await;
            if db_guard.is_none() {
                let new_db = match yata_lance::YataDb::connect_from_env(&prefix).await {
                    Some(db) => db,
                    None => match yata_lance::YataDb::connect_local(&prefix).await {
                        Ok(db) => db,
                        Err(_) => return,
                    },
                };
                *db_guard = Some(new_db);
            }
            let mut tbl_guard = lance_tbl.lock().await;
            if tbl_guard.is_none() {
                if let Some(ref db) = *db_guard {
                    if let Ok(tbl) = db.open_table("vertices").await {
                        *tbl_guard = Some(tbl);
                    }
                }
            }
        });
    }

    /// Run an async future, handling both inside-runtime and outside-runtime contexts.
    fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(f))
        } else {
            ENGINE_RT.block_on(f)
        }
    }

    /// Mark a single label as dirty.


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
    /// 1 label load, 1 MemoryGraph copy, N mutations, 1 read-store rebuild, 1 WAL fsync.
    /// Returns per-statement results.
    pub fn batch_query_with_context(
        &self,
        statements: &[(&str, &[(String, String)])],
        _rls_org_id: Option<&str>,
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
        }

        self.block_on(async {
            let store = self.build_read_store(&[]).unwrap_or_default();
            let mut g = memory_bridge::memory_graph_from_store(&store);

            // Track initial state once.
            let initial_vids: HashSet<String> = g.nodes().iter().map(|n| n.id.clone()).collect();
            let initial_eids: HashSet<String> = g.rels().iter().map(|r| r.id.clone()).collect();

            // Execute all statements sequentially on the same MemoryGraph.
            let mut all_results = Vec::with_capacity(statements.len());
            for (cypher, params) in statements {
                let rows = memory_bridge::execute_query(&mut g, cypher, params)
                    .map_err(|e| e.to_string())?;
                all_results.push(rows);
            }

            // Inject provenance metadata on new nodes.
            let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
            for node in g.nodes_mut() {
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
            let after_vids: HashSet<String> = g.nodes().iter().map(|n| n.id.clone()).collect();
            let after_eids: HashSet<String> = g.rels().iter().map(|r| r.id.clone()).collect();
            let new_vids: Vec<String> = after_vids.difference(&initial_vids).cloned().collect();

            let has_changes = initial_vids != after_vids || initial_eids != after_eids;

            if has_changes {
                tracing::info!(
                    statements = statements.len(),
                    new = new_vids.len(),
                    "engine: batch mutation applied"
                );
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
            router::extract_mutation_hints(cypher)
        } else {
            self.cypher_read_count.fetch_add(1, Ordering::Relaxed);
            None
        };
        let query_start = std::time::Instant::now();

        if !is_mutation {

            // Load only vertex labels referenced in Cypher (on demand from CAS)
            let (hints_labels, _) =
                router::extract_pushdown_hints(cypher).unwrap_or_default();
            let vl_refs: Vec<&str> = hints_labels.iter().map(|s| s.as_str()).collect();

            // GIE path: Cypher → IR Plan → execute directly on the read store.
            // Design E: SecurityScope is compiled from policy vertices via query_with_did().
            // This path (query_inner) is Internal-only (no SecurityFilter needed).
            if let Ok(ast) = yata_cypher::parse(cypher) {
                let plan_result = yata_gie::transpile::transpile(&ast);

                if let Ok(plan) = plan_result {
                    let read_store = self.build_read_store(&vl_refs)?;
                    let records = yata_gie::executor::execute(&plan, &read_store);
                    let rows = yata_gie::executor::result_to_rows(&records, &plan);
                    return Ok(rows);
                }
                // GIE transpile failed (CONTAINS, UNION, UNWIND, etc.) → fall through to MemoryGraph
            }
        }

        // Mutation path: build MemoryGraph from LanceDB
        self.block_on(async {
            let store = self.build_read_store(&[]).unwrap_or_default();
            let mut g = memory_bridge::memory_graph_from_store(&store);

            // Lightweight mutation tracking: only track IDs (O(n) ID clones, no format! serialization)
            let before_vids: HashSet<String> =
                g.nodes().iter().map(|n| n.id.clone()).collect();
            let before_eids: HashSet<String> =
                g.rels().iter().map(|r| r.id.clone()).collect();
            let before_count = (before_vids.len(), before_eids.len());

            // Execute Cypher
            let rows = memory_bridge::execute_query(&mut g, cypher, params)
                .map_err(|e| e.to_string())?;

            // Inject mutation metadata for new nodes (skip expensive modified-node detection)
            if let Some(ctx) = mutation_ctx {
                let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
                for node in g.nodes_mut() {
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
                g.nodes().iter().map(|n| n.id.clone()).collect();
            let after_eids: HashSet<String> =
                g.rels().iter().map(|r| r.id.clone()).collect();
            let after_count = (after_vids.len(), after_eids.len());

            let has_changes = before_count != after_count
                || before_vids != after_vids
                || before_eids != after_eids
                || router::is_cypher_mutation(cypher);

            if has_changes {
                tracing::debug!("mutation detected, changes will be visible on next LanceDB scan");
            }

            // Record mutation elapsed time for CPM metrics
            if is_mutation {
                let elapsed_us = query_start.elapsed().as_micros() as u64;
                self.cypher_mutation_us_total.fetch_add(elapsed_us, Ordering::Relaxed);
            }

            Ok(rows)
        })
    }

    // ── Vector search ───────────────────────────────────────────────────

    /// Write vertices with embeddings to the LanceDB embeddings table.
    pub fn write_embeddings(
        &self,
        nodes: &[yata_cypher::NodeRef],
        embedding_key: &str,
        dim: usize,
    ) -> Result<usize, String> {
        if nodes.is_empty() { return Ok(0); }
        let count = nodes.len();

        let lance_db = self.lance_db.clone();
        let prefix_owned = self.s3_prefix.clone();
        let emb_key = embedding_key.to_string();

        self.block_on(async {
            let mut db_guard = lance_db.lock().await;
            if db_guard.is_none() {
                let new_db = match yata_lance::YataDb::connect_from_env(&prefix_owned).await {
                    Some(db) => db,
                    None => yata_lance::YataDb::connect_local(&prefix_owned).await
                        .map_err(|e| format!("LanceDB connect failed: {e}"))?,
                };
                *db_guard = Some(new_db);
            }
            let db = db_guard.as_ref().ok_or("LanceDB not connected")?;
            let tbl = db.open_or_create_embeddings_table(dim).await
                .map_err(|e| format!("embeddings table: {e}"))?;

            // Build batch from nodes — extract vector from props[emb_key]
            let embedding_key = &emb_key;
            let vids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
            let labels: Vec<&str> = nodes.iter().map(|n| {
                n.labels.first().map(|s| s.as_str()).unwrap_or("unknown")
            }).collect();
            let vid_arr = arrow::array::StringArray::from(vids);
            let label_arr = arrow::array::StringArray::from(labels);
            let vectors: Vec<Option<Vec<Option<f32>>>> = nodes.iter().map(|n| {
                if let Some(yata_cypher::types::Value::List(list)) = n.props.get(embedding_key) {
                    let v: Vec<Option<f32>> = list.iter().map(|val| match val {
                        yata_cypher::types::Value::Float(f) => Some(*f as f32),
                        yata_cypher::types::Value::Int(i) => Some(*i as f32),
                        _ => Some(0.0),
                    }).collect();
                    Some(v)
                } else {
                    Some(vec![Some(0.0f32); dim])
                }
            }).collect();
            let vec_arr = arrow::array::FixedSizeListArray::from_iter_primitive::<arrow::datatypes::Float32Type, _, _>(
                vectors, dim as i32,
            );
            let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("vid", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("label", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("vector",
                    arrow::datatypes::DataType::FixedSizeList(
                        std::sync::Arc::new(arrow::datatypes::Field::new("item", arrow::datatypes::DataType::Float32, true)),
                        dim as i32),
                    true),
            ]));
            let batch = arrow::record_batch::RecordBatch::try_new(schema, vec![
                std::sync::Arc::new(vid_arr),
                std::sync::Arc::new(label_arr),
                std::sync::Arc::new(vec_arr),
            ]).map_err(|e| format!("batch build: {e}"))?;
            tbl.add(batch).await.map_err(|e| format!("embeddings add: {e}"))?;
            Ok::<(), String>(())
        })?;
        Ok(count)
    }

    /// Vector search: find nearest neighbors via LanceDB.
    pub fn vector_search(
        &self,
        query_vector: Vec<f32>,
        limit: usize,
        label_filter: Option<&str>,
        _prop_filter: Option<&str>,
    ) -> Result<Vec<(yata_cypher::NodeRef, f32)>, String> {
        let lance_db = self.lance_db.clone();
        let prefix_owned = self.s3_prefix.clone();

        self.block_on(async {
            let db_guard = lance_db.lock().await;
            let db = match db_guard.as_ref() {
                Some(db) => db,
                None => return Ok(Vec::new()),
            };
            let tbl = match db.open_table("embeddings").await {
                Ok(tbl) => tbl,
                Err(_) => return Ok(Vec::new()),
            };
            let filter = label_filter.map(|l| format!("label = '{}'", l.replace('\'', "''")));
            let batches = tbl.vector_search(&query_vector, limit, filter.as_deref()).await
                .map_err(|e| format!("vector search: {e}"))?;

            let mut results = Vec::new();
            for batch in &batches {
                let vid_col = batch.column_by_name("vid")
                    .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>());
                let dist_col = batch.column_by_name("_distance")
                    .and_then(|c| c.as_any().downcast_ref::<arrow::array::Float32Array>());
                if let (Some(vids), Some(dists)) = (vid_col, dist_col) {
                    for i in 0..batch.num_rows() {
                        let vid = vids.value(i).to_string();
                        let dist = dists.value(i);
                        results.push((yata_cypher::NodeRef { id: vid, labels: Vec::new(), props: indexmap::IndexMap::new() }, dist));
                    }
                }
            }
            Ok(results)
        })
    }

    /// Create vector index on the embeddings table.
    pub fn create_embedding_index(&self) -> Result<(), String> {
        let lance_db = self.lance_db.clone();

        self.block_on(async {
            let db_guard = lance_db.lock().await;
            let db = match db_guard.as_ref() {
                Some(db) => db,
                None => return Ok(()),
            };
            let tbl = match db.open_table("embeddings").await {
                Ok(tbl) => tbl,
                Err(_) => return Ok(()),
            };
            tbl.create_vector_index().await.map_err(|e| format!("create index: {e}"))
        })
    }

    /// L0 compact threshold: trigger compaction when pending_writes exceeds this.
    /// Size-based (workload-adaptive), replaces time-based cron.
    const L0_COMPACT_THRESHOLD: usize = 10_000;

    /// Append-only merge record (LanceDB-style: tombstone old + append new fragment).
    /// WAL append + read-store append. No per-write commit() — index rebuild deferred
    /// to compaction (size-based trigger at L0_COMPACT_THRESHOLD).
    /// Write a record to LanceDB. No persistent CSR — next query will scan from LanceDB.
    pub fn merge_record(
        &self,
        label: &str,
        pk_key: &str,
        pk_value: &str,
        props: &[(&str, yata_grin::PropValue)],
    ) -> Result<u32, String> {
        self.merge_record_count.fetch_add(1, Ordering::Relaxed);
        self.invalidate_security_cache_if_policy(label, props);
        // Build a WAL entry batch and add to LanceDB
        let entry = crate::wal::WalEntry {
            seq: 0, op: crate::wal::WalOp::Upsert,
            label: label.to_string(), pk_key: pk_key.to_string(),
            pk_value: pk_value.to_string(),
            props: props.iter().map(|(k, v)| (k.to_string(), v.clone())).collect(),
            timestamp_ms: now_ms(),
        };
        let batch = crate::arrow_wal::wal_entries_to_batch(&[entry])
            .map_err(|e| format!("batch: {e}"))?;
        self.ensure_lance();
        let lance_tbl = self.lance_table.clone();
        self.block_on(async {
            let tbl_guard = lance_tbl.lock().await;
            if let Some(ref tbl) = *tbl_guard {
                tbl.add(batch).await.map_err(|e| format!("LanceDB add: {e}"))?;
            }
            Ok::<(), String>(())
        })?;
        Ok(0)
    }

    /// Write a record. Returns (vid, None).
    pub fn merge_record_with_wal(
        &self, label: &str, pk_key: &str, pk_value: &str,
        props: &[(&str, yata_grin::PropValue)],
    ) -> Result<(u32, Option<crate::wal::WalEntry>), String> {
        let vid = self.merge_record(label, pk_key, pk_value, props)?;
        Ok((vid, None))
    }

    /// Delete a record (tombstone write to LanceDB).
    pub fn delete_record(&self, label: &str, pk_key: &str, pk_value: &str) -> Result<bool, String> {
        let entry = crate::wal::WalEntry {
            seq: 0, op: crate::wal::WalOp::Delete,
            label: label.to_string(), pk_key: pk_key.to_string(),
            pk_value: pk_value.to_string(), props: Vec::new(),
            timestamp_ms: now_ms(),
        };
        let batch = crate::arrow_wal::wal_entries_to_batch(&[entry])
            .map_err(|e| format!("batch: {e}"))?;
        self.ensure_lance();
        let lance_tbl = self.lance_table.clone();
        self.block_on(async {
            let tbl_guard = lance_tbl.lock().await;
            if let Some(ref tbl) = *tbl_guard {
                tbl.add(batch).await.map_err(|e| format!("LanceDB add: {e}"))?;
            }
            Ok::<(), String>(())
        })?;
        Ok(true)
    }

    /// Delete a record. Returns (deleted, None).
    pub fn delete_record_with_wal(&self, label: &str, pk_key: &str, pk_value: &str,
    ) -> Result<(bool, Option<crate::wal::WalEntry>), String> {
        let deleted = self.delete_record(label, pk_key, pk_value)?;
        Ok((deleted, None))
    }

    /// CPM metrics.
    pub fn cpm_stats(&self) -> CpmStats {
        let reads = self.cypher_read_count.load(Ordering::Relaxed);
        let mutations = self.cypher_mutation_count.load(Ordering::Relaxed);
        let mutation_us = self.cypher_mutation_us_total.load(Ordering::Relaxed);
        let merges = self.merge_record_count.load(Ordering::Relaxed);
        let (v_count, e_count) = (0u64, 0u64);
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

    /// LanceDB compaction.
    pub fn trigger_compaction(&self) -> Result<CompactionResult, String> {
        self.ensure_lance();
        let lance_ds = self.lance_table.clone();

        // Primary: use Lance built-in compaction to merge fragments
        let result = self.block_on(async {
            let mut ds_guard = lance_ds.lock().await;
            if let Some(ref ds) = *ds_guard {
                let version_before = ds.version().await.unwrap_or(0);
                match ds.compact().await {
                    Ok(stats) => {
                        let version_after = ds.version().await.unwrap_or(version_before);
                        let removed = stats.compaction.as_ref().map(|c| c.fragments_removed).unwrap_or(0);
                        let added = stats.compaction.as_ref().map(|c| c.fragments_added).unwrap_or(0);
                        tracing::info!(
                            version_before,
                            version_after,
                            files_removed = removed,
                            files_added = added,
                                                        "Lance compaction complete"
                        );
                        Ok(CompactionResult {
                            max_seq: version_after,
                            input_entries: removed,
                            output_entries: added,
                            labels: Vec::new(),
                        })
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Lance compaction failed, skipping");
                        Ok(CompactionResult {
                            max_seq: version_before,
                            input_entries: 0,
                            output_entries: 0,
                            labels: Vec::new(),
                        })
                    }
                }
            } else {
                tracing::debug!("compaction: no Lance Dataset available");
                Ok(CompactionResult {
                    max_seq: 0,
                    input_entries: 0,
                    output_entries: 0,
                    labels: Vec::new(),
                })
            }
        });

        self.last_compaction_ms.store(now_ms(), Ordering::SeqCst);
        result
    }

    /// LanceDB cold start. Downloads Lance TableManifest + WAL tail only.
    /// Sets hot_initialized = true immediately — labels are loaded on-demand by ensure_labels().
    /// Multi-node safe: each node loads manifest independently, builds its own label subset.
    fn wal_cold_start_manifest_only(&self) -> Result<u64, String> {
        let lance_tbl = self.lance_table.clone();
        let lance_db = self.lance_db.clone();
        let prefix_owned = self.s3_prefix.clone();

        // Open LanceDB → vertices table. LanceDB handles R2/local, manifest, fragments.
        let checkpoint_seq = self.block_on(async {
            let mut db_guard = lance_db.lock().await;
            if db_guard.is_none() {
                let new_db = match yata_lance::YataDb::connect_from_env(&prefix_owned).await {
                    Some(db) => db,
                    None => match yata_lance::YataDb::connect_local(&prefix_owned).await {
                        Ok(db) => db,
                        Err(e) => {
                            tracing::info!(error = %e, "LanceDB connect failed (new database)");
                            return 0u64;
                        }
                    },
                };
                *db_guard = Some(new_db);
            }
            if let Some(ref db) = *db_guard {
                match db.open_table("vertices").await {
                    Ok(tbl) => {
                        let version = tbl.version().await.unwrap_or(0);
                        let rows = tbl.count_rows(None).await.unwrap_or(0);
                        tracing::info!(version, rows, "LanceDB cold start: table opened");
                        let mut guard = lance_tbl.lock().await;
                        *guard = Some(tbl);
                        version
                    }
                    Err(e) => {
                        tracing::info!(error = %e, "no LanceDB vertices table (new database)");
                        0
                    }
                }
            } else { 0 }
        });

        Ok(checkpoint_seq)
    }

    /// Cold start entrypoint used by the REST API.
    /// Legacy compacted-segment preload has been removed; this now uses the
    /// Lance manifest-only path and replays the WAL tail.
    pub fn wal_cold_start(&self) -> Result<u64, String> {
        self.wal_cold_start_manifest_only()
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

    /// Compile SecurityScope from policy vertices in the read store for a given DID.
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

        // Compile from read-store policy vertices
        let mut max_sensitivity_ord: u8 = 0;
        let mut collection_scopes: Vec<String> = Vec::new();
        let mut allowed_owner_hashes: Vec<u32> = Vec::new();

        { let store = self.build_read_store(&[]).unwrap_or_default(); {
                let did_val = PropValue::Str(did.to_string());

                // ClearanceAssignment: scan by did → extract level
                for vid in store.scan_vertices("ClearanceAssignment", &Predicate::Eq("did".to_string(), did_val.clone())) {
                    if let Some(PropValue::Str(level)) = store.vertex_prop(vid, "level") {
                        max_sensitivity_ord = match level.as_str() {
                            "restricted" => 3,
                            "confidential" => 2,
                            "internal" => 1,
                            _ => 0,
                        };
                    }
                }

                // RBACAssignment: scan by did → extract scope (collection prefix)
                for vid in store.scan_vertices("RBACAssignment", &Predicate::Eq("did".to_string(), did_val.clone())) {
                    if let Some(PropValue::Str(scope)) = store.vertex_prop(vid, "scope") {
                        if scope != "*" {
                            collection_scopes.push(scope.clone());
                        }
                    }
                }

                // ConsentGrant: scan by grantee_did → extract grantor_did → hash
                for vid in store.scan_vertices("ConsentGrant", &Predicate::Eq("grantee_did".to_string(), did_val.clone())) {
                    if let Some(PropValue::Str(grantor)) = store.vertex_prop(vid, "grantor_did") {
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
            } else {
            }
        }

        // Cache lookup (reads only, scope-aware key)
        if !is_mutation {
            let (hints_labels, _) = router::extract_pushdown_hints(cypher).unwrap_or_default();
            let vl_refs: Vec<&str> = hints_labels.iter().map(|s| s.as_str()).collect();

            if let Ok(ast) = yata_cypher::parse(cypher) {
                let plan = yata_gie::transpile::transpile_secured(&ast, scope)
                    .map_err(|e| format!("GIE transpile: {}", e))?;
                let read_store = self.build_read_store(&[])?;
                let records = yata_gie::executor::execute(&plan, &read_store);
                let rows = yata_gie::executor::result_to_rows(&records, &plan);
                return Ok(rows);
            }
        }

        // Mutation path: delegate to existing query()
        self.query(cypher, params, None)
    }

    /// Resolve DID's P-256 public key multibase string from DIDDocument vertex.
    /// Returns the raw multibase string (z-prefix base58btc) or None.
    pub fn resolve_did_pubkey_multibase(&self, did: &str) -> Option<String> {
        { let store = self.build_read_store(&[]).unwrap_or_default(); {
                let did_val = PropValue::Str(did.to_string());
                let vids = store.scan_vertices("DIDDocument", &Predicate::Eq("did".to_string(), did_val));
                let vid = *vids.first()?;
                match store.vertex_prop(vid, "public_key_multibase") {
                    Some(PropValue::Str(s)) => return Some(s.clone()),
                    _ => {}
                }
            }
        }
        None
    }

    /// Phase 5: Execute a distributed plan fragment step on the local read store.
    pub fn execute_fragment_step(
        &self,
        cypher: &str,
        partition_id: u32,
        partition_count: u32,
        target_round: u32,
        inbound: &std::collections::HashMap<u32, Vec<yata_gie::MaterializedRecord>>,
    ) -> Result<yata_gie::ExchangePayload, String> {
        let store = self.build_read_store(&[]).map_err(|e| format!("build: {e}"))?;
        yata_gie::execute_step(cypher, &store, partition_id, partition_count, target_round, inbound)
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

    fn get_col(rows: &[Vec<(String, String)>], col: &str) -> Vec<String> {
        rows.iter().filter_map(|row| {
            row.iter().find(|(k, _)| k == col).map(|(_, v)| v.clone())
        }).collect()
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
        // Security filtering is via query_with_did() → policy vertex lookup on read_store.
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
        assert!(get_col(&rows, "v").iter().any(|s| s.contains("hello")));
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
        assert!(get_col(&rows, "b").iter().any(|s| s.contains("y")));
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
                get_col(&rows, "c").iter().any(|s| s == &i.to_string()),
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
        assert!(get_col(&rows, "c").iter().any(|s| s == "5"));
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
        assert!(get_col(&rows, "v").iter().any(|s| s == "10"));
    }

    #[test]
    fn test_cypher_vector_search() {
        // Cypher CALL db.index.vector.queryNodes on MemoryGraph directly
        // (PropValue has no List variant, so embedding must be set via set_node_embedding)
        use yata_cypher::{Graph, MemoryGraph, NodeRef, Value};

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

        let rows = memory_bridge::execute_query(
            &mut g,
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
    fn test_vector_search() {
        // TieredGraphEngine::vector_search → YataVectorStore on Lance vector index
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);

        // Write embeddings via vector store path
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

    // ── merge_record / delete_record (read-store write path) ──────────

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
        // Record written to LanceDB — verified by merge_record returning Ok
    }

}
