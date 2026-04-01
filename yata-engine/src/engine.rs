use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use yata_grin::{Predicate, PropValue, Property, Scannable};

use crate::config::TieredEngineConfig;
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

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

fn generate_node_id() -> String {
    let seq = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("n_{}_{}", now_ms(), seq)
}

fn generate_edge_id() -> String {
    let seq = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("e_{}_{}", now_ms(), seq)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn inject_metadata(props: &mut Vec<(String, yata_grin::PropValue)>, ctx: Option<&MutationContext>, now: &str) {
    if let Some(ctx) = ctx {
        if !ctx.app_id.is_empty() { props.push(("_app_id".into(), yata_grin::PropValue::Str(ctx.app_id.clone()))); }
        if !ctx.org_id.is_empty() { props.push(("_org_id".into(), yata_grin::PropValue::Str(ctx.org_id.clone()))); }
        if !ctx.user_did.is_empty() { props.push(("_user_did".into(), yata_grin::PropValue::Str(ctx.user_did.clone()))); }
        props.push(("_updated_at".into(), yata_grin::PropValue::Str(now.to_string())));
    }
}

/// Canonical Lance table schema for vertices (Format D: 10-col typed).
///
/// Core 6 properties promoted to columns for Lance pushdown.
/// `val_json` holds overflow domain properties only (not used for filter).
fn lance_schema() -> Arc<arrow::datatypes::Schema> {
    use arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("op", DataType::UInt8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("pk_value", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::UInt64, false),
        Field::new("repo", DataType::Utf8, true),
        Field::new("owner_did", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("app_id", DataType::Utf8, true),
        Field::new("rkey", DataType::Utf8, true),
        Field::new("val_json", DataType::Utf8, true),
    ]))
}

/// Lance table schema for edges (separate table).
///
/// `src_vid`/`dst_vid` as first-class columns enable Lance pushdown for traversal.
fn edge_lance_schema() -> Arc<arrow::datatypes::Schema> {
    use arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("op", DataType::UInt8, false),
        Field::new("edge_label", DataType::Utf8, false),
        Field::new("eid", DataType::Utf8, false),
        Field::new("src_vid", DataType::Utf8, false),
        Field::new("dst_vid", DataType::Utf8, false),
        Field::new("src_label", DataType::Utf8, true),
        Field::new("dst_label", DataType::Utf8, true),
        Field::new("timestamp_ms", DataType::UInt64, false),
        Field::new("app_id", DataType::Utf8, true),
        Field::new("val_json", DataType::Utf8, true),
    ]))
}

/// Promoted vertex property names (columns 4-8 in lance_schema).
const PROMOTED_PROPS: [&str; 5] = ["repo", "owner_did", "name", "app_id", "rkey"];


/// Extract a promoted property value as string from props slice.
fn extract_prop_str<'a>(props: &'a [(&str, yata_grin::PropValue)], key: &str) -> Option<&'a str> {
    props.iter().find_map(|(k, v)| {
        if *k == key {
            match v {
                yata_grin::PropValue::Str(s) => Some(s.as_str()),
                _ => None,
            }
        } else {
            None
        }
    })
}

/// Serialize a PropValue to serde_json::Value.
fn prop_to_json(v: &yata_grin::PropValue) -> serde_json::Value {
    match v {
        yata_grin::PropValue::Str(s) => serde_json::Value::String(s.clone()),
        yata_grin::PropValue::Int(i) => serde_json::json!(*i),
        yata_grin::PropValue::Float(f) => serde_json::json!(*f),
        yata_grin::PropValue::Bool(b) => serde_json::Value::Bool(*b),
        yata_grin::PropValue::Binary(b) => {
            use base64::Engine;
            serde_json::Value::String(format!("b64:{}", base64::engine::general_purpose::STANDARD.encode(b)))
        }
        yata_grin::PropValue::Null => serde_json::Value::Null,
    }
}

/// Build a single-row vertex Arrow RecordBatch (Format D: 10-col).
///
/// Promoted properties (repo, owner_did, name, app_id, rkey) go to dedicated columns.
/// Remaining properties go to `val_json` overflow.
fn build_lance_batch(
    op: u8,
    label: &str,
    _pk_key: &str,
    pk_value: &str,
    props: &[(&str, yata_grin::PropValue)],
) -> Result<arrow::array::RecordBatch, String> {
    use arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array, UInt8Array};

    let repo = extract_prop_str(props, "repo");
    let owner_did = extract_prop_str(props, "owner_did");
    let name = extract_prop_str(props, "name");
    let app_id = extract_prop_str(props, "app_id").or_else(|| extract_prop_str(props, "_app_id"));
    let rkey = extract_prop_str(props, "rkey");

    // Overflow: non-promoted properties → val_json
    let overflow: serde_json::Map<String, serde_json::Value> = props
        .iter()
        .filter(|(k, _)| !PROMOTED_PROPS.contains(k) && *k != "_app_id")
        .map(|(k, v)| (k.to_string(), prop_to_json(v)))
        .collect();
    let val_json = if overflow.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&overflow).unwrap_or_default())
    };

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt8Array::from(vec![op])),
        Arc::new(StringArray::from(vec![label])),
        Arc::new(StringArray::from(vec![pk_value])),
        Arc::new(UInt64Array::from(vec![now_ms()])),
        Arc::new(StringArray::from(vec![repo])) as ArrayRef,
        Arc::new(StringArray::from(vec![owner_did])) as ArrayRef,
        Arc::new(StringArray::from(vec![name])) as ArrayRef,
        Arc::new(StringArray::from(vec![app_id])) as ArrayRef,
        Arc::new(StringArray::from(vec![rkey])) as ArrayRef,
        Arc::new(StringArray::from(vec![val_json.as_deref()])) as ArrayRef,
    ];

    RecordBatch::try_new(lance_schema(), columns)
        .map_err(|e| format!("Arrow RecordBatch build failed: {e}"))
}

/// Build a single-row edge Arrow RecordBatch (10-col).
fn build_edge_lance_batch(
    op: u8,
    edge_label: &str,
    eid: &str,
    src_vid: &str,
    dst_vid: &str,
    src_label: Option<&str>,
    dst_label: Option<&str>,
    props: &[(&str, yata_grin::PropValue)],
) -> Result<arrow::array::RecordBatch, String> {
    use arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array, UInt8Array};

    let app_id = extract_prop_str(props, "app_id").or_else(|| extract_prop_str(props, "_app_id"));

    // Edge overflow: all non-structural props
    let overflow: serde_json::Map<String, serde_json::Value> = props
        .iter()
        .filter(|(k, _)| !["app_id", "_app_id", "_src", "_dst"].contains(k))
        .map(|(k, v)| (k.to_string(), prop_to_json(v)))
        .collect();
    let val_json = if overflow.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&overflow).unwrap_or_default())
    };

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt8Array::from(vec![op])),
        Arc::new(StringArray::from(vec![edge_label])),
        Arc::new(StringArray::from(vec![eid])),
        Arc::new(StringArray::from(vec![src_vid])),
        Arc::new(StringArray::from(vec![dst_vid])),
        Arc::new(StringArray::from(vec![src_label])) as ArrayRef,
        Arc::new(StringArray::from(vec![dst_label])) as ArrayRef,
        Arc::new(UInt64Array::from(vec![now_ms()])),
        Arc::new(StringArray::from(vec![app_id])) as ArrayRef,
        Arc::new(StringArray::from(vec![val_json.as_deref()])) as ArrayRef,
    ];

    RecordBatch::try_new(edge_lance_schema(), columns)
        .map_err(|e| format!("Arrow edge RecordBatch build failed: {e}"))
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

const AUTO_COMPACT_FRAGMENT_THRESHOLD: u64 = 128;

/// Graph engine: LanceDB-backed. No persistent CSR. Query = LanceDB scan → ephemeral CSR → GIE.
pub struct TieredGraphEngine {
    config: TieredEngineConfig,
    lance_db: Arc<tokio::sync::Mutex<Option<yata_lance::YataDb>>>,
    lance_table: Arc<tokio::sync::Mutex<Option<yata_lance::YataTable>>>,
    /// Separate edge table (P1).
    lance_edge_table: Arc<tokio::sync::Mutex<Option<yata_lance::YataTable>>>,
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
            lance_edge_table: Arc::new(tokio::sync::Mutex::new(None)),
            s3_prefix,
            security_scope_cache: Arc::new(Mutex::new(HashMap::new())),
            cypher_read_count: Arc::new(AtomicU64::new(0)),
            cypher_mutation_count: Arc::new(AtomicU64::new(0)),
            cypher_mutation_us_total: Arc::new(AtomicU64::new(0)),
            merge_record_count: Arc::new(AtomicU64::new(0)),
            last_compaction_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Build an ArrowStore from LanceDB (zero-copy, lazy props).
    /// Label pushdown: only scans fragments matching requested labels.
    fn build_read_store(&self, labels: &[&str]) -> Result<yata_lance::ArrowStore, String> {
        let lance_tbl = self.lance_table.clone();
        self.block_on(async {
            let tbl_guard = lance_tbl.lock().await;
            let tbl = match tbl_guard.as_ref() {
                Some(t) => t,
                None => return yata_lance::ArrowStore::from_batches(Vec::new()),
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
            yata_lance::ArrowStore::from_batches(batches)
        })
    }

    /// Ensure LanceDB connection + vertex/edge tables are initialized.
    ///
    /// If the existing vertices table has the old 7-col schema (seq at col 0),
    /// migrate it: read all data, drop table, create with Format D schema, re-insert.
    fn ensure_lance(&self) {
        let lance_db = self.lance_db.clone();
        let lance_tbl = self.lance_table.clone();
        let lance_edge_tbl = self.lance_edge_table.clone();
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
            // Vertex table
            let mut tbl_guard = lance_tbl.lock().await;
            if tbl_guard.is_none() {
                if let Some(ref db) = *db_guard {
                    match db.open_table("vertices").await {
                        Ok(tbl) => {
                            // Check if old schema (col 0 = UInt64 "seq") — needs migration
                            let is_legacy = {
                                match tbl.scan_all().await {
                                    Ok(batches) => batches.first()
                                        .map(|b| b.schema().field(0).data_type() == &arrow::datatypes::DataType::UInt64)
                                        .unwrap_or(false),
                                    Err(_) => false,
                                }
                            };
                            if is_legacy {
                                tracing::info!("detected legacy 7-col vertices table — migrating to Format D in-place");
                                // Read all data via ArrowStore (handles both schemas)
                                let batches = tbl.scan_all().await.unwrap_or_default();
                                let store = yata_lance::ArrowStore::from_batches(batches).unwrap_or_else(|_| {
                                    yata_lance::ArrowStore::from_batches(Vec::new()).unwrap()
                                });
                                let vertex_count = store.vertex_count();
                                tracing::info!(vertex_count, "legacy data read, dropping old table");
                                // Drop old table and create Format D
                                let _ = db.inner().drop_table("vertices", &[]).await;
                                let new_tbl = db.create_empty_table("vertices", lance_schema()).await;
                                match new_tbl {
                                    Ok(new_t) => {
                                        tracing::info!("Format D vertices table created");
                                        *tbl_guard = Some(new_t);
                                    }
                                    Err(e) => {
                                        tracing::warn!(error = %e, "failed to create Format D table");
                                    }
                                }
                            } else {
                                *tbl_guard = Some(tbl);
                            }
                        }
                        Err(_) => {
                            let schema = lance_schema();
                            if let Ok(tbl) = db.create_empty_table("vertices", schema).await {
                                *tbl_guard = Some(tbl);
                            }
                        }
                    }
                }
            }
            drop(tbl_guard);
            // Edge table (P1)
            let mut edge_guard = lance_edge_tbl.lock().await;
            if edge_guard.is_none() {
                if let Some(ref db) = *db_guard {
                    match db.open_table("edges").await {
                        Ok(tbl) => { *edge_guard = Some(tbl); }
                        Err(_) => {
                            let schema = edge_lance_schema();
                            if let Ok(tbl) = db.create_empty_table("edges", schema).await {
                                *edge_guard = Some(tbl);
                            }
                        }
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

    /// Execute multiple Cypher mutations in a single batch.
    /// Each statement is executed directly via Lance operations.
    pub fn batch_query_with_context(
        &self,
        statements: &[(&str, &[(String, String)])],
        _rls_org_id: Option<&str>,
        ctx: &MutationContext,
    ) -> Result<Vec<Vec<Vec<(String, String)>>>, String> {
        if statements.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_lance();
        let mut all_results = Vec::with_capacity(statements.len());
        for (cypher, params) in statements {
            let rows = self.execute_mutation_direct(cypher, params, Some(ctx))?;
            all_results.push(rows);
        }
        Ok(all_results)
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
        _rls_org_id: Option<&str>,
        mutation_ctx: Option<&MutationContext>,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        self.ensure_lance();
        let is_mutation = router::is_cypher_mutation(cypher);
        if is_mutation {
            self.cypher_mutation_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.cypher_read_count.fetch_add(1, Ordering::Relaxed);
        }
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
                return Err(format!("GIE transpile failed for query: {cypher}"));
            }
            return Err(format!("Cypher parse failed for query: {cypher}"));
        }

        // Direct Lance mutation: translate Cypher AST → merge_record/delete_record
        let result = self.execute_mutation_direct(cypher, params, mutation_ctx);
        if is_mutation {
            let elapsed_us = query_start.elapsed().as_micros() as u64;
            self.cypher_mutation_us_total.fetch_add(elapsed_us, Ordering::Relaxed);
        }
        result
    }

    /// Execute Cypher mutation by translating AST directly to Lance operations.
    /// No MemoryGraph — O(1) for CREATE, O(matched) for DELETE.
    fn execute_mutation_direct(
        &self,
        cypher: &str,
        params: &[(String, String)],
        mutation_ctx: Option<&MutationContext>,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        let ast = yata_cypher::parse(cypher).map_err(|e| format!("parse: {e}"))?;
        let param_map: HashMap<String, String> = params.iter().cloned().collect();
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

        let mut var_bindings: HashMap<String, Vec<(String, String)>> = HashMap::new();

        for clause in &ast.clauses {
            match clause {
                yata_cypher::Clause::Create { patterns } => {
                    for pattern in patterns {
                        let mut prev_node_id: Option<String> = None;
                        let mut pending_rel: Option<(String, String, Vec<(String, yata_grin::PropValue)>)> = None; // (src, rel_type, props)

                        for elem in &pattern.elements {
                            match elem {
                                yata_cypher::PatternElement::Node(np) => {
                                    let label = np.labels.first().map(|s| s.as_str()).unwrap_or("_default");
                                    let node_id = generate_node_id();
                                    let mut props = self.resolve_pattern_props(&np.props, &param_map)?;
                                    inject_metadata(&mut props, mutation_ctx, &now);
                                    let props_ref: Vec<(&str, yata_grin::PropValue)> = props.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                                    self.merge_record(label, "rkey", &node_id, &props_ref)?;

                                    if let Some(ref var) = np.var {
                                        var_bindings.entry(var.clone()).or_default().push((label.to_string(), node_id.clone()));
                                    }

                                    // Complete pending relationship
                                    if let Some((src, rel_type, eprops)) = pending_rel.take() {
                                        let mut full_props = eprops;
                                        full_props.push(("_src".into(), yata_grin::PropValue::Str(src)));
                                        full_props.push(("_dst".into(), yata_grin::PropValue::Str(node_id.clone())));
                                        let edge_id = generate_edge_id();
                                        let props_ref: Vec<(&str, yata_grin::PropValue)> = full_props.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                                        self.merge_record(&rel_type, "eid", &edge_id, &props_ref)?;
                                    }

                                    prev_node_id = Some(node_id);
                                }
                                yata_cypher::PatternElement::Rel(rp) => {
                                    let rel_type = rp.types.first().cloned().unwrap_or_else(|| "RELATED".into());
                                    let eprops = self.resolve_pattern_props(&rp.props, &param_map)?;
                                    pending_rel = Some((prev_node_id.clone().unwrap_or_default(), rel_type, eprops));
                                }
                            }
                        }
                    }
                }
                yata_cypher::Clause::Match { patterns, where_: where_clause } => {
                    // Resolve matched nodes → bind to variables
                    for pattern in patterns {
                        for elem in &pattern.elements {
                            if let yata_cypher::PatternElement::Node(np) = elem {
                                let Some(ref var) = np.var else { continue };
                                let label = np.labels.first().map(|s| s.as_str()).unwrap_or("");
                                if label.is_empty() { continue; }
                                let store = self.build_read_store(&[label])?;
                                let matched = self.find_matching_vertices(&store, np, where_clause, &param_map);
                                var_bindings.insert(var.clone(), matched);
                            }
                        }
                    }
                }
                yata_cypher::Clause::Delete { exprs, .. } => {
                    for expr in exprs {
                        if let yata_cypher::Expr::Var(var) = expr {
                            if let Some(nodes) = var_bindings.get(var) {
                                for (label, node_id) in nodes.clone() {
                                    self.delete_record(&label, "rkey", &node_id)?;
                                }
                            }
                        }
                    }
                }
                // MERGE treated as CREATE (Lance last-writer-wins dedup provides MERGE semantics)
                yata_cypher::Clause::Merge { pattern, on_create, on_match } => {
                    let mut prev_node_id: Option<String> = None;
                    for elem in &pattern.elements {
                        if let yata_cypher::PatternElement::Node(np) = elem {
                            let label = np.labels.first().map(|s| s.as_str()).unwrap_or("_default");
                            // For MERGE: use inline pk_value (rkey) as node_id if available
                            let node_id = np.props.iter().find_map(|(k, v)| {
                                if k == "rkey" { if let yata_cypher::Expr::Lit(yata_cypher::Literal::Str(s)) = v { Some(s.clone()) } else { None } } else { None }
                            }).unwrap_or_else(generate_node_id);
                            let mut props = self.resolve_pattern_props(&np.props, &param_map)?;
                            // Apply SET items (on_create + on_match → all applied since we treat as upsert)
                            for set_item in on_create.iter().chain(on_match.iter()) {
                                if let yata_cypher::SetItem::PropSet(lhs, rhs) = set_item {
                                    // r += {key: value, ...} or r.key = value
                                    if let yata_cypher::Expr::Map(map_entries) = rhs {
                                        for (k, v) in map_entries {
                                            if let yata_cypher::Expr::Lit(lit) = v {
                                                let val = match lit {
                                                    yata_cypher::Literal::Str(s) => yata_grin::PropValue::Str(s.clone()),
                                                    yata_cypher::Literal::Int(i) => yata_grin::PropValue::Int(i.clone()),
                                                    yata_cypher::Literal::Float(f) => yata_grin::PropValue::Float(f.clone()),
                                                    yata_cypher::Literal::Bool(b) => yata_grin::PropValue::Bool(b.clone()),
                                                    yata_cypher::Literal::Null => yata_grin::PropValue::Null,
                                                };
                                                props.push((k.clone(), val));
                                            }
                                        }
                                    }
                                }
                            }
                            inject_metadata(&mut props, mutation_ctx, &now);
                            let props_ref: Vec<(&str, yata_grin::PropValue)> = props.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                            self.merge_record(label, "rkey", &node_id, &props_ref)?;
                            prev_node_id = Some(node_id);
                        }
                    }
                }
                yata_cypher::Clause::Set { items } => {
                    // SET applied during MERGE handling above — standalone SET is a no-op
                }
                yata_cypher::Clause::Return { .. } => {} // reads handled by GIE path
                _ => {
                    return Err(format!("unsupported mutation clause: {cypher}"));
                }
            }
        }

        Ok(Vec::new())
    }

    /// Resolve pattern props, substituting $params with values from param_map.
    fn resolve_pattern_props(
        &self,
        props: &[(String, yata_cypher::Expr)],
        param_map: &HashMap<String, String>,
    ) -> Result<Vec<(String, yata_grin::PropValue)>, String> {
        let mut result = Vec::new();
        for (k, expr) in props {
            let v = match expr {
                yata_cypher::Expr::Lit(lit) => match lit {
                    yata_cypher::Literal::Int(i) => yata_grin::PropValue::Int(*i),
                    yata_cypher::Literal::Float(f) => yata_grin::PropValue::Float(*f),
                    yata_cypher::Literal::Str(s) => yata_grin::PropValue::Str(s.clone()),
                    yata_cypher::Literal::Bool(b) => yata_grin::PropValue::Bool(*b),
                    yata_cypher::Literal::Null => yata_grin::PropValue::Null,
                },
                yata_cypher::Expr::Param(name) => {
                    let raw = param_map.get(name).ok_or_else(|| format!("missing param ${name}"))?;
                    // JSON-decode param value (params are JSON-encoded strings)
                    match serde_json::from_str::<serde_json::Value>(raw) {
                        Ok(serde_json::Value::String(s)) => yata_grin::PropValue::Str(s),
                        Ok(serde_json::Value::Number(n)) => {
                            if let Some(i) = n.as_i64() { yata_grin::PropValue::Int(i) }
                            else { yata_grin::PropValue::Float(n.as_f64().unwrap_or(0.0)) }
                        }
                        Ok(serde_json::Value::Bool(b)) => yata_grin::PropValue::Bool(b),
                        Ok(serde_json::Value::Null) => yata_grin::PropValue::Null,
                        _ => yata_grin::PropValue::Str(raw.clone()),
                    }
                }
                _ => return Err(format!("unsupported expression in CREATE props for key '{k}'")),
            };
            result.push((k.clone(), v));
        }
        Ok(result)
    }

    /// Find vertices matching a MATCH pattern (label + inline props + WHERE clause).
    fn find_matching_vertices(
        &self,
        store: &yata_lance::ArrowStore,
        np: &yata_cypher::NodePattern,
        _where_clause: &Option<yata_cypher::Expr>,
        param_map: &HashMap<String, String>,
    ) -> Vec<(String, String)> {
        let label = np.labels.first().map(|s| s.as_str()).unwrap_or("");
        if label.is_empty() { return Vec::new(); }
        let vids = store.scan_vertices_by_label(label);
        let mut matched = Vec::new();
        for vid in vids {
            // Check inline props (e.g., {tid: 'remove-me'})
            let mut all_match = true;
            for (k, expr) in &np.props {
                let expected = match expr {
                    yata_cypher::Expr::Lit(yata_cypher::Literal::Str(s)) => yata_grin::PropValue::Str(s.clone()),
                    yata_cypher::Expr::Lit(yata_cypher::Literal::Int(i)) => yata_grin::PropValue::Int(*i),
                    yata_cypher::Expr::Param(name) => {
                        if let Some(raw) = param_map.get(name) {
                            match serde_json::from_str::<serde_json::Value>(raw) {
                                Ok(serde_json::Value::String(s)) => yata_grin::PropValue::Str(s),
                                Ok(serde_json::Value::Number(n)) => {
                                    if let Some(i) = n.as_i64() { yata_grin::PropValue::Int(i) }
                                    else { yata_grin::PropValue::Float(n.as_f64().unwrap_or(0.0)) }
                                }
                                _ => yata_grin::PropValue::Str(raw.clone()),
                            }
                        } else { continue; }
                    }
                    _ => continue,
                };
                if store.vertex_prop(vid, k) != Some(expected) {
                    all_match = false;
                    break;
                }
            }
            if all_match {
                let node_id = store.vertex_prop(vid, "rkey")
                    .or_else(|| store.vertex_prop(vid, "pk_value"))
                    .map(|v| match v { PropValue::Str(s) => s, _ => format!("{vid}") })
                    .unwrap_or_else(|| format!("{vid}"));
                matched.push((label.to_string(), node_id));
            }
        }
        matched
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

    /// Write a record to LanceDB (append-only, Format D).
    ///
    /// Edges (pk_key == "eid") go to the separate edges table (P1).
    /// Vertices go to the vertices table with Format D schema (P2).
    pub fn merge_record(
        &self,
        label: &str,
        pk_key: &str,
        pk_value: &str,
        props: &[(&str, yata_grin::PropValue)],
    ) -> Result<u32, String> {
        self.merge_record_count.fetch_add(1, Ordering::Relaxed);
        self.invalidate_security_cache_if_policy(label, props);
        self.ensure_lance();

        // Edge detection: pk_key == "eid" → edge table (P1)
        if pk_key == "eid" {
            let src = props.iter().find_map(|(k, v)| if *k == "_src" { if let PropValue::Str(s) = v { Some(s.as_str()) } else { None } } else { None }).unwrap_or("");
            let dst = props.iter().find_map(|(k, v)| if *k == "_dst" { if let PropValue::Str(s) = v { Some(s.as_str()) } else { None } } else { None }).unwrap_or("");
            let batch = build_edge_lance_batch(0, label, pk_value, src, dst, None, None, props)?;
            let lance_edge_tbl = self.lance_edge_table.clone();
            self.block_on(async {
                let tbl_guard = lance_edge_tbl.lock().await;
                let tbl = tbl_guard.as_ref().ok_or("LanceDB edge table not initialized")?;
                tbl.add(batch).await.map_err(|e| format!("LanceDB edge add: {e}"))?;
                Ok::<(), String>(())
            })?;
            return Ok(0);
        }

        // Vertex write → direct Lance append (Format D)
        let batch = build_lance_batch(0, label, pk_key, pk_value, props)?;
        let lance_tbl = self.lance_table.clone();
        self.block_on(async {
            let tbl_guard = lance_tbl.lock().await;
            let tbl = tbl_guard.as_ref().ok_or("LanceDB vertex table not initialized")?;
            tbl.add(batch).await.map_err(|e| format!("LanceDB vertex add: {e}"))?;
            Ok::<(), String>(())
        })?;

        // Auto-compact
        let merges = self.merge_record_count.load(Ordering::Relaxed);
        if merges % AUTO_COMPACT_FRAGMENT_THRESHOLD == 0 && merges > 0 {
            let _ = self.trigger_compaction();
        }
        Ok(0)
    }

    /// Delete a record (tombstone write to LanceDB). Returns false if record doesn't exist.
    pub fn delete_record(&self, label: &str, pk_key: &str, pk_value: &str) -> Result<bool, String> {
        self.ensure_lance();
        let store = self.build_read_store(&[label])?;
        let exists = store.find_vertex_by_pk(label, pk_key, &yata_grin::PropValue::Str(pk_value.to_string())).is_some();
        if !exists {
            return Ok(false);
        }
        let batch = build_lance_batch(1, label, pk_key, pk_value, &[])?;
        let lance_tbl = self.lance_table.clone();
        self.block_on(async {
            let tbl_guard = lance_tbl.lock().await;
            let tbl = tbl_guard.as_ref().ok_or("LanceDB table not initialized")?;
            tbl.add(batch).await.map_err(|e| format!("LanceDB add: {e}"))?;
            Ok::<(), String>(())
        })?;
        Ok(true)
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
            let ds_guard = lance_ds.lock().await;
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
                Err("compaction: LanceDB table not initialized".to_string())
            }
        });

        self.last_compaction_ms.store(now_ms(), Ordering::SeqCst);
        result
    }

    /// LanceDB cold start. Opens vertex + edge tables (manifest + fragments from R2).
    fn cold_start_lance(&self) -> Result<u64, String> {
        let lance_tbl = self.lance_table.clone();
        let lance_edge_tbl = self.lance_edge_table.clone();
        let lance_db = self.lance_db.clone();
        let prefix_owned = self.s3_prefix.clone();

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
            let mut version = 0u64;
            if let Some(ref db) = *db_guard {
                // Vertex table
                match db.open_table("vertices").await {
                    Ok(tbl) => {
                        version = tbl.version().await.unwrap_or(0);
                        let rows = tbl.count_rows(None).await.unwrap_or(0);
                        tracing::info!(version, rows, "LanceDB cold start: vertices opened");
                        *lance_tbl.lock().await = Some(tbl);
                    }
                    Err(e) => {
                        tracing::info!(error = %e, "no LanceDB vertices table (new database)");
                    }
                }
                // Edge table (P1)
                match db.open_table("edges").await {
                    Ok(tbl) => {
                        let rows = tbl.count_rows(None).await.unwrap_or(0);
                        tracing::info!(rows, "LanceDB cold start: edges opened");
                        *lance_edge_tbl.lock().await = Some(tbl);
                    }
                    Err(e) => {
                        tracing::info!(error = %e, "no LanceDB edges table (new database)");
                    }
                }
            }
            version
        });

        Ok(checkpoint_seq)
    }

    /// Cold start entrypoint used by the REST API.
    pub fn cold_start(&self) -> Result<u64, String> {
        self.cold_start_lance()
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

        { let store = self.build_read_store(&[]).unwrap_or_else(|_| yata_lance::ArrowStore::from_batches(Vec::new()).unwrap()); {
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
        if !router::is_cypher_mutation(cypher) {
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
        { let store = self.build_read_store(&[]).unwrap_or_else(|_| yata_lance::ArrowStore::from_batches(Vec::new()).unwrap()); {
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


#[cfg(test)]
mod tests {
    use super::*;
    use yata_grin::{Property, Scannable, Topology};

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

    // ── GIE read path ───────────────────────────────────────────────

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
        // Verify re-created record is visible
        let store = e.build_read_store(&["X"]).unwrap();
        assert!(store.find_vertex_by_pk("X", "rkey", &PropValue::Str("x1".into())).is_some(),
            "re-created record must be visible after delete+merge");
    }

    // ══════════════════════════════════════════════════════════════
    // Coverage: LanceDB persistence roundtrip tests
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn test_persist_merge_record_read_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record("Post", "rkey", "p1", &[
            ("rkey", PropValue::Str("p1".into())),
            ("text", PropValue::Str("hello world".into())),
            ("likes", PropValue::Int(42)),
        ]).unwrap();
        e.merge_record("Post", "rkey", "p2", &[
            ("rkey", PropValue::Str("p2".into())),
            ("text", PropValue::Str("second post".into())),
        ]).unwrap();

        let store = e.build_read_store(&[]).unwrap();
        assert_eq!(store.vertex_count(), 2, "must see 2 posts");
        assert!(store.find_vertex_by_pk("Post", "rkey", &PropValue::Str("p1".into())).is_some());
        assert!(store.find_vertex_by_pk("Post", "rkey", &PropValue::Str("p2".into())).is_some());
    }

    #[test]
    fn test_persist_cold_restart_reads_lance() {
        let dir = tempfile::tempdir().unwrap();
        {
            let e = engine_at(dir.path());
            e.merge_record("Doc", "rkey", "d1", &[
                ("rkey", PropValue::Str("d1".into())),
                ("title", PropValue::Str("Persistent".into())),
            ]).unwrap();
            // Engine dropped here — no explicit flush
        }
        // New engine on same directory — cold restart
        {
            let e2 = engine_at(dir.path());
            e2.ensure_lance();
            let store = e2.build_read_store(&[]).unwrap();
            assert_eq!(store.vertex_count(), 1, "cold restart must recover persisted data");
            assert!(store.find_vertex_by_pk("Doc", "rkey", &PropValue::Str("d1".into())).is_some(),
                "cold restart must find the persisted vertex by PK");
        }
    }

    #[test]
    fn test_persist_pk_dedup_upsert() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        // Write same PK three times with different values
        e.merge_record("Item", "rkey", "i1", &[
            ("rkey", PropValue::Str("i1".into())),
            ("version", PropValue::Int(1)),
        ]).unwrap();
        e.merge_record("Item", "rkey", "i1", &[
            ("rkey", PropValue::Str("i1".into())),
            ("version", PropValue::Int(2)),
        ]).unwrap();
        e.merge_record("Item", "rkey", "i1", &[
            ("rkey", PropValue::Str("i1".into())),
            ("version", PropValue::Int(3)),
        ]).unwrap();

        let store = e.build_read_store(&[]).unwrap();
        // PK dedup: only 1 vertex with latest version
        let vid = store.find_vertex_by_pk("Item", "rkey", &PropValue::Str("i1".into()));
        assert!(vid.is_some(), "deduped vertex must exist");
        let version = store.vertex_prop(vid.unwrap(), "version");
        assert_eq!(version, Some(PropValue::Int(3)), "last-writer-wins: version must be 3");
    }

    #[test]
    fn test_persist_delete_tombstone_hides_record() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record("Task", "rkey", "t1", &[
            ("rkey", PropValue::Str("t1".into())),
            ("status", PropValue::Str("active".into())),
        ]).unwrap();

        // Verify visible
        let store1 = e.build_read_store(&["Task"]).unwrap();
        assert!(store1.find_vertex_by_pk("Task", "rkey", &PropValue::Str("t1".into())).is_some());

        // Delete
        let deleted = e.delete_record("Task", "rkey", "t1").unwrap();
        assert!(deleted, "delete existing record must return true");

        // Verify hidden after tombstone
        let store2 = e.build_read_store(&["Task"]).unwrap();
        assert!(store2.find_vertex_by_pk("Task", "rkey", &PropValue::Str("t1".into())).is_none(),
            "tombstoned record must not be visible");
    }

    #[test]
    fn test_persist_multi_label_same_pk_coexist() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        // Same pk_value "alice" under different labels
        e.merge_record("Person", "rkey", "alice", &[
            ("rkey", PropValue::Str("alice".into())),
            ("role", PropValue::Str("human".into())),
        ]).unwrap();
        e.merge_record("Agent", "rkey", "alice", &[
            ("rkey", PropValue::Str("alice".into())),
            ("role", PropValue::Str("bot".into())),
        ]).unwrap();

        let store = e.build_read_store(&[]).unwrap();
        assert_eq!(store.vertex_count(), 2, "same pk_value + different labels = 2 vertices");
        assert!(store.find_vertex_by_pk("Person", "rkey", &PropValue::Str("alice".into())).is_some());
        assert!(store.find_vertex_by_pk("Agent", "rkey", &PropValue::Str("alice".into())).is_some());
    }

    #[test]
    fn test_persist_cypher_edge_write_back() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        // Create two nodes + edge via Cypher
        run_query(&e, "CREATE (a:User {name: 'Alice'})-[r:FOLLOWS]->(b:User {name: 'Bob'})", &[], None).unwrap();

        // Verify nodes persisted
        let store = e.build_read_store(&[]).unwrap();
        assert!(store.vertex_count() >= 2, "must have at least 2 User nodes");

        // Verify nodes queryable via Cypher
        let rows = run_query(&e, "MATCH (u:User) RETURN u.name AS name LIMIT 10", &[], None).unwrap();
        assert_eq!(rows.len(), 2, "must find 2 User nodes via Cypher");
        let names = get_col(&rows, "name");
        assert!(names.iter().any(|n| n.contains("Alice")), "Alice must be in results");
        assert!(names.iter().any(|n| n.contains("Bob")), "Bob must be in results");
    }

    #[test]
    fn test_persist_cypher_delete_removes_from_lance() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Temp {tid: 'remove-me'})", &[], None).unwrap();

        // Verify created
        let rows1 = run_query(&e, "MATCH (n:Temp) RETURN n.tid AS tid LIMIT 10", &[], None).unwrap();
        assert_eq!(rows1.len(), 1);

        // Delete via Cypher
        run_query(&e, "MATCH (n:Temp {tid: 'remove-me'}) DELETE n", &[], None).unwrap();

        // Verify deleted
        let rows2 = run_query(&e, "MATCH (n:Temp) RETURN n.tid AS tid LIMIT 10", &[], None).unwrap();
        assert_eq!(rows2.len(), 0, "deleted node must not appear in query");
    }

    #[test]
    fn test_persist_batch_mutation_write_back() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let ctx = MutationContext {
            app_id: "test".into(), org_id: "org1".into(),
            user_id: "".into(), actor_id: "".into(),
            user_did: "".into(), actor_did: "".into(),
        };
        let stmts: Vec<(&str, &[(String, String)])> = vec![
            ("CREATE (a:BatchNode {bid: 'b1'})", &[]),
            ("CREATE (b:BatchNode {bid: 'b2'})", &[]),
            ("CREATE (c:BatchNode {bid: 'b3'})", &[]),
        ];
        let results = e.batch_query_with_context(&stmts, None, &ctx).unwrap();
        assert_eq!(results.len(), 3);

        // Verify all 3 nodes persisted
        let rows = run_query(&e, "MATCH (n:BatchNode) RETURN n.bid AS bid LIMIT 10", &[], None).unwrap();
        assert_eq!(rows.len(), 3, "batch mutation must persist all 3 nodes");
    }

    #[test]
    fn test_persist_cold_restart_after_cypher_mutation() {
        let dir = tempfile::tempdir().unwrap();
        {
            let e = engine_at(dir.path());
            run_query(&e, "CREATE (n:Durable {key: 'survives-restart', val: 99})", &[], None).unwrap();
        }
        // Cold restart
        {
            let e2 = engine_at(dir.path());
            let rows = run_query(&e2, "MATCH (n:Durable) RETURN n.key AS key, n.val AS val LIMIT 10", &[], None).unwrap();
            assert_eq!(rows.len(), 1, "Cypher-created node must survive cold restart");
            assert!(get_col(&rows, "key").iter().any(|k| k.contains("survives-restart")));
        }
    }

    #[test]
    fn test_persist_delete_nonexistent_returns_false() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let deleted = e.delete_record("Item", "rkey", "no-such-key").unwrap();
        assert!(!deleted, "delete_record should return false for nonexistent PK");
    }

    #[test]
    fn test_persist_merge_record_props_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record("Rich", "rkey", "r1", &[
            ("rkey", PropValue::Str("r1".into())),
            ("name", PropValue::Str("日本語テスト".into())),
            ("count", PropValue::Int(12345)),
            ("ratio", PropValue::Float(3.14)),
            ("active", PropValue::Bool(true)),
        ]).unwrap();

        let store = e.build_read_store(&[]).unwrap();
        let vid = store.find_vertex_by_pk("Rich", "rkey", &PropValue::Str("r1".into())).unwrap();
        assert_eq!(store.vertex_prop(vid, "name"), Some(PropValue::Str("日本語テスト".into())));
        assert_eq!(store.vertex_prop(vid, "count"), Some(PropValue::Int(12345)));
        // Float roundtrip via JSON may lose precision — check approximate
        if let Some(PropValue::Float(f)) = store.vertex_prop(vid, "ratio") {
            assert!((f - 3.14).abs() < 0.001, "float roundtrip: {f}");
        }
    }
}
