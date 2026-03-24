//! Embedded yata REST API — Cypher query endpoint for Worker WASM proxy.
//!
//! Exposes `/api/cypher/query` and `/api/cypher/exec` for remote Cypher execution.
//! All queries are RLS-scoped by X-GFTD-ORG-ID header.
//! Auth: X-GFTD-APP-SECRET must match YATA_API_SECRET env var.

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Trait abstracting the graph query interface needed by the REST API.
pub trait GraphQueryExecutor: Send + Sync + 'static {
    fn query(
        &self,
        cypher: &str,
        params: &[(String, String)],
        rls_org_id: Option<&str>,
    ) -> Result<Vec<Vec<(String, String)>>, String>;

    fn import_blob(
        &self,
        label: &str,
        blob_type: yata_store::BlobType,
        partition_id: u32,
        r2_key: &str,
        data: bytes::Bytes,
    ) -> yata_store::ObjectId;

    fn rebuild_from_manifest(&self, manifest: &yata_store::FragmentManifest);

    fn force_snapshot_flush(&self);

    fn fragment_manifest(&self) -> Option<yata_store::FragmentManifest>;

    fn export_blob(&self, obj_id: yata_store::ObjectId) -> Option<bytes::Bytes>;

    fn trigger_snapshot(&self) -> Result<(u64, u64), String> {
        Err("trigger_snapshot not implemented".to_string())
    }
}

pub struct YataRestState<G: GraphQueryExecutor> {
    pub graph: Arc<G>,
    pub api_secret: String,
}

impl<G: GraphQueryExecutor> Clone for YataRestState<G> {
    fn clone(&self) -> Self {
        Self {
            graph: self.graph.clone(),
            api_secret: self.api_secret.clone(),
        }
    }
}

pub fn router<G: GraphQueryExecutor>(state: YataRestState<G>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        .route("/api/cypher/query", post(cypher_query::<G>))
        .route("/api/cypher/exec", post(cypher_exec::<G>))
        // Flight SQL — Arrow IPC over HTTP (CF Container compatible)
        .route("/api/flight-sql/query", post(flight_sql_query::<G>))
        // Trigger snapshot: serialize CSR → R2 PUT (Arrow IPC per-label)
        .route("/api/snapshot", post(trigger_snapshot_handler::<G>))
        // DO R2 proxy snapshot endpoints
        .route("/internal/snapshot/import-manifest", post(import_snapshot_manifest::<G>))
        .route("/internal/snapshot/import-blob", post(import_snapshot_blob::<G>))
        .route("/internal/snapshot/rebuild", post(rebuild_from_snapshot::<G>))
        .route("/internal/snapshot/export-manifest", get(export_snapshot_manifest::<G>))
        .route("/internal/snapshot/export-blob", get(export_snapshot_blob::<G>))
        .route("/internal/diag/page-in", post(diag_page_in::<G>))
        .with_state(state)
}

pub async fn serve<G: GraphQueryExecutor + Clone>(state: YataRestState<G>, port: u16) -> anyhow::Result<()> {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    tracing::info!(port, "yata REST API listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn authorize<G: GraphQueryExecutor>(headers: &HeaderMap, state: &YataRestState<G>) -> Result<String, StatusCode> {
    // Empty api_secret = internal-only Container (DO fetch, no external route).
    // Cloudflare guarantees only the owning Worker DO can reach this endpoint.
    if !state.api_secret.is_empty() {
        let secret = headers
            .get("x-gftd-app-secret")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if secret != state.api_secret {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    let org_id = headers
        .get("x-gftd-org-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    // Empty or "anon" org_id = no RLS filter (GIE fast path).
    // Security is enforced by GIE SecurityFilter (vertex property: sensitivity_ord, owner_hash),
    // not by org_id partition. appId/org_id is legacy routing only.
    Ok(org_id)
}

#[derive(Serialize)]
struct HealthResp {
    status: String,
}

async fn health() -> impl IntoResponse {
    Json(HealthResp {
        status: "ok".into(),
    })
}

#[derive(Deserialize)]
struct CypherReq {
    query: String,
    #[serde(default)]
    params: String,
}

#[derive(Serialize)]
struct QueryResp {
    rows: Vec<Vec<(String, String)>>,
}

#[derive(Serialize)]
struct ExecResp {
    ok: bool,
}

#[derive(Serialize)]
struct ErrResp {
    error: String,
}

async fn cypher_query<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
    headers: HeaderMap,
    Json(req): Json<CypherReq>,
) -> impl IntoResponse {
    let org_id = match authorize(&headers, &state) {
        Ok(o) => o,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error":"unauthorized"})),
            );
        }
    };
    let params = parse_params(&req.params);
    match state.graph.query(&req.query, &params, Some(&org_id)) {
        Ok(rows) => (StatusCode::OK, Json(serde_json::json!({"rows": rows}))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

async fn cypher_exec<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
    headers: HeaderMap,
    Json(req): Json<CypherReq>,
) -> impl IntoResponse {
    let org_id = match authorize(&headers, &state) {
        Ok(o) => o,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error":"unauthorized"})),
            );
        }
    };
    let params = parse_params(&req.params);
    match state.graph.query(&req.query, &params, Some(&org_id)) {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"ok": true}))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e})),
        ),
    }
}

fn parse_params(json_str: &str) -> Vec<(String, String)> {
    if json_str.is_empty() || json_str == "{}" {
        return vec![];
    }
    let map: serde_json::Map<String, serde_json::Value> = match serde_json::from_str(json_str) {
        Ok(m) => m,
        Err(_) => return vec![],
    };
    map.into_iter()
        .map(|(k, v)| {
            let s = match &v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => String::new(),
                other => other.to_string(),
            };
            (k, s)
        })
        .collect()
}

// ── DO R2 proxy snapshot endpoints ────────────────────────────────────────
//
// These endpoints allow the MagatamaContainer DO (TypeScript) to proxy
// snapshot data between R2 and the Container's Vineyard store.
// The Container itself doesn't need S3 HTTP access — the DO handles R2 I/O.

/// Per-restore session state: holds the manifest and imported blob ObjectIds.
/// This is cleared after rebuild completes.
static IMPORT_SESSION: std::sync::LazyLock<Mutex<Option<SnapshotImportSession>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));

struct SnapshotImportSession {
    manifest: yata_engine::snapshot::SnapshotManifest,
    vertex_blobs: HashMap<String, yata_store::ObjectId>,
    edge_blobs: HashMap<String, yata_store::ObjectId>,
}

/// POST /internal/snapshot/import-manifest — Store manifest JSON for an in-progress restore.
async fn import_snapshot_manifest<G: GraphQueryExecutor>(
    State(_state): State<YataRestState<G>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let manifest: yata_engine::snapshot::SnapshotManifest = match serde_json::from_slice(&body) {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("invalid manifest: {e}")})),
            );
        }
    };
    tracing::info!(
        vertices = manifest.vertex_count,
        edges = manifest.edge_count,
        v_labels = manifest.vertex_labels.len(),
        e_labels = manifest.edge_labels.len(),
        "snapshot import: manifest received"
    );
    if let Ok(mut session) = IMPORT_SESSION.lock() {
        *session = Some(SnapshotImportSession {
            manifest,
            vertex_blobs: HashMap::new(),
            edge_blobs: HashMap::new(),
        });
    }
    (StatusCode::OK, Json(serde_json::json!({"ok": true})))
}

#[derive(Deserialize)]
struct ImportBlobParams {
    label: String,
    #[serde(rename = "type")]
    blob_type: String, // "vertex" or "edge"
}

/// POST /internal/snapshot/import-blob?label=X&type=vertex — Import an Arrow IPC blob.
async fn import_snapshot_blob<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
    Query(params): Query<ImportBlobParams>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let (bt, r2_key) = match params.blob_type.as_str() {
        "vertex" => (
            yata_store::BlobType::ArrowVertexGroup,
            format!("snap/v/{}.arrow", params.label),
        ),
        "edge" => (
            yata_store::BlobType::ArrowEdgeGroup,
            format!("snap/e/{}.arrow", params.label),
        ),
        other => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("unknown blob type: {other}")})),
            );
        }
    };

    let partition_id = IMPORT_SESSION
        .lock()
        .ok()
        .and_then(|s| s.as_ref().map(|sess| sess.manifest.partition_id.get()))
        .unwrap_or(0);

    let obj_id = state.graph.import_blob(
        &params.label,
        bt,
        partition_id,
        &r2_key,
        bytes::Bytes::from(body.to_vec()),
    );

    // Track the ObjectId in the import session
    if let Ok(mut session) = IMPORT_SESSION.lock() {
        if let Some(ref mut sess) = *session {
            match params.blob_type.as_str() {
                "vertex" => { sess.vertex_blobs.insert(params.label.clone(), obj_id); }
                "edge" => { sess.edge_blobs.insert(params.label.clone(), obj_id); }
                _ => {}
            }
        }
    }

    tracing::info!(
        label = %params.label,
        blob_type = %params.blob_type,
        bytes = body.len(),
        "snapshot import: blob received"
    );
    (StatusCode::OK, Json(serde_json::json!({"ok": true, "object_id": obj_id.0})))
}

/// POST /internal/snapshot/rebuild — Rebuild CSR from imported Vineyard blobs.
async fn rebuild_from_snapshot<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
) -> impl IntoResponse {
    let session = IMPORT_SESSION.lock().ok().and_then(|mut s| s.take());
    let Some(sess) = session else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "no import session active (call import-manifest first)"})),
        );
    };

    let manifest = yata_store::FragmentManifest {
        vertex_labels: sess.vertex_blobs,
        edge_labels: sess.edge_blobs,
        csr_object: None,
        schema_object: None,
        partition_id: sess.manifest.partition_id.get(),
        timestamp_ns: sess.manifest.timestamp_ns,
        vertex_count: sess.manifest.vertex_count,
        edge_count: sess.manifest.edge_count,
    };

    let graph = state.graph.clone();
    let result = tokio::task::spawn_blocking(move || {
        graph.rebuild_from_manifest(&manifest);
    })
    .await;

    match result {
        Ok(()) => {
            tracing::info!(
                vertices = sess.manifest.vertex_count,
                edges = sess.manifest.edge_count,
                "snapshot import: CSR rebuilt from Vineyard"
            );
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "ok": true,
                    "vertex_count": sess.manifest.vertex_count,
                    "edge_count": sess.manifest.edge_count,
                })),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("rebuild failed: {e}")})),
        ),
    }
}

/// POST /api/snapshot — Trigger snapshot: serialize CSR → Arrow IPC → R2 PUT.
/// Called by YataRPC.snapshot() for persistence.
async fn trigger_snapshot_handler<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
) -> impl IntoResponse {
    match state.graph.trigger_snapshot() {
        Ok((v, e)) => (StatusCode::OK, Json(serde_json::json!({"vertices": v, "edges": e}))),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": err}))),
    }
}

/// GET /internal/snapshot/export-manifest — Return the current fragment manifest.
async fn export_snapshot_manifest<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
) -> impl IntoResponse {
    let graph = state.graph.clone();
    let manifest = tokio::task::spawn_blocking(move || {
        graph.force_snapshot_flush();
        graph.fragment_manifest()
    })
    .await;

    match manifest {
        Ok(Some(fm)) => {
            let snap = serde_json::json!({
                "vertex_count": fm.vertex_count,
                "edge_count": fm.edge_count,
                "partition_id": fm.partition_id,
                "timestamp_ns": fm.timestamp_ns,
                "vertex_labels": fm.vertex_labels.keys().collect::<Vec<_>>(),
                "edge_labels": fm.edge_labels.keys().collect::<Vec<_>>(),
                "vertex_object_ids": fm.vertex_labels.iter().map(|(k, v)| (k.clone(), v.0)).collect::<HashMap<String, u64>>(),
                "edge_object_ids": fm.edge_labels.iter().map(|(k, v)| (k.clone(), v.0)).collect::<HashMap<String, u64>>(),
            });
            (StatusCode::OK, Json(snap))
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "no snapshot manifest available"})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("snapshot flush failed: {e}")})),
        ),
    }
}

#[derive(Deserialize)]
struct ExportBlobParams {
    object_id: u64,
}

/// GET /internal/snapshot/export-blob?object_id=N — Return a Vineyard blob by ObjectId.
async fn export_snapshot_blob<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
    Query(params): Query<ExportBlobParams>,
) -> impl IntoResponse {
    let obj_id = yata_store::ObjectId(params.object_id);
    match state.graph.export_blob(obj_id) {
        Some(data) => {
            let response = axum::http::Response::builder()
                .status(200)
                .header("Content-Type", "application/octet-stream")
                .header("Content-Length", data.len().to_string())
                .body(axum::body::Body::from(data))
                .unwrap();
            response.into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "blob not found"})),
        )
            .into_response(),
    }
}

/// POST /internal/diag/page-in — Diagnostic: force ensure_labels + report S3/manifest/CSR state.
async fn diag_page_in<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
) -> impl IntoResponse {
    let graph = state.graph.clone();
    let result = tokio::task::spawn_blocking(move || {
        // 1. Check S3 env vars
        let s3_endpoint = std::env::var("YATA_S3_ENDPOINT").unwrap_or_default();
        let s3_bucket = std::env::var("YATA_S3_BUCKET").unwrap_or_default();
        let s3_key_id = std::env::var("YATA_S3_KEY_ID").or_else(|_| std::env::var("YATA_S3_ACCESS_KEY_ID")).unwrap_or_default();
        let s3_secret_len = std::env::var("YATA_S3_SECRET_KEY").or_else(|_| std::env::var("YATA_S3_SECRET_ACCESS_KEY")).unwrap_or_default().len();
        let s3_prefix = std::env::var("YATA_S3_PREFIX").unwrap_or_default();

        // 2. Get manifest
        let manifest = graph.fragment_manifest();

        // 3. Try explicit page-in by querying typed label
        let page_in_result = graph.query("MATCH (n:Post) RETURN count(n) AS cnt", &[], None);
        let untyped_result = graph.query("MATCH (n) RETURN count(n) AS cnt", &[], None);

        // 4. Check R2 blob directly
        let mut r2_check = serde_json::json!(null);
        if !s3_endpoint.is_empty() && !s3_bucket.is_empty() && s3_key_id.len() > 0 && s3_secret_len > 0 {
            let s3_secret_val = std::env::var("YATA_S3_SECRET_KEY").or_else(|_| std::env::var("YATA_S3_SECRET_ACCESS_KEY")).unwrap_or_default();
            let client = yata_s3::s3::S3Client::new(&s3_endpoint, &s3_bucket, &s3_key_id, &s3_secret_val, "auto");
            let manifest_key = format!("{s3_prefix}snap/manifest.json");
            let manifest_data = client.get_sync(&manifest_key);
            let blob_key = format!("{s3_prefix}snap/v/Post.arrow");
            let blob_data = client.get_sync(&blob_key);
            r2_check = serde_json::json!({
                "manifest_key": manifest_key,
                "manifest_ok": manifest_data.as_ref().map(|d| d.is_some()).unwrap_or(false),
                "manifest_bytes": manifest_data.as_ref().ok().and_then(|d| d.as_ref().map(|b| b.len())),
                "blob_key": blob_key,
                "blob_ok": blob_data.as_ref().map(|d| d.is_some()).unwrap_or(false),
                "blob_bytes": blob_data.as_ref().ok().and_then(|d| d.as_ref().map(|b| b.len())),
            });
        }

        serde_json::json!({
            "s3_endpoint": s3_endpoint,
            "s3_bucket": s3_bucket,
            "s3_key_id_len": s3_key_id.len(),
            "s3_secret_len": s3_secret_len,
            "s3_prefix": s3_prefix,
            "manifest": manifest.as_ref().map(|m| serde_json::json!({
                "vertex_count": m.vertex_count,
                "edge_count": m.edge_count,
                "vertex_labels": m.vertex_labels.keys().collect::<Vec<_>>(),
                "edge_labels": m.edge_labels.keys().collect::<Vec<_>>(),
            })),
            "count_typed_post": page_in_result.as_ref().map(|rows| rows.len()).unwrap_or(0),
            "count_typed_err": page_in_result.as_ref().err().map(|e| e.to_string()),
            "count_untyped": untyped_result.as_ref().map(|rows| rows.len()).unwrap_or(0),
            "r2_direct_check": r2_check,
        })
    }).await;

    match result {
        Ok(json) => (StatusCode::OK, Json(json)),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))),
    }
}

// ── Flight SQL — Arrow IPC over HTTP ────────────────────────────────────────
//
// Translates SQL SELECT → Cypher MATCH via yata-flight sql_plan parser,
// then returns result as Arrow IPC stream bytes.
// Content-Type: application/vnd.apache.arrow.stream

#[derive(Deserialize)]
struct FlightSqlReq {
    sql: String,
    #[serde(default = "default_flight_params")]
    params: String,
}

fn default_flight_params() -> String {
    "[]".to_string()
}

/// POST /api/flight-sql/query — returns Arrow IPC bytes
async fn flight_sql_query<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
    headers: HeaderMap,
    Json(req): Json<FlightSqlReq>,
) -> impl IntoResponse {
    let org_id = match authorize(&headers, &state) {
        Ok(o) => o,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                "unauthorized".to_string().into_bytes(),
            )
                .into_response();
        }
    };

    // Parse SQL → extract table name + columns + predicates
    let plan = match yata_flight::sql_plan::parse_select(&req.sql) {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    // Translate SQL plan to Cypher query for execution via graph host
    let cypher = sql_plan_to_cypher(&plan);
    let rows = match state.graph.query(&cypher, &[], Some(&org_id)) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
                .into_response();
        }
    };

    // Convert Cypher result rows to Arrow RecordBatch
    let batch = match cypher_rows_to_arrow(&rows, &plan.columns) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
                .into_response();
        }
    };

    // Serialize to Arrow IPC stream
    let ipc_bytes = match yata_arrow::batch_to_ipc(&batch) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        [("content-type", "application/vnd.apache.arrow.stream")],
        ipc_bytes.to_vec(),
    )
        .into_response()
}

/// Translate SQL plan → Cypher query string.
fn sql_plan_to_cypher(plan: &yata_flight::sql_plan::SqlPlan) -> String {
    let mut cypher = format!("MATCH (n:{})", plan.table);

    if !plan.predicates.is_empty() {
        cypher.push_str(" WHERE ");
        for (i, pred) in plan.predicates.iter().enumerate() {
            if i > 0 {
                cypher.push_str(" AND ");
            }
            let op = match pred.op {
                yata_flight::sql_plan::CompareOp::Eq => "=",
                yata_flight::sql_plan::CompareOp::Neq => "<>",
                yata_flight::sql_plan::CompareOp::Lt => "<",
                yata_flight::sql_plan::CompareOp::Gt => ">",
                yata_flight::sql_plan::CompareOp::Lte => "<=",
                yata_flight::sql_plan::CompareOp::Gte => ">=",
            };
            let val = match &pred.value {
                yata_grin::PropValue::Int(n) => n.to_string(),
                yata_grin::PropValue::Float(f) => f.to_string(),
                yata_grin::PropValue::Str(s) => format!("'{}'", s.replace('\'', "\\'")),
                yata_grin::PropValue::Bool(b) => b.to_string(),
                yata_grin::PropValue::Null => "null".to_string(),
            };
            cypher.push_str(&format!("n.{} {} {}", pred.column, op, val));
        }
    }

    cypher.push_str(" RETURN n");

    if !plan.order_by.is_empty() {
        cypher.push_str(" ORDER BY ");
        for (i, o) in plan.order_by.iter().enumerate() {
            if i > 0 {
                cypher.push_str(", ");
            }
            cypher.push_str(&format!("n.{}", o.column));
            if !o.ascending {
                cypher.push_str(" DESC");
            }
        }
    }

    if let Some(limit) = plan.limit {
        cypher.push_str(&format!(" LIMIT {}", limit));
    }
    if let Some(offset) = plan.offset {
        cypher.push_str(&format!(" SKIP {}", offset));
    }

    cypher
}

/// Convert Cypher query result rows to Arrow RecordBatch.
fn cypher_rows_to_arrow(
    rows: &[Vec<(String, String)>],
    _columns: &[String],
) -> Result<arrow::record_batch::RecordBatch, String> {
    use arrow::array::{StringBuilder, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};

    if rows.is_empty() {
        let schema = Arc::new(Schema::empty());
        return Ok(RecordBatch::new_empty(schema));
    }

    // Discover columns from first row
    let col_names: Vec<String> = rows[0].iter().map(|(k, _)| k.clone()).collect();
    let fields: Vec<Field> = col_names
        .iter()
        .map(|name| Field::new(name.as_str(), DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Build string columns
    let mut builders: Vec<StringBuilder> = col_names
        .iter()
        .map(|_| StringBuilder::new())
        .collect();

    for row in rows {
        let row_map: HashMap<&str, &str> = row.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        for (i, col) in col_names.iter().enumerate() {
            match row_map.get(col.as_str()) {
                Some(v) => builders[i].append_value(v),
                None => builders[i].append_null(),
            }
        }
    }

    let arrays: Vec<arrow::array::ArrayRef> = builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as arrow::array::ArrayRef)
        .collect();

    RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())
}

// ── TieredGraphEngine GraphQueryExecutor impl (standalone yata-server, no magatama-engine) ──

impl GraphQueryExecutor for yata_engine::TieredGraphEngine {
    fn query(
        &self,
        cypher: &str,
        params: &[(String, String)],
        rls_org_id: Option<&str>,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        self.query(cypher, params, rls_org_id)
    }

    fn import_blob(
        &self,
        label: &str,
        blob_type: yata_store::BlobType,
        partition_id: u32,
        _r2_key: &str,
        data: bytes::Bytes,
    ) -> yata_store::ObjectId {
        use yata_store::vineyard::ObjectMeta;
        let size_bytes = data.len() as u64;
        self.vineyard().put(
            ObjectMeta {
                id: yata_store::ObjectId(0),
                blob_type,
                label: label.to_string(),
                partition_id,
                size_bytes,
                r2_key: _r2_key.to_string(),
                fields: std::collections::HashMap::new(),
                created_at: 0,
            },
            data,
        )
    }

    fn rebuild_from_manifest(&self, manifest: &yata_store::FragmentManifest) {
        self.restore_from_vineyard(manifest);
    }

    fn force_snapshot_flush(&self) {
        let _ = self.trigger_snapshot();
    }

    fn fragment_manifest(&self) -> Option<yata_store::FragmentManifest> {
        self.fragment_manifest()
    }

    fn export_blob(&self, obj_id: yata_store::ObjectId) -> Option<bytes::Bytes> {
        self.vineyard().get_blob(obj_id)
    }

    fn trigger_snapshot(&self) -> Result<(u64, u64), String> {
        self.trigger_snapshot()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    /// In-memory graph executor for tests — delegates to yata-engine TieredGraphEngine.
    #[derive(Clone)]
    struct TestGraphExecutor {
        engine: Arc<yata_engine::TieredGraphEngine>,
    }

    impl TestGraphExecutor {
        fn new() -> Self {
            let engine = Arc::new(yata_engine::TieredGraphEngine::new(
                yata_engine::TieredEngineConfig::default(),
                "memory://test",
            ));
            Self { engine }
        }
    }

    impl GraphQueryExecutor for TestGraphExecutor {
        fn query(
            &self,
            cypher: &str,
            params: &[(String, String)],
            rls_org_id: Option<&str>,
        ) -> Result<Vec<Vec<(String, String)>>, String> {
            self.engine.query(cypher, params, rls_org_id)
        }

        fn import_blob(
            &self,
            _label: &str,
            _blob_type: yata_store::BlobType,
            _partition_id: u32,
            _r2_key: &str,
            _data: bytes::Bytes,
        ) -> yata_store::ObjectId {
            yata_store::ObjectId(0)
        }

        fn rebuild_from_manifest(&self, _manifest: &yata_store::FragmentManifest) {}

        fn force_snapshot_flush(&self) {}

        fn fragment_manifest(&self) -> Option<yata_store::FragmentManifest> {
            None
        }

        fn export_blob(&self, _obj_id: yata_store::ObjectId) -> Option<bytes::Bytes> {
            None
        }
    }

    fn test_state() -> YataRestState<TestGraphExecutor> {
        let graph = Arc::new(TestGraphExecutor::new());
        YataRestState {
            graph,
            api_secret: "test-secret".to_string(),
        }
    }

    async fn body_json(resp: axum::http::Response<Body>) -> serde_json::Value {
        let body = resp.into_body();
        let bytes = body.collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    // ── health ───────────────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_health() {
        let app = router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let json = body_json(resp).await;
        assert_eq!(json["status"], "ok");
    }

    // ── parse_params ──────────────────────────────────────────────────

    #[test]
    fn test_parse_params_empty() {
        assert!(parse_params("").is_empty());
        assert!(parse_params("{}").is_empty());
    }

    #[test]
    fn test_parse_params_string_values() {
        let params = parse_params(r#"{"name": "Alice", "age": 30}"#);
        assert_eq!(params.len(), 2);
        let m: std::collections::HashMap<_, _> = params.into_iter().collect();
        assert_eq!(m["name"], "Alice");
        assert_eq!(m["age"], "30");
    }

}
