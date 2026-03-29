//! yata XRPC API — Cypher query endpoint for Workers RPC.
//!
//! XRPC-only: `/xrpc/ai.gftd.yata.cypher` (unified read+write).
//! All queries are RLS-scoped via X-Magatama-Verified headers.
//! Auth: X-Magatama-Verified: true (Workers RPC internal) or x-gftd-app-secret.

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

    // ── WAL Projection API ──

    /// Read WAL tail entries after a given sequence number.
    fn wal_tail(&self, after_seq: u64, limit: usize) -> Result<Vec<yata_engine::wal::WalEntry>, String> {
        let _ = (after_seq, limit);
        Err("wal_tail not implemented".to_string())
    }

    /// Apply WAL entries to the CSR (read container).
    fn wal_apply(&self, entries: &[yata_engine::wal::WalEntry]) -> Result<u64, String> {
        let _ = entries;
        Err("wal_apply not implemented".to_string())
    }

    /// Flush WAL to R2 segment (write container).
    fn wal_flush_segment(&self) -> Result<(u64, u64, usize), String> {
        Err("wal_flush_segment not implemented".to_string())
    }

    /// Create a checkpoint (ArrowFragment + WAL metadata) for cold start recovery.
    fn wal_checkpoint(&self) -> Result<(u64, u64), String> {
        Err("wal_checkpoint not implemented".to_string())
    }

    /// Cold start from R2 checkpoint + WAL segment replay.
    fn wal_cold_start(&self) -> Result<u64, String> {
        Err("wal_cold_start not implemented".to_string())
    }

    /// Current WAL head sequence number.
    fn wal_head_seq(&self) -> u64 { 0 }

    /// Merge record and return the WAL entry for pushing to read replicas.
    fn merge_record_with_wal(
        &self,
        label: &str,
        pk_key: &str,
        pk_value: &str,
        props: &[(&str, yata_grin::PropValue)],
    ) -> Result<(u32, Option<yata_engine::wal::WalEntry>), String> {
        let _ = (label, pk_key, pk_value, props);
        Err("merge_record_with_wal not implemented".to_string())
    }
}

pub struct YataRestState<G: GraphQueryExecutor> {
    pub graph: Arc<G>,
    pub api_secret: String,
    /// When true, reject write operations (mergeRecord, triggerSnapshot, mutation cypher).
    /// Set via YATA_READONLY env var for read replica containers.
    pub readonly: bool,
}

impl<G: GraphQueryExecutor> Clone for YataRestState<G> {
    fn clone(&self) -> Self {
        Self {
            graph: self.graph.clone(),
            api_secret: self.api_secret.clone(),
            readonly: self.readonly,
        }
    }
}

pub fn router<G: GraphQueryExecutor>(state: YataRestState<G>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        // XRPC — primary API (Workers RPC only)
        .route("/xrpc/ai.gftd.yata.cypher", post(xrpc_cypher::<G>))
        // WAL Projection API
        .route("/xrpc/ai.gftd.yata.walTail", post(wal_tail_handler::<G>))
        .route("/xrpc/ai.gftd.yata.walApply", post(wal_apply_handler::<G>))
        .route("/xrpc/ai.gftd.yata.walFlushSegment", post(wal_flush_segment_handler::<G>))
        .route("/xrpc/ai.gftd.yata.walCheckpoint", post(wal_checkpoint_handler::<G>))
        .route("/xrpc/ai.gftd.yata.walColdStart", post(wal_cold_start_handler::<G>))
        .route("/xrpc/ai.gftd.yata.mergeRecordWal", post(merge_record_wal_handler::<G>))
        .with_state(state)
}

pub async fn serve<G: GraphQueryExecutor + Clone>(state: YataRestState<G>, port: u16) -> anyhow::Result<()> {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    tracing::info!(port, "yata REST API listening");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Authorize via X-Magatama-Verified (Workers RPC internal) or x-gftd-app-secret.
/// Returns (org_id, rls_scope_json).
fn authorize<G: GraphQueryExecutor>(headers: &HeaderMap, state: &YataRestState<G>) -> Result<(String, Option<serde_json::Value>), StatusCode> {
    // Workers RPC internal: X-Magatama-Verified: true
    if let Some(verified) = headers.get("X-Magatama-Verified") {
        if verified.to_str().unwrap_or("") == "true" {
            let org_id = headers
                .get("X-Magatama-Verified-Org")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("anon")
                .to_string();
            let rls_scope = headers
                .get("X-RLS-Scope")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());
            return Ok((org_id, rls_scope));
        }
    }
    // Fallback: internal token
    if let Some(token_hdr) = headers.get("X-Magatama-Internal-Token") {
        let token = token_hdr.to_str().unwrap_or("");
        if !state.api_secret.is_empty() && token == state.api_secret {
            let org_id = headers
                .get("X-Magatama-Org-Id")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("anon")
                .to_string();
            return Ok((org_id, None));
        }
    }
    // Legacy: x-gftd-app-secret
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
        .or_else(|| headers.get("X-Magatama-Verified-Org"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anon")
        .to_string();
    Ok((org_id, None))
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

/// XRPC Cypher request: { statement, parameters? }
#[derive(Deserialize)]
struct XrpcCypherReq {
    statement: String,
    #[serde(default)]
    parameters: serde_json::Map<String, serde_json::Value>,
}

fn xrpc_parse_params(
    params: &serde_json::Map<String, serde_json::Value>,
) -> Vec<(String, String)> {
    params
        .iter()
        .map(|(k, v)| {
            let encoded = match v {
                serde_json::Value::String(s) => serde_json::to_string(s).unwrap_or_default(),
                other => other.to_string(),
            };
            (k.clone(), encoded)
        })
        .collect()
}

/// POST /xrpc/ai.gftd.yata.cypher — unified Cypher read+write.
async fn xrpc_cypher<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
    headers: HeaderMap,
    Json(req): Json<XrpcCypherReq>,
) -> impl IntoResponse {
    let (org_id, rls_scope) = match authorize(&headers, &state) {
        Ok(a) => a,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"code":"unauthenticated","message":"unauthorized"})),
            );
        }
    };
    let mut params = xrpc_parse_params(&req.parameters);
    if !org_id.is_empty() && org_id != "anon" {
        params.push(("_rls_org_id".to_string(), format!("\"{}\"", org_id)));
    }
    // Inject RLS scope metadata for governance-aware filtering
    if let Some(ref scope) = rls_scope {
        if let Some(ud) = scope.get("user_did").and_then(|v| v.as_str()) {
            params.push(("_rls_user_did".to_string(), format!("\"{}\"", ud)));
        }
        if let Some(ad) = scope.get("actor_did").and_then(|v| v.as_str()) {
            params.push(("_rls_actor_did".to_string(), format!("\"{}\"", ad)));
        }
        if let Some(cl) = scope.get("clearance").and_then(|v| v.as_str()) {
            params.push(("_rls_clearance".to_string(), format!("\"{}\"", cl)));
        }
        if let Some(grants) = scope.get("consent_grants") {
            if let Ok(json) = serde_json::to_string(grants) {
                params.push(("_rls_consent_grants".to_string(), json));
            }
        }
    }

    let stmt = req.statement;
    let org = org_id.clone();
    let graph = state.graph.clone();
    let result = tokio::task::spawn_blocking(move || graph.query(&stmt, &params, Some(&org))).await;

    match result {
        Ok(Ok(raw_rows)) => {
            let columns: Vec<String> = if let Some(first) = raw_rows.first() {
                first.iter().map(|(col, _)| col.clone()).collect()
            } else {
                Vec::new()
            };
            let filtered_rows: Vec<Vec<serde_json::Value>> = raw_rows
                .into_iter()
                .filter(|row| {
                    for (k, v) in row {
                        if k == "org_id" {
                            let s: String = serde_json::from_str(v).unwrap_or_default();
                            if !s.is_empty() && s != org_id && s != "anon" {
                                return false;
                            }
                        }
                    }
                    true
                })
                .map(|row| {
                    row.into_iter()
                        .map(|(_, json_str)| {
                            serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null)
                        })
                        .collect()
                })
                .collect();

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "columns": columns,
                    "rows": filtered_rows,
                    "org_id": org_id,
                })),
            )
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"code":"internal","message":e})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"code":"internal","message":e.to_string()})),
        ),
    }
}

// ── DO R2 proxy snapshot endpoints ────────────────────────────────────────
//
// These endpoints allow the MagatamaContainer DO (TypeScript) to proxy
// snapshot data between R2 and the Container's Vineyard store.
// Legacy endpoints removed: triggerSnapshot, rebuild, exportSnapshot, mergeRecord (Cypher MERGE).
// WAL Projection: mergeRecordWal + walApply + walCheckpoint.

// ── WAL Projection handlers ────────────────────────────────────────────

/// POST /xrpc/ai.gftd.yata.walTail — Read WAL entries after a given sequence.
/// Write Container only. Used by coordinator to fetch entries for pushing to read replicas.
async fn wal_tail_handler<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    let after_seq = req.get("after_seq").and_then(|v| v.as_u64()).unwrap_or(0);
    let limit = req.get("limit").and_then(|v| v.as_u64()).unwrap_or(1000) as usize;
    let graph = state.graph.clone();
    let result = tokio::task::spawn_blocking(move || graph.wal_tail(after_seq, limit)).await;
    match result {
        Ok(Ok(entries)) => {
            let head_seq = state.graph.wal_head_seq();
            (StatusCode::OK, Json(serde_json::json!({
                "entries": entries,
                "head_seq": head_seq,
                "count": entries.len(),
            })))
        }
        Ok(Err(e)) => (StatusCode::CONFLICT, Json(serde_json::json!({"error": e}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))),
    }
}

/// POST /xrpc/ai.gftd.yata.walApply — Apply WAL entries to CSR (incremental merge).
/// Read Container only.
async fn wal_apply_handler<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    let entries: Vec<yata_engine::wal::WalEntry> = req.get("entries")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    if entries.is_empty() {
        return (StatusCode::OK, Json(serde_json::json!({"applied": 0})));
    }
    let graph = state.graph.clone();
    let result = tokio::task::spawn_blocking(move || graph.wal_apply(&entries)).await;
    match result {
        Ok(Ok(applied)) => (StatusCode::OK, Json(serde_json::json!({"applied": applied}))),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))),
    }
}

/// POST /xrpc/ai.gftd.yata.walFlushSegment — Flush WAL to R2 segment.
/// Write Container only.
async fn wal_flush_segment_handler<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
) -> impl IntoResponse {
    if state.readonly {
        return (StatusCode::METHOD_NOT_ALLOWED, Json(serde_json::json!({"error": "read-only container"})));
    }
    let graph = state.graph.clone();
    let result = tokio::task::spawn_blocking(move || graph.wal_flush_segment()).await;
    match result {
        Ok(Ok((start, end, bytes))) => (StatusCode::OK, Json(serde_json::json!({
            "seq_start": start, "seq_end": end, "bytes": bytes
        }))),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))),
    }
}

/// POST /xrpc/ai.gftd.yata.walCheckpoint — Create ArrowFragment checkpoint + WAL metadata.
/// Write Container only.
async fn wal_checkpoint_handler<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
) -> impl IntoResponse {
    if state.readonly {
        return (StatusCode::METHOD_NOT_ALLOWED, Json(serde_json::json!({"error": "read-only container"})));
    }
    let graph = state.graph.clone();
    let result = tokio::task::spawn_blocking(move || graph.wal_checkpoint()).await;
    match result {
        Ok(Ok((v, e))) => (StatusCode::OK, Json(serde_json::json!({"vertices": v, "edges": e}))),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))),
    }
}

/// POST /xrpc/ai.gftd.yata.walColdStart — Cold start from R2 checkpoint + WAL replay.
/// Read Container only.
async fn wal_cold_start_handler<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
) -> impl IntoResponse {
    let graph = state.graph.clone();
    let result = tokio::task::spawn_blocking(move || graph.wal_cold_start()).await;
    match result {
        Ok(Ok(checkpoint_seq)) => (StatusCode::OK, Json(serde_json::json!({"checkpoint_seq": checkpoint_seq}))),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))),
    }
}

/// POST /xrpc/ai.gftd.yata.mergeRecordWal — Merge record AND return WAL entry.
/// Write Container only. Coordinator uses this to get the WAL entry for pushing to read replicas.
async fn merge_record_wal_handler<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
    headers: HeaderMap,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    if state.readonly {
        return (StatusCode::METHOD_NOT_ALLOWED, Json(serde_json::json!({"error": "read-only container"})));
    }
    let (_, _) = match authorize(&headers, &state) {
        Ok(a) => a,
        Err(s) => return (s, Json(serde_json::json!({"error": "unauthorized"}))),
    };

    let label = req.get("label").and_then(|v| v.as_str()).unwrap_or("");
    let pk_key = req.get("pk_key").and_then(|v| v.as_str()).unwrap_or("rkey");
    let pk_value = req.get("pk_value").and_then(|v| v.as_str()).unwrap_or("");
    let props_val = req.get("props").cloned().unwrap_or(serde_json::Value::Object(Default::default()));

    if label.is_empty() || pk_value.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "label and pk_value required"})));
    }

    // Convert JSON props to PropValue pairs
    let mut prop_pairs: Vec<(String, yata_grin::PropValue)> = Vec::new();
    if let Some(obj) = props_val.as_object() {
        for (k, v) in obj {
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
            prop_pairs.push((k.clone(), pv));
        }
    }
    let props_ref: Vec<(&str, yata_grin::PropValue)> = prop_pairs.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();

    match state.graph.merge_record_with_wal(label, pk_key, pk_value, &props_ref) {
        Ok((vid, wal_entry)) => (StatusCode::OK, Json(serde_json::json!({
            "ok": true,
            "vid": vid,
            "wal_entry": wal_entry,
        }))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e}))),
    }
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

    // ── WAL Projection ──

    fn wal_tail(&self, after_seq: u64, limit: usize) -> Result<Vec<yata_engine::wal::WalEntry>, String> {
        self.wal_tail(after_seq, limit)
    }

    fn wal_apply(&self, entries: &[yata_engine::wal::WalEntry]) -> Result<u64, String> {
        self.wal_apply(entries)
    }

    fn wal_flush_segment(&self) -> Result<(u64, u64, usize), String> {
        self.wal_flush_segment()
    }

    fn wal_checkpoint(&self) -> Result<(u64, u64), String> {
        self.wal_checkpoint()
    }

    fn wal_cold_start(&self) -> Result<u64, String> {
        self.wal_cold_start()
    }

    fn wal_head_seq(&self) -> u64 {
        self.wal_head_seq()
    }

    fn merge_record_with_wal(
        &self,
        label: &str,
        pk_key: &str,
        pk_value: &str,
        props: &[(&str, yata_grin::PropValue)],
    ) -> Result<(u32, Option<yata_engine::wal::WalEntry>), String> {
        self.merge_record_with_wal(label, pk_key, pk_value, props)
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

        fn wal_tail(&self, after_seq: u64, limit: usize) -> Result<Vec<yata_engine::wal::WalEntry>, String> {
            self.engine.wal_tail(after_seq, limit)
        }

        fn wal_apply(&self, entries: &[yata_engine::wal::WalEntry]) -> Result<u64, String> {
            self.engine.wal_apply(entries)
        }

        fn wal_flush_segment(&self) -> Result<(u64, u64, usize), String> {
            self.engine.wal_flush_segment()
        }

        fn wal_checkpoint(&self) -> Result<(u64, u64), String> {
            self.engine.wal_checkpoint()
        }

        fn wal_cold_start(&self) -> Result<u64, String> {
            self.engine.wal_cold_start()
        }

        fn wal_head_seq(&self) -> u64 {
            self.engine.wal_head_seq()
        }

        fn merge_record_with_wal(
            &self,
            label: &str,
            pk_key: &str,
            pk_value: &str,
            props: &[(&str, yata_grin::PropValue)],
        ) -> Result<(u32, Option<yata_engine::wal::WalEntry>), String> {
            self.engine.merge_record_with_wal(label, pk_key, pk_value, props)
        }
    }

    fn test_state() -> YataRestState<TestGraphExecutor> {
        let graph = Arc::new(TestGraphExecutor::new());
        YataRestState {
            graph,
            api_secret: "test-secret".to_string(),
            readonly: false,
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

}
