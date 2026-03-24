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

    fn rebuild_from_r2(&self);

    fn force_snapshot_flush(&self);

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
        // XRPC — primary API (Workers RPC only)
        .route("/xrpc/ai.gftd.yata.cypher", post(xrpc_cypher::<G>))
        .route("/xrpc/ai.gftd.yata.triggerSnapshot", post(trigger_snapshot_handler::<G>))
        .route("/internal/snapshot/rebuild", post(rebuild_from_snapshot::<G>))
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
// The Container itself doesn't need S3 HTTP access — the DO handles R2 I/O.


/// POST /internal/snapshot/rebuild — Rebuild CSR from R2 ArrowFragment.
async fn rebuild_from_snapshot<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
) -> impl IntoResponse {
    let graph = state.graph.clone();
    let result = tokio::task::spawn_blocking(move || {
        graph.rebuild_from_r2();
    })
    .await;

    match result {
        Ok(()) => {
            tracing::info!("CSR rebuilt from R2 ArrowFragment");
            (StatusCode::OK, Json(serde_json::json!({"ok": true})))
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

// Legacy snapshot import/export/diag endpoints removed.
// ArrowFragment format: snapshot → R2 PUT (trigger_snapshot), page-in → R2 GET (ensure_labels).



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

    fn rebuild_from_r2(&self) {
        self.restore_from_r2();
    }

    fn force_snapshot_flush(&self) {
        let _ = self.trigger_snapshot();
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

        fn rebuild_from_r2(&self) {}

        fn force_snapshot_flush(&self) {}
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

}
