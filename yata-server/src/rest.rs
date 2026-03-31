//! yata XRPC API — Cypher query endpoint for Workers RPC.
//!
//! XRPC-only: `/xrpc/ai.gftd.yata.cypher` (unified read+write).
//! Design E: yata-native JWT auth + SecurityScope lazy compile from policy vertices.
//! Auth: X-Magatama-Verified: true (internal bypass) or Authorization: Bearer {ES256 JWT}.

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Trait abstracting the graph query interface needed by the REST API.
pub trait GraphQueryExecutor: Send + Sync + 'static {
    fn query(
        &self,
        cypher: &str,
        params: &[(String, String)],
        rls_org_id: Option<&str>,
    ) -> Result<Vec<Vec<(String, String)>>, String>;

    /// Query with DID-based SecurityScope (Design E path).
    /// Compiles SecurityScope from policy vertices, then applies GIE SecurityFilter.
    fn query_with_did(
        &self,
        cypher: &str,
        params: &[(String, String)],
        _did: &str,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        // Default: fall back to public query (no RLS)
        self.query(cypher, params, None)
    }

    /// Resolve a DID's P-256 public key from the DIDDocument vertex.
    /// Returns uncompressed P-256 key (65 bytes) or None.
    fn resolve_did_pubkey(&self, _did: &str) -> Option<Vec<u8>> {
        None
    }

    /// Phase 5: Execute a distributed plan fragment step.
    /// Returns exchange payload with outbound data or final results.
    fn execute_fragment_step(
        &self,
        cypher: &str,
        partition_id: u32,
        partition_count: u32,
        target_round: u32,
        inbound: &std::collections::HashMap<u32, Vec<yata_gie::MaterializedRecord>>,
    ) -> Result<yata_gie::ExchangePayload, String> {
        let _ = (cypher, partition_id, partition_count, target_round, inbound);
        Err("execute_fragment_step not implemented".into())
    }

    /// Cold start: open LanceDB table.
    fn cold_start(&self) -> Result<u64, String> {
        Err("cold_start not implemented".to_string())
    }

    /// L1 Compaction: PK-dedup Lance fragments.
    fn trigger_compaction(&self) -> Result<yata_engine::engine::CompactionResult, String> {
        Err("trigger_compaction not implemented".to_string())
    }

    /// CPM metrics: read/mutation/mergeRecord counters.
    fn cpm_stats(&self) -> Option<yata_engine::engine::CpmStats> {
        None
    }
}

pub struct YataRestState<G: GraphQueryExecutor> {
    pub graph: Arc<G>,
    /// When true, reject write operations (mergeRecord, compact, mutation cypher).
    /// Set via YATA_READONLY env var for read replica containers.
    pub readonly: bool,
}

impl<G: GraphQueryExecutor> Clone for YataRestState<G> {
    fn clone(&self) -> Self {
        Self {
            graph: self.graph.clone(),
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
        .route("/xrpc/ai.gftd.yata.cypherBatch", post(xrpc_cypher_batch::<G>))
        // WAL Projection API
        .route("/xrpc/ai.gftd.yata.coldStart", post(cold_start_handler::<G>))
        .route("/xrpc/ai.gftd.yata.compact", post(compact_handler::<G>))
        .route("/xrpc/ai.gftd.yata.stats", get(stats_handler::<G>))
        // Phase 5: Distributed GIE fragment execution
        .route("/xrpc/ai.gftd.yata.executeFragment", post(execute_fragment_handler::<G>))
        .with_state(state)
}

pub async fn serve<G: GraphQueryExecutor + Clone>(state: YataRestState<G>, port: u16) -> anyhow::Result<()> {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    tracing::info!(port, "yata REST API listening");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Authentication result from authorize().
/// Design E: yata-native JWT verification with 3 auth levels.
enum AuthResult {
    /// Workers RPC internal (X-Magatama-Verified: true) — bypass all security filtering.
    Internal,
    /// JWT verified — DID extracted for SecurityScope compilation.
    Authenticated { did: String },
    /// No authentication — public data only (sensitivity_ord = 0).
    Public,
}

/// Authorize request. Design E: yata-native JWT + graph-based security.
///
/// Priority:
/// 1. X-Magatama-Verified: true → Internal (bypass, Workers RPC trust)
/// 2. Authorization: Bearer {jwt} → JWT verify → Authenticated { did }
/// 3. No auth → Public
fn authorize<G: GraphQueryExecutor>(headers: &HeaderMap, state: &YataRestState<G>) -> Result<AuthResult, StatusCode> {
    // 1. Workers RPC internal trust (coordinator sets this header)
    if let Some(verified) = headers.get("X-Magatama-Verified") {
        if verified.to_str().unwrap_or("") == "true" {
            return Ok(AuthResult::Internal);
        }
    }

    // 2. JWT verification (Design E: yata-native ES256 verify)
    if let Some(auth_header) = headers.get("Authorization") {
        let auth_str = auth_header.to_str().unwrap_or("");
        if let Some(token) = auth_str.strip_prefix("Bearer ") {
            let resolve_key = |did: &str| -> Option<Vec<u8>> {
                // Resolve P-256 public key from the DIDDocument vertex
                let pubkey_multibase = state.graph.resolve_did_pubkey(did)?;
                // pubkey_multibase is already uncompressed P-256 (65 bytes)
                Some(pubkey_multibase)
            };
            match crate::jwt::verify_es256_jwt(token, "did:web:pds.gftd.ai", resolve_key) {
                Ok(claims) => return Ok(AuthResult::Authenticated { did: claims.iss }),
                Err(e) => {
                    tracing::warn!("JWT verification failed: {}", e);
                    return Err(StatusCode::UNAUTHORIZED);
                }
            }
        }
    }

    // 3. No auth → public access
    Ok(AuthResult::Public)
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

/// GET /xrpc/ai.gftd.yata.stats — CPM metrics (read/mutation/mergeRecord counters).
async fn stats_handler<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
) -> impl IntoResponse {
    match state.graph.cpm_stats() {
        Some(stats) => (StatusCode::OK, Json(serde_json::to_value(stats).unwrap_or_default())),
        None => (StatusCode::OK, Json(serde_json::json!({"error": "stats not available"}))),
    }
}

// ── Phase 5: Distributed GIE fragment execution ──

#[derive(Deserialize)]
struct ExecuteFragmentReq {
    cypher: String,
    partition_id: u32,
    partition_count: u32,
    #[serde(default)]
    round: u32,
    #[serde(default)]
    inbound: std::collections::HashMap<u32, Vec<yata_gie::MaterializedRecord>>,
}

async fn execute_fragment_handler<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
    headers: HeaderMap,
    Json(req): Json<ExecuteFragmentReq>,
) -> impl IntoResponse {
    match authorize(&headers, &state) {
        Ok(AuthResult::Internal) => {}
        _ => return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "internal only"}))),
    };
    let graph = state.graph.clone();
    let result = tokio::task::spawn_blocking(move || {
        graph.execute_fragment_step(
            &req.cypher,
            req.partition_id,
            req.partition_count,
            req.round,
            &req.inbound,
        )
    }).await;
    match result {
        Ok(Ok(payload)) => (StatusCode::OK, Json(serde_json::to_value(payload).unwrap_or_default())),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))),
    }
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
/// Design E: auth dispatches to 3 paths (Internal/Authenticated/Public).
async fn xrpc_cypher<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
    headers: HeaderMap,
    Json(req): Json<XrpcCypherReq>,
) -> impl IntoResponse {
    let auth = match authorize(&headers, &state) {
        Ok(a) => a,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"code":"unauthenticated","message":"unauthorized"})),
            );
        }
    };

    let params = xrpc_parse_params(&req.parameters);
    let stmt = req.statement;
    let graph = state.graph.clone();

    let result = match auth {
        AuthResult::Internal => {
            // Bypass: internal requests (coordinator, PDS) skip SecurityFilter
            tokio::task::spawn_blocking(move || graph.query(&stmt, &params, None)).await
        }
        AuthResult::Authenticated { did } => {
            // Design E: DID-based SecurityScope compiled from policy vertices
            tokio::task::spawn_blocking(move || graph.query_with_did(&stmt, &params, &did)).await
        }
        AuthResult::Public => {
            // Public: only sensitivity_ord=0 data visible
            tokio::task::spawn_blocking(move || graph.query_with_did(&stmt, &params, "")).await
        }
    };

    match result {
        Ok(Ok(raw_rows)) => {
            let columns: Vec<String> = if let Some(first) = raw_rows.first() {
                first.iter().map(|(col, _)| col.clone()).collect()
            } else {
                Vec::new()
            };
            // No post-filter: GIE SecurityFilter handles all governance inline
            let rows: Vec<Vec<serde_json::Value>> = raw_rows
                .into_iter()
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
                    "rows": rows,
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

/// POST /xrpc/ai.gftd.yata.cypherBatch — execute multiple Cypher statements in one HTTP round-trip.
/// CP3 optimization: reduces N × ~1-5ms coordinator overhead → 1 × ~1-5ms.
/// All statements execute on the same Container (same partition) with shared auth context.
#[derive(Deserialize)]
struct XrpcCypherBatchReq {
    statements: Vec<XrpcCypherReq>,
}

async fn xrpc_cypher_batch<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
    headers: HeaderMap,
    Json(req): Json<XrpcCypherBatchReq>,
) -> impl IntoResponse {
    let auth = match authorize(&headers, &state) {
        Ok(a) => a,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"code":"unauthenticated","message":"unauthorized"})),
            );
        }
    };

    let results: Vec<serde_json::Value> = req.statements.into_iter().map(|stmt_req| {
        let params = xrpc_parse_params(&stmt_req.parameters);
        let result = match &auth {
            AuthResult::Internal => state.graph.query(&stmt_req.statement, &params, None),
            AuthResult::Authenticated { did } => state.graph.query_with_did(&stmt_req.statement, &params, did),
            AuthResult::Public => state.graph.query_with_did(&stmt_req.statement, &params, ""),
        };
        match result {
            Ok(raw_rows) => {
                let columns: Vec<String> = if let Some(first) = raw_rows.first() {
                    first.iter().map(|(col, _)| col.clone()).collect()
                } else {
                    Vec::new()
                };
                let rows: Vec<Vec<serde_json::Value>> = raw_rows
                    .into_iter()
                    .map(|row| row.into_iter().map(|(_, json_str)| {
                        serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null)
                    }).collect())
                    .collect();
                serde_json::json!({"columns": columns, "rows": rows})
            }
            Err(e) => serde_json::json!({"error": e}),
        }
    }).collect();

    (StatusCode::OK, Json(serde_json::json!({"results": results})))
}


/// POST /xrpc/ai.gftd.yata.coldStart — Open LanceDB table from R2.
async fn cold_start_handler<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
) -> impl IntoResponse {
    let graph = state.graph.clone();
    let result = tokio::task::spawn_blocking(move || graph.cold_start()).await;
    match result {
        Ok(Ok(checkpoint_seq)) => (StatusCode::OK, Json(serde_json::json!({"checkpoint_seq": checkpoint_seq}))),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))),
    }
}

/// POST /xrpc/ai.gftd.yata.compact — L1 Compaction: PK-dedup Lance fragments.
async fn compact_handler<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
) -> impl IntoResponse {
    if state.readonly {
        return (StatusCode::METHOD_NOT_ALLOWED, Json(serde_json::json!({"error": "read-only container"})));
    }
    let graph = state.graph.clone();
    let result = tokio::task::spawn_blocking(move || graph.trigger_compaction()).await;
    match result {
        Ok(Ok(r)) => (StatusCode::OK, Json(serde_json::json!({
            "input_entries": r.input_entries,
            "output_entries": r.output_entries,
            "compacted_seq": r.max_seq,
            "labels": r.labels,
        }))),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))),
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

    fn query_with_did(
        &self,
        cypher: &str,
        params: &[(String, String)],
        did: &str,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        self.query_with_did(cypher, params, did)
    }

    fn resolve_did_pubkey(&self, did: &str) -> Option<Vec<u8>> {
        let multibase = self.resolve_did_pubkey_multibase(did)?;
        crate::jwt::resolve_multibase_p256_key(&multibase)
    }

    fn cold_start(&self) -> Result<u64, String> {
        self.cold_start()
    }

    fn trigger_compaction(&self) -> Result<yata_engine::engine::CompactionResult, String> {
        self.trigger_compaction()
    }

    fn cpm_stats(&self) -> Option<yata_engine::engine::CpmStats> {
        Some(self.cpm_stats())
    }

    fn execute_fragment_step(
        &self,
        cypher: &str,
        partition_id: u32,
        partition_count: u32,
        target_round: u32,
        inbound: &std::collections::HashMap<u32, Vec<yata_gie::MaterializedRecord>>,
    ) -> Result<yata_gie::ExchangePayload, String> {
        self.execute_fragment_step(cypher, partition_id, partition_count, target_round, inbound)
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

        fn cold_start(&self) -> Result<u64, String> {
            self.engine.cold_start()
        }

        fn trigger_compaction(&self) -> Result<yata_engine::engine::CompactionResult, String> {
            self.engine.trigger_compaction()
        }

        fn cpm_stats(&self) -> Option<yata_engine::engine::CpmStats> {
            Some(self.engine.cpm_stats())
        }
    }

    fn test_state() -> YataRestState<TestGraphExecutor> {
        let graph = Arc::new(TestGraphExecutor::new());
        YataRestState {
            graph,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cypher_batch() {
        let app = router(test_state());
        let body = serde_json::json!({
            "statements": [
                {"statement": "RETURN 1 AS x", "parameters": {}},
                {"statement": "RETURN 2 AS y", "parameters": {}}
            ]
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/xrpc/ai.gftd.yata.cypherBatch")
                    .header("Content-Type", "application/json")
                    .header("X-Magatama-Verified", "true")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let json = body_json(resp).await;
        let results = json["results"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0]["columns"].as_array().is_some());
        assert!(results[1]["columns"].as_array().is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_stats() {
        let app = router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/xrpc/ai.gftd.yata.stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let json = body_json(resp).await;
        assert_eq!(json["cypher_read_count"], 0);
        assert_eq!(json["cypher_mutation_count"], 0);
        assert_eq!(json["merge_record_count"], 0);
        assert_eq!(json["last_compaction_ms"], 0);
    }

}
