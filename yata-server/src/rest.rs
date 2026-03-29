//! yata XRPC API — Cypher query endpoint for Workers RPC.
//!
//! XRPC-only: `/xrpc/ai.gftd.yata.cypher` (unified read+write).
//! Design E: yata-native JWT auth + SecurityScope lazy compile from CSR policy vertices.
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
    /// Compiles SecurityScope from policy vertices in CSR, then applies GIE SecurityFilter.
    fn query_with_did(
        &self,
        cypher: &str,
        params: &[(String, String)],
        _did: &str,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        // Default: fall back to public query (no RLS)
        self.query(cypher, params, None)
    }

    /// Resolve a DID's P-256 public key from CSR DIDDocument vertex.
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

    /// L1 Compaction: PK-dedup WAL segments → compacted Arrow IPC segment.
    fn trigger_compaction(&self) -> Result<yata_engine::compaction::CompactionResult, String> {
        Err("trigger_compaction not implemented".to_string())
    }

    /// Current WAL head sequence number.
    fn wal_head_seq(&self) -> u64 { 0 }

    /// CPM metrics: read/mutation/mergeRecord counters.
    fn cpm_stats(&self) -> Option<yata_engine::engine::CpmStats> {
        None
    }

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
    /// When true, reject write operations (mergeRecord, triggerSnapshot, mutation cypher).
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
        .route("/xrpc/ai.gftd.yata.walTail", post(wal_tail_handler::<G>))
        .route("/xrpc/ai.gftd.yata.walApply", post(wal_apply_handler::<G>))
        // Phase 5: Arrow IPC binary transport (zero JSON overhead)
        .route("/xrpc/ai.gftd.yata.walTailArrow", post(wal_tail_arrow_handler::<G>))
        .route("/xrpc/ai.gftd.yata.walApplyArrow", post(wal_apply_arrow_handler::<G>))
        .route("/xrpc/ai.gftd.yata.walFlushSegment", post(wal_flush_segment_handler::<G>))
        .route("/xrpc/ai.gftd.yata.walCheckpoint", post(wal_checkpoint_handler::<G>))
        .route("/xrpc/ai.gftd.yata.walColdStart", post(wal_cold_start_handler::<G>))
        .route("/xrpc/ai.gftd.yata.compact", post(compact_handler::<G>))
        .route("/xrpc/ai.gftd.yata.mergeRecordWal", post(merge_record_wal_handler::<G>))
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
                // Resolve P-256 public key from CSR DIDDocument vertex
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

// ── Phase 5: Arrow IPC binary WAL transport (zero JSON overhead) ──

/// POST /xrpc/ai.gftd.yata.walTailArrow — Read WAL tail as Arrow IPC bytes.
///
/// Request: JSON `{"after_seq": N, "limit": N}`
/// Response: `application/vnd.apache.arrow.ipc` body (Arrow IPC File format).
/// Header `X-Wal-Head-Seq` contains the current WAL head sequence.
///
/// Zero-copy path: WAL entries → Arrow IPC serialize → HTTP body.
/// No JSON encode for entry props. Read replica deserializes Arrow directly.
async fn wal_tail_arrow_handler<G: GraphQueryExecutor>(
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
            match yata_engine::arrow_wal::serialize_segment_arrow(&entries) {
                Ok(arrow_bytes) => {
                    axum::response::Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/vnd.apache.arrow.ipc")
                        .header("x-wal-head-seq", head_seq.to_string())
                        .header("x-wal-count", entries.len().to_string())
                        .body(axum::body::Body::from(arrow_bytes.to_vec()))
                        .unwrap()
                }
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": format!("Arrow serialize failed: {e}")})),
                ).into_response(),
            }
        }
        Ok(Err(e)) => (StatusCode::CONFLICT, Json(serde_json::json!({"error": e}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    }
}

/// POST /xrpc/ai.gftd.yata.walApplyArrow — Apply WAL entries from Arrow IPC body.
///
/// Request: `application/vnd.apache.arrow.ipc` body (Arrow IPC File format).
/// Response: JSON `{"applied": N}`
///
/// Zero-copy path: HTTP body → Arrow IPC deserialize → typed WalEntry → wal_apply.
/// No JSON parse for entry props.
async fn wal_apply_arrow_handler<G: GraphQueryExecutor>(
    State(state): State<YataRestState<G>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    if body.is_empty() {
        return (StatusCode::OK, Json(serde_json::json!({"applied": 0})));
    }
    let entries = match yata_engine::arrow_wal::deserialize_segment_arrow(&body) {
        Ok(e) => e,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": format!("Arrow deserialize failed: {e}")}))),
    };
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

/// POST /xrpc/ai.gftd.yata.compact — L1 Compaction: PK-dedup WAL segments.
/// Write Container only.
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
            "bytes": r.data.len(),
        }))),
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
    match authorize(&headers, &state) {
        Ok(AuthResult::Internal) => {} // allow
        Ok(AuthResult::Authenticated { .. }) => {} // allow authenticated writes
        _ => return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "unauthorized"}))),
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
                serde_json::Value::String(s) => {
                    // Detect base64-encoded binary with "b64:" prefix.
                    if let Some(encoded) = s.strip_prefix("b64:") {
                        if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded) {
                            yata_grin::PropValue::Binary(bytes)
                        } else {
                            yata_grin::PropValue::Str(s.clone())
                        }
                    } else {
                        yata_grin::PropValue::Str(s.clone())
                    }
                }
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

    fn trigger_compaction(&self) -> Result<yata_engine::compaction::CompactionResult, String> {
        self.trigger_compaction()
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

        fn trigger_compaction(&self) -> Result<yata_engine::compaction::CompactionResult, String> {
            self.engine.trigger_compaction()
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
        assert_eq!(json["last_snapshot_serialize_ms"], 0);
    }

}
