//! Neo4j Query API v2 compatible HTTP endpoint.
//!
//! Spec: <https://neo4j.com/docs/query-api/current/>
//!
//! Endpoints:
//!   POST /db/{db}/query/v2        — execute Cypher, return JSON
//!   POST /db/{db}/query/v2/tx     — begin explicit transaction (stub)
//!   POST /db/{db}/query/v2/tx/{id}  — execute in transaction (stub)
//!   DELETE /db/{db}/query/v2/tx/{id} — rollback transaction (stub)
//!   POST /db/{db}/query/v2/tx/{id}/commit — commit transaction (stub)

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use yata_graph::LanceGraphStore;

/// Shared state for the Query API router.
#[derive(Clone)]
pub struct QueryApiState {
    pub graph: Arc<LanceGraphStore>,
}

// ── Request / Response types (Neo4j Query API v2 compatible) ────────────────

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub statement: String,
    #[serde(default)]
    pub parameters: serde_json::Map<String, serde_json::Value>,
    #[serde(rename = "includeCounters", default)]
    pub include_counters: bool,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub data: QueryData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub counters: Option<QueryCounters>,
    #[serde(rename = "bookmarkId")]
    pub bookmark_id: String,
}

#[derive(Debug, Serialize)]
pub struct QueryData {
    pub fields: Vec<String>,
    pub values: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Serialize)]
pub struct QueryCounters {
    #[serde(rename = "containsUpdates")]
    pub contains_updates: bool,
    #[serde(rename = "nodesCreated")]
    pub nodes_created: i64,
    #[serde(rename = "nodesDeleted")]
    pub nodes_deleted: i64,
    #[serde(rename = "relationshipsCreated")]
    pub relationships_created: i64,
    #[serde(rename = "relationshipsDeleted")]
    pub relationships_deleted: i64,
    #[serde(rename = "propertiesSet")]
    pub properties_set: i64,
    #[serde(rename = "labelsAdded")]
    pub labels_added: i64,
    #[serde(rename = "labelsRemoved")]
    pub labels_removed: i64,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub errors: Vec<ApiError>,
}

#[derive(Debug, Serialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
}

// ── Transaction stubs ───────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct TxResponse {
    pub id: String,
    #[serde(rename = "expiresAt")]
    pub expires_at: String,
}

// ── Router ──────────────────────────────────────────────────────────────────

pub fn router(graph: Arc<LanceGraphStore>) -> Router {
    let state = QueryApiState { graph };

    Router::new()
        // Neo4j Query API v2 endpoints
        .route("/db/{db}/query/v2", post(handle_query))
        // Transaction endpoints (stubs — yata has no real transactions)
        .route("/db/{db}/query/v2/tx", post(handle_tx_begin))
        .route("/db/{db}/query/v2/tx/{tx_id}", post(handle_tx_run))
        .route("/db/{db}/query/v2/tx/{tx_id}", delete(handle_tx_rollback))
        .route("/db/{db}/query/v2/tx/{tx_id}/commit", post(handle_tx_commit))
        // Discovery endpoint
        .route("/", get(handle_discovery))
        .with_state(state)
}

// ── Handlers ────────────────────────────────────────────────────────────────

async fn handle_query(
    State(state): State<QueryApiState>,
    Path(_db): Path<String>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    tracing::debug!(statement = %req.statement, "query api v2");

    // Convert parameters to (String, String) pairs for yata-cypher
    let params: Vec<(String, String)> = req
        .parameters
        .iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect();

    // Load graph and execute
    let qg_result = state.graph.to_memory_graph().await;
    match qg_result {
        Ok(mut qg) => match qg.query(&req.statement, &params) {
            Ok(rows) => {
                // Extract fields from first row
                let fields: Vec<String> = rows
                    .first()
                    .map(|row| row.iter().map(|(col, _)| col.clone()).collect())
                    .unwrap_or_default();

                // Convert rows to JSON values
                let values: Vec<Vec<serde_json::Value>> = rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|(_, val)| {
                                serde_json::from_str(val)
                                    .unwrap_or(serde_json::Value::String(val.clone()))
                            })
                            .collect()
                    })
                    .collect();

                let counters = if req.include_counters {
                    Some(QueryCounters {
                        contains_updates: false,
                        nodes_created: 0,
                        nodes_deleted: 0,
                        relationships_created: 0,
                        relationships_deleted: 0,
                        properties_set: 0,
                        labels_added: 0,
                        labels_removed: 0,
                    })
                } else {
                    None
                };

                let resp = QueryResponse {
                    data: QueryData { fields, values },
                    counters,
                    bookmark_id: "yata:0".to_string(),
                };
                (StatusCode::OK, Json(serde_json::to_value(resp).unwrap())).into_response()
            }
            Err(e) => {
                let resp = ErrorResponse {
                    errors: vec![ApiError {
                        code: "Neo.ClientError.Statement.SyntaxError".to_string(),
                        message: e.to_string(),
                    }],
                };
                (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::to_value(resp).unwrap()),
                )
                    .into_response()
            }
        },
        Err(e) => {
            let resp = ErrorResponse {
                errors: vec![ApiError {
                    code: "Neo.DatabaseError.General.UnknownError".to_string(),
                    message: e.to_string(),
                }],
            };
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::to_value(resp).unwrap()),
            )
                .into_response()
        }
    }
}

/// Begin transaction — returns a stub transaction ID.
async fn handle_tx_begin(
    Path(_db): Path<String>,
) -> impl IntoResponse {
    let resp = TxResponse {
        id: "yata-tx-1".to_string(),
        expires_at: "2099-12-31T23:59:59Z".to_string(),
    };
    (StatusCode::CREATED, Json(serde_json::to_value(resp).unwrap()))
}

/// Execute in transaction — delegates to normal query execution.
async fn handle_tx_run(
    state: State<QueryApiState>,
    Path((_db, _tx_id)): Path<(String, String)>,
    body: Json<QueryRequest>,
) -> impl IntoResponse {
    // Re-use same handler; yata has no transaction isolation
    handle_query(state, Path("yata".to_string()), body).await
}

/// Rollback transaction (no-op).
async fn handle_tx_rollback(
    Path((_db, _tx_id)): Path<(String, String)>,
) -> impl IntoResponse {
    StatusCode::ACCEPTED
}

/// Commit transaction (no-op).
async fn handle_tx_commit(
    Path((_db, _tx_id)): Path<(String, String)>,
) -> impl IntoResponse {
    StatusCode::OK
}

/// Discovery endpoint — returns server info compatible with Neo4j drivers.
async fn handle_discovery() -> impl IntoResponse {
    let info = serde_json::json!({
        "neo4j_version": "5.0.0-yata",
        "neo4j_edition": "community",
        "bolt_routing": "neo4j://localhost:7687",
        "bolt_direct": "bolt://localhost:7687",
        "query": "/db/{databaseName}/query/v2",
        "transaction": "/db/{databaseName}/query/v2/tx",
    });
    (StatusCode::OK, Json(info))
}
