//! Neo4j Query API v2 compatible HTTP endpoint.
//!
//! Uses `LanceGraphStore::cached_query()` — CSR + query result LRU cache.

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

#[derive(Clone)]
pub struct QueryApiState {
    pub graph: Arc<LanceGraphStore>,
}

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
    #[serde(rename = "cacheHit")]
    pub cache_hit: bool,
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
    pub nodes_created: usize,
    #[serde(rename = "nodesDeleted")]
    pub nodes_deleted: usize,
    #[serde(rename = "relationshipsCreated")]
    pub relationships_created: usize,
    #[serde(rename = "relationshipsDeleted")]
    pub relationships_deleted: usize,
    #[serde(rename = "propertiesSet")]
    pub properties_set: usize,
    #[serde(rename = "labelsAdded")]
    pub labels_added: usize,
    #[serde(rename = "labelsRemoved")]
    pub labels_removed: usize,
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

#[derive(Debug, Serialize)]
pub struct TxResponse {
    pub id: String,
    #[serde(rename = "expiresAt")]
    pub expires_at: String,
}

pub fn router(graph: Arc<LanceGraphStore>) -> Router {
    let state = QueryApiState { graph };
    Router::new()
        .route("/db/{db}/query/v2", post(handle_query))
        .route("/db/{db}/query/v2/tx", post(handle_tx_begin))
        .route("/db/{db}/query/v2/tx/{tx_id}", post(handle_tx_run))
        .route("/db/{db}/query/v2/tx/{tx_id}", delete(handle_tx_rollback))
        .route("/db/{db}/query/v2/tx/{tx_id}/commit", post(handle_tx_commit))
        .route("/", get(handle_discovery))
        .with_state(state)
}

async fn handle_query(
    State(state): State<QueryApiState>,
    Path(_db): Path<String>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    execute_cypher(&state.graph, &req).await
}

async fn execute_cypher(graph: &LanceGraphStore, req: &QueryRequest) -> axum::response::Response {
    tracing::debug!(statement = %req.statement, "query api v2");

    let params: Vec<(String, String)> = req
        .parameters
        .iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect();

    match graph.cached_query(&req.statement, &params).await {
        Ok(result) => {
            let fields: Vec<String> = result
                .rows
                .first()
                .map(|row| row.iter().map(|(col, _)| col.clone()).collect())
                .unwrap_or_default();

            let values: Vec<Vec<serde_json::Value>> = result
                .rows
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

            let counters = if req.include_counters || result.delta.is_some() {
                let d = result.delta.unwrap_or_default();
                Some(QueryCounters {
                    contains_updates: d.nodes_created + d.edges_created + d.nodes_deleted + d.edges_deleted > 0,
                    nodes_created: d.nodes_created,
                    nodes_deleted: d.nodes_deleted,
                    relationships_created: d.edges_created,
                    relationships_deleted: d.edges_deleted,
                    properties_set: d.nodes_modified + d.edges_modified,
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
                cache_hit: result.cache_hit,
            };
            (StatusCode::OK, Json(serde_json::to_value(resp).unwrap())).into_response()
        }
        Err(e) => {
            let code = match &e {
                yata_graph::GraphError::Query(_) => "Neo.ClientError.Statement.SyntaxError",
                _ => "Neo.DatabaseError.General.UnknownError",
            };
            let status = match &e {
                yata_graph::GraphError::Query(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            let resp = ErrorResponse {
                errors: vec![ApiError {
                    code: code.to_string(),
                    message: e.to_string(),
                }],
            };
            (status, Json(serde_json::to_value(resp).unwrap())).into_response()
        }
    }
}

async fn handle_tx_begin(Path(_db): Path<String>) -> impl IntoResponse {
    (StatusCode::CREATED, Json(serde_json::json!({"id": "yata-tx-1", "expiresAt": "2099-12-31T23:59:59Z"})))
}

async fn handle_tx_run(
    state: State<QueryApiState>,
    Path((_db, _tx_id)): Path<(String, String)>,
    body: Json<QueryRequest>,
) -> impl IntoResponse {
    execute_cypher(&state.graph, &body).await
}

async fn handle_tx_rollback(Path((_db, _tx_id)): Path<(String, String)>) -> impl IntoResponse {
    StatusCode::ACCEPTED
}

async fn handle_tx_commit(Path((_db, _tx_id)): Path<(String, String)>) -> impl IntoResponse {
    StatusCode::OK
}

async fn handle_discovery() -> impl IntoResponse {
    Json(serde_json::json!({
        "neo4j_version": "5.0.0-yata",
        "neo4j_edition": "community",
        "bolt_routing": "neo4j://localhost:7687",
        "bolt_direct": "bolt://localhost:7687",
        "query": "/db/{databaseName}/query/v2",
        "transaction": "/db/{databaseName}/query/v2/tx",
    }))
}
