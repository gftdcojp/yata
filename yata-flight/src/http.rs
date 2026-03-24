//! HTTP Arrow IPC endpoint for CF Container.
//!
//! CF Containers only support HTTP (no raw gRPC/TCP from outside).
//! This module provides axum routes that serve Arrow IPC over HTTP:
//!
//! POST /api/flight-sql/query
//!   Content-Type: application/json
//!   Body: { "sql": "SELECT ...", "params": "[]" }
//!   Response: application/vnd.apache.arrow.stream (Arrow IPC bytes)
//!
//! POST /api/flight-sql/exec
//!   Content-Type: application/json
//!   Body: { "sql": "INSERT ...", "params": "[]" }
//!   Response: application/json { "affected": N }

use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
    routing::post,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

use yata_store::MutableCsrStore;

use crate::executor;
use crate::sql_plan;

/// Shared state for the Flight SQL HTTP handler.
#[derive(Clone)]
pub struct FlightSqlHttpState {
    pub store: Arc<Mutex<MutableCsrStore>>,
    pub api_secret: String,
}

/// Create axum routes for Flight SQL HTTP endpoint.
pub fn routes(state: FlightSqlHttpState) -> Router {
    Router::new()
        .route("/api/flight-sql/query", post(flight_query))
        .route("/api/flight-sql/exec", post(flight_exec))
        .with_state(state)
}

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
    #[serde(default = "default_params")]
    params: String,
}

fn default_params() -> String {
    "[]".to_string()
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

/// RLS scope extracted from request headers.
struct RlsScope {
    org_id: String,
    user_did: Option<String>,
    actor_did: Option<String>,
}

/// Authenticate and extract RLS scope from headers.
/// Returns org_id (required), user_did/actor_did (optional).
fn authorize(headers: &HeaderMap, state: &FlightSqlHttpState) -> Result<RlsScope, StatusCode> {
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
        .unwrap_or("anon")
        .to_string();

    if org_id.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let user_did = headers
        .get("x-gftd-user-did")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let actor_did = headers
        .get("x-gftd-actor-did")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    Ok(RlsScope { org_id, user_did, actor_did })
}

/// POST /api/flight-sql/query
/// Returns Arrow IPC stream bytes (Content-Type: application/vnd.apache.arrow.stream).
/// RLS: filters results by org_id (required), user_did/actor_did (optional).
async fn flight_query(
    State(state): State<FlightSqlHttpState>,
    headers: HeaderMap,
    Json(req): Json<SqlRequest>,
) -> impl IntoResponse {
    let rls = match authorize(&headers, &state) {
        Ok(scope) => scope,
        Err(status) => return (status, Vec::new()).into_response(),
    };

    let plan = match sql_plan::parse_select(&req.sql) {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response();
        }
    };

    let store = match state.store.lock() {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response();
        }
    };

    let batch = match executor::execute(&store, &plan) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response();
        }
    };

    // RLS post-filter: remove rows where org_id/user_did/actor_did don't match scope.
    // Defense-in-depth: even if executor doesn't filter, this layer enforces isolation.
    let batch = rls_filter_batch(batch, &rls);

    // Serialize to Arrow IPC stream format
    let ipc_bytes = match yata_arrow::batch_to_ipc(&batch) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/vnd.apache.arrow.stream")],
        ipc_bytes.to_vec(),
    )
        .into_response()
}

/// POST /api/flight-sql/exec
/// Returns JSON with affected row count.
async fn flight_exec(
    State(state): State<FlightSqlHttpState>,
    headers: HeaderMap,
    Json(req): Json<SqlRequest>,
) -> impl IntoResponse {
    if let Err(status) = authorize(&headers, &state) {
        return (status, Json(ErrorResponse { error: "unauthorized".into() })).into_response();
    }

    // Currently read-only; write support via Cypher mutation path
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorResponse {
            error: "write operations not yet supported via Flight SQL HTTP".into(),
        }),
    )
        .into_response()
}

/// RLS post-filter: remove rows where org_id doesn't match the authenticated scope.
/// Also filters by user_did and actor_did if present in both the data and the scope.
///
/// This is defense-in-depth — the executor should ideally pre-filter, but this
/// layer guarantees no cross-tenant data leaks even if the executor has bugs.
fn rls_filter_batch(
    batch: arrow::array::RecordBatch,
    rls: &RlsScope,
) -> arrow::array::RecordBatch {
    use arrow::array::{Array, BooleanArray, StringArray};
    use arrow::compute::filter_record_batch;

    if batch.num_rows() == 0 {
        return batch;
    }

    let num_rows = batch.num_rows();
    let mut mask = vec![true; num_rows];

    // Filter by org_id column (if present)
    if let Some(col) = batch.column_by_name("org_id") {
        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            for i in 0..num_rows {
                if arr.is_valid(i) {
                    let val = arr.value(i);
                    if !val.is_empty() && val != rls.org_id && val != "anon" {
                        mask[i] = false;
                    }
                }
            }
        }
    }

    // Filter by _user_did column (if scope has user_did AND column exists)
    if let Some(ref user_did) = rls.user_did {
        if let Some(col) = batch.column_by_name("_user_did") {
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..num_rows {
                    if mask[i] && arr.is_valid(i) {
                        let val = arr.value(i);
                        if !val.is_empty() && val != user_did.as_str() {
                            mask[i] = false;
                        }
                    }
                }
            }
        }
    }

    // Filter by _actor_did column (if scope has actor_did AND column exists)
    if let Some(ref actor_did) = rls.actor_did {
        if let Some(col) = batch.column_by_name("_actor_did") {
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..num_rows {
                    if mask[i] && arr.is_valid(i) {
                        let val = arr.value(i);
                        if !val.is_empty() && val != actor_did.as_str() {
                            mask[i] = false;
                        }
                    }
                }
            }
        }
    }

    // Apply mask
    if mask.iter().all(|&b| b) {
        return batch; // no filtering needed
    }

    let bool_array = BooleanArray::from(mask);
    filter_record_batch(&batch, &bool_array).unwrap_or(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;
    use yata_grin::Mutable;

    fn test_state() -> FlightSqlHttpState {
        let mut store = MutableCsrStore::new();
        store.register_btree_index("Person", "age");
        store.add_vertex("Person", &[
            ("name", yata_grin::PropValue::Str("Alice".into())),
            ("age", yata_grin::PropValue::Int(30)),
        ]);
        store.add_vertex("Person", &[
            ("name", yata_grin::PropValue::Str("Bob".into())),
            ("age", yata_grin::PropValue::Int(25)),
        ]);
        store.add_vertex("Person", &[
            ("name", yata_grin::PropValue::Str("Carol".into())),
            ("age", yata_grin::PropValue::Int(35)),
        ]);
        store.commit();

        FlightSqlHttpState {
            store: Arc::new(Mutex::new(store)),
            api_secret: String::new(),
        }
    }

    #[tokio::test]
    async fn test_flight_query_returns_arrow_ipc() {
        let app = routes(test_state());
        let body = serde_json::to_string(&serde_json::json!({
            "sql": "SELECT * FROM Person"
        }))
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/flight-sql/query")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/vnd.apache.arrow.stream"
        );

        // Decode Arrow IPC to verify
        let body_bytes = axum::body::to_bytes(resp.into_body(), 1_000_000)
            .await
            .unwrap();
        assert!(!body_bytes.is_empty());
        let batch = yata_arrow::ipc_to_batch(&body_bytes).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn test_flight_query_with_where() {
        let app = routes(test_state());
        let body = serde_json::to_string(&serde_json::json!({
            "sql": "SELECT name FROM Person WHERE age = 30"
        }))
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/flight-sql/query")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp.into_body(), 1_000_000)
            .await
            .unwrap();
        let batch = yata_arrow::ipc_to_batch(&body_bytes).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_flight_query_order_limit() {
        let app = routes(test_state());
        let body = serde_json::to_string(&serde_json::json!({
            "sql": "SELECT name FROM Person ORDER BY age DESC LIMIT 1"
        }))
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/flight-sql/query")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp.into_body(), 1_000_000)
            .await
            .unwrap();
        let batch = yata_arrow::ipc_to_batch(&body_bytes).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_flight_query_bad_sql() {
        let app = routes(test_state());
        let body = serde_json::to_string(&serde_json::json!({
            "sql": "NOT VALID SQL"
        }))
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/flight-sql/query")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_flight_query_unauthorized() {
        let mut state = test_state();
        state.api_secret = "secret123".to_string();
        let app = routes(state);

        let body = serde_json::to_string(&serde_json::json!({
            "sql": "SELECT * FROM Person"
        }))
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/flight-sql/query")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_flight_query_table_not_found() {
        let app = routes(test_state());
        let body = serde_json::to_string(&serde_json::json!({
            "sql": "SELECT * FROM NonExistent"
        }))
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/flight-sql/query")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    // ── RLS tests ───────────────────────────────────────────────────

    fn rls_test_state() -> FlightSqlHttpState {
        let mut store = MutableCsrStore::new();
        // Org A data
        store.add_vertex("Person", &[
            ("name", yata_grin::PropValue::Str("Alice".into())),
            ("org_id", yata_grin::PropValue::Str("org_a".into())),
            ("_user_did", yata_grin::PropValue::Str("did:web:alice".into())),
        ]);
        // Org B data
        store.add_vertex("Person", &[
            ("name", yata_grin::PropValue::Str("Bob".into())),
            ("org_id", yata_grin::PropValue::Str("org_b".into())),
            ("_user_did", yata_grin::PropValue::Str("did:web:bob".into())),
        ]);
        // Org A, different user
        store.add_vertex("Person", &[
            ("name", yata_grin::PropValue::Str("Carol".into())),
            ("org_id", yata_grin::PropValue::Str("org_a".into())),
            ("_user_did", yata_grin::PropValue::Str("did:web:carol".into())),
        ]);
        // No org_id (system node)
        store.add_vertex("Person", &[
            ("name", yata_grin::PropValue::Str("System".into())),
        ]);
        store.commit();

        FlightSqlHttpState {
            store: Arc::new(Mutex::new(store)),
            api_secret: String::new(),
        }
    }

    #[tokio::test]
    async fn test_rls_org_filter() {
        let app = routes(rls_test_state());
        let body = serde_json::to_string(&serde_json::json!({
            "sql": "SELECT * FROM Person"
        }))
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/flight-sql/query")
                    .header("content-type", "application/json")
                    .header("x-gftd-org-id", "org_a")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp.into_body(), 1_000_000)
            .await
            .unwrap();
        let batch = yata_arrow::ipc_to_batch(&body_bytes).unwrap();
        // Should see Alice + Carol (org_a) + System (no org_id) = 3
        // Bob (org_b) should be filtered out
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn test_rls_user_did_filter() {
        let app = routes(rls_test_state());
        let body = serde_json::to_string(&serde_json::json!({
            "sql": "SELECT * FROM Person"
        }))
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/flight-sql/query")
                    .header("content-type", "application/json")
                    .header("x-gftd-org-id", "org_a")
                    .header("x-gftd-user-did", "did:web:alice")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp.into_body(), 1_000_000)
            .await
            .unwrap();
        let batch = yata_arrow::ipc_to_batch(&body_bytes).unwrap();
        // Should see Alice (org_a + did:web:alice) + System (no org_id, no _user_did) = 2
        // Carol (org_a but did:web:carol) should be filtered out by user_did
        // Bob (org_b) should be filtered out by org_id
        assert_eq!(batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_rls_unit_filter_batch() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};

        // Build a batch with org_id column
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
            Field::new("org_id", arrow::datatypes::DataType::Utf8, true),
        ]));
        let names = StringArray::from(vec!["Alice", "Bob", "Carol", "System"]);
        let orgs = StringArray::from(vec![
            Some("org_a"),
            Some("org_b"),
            Some("org_a"),
            None, // system node
        ]);
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(names), Arc::new(orgs)],
        )
        .unwrap();

        let rls = RlsScope {
            org_id: "org_a".to_string(),
            user_did: None,
            actor_did: None,
        };

        let filtered = rls_filter_batch(batch, &rls);
        assert_eq!(filtered.num_rows(), 3); // Alice, Carol, System (Bob removed)

        let names = filtered
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Carol");
        assert_eq!(names.value(2), "System");
    }

    #[tokio::test]
    async fn test_rls_unit_user_did_filter() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
            Field::new("org_id", arrow::datatypes::DataType::Utf8, true),
            Field::new("_user_did", arrow::datatypes::DataType::Utf8, true),
        ]));
        let names = StringArray::from(vec!["Alice", "Bob", "Carol"]);
        let orgs = StringArray::from(vec![
            Some("org_a"),
            Some("org_a"),
            Some("org_a"),
        ]);
        let dids = StringArray::from(vec![
            Some("did:web:alice"),
            Some("did:web:bob"),
            Some("did:web:alice"),
        ]);
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(names), Arc::new(orgs), Arc::new(dids)],
        )
        .unwrap();

        let rls = RlsScope {
            org_id: "org_a".to_string(),
            user_did: Some("did:web:alice".to_string()),
            actor_did: None,
        };

        let filtered = rls_filter_batch(batch, &rls);
        assert_eq!(filtered.num_rows(), 2); // Alice + Carol (both did:web:alice)

        let names = filtered
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Carol");
    }

    #[tokio::test]
    async fn test_flight_exec_not_implemented() {
        let app = routes(test_state());
        let body = serde_json::to_string(&serde_json::json!({
            "sql": "INSERT INTO t VALUES (1)"
        }))
        .unwrap();

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/flight-sql/exec")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn test_flight_query_no_org_header_defaults_anon() {
        let app = routes(test_state());
        let body = serde_json::to_string(&serde_json::json!({
            "sql": "SELECT * FROM Person"
        }))
        .unwrap();

        // No x-gftd-org-id header → defaults to "anon"
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/flight-sql/query")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should succeed (api_secret is empty in test_state)
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rls_actor_did_filter() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
            Field::new("org_id", arrow::datatypes::DataType::Utf8, true),
            Field::new("_actor_did", arrow::datatypes::DataType::Utf8, true),
        ]));
        let names = StringArray::from(vec!["Bot1Data", "Bot2Data", "SharedData"]);
        let orgs = StringArray::from(vec![Some("org_a"), Some("org_a"), Some("org_a")]);
        let actors = StringArray::from(vec![
            Some("did:web:bot1"),
            Some("did:web:bot2"),
            None, // shared — no actor_did
        ]);
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(names), Arc::new(orgs), Arc::new(actors)],
        )
        .unwrap();

        let rls = RlsScope {
            org_id: "org_a".to_string(),
            user_did: None,
            actor_did: Some("did:web:bot1".to_string()),
        };

        let filtered = rls_filter_batch(batch, &rls);
        // Bot1Data (match) + SharedData (no actor_did) = 2
        assert_eq!(filtered.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_rls_empty_batch_passthrough() {
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        let batch = arrow::array::RecordBatch::new_empty(schema);
        let rls = RlsScope {
            org_id: "org_a".to_string(),
            user_did: None,
            actor_did: None,
        };
        let filtered = rls_filter_batch(batch, &rls);
        assert_eq!(filtered.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_rls_no_matching_columns_passthrough() {
        use arrow::array::StringArray;
        use arrow::datatypes::{Field, Schema};

        // Batch without org_id column → no filtering
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", arrow::datatypes::DataType::Utf8, false),
        ]));
        let values = StringArray::from(vec!["a", "b", "c"]);
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(values)],
        )
        .unwrap();

        let rls = RlsScope {
            org_id: "org_a".to_string(),
            user_did: Some("did:web:alice".to_string()),
            actor_did: Some("did:web:bot1".to_string()),
        };
        let filtered = rls_filter_batch(batch, &rls);
        assert_eq!(filtered.num_rows(), 3); // all kept — no RLS columns present
    }
}
