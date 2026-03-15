use std::{env, sync::Arc, time::Duration};

use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value as JValue;
use tokio::{sync::RwLock, time};
use tracing::{error, info, warn};
use yata_cypher::{parse, Executor, Graph, MemoryGraph, NodeRef, RelRef, Row};

// ---- State ---------------------------------------------------------------

struct AppState {
    graph: RwLock<MemoryGraph>,
    tonbo_url: String,
    models_table: String,
    follows_table: String,
}

// ---- API types -----------------------------------------------------------

#[derive(Deserialize)]
struct QueryRequest {
    query: String,
    #[serde(default)]
    params: serde_json::Map<String, JValue>,
}

#[derive(Serialize)]
struct QueryResponse {
    rows: Vec<serde_json::Map<String, JValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    node_count: usize,
    rel_count: usize,
}

// ---- Tonbo REST types ----------------------------------------------------

#[derive(Deserialize)]
struct TonboQueryResp {
    #[serde(default)]
    rows: Vec<serde_json::Map<String, JValue>>,
}

// ---- Handlers ------------------------------------------------------------

async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    let g = state.graph.read().await;
    let nc = g.nodes().len();
    let rc = g.rels().len();
    Json(HealthResponse { status: "ok", node_count: nc, rel_count: rc })
}

async fn query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryRequest>,
) -> (StatusCode, Json<QueryResponse>) {
    let query_ast = match parse(&req.query) {
        Ok(q) => q,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(QueryResponse { rows: Vec::new(), error: Some(e.to_string()) }),
            );
        }
    };

    let mut params: IndexMap<String, yata_cypher::Value> = IndexMap::new();
    for (k, v) in &req.params {
        params.insert(k.clone(), json_to_cypher(v));
    }

    let exec = Executor::with_params(params);
    let mut g = state.graph.write().await;
    match exec.execute(&query_ast, &mut *g) {
        Ok(result) => {
            let rows: Vec<serde_json::Map<String, JValue>> = result
                .rows
                .into_iter()
                .map(|row: Row| {
                    row.0.into_iter().map(|(k, v)| (k, cypher_to_json(v))).collect()
                })
                .collect();
            (StatusCode::OK, Json(QueryResponse { rows, error: None }))
        }
        Err(e) => {
            warn!("cypher error: {e}");
            (
                StatusCode::BAD_REQUEST,
                Json(QueryResponse { rows: Vec::new(), error: Some(e.to_string()) }),
            )
        }
    }
}

async fn sync_now(State(state): State<Arc<AppState>>) -> Json<JValue> {
    match load_graph(&state.tonbo_url, &state.models_table, &state.follows_table).await {
        Ok(g) => {
            let nc = g.nodes().len();
            let rc = g.rels().len();
            *state.graph.write().await = g;
            info!("manual sync: {nc} nodes, {rc} rels");
            Json(serde_json::json!({"ok": true, "nodes": nc, "rels": rc}))
        }
        Err(e) => {
            error!("sync failed: {e}");
            Json(serde_json::json!({"ok": false, "error": e.to_string()}))
        }
    }
}

// ---- Graph loader --------------------------------------------------------

async fn load_graph(
    tonbo_url: &str,
    models_table: &str,
    follows_table: &str,
) -> anyhow::Result<MemoryGraph> {
    let client = reqwest::Client::builder().timeout(Duration::from_secs(30)).build()?;
    let mut g = MemoryGraph::new();

    // ---- models ----
    let resp: TonboQueryResp = client
        .post(format!("{tonbo_url}/v1/table/{models_table}/query/"))
        .json(&serde_json::json!({"limit": 50000, "offset": 0}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    for row in &resp.rows {
        let model_id = row_str(row, "model_id");
        if model_id.is_empty() {
            continue;
        }
        let mut props = IndexMap::new();
        props.insert("model_id".to_string(), yata_cypher::Value::Str(model_id.clone()));
        props.insert("name".to_string(), yata_cypher::Value::Str(row_str(row, "name")));
        props.insert("status".to_string(), yata_cypher::Value::Str(row_str(row, "status")));
        props.insert(
            "tags_json".to_string(),
            yata_cypher::Value::Str(row_str(row, "tags_json")),
        );
        g.add_node(NodeRef {
            id: format!("model:{model_id}"),
            labels: vec!["Model".to_string()],
            props,
        });
    }
    info!("loaded {} model nodes from tonbo", resp.rows.len());

    // ---- follows ----
    let resp: TonboQueryResp = client
        .post(format!("{tonbo_url}/v1/table/{follows_table}/query/"))
        .json(&serde_json::json!({"filter": "deleted = '0'", "limit": 100000, "offset": 0}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let mut follow_count = 0usize;
    for row in &resp.rows {
        let user_id = row_str(row, "user_id");
        let model_id = row_str(row, "model_id");
        if user_id.is_empty() || model_id.is_empty() {
            continue;
        }

        let user_node_id = format!("user:{user_id}");
        if g.node_by_id(&user_node_id).is_none() {
            let mut props = IndexMap::new();
            props.insert("user_id".to_string(), yata_cypher::Value::Str(user_id.clone()));
            g.add_node(NodeRef {
                id: user_node_id.clone(),
                labels: vec!["User".to_string()],
                props,
            });
        }

        let model_node_id = format!("model:{model_id}");
        if g.node_by_id(&model_node_id).is_some() {
            g.add_rel(RelRef {
                id: format!("follows:{user_id}:{model_id}"),
                rel_type: "FOLLOWS".to_string(),
                src: user_node_id,
                dst: model_node_id,
                props: IndexMap::new(),
            });
            follow_count += 1;
        }
    }
    info!("loaded {follow_count} FOLLOWS edges from tonbo");

    Ok(g)
}

fn row_str(row: &serde_json::Map<String, JValue>, key: &str) -> String {
    match row.get(key) {
        Some(JValue::String(s)) => s.clone(),
        Some(v) if !v.is_null() => v.to_string(),
        _ => String::new(),
    }
}

// ---- Value conversion ----------------------------------------------------

fn json_to_cypher(v: &JValue) -> yata_cypher::Value {
    match v {
        JValue::Null => yata_cypher::Value::Null,
        JValue::Bool(b) => yata_cypher::Value::Bool(*b),
        JValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                yata_cypher::Value::Int(i)
            } else {
                yata_cypher::Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        JValue::String(s) => yata_cypher::Value::Str(s.clone()),
        JValue::Array(arr) => {
            yata_cypher::Value::List(arr.iter().map(json_to_cypher).collect())
        }
        JValue::Object(m) => {
            let mut map = IndexMap::new();
            for (k, val) in m {
                map.insert(k.clone(), json_to_cypher(val));
            }
            yata_cypher::Value::Map(map)
        }
    }
}

fn cypher_to_json(v: yata_cypher::Value) -> JValue {
    match v {
        yata_cypher::Value::Null => JValue::Null,
        yata_cypher::Value::Bool(b) => JValue::Bool(b),
        yata_cypher::Value::Int(i) => JValue::Number(i.into()),
        yata_cypher::Value::Float(f) => {
            serde_json::Number::from_f64(f).map(JValue::Number).unwrap_or(JValue::Null)
        }
        yata_cypher::Value::Str(s) => JValue::String(s),
        yata_cypher::Value::List(l) => {
            JValue::Array(l.into_iter().map(cypher_to_json).collect())
        }
        yata_cypher::Value::Map(m) => {
            JValue::Object(m.into_iter().map(|(k, v)| (k, cypher_to_json(v))).collect())
        }
        yata_cypher::Value::Node(n) => {
            let mut m = serde_json::Map::new();
            m.insert("id".to_string(), JValue::String(n.id));
            m.insert(
                "labels".to_string(),
                JValue::Array(n.labels.into_iter().map(JValue::String).collect()),
            );
            for (k, v) in n.props {
                m.insert(k, cypher_to_json(v));
            }
            JValue::Object(m)
        }
        yata_cypher::Value::Rel(r) => {
            let mut m = serde_json::Map::new();
            m.insert("id".to_string(), JValue::String(r.id));
            m.insert("type".to_string(), JValue::String(r.rel_type));
            m.insert("src".to_string(), JValue::String(r.src));
            m.insert("dst".to_string(), JValue::String(r.dst));
            JValue::Object(m)
        }
    }
}

// ---- main ----------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("yata_cypher_server=info".parse()?),
        )
        .init();

    let port = env::var("PORT").unwrap_or_else(|_| "8090".to_string());
    let tonbo_url = env::var("TONBO_BASE_URL")
        .unwrap_or_else(|_| "http://tonbo.spinkube.svc.cluster.local:8084".to_string());
    let models_table =
        env::var("MODELS_TABLE").unwrap_or_else(|_| "shinshi_models".to_string());
    let follows_table =
        env::var("FOLLOWS_TABLE").unwrap_or_else(|_| "shinshi_follows".to_string());
    let sync_interval_secs: u64 = env::var("SYNC_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);

    info!("yata-cypher-server starting on :{port}");
    info!(
        "tonbo_url={tonbo_url} models={models_table} follows={follows_table} sync_every={sync_interval_secs}s"
    );

    let graph = match load_graph(&tonbo_url, &models_table, &follows_table).await {
        Ok(g) => {
            info!("initial load: {} nodes, {} rels", g.nodes().len(), g.rels().len());
            g
        }
        Err(e) => {
            warn!("initial load failed (starting empty): {e}");
            MemoryGraph::new()
        }
    };

    let state = Arc::new(AppState {
        graph: RwLock::new(graph),
        tonbo_url: tonbo_url.clone(),
        models_table: models_table.clone(),
        follows_table: follows_table.clone(),
    });

    // Background sync task
    let bg = state.clone();
    tokio::spawn(async move {
        let mut ticker = time::interval(Duration::from_secs(sync_interval_secs));
        ticker.tick().await; // skip first immediate fire
        loop {
            ticker.tick().await;
            match load_graph(&bg.tonbo_url, &bg.models_table, &bg.follows_table).await {
                Ok(g) => {
                    let nc = g.nodes().len();
                    let rc = g.rels().len();
                    *bg.graph.write().await = g;
                    info!("bg sync: {nc} nodes, {rc} rels");
                }
                Err(e) => warn!("bg sync failed: {e}"),
            }
        }
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/query", post(query))
        .route("/sync", post(sync_now))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    info!("listening on 0.0.0.0:{port}");
    axum::serve(listener, app).await?;

    Ok(())
}
