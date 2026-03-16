//! yata-gateway — standalone Bolt + Arrow Flight SQL + Neo4j Query API gateway.
//!
//! Starts:
//!   1. yata Broker (PVC data + B2 tiered sync)
//!   2. Arrow Flight SQL server (default :32010)
//!   3. Bolt v4 server (default :7687)
//!   4. Neo4j Query API v2 HTTP server (default :7474)
//!   5. Prometheus metrics (default :9090)

mod query_api;

use std::net::SocketAddr;
use std::sync::Arc;

use tracing_subscriber::EnvFilter;
use yata_server::{Broker, BrokerConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    // ── Config ───────────────────────────────────────────────────────────────
    let config = load_config()?;
    tracing::info!(?config, "starting yata-gateway");

    // ── Prometheus metrics ───────────────────────────────────────────────────
    let metrics_addr: SocketAddr = std::env::var("YATA_METRICS_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:9090".to_string())
        .parse()?;
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    let handle = builder
        .with_http_listener(metrics_addr)
        .install_recorder()?;
    tracing::info!(%metrics_addr, "prometheus metrics");

    // ── Broker ───────────────────────────────────────────────────────────────
    let broker = Arc::new(Broker::new(config.clone()).await?);
    broker.clone().start_background_tasks().await?;
    tracing::info!("yata broker ready");

    // ── Arrow Flight SQL ─────────────────────────────────────────────────────
    let flight_addr: SocketAddr = std::env::var("YATA_FLIGHT_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:32010".to_string())
        .parse()?;
    let lance_uri = config.lance_uri.clone();
    let graph_uri = config.graph_uri.clone();
    tokio::spawn(async move {
        if let Err(e) = yata_flight::serve(lance_uri, flight_addr, graph_uri).await {
            tracing::error!("flight server error: {e}");
        }
    });

    if let Some(ref graph) = broker.graph {
        let graph = graph.clone();

        // ── Bolt v4 ──────────────────────────────────────────────────────────
        let bolt_addr: SocketAddr = std::env::var("YATA_BOLT_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:7687".to_string())
            .parse()?;
        let bolt_graph = graph.clone();
        tokio::spawn(async move {
            if let Err(e) = yata_bolt::serve(bolt_graph, bolt_addr).await {
                tracing::error!("bolt server error: {e}");
            }
        });

        // ── Neo4j Query API v2 (HTTP) ────────────────────────────────────────
        let http_addr: SocketAddr = std::env::var("YATA_HTTP_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:7474".to_string())
            .parse()?;
        let app = query_api::router(graph);
        tracing::info!(%http_addr, "neo4j query api v2 listening");
        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!("http server error: {e}");
            }
        });
    } else {
        tracing::warn!("graph_uri not set — bolt + query api disabled");
    }

    tracing::info!("yata-gateway ready");
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    drop(handle);

    Ok(())
}

fn load_config() -> anyhow::Result<BrokerConfig> {
    if let Ok(path) = std::env::var("YATA_CONFIG") {
        let content = std::fs::read_to_string(&path)?;
        let config: BrokerConfig = toml::from_str(&content)?;
        return Ok(config);
    }

    let mut config = BrokerConfig::default();

    if let Ok(v) = std::env::var("YATA_DATA_DIR") {
        config.data_dir = v.into();
    }
    if let Ok(v) = std::env::var("YATA_LANCE_URI") {
        config.lance_uri = v;
    }
    if let Ok(v) = std::env::var("YATA_GRAPH_URI") {
        config.graph_uri = Some(v);
    }
    if let Ok(v) = std::env::var("YATA_B2_ENDPOINT") {
        config.b2.endpoint = v;
    }
    if let Ok(v) = std::env::var("YATA_B2_BUCKET") {
        config.b2.bucket = v;
    }
    if let Ok(v) = std::env::var("YATA_B2_KEY_ID") {
        config.b2.key_id = v;
    }
    if let Ok(v) = std::env::var("YATA_B2_APPLICATION_KEY") {
        config.b2.application_key = v;
    }
    if let Ok(v) = std::env::var("YATA_B2_REGION") {
        config.b2.region = v;
    }
    if let Ok(v) = std::env::var("YATA_B2_PREFIX") {
        config.b2.prefix = v;
    }

    Ok(config)
}
