use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use yata_engine::{TieredGraphEngine, config::TieredEngineConfig};
use yata_server::rest;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    // Prometheus metrics
    let metrics_port: u16 = std::env::var("YATA_METRICS_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9090);
    if let Err(e) = yata_server::metrics::install(metrics_port) {
        tracing::warn!("prometheus metrics disabled: {e}");
    } else {
        yata_server::metrics::describe();
    }

    // Build TieredGraphEngine from env vars
    let data_dir = std::env::var("YATA_DATA_DIR").unwrap_or_else(|_| "/data/yata".into());
    // TieredGraphEngine auto-builds S3 client from YATA_S3_* env vars on first query.
    // R2 page-in happens lazily via ensure_labels().
    let engine = Arc::new(TieredGraphEngine::new(TieredEngineConfig::default(), &data_dir));
    tracing::info!(%data_dir, "TieredGraphEngine initialized");

    // REST API server
    let rest_port: u16 = std::env::var("YATA_REST_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8081);

    let readonly = std::env::var("YATA_READONLY").unwrap_or_default() == "true";
    if readonly {
        tracing::info!("read-only mode: write endpoints (mergeRecord, compact) disabled");
    }

    let state = rest::YataRestState {
        graph: engine.clone(),
        readonly,
    };
    let router = rest::router(state);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], rest_port));
    tracing::info!(%addr, "yata-server REST API listening");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(async { tokio::signal::ctrl_c().await.ok(); })
        .await?;

    Ok(())
}
