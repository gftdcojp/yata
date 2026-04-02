use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use yata_engine::{TieredGraphEngine, config::TieredEngineConfig};
use yata_server::rest;

// AVX-512 linker stub for zig cross-compile (lance-linalg FlatIndex)
#[path = "avx512_stub.rs"]
mod avx512_stub;

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

    let shutdown_engine = engine.clone();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            // Listen for both SIGTERM (Container) and Ctrl+C (dev)
            let ctrl_c = tokio::signal::ctrl_c();
            #[cfg(unix)]
            let sigterm = async {
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler")
                    .recv()
                    .await;
            };
            #[cfg(not(unix))]
            let sigterm = std::future::pending::<()>();
            tokio::select! {
                _ = ctrl_c => tracing::info!("received Ctrl+C"),
                _ = sigterm => tracing::info!("received SIGTERM"),
            }
        })
        .await?;

    // Graceful shutdown: flush all pending writes to R2 via compaction
    tracing::info!("graceful shutdown: triggering final compaction...");
    match shutdown_engine.trigger_compaction() {
        Ok(stats) => tracing::info!(
            input = stats.input_entries,
            output = stats.output_entries,
            "graceful shutdown: final compaction complete"
        ),
        Err(e) => tracing::error!(error = %e, "graceful shutdown: final compaction FAILED"),
    }
    tracing::info!("yata-server shutdown complete");

    Ok(())
}
