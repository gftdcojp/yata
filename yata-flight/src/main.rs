use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let lance_uri = std::env::var("YATA_LANCE_URI").unwrap_or_else(|_| "./yata-lance".to_string());
    let addr: SocketAddr = std::env::var("YATA_FLIGHT_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:32010".to_string())
        .parse()?;

    tracing::info!(%lance_uri, %addr, "starting yata-flight");

    yata_flight::serve(lance_uri, addr).await
}
