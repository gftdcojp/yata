use tracing_subscriber::EnvFilter;
use yata_server::{Broker, BrokerConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let config = BrokerConfig::default();
    tracing::info!(?config, "starting yata broker");

    let broker = Arc::new(Broker::new(config).await?);
    broker.clone().start_background_tasks().await?;

    tracing::info!("yata broker ready");
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");

    Ok(())
}
