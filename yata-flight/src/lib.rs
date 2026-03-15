mod catalog;
mod codec;
mod error;
mod service;

pub use catalog::YataTableCatalog;
pub use codec::{CypherTicket, ScanTicket};
pub use error::FlightError;
pub use service::YataFlightService;

use arrow_flight::flight_service_server::FlightServiceServer;
use std::net::SocketAddr;

/// Start the Arrow Flight server backed by Lance datasets at `lance_base_uri`.
///
/// Tables exposed:
/// - `yata_messages`
/// - `yata_events`
/// - `yata_objects`
/// - `yata_event_object_edges`
/// - `yata_object_object_edges`
/// - `yata_kv_history`
/// - `yata_blobs`
///
/// When `graph_base_uri` is Some, Cypher queries via `CypherTicket` are enabled,
/// backed by `graph_vertices` / `graph_edges` Lance tables at that URI.
pub async fn serve(
    lance_base_uri: impl Into<String>,
    addr: SocketAddr,
    graph_base_uri: Option<String>,
) -> anyhow::Result<()> {
    let service = match graph_base_uri {
        Some(g) => YataFlightService::new_with_graph(lance_base_uri.into(), g).await?,
        None => YataFlightService::new(lance_base_uri.into()).await?,
    };
    let svc = FlightServiceServer::new(service);
    tracing::info!(%addr, "yata-flight listening");
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve(addr)
        .await?;
    Ok(())
}
