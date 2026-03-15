mod catalog;
mod codec;
mod error;
mod service;

pub use catalog::YataTableCatalog;
pub use codec::ScanTicket;
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
pub async fn serve(lance_base_uri: impl Into<String>, addr: SocketAddr) -> anyhow::Result<()> {
    let service = YataFlightService::new(lance_base_uri.into());
    let svc = FlightServiceServer::new(service);
    tracing::info!(%addr, "yata-flight listening");
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve(addr)
        .await?;
    Ok(())
}
