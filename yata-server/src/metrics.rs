//! Prometheus metrics for yata broker.
//!
//! Exposes `/metrics` HTTP endpoint on a configurable port (default 9090).
//! Uses the `metrics` crate with `metrics-exporter-prometheus` backend.

use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;

/// Metric name constants.
pub mod names {
    pub const LOG_APPENDS: &str = "yata_log_appends_total";
    pub const LOG_BYTES_WRITTEN: &str = "yata_log_bytes_written_total";
    pub const LOG_SEGMENTS_COMPACTED: &str = "yata_log_segments_compacted_total";
    pub const OCEL_EVENTS_PROJECTED: &str = "yata_ocel_events_projected_total";
    pub const B2_SYNCS: &str = "yata_s3_syncs_total";
}

/// Install the Prometheus exporter and start the HTTP listener.
///
/// Returns `Ok(())` if the exporter was installed successfully.
/// The HTTP server runs on a background task automatically.
pub fn install(port: u16) -> anyhow::Result<()> {
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();

    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .map_err(|e| anyhow::anyhow!("prometheus exporter: {e}"))?;

    tracing::info!(%addr, "prometheus metrics server started");
    Ok(())
}

/// Register metric descriptions (called once at startup).
pub fn describe() {
    use metrics::describe_counter;
    use metrics::describe_histogram;

    describe_counter!(names::LOG_APPENDS, "Total log append operations");
    describe_counter!(
        names::LOG_BYTES_WRITTEN,
        "Total bytes written to log segments"
    );
    describe_counter!(
        names::LOG_SEGMENTS_COMPACTED,
        "Total log segments removed by compaction"
    );
    describe_counter!(names::OCEL_EVENTS_PROJECTED, "Total OCEL events projected");
    describe_counter!(names::B2_SYNCS, "Total B2 sync cycles");
}
