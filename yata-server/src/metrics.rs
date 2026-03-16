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
    pub const KV_OPS: &str = "yata_kv_ops_total";
    pub const KV_ENTRIES: &str = "yata_kv_entries_gauge";
    pub const KV_TTL_REAPED: &str = "yata_kv_ttl_reaped_total";
    pub const LANCE_FLUSHES: &str = "yata_lance_flushes_total";
    pub const LANCE_ROWS_WRITTEN: &str = "yata_lance_rows_written_total";
    pub const LANCE_FLUSH_DURATION: &str = "yata_lance_flush_duration_seconds";
    pub const NATS_ARROW_PUBLISHED: &str = "yata_nats_arrow_published_total";
    pub const NATS_ARROW_CONSUMED: &str = "yata_nats_arrow_consumed_total";
    pub const NATS_LANCE_WRITE_DURATION: &str = "yata_nats_lance_write_duration_seconds";
    pub const FLIGHT_REQUESTS: &str = "yata_flight_requests_total";
    pub const FLIGHT_REQUEST_DURATION: &str = "yata_flight_request_duration_seconds";
    pub const LOG_SEGMENTS_COMPACTED: &str = "yata_log_segments_compacted_total";
    pub const OCEL_EVENTS_PROJECTED: &str = "yata_ocel_events_projected_total";
    pub const B2_SYNCS: &str = "yata_b2_syncs_total";
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
    use metrics::describe_gauge;
    use metrics::describe_histogram;

    describe_counter!(names::LOG_APPENDS, "Total log append operations");
    describe_counter!(names::LOG_BYTES_WRITTEN, "Total bytes written to log segments");
    describe_counter!(names::KV_OPS, "Total KV operations by type");
    describe_gauge!(names::KV_ENTRIES, "Current number of KV entries per bucket");
    describe_counter!(names::KV_TTL_REAPED, "Total KV entries reaped by TTL expiry");
    describe_counter!(names::LANCE_FLUSHES, "Total Lance flush cycles");
    describe_counter!(names::LANCE_ROWS_WRITTEN, "Total rows written to Lance tables");
    describe_histogram!(names::LANCE_FLUSH_DURATION, "Lance flush duration in seconds");
    describe_counter!(names::NATS_ARROW_PUBLISHED, "Total Arrow batches published to NATS");
    describe_counter!(names::NATS_ARROW_CONSUMED, "Total Arrow batches consumed from NATS");
    describe_histogram!(names::NATS_LANCE_WRITE_DURATION, "NatsLanceWriter flush duration in seconds");
    describe_counter!(names::FLIGHT_REQUESTS, "Total Arrow Flight requests");
    describe_histogram!(names::FLIGHT_REQUEST_DURATION, "Arrow Flight request duration in seconds");
    describe_counter!(names::LOG_SEGMENTS_COMPACTED, "Total log segments removed by compaction");
    describe_counter!(names::OCEL_EVENTS_PROJECTED, "Total OCEL events projected");
    describe_counter!(names::B2_SYNCS, "Total B2 sync cycles");
}
