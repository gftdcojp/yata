//! Arrow Flight SQL endpoint for yata graph engine.
//!
//! Two transport modes:
//! - `http` (default): axum HTTP endpoint for CF Container (no gRPC/TCP needed)
//! - `grpc` (opt-in): tonic gRPC Flight SQL for native deployment

pub mod executor;
pub mod sql_plan;

#[cfg(feature = "http")]
pub mod http;

#[cfg(feature = "grpc")]
pub mod service;

#[cfg(test)]
mod tests;

#[cfg(feature = "http")]
pub use http::{FlightSqlHttpState, routes as flight_sql_routes};

#[cfg(feature = "grpc")]
pub use service::YataFlightSqlService;
