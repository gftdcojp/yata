//! Arrow Flight SQL endpoint for yata graph engine.
//!
//! HTTP transport: axum HTTP endpoint for CF Container (Workers RPC only).

pub mod executor;
pub mod sql_plan;

#[cfg(feature = "http")]
pub mod http;

#[cfg(test)]
mod tests;

#[cfg(feature = "http")]
pub use http::{FlightSqlHttpState, routes as flight_sql_routes};
