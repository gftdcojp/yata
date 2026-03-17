//! yata-cdc: Change Data Capture for graph mutations.
//!
//! Converts yata-store WalEntry batches into CdcEvent streams,
//! enabling real-time graph change propagation to external consumers
//! (AT Protocol Firehose, Arrow Flight, etc.).
//!
//! Inspired by GraphScope GART's CDC bridge architecture.

pub mod emitter;

pub use emitter::{CdcEmitter, GraphCdcEvent};
