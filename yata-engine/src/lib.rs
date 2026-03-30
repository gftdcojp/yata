//! yata-engine: Lance-backed graph engine with WAL projection.
//!
//! - Query routing
//! - Query result cache (LRU + TTL + generation invalidation)
//! - Delta write-back (mutation tracking + read-store rebuild)
//! - Lance page-in / page-out (lazy label loading)

pub mod arrow_wal;
pub mod cache;
pub mod compaction;
pub mod config;
pub mod engine;
pub mod hints;
pub mod memory_bridge;
pub mod router;
pub mod sharded_coordinator;
pub mod wal;

pub use config::{TieredEngineConfig, WalFormat};
pub use engine::{CpmStats, MutationContext, TieredGraphEngine};
pub use wal::{WalEntry, WalOp, WalRingBuffer};
