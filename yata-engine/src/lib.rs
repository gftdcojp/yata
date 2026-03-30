//! yata-engine: Vineyard-based graph engine — HOT (MutableCSR) + Vineyard (persistent blobs).
//!
//! - Query routing (CSR hot-path)
//! - Query result cache (LRU + TTL + generation invalidation)
//! - Delta write-back (mutation tracking + CSR rebuild)
//! - Vineyard page-in / page-out (lazy label loading)

pub mod arrow_wal;
pub mod cache;
pub mod compaction;
pub mod config;
pub mod distributed;
pub mod engine;
pub mod frontier;
pub mod loader;
pub mod partition_query;
pub mod partition_router;
pub mod router;
pub mod sharded_coordinator;
pub mod wal;

pub use config::{TieredEngineConfig, WalFormat};
pub use engine::{CpmStats, MutationContext, TieredGraphEngine};
pub use partition_query::PartitionQueryMetrics;
pub use partition_router::{PartitionHints, PartitionScope, extract_partition_hints, route};
pub use wal::{WalEntry, WalOp, WalRingBuffer};
