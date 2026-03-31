//! yata-engine: LanceDB-backed graph engine. No persistent CSR. No WAL.

pub mod arrow_wal;
pub mod config;
pub mod engine;
pub mod memory_bridge;
pub mod router;
pub mod wal;

pub use config::TieredEngineConfig;
pub use engine::{CompactionResult, CpmStats, MutationContext, TieredGraphEngine};
pub use wal::{WalEntry, WalOp};
