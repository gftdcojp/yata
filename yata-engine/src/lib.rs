//! yata-engine: LanceDB-backed graph engine. No persistent CSR. No WAL.

pub mod config;
pub mod engine;
pub mod hints;
pub mod memory_bridge;
pub mod router;

pub use config::TieredEngineConfig;
pub use engine::{CompactionResult, CpmStats, MutationContext, TieredGraphEngine};
