//! yata-engine: Tiered graph engine — HOT (MutableCSR) → WARM (Lance) → COLD (B2).
//!
//! Absorbs all graph intelligence previously in magatama-host/graph_host.rs:
//! - Query routing (CSR hot-path vs Lance warm path)
//! - Query result cache (LRU + TTL + generation invalidation)
//! - Delta write-back (mutation tracking + Lance persist + CSR rebuild)
//! - RLS filtering (org_id scoping)
//! - Lance SQL pushdown (Cypher AST hint extraction)

pub mod config;
pub mod cache;
pub mod router;
pub mod rls;
pub mod loader;
pub mod writer;
pub mod engine;

pub use config::TieredEngineConfig;
pub use engine::TieredGraphEngine;
