//! yata-lance — LanceDB persistence for yata graph.
//!
//! Uses `lancedb` crate (high-level API) for all storage operations.
//! LanceDB handles versioning, manifest, fragments, compaction, and S3/R2 internally.
//! Schema definitions and graph materialization (read_store) remain yata-specific.

pub mod dataset;
pub mod read_store;
pub mod schema;

pub use dataset::{YataDb, YataTable};
pub use read_store::LanceReadStore;
pub use schema::{
    EDGE_LIVE_IN_TABLE,
    EDGE_LIVE_OUT_TABLE,
    EDGE_LOG_TABLE,
    GRAPH_FORMAT,
    VERTEX_LIVE_TABLE,
    VERTEX_LOG_TABLE,
    edge_live_in_schema,
    edge_live_out_schema,
    edge_log_schema,
    vertex_live_schema,
    vertex_log_schema,
};

// Re-export lancedb for downstream crates
pub use lancedb;
