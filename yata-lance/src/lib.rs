//! yata-lance — Lance format persistence for yata graph.
//!
//! COO sorted graph data stored as Lance tables:
//! - `vertices` table: (label, rkey, collection, value_b64, repo, updated_at, sensitivity_ord, owner_hash)
//! - `edges` table: (src_label, src_rkey, edge_label, dst_label, dst_rkey)
//!
//! Uses Lance built-in: versioning, compaction, deletion vectors, BTree indices.
//! R2/S3 backend via custom ObjectStore adapter wrapping yata-s3 (ureq+rustls, no reqwest).

pub mod adapter;
pub mod schema;
pub mod store;

pub use schema::{VERTICES_SCHEMA, EDGES_SCHEMA};
pub use store::LanceGraphStore;
