//! yata-lance — Lance-table-compatible persistence for yata graph.
//!
//! COO sorted graph data stored as typed Arrow IPC tables following Lance conventions:
//! - `vertices` table: (label, rkey, collection, value_b64, repo, updated_at, sensitivity_ord, owner_hash)
//! - `edges` table: (src_label, src_rkey, edge_label, dst_label, dst_rkey)
//! - Immutable fragments (append-only, tombstone via deletion vector)
//! - Versioned manifest (inverted naming, O(1) latest lookup)
//!
//! Uses Arrow IPC File format (same as Lance fragments). Schema-compatible with LanceDB
//! for future native lance crate migration when Arrow version alignment is resolved.
//!
//! Lance crate dependency blocked by: lance 0.20 pins Arrow 53 + chrono >=0.4.41
//! → arrow-arith 53 quarter() ambiguity (fixed in Arrow 54, but lance pins 53).

pub mod schema;
pub mod table;

pub use schema::{VERTICES_SCHEMA, EDGES_SCHEMA};
pub use table::{LanceTable, VertexRow, Fragment};
