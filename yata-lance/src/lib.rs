//! yata-lance — Lance persistence for yata graph.
//!
//! COO sorted graph data stored as typed tables:
//! - `vertices` table: (label, rkey, collection, value_b64, repo, updated_at, sensitivity_ord, owner_hash)
//! - `edges` table: (src_label, src_rkey, edge_label, dst_label, dst_rkey)
//!
//! Two format paths:
//! - **Lance v2 native** (`native-lance` feature, default): lance-file crate for
//!   columnar encoding (bitpacking, FSST, dictionary). ~3x Shannon efficiency vs Arrow IPC.
//! - **Arrow IPC** (legacy): plain Arrow IPC File format for backward compatibility.

pub mod schema;
pub mod table;

#[cfg(feature = "native-lance")]
pub mod native;

pub use schema::{EDGES_SCHEMA, VERTICES_SCHEMA};
pub use table::{Fragment, LanceTable, VertexRow};
