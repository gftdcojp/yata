//! YataFragment: native property graph snapshot format for yata.
//!
//! Arrow columnar format with packed NbrUnit CSR topology.
//! CF Container: blobs map to R2 keys.

pub mod blob;
pub mod convert;
pub mod fragment;
pub mod nbr;
pub mod schema;
#[cfg(test)]
mod tests;

pub use blob::{BlobStore, MemoryBlobStore, ObjectId, ObjectMeta};
pub use fragment::{YataFragment, DEFAULT_CHUNK_ROWS, split_record_batch};
pub use nbr::NbrUnit;
pub use schema::{PropertyDef, PropertyGraphSchema, SchemaEntry};

/// Backward-compat alias.
pub type ArrowFragment = YataFragment;
