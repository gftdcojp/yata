//! Vineyard-native ArrowFragment format for yata.
//!
//! Implements the v6d.io ArrowFragment blob layout in pure Rust (no vineyardd dependency).
//! CF Container: blobs map to R2 keys.
//!
//! Reference: https://github.com/v6d-io/v6d/blob/main/modules/graph/fragment/arrow_fragment.vineyard-mod

pub mod blob;
pub mod fragment;
pub mod nbr;
pub mod schema;
#[cfg(test)]
mod tests;

pub use blob::{BlobId, BlobStore, MemoryBlobStore, ObjectId, ObjectMeta};
pub use fragment::ArrowFragment;
pub use nbr::NbrUnit;
pub use schema::{PropertyDef, PropertyGraphSchema, SchemaEntry};
