//! yata-mdag: Merkle DAG graph sync layer.
//!
//! Provides content-addressed graph storage as a COLD sync tier:
//! - O(changed) sync via Merkle diff (vs O(all) full dataset sync)
//! - Graph federation via AT Protocol (share root CID across PDS)
//! - Cryptographic integrity (every block verified by Blake3)
//! - Automatic dedup (identical content → same CAS entry)
//! - Time-travel (each commit = new root CID, linked via parent chain)
//!
//! NOT for queries — Arrow/CSR (HOT/WARM tiers) handle that.
//! This crate is for sync, storage, federation, and integrity.

pub mod blocks;
pub mod commit;
pub mod deserialize;
pub mod diff;
pub mod error;
pub mod serialize;

pub use blocks::{
    EdgeBlock, GraphRootBlock, LabelEdgeGroup, LabelVertexGroup, SchemaBlock, VertexBlock,
};
pub use commit::CommitLog;
pub use deserialize::load_graph;
pub use diff::{merkle_diff, MdagDiff};
pub use error::{MdagError, Result};
pub use serialize::commit_graph;
