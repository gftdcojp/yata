//! yata-mdag: Merkle DAG graph sync layer.
//!
//! NOTE: Authoritative source is `wproto::mdag`. This crate retains its own
//! implementation for backward compatibility (no circular dependency).
//! New code should prefer `wproto::mdag` when available.

pub mod arrow_codec;
pub mod blocks;
pub mod commit;
pub mod deserialize;
pub mod diff;
pub mod error;
pub mod index;
pub mod repo;
pub mod serialize;
pub mod verify;

pub use blocks::{
    EdgeBlock, GraphRootBlock, IndexVertexRef, LabelEdgeGroup, LabelVertexGroup,
    PropertyIndexBlock, SchemaBlock, VertexBlock,
};
pub use commit::{CommitLog, HeadPusher};
pub use deserialize::{
    RootIndex, load_edge_label, load_graph, load_graph_to_vineyard, load_graph_with_partition,
    load_root_index, load_vertex_label,
};
pub use diff::{MdagDiff, merkle_diff};
pub use error::{MdagError, Result};
pub use index::{FederatedApp, FederatedIndex, PropertyIndex};
pub use repo::{RepoRegistry, RepoScope};
pub use serialize::{
    commit_graph, commit_graph_from_vineyard, commit_graph_indexed, delta_commit_graph,
    delta_commit_graph_cached,
};
pub use verify::{sign_root, verify_root};
