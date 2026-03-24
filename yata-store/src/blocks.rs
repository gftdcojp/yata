//! Graph data block types for Arrow IPC serialization.
//!
//! These types represent per-label vertex and edge groups stored as
//! Arrow IPC blobs in Vineyard / R2 snapshots.

use serde::{Deserialize, Serialize};
use yata_grin::PropValue;

/// A single vertex (embedded inline in LabelVertexGroup).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct VertexBlock {
    pub vid: u32,
    pub labels: Vec<String>,
    /// Props sorted by key for deterministic encoding.
    pub props: Vec<(String, PropValue)>,
}

/// A single edge (embedded inline in LabelEdgeGroup).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct EdgeBlock {
    pub edge_id: u32,
    pub src: u32,
    pub dst: u32,
    pub label: String,
    /// Props sorted by key for deterministic encoding.
    pub props: Vec<(String, PropValue)>,
}

/// All vertices sharing a single label, stored inline in one Arrow IPC blob.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LabelVertexGroup {
    pub label: String,
    /// Inline vertex data (sorted by vid for deterministic encoding).
    pub vertices: Vec<VertexBlock>,
    pub count: u32,
}

/// All edges sharing a single relationship type, stored inline in one Arrow IPC blob.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LabelEdgeGroup {
    pub label: String,
    /// Inline edge data (sorted by (src, dst, edge_id) for deterministic encoding).
    pub edges: Vec<EdgeBlock>,
    pub count: u32,
}
