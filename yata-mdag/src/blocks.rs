//! MDAG block types — CBOR-serializable graph data blocks.
//!
//! Hierarchy: GraphRootBlock → LabelVertexGroup/LabelEdgeGroup → VertexBlock/EdgeBlock
//! All blocks stored in CAS keyed by Blake3 hash of their CBOR encoding.

use serde::{Deserialize, Serialize};
use yata_core::Blake3Hash;
use yata_grin::PropValue;

/// A single vertex serialized for CAS storage.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct VertexBlock {
    pub vid: u32,
    pub labels: Vec<String>,
    /// Props sorted by key for deterministic CID.
    pub props: Vec<(String, PropValue)>,
}

/// A single edge serialized for CAS storage.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct EdgeBlock {
    pub edge_id: u32,
    pub src: u32,
    pub dst: u32,
    pub label: String,
    /// Props sorted by key for deterministic CID.
    pub props: Vec<(String, PropValue)>,
}

/// All vertices sharing a single label, referenced by CID.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LabelVertexGroup {
    pub label: String,
    /// Sorted CIDs of VertexBlock entries.
    pub vertex_cids: Vec<Blake3Hash>,
    pub count: u32,
}

/// All edges sharing a single relationship type, referenced by CID.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LabelEdgeGroup {
    pub label: String,
    /// Sorted CIDs of EdgeBlock entries.
    pub edge_cids: Vec<Blake3Hash>,
    pub count: u32,
}

/// Graph schema snapshot.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SchemaBlock {
    pub vertex_labels: Vec<String>,
    pub edge_labels: Vec<String>,
    pub vertex_primary_keys: Vec<(String, String)>,
}

/// Graph root — one per commit. CID of this block identifies the entire graph state.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct GraphRootBlock {
    pub version: u64,
    /// Previous root CID (forms commit chain for time-travel).
    pub parent: Option<Blake3Hash>,
    pub schema_cid: Blake3Hash,
    /// CIDs of LabelVertexGroup blocks, sorted by label.
    pub vertex_groups: Vec<Blake3Hash>,
    /// CIDs of LabelEdgeGroup blocks, sorted by label.
    pub edge_groups: Vec<Blake3Hash>,
    pub vertex_count: u32,
    pub edge_count: u32,
    pub timestamp_ns: i64,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vertex_block_cbor_roundtrip() {
        let block = VertexBlock {
            vid: 42,
            labels: vec!["Person".into()],
            props: vec![
                ("age".into(), PropValue::Int(30)),
                ("name".into(), PropValue::Str("Alice".into())),
            ],
        };
        let bytes = yata_cbor::encode(&block).unwrap();
        let decoded: VertexBlock = yata_cbor::decode(&bytes).unwrap();
        assert_eq!(block, decoded);
    }

    #[test]
    fn test_edge_block_cbor_roundtrip() {
        let block = EdgeBlock {
            edge_id: 7,
            src: 1,
            dst: 2,
            label: "KNOWS".into(),
            props: vec![("weight".into(), PropValue::Float(0.9))],
        };
        let bytes = yata_cbor::encode(&block).unwrap();
        let decoded: EdgeBlock = yata_cbor::decode(&bytes).unwrap();
        assert_eq!(block, decoded);
    }

    #[test]
    fn test_deterministic_cid() {
        // Same data, same order → same CID
        let a = VertexBlock {
            vid: 1,
            labels: vec!["A".into()],
            props: vec![("x".into(), PropValue::Int(1)), ("y".into(), PropValue::Int(2))],
        };
        let b = VertexBlock {
            vid: 1,
            labels: vec!["A".into()],
            props: vec![("x".into(), PropValue::Int(1)), ("y".into(), PropValue::Int(2))],
        };
        assert_eq!(yata_cbor::cbor_cid(&a).unwrap(), yata_cbor::cbor_cid(&b).unwrap());
    }

    #[test]
    fn test_different_props_different_cid() {
        let a = VertexBlock {
            vid: 1, labels: vec!["A".into()],
            props: vec![("x".into(), PropValue::Int(1))],
        };
        let b = VertexBlock {
            vid: 1, labels: vec!["A".into()],
            props: vec![("x".into(), PropValue::Int(2))],
        };
        assert_ne!(yata_cbor::cbor_cid(&a).unwrap(), yata_cbor::cbor_cid(&b).unwrap());
    }

    #[test]
    fn test_root_block_roundtrip() {
        let root = GraphRootBlock {
            version: 1,
            parent: None,
            schema_cid: Blake3Hash::of(b"schema"),
            vertex_groups: vec![Blake3Hash::of(b"vg1")],
            edge_groups: vec![],
            vertex_count: 10,
            edge_count: 0,
            timestamp_ns: 1234567890,
            message: "initial".into(),
        };
        let bytes = yata_cbor::encode(&root).unwrap();
        let decoded: GraphRootBlock = yata_cbor::decode(&bytes).unwrap();
        assert_eq!(root, decoded);
    }
}
