//! MDAG block types — graph data blocks stored in CAS.
//!
//! Hierarchy: GraphRootBlock → LabelVertexGroup/LabelEdgeGroup (inline vertices/edges)
//! Data blocks (vertex/edge groups): Arrow IPC columnar format (zero-copy scan, SIMD vectorizable).
//! Metadata blocks (root, schema): CBOR (small, self-describing).
//! All blocks stored in CAS keyed by Blake3 hash.
//!
//! R2 object count optimization: vertices and edges are embedded inline in their
//! label group block. One CAS put per label group (not per vertex/edge).
//! For a graph with L vertex labels and E edge labels, a full commit produces
//! L + E + 1 (schema) + 1 (root) CAS objects total.

use serde::{Deserialize, Serialize};
use yata_core::{Blake3Hash, PartitionId};
use yata_grin::PropValue;

/// A single vertex (embedded inline in LabelVertexGroup).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct VertexBlock {
    pub vid: u32,
    pub labels: Vec<String>,
    /// Props sorted by key for deterministic CID.
    pub props: Vec<(String, PropValue)>,
}

/// A single edge (embedded inline in LabelEdgeGroup).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct EdgeBlock {
    pub edge_id: u32,
    pub src: u32,
    pub dst: u32,
    pub label: String,
    /// Props sorted by key for deterministic CID.
    pub props: Vec<(String, PropValue)>,
}

/// All vertices sharing a single label, stored inline in one CAS blob.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LabelVertexGroup {
    pub label: String,
    /// Inline vertex data (sorted by vid for deterministic CID).
    pub vertices: Vec<VertexBlock>,
    pub count: u32,
}

/// All edges sharing a single relationship type, stored inline in one CAS blob.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LabelEdgeGroup {
    pub label: String,
    /// Inline edge data (sorted by (src, dst, edge_id) for deterministic CID).
    pub edges: Vec<EdgeBlock>,
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
    #[serde(default)]
    pub partition_id: PartitionId,
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
    /// CIDs of PropertyIndexBlock blocks (secondary indexes for federated query).
    #[serde(default)]
    pub index_cids: Vec<Blake3Hash>,
    /// CID of the vector index snapshot (Arrow IPC, persisted via yata-vex).
    #[serde(default)]
    pub vex_cid: Option<Blake3Hash>,
    /// Vineyard ObjectId provenance: label → ObjectId.0 (optional, for Vineyard-based snapshots).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vineyard_ids: Option<std::collections::HashMap<String, u64>>,
    /// DID of the signer (did:key:z6Mk... or did:web:...).
    pub signer_did: String,
    /// Ed25519 signature over serialized block (with signature field zeroed).
    pub signature: Vec<u8>,
}

/// A vertex reference within the index: points to the label group CID + vid offset.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexVertexRef {
    /// CID of the LabelVertexGroup containing this vertex.
    pub group_cid: Blake3Hash,
    /// Vertex label (for quick label filtering without fetching the group).
    pub label: String,
    /// Vertex ID within the CSR.
    pub vid: u32,
}

/// Secondary property index: maps a single property key's values to vertex locations.
/// One CAS blob per indexed property key.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PropertyIndexBlock {
    /// The property key this index covers (e.g. "email", "node_id", "did").
    pub key: String,
    /// Sorted entries: prop_value → Vec<IndexVertexRef>.
    /// BTreeMap serialization is deterministic (sorted keys) → deterministic CID.
    pub entries: Vec<(String, Vec<IndexVertexRef>)>,
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
        let a = VertexBlock {
            vid: 1,
            labels: vec!["A".into()],
            props: vec![
                ("x".into(), PropValue::Int(1)),
                ("y".into(), PropValue::Int(2)),
            ],
        };
        let b = VertexBlock {
            vid: 1,
            labels: vec!["A".into()],
            props: vec![
                ("x".into(), PropValue::Int(1)),
                ("y".into(), PropValue::Int(2)),
            ],
        };
        assert_eq!(
            yata_cbor::cbor_cid(&a).unwrap(),
            yata_cbor::cbor_cid(&b).unwrap()
        );
    }

    #[test]
    fn test_different_props_different_cid() {
        let a = VertexBlock {
            vid: 1,
            labels: vec!["A".into()],
            props: vec![("x".into(), PropValue::Int(1))],
        };
        let b = VertexBlock {
            vid: 1,
            labels: vec!["A".into()],
            props: vec![("x".into(), PropValue::Int(2))],
        };
        assert_ne!(
            yata_cbor::cbor_cid(&a).unwrap(),
            yata_cbor::cbor_cid(&b).unwrap()
        );
    }

    #[test]
    fn test_label_vertex_group_inline_roundtrip() {
        let group = LabelVertexGroup {
            label: "Person".into(),
            vertices: vec![
                VertexBlock {
                    vid: 0,
                    labels: vec!["Person".into()],
                    props: vec![("name".into(), PropValue::Str("Alice".into()))],
                },
                VertexBlock {
                    vid: 1,
                    labels: vec!["Person".into()],
                    props: vec![("name".into(), PropValue::Str("Bob".into()))],
                },
            ],
            count: 2,
        };
        let bytes = yata_cbor::encode(&group).unwrap();
        let decoded: LabelVertexGroup = yata_cbor::decode(&bytes).unwrap();
        assert_eq!(group, decoded);
    }

    #[test]
    fn test_label_edge_group_inline_roundtrip() {
        let group = LabelEdgeGroup {
            label: "KNOWS".into(),
            edges: vec![EdgeBlock {
                edge_id: 0,
                src: 0,
                dst: 1,
                label: "KNOWS".into(),
                props: vec![],
            }],
            count: 1,
        };
        let bytes = yata_cbor::encode(&group).unwrap();
        let decoded: LabelEdgeGroup = yata_cbor::decode(&bytes).unwrap();
        assert_eq!(group, decoded);
    }

    #[test]
    fn test_root_block_roundtrip() {
        let root = GraphRootBlock {
            version: 1,
            partition_id: PartitionId::from(0),
            parent: None,
            schema_cid: Blake3Hash::of(b"schema"),
            vertex_groups: vec![Blake3Hash::of(b"vg1")],
            edge_groups: vec![],
            vertex_count: 10,
            edge_count: 0,
            timestamp_ns: 1234567890,
            message: "initial".into(),
            index_cids: vec![],
            vex_cid: None,
            vineyard_ids: None,
            signer_did: "did:key:z6MkTest".into(),
            signature: vec![0u8; 64],
        };
        let bytes = yata_cbor::encode(&root).unwrap();
        let decoded: GraphRootBlock = yata_cbor::decode(&bytes).unwrap();
        assert_eq!(root, decoded);
    }
}
