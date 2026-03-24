//! MDAG → Graph deserialization: root CID → MutableCsrStore.
//! Data blocks: Arrow IPC (detected by `ARW\x01` magic). Metadata blocks: CBOR.

use std::collections::HashMap;
use yata_cas::CasStore;
use yata_core::{Blake3Hash, PartitionId};
use yata_grin::{Mutable, PropValue};
use yata_store::MutableCsrStore;

use crate::blocks::*;
use crate::error::{MdagError, Result};

/// Load a graph from MDAG CAS blocks into a fresh MutableCsrStore.
/// Fetches only label group blobs (not individual vertices/edges — they're inline).
pub async fn load_graph(root_cid: &Blake3Hash, cas: &dyn CasStore) -> Result<MutableCsrStore> {
    let root: GraphRootBlock = fetch_block(cas, root_cid).await?;
    load_graph_from_root(root, cas).await
}

/// Load a graph from MDAG CAS blocks into a fresh MutableCsrStore for a specific partition.
pub async fn load_graph_with_partition(
    root_cid: &Blake3Hash,
    cas: &dyn CasStore,
    partition_id: PartitionId,
) -> Result<MutableCsrStore> {
    let mut root: GraphRootBlock = fetch_block(cas, root_cid).await?;
    root.partition_id = partition_id;
    load_graph_from_root(root, cas).await
}

async fn load_graph_from_root(root: GraphRootBlock, cas: &dyn CasStore) -> Result<MutableCsrStore> {
    let _schema: SchemaBlock = fetch_block(cas, &root.schema_cid).await?;

    let mut store = MutableCsrStore::new_with_partition_id(root.partition_id);
    let mut vid_map: HashMap<u32, u32> = HashMap::new();

    // ── Load vertices (Arrow IPC or legacy CBOR) ──────────────────────────
    for group_cid in &root.vertex_groups {
        let group = fetch_vertex_group(cas, group_cid).await?;
        for vb in &group.vertices {
            let props: Vec<(&str, PropValue)> = vb
                .props
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            let new_vid = store.add_vertex_with_labels(&vb.labels, &props);
            vid_map.insert(vb.vid, new_vid);
        }
    }

    // ── Load edges (Arrow IPC or legacy CBOR) ───────────────────────────
    for group_cid in &root.edge_groups {
        let group = fetch_edge_group(cas, group_cid).await?;
        for eb in &group.edges {
            let src = vid_map.get(&eb.src).copied().unwrap_or(eb.src);
            let dst = vid_map.get(&eb.dst).copied().unwrap_or(eb.dst);
            let props: Vec<(&str, PropValue)> = eb
                .props
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            store.add_edge(src, dst, &eb.label, &props);
        }
    }

    store.commit();
    Ok(store)
}

/// Load only the root block and build a label→CID map (no vertex/edge data fetched).
/// Used for lazy label-partitioned CSR loading.
pub async fn load_root_index(root_cid: &Blake3Hash, cas: &dyn CasStore) -> Result<RootIndex> {
    let root: GraphRootBlock = fetch_block(cas, root_cid).await?;
    let schema: SchemaBlock = fetch_block(cas, &root.schema_cid).await?;

    // Use schema label order to map CIDs (avoids fetching full data blocks).
    // Falls back to fetching group headers if counts don't match.
    let mut vertex_label_cids = HashMap::new();
    if schema.vertex_labels.len() == root.vertex_groups.len() {
        for (label, cid) in schema.vertex_labels.iter().zip(root.vertex_groups.iter()) {
            vertex_label_cids.insert(label.clone(), cid.clone());
        }
    } else {
        for group_cid in &root.vertex_groups {
            let group = fetch_vertex_group(cas, group_cid).await?;
            vertex_label_cids.insert(group.label, group_cid.clone());
        }
    }
    let mut edge_label_cids = HashMap::new();
    if schema.edge_labels.len() == root.edge_groups.len() {
        for (label, cid) in schema.edge_labels.iter().zip(root.edge_groups.iter()) {
            edge_label_cids.insert(label.clone(), cid.clone());
        }
    } else {
        for group_cid in &root.edge_groups {
            let group = fetch_edge_group(cas, group_cid).await?;
            edge_label_cids.insert(group.label, group_cid.clone());
        }
    }

    Ok(RootIndex {
        root_cid: root_cid.clone(),
        partition_id: root.partition_id,
        vertex_label_cids,
        edge_label_cids,
        vertex_count: root.vertex_count,
        edge_count: root.edge_count,
        schema,
    })
}

/// Root index: label→CID map for lazy loading.
#[derive(Debug, Clone)]
pub struct RootIndex {
    pub root_cid: Blake3Hash,
    pub partition_id: PartitionId,
    pub vertex_label_cids: HashMap<String, Blake3Hash>,
    pub edge_label_cids: HashMap<String, Blake3Hash>,
    pub vertex_count: u32,
    pub edge_count: u32,
    pub schema: SchemaBlock,
}

/// Load a single label's vertices from CAS (Arrow IPC or legacy CBOR).
pub async fn load_vertex_label(cid: &Blake3Hash, cas: &dyn CasStore) -> Result<LabelVertexGroup> {
    fetch_vertex_group(cas, cid).await
}

/// Load a single label's edges from CAS (Arrow IPC or legacy CBOR).
pub async fn load_edge_label(cid: &Blake3Hash, cas: &dyn CasStore) -> Result<LabelEdgeGroup> {
    fetch_edge_group(cas, cid).await
}

/// Fetch vertex group: Arrow IPC (new) or CBOR (legacy).
async fn fetch_vertex_group(cas: &dyn CasStore, cid: &Blake3Hash) -> Result<LabelVertexGroup> {
    let data = fetch_raw(cas, cid).await?;
    if crate::arrow_codec::is_arrow_block(&data) {
        crate::arrow_codec::decode_vertex_group(&data)
    } else {
        yata_cbor::decode(&data).map_err(|e| MdagError::Deserialize(e.to_string()))
    }
}

/// Fetch edge group: Arrow IPC (new) or CBOR (legacy).
async fn fetch_edge_group(cas: &dyn CasStore, cid: &Blake3Hash) -> Result<LabelEdgeGroup> {
    let data = fetch_raw(cas, cid).await?;
    if crate::arrow_codec::is_arrow_block(&data) {
        crate::arrow_codec::decode_edge_group(&data)
    } else {
        yata_cbor::decode(&data).map_err(|e| MdagError::Deserialize(e.to_string()))
    }
}

/// Fetch raw bytes from CAS.
async fn fetch_raw(cas: &dyn CasStore, cid: &Blake3Hash) -> Result<bytes::Bytes> {
    cas.get(cid)
        .await?
        .ok_or_else(|| MdagError::NotFound(cid.hex()))
}

/// Fetch a CAS block by CID and CBOR-decode it (for metadata blocks only).
async fn fetch_block<T: serde::de::DeserializeOwned>(
    cas: &dyn CasStore,
    cid: &Blake3Hash,
) -> Result<T> {
    let data = fetch_raw(cas, cid).await?;
    yata_cbor::decode(&data).map_err(|e| MdagError::Deserialize(e.to_string()))
}

/// Load MDAG label groups into Vineyard blobs (no CSR materialization).
/// Returns a FragmentManifest for lazy CSR build later.
pub async fn load_graph_to_vineyard(
    root_cid: &Blake3Hash,
    cas: &dyn CasStore,
    vineyard: &dyn yata_store::VineyardStore,
) -> Result<yata_store::FragmentManifest> {
    let root: GraphRootBlock = fetch_block(cas, root_cid).await?;
    let schema: SchemaBlock = fetch_block(cas, &root.schema_cid).await?;
    let now_ns = root.timestamp_ns;
    let partition_id = root.partition_id.get();

    let mut vertex_labels = HashMap::new();
    let v_labels_ordered: Vec<String> = if schema.vertex_labels.len() == root.vertex_groups.len() {
        schema.vertex_labels.clone()
    } else {
        (0..root.vertex_groups.len())
            .map(|i| format!("_v{i}"))
            .collect()
    };
    for (i, group_cid) in root.vertex_groups.iter().enumerate() {
        let data = fetch_raw(cas, group_cid).await?;
        let label = v_labels_ordered.get(i).cloned().unwrap_or_default();
        let meta = yata_store::ObjectMeta {
            id: yata_store::ObjectId(0),
            blob_type: yata_store::BlobType::ArrowVertexGroup,
            label: label.clone(),
            partition_id,
            size_bytes: 0,
            r2_key: format!("mdag/{}", group_cid.hex()),
            fields: HashMap::new(),
            created_at: now_ns,
        };
        let id = vineyard.put(meta, data);
        vertex_labels.insert(label, id);
    }

    let mut edge_labels = HashMap::new();
    let e_labels_ordered: Vec<String> = if schema.edge_labels.len() == root.edge_groups.len() {
        schema.edge_labels.clone()
    } else {
        (0..root.edge_groups.len())
            .map(|i| format!("_e{i}"))
            .collect()
    };
    for (i, group_cid) in root.edge_groups.iter().enumerate() {
        let data = fetch_raw(cas, group_cid).await?;
        let label = e_labels_ordered.get(i).cloned().unwrap_or_default();
        let meta = yata_store::ObjectMeta {
            id: yata_store::ObjectId(0),
            blob_type: yata_store::BlobType::ArrowEdgeGroup,
            label: label.clone(),
            partition_id,
            size_bytes: 0,
            r2_key: format!("mdag/{}", group_cid.hex()),
            fields: HashMap::new(),
            created_at: now_ns,
        };
        let id = vineyard.put(meta, data);
        edge_labels.insert(label, id);
    }

    Ok(yata_store::FragmentManifest {
        vertex_labels,
        edge_labels,
        csr_object: None,
        schema_object: None,
        partition_id,
        timestamp_ns: now_ns,
        vertex_count: root.vertex_count as u64,
        edge_count: root.edge_count as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialize::commit_graph;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    use yata_cas::LocalCasStore;
    use yata_grin::{Property, Scannable, Topology};

    fn test_key() -> (SigningKey, &'static str) {
        (SigningKey::generate(&mut OsRng), "did:key:z6MkTest")
    }

    #[tokio::test]
    async fn test_roundtrip_single_vertex() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let mut original = MutableCsrStore::new();
        original.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        original.commit();

        let (sk, did) = test_key();
        let root_cid = commit_graph(&original, &cas, None, "test", &sk, did)
            .await
            .unwrap();
        let loaded = load_graph(&root_cid, &cas).await.unwrap();

        assert_eq!(loaded.vertex_count(), 1);
        let vids = loaded.scan_all_vertices();
        assert_eq!(vids.len(), 1);
        let labels = <MutableCsrStore as Property>::vertex_labels(&loaded, vids[0]);
        assert_eq!(labels, vec!["Person"]);
        assert_eq!(
            loaded.vertex_prop(vids[0], "name"),
            Some(PropValue::Str("Alice".into()))
        );
        assert_eq!(loaded.vertex_prop(vids[0], "age"), Some(PropValue::Int(30)));
    }

    #[tokio::test]
    async fn test_roundtrip_single_vertex_with_partition() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let mut original = MutableCsrStore::new_partition(4);
        original.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        original.commit();

        let (sk, did) = test_key();
        let root_cid = commit_graph(&original, &cas, None, "partition", &sk, did)
            .await
            .unwrap();
        let loaded = load_graph(&root_cid, &cas).await.unwrap();

        assert_eq!(loaded.partition_id_raw(), PartitionId::from(4));
        assert_eq!(loaded.vertex_count(), 1);
    }

    #[tokio::test]
    async fn test_load_root_index_preserves_partition_id() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let mut original = MutableCsrStore::new_partition(6);
        original.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        original.commit();

        let (sk, did) = test_key();
        let root_cid = commit_graph(&original, &cas, None, "root-index", &sk, did)
            .await
            .unwrap();
        let index = load_root_index(&root_cid, &cas).await.unwrap();

        assert_eq!(index.partition_id, PartitionId::from(6));
        assert_eq!(index.vertex_count, 1);
    }

    #[tokio::test]
    async fn test_roundtrip_with_edges() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let mut original = MutableCsrStore::new();
        let a = original.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        let b = original.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        original.add_edge(a, b, "KNOWS", &[("since", PropValue::Int(2020))]);
        original.commit();

        let (sk, did) = test_key();
        let root_cid = commit_graph(&original, &cas, None, "edges", &sk, did)
            .await
            .unwrap();
        let loaded = load_graph(&root_cid, &cas).await.unwrap();

        assert_eq!(loaded.vertex_count(), 2);
        assert_eq!(loaded.edge_count(), 1);

        let vids = loaded.scan_all_vertices();
        let mut has_edge = false;
        for vid in &vids {
            let out = loaded.out_neighbors(*vid);
            if !out.is_empty() {
                has_edge = true;
                assert_eq!(out[0].edge_label, "KNOWS");
            }
        }
        assert!(has_edge);
    }

    #[tokio::test]
    async fn test_roundtrip_multi_label() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let mut original = MutableCsrStore::new();
        original.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        original.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        original.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        original.commit();

        let (sk, did) = test_key();
        let root_cid = commit_graph(&original, &cas, None, "multi", &sk, did)
            .await
            .unwrap();
        let loaded = load_graph(&root_cid, &cas).await.unwrap();

        assert_eq!(loaded.vertex_count(), 3);
        let persons = loaded.scan_vertices_by_label("Person");
        let companies = loaded.scan_vertices_by_label("Company");
        assert_eq!(persons.len(), 2);
        assert_eq!(companies.len(), 1);
    }
}
