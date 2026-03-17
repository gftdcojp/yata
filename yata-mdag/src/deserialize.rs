//! MDAG → Graph deserialization: root CID → MutableCsrStore.

use std::collections::HashMap;
use yata_cas::CasStore;
use yata_core::Blake3Hash;
use yata_grin::{Mutable, PropValue};
use yata_store::MutableCsrStore;

use crate::blocks::*;
use crate::error::{MdagError, Result};

/// Load a graph from MDAG CAS blocks into a fresh MutableCsrStore.
pub async fn load_graph(
    root_cid: &Blake3Hash,
    cas: &dyn CasStore,
) -> Result<MutableCsrStore> {
    let root: GraphRootBlock = fetch_block(cas, root_cid).await?;
    let _schema: SchemaBlock = fetch_block(cas, &root.schema_cid).await?;

    let mut store = MutableCsrStore::new();
    let mut vid_map: HashMap<u32, u32> = HashMap::new();

    // ── Load vertices ─────────────────────────────────────────────────────
    for group_cid in &root.vertex_groups {
        let group: LabelVertexGroup = fetch_block(cas, group_cid).await?;
        for vcid in &group.vertex_cids {
            let vb: VertexBlock = fetch_block(cas, vcid).await?;
            let props: Vec<(&str, PropValue)> = vb
                .props
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            let new_vid = store.add_vertex_with_labels(&vb.labels, &props);
            vid_map.insert(vb.vid, new_vid);
        }
    }

    // ── Load edges ────────────────────────────────────────────────────────
    for group_cid in &root.edge_groups {
        let group: LabelEdgeGroup = fetch_block(cas, group_cid).await?;
        for ecid in &group.edge_cids {
            let eb: EdgeBlock = fetch_block(cas, ecid).await?;
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

/// Fetch a CAS block by CID and CBOR-decode it.
async fn fetch_block<T: serde::de::DeserializeOwned>(
    cas: &dyn CasStore,
    cid: &Blake3Hash,
) -> Result<T> {
    let data = cas
        .get(cid)
        .await?
        .ok_or_else(|| MdagError::NotFound(cid.hex()))?;
    yata_cbor::decode(&data).map_err(|e| MdagError::Deserialize(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialize::commit_graph;
    use yata_cas::LocalCasStore;
    use yata_grin::{Topology, Property, Scannable};

    #[tokio::test]
    async fn test_roundtrip_single_vertex() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let mut original = MutableCsrStore::new();
        original.add_vertex("Person", &[("name", PropValue::Str("Alice".into())), ("age", PropValue::Int(30))]);
        original.commit();

        let root_cid = commit_graph(&original, &cas, None, "test").await.unwrap();
        let loaded = load_graph(&root_cid, &cas).await.unwrap();

        assert_eq!(loaded.vertex_count(), 1);
        let vids = loaded.scan_all_vertices();
        assert_eq!(vids.len(), 1);
        let labels = <MutableCsrStore as Property>::vertex_labels(&loaded, vids[0]);
        assert_eq!(labels, vec!["Person"]);
        assert_eq!(loaded.vertex_prop(vids[0], "name"), Some(PropValue::Str("Alice".into())));
        assert_eq!(loaded.vertex_prop(vids[0], "age"), Some(PropValue::Int(30)));
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

        let root_cid = commit_graph(&original, &cas, None, "edges").await.unwrap();
        let loaded = load_graph(&root_cid, &cas).await.unwrap();

        assert_eq!(loaded.vertex_count(), 2);
        assert_eq!(loaded.edge_count(), 1);

        // Verify topology
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

        let root_cid = commit_graph(&original, &cas, None, "multi").await.unwrap();
        let loaded = load_graph(&root_cid, &cas).await.unwrap();

        assert_eq!(loaded.vertex_count(), 3);
        let persons = loaded.scan_vertices_by_label("Person");
        let companies = loaded.scan_vertices_by_label("Company");
        assert_eq!(persons.len(), 2);
        assert_eq!(companies.len(), 1);
    }
}
