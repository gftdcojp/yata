//! Graph → MDAG serialization: MutableCsrStore → root CID in CAS.

use std::collections::BTreeMap;
use bytes::Bytes;
use yata_cas::CasStore;
use yata_core::Blake3Hash;
use yata_grin::*;
use yata_store::MutableCsrStore;

use crate::blocks::*;
use crate::error::Result;

/// Serialize a MutableCsrStore graph into MDAG blocks stored in CAS.
/// Returns the root CID identifying the entire graph state.
pub async fn commit_graph(
    store: &MutableCsrStore,
    cas: &dyn CasStore,
    parent: Option<Blake3Hash>,
    message: &str,
) -> Result<Blake3Hash> {
    // ── 1. Serialize vertices ─────────────────────────────────────────────
    let all_vids = store.scan_all_vertices();
    let mut vertex_by_label: BTreeMap<String, Vec<Blake3Hash>> = BTreeMap::new();

    for vid in &all_vids {
        let labels = <MutableCsrStore as Property>::vertex_labels(store, *vid);
        let prop_keys = if let Some(first_label) = labels.first() {
            store.vertex_prop_keys(first_label)
        } else {
            Vec::new()
        };
        let mut props: Vec<(String, PropValue)> = prop_keys
            .iter()
            .filter_map(|k| store.vertex_prop(*vid, k).map(|v| (k.clone(), v)))
            .collect();
        props.sort_by(|a, b| a.0.cmp(&b.0));

        let block = VertexBlock {
            vid: *vid,
            labels: labels.clone(),
            props,
        };
        let cid = put_block(cas, &block).await?;

        let primary_label = labels.into_iter().next().unwrap_or_default();
        vertex_by_label.entry(primary_label).or_default().push(cid);
    }

    // ── 2. Build label vertex groups ──────────────────────────────────────
    let mut vertex_group_cids: Vec<Blake3Hash> = Vec::new();
    for (label, mut cids) in vertex_by_label {
        cids.sort_by(|a, b| a.hex().cmp(&b.hex()));
        let group = LabelVertexGroup {
            count: cids.len() as u32,
            label,
            vertex_cids: cids,
        };
        vertex_group_cids.push(put_block(cas, &group).await?);
    }

    // ── 3. Serialize edges ────────────────────────────────────────────────
    let mut edge_by_label: BTreeMap<String, Vec<Blake3Hash>> = BTreeMap::new();
    let edge_labels = <MutableCsrStore as Schema>::edge_labels(store);

    for vid in &all_vids {
        for elabel in &edge_labels {
            let neighbors = store.out_neighbors_by_label(*vid, elabel);
            for n in neighbors {
                let prop_keys = store.edge_prop_keys(elabel);
                let mut props: Vec<(String, PropValue)> = prop_keys
                    .iter()
                    .filter_map(|k| store.edge_prop(n.edge_id, k).map(|v| (k.clone(), v)))
                    .collect();
                props.sort_by(|a, b| a.0.cmp(&b.0));

                let block = EdgeBlock {
                    edge_id: n.edge_id,
                    src: *vid,
                    dst: n.vid,
                    label: elabel.clone(),
                    props,
                };
                let cid = put_block(cas, &block).await?;
                edge_by_label.entry(elabel.clone()).or_default().push(cid);
            }
        }
    }

    // ── 4. Build label edge groups ────────────────────────────────────────
    let mut edge_group_cids: Vec<Blake3Hash> = Vec::new();
    for (label, mut cids) in edge_by_label {
        cids.sort_by(|a, b| a.hex().cmp(&b.hex()));
        let group = LabelEdgeGroup {
            count: cids.len() as u32,
            label,
            edge_cids: cids,
        };
        edge_group_cids.push(put_block(cas, &group).await?);
    }

    // ── 5. Schema block ──────────────────────────────────────────────────
    let vlabels = <MutableCsrStore as Schema>::vertex_labels(store);
    let elabels = <MutableCsrStore as Schema>::edge_labels(store);
    let pks: Vec<(String, String)> = vlabels
        .iter()
        .filter_map(|l| store.vertex_primary_key(l).map(|pk| (l.clone(), pk)))
        .collect();
    let schema = SchemaBlock {
        vertex_labels: vlabels,
        edge_labels: elabels,
        vertex_primary_keys: pks,
    };
    let schema_cid = put_block(cas, &schema).await?;

    // ── 6. Root block ────────────────────────────────────────────────────
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    let root = GraphRootBlock {
        version: store.version(),
        parent,
        schema_cid,
        vertex_groups: vertex_group_cids,
        edge_groups: edge_group_cids,
        vertex_count: store.vertex_count() as u32,
        edge_count: store.edge_count() as u32,
        timestamp_ns: now_ns,
        message: message.to_string(),
    };
    put_block(cas, &root).await
}

/// CBOR-encode a block and store in CAS, returning its CID (Blake3 hash).
async fn put_block<T: serde::Serialize>(cas: &dyn CasStore, block: &T) -> Result<Blake3Hash> {
    let cbor = yata_cbor::encode(block)?;
    let hash = cas.put(Bytes::from(cbor)).await?;
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_cas::LocalCasStore;

    #[tokio::test]
    async fn test_commit_empty_graph() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let store = MutableCsrStore::new();
        let root_cid = commit_graph(&store, &cas, None, "empty").await.unwrap();
        // Verify root block
        let data = cas.get(&root_cid).await.unwrap().unwrap();
        let root: GraphRootBlock = yata_cbor::decode(&data).unwrap();
        assert_eq!(root.vertex_count, 0);
        assert_eq!(root.edge_count, 0);
        assert!(root.parent.is_none());
    }

    #[tokio::test]
    async fn test_commit_single_vertex() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.commit();

        let root_cid = commit_graph(&store, &cas, None, "one vertex").await.unwrap();
        let data = cas.get(&root_cid).await.unwrap().unwrap();
        let root: GraphRootBlock = yata_cbor::decode(&data).unwrap();
        assert_eq!(root.vertex_count, 1);
        assert_eq!(root.vertex_groups.len(), 1);
    }

    #[tokio::test]
    async fn test_commit_multi_label() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.commit();

        let root_cid = commit_graph(&store, &cas, None, "two labels").await.unwrap();
        let data = cas.get(&root_cid).await.unwrap().unwrap();
        let root: GraphRootBlock = yata_cbor::decode(&data).unwrap();
        assert_eq!(root.vertex_count, 2);
        assert_eq!(root.vertex_groups.len(), 2); // Person + Company
    }

    #[tokio::test]
    async fn test_commit_with_edges() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let mut store = MutableCsrStore::new();
        let a = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        let b = store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_edge(a, b, "KNOWS", &[("since", PropValue::Int(2020))]);
        store.commit();

        let root_cid = commit_graph(&store, &cas, None, "with edge").await.unwrap();
        let data = cas.get(&root_cid).await.unwrap().unwrap();
        let root: GraphRootBlock = yata_cbor::decode(&data).unwrap();
        assert_eq!(root.vertex_count, 2);
        assert_eq!(root.edge_count, 1);
        assert_eq!(root.edge_groups.len(), 1);
    }
}
