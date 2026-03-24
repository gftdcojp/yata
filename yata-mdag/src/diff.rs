//! Merkle diff: compute minimal set of changed blocks between two graph roots.
//!
//! With inline vertices/edges, diff works at two levels:
//! 1. Label group CID comparison — O(1) skip for unchanged labels
//! 2. For changed groups: inline vertex/edge set diff by vid/edge_id

use std::collections::{BTreeMap, BTreeSet, HashSet};
use yata_cas::CasStore;
use yata_core::Blake3Hash;

use crate::blocks::*;
use crate::error::{MdagError, Result};

/// Diff result between two MDAG graph roots.
#[derive(Debug, Clone, Default)]
pub struct MdagDiff {
    pub added_vertices: Vec<VertexBlock>,
    pub removed_vertices: Vec<VertexBlock>,
    pub added_edges: Vec<EdgeBlock>,
    pub removed_edges: Vec<EdgeBlock>,
    pub added_labels: Vec<String>,
    pub removed_labels: Vec<String>,
    pub schema_changed: bool,
}

impl MdagDiff {
    pub fn is_empty(&self) -> bool {
        self.added_vertices.is_empty()
            && self.removed_vertices.is_empty()
            && self.added_edges.is_empty()
            && self.removed_edges.is_empty()
            && self.added_labels.is_empty()
            && self.removed_labels.is_empty()
            && !self.schema_changed
    }

    pub fn total_changes(&self) -> usize {
        self.added_vertices.len()
            + self.removed_vertices.len()
            + self.added_edges.len()
            + self.removed_edges.len()
    }
}

/// Compute the Merkle diff between two graph root CIDs.
/// O(changed labels + changed vertices/edges within changed labels).
/// Unchanged label groups are skipped in O(1) via CID comparison.
pub async fn merkle_diff(
    old_root_cid: &Blake3Hash,
    new_root_cid: &Blake3Hash,
    cas: &dyn CasStore,
) -> Result<MdagDiff> {
    if old_root_cid == new_root_cid {
        return Ok(MdagDiff::default());
    }

    let old_root: GraphRootBlock = fetch(cas, old_root_cid).await?;
    let new_root: GraphRootBlock = fetch(cas, new_root_cid).await?;

    let mut diff = MdagDiff::default();
    diff.schema_changed = old_root.schema_cid != new_root.schema_cid;

    // ── Diff vertex groups ────────────────────────────────────────────────
    let old_vg = load_vertex_cid_map(cas, &old_root.vertex_groups).await?;
    let new_vg = load_vertex_cid_map(cas, &new_root.vertex_groups).await?;

    let all_labels: BTreeSet<&str> = old_vg
        .keys()
        .chain(new_vg.keys())
        .map(|s| s.as_str())
        .collect();
    for label in all_labels {
        match (old_vg.get(label), new_vg.get(label)) {
            (None, Some((_, new_g))) => {
                diff.added_labels.push(label.to_string());
                diff.added_vertices.extend(new_g.vertices.clone());
            }
            (Some((_, old_g)), None) => {
                diff.removed_labels.push(label.to_string());
                diff.removed_vertices.extend(old_g.vertices.clone());
            }
            (Some((old_cid, old_g)), Some((new_cid, new_g))) => {
                if old_cid == new_cid {
                    continue; // O(1) skip — label group unchanged
                }
                // Diff inline vertices by vid
                let old_set: HashSet<u32> = old_g.vertices.iter().map(|v| v.vid).collect();
                let new_set: HashSet<u32> = new_g.vertices.iter().map(|v| v.vid).collect();
                let old_map: BTreeMap<u32, &VertexBlock> =
                    old_g.vertices.iter().map(|v| (v.vid, v)).collect();
                let new_map: BTreeMap<u32, &VertexBlock> =
                    new_g.vertices.iter().map(|v| (v.vid, v)).collect();

                // Added vertices (in new but not in old)
                for vid in new_set.difference(&old_set) {
                    if let Some(v) = new_map.get(vid) {
                        diff.added_vertices.push((*v).clone());
                    }
                }
                // Removed vertices (in old but not in new)
                for vid in old_set.difference(&new_set) {
                    if let Some(v) = old_map.get(vid) {
                        diff.removed_vertices.push((*v).clone());
                    }
                }
                // Modified vertices (same vid, different content)
                for vid in old_set.intersection(&new_set) {
                    let old_v = old_map[vid];
                    let new_v = new_map[vid];
                    if old_v != new_v {
                        diff.removed_vertices.push(old_v.clone());
                        diff.added_vertices.push(new_v.clone());
                    }
                }
            }
            (None, None) => unreachable!(),
        }
    }

    // ── Diff edge groups ──────────────────────────────────────────────────
    let old_eg = load_edge_cid_map(cas, &old_root.edge_groups).await?;
    let new_eg = load_edge_cid_map(cas, &new_root.edge_groups).await?;

    let all_edge_labels: BTreeSet<&str> = old_eg
        .keys()
        .chain(new_eg.keys())
        .map(|s| s.as_str())
        .collect();
    for label in all_edge_labels {
        match (old_eg.get(label), new_eg.get(label)) {
            (None, Some((_, new_g))) => {
                diff.added_edges.extend(new_g.edges.clone());
            }
            (Some((_, old_g)), None) => {
                diff.removed_edges.extend(old_g.edges.clone());
            }
            (Some((old_cid, old_g)), Some((new_cid, new_g))) => {
                if old_cid == new_cid {
                    continue;
                }
                let old_set: HashSet<u32> = old_g.edges.iter().map(|e| e.edge_id).collect();
                let new_set: HashSet<u32> = new_g.edges.iter().map(|e| e.edge_id).collect();
                let old_map: BTreeMap<u32, &EdgeBlock> =
                    old_g.edges.iter().map(|e| (e.edge_id, e)).collect();
                let new_map: BTreeMap<u32, &EdgeBlock> =
                    new_g.edges.iter().map(|e| (e.edge_id, e)).collect();

                for eid in new_set.difference(&old_set) {
                    if let Some(e) = new_map.get(eid) {
                        diff.added_edges.push((*e).clone());
                    }
                }
                for eid in old_set.difference(&new_set) {
                    if let Some(e) = old_map.get(eid) {
                        diff.removed_edges.push((*e).clone());
                    }
                }
                for eid in old_set.intersection(&new_set) {
                    let old_e = old_map[eid];
                    let new_e = new_map[eid];
                    if old_e != new_e {
                        diff.removed_edges.push(old_e.clone());
                        diff.added_edges.push(new_e.clone());
                    }
                }
            }
            (None, None) => unreachable!(),
        }
    }

    Ok(diff)
}

async fn load_vertex_cid_map(
    cas: &dyn CasStore,
    cids: &[Blake3Hash],
) -> Result<BTreeMap<String, (Blake3Hash, LabelVertexGroup)>> {
    let mut map = BTreeMap::new();
    for cid in cids {
        let data = cas
            .get(cid)
            .await?
            .ok_or_else(|| MdagError::NotFound(cid.hex()))?;
        let group = if crate::arrow_codec::is_arrow_block(&data) {
            crate::arrow_codec::decode_vertex_group(&data)?
        } else {
            yata_cbor::decode(&data).map_err(|e| MdagError::Deserialize(e.to_string()))?
        };
        map.insert(group.label.clone(), (cid.clone(), group));
    }
    Ok(map)
}

async fn load_edge_cid_map(
    cas: &dyn CasStore,
    cids: &[Blake3Hash],
) -> Result<BTreeMap<String, (Blake3Hash, LabelEdgeGroup)>> {
    let mut map = BTreeMap::new();
    for cid in cids {
        let data = cas
            .get(cid)
            .await?
            .ok_or_else(|| MdagError::NotFound(cid.hex()))?;
        let group = if crate::arrow_codec::is_arrow_block(&data) {
            crate::arrow_codec::decode_edge_group(&data)?
        } else {
            yata_cbor::decode(&data).map_err(|e| MdagError::Deserialize(e.to_string()))?
        };
        map.insert(group.label.clone(), (cid.clone(), group));
    }
    Ok(map)
}

async fn fetch<T: serde::de::DeserializeOwned>(cas: &dyn CasStore, cid: &Blake3Hash) -> Result<T> {
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
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    use yata_cas::LocalCasStore;
    use yata_grin::{Mutable, PropValue};
    use yata_store::MutableCsrStore;

    fn test_key() -> (SigningKey, &'static str) {
        (SigningKey::generate(&mut OsRng), "did:key:z6MkTest")
    }

    #[tokio::test]
    async fn test_diff_same_root() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let mut store = MutableCsrStore::new();
        store.add_vertex("A", &[("x", PropValue::Int(1))]);
        store.commit();

        let (sk, did) = test_key();
        let root = commit_graph(&store, &cas, None, "v1", &sk, did)
            .await
            .unwrap();
        let diff = merkle_diff(&root, &root, &cas).await.unwrap();
        assert!(diff.is_empty());
    }

    #[tokio::test]
    async fn test_diff_add_vertex() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let (sk, did) = test_key();
        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.commit();
        let root1 = commit_graph(&store, &cas, None, "v1", &sk, did)
            .await
            .unwrap();

        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.commit();
        let root2 = commit_graph(&store, &cas, Some(root1.clone()), "v2", &sk, did)
            .await
            .unwrap();

        let diff = merkle_diff(&root1, &root2, &cas).await.unwrap();
        assert!(!diff.is_empty());
        assert_eq!(diff.added_vertices.len(), 1);
        assert_eq!(diff.removed_vertices.len(), 0);
        assert_eq!(
            diff.added_vertices[0].props[0].1,
            PropValue::Str("Bob".into())
        );
    }

    #[tokio::test]
    async fn test_diff_new_label() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let (sk, did) = test_key();
        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.commit();
        let root1 = commit_graph(&store, &cas, None, "v1", &sk, did)
            .await
            .unwrap();

        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.commit();
        let root2 = commit_graph(&store, &cas, Some(root1.clone()), "v2", &sk, did)
            .await
            .unwrap();

        let diff = merkle_diff(&root1, &root2, &cas).await.unwrap();
        assert!(diff.added_labels.contains(&"Company".to_string()));
    }

    #[tokio::test]
    async fn test_diff_change_property() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let (sk, did) = test_key();
        let mut store = MutableCsrStore::new();
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        store.commit();
        let root1 = commit_graph(&store, &cas, None, "v1", &sk, did)
            .await
            .unwrap();

        store.set_vertex_prop(0, "age", PropValue::Int(31));
        store.commit();
        let root2 = commit_graph(&store, &cas, Some(root1.clone()), "v2", &sk, did)
            .await
            .unwrap();

        let diff = merkle_diff(&root1, &root2, &cas).await.unwrap();
        assert_eq!(diff.removed_vertices.len(), 1);
        assert_eq!(diff.added_vertices.len(), 1);
        assert_eq!(
            diff.removed_vertices[0]
                .props
                .iter()
                .find(|(k, _)| k == "age")
                .unwrap()
                .1,
            PropValue::Int(30)
        );
        assert_eq!(
            diff.added_vertices[0]
                .props
                .iter()
                .find(|(k, _)| k == "age")
                .unwrap()
                .1,
            PropValue::Int(31)
        );
    }
}
