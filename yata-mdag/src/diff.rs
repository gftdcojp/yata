//! Merkle diff: compute minimal set of changed blocks between two graph roots.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use yata_cas::CasStore;
use yata_core::Blake3Hash;

use crate::blocks::*;
use crate::error::{MdagError, Result};

/// Diff result between two MDAG graph roots.
#[derive(Debug, Clone, Default)]
pub struct MdagDiff {
    pub added_vertex_cids: Vec<Blake3Hash>,
    pub removed_vertex_cids: Vec<Blake3Hash>,
    pub added_edge_cids: Vec<Blake3Hash>,
    pub removed_edge_cids: Vec<Blake3Hash>,
    pub added_labels: Vec<String>,
    pub removed_labels: Vec<String>,
    pub schema_changed: bool,
}

impl MdagDiff {
    pub fn is_empty(&self) -> bool {
        self.added_vertex_cids.is_empty()
            && self.removed_vertex_cids.is_empty()
            && self.added_edge_cids.is_empty()
            && self.removed_edge_cids.is_empty()
            && self.added_labels.is_empty()
            && self.removed_labels.is_empty()
            && !self.schema_changed
    }

    pub fn total_changes(&self) -> usize {
        self.added_vertex_cids.len()
            + self.removed_vertex_cids.len()
            + self.added_edge_cids.len()
            + self.removed_edge_cids.len()
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
    let old_vg = load_label_map::<LabelVertexGroup>(cas, &old_root.vertex_groups).await?;
    let new_vg = load_label_map::<LabelVertexGroup>(cas, &new_root.vertex_groups).await?;

    let all_labels: BTreeSet<&str> = old_vg.keys().chain(new_vg.keys()).map(|s| s.as_str()).collect();
    for label in all_labels {
        match (old_vg.get(label), new_vg.get(label)) {
            (None, Some(new_g)) => {
                diff.added_labels.push(label.to_string());
                diff.added_vertex_cids.extend(new_g.vertex_cids.clone());
            }
            (Some(old_g), None) => {
                diff.removed_labels.push(label.to_string());
                diff.removed_vertex_cids.extend(old_g.vertex_cids.clone());
            }
            (Some(old_g), Some(new_g)) => {
                let old_set: HashSet<_> = old_g.vertex_cids.iter().collect();
                let new_set: HashSet<_> = new_g.vertex_cids.iter().collect();
                for cid in new_set.difference(&old_set) {
                    diff.added_vertex_cids.push((*cid).clone());
                }
                for cid in old_set.difference(&new_set) {
                    diff.removed_vertex_cids.push((*cid).clone());
                }
            }
            (None, None) => unreachable!(),
        }
    }

    // ── Diff edge groups ──────────────────────────────────────────────────
    let old_eg = load_label_map::<LabelEdgeGroup>(cas, &old_root.edge_groups).await?;
    let new_eg = load_label_map::<LabelEdgeGroup>(cas, &new_root.edge_groups).await?;

    let all_edge_labels: BTreeSet<&str> = old_eg.keys().chain(new_eg.keys()).map(|s| s.as_str()).collect();
    for label in all_edge_labels {
        match (old_eg.get(label), new_eg.get(label)) {
            (None, Some(new_g)) => {
                diff.added_edge_cids.extend(new_g.edge_cids.clone());
            }
            (Some(old_g), None) => {
                diff.removed_edge_cids.extend(old_g.edge_cids.clone());
            }
            (Some(old_g), Some(new_g)) => {
                let old_set: HashSet<_> = old_g.edge_cids.iter().collect();
                let new_set: HashSet<_> = new_g.edge_cids.iter().collect();
                for cid in new_set.difference(&old_set) {
                    diff.added_edge_cids.push((*cid).clone());
                }
                for cid in old_set.difference(&new_set) {
                    diff.removed_edge_cids.push((*cid).clone());
                }
            }
            (None, None) => unreachable!(),
        }
    }

    Ok(diff)
}

/// Trait for extracting label from a group block.
trait LabelGroup {
    fn label(&self) -> &str;
}
impl LabelGroup for LabelVertexGroup {
    fn label(&self) -> &str { &self.label }
}
impl LabelGroup for LabelEdgeGroup {
    fn label(&self) -> &str { &self.label }
}

async fn load_label_map<T: serde::de::DeserializeOwned + LabelGroup>(
    cas: &dyn CasStore,
    cids: &[Blake3Hash],
) -> Result<BTreeMap<String, T>> {
    let mut map = BTreeMap::new();
    for cid in cids {
        let group: T = fetch(cas, cid).await?;
        map.insert(group.label().to_string(), group);
    }
    Ok(map)
}

async fn fetch<T: serde::de::DeserializeOwned>(cas: &dyn CasStore, cid: &Blake3Hash) -> Result<T> {
    let data = cas.get(cid).await?.ok_or_else(|| MdagError::NotFound(cid.hex()))?;
    yata_cbor::decode(&data).map_err(|e| MdagError::Deserialize(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialize::commit_graph;
    use yata_cas::LocalCasStore;
    use yata_grin::{Mutable, PropValue};
    use yata_store::MutableCsrStore;

    #[tokio::test]
    async fn test_diff_same_root() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let mut store = MutableCsrStore::new();
        store.add_vertex("A", &[("x", PropValue::Int(1))]);
        store.commit();

        let root = commit_graph(&store, &cas, None, "v1").await.unwrap();
        let diff = merkle_diff(&root, &root, &cas).await.unwrap();
        assert!(diff.is_empty());
    }

    #[tokio::test]
    async fn test_diff_add_vertex() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.commit();
        let root1 = commit_graph(&store, &cas, None, "v1").await.unwrap();

        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.commit();
        let root2 = commit_graph(&store, &cas, Some(root1.clone()), "v2").await.unwrap();

        let diff = merkle_diff(&root1, &root2, &cas).await.unwrap();
        assert!(!diff.is_empty());
        assert_eq!(diff.added_vertex_cids.len(), 1); // Bob added
        assert_eq!(diff.removed_vertex_cids.len(), 0);
    }

    #[tokio::test]
    async fn test_diff_new_label() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.commit();
        let root1 = commit_graph(&store, &cas, None, "v1").await.unwrap();

        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.commit();
        let root2 = commit_graph(&store, &cas, Some(root1.clone()), "v2").await.unwrap();

        let diff = merkle_diff(&root1, &root2, &cas).await.unwrap();
        assert!(diff.added_labels.contains(&"Company".to_string()));
    }

    #[tokio::test]
    async fn test_diff_change_property() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into())), ("age", PropValue::Int(30))]);
        store.commit();
        let root1 = commit_graph(&store, &cas, None, "v1").await.unwrap();

        // Change property → different CID for vertex block
        store.set_vertex_prop(0, "age", PropValue::Int(31));
        store.commit();
        let root2 = commit_graph(&store, &cas, Some(root1.clone()), "v2").await.unwrap();

        let diff = merkle_diff(&root1, &root2, &cas).await.unwrap();
        // Old vertex CID removed, new one added
        assert_eq!(diff.removed_vertex_cids.len(), 1);
        assert_eq!(diff.added_vertex_cids.len(), 1);
    }
}
