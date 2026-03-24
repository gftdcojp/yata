//! Property index query: lookup vertices by property value without full CSR restore.
//!
//! Federated query flow:
//! 1. Fetch PropertyIndexBlock from CAS (~1ms, 1 blob)
//! 2. Lookup prop_value → Vec<IndexVertexRef> (in-memory BTreeMap search)
//! 3. Fetch only the needed LabelVertexGroup blobs (~2ms per blob)
//! 4. Optionally fetch LabelEdgeGroup blobs for edge traversal
//! 5. Build a small partial MutableCsrStore for local Cypher execution

use std::collections::{BTreeMap, HashMap, HashSet};
use yata_cas::CasStore;
use yata_core::{Blake3Hash, PartitionId};
use yata_grin::{Mutable, PropValue, Property, Topology};
use yata_store::MutableCsrStore;

use crate::blocks::*;
use crate::error::{MdagError, Result};

/// Loaded property index: in-memory lookup structure.
pub struct PropertyIndex {
    /// key → value → Vec<IndexVertexRef>
    pub indexes: BTreeMap<String, BTreeMap<String, Vec<IndexVertexRef>>>,
}

impl PropertyIndex {
    /// Load all property indexes from a graph root's index_cids.
    pub async fn load(cas: &dyn CasStore, index_cids: &[Blake3Hash]) -> Result<Self> {
        let mut indexes = BTreeMap::new();
        for cid in index_cids {
            let data = cas
                .get(cid)
                .await?
                .ok_or_else(|| MdagError::NotFound(cid.hex()))?;
            let block: PropertyIndexBlock =
                yata_cbor::decode(&data).map_err(|e| MdagError::Deserialize(e.to_string()))?;
            let entries: BTreeMap<String, Vec<IndexVertexRef>> =
                block.entries.into_iter().collect();
            indexes.insert(block.key, entries);
        }
        Ok(Self { indexes })
    }

    /// Lookup vertices by a property key-value pair.
    /// Returns IndexVertexRef entries pointing to CAS blobs.
    pub fn lookup(&self, key: &str, value: &str) -> Vec<&IndexVertexRef> {
        self.indexes
            .get(key)
            .and_then(|m| m.get(value))
            .map(|refs| refs.iter().collect())
            .unwrap_or_default()
    }

    /// List all values for a given property key.
    pub fn values(&self, key: &str) -> Vec<&str> {
        self.indexes
            .get(key)
            .map(|m| m.keys().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// List all indexed property keys.
    pub fn keys(&self) -> Vec<&str> {
        self.indexes.keys().map(|s| s.as_str()).collect()
    }
}

/// Load a partial graph: only the vertex groups and edge groups needed for
/// the given IndexVertexRef set. Returns a small MutableCsrStore suitable for
/// local Cypher execution.
///
/// `root` is used to find edge groups for traversal.
/// `hops` controls how many edge-traversal levels to include (0 = vertices only).
pub async fn load_partial_graph(
    cas: &dyn CasStore,
    root: &GraphRootBlock,
    refs: &[&IndexVertexRef],
    hops: u32,
) -> Result<MutableCsrStore> {
    // Collect unique group CIDs we need to fetch
    let needed_group_cids: HashSet<Blake3Hash> = refs.iter().map(|r| r.group_cid.clone()).collect();
    let needed_vids: HashSet<u32> = refs.iter().map(|r| r.vid).collect();

    let mut store = MutableCsrStore::new_with_partition_id(root.partition_id);
    let mut vid_map: HashMap<u32, u32> = HashMap::new();

    // Load needed vertex groups
    for group_cid in &root.vertex_groups {
        if !needed_group_cids.contains(group_cid) && hops == 0 {
            continue;
        }
        let data = cas
            .get(group_cid)
            .await?
            .ok_or_else(|| MdagError::NotFound(group_cid.hex()))?;
        let group: LabelVertexGroup = if crate::arrow_codec::is_arrow_block(&data) {
            crate::arrow_codec::decode_vertex_group(&data)?
        } else {
            yata_cbor::decode(&data).map_err(|e| MdagError::Deserialize(e.to_string()))?
        };

        for vb in &group.vertices {
            // For hop=0, only include referenced vertices.
            // For hop>0, include all vertices (needed for edge traversal targets).
            if hops == 0 && !needed_vids.contains(&vb.vid) {
                continue;
            }
            let props: Vec<(&str, PropValue)> = vb
                .props
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            let new_vid = store.add_vertex_with_labels(&vb.labels, &props);
            vid_map.insert(vb.vid, new_vid);
        }
    }

    // Load edge groups for traversal (when hops > 0)
    if hops > 0 {
        for group_cid in &root.edge_groups {
            let data = cas
                .get(group_cid)
                .await?
                .ok_or_else(|| MdagError::NotFound(group_cid.hex()))?;
            let group: LabelEdgeGroup = if crate::arrow_codec::is_arrow_block(&data) {
                crate::arrow_codec::decode_edge_group(&data)?
            } else {
                yata_cbor::decode(&data).map_err(|e| MdagError::Deserialize(e.to_string()))?
            };

            for eb in &group.edges {
                let src = vid_map.get(&eb.src).copied();
                let dst = vid_map.get(&eb.dst).copied();
                if let (Some(s), Some(d)) = (src, dst) {
                    let props: Vec<(&str, PropValue)> = eb
                        .props
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.clone()))
                        .collect();
                    store.add_edge(s, d, &eb.label, &props);
                }
            }
        }
    }

    store.commit();
    Ok(store)
}

/// Multi-app federated index: merges PropertyIndexes from multiple apps.
pub struct FederatedIndex {
    /// app_id → (root, PropertyIndex, CasStore)
    apps: Vec<FederatedApp>,
}

pub struct FederatedApp {
    pub app_id: String,
    pub partition_id: PartitionId,
    pub root: GraphRootBlock,
    pub root_cid: Blake3Hash,
    pub index: PropertyIndex,
    pub cas: std::sync::Arc<dyn CasStore>,
}

impl FederatedIndex {
    pub fn new() -> Self {
        Self { apps: Vec::new() }
    }

    /// Register an app's graph root + index.
    pub fn add_app(&mut self, app: FederatedApp) {
        self.apps.push(app);
    }

    /// Lookup across all apps. Returns (app_id, IndexVertexRef) pairs.
    pub fn lookup(&self, key: &str, value: &str) -> Vec<(&str, &IndexVertexRef)> {
        let mut results = Vec::new();
        for app in &self.apps {
            for vref in app.index.lookup(key, value) {
                results.push((app.app_id.as_str(), vref));
            }
        }
        results
    }

    /// Lookup across all apps with partition metadata.
    pub fn lookup_with_partition(
        &self,
        key: &str,
        value: &str,
    ) -> Vec<(&str, PartitionId, &IndexVertexRef)> {
        let mut results = Vec::new();
        for app in &self.apps {
            for vref in app.index.lookup(key, value) {
                results.push((app.app_id.as_str(), app.partition_id, vref));
            }
        }
        results
    }

    /// List registered apps with their partition IDs.
    pub fn app_partitions(&self) -> Vec<(&str, PartitionId)> {
        self.apps
            .iter()
            .map(|app| (app.app_id.as_str(), app.partition_id))
            .collect()
    }

    /// Load partial graphs from all apps that match the lookup,
    /// then merge into a single MutableCsrStore with source annotations.
    pub async fn load_and_merge(
        &self,
        key: &str,
        value: &str,
        hops: u32,
    ) -> Result<MutableCsrStore> {
        let mut merged = MutableCsrStore::new();
        let mut global_vid_map: HashMap<(usize, u32), u32> = HashMap::new(); // (app_idx, old_vid) → new_vid

        for (app_idx, app) in self.apps.iter().enumerate() {
            let refs = app.index.lookup(key, value);
            if refs.is_empty() {
                continue;
            }

            let partial = load_partial_graph(app.cas.as_ref(), &app.root, &refs, hops).await?;

            // Merge partial into global store, adding _source property
            let all_vids = partial.scan_all_vertices();
            for vid in &all_vids {
                let labels = <MutableCsrStore as Property>::vertex_labels(&partial, *vid);
                let prop_keys = if let Some(l) = labels.first() {
                    partial.vertex_prop_keys(l)
                } else {
                    Vec::new()
                };
                let mut props: Vec<(&str, PropValue)> = prop_keys
                    .iter()
                    .filter_map(|k| {
                        <MutableCsrStore as Property>::vertex_prop(&partial, *vid, k)
                            .map(|v| (k.as_str(), v))
                    })
                    .collect();
                // Add _source annotation
                props.push(("_source", PropValue::Str(app.app_id.clone())));

                let new_vid = merged.add_vertex_with_labels(&labels, &props);
                global_vid_map.insert((app_idx, *vid), new_vid);
            }

            // Merge edges
            for vid in &all_vids {
                let out = partial.out_neighbors(*vid);
                for n in out {
                    let src = global_vid_map.get(&(app_idx, *vid)).copied();
                    let dst = global_vid_map.get(&(app_idx, n.vid)).copied();
                    if let (Some(s), Some(d)) = (src, dst) {
                        let prop_keys = partial.edge_prop_keys(&n.edge_label);
                        let props: Vec<(&str, PropValue)> = prop_keys
                            .iter()
                            .filter_map(|k| {
                                <MutableCsrStore as Property>::edge_prop(&partial, n.edge_id, k)
                                    .map(|v| (k.as_str(), v))
                            })
                            .collect();
                        merged.add_edge(s, d, &n.edge_label, &props);
                    }
                }
            }
        }

        merged.commit();
        Ok(merged)
    }

    /// List all registered app IDs.
    pub fn app_ids(&self) -> Vec<&str> {
        self.apps.iter().map(|a| a.app_id.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialize::commit_graph_indexed;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    use yata_cas::LocalCasStore;

    fn test_key() -> (SigningKey, &'static str) {
        (SigningKey::generate(&mut OsRng), "did:key:z6MkTest")
    }

    #[tokio::test]
    async fn test_property_index_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let mut store = MutableCsrStore::new();
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("email", PropValue::Str("alice@example.com".into())),
            ],
        );
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Bob".into())),
                ("email", PropValue::Str("bob@example.com".into())),
            ],
        );
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.commit();

        let (sk, did) = test_key();
        let root_cid =
            commit_graph_indexed(&store, &cas, None, "indexed", &["email", "name"], &sk, did)
                .await
                .unwrap();
        let data = cas.get(&root_cid).await.unwrap().unwrap();
        let root: GraphRootBlock = yata_cbor::decode(&data).unwrap();

        assert_eq!(root.index_cids.len(), 2); // email + name

        let index = PropertyIndex::load(&cas, &root.index_cids).await.unwrap();
        assert_eq!(index.keys().len(), 2);

        // Lookup by email
        let refs = index.lookup("email", "alice@example.com");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].label, "Person");

        let refs = index.lookup("email", "bob@example.com");
        assert_eq!(refs.len(), 1);

        // Name index should have 3 entries (Alice, Bob, GFTD)
        let names = index.values("name");
        assert_eq!(names.len(), 3);

        // Missing value returns empty
        let refs = index.lookup("email", "nobody@example.com");
        assert!(refs.is_empty());
    }

    #[tokio::test]
    async fn test_partial_graph_load() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();

        let mut store = MutableCsrStore::new();
        let a = store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("email", PropValue::Str("alice@example.com".into())),
            ],
        );
        let b = store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Bob".into())),
                ("email", PropValue::Str("bob@example.com".into())),
            ],
        );
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.add_edge(a, b, "KNOWS", &[("since", PropValue::Int(2020))]);
        store.commit();

        let (sk, did) = test_key();
        let root_cid = commit_graph_indexed(&store, &cas, None, "test", &["email"], &sk, did)
            .await
            .unwrap();
        let data = cas.get(&root_cid).await.unwrap().unwrap();
        let root: GraphRootBlock = yata_cbor::decode(&data).unwrap();

        let index = PropertyIndex::load(&cas, &root.index_cids).await.unwrap();
        let refs = index.lookup("email", "alice@example.com");
        assert_eq!(refs.len(), 1);

        // Load partial with hops=0: only Alice
        let partial = load_partial_graph(&cas, &root, &refs, 0).await.unwrap();
        assert_eq!(Topology::vertex_count(&partial), 1);
        assert_eq!(Topology::edge_count(&partial), 0);

        // Load partial with hops=1: Alice + Bob + KNOWS edge
        let partial = load_partial_graph(&cas, &root, &refs, 1).await.unwrap();
        assert!(Topology::vertex_count(&partial) >= 2); // Alice + Bob (+ possibly GFTD)
        assert!(Topology::edge_count(&partial) >= 1); // KNOWS
    }

    #[tokio::test]
    async fn test_federated_index_multi_app() {
        let dir = tempfile::tempdir().unwrap();

        // App 1: intel
        let cas1 = std::sync::Arc::new(LocalCasStore::new(dir.path().join("intel")).await.unwrap());
        let mut store1 = MutableCsrStore::new();
        store1.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Tanaka".into())),
                ("email", PropValue::Str("tanaka@example.com".into())),
                ("role", PropValue::Str("analyst".into())),
            ],
        );
        store1.commit();
        let (sk, did) = test_key();
        let root1_cid =
            commit_graph_indexed(&store1, cas1.as_ref(), None, "intel", &["email"], &sk, did)
                .await
                .unwrap();
        let root1_data = cas1.get(&root1_cid).await.unwrap().unwrap();
        let root1: GraphRootBlock = yata_cbor::decode(&root1_data).unwrap();
        let index1 = PropertyIndex::load(cas1.as_ref(), &root1.index_cids)
            .await
            .unwrap();

        // App 2: resources
        let cas2 = std::sync::Arc::new(
            LocalCasStore::new(dir.path().join("resources"))
                .await
                .unwrap(),
        );
        let mut store2 = MutableCsrStore::new();
        let t = store2.add_vertex(
            "ResourceNode",
            &[
                ("name", PropValue::Str("Tanaka".into())),
                ("email", PropValue::Str("tanaka@example.com".into())),
                ("department", PropValue::Str("engineering".into())),
            ],
        );
        let p = store2.add_vertex(
            "ResourceNode",
            &[("name", PropValue::Str("Project X".into()))],
        );
        store2.add_edge(t, p, "WORKS_ON", &[]);
        store2.commit();
        let root2_cid = commit_graph_indexed(
            &store2,
            cas2.as_ref(),
            None,
            "resources",
            &["email"],
            &sk,
            did,
        )
        .await
        .unwrap();
        let root2_data = cas2.get(&root2_cid).await.unwrap().unwrap();
        let root2: GraphRootBlock = yata_cbor::decode(&root2_data).unwrap();
        let index2 = PropertyIndex::load(cas2.as_ref(), &root2.index_cids)
            .await
            .unwrap();

        // Build federated index
        let mut fed = FederatedIndex::new();
        fed.add_app(FederatedApp {
            app_id: "intel".into(),
            partition_id: root1.partition_id,
            root: root1,
            root_cid: root1_cid,
            index: index1,
            cas: cas1,
        });
        fed.add_app(FederatedApp {
            app_id: "resources".into(),
            partition_id: root2.partition_id,
            root: root2,
            root_cid: root2_cid,
            index: index2,
            cas: cas2,
        });

        // Federated lookup
        let results = fed.lookup("email", "tanaka@example.com");
        assert_eq!(results.len(), 2); // found in both apps
        let app_ids: Vec<&str> = results.iter().map(|(a, _)| *a).collect();
        assert!(app_ids.contains(&"intel"));
        assert!(app_ids.contains(&"resources"));

        let results_with_partition = fed.lookup_with_partition("email", "tanaka@example.com");
        assert_eq!(results_with_partition.len(), 2);
        assert!(
            results_with_partition
                .iter()
                .all(|(_, partition_id, _)| partition_id.get() == 0)
        );

        let app_partitions = fed.app_partitions();
        assert_eq!(app_partitions.len(), 2);

        // Federated merge with 1-hop traversal
        let merged = fed
            .load_and_merge("email", "tanaka@example.com", 1)
            .await
            .unwrap();
        assert!(Topology::vertex_count(&merged) >= 2); // Tanaka from intel + Tanaka from resources (+ possibly Project X)

        // Verify _source annotations
        let all_vids = merged.scan_all_vertices();
        let mut sources: HashSet<String> = HashSet::new();
        for vid in &all_vids {
            if let Some(PropValue::Str(s)) =
                <MutableCsrStore as Property>::vertex_prop(&merged, *vid, "_source")
            {
                sources.insert(s);
            }
        }
        assert!(sources.contains("intel"));
        assert!(sources.contains("resources"));
    }
}
