use std::collections::HashMap;
use yata_core::PartitionId;
use yata_cypher::Graph;
use yata_graph::GraphStore;
use yata_grin::{Mutable, PropValue, Topology};
use yata_store::MutableCsrStore;

/// Convert yata_cypher::Value → yata_grin::PropValue for CSR property storage.
pub fn cypher_to_prop(v: &yata_cypher::types::Value) -> PropValue {
    match v {
        yata_cypher::types::Value::Null => PropValue::Null,
        yata_cypher::types::Value::Bool(b) => PropValue::Bool(*b),
        yata_cypher::types::Value::Int(i) => PropValue::Int(*i),
        yata_cypher::types::Value::Float(f) => PropValue::Float(*f),
        yata_cypher::types::Value::Str(s) => PropValue::Str(s.clone()),
        other => PropValue::Str(format!("{}", other)),
    }
}

/// Load all vertices/edges from GraphStore into a fresh MutableCsrStore.
pub async fn load_csr_from_graph(store: &GraphStore) -> Result<MutableCsrStore, String> {
    load_csr_from_graph_with_partition(store, PartitionId::from(0)).await
}

/// Load all vertices/edges from GraphStore into a fresh MutableCsrStore for a specific partition.
pub async fn load_csr_from_graph_with_partition(
    store: &GraphStore,
    partition_id: PartitionId,
) -> Result<MutableCsrStore, String> {
    let vertices = store.load_vertices().await.map_err(|e| e.to_string())?;
    let edges = store.load_edges().await.map_err(|e| e.to_string())?;

    let mut csr = MutableCsrStore::new_with_partition_id(partition_id);
    let mut vid_map: HashMap<String, u32> = HashMap::new();

    for node in &vertices {
        let props: Vec<(&str, PropValue)> = node
            .props
            .iter()
            .map(|(k, v): (&String, &yata_cypher::types::Value)| (k.as_str(), cypher_to_prop(v)))
            .collect();
        let mut all_props = props;
        all_props.push(("_vid", PropValue::Str(node.id.clone())));
        let vid = csr.add_vertex_with_labels(&node.labels, &all_props);
        vid_map.insert(node.id.clone(), vid);
    }

    for edge in &edges {
        let src = vid_map.get(&edge.src).copied();
        let dst = vid_map.get(&edge.dst).copied();
        if let (Some(s), Some(d)) = (src, dst) {
            let props: Vec<(&str, PropValue)> = edge
                .props
                .iter()
                .map(|(k, v): (&String, &yata_cypher::types::Value)| {
                    (k.as_str(), cypher_to_prop(v))
                })
                .collect();
            csr.add_edge(s, d, &edge.rel_type, &props);
        }
    }

    csr.commit();
    tracing::info!(
        vertices = vertices.len(),
        edges = edges.len(),
        "engine: CSR initialized from GraphStore"
    );
    Ok(csr)
}

/// Rebuild a CSR store from a post-mutation MemoryGraph.
pub fn rebuild_csr_from_graph(g: &yata_graph::QueryableGraph) -> MutableCsrStore {
    rebuild_csr_from_graph_with_partition(g, PartitionId::from(0))
}

/// Rebuild a CSR store from a post-mutation MemoryGraph for a specific partition.
pub fn rebuild_csr_from_graph_with_partition(
    g: &yata_graph::QueryableGraph,
    partition_id: PartitionId,
) -> MutableCsrStore {
    let nodes = g.0.nodes();
    let rels = g.0.rels();
    let mut csr = MutableCsrStore::new_with_partition_id(partition_id);
    let mut vid_map: HashMap<String, u32> = HashMap::new();

    for node in &nodes {
        let props: Vec<(&str, PropValue)> = node
            .props
            .iter()
            .map(|(k, v): (&String, &yata_cypher::types::Value)| (k.as_str(), cypher_to_prop(v)))
            .collect();
        let mut all_props = props;
        all_props.push(("_vid", PropValue::Str(node.id.clone())));
        let vid = csr.add_vertex_with_labels(&node.labels, &all_props);
        vid_map.insert(node.id.clone(), vid);
    }

    for rel in &rels {
        if let (Some(&s), Some(&d)) = (vid_map.get(&rel.src), vid_map.get(&rel.dst)) {
            let props: Vec<(&str, PropValue)> = rel
                .props
                .iter()
                .map(|(k, v): (&String, &yata_cypher::types::Value)| {
                    (k.as_str(), cypher_to_prop(v))
                })
                .collect();
            csr.add_edge(s, d, &rel.rel_type, &props);
        }
    }

    csr.commit();
    csr
}

/// Restore CSR from an ArrowFragment (deserialized from R2 blobs).
///
/// Reads vertex RecordBatch columns → add_vertex, then uses CSR topology
/// from NbrUnit packed bytes for edge reconstruction.
pub fn restore_csr_from_fragment(
    frag: &yata_vineyard::ArrowFragment,
    partition_id: PartitionId,
) -> MutableCsrStore {
    use arrow::array::{Int64Array, Float64Array, StringArray, BooleanArray};

    let mut csr = MutableCsrStore::new_with_partition_id(partition_id);

    // Restore vertices per label
    for (vlabel_id, entry) in frag.schema.vertex_entries.iter().enumerate() {
        let label = &entry.label;
        if let Some(batch) = frag.vertex_table(vlabel_id) {
            for row in 0..batch.num_rows() {
                let mut props: Vec<(&str, PropValue)> = Vec::new();
                for prop_def in &entry.props {
                    let col = batch.column_by_name(&prop_def.name);
                    if let Some(col) = col {
                        let val = if col.is_null(row) {
                            PropValue::Null
                        } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                            PropValue::Str(arr.value(row).to_string())
                        } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                            PropValue::Int(arr.value(row))
                        } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                            PropValue::Float(arr.value(row))
                        } else if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                            PropValue::Bool(arr.value(row))
                        } else {
                            PropValue::Null
                        };
                        props.push((&prop_def.name, val));
                    }
                }
                csr.add_vertex(label, &props);
            }
        }
    }

    // Restore edges from NbrUnit CSR topology
    for (elabel_id, edge_entry) in frag.schema.edge_entries.iter().enumerate() {
        let edge_label = &edge_entry.label;
        for (vlabel_id, _) in frag.schema.vertex_entries.iter().enumerate() {
            // For each vertex in this label, extract outgoing neighbors
            let vcount = frag.inner_vertex_num(vlabel_id);
            for vid in 0..vcount {
                if let Some(nbrs) = frag.out_neighbors(vlabel_id, elabel_id, vid) {
                    for nbr in nbrs {
                        // NbrUnit: vid = dst vertex, eid = edge id
                        csr.add_edge(vid as u32, nbr.vid as u32, edge_label, &[]);
                    }
                }
            }
        }
    }

    csr.commit();
    tracing::info!(
        vertices = csr.vertex_count(),
        edges = csr.edge_count(),
        "CSR restored from ArrowFragment"
    );
    csr
}

/// Page-in ArrowFragment from R2 and restore to CSR.
/// Fetches `snap/fragment/meta.json` + individual blobs → ArrowFragment::deserialize → CSR.
pub fn page_in_from_r2(
    s3: &yata_s3::s3::S3Client,
    prefix: &str,
    partition_id: PartitionId,
) -> Result<MutableCsrStore, String> {
    use yata_vineyard::blob::{BlobStore, MemoryBlobStore};

    // 1. Fetch fragment meta
    let meta_key = format!("{prefix}snap/fragment/meta.json");
    let meta_bytes = s3.get_sync(&meta_key)
        .map_err(|e| format!("R2 meta fetch: {e}"))?
        .ok_or_else(|| "no fragment meta.json in R2".to_string())?;
    let meta: yata_vineyard::blob::ObjectMeta = serde_json::from_slice(&meta_bytes)
        .map_err(|e| format!("parse fragment meta: {e}"))?;

    // 2. Fetch all blobs referenced in meta (name-based, no CAS hash)
    let blob_store = MemoryBlobStore::new();
    for name in meta.blobs.keys() {
        let blob_key = format!("{prefix}snap/fragment/{name}");
        match s3.get_sync(&blob_key) {
            Ok(Some(data)) => {
                blob_store.put(name, data);
            }
            Ok(None) => {
                tracing::warn!(name, "R2 blob not found, skipping");
            }
            Err(e) => {
                tracing::warn!(name, error = %e, "R2 blob fetch failed, skipping");
            }
        }
    }

    // 3. Deserialize ArrowFragment
    let frag = yata_vineyard::ArrowFragment::deserialize(&meta, &blob_store)
        .map_err(|e| format!("ArrowFragment deserialize: {e}"))?;

    // 4. Restore CSR
    Ok(restore_csr_from_fragment(&frag, partition_id))
}

/// Page-in a single vertex label's chunk from R2 (selective chunk loading).
///
/// Fetches only `vertex_table_{label_id}_chunk_{chunk_id}.arrow` from R2.
/// Returns the raw RecordBatch without CSR reconstruction (caller merges into existing store).
///
/// This is the building block for future on-demand chunk loading:
/// query touches label X → check which chunks are loaded → page-in missing chunks only.
pub fn page_in_vertex_chunk_from_r2(
    s3: &yata_s3::s3::S3Client,
    prefix: &str,
    label_id: usize,
    chunk_id: usize,
) -> Result<arrow::record_batch::RecordBatch, String> {
    let blob_key = format!("{prefix}snap/fragment/vertex_table_{label_id}_chunk_{chunk_id}");
    let data = s3
        .get_sync(&blob_key)
        .map_err(|e| format!("R2 chunk fetch: {e}"))?
        .ok_or_else(|| format!("chunk not found: vertex_table_{label_id}_chunk_{chunk_id}"))?;
    yata_arrow::ipc_to_batch(&data)
        .map_err(|e| format!("Arrow IPC decode chunk: {e}"))
}

/// Get the chunk count for a vertex label from fragment meta.
/// Returns None if the label uses single-blob format (pre-chunking).
pub fn vertex_label_chunk_count(
    meta: &yata_vineyard::blob::ObjectMeta,
    label_id: usize,
) -> Option<usize> {
    let key = format!("vertex_table_{}_chunks", label_id);
    meta.get_field(&key).and_then(|v| v.as_i64()).map(|n| n as usize)
}

/// Fetch only the fragment meta.json from R2 (without loading any blobs).
/// Useful for inspecting chunk layout before deciding what to page-in.
pub fn fetch_fragment_meta(
    s3: &yata_s3::s3::S3Client,
    prefix: &str,
) -> Result<yata_vineyard::blob::ObjectMeta, String> {
    let meta_key = format!("{prefix}snap/fragment/meta.json");
    let meta_bytes = s3.get_sync(&meta_key)
        .map_err(|e| format!("R2 meta fetch: {e}"))?
        .ok_or_else(|| "no fragment meta.json in R2".to_string())?;
    serde_json::from_slice(&meta_bytes)
        .map_err(|e| format!("parse fragment meta: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use yata_cypher::{Graph, MemoryGraph, NodeRef};
    use yata_graph::QueryableGraph;
    use yata_grin::Topology;

    #[test]
    fn test_rebuild_csr_with_partition_preserves_partition_id() {
        let mut graph = MemoryGraph::new();
        graph.add_node(NodeRef {
            id: "g1".into(),
            labels: vec!["Person".into()],
            props: IndexMap::new(),
        });

        let csr =
            rebuild_csr_from_graph_with_partition(&QueryableGraph(graph), PartitionId::from(9));
        assert_eq!(csr.partition_id_raw(), PartitionId::from(9));
        assert_eq!(csr.vertex_count(), 1);
    }
}
