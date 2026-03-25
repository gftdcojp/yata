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

// ── Phase 3c: Label-Selective Page-In ──────────────────────────────

/// Selective page-in: fetch meta+schema+ivnums+CSR topology from R2,
/// but only fetch vertex property tables for `needed_labels`.
/// Non-needed labels get stub vertices (correct VID ordering, no properties).
///
/// Returns (CSR, meta, schema, labels_with_properties).
pub fn page_in_selective_from_r2(
    s3: &yata_s3::s3::S3Client,
    prefix: &str,
    partition_id: PartitionId,
    needed_labels: &std::collections::HashSet<String>,
) -> Result<(MutableCsrStore, yata_vineyard::blob::ObjectMeta, yata_vineyard::schema::PropertyGraphSchema, std::collections::HashSet<String>), String> {
    use yata_vineyard::blob::{BlobStore, MemoryBlobStore};

    // 1. Fetch meta
    let meta = fetch_fragment_meta(s3, prefix)?;

    // 2. Build selective blob store
    let blob_store = MemoryBlobStore::new();

    // Always fetch: schema, ivnums
    for name in ["schema", "ivnums"] {
        let key = format!("{prefix}snap/fragment/{name}");
        match s3.get_sync(&key) {
            Ok(Some(data)) => blob_store.put(name, data),
            Ok(None) => tracing::warn!(name, "R2 blob not found"),
            Err(e) => tracing::warn!(name, error = %e, "R2 blob fetch failed"),
        }
    }

    // Parse schema
    let schema_bytes = blob_store.get("schema")
        .ok_or("schema blob missing")?;
    let schema: yata_vineyard::schema::PropertyGraphSchema =
        serde_json::from_slice(&schema_bytes)
            .map_err(|e| format!("parse schema: {e}"))?;

    // Determine which vertex labels need properties
    let needed_vlabel_ids: std::collections::HashSet<usize> = schema.vertex_entries.iter()
        .enumerate()
        .filter(|(_, e)| needed_labels.contains(&e.label))
        .map(|(i, _)| i)
        .collect();

    // 3. Fetch vertex tables for needed labels only (chunk-aware)
    for (i, _entry) in schema.vertex_entries.iter().enumerate() {
        if !needed_vlabel_ids.contains(&i) { continue; }

        let chunks_key = format!("vertex_table_{}_chunks", i);
        if let Some(count) = meta.get_field(&chunks_key).and_then(|v| v.as_i64()) {
            for j in 0..count as usize {
                let name = format!("vertex_table_{}_chunk_{}", i, j);
                if meta.blobs.contains_key(&name) {
                    let key = format!("{prefix}snap/fragment/{name}");
                    if let Ok(Some(data)) = s3.get_sync(&key) {
                        blob_store.put(&name, data);
                    }
                }
            }
        } else {
            let name = format!("vertex_table_{}", i);
            if meta.blobs.contains_key(&name) {
                let key = format!("{prefix}snap/fragment/{name}");
                if let Ok(Some(data)) = s3.get_sync(&key) {
                    blob_store.put(&name, data);
                }
            }
        }
    }

    // 4. Always fetch edge tables + CSR topology (small relative to vertex properties)
    for name in meta.blobs.keys() {
        if name.starts_with("edge_table") || name.starts_with("oe_") || name.starts_with("ie_") {
            let key = format!("{prefix}snap/fragment/{name}");
            match s3.get_sync(&key) {
                Ok(Some(data)) => blob_store.put(name, data),
                Ok(None) => tracing::warn!(name, "R2 CSR blob not found"),
                Err(e) => tracing::warn!(name, error = %e, "R2 CSR blob fetch failed"),
            }
        }
    }

    // 5. Build filtered meta (remove blob refs for non-fetched vertex tables)
    let mut filtered_meta = meta.clone();
    for (i, _) in schema.vertex_entries.iter().enumerate() {
        if needed_vlabel_ids.contains(&i) { continue; }
        // Remove references to non-fetched vertex table blobs
        let single_name = format!("vertex_table_{}", i);
        filtered_meta.blobs.remove(&single_name);
        let chunks_key = format!("vertex_table_{}_chunks", i);
        if let Some(count) = meta.get_field(&chunks_key).and_then(|v| v.as_i64()) {
            for j in 0..count as usize {
                filtered_meta.blobs.remove(&format!("vertex_table_{}_chunk_{}", i, j));
            }
        }
        // Remove chunk count field so deserialize treats as "no data"
        filtered_meta.fields.remove(&chunks_key);
    }

    // 6. Deserialize partial ArrowFragment
    let frag = yata_vineyard::ArrowFragment::deserialize(&filtered_meta, &blob_store)
        .map_err(|e| format!("ArrowFragment selective deserialize: {e}"))?;

    // 7. Restore CSR with stubs for non-loaded labels
    let csr = restore_csr_selective(&frag, partition_id, &needed_vlabel_ids);

    let loaded: std::collections::HashSet<String> = needed_vlabel_ids.iter()
        .filter_map(|&i| schema.vertex_entries.get(i).map(|e| e.label.clone()))
        .collect();

    tracing::info!(
        needed = needed_labels.len(),
        loaded = loaded.len(),
        total_labels = schema.vertex_entries.len(),
        vertices = csr.vertex_count(),
        edges = csr.edge_count(),
        "selective page-in complete"
    );

    Ok((csr, meta, schema, loaded))
}

/// Restore CSR from ArrowFragment, using stubs for non-loaded vertex labels.
///
/// Labels in `loaded_vlabel_ids` get full properties from vertex_table.
/// Other labels get `ivnums[i]` stub vertices (just label, no props) to maintain VID ordering.
fn restore_csr_selective(
    frag: &yata_vineyard::ArrowFragment,
    partition_id: PartitionId,
    loaded_vlabel_ids: &std::collections::HashSet<usize>,
) -> MutableCsrStore {
    let mut csr = MutableCsrStore::new_with_partition_id(partition_id);

    // Restore vertices per label
    for (vlabel_id, entry) in frag.schema.vertex_entries.iter().enumerate() {
        let label = &entry.label;
        if loaded_vlabel_ids.contains(&vlabel_id) {
            // Full restore with properties
            if let Some(batch) = frag.vertex_table(vlabel_id) {
                for row in 0..batch.num_rows() {
                    let mut props: Vec<(&str, PropValue)> = Vec::new();
                    for prop_def in &entry.props {
                        if let Some(col) = batch.column_by_name(&prop_def.name) {
                            let val = extract_arrow_value(col, row);
                            props.push((&prop_def.name, val));
                        }
                    }
                    csr.add_vertex(label, &props);
                }
            }
        } else {
            // Stub vertices: maintain VID ordering without properties
            let count = frag.inner_vertex_num(vlabel_id);
            for _ in 0..count {
                csr.add_vertex(label, &[]);
            }
        }
    }

    // Restore edges from NbrUnit CSR topology (always loaded)
    for (elabel_id, edge_entry) in frag.schema.edge_entries.iter().enumerate() {
        let edge_label = &edge_entry.label;
        for (vlabel_id, _) in frag.schema.vertex_entries.iter().enumerate() {
            let vcount = frag.inner_vertex_num(vlabel_id);
            for vid in 0..vcount {
                if let Some(nbrs) = frag.out_neighbors(vlabel_id, elabel_id, vid) {
                    for nbr in nbrs {
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
        loaded_labels = loaded_vlabel_ids.len(),
        total_labels = frag.schema.vertex_entries.len(),
        "CSR restored (selective)"
    );
    csr
}

/// Enrich an already-loaded CSR with vertex properties for a specific label.
/// Fetches the vertex_table from R2 and calls `set_vertex_prop` on existing stub vertices.
///
/// `vid_offset` = first VID of this label (sum of ivnums for all preceding labels).
pub fn enrich_label_from_r2(
    s3: &yata_s3::s3::S3Client,
    prefix: &str,
    meta: &yata_vineyard::blob::ObjectMeta,
    schema: &yata_vineyard::schema::PropertyGraphSchema,
    label_name: &str,
    csr: &mut MutableCsrStore,
    vid_offset: u32,
) -> Result<(), String> {
    use yata_vineyard::blob::{BlobStore, MemoryBlobStore};

    let (label_id, entry) = schema.vertex_entries.iter().enumerate()
        .find(|(_, e)| e.label == label_name)
        .ok_or_else(|| format!("label not in schema: {label_name}"))?;

    // Fetch vertex_table (chunk-aware)
    let blob_store = MemoryBlobStore::new();
    let chunks_key = format!("vertex_table_{}_chunks", label_id);
    if let Some(count) = meta.get_field(&chunks_key).and_then(|v| v.as_i64()) {
        for j in 0..count as usize {
            let name = format!("vertex_table_{}_chunk_{}", label_id, j);
            let key = format!("{prefix}snap/fragment/{name}");
            if let Ok(Some(data)) = s3.get_sync(&key) {
                blob_store.put(&name, data);
            }
        }
        // Deserialize and concatenate chunks
        let batches = (0..count as usize).filter_map(|j| {
            let name = format!("vertex_table_{}_chunk_{}", label_id, j);
            let ipc = blob_store.get(&name)?;
            yata_arrow::ipc_to_batch(&ipc).ok()
        }).collect::<Vec<_>>();
        if batches.is_empty() { return Ok(()); }
        let schema_ref = batches[0].schema();
        let batch = arrow::compute::concat_batches(&schema_ref, &batches)
            .map_err(|e| format!("concat chunks: {e}"))?;
        apply_batch_properties(&batch, entry, csr, vid_offset);
    } else {
        let name = format!("vertex_table_{}", label_id);
        let key = format!("{prefix}snap/fragment/{name}");
        if let Ok(Some(data)) = s3.get_sync(&key) {
            let batch = yata_arrow::ipc_to_batch(&data)
                .map_err(|e| format!("vertex table decode: {e}"))?;
            apply_batch_properties(&batch, entry, csr, vid_offset);
        }
    }

    tracing::info!(label = label_name, vid_offset, "label properties enriched from R2");
    Ok(())
}

/// Apply Arrow RecordBatch properties to existing CSR vertices via set_vertex_prop.
fn apply_batch_properties(
    batch: &arrow::record_batch::RecordBatch,
    entry: &yata_vineyard::schema::SchemaEntry,
    csr: &mut MutableCsrStore,
    vid_offset: u32,
) {
    for row in 0..batch.num_rows() {
        let vid = vid_offset + row as u32;
        for prop_def in &entry.props {
            if let Some(col) = batch.column_by_name(&prop_def.name) {
                let val = extract_arrow_value(col, row);
                if !matches!(val, PropValue::Null) {
                    csr.set_vertex_prop(vid, &prop_def.name, val);
                }
            }
        }
    }
}

/// Extract a PropValue from an Arrow column at a given row.
fn extract_arrow_value(col: &dyn arrow::array::Array, row: usize) -> PropValue {
    use arrow::array::{Int64Array, Float64Array, StringArray, BooleanArray};
    if col.is_null(row) {
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
    }
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
