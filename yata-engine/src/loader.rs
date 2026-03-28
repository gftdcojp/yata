use std::collections::HashMap;
use yata_core::PartitionId;
use yata_cypher::Graph;
use yata_graph::GraphStore;
use yata_grin::{Mutable, PropValue, Topology};
use yata_store::MutableCsrStore;

// ── 3-Tier Blob Fetch: Disk Cache → R2 ──────────────────────────────

/// Resolve the disk cache directory from YATA_VINEYARD_DIR env var.
/// Returns `Some("{dir}/snap/fragment")` if set, None otherwise.
fn disk_cache_dir() -> Option<String> {
    std::env::var("YATA_VINEYARD_DIR").ok().map(|d| format!("{d}/snap/fragment"))
}

/// Fetch a blob by name with 3-tier strategy:
///   1. Disk cache (`YATA_VINEYARD_DIR/snap/fragment/{name}`) — ~100µs
///   2. R2 GET (`{prefix}snap/fragment/{name}`) — ~3-5ms
///   3. On R2 hit: write to disk cache for next time
///
/// Returns the blob bytes, or None if not found in either tier.
fn fetch_blob_cached(
    s3: &yata_s3::s3::S3Client,
    prefix: &str,
    name: &str,
    disk_dir: Option<&str>,
) -> Option<bytes::Bytes> {
    // Tier 1: disk cache
    if let Some(dir) = disk_dir {
        let path = format!("{dir}/{name}");
        if let Ok(data) = std::fs::read(&path) {
            tracing::trace!(name, "blob from disk cache");
            return Some(bytes::Bytes::from(data));
        }
    }

    // Tier 2: R2
    let key = format!("{prefix}snap/fragment/{name}");
    match s3.get_sync(&key) {
        Ok(Some(data)) => {
            // Write-through to disk cache
            if let Some(dir) = disk_dir {
                let path = format!("{dir}/{name}");
                // Ensure parent dir exists for chunked blob names
                if let Some(parent) = std::path::Path::new(&path).parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                if let Err(e) = std::fs::write(&path, &data[..]) {
                    tracing::trace!(name, error = %e, "disk cache write failed (non-fatal)");
                }
            }
            Some(data)
        }
        Ok(None) => {
            tracing::warn!(name, "blob not found in R2");
            None
        }
        Err(e) => {
            tracing::warn!(name, error = %e, "R2 blob fetch failed");
            None
        }
    }
}

/// Check if a full snapshot exists on disk (meta.json present).
fn disk_cache_has_snapshot(disk_dir: &str) -> bool {
    std::path::Path::new(&format!("{disk_dir}/meta.json")).exists()
}

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

/// Page-in ArrowFragment and restore to CSR.
///
/// 3-tier fetch: disk cache (`YATA_VINEYARD_DIR`) → R2 → write-through to disk.
/// Cold start with warm disk: ~100µs per blob (vs ~3-5ms R2 GET).
pub fn page_in_from_r2(
    s3: &yata_s3::s3::S3Client,
    prefix: &str,
    partition_id: PartitionId,
) -> Result<MutableCsrStore, String> {
    use yata_vineyard::blob::{BlobStore, MemoryBlobStore};

    let disk_dir = disk_cache_dir();
    let disk_ref = disk_dir.as_deref();

    // 1. Fetch fragment meta (disk → R2)
    let meta_bytes = fetch_blob_cached(s3, prefix, "meta.json", disk_ref)
        .ok_or_else(|| "no fragment meta.json in disk or R2".to_string())?;
    let meta: yata_vineyard::blob::ObjectMeta = serde_json::from_slice(&meta_bytes)
        .map_err(|e| format!("parse fragment meta: {e}"))?;

    // 2. Fetch all blobs (disk → R2, write-through)
    let blob_store = MemoryBlobStore::new();
    let mut disk_hits = 0u32;
    let mut r2_hits = 0u32;
    for name in meta.blobs.keys() {
        let was_on_disk = disk_ref.map_or(false, |d| {
            std::path::Path::new(&format!("{d}/{name}")).exists()
        });
        if let Some(data) = fetch_blob_cached(s3, prefix, name, disk_ref) {
            blob_store.put(name, data);
            if was_on_disk { disk_hits += 1; } else { r2_hits += 1; }
        }
    }

    // 3. Deserialize ArrowFragment
    let frag = yata_vineyard::ArrowFragment::deserialize(&meta, &blob_store)
        .map_err(|e| format!("ArrowFragment deserialize: {e}"))?;

    // 4. Restore CSR
    let csr = restore_csr_from_fragment(&frag, partition_id);
    tracing::info!(
        disk_hits, r2_hits,
        total_blobs = meta.blobs.len(),
        vertices = csr.vertex_count(),
        "page-in complete (disk cache → R2)"
    );
    Ok(csr)
}

// ── 2-Phase Cold Start: Topology-Only + On-Demand Property Enrichment ──

/// Phase 1: Topology-only page-in from R2.
///
/// Loads meta + schema + ivnums + ALL CSR topology (offsets + nbr_units).
/// Vertex properties are NOT loaded — stub vertices (label + VID only) are created.
/// This is ~25% of total data size and allows query acceptance immediately.
///
/// Returns (CSR, meta, schema, label→vid_offset map).
pub fn page_in_topology_from_r2(
    s3: &yata_s3::s3::S3Client,
    prefix: &str,
    partition_id: PartitionId,
) -> Result<(MutableCsrStore, yata_vineyard::blob::ObjectMeta, yata_vineyard::schema::PropertyGraphSchema, HashMap<String, u32>), String> {
    use yata_vineyard::blob::{BlobStore, MemoryBlobStore};

    let disk_dir = disk_cache_dir();
    let disk_ref = disk_dir.as_deref();

    // 1. Fetch meta
    let meta_bytes = fetch_blob_cached(s3, prefix, "meta.json", disk_ref)
        .ok_or("no fragment meta.json in disk or R2")?;
    let meta: yata_vineyard::blob::ObjectMeta = serde_json::from_slice(&meta_bytes)
        .map_err(|e| format!("parse fragment meta: {e}"))?;

    // 2. Fetch schema + ivnums (always needed)
    let blob_store = MemoryBlobStore::new();
    for name in ["schema", "ivnums"] {
        if let Some(data) = fetch_blob_cached(s3, prefix, name, disk_ref) {
            blob_store.put(name, data);
        }
    }

    let schema_bytes = blob_store.get("schema")
        .ok_or("schema blob missing")?;
    let schema: yata_vineyard::schema::PropertyGraphSchema =
        serde_json::from_slice(&schema_bytes)
            .map_err(|e| format!("parse schema: {e}"))?;

    // 3. Fetch ALL CSR topology + edge tables (small, always needed for traversal)
    for name in meta.blobs.keys() {
        if name.starts_with("edge_table") || name.starts_with("oe_") || name.starts_with("ie_") {
            if let Some(data) = fetch_blob_cached(s3, prefix, name, disk_ref) {
                blob_store.put(name, data);
            }
        }
    }

    // 4. Do NOT fetch any vertex_table blobs — stubs only.
    //    Build a filtered meta that excludes all vertex_table references.
    let mut topo_meta = meta.clone();
    for (i, _) in schema.vertex_entries.iter().enumerate() {
        let single_name = format!("vertex_table_{}", i);
        topo_meta.blobs.remove(&single_name);
        let chunks_key = format!("vertex_table_{}_chunks", i);
        if let Some(count) = meta.get_field(&chunks_key).and_then(|v| v.as_i64()) {
            for j in 0..count as usize {
                topo_meta.blobs.remove(&format!("vertex_table_{}_chunk_{}", i, j));
            }
        }
        topo_meta.fields.remove(&chunks_key);
    }

    // 5. Deserialize topology-only ArrowFragment (no vertex tables → empty vertex_tables vec)
    let frag = yata_vineyard::ArrowFragment::deserialize(&topo_meta, &blob_store)
        .map_err(|e| format!("topology deserialize: {e}"))?;

    // 6. Build CSR with stubs for ALL labels (VID ordering from ivnums)
    let no_labels: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let csr = restore_csr_selective(&frag, partition_id, &no_labels);

    // 7. Compute VID offsets per label
    let mut offsets = HashMap::new();
    let mut offset = 0u32;
    for entry in &schema.vertex_entries {
        offsets.insert(entry.label.clone(), offset);
        let count = yata_grin::Scannable::scan_vertices_by_label(&csr, &entry.label).len();
        offset += count as u32;
    }

    tracing::info!(
        vertices = csr.vertex_count(),
        edges = csr.edge_count(),
        labels = schema.vertex_entries.len(),
        "topology-only page-in complete (stubs, no properties)"
    );

    Ok((csr, meta, schema, offsets))
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

    let disk_dir = disk_cache_dir();
    let disk_ref = disk_dir.as_deref();

    // 1. Fetch meta (disk → R2)
    let meta_bytes = fetch_blob_cached(s3, prefix, "meta.json", disk_ref)
        .ok_or("no fragment meta.json in disk or R2")?;
    let meta: yata_vineyard::blob::ObjectMeta = serde_json::from_slice(&meta_bytes)
        .map_err(|e| format!("parse fragment meta: {e}"))?;

    // 2. Build selective blob store
    let blob_store = MemoryBlobStore::new();

    // Always fetch: schema, ivnums (disk → R2)
    for name in ["schema", "ivnums"] {
        if let Some(data) = fetch_blob_cached(s3, prefix, name, disk_ref) {
            blob_store.put(name, data);
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

    // 3. Fetch vertex tables for needed labels only (chunk-aware, disk → R2)
    for (i, _entry) in schema.vertex_entries.iter().enumerate() {
        if !needed_vlabel_ids.contains(&i) { continue; }

        let chunks_key = format!("vertex_table_{}_chunks", i);
        if let Some(count) = meta.get_field(&chunks_key).and_then(|v| v.as_i64()) {
            for j in 0..count as usize {
                let name = format!("vertex_table_{}_chunk_{}", i, j);
                if meta.blobs.contains_key(&name) {
                    if let Some(data) = fetch_blob_cached(s3, prefix, &name, disk_ref) {
                        blob_store.put(&name, data);
                    }
                }
            }
        } else {
            let name = format!("vertex_table_{}", i);
            if meta.blobs.contains_key(&name) {
                if let Some(data) = fetch_blob_cached(s3, prefix, &name, disk_ref) {
                    blob_store.put(&name, data);
                }
            }
        }
    }

    // 4. Always fetch edge tables + CSR topology (disk → R2)
    for name in meta.blobs.keys() {
        if name.starts_with("edge_table") || name.starts_with("oe_") || name.starts_with("ie_") {
            if let Some(data) = fetch_blob_cached(s3, prefix, name, disk_ref) {
                blob_store.put(name, data);
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

    let disk_dir = disk_cache_dir();
    let disk_ref = disk_dir.as_deref();

    // Fetch vertex_table (chunk-aware, disk → R2)
    let blob_store = MemoryBlobStore::new();
    let chunks_key = format!("vertex_table_{}_chunks", label_id);
    if let Some(count) = meta.get_field(&chunks_key).and_then(|v| v.as_i64()) {
        for j in 0..count as usize {
            let name = format!("vertex_table_{}_chunk_{}", label_id, j);
            if let Some(data) = fetch_blob_cached(s3, prefix, &name, disk_ref) {
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
        if let Some(data) = fetch_blob_cached(s3, prefix, &name, disk_ref) {
            let batch = yata_arrow::ipc_to_batch(&data)
                .map_err(|e| format!("vertex table decode: {e}"))?;
            apply_batch_properties(&batch, entry, csr, vid_offset);
        }
    }

    tracing::info!(label = label_name, vid_offset, "label properties enriched (disk → R2)");
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

    // ── cypher_to_prop conversion ────────────────────────────────────

    #[test]
    fn test_cypher_to_prop_null() {
        let v = cypher_to_prop(&yata_cypher::types::Value::Null);
        assert!(matches!(v, PropValue::Null));
    }

    #[test]
    fn test_cypher_to_prop_bool() {
        assert!(matches!(
            cypher_to_prop(&yata_cypher::types::Value::Bool(true)),
            PropValue::Bool(true)
        ));
        assert!(matches!(
            cypher_to_prop(&yata_cypher::types::Value::Bool(false)),
            PropValue::Bool(false)
        ));
    }

    #[test]
    fn test_cypher_to_prop_int() {
        let v = cypher_to_prop(&yata_cypher::types::Value::Int(42));
        assert_eq!(v, PropValue::Int(42));
    }

    #[test]
    fn test_cypher_to_prop_float() {
        if let PropValue::Float(f) = cypher_to_prop(&yata_cypher::types::Value::Float(3.14)) {
            assert!((f - 3.14).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn test_cypher_to_prop_string() {
        let v = cypher_to_prop(&yata_cypher::types::Value::Str("hello".into()));
        assert_eq!(v, PropValue::Str("hello".into()));
    }

    #[test]
    fn test_cypher_to_prop_list_falls_back_to_string() {
        let v = cypher_to_prop(&yata_cypher::types::Value::List(vec![
            yata_cypher::types::Value::Int(1),
        ]));
        // Lists are converted to string representation
        match v {
            PropValue::Str(_) => {}
            _ => panic!("expected Str for list fallback, got: {:?}", v),
        }
    }

    // ── rebuild_csr_from_graph ────────────────────────────────────────

    #[test]
    fn test_rebuild_csr_with_edges() {
        let mut graph = MemoryGraph::new();
        graph.add_node(NodeRef {
            id: "a".into(),
            labels: vec!["Person".into()],
            props: IndexMap::new(),
        });
        graph.add_node(NodeRef {
            id: "b".into(),
            labels: vec!["Person".into()],
            props: IndexMap::new(),
        });
        graph.add_rel(yata_cypher::RelRef {
            id: "e1".into(),
            src: "a".into(),
            dst: "b".into(),
            rel_type: "KNOWS".into(),
            props: IndexMap::new(),
        });

        let csr = rebuild_csr_from_graph(&QueryableGraph(graph));
        assert_eq!(csr.vertex_count(), 2);
        assert_eq!(csr.edge_count(), 1);
    }

    #[test]
    fn test_rebuild_csr_with_properties() {
        use yata_grin::Property;
        let mut graph = MemoryGraph::new();
        let mut props = IndexMap::new();
        props.insert("name".into(), yata_cypher::types::Value::Str("Alice".into()));
        props.insert("age".into(), yata_cypher::types::Value::Int(30));
        graph.add_node(NodeRef {
            id: "a".into(),
            labels: vec!["Person".into()],
            props,
        });

        let csr = rebuild_csr_from_graph(&QueryableGraph(graph));
        assert_eq!(csr.vertex_count(), 1);
        let vids = yata_grin::Scannable::scan_all_vertices(&csr);
        let name = Property::vertex_prop(&csr, vids[0], "name");
        assert_eq!(name, Some(PropValue::Str("Alice".into())));
    }

    #[test]
    fn test_rebuild_csr_empty_graph() {
        let graph = MemoryGraph::new();
        let csr = rebuild_csr_from_graph(&QueryableGraph(graph));
        assert_eq!(csr.vertex_count(), 0);
        assert_eq!(csr.edge_count(), 0);
    }

    // ── vertex_label_chunk_count ──────────────────────────────────────

    #[test]
    fn test_vertex_label_chunk_count_none_for_missing() {
        let meta = yata_vineyard::blob::ObjectMeta {
            id: yata_vineyard::blob::ObjectId(0),
            typename: String::new(),
            fields: HashMap::new(),
            members: HashMap::new(),
            blobs: HashMap::new(),
        };
        assert_eq!(vertex_label_chunk_count(&meta, 0), None);
    }

    #[test]
    fn test_vertex_label_chunk_count_some_when_present() {
        let mut meta = yata_vineyard::blob::ObjectMeta {
            id: yata_vineyard::blob::ObjectId(0),
            typename: String::new(),
            fields: HashMap::new(),
            members: HashMap::new(),
            blobs: HashMap::new(),
        };
        meta.fields.insert(
            "vertex_table_0_chunks".into(),
            yata_vineyard::blob::MetaValue::Int(5),
        );
        assert_eq!(vertex_label_chunk_count(&meta, 0), Some(5));
    }
}

// ── WAL Replay: Pipeline R2 → CSR Recovery ──────────────────────────

/// Replay Pipeline WAL records from R2 into a CSR store.
///
/// Pipeline WAL is stored at `pipeline/wal/{timestamp}/` in R2 as JSON files.
/// Each file contains an array of records with `ops` and `records` fields.
/// This function lists WAL files, parses them, and merges records into the CSR.
///
/// The `since_snapshot` timestamp (ms) is used to skip WAL entries older than
/// the last known snapshot. If 0, replays ALL WAL entries.
pub fn replay_wal_from_r2(
    s3: &yata_s3::s3::S3Client,
    csr: &mut MutableCsrStore,
    since_snapshot_ms: u64,
) -> Result<u64, String> {
    let wal_prefix = "pipeline/wal/";

    // List WAL objects in R2
    let objects = s3.list_sync(wal_prefix)
        .map_err(|e| format!("WAL list failed: {e}"))?;

    if objects.is_empty() {
        tracing::info!("WAL replay: no WAL files found");
        return Ok(0);
    }

    let mut replayed = 0u64;
    let mut errors = 0u64;

    for obj in &objects {
        let obj_key = &obj.key;
        // Fetch WAL JSON
        let data = match s3.get_sync(obj_key) {
            Ok(Some(d)) => d,
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(key = %obj_key, error = %e, "WAL replay: skip unreadable file");
                errors += 1;
                continue;
            }
        };

        // Parse WAL records (array of {seq, rev, cid, repo, ops, created, records})
        let records: Vec<serde_json::Value> = match serde_json::from_slice(&data) {
            Ok(v) => {
                if let serde_json::Value::Array(arr) = v { arr } else { vec![v] }
            }
            Err(e) => {
                tracing::warn!(key = %obj_key, error = %e, "WAL replay: skip unparseable file");
                errors += 1;
                continue;
            }
        };

        for record in &records {
            // Skip records older than snapshot
            let created = record.get("created").and_then(|v| v.as_u64()).unwrap_or(0);
            if since_snapshot_ms > 0 && created < since_snapshot_ms {
                continue;
            }

            // Parse ops: [{action, collection, rkey}]
            let ops_str = record.get("ops").and_then(|v| v.as_str()).unwrap_or("[]");
            let ops: Vec<serde_json::Value> = serde_json::from_str(ops_str).unwrap_or_default();

            // Parse records: {"collection/rkey": "json"}
            let records_str = record.get("records").and_then(|v| v.as_str()).unwrap_or("{}");
            let records_map: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(records_str).unwrap_or_default();

            let repo = record.get("repo").and_then(|v| v.as_str()).unwrap_or("");

            for op in &ops {
                let action = op.get("action").and_then(|v| v.as_str()).unwrap_or("");
                let collection = op.get("collection").and_then(|v| v.as_str()).unwrap_or("");
                let rkey = op.get("rkey").and_then(|v| v.as_str()).unwrap_or("");

                if action != "create" || collection.is_empty() || rkey.is_empty() {
                    continue;
                }

                // Derive label from collection (same as PDS collectionToLabel)
                let label = collection_to_label(collection);

                // Get record JSON
                let record_key = format!("{collection}/{rkey}");
                let record_json = match records_map.get(&record_key) {
                    Some(serde_json::Value::String(s)) => s.clone(),
                    Some(v) => v.to_string(),
                    None => continue,
                };

                // Build props from record
                let mut props: Vec<(&str, PropValue)> = vec![
                    ("rkey", PropValue::Str(rkey.to_string())),
                    ("collection", PropValue::Str(collection.to_string())),
                    ("repo", PropValue::Str(repo.to_string())),
                    ("sensitivity_ord", PropValue::Str("0".to_string())),
                ];

                // Base64 encode value (inline, no external crate)
                let value_b64_owned = simple_base64_encode(record_json.as_bytes());

                // Parse record JSON for top-level props (displayName, etc.)
                if let Ok(parsed) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&record_json) {
                    // Extract common props
                    for key in ["did", "display_name", "displayName", "description", "handle", "sensitivity", "status"] {
                        if let Some(serde_json::Value::String(v)) = parsed.get(key) {
                            let prop_key = if key == "displayName" { "display_name" } else { key };
                            props.push((prop_key, PropValue::Str(v.clone())));
                        }
                    }
                }

                // We need to push value_b64 as a prop but it was moved. Re-derive.
                // Actually, let's use a separate vec for owned strings
                let label_owned = label.clone();
                let rkey_pv = PropValue::Str(rkey.to_string());

                // Build final props with value_b64
                let mut final_props: Vec<(String, PropValue)> = props.iter().map(|(k,v)| (k.to_string(), v.clone())).collect();
                final_props.push(("value_b64".to_string(), PropValue::Str(value_b64_owned)));
                let final_refs: Vec<(&str, PropValue)> = final_props.iter().map(|(k,v)| (k.as_str(), v.clone())).collect();

                csr.merge_by_pk(&label_owned, "rkey", &rkey_pv, &final_refs);
                replayed += 1;
            }
        }
    }

    csr.commit();
    tracing::info!(replayed, errors, wal_files = objects.len(), "WAL replay complete");
    Ok(replayed)
}

/// Simple base64 encode (standard alphabet, with padding). No external crate needed.
fn simple_base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 { result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char); } else { result.push('='); }
        if chunk.len() > 2 { result.push(CHARS[(triple & 0x3F) as usize] as char); } else { result.push('='); }
    }
    result
}

/// Convert AT Protocol collection NSID to yata graph label (PascalCase).
/// e.g. "app.bsky.feed.post" → "Post", "ai.gftd.apps.shinshi.model" → "Model"
fn collection_to_label(collection: &str) -> String {
    let last = collection.rsplit('.').next().unwrap_or(collection);
    // Split by _ or - and PascalCase
    last.split(|c: char| c == '_' || c == '-')
        .filter(|s| !s.is_empty())
        .map(|s| {
            let mut c = s.chars();
            match c.next() {
                Some(first) => {
                    let mut r = first.to_uppercase().to_string();
                    r.extend(c);
                    r
                }
                None => String::new(),
            }
        })
        .collect()
}
