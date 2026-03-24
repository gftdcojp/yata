//! GraphScope Flex-style snapshot persistence.
//!
//! Replaces MDAG CAS as the default persistence path.
//! Serializes the full MutableCsrStore to per-label Arrow IPC files + CSR topology,
//! uploads to R2 as a single atomic snapshot. SIGTERM-safe (synchronous flush).
//!
//! R2 layout:
//!   {prefix}snap/manifest.json     — snapshot metadata (counts, labels, LSN)
//!   {prefix}snap/v/{label}.arrow   — per-label vertex properties (Arrow IPC File)
//!   {prefix}snap/e/{label}.arrow   — per-label edge properties (Arrow IPC File)
//!   {prefix}snap/topo.bin          — CSR topology (offsets + edge_ids, bincode)
//!   {prefix}snap/schema.json       — schema (labels, PKs)

use std::collections::HashMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use yata_core::{
    GLOBAL_EID_PROP_KEY, GLOBAL_VID_PROP_KEY, PartitionId, SNAPSHOT_FORMAT_VERSION,
    SNAPSHOT_SCHEMA_VERSION,
};
use yata_grin::*;
use yata_store::MutableCsrStore;

use yata_store::arrow_codec;
use yata_store::blocks::{EdgeBlock, LabelEdgeGroup, LabelVertexGroup, VertexBlock};

/// Snapshot manifest stored as `{prefix}snap/manifest.json`.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub version: u32,
    pub partition_id: PartitionId,
    pub wal_lsn: u64,
    pub timestamp_ns: i64,
    pub vertex_count: u64,
    pub edge_count: u64,
    pub vertex_labels: Vec<String>,
    pub edge_labels: Vec<String>,
    /// Total partition count at snapshot time (1 for single-partition mode).
    #[serde(default = "default_partition_count")]
    pub partition_count: u32,
    /// Number of global→local ID mappings stored.
    #[serde(default)]
    pub global_map_count: u64,
}

fn default_partition_count() -> u32 {
    1
}

/// Schema stored as `{prefix}snap/schema.json`.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotSchema {
    pub version: u32,
    pub vertex_labels: Vec<String>,
    pub edge_labels: Vec<String>,
    pub vertex_primary_keys: HashMap<String, String>,
}

/// A serialized snapshot ready for upload.
#[derive(Debug)]
pub struct SnapshotBundle {
    pub manifest: SnapshotManifest,
    pub schema: SnapshotSchema,
    /// label -> Arrow IPC bytes
    pub vertex_groups: Vec<(String, Bytes)>,
    /// label -> Arrow IPC bytes
    pub edge_groups: Vec<(String, Bytes)>,
    /// CSR topology (bincode-serialized offsets + edge_ids)
    pub topology: Bytes,
}

/// Serialize a MutableCsrStore to a SnapshotBundle.
pub fn serialize_snapshot(store: &MutableCsrStore, wal_lsn: u64) -> Result<SnapshotBundle, String> {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    let v_labels: Vec<String> = <MutableCsrStore as Schema>::vertex_labels(store);
    let e_labels: Vec<String> = <MutableCsrStore as Schema>::edge_labels(store);

    // ── Vertex groups (per-label Arrow IPC) ──
    let all_vids: Vec<u32> = (0..store.vertex_count_raw())
        .filter(|vid| {
            store
                .vertex_alive_raw()
                .get(*vid as usize)
                .copied()
                .unwrap_or(false)
        })
        .collect();

    let mut vertex_by_label: HashMap<String, Vec<VertexBlock>> = HashMap::new();
    for &vid in &all_vids {
        let labels = <MutableCsrStore as Property>::vertex_labels(store, vid);
        let primary_label = labels.first().cloned().unwrap_or_default();
        let prop_keys = if let Some(first_label) = labels.first() {
            store.vertex_prop_keys(first_label)
        } else {
            Vec::new()
        };
        let mut props: Vec<(String, PropValue)> = prop_keys
            .iter()
            .filter_map(|k| store.vertex_prop(vid, k).map(|v| (k.clone(), v)))
            .collect();
        if let Some(global_vid) = store.global_vid(vid) {
            props.push((
                GLOBAL_VID_PROP_KEY.to_string(),
                PropValue::Int(global_vid.get() as i64),
            ));
        }
        props.sort_by(|a, b| a.0.cmp(&b.0));
        vertex_by_label
            .entry(primary_label)
            .or_default()
            .push(VertexBlock {
                vid,
                labels: labels.clone(),
                props,
            });
    }

    let mut vertex_groups = Vec::new();
    for label in &v_labels {
        if let Some(mut vertices) = vertex_by_label.remove(label.as_str()) {
            vertices.sort_by_key(|v| v.vid);
            let group = LabelVertexGroup {
                count: vertices.len() as u32,
                label: label.clone(),
                vertices,
            };
            let arrow_bytes = arrow_codec::encode_vertex_group(&group)
                .map_err(|e| format!("encode vertex group {label}: {e}"))?;
            vertex_groups.push((label.clone(), Bytes::from(arrow_bytes)));
        }
    }

    // ── Edge groups (per-label Arrow IPC) ──
    let mut edge_by_label: HashMap<String, Vec<EdgeBlock>> = HashMap::new();
    for eid in 0..store.edge_count_raw() as usize {
        if !store.edge_alive_raw().get(eid).copied().unwrap_or(false) {
            continue;
        }
        let label = store
            .edge_labels_raw()
            .get(eid)
            .cloned()
            .unwrap_or_default();
        let src = store.edge_src_raw().get(eid).copied().unwrap_or(0);
        let dst = store.edge_dst_raw().get(eid).copied().unwrap_or(0);
        let mut props: Vec<(String, PropValue)> = store
            .edge_props_raw()
            .get(eid)
            .map(|m| {
                let mut v: Vec<_> = m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                v.sort_by(|a, b| a.0.cmp(&b.0));
                v
            })
            .unwrap_or_default();
        if let Some(global_eid) = store.global_eid(eid as u32) {
            props.push((
                GLOBAL_EID_PROP_KEY.to_string(),
                PropValue::Int(global_eid.get() as i64),
            ));
            props.sort_by(|a, b| a.0.cmp(&b.0));
        }
        edge_by_label
            .entry(label.clone())
            .or_default()
            .push(EdgeBlock {
                edge_id: eid as u32,
                src,
                dst,
                label,
                props,
            });
    }

    let mut edge_groups = Vec::new();
    for label in &e_labels {
        if let Some(mut edges) = edge_by_label.remove(label.as_str()) {
            edges.sort_by(|a, b| (a.src, a.dst, a.edge_id).cmp(&(b.src, b.dst, b.edge_id)));
            let group = LabelEdgeGroup {
                count: edges.len() as u32,
                label: label.clone(),
                edges,
            };
            let arrow_bytes = arrow_codec::encode_edge_group(&group)
                .map_err(|e| format!("encode edge group {label}: {e}"))?;
            edge_groups.push((label.clone(), Bytes::from(arrow_bytes)));
        }
    }

    // ── CSR topology (GraphScope .adj equivalent) ──
    let topo = serialize_topology(store);

    let manifest = SnapshotManifest {
        version: SNAPSHOT_FORMAT_VERSION,
        partition_id: store.partition_id_raw(),
        wal_lsn,
        timestamp_ns: now_ns,
        vertex_count: all_vids.len() as u64,
        edge_count: store.edge_count_raw() as u64,
        vertex_labels: v_labels.clone(),
        edge_labels: e_labels.clone(),
        partition_count: 1,
        global_map_count: store.global_map().len() as u64,
    };

    let schema = SnapshotSchema {
        version: SNAPSHOT_SCHEMA_VERSION,
        vertex_labels: v_labels,
        edge_labels: e_labels,
        vertex_primary_keys: store.vertex_pk_raw().clone(),
    };

    Ok(SnapshotBundle {
        manifest,
        schema,
        vertex_groups,
        edge_groups,
        topology: topo,
    })
}

pub fn restore_snapshot_bundle(bundle: &SnapshotBundle) -> Result<MutableCsrStore, String> {
    if bundle.manifest.version != SNAPSHOT_FORMAT_VERSION {
        return Err(format!(
            "snapshot version mismatch: expected {}, got {}",
            SNAPSHOT_FORMAT_VERSION, bundle.manifest.version
        ));
    }
    if bundle.schema.version != SNAPSHOT_SCHEMA_VERSION {
        return Err(format!(
            "snapshot schema version mismatch: expected {}, got {}",
            SNAPSHOT_SCHEMA_VERSION, bundle.schema.version
        ));
    }
    let schema = &bundle.schema;

    let mut store = MutableCsrStore::new_with_partition_id(bundle.manifest.partition_id);

    // Set primary keys
    for (label, pk) in &schema.vertex_primary_keys {
        store.set_vertex_primary_key(label, pk);
    }

    // Load vertex groups
    for (label, data) in &bundle.vertex_groups {
        let group = arrow_codec::decode_vertex_group(data)
            .map_err(|e| format!("decode vertex group {label}: {e}"))?;
        for vb in &group.vertices {
            let mut props_owned = vb.props.clone();
            let global_vid = props_owned
                .iter()
                .position(|(k, _)| k == GLOBAL_VID_PROP_KEY)
                .and_then(|idx| match props_owned.remove(idx).1 {
                    PropValue::Int(v) if v >= 0 => Some(v as u64),
                    _ => None,
                });
            let props: Vec<(&str, PropValue)> = props_owned
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            let labels = if vb.labels.is_empty() {
                vec![group.label.clone()]
            } else {
                vb.labels.clone()
            };
            store.add_vertex_with_labels_and_optional_global_id(
                global_vid.map(Into::into),
                &labels,
                &props,
            );
        }
    }

    // Load edge groups
    for (label, data) in &bundle.edge_groups {
        let group = arrow_codec::decode_edge_group(data)
            .map_err(|e| format!("decode edge group {label}: {e}"))?;
        for eb in &group.edges {
            let mut props_owned = eb.props.clone();
            let global_eid = props_owned
                .iter()
                .position(|(k, _)| k == GLOBAL_EID_PROP_KEY)
                .and_then(|idx| match props_owned.remove(idx).1 {
                    PropValue::Int(v) if v >= 0 => Some(v as u64),
                    _ => None,
                });
            let props: Vec<(&str, PropValue)> = props_owned
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            store.add_edge_with_optional_global_id(
                global_eid.map(Into::into),
                eb.src,
                eb.dst,
                &eb.label,
                &props,
            );
        }
    }

    // Commit to rebuild CSR from loaded edges
    store.commit();

    Ok(store)
}

// ── CSR topology serialization ──

#[derive(Serialize, Deserialize)]
struct TopologyData {
    out_csr: HashMap<String, CsrSegmentData>,
    in_csr: HashMap<String, CsrSegmentData>,
}

#[derive(Serialize, Deserialize)]
struct CsrSegmentData {
    offsets: Vec<u32>,
    edge_ids: Vec<u32>,
}

/// Serialize CSR topology to bytes (public for partition_snapshot reuse).
pub fn serialize_topology_bytes(store: &MutableCsrStore) -> Bytes {
    serialize_topology(store)
}

fn serialize_topology(store: &MutableCsrStore) -> Bytes {
    let mut out = HashMap::new();
    for (label, seg) in store.out_csr_raw() {
        out.insert(
            label.clone(),
            CsrSegmentData {
                offsets: seg.offsets.clone(),
                edge_ids: seg.edge_ids.clone(),
            },
        );
    }
    let mut inc = HashMap::new();
    for (label, seg) in store.in_csr_raw() {
        inc.insert(
            label.clone(),
            CsrSegmentData {
                offsets: seg.offsets.clone(),
                edge_ids: seg.edge_ids.clone(),
            },
        );
    }
    let topo = TopologyData {
        out_csr: out,
        in_csr: inc,
    };
    Bytes::from(serde_json::to_vec(&topo).unwrap_or_default())
}

// ── Vineyard-based snapshot ─────────────────────────────────────────

use yata_store::vineyard::{BlobType, FragmentManifest, ObjectMeta, VineyardStore};

/// Serialize a MutableCsrStore into Vineyard blobs and return a FragmentManifest.
/// Each per-label Arrow IPC chunk is stored as a Vineyard object. The CSR topology
/// is also stored. The caller can then upload individual blobs to R2 from the Vineyard cache.
pub fn serialize_snapshot_to_vineyard(
    store: &MutableCsrStore,
    vineyard: &dyn VineyardStore,
    wal_lsn: u64,
) -> Result<FragmentManifest, String> {
    let bundle = serialize_snapshot(store, wal_lsn)?;
    let now_ns = bundle.manifest.timestamp_ns;
    let partition_id = bundle.manifest.partition_id.get();

    let mut vertex_labels_map = HashMap::new();
    for (label, data) in &bundle.vertex_groups {
        let row_count = arrow_codec::decode_vertex_group(data)
            .map(|g| g.vertices.len())
            .unwrap_or(0);
        let mut fields = HashMap::new();
        fields.insert("row_count".to_string(), row_count.to_string());
        fields.insert("wal_lsn".to_string(), wal_lsn.to_string());
        let meta = ObjectMeta {
            id: yata_store::ObjectId(0),
            blob_type: BlobType::ArrowVertexGroup,
            label: label.clone(),
            partition_id,
            size_bytes: 0,
            r2_key: format!("snap/v/{label}.arrow"),
            fields,
            created_at: now_ns,
        };
        let id = vineyard.put(meta, data.clone());
        vertex_labels_map.insert(label.clone(), id);
    }

    let mut edge_labels_map = HashMap::new();
    for (label, data) in &bundle.edge_groups {
        let row_count = arrow_codec::decode_edge_group(data)
            .map(|g| g.edges.len())
            .unwrap_or(0);
        let mut fields = HashMap::new();
        fields.insert("row_count".to_string(), row_count.to_string());
        fields.insert("wal_lsn".to_string(), wal_lsn.to_string());
        let meta = ObjectMeta {
            id: yata_store::ObjectId(0),
            blob_type: BlobType::ArrowEdgeGroup,
            label: label.clone(),
            partition_id,
            size_bytes: 0,
            r2_key: format!("snap/e/{label}.arrow"),
            fields,
            created_at: now_ns,
        };
        let id = vineyard.put(meta, data.clone());
        edge_labels_map.insert(label.clone(), id);
    }

    // CSR topology
    let csr_object = if !bundle.topology.is_empty() {
        let meta = ObjectMeta {
            id: yata_store::ObjectId(0),
            blob_type: BlobType::CsrTopology,
            label: String::new(),
            partition_id,
            size_bytes: 0,
            r2_key: "snap/topo.bin".to_string(),
            fields: HashMap::new(),
            created_at: now_ns,
        };
        Some(vineyard.put(meta, bundle.topology.clone()))
    } else {
        None
    };

    Ok(FragmentManifest {
        vertex_labels: vertex_labels_map,
        edge_labels: edge_labels_map,
        csr_object,
        schema_object: None,
        partition_id,
        timestamp_ns: now_ns,
        vertex_count: bundle.manifest.vertex_count,
        edge_count: bundle.manifest.edge_count,
    })
}

/// Chunk size for Arrow IPC blob splitting (Phase 3 design: 1兆 scale — NOT WIRED into page-in).
/// Each chunk contains at most this many vertices/edges.
/// R2 key: snap/v/{label}/chunk_{N}.arrow
/// NOTE: serialize_snapshot_chunked_to_vineyard exists but is NOT called from any page-in path.
pub const CHUNK_SIZE: usize = 100_000;

/// Serialize with chunk splitting: each per-label group is split into CHUNK_SIZE chunks.
/// Returns chunk keys → Vineyard blob IDs (for R2 upload).
pub fn serialize_snapshot_chunked_to_vineyard(
    store: &MutableCsrStore,
    vineyard: &dyn VineyardStore,
    wal_lsn: u64,
) -> Result<(FragmentManifest, Vec<String>), String> {
    let bundle = serialize_snapshot(store, wal_lsn)?;
    let now_ns = bundle.manifest.timestamp_ns;
    let partition_id = bundle.manifest.partition_id.get();

    let mut vertex_labels_map = HashMap::new();
    let mut chunk_keys = Vec::new();

    for (label, data) in &bundle.vertex_groups {
        let group = arrow_codec::decode_vertex_group(data)
            .map_err(|e| format!("decode vertex group {label}: {e}"))?;

        if group.vertices.len() <= CHUNK_SIZE {
            // Small label: single blob (no chunking)
            let meta = ObjectMeta {
                id: yata_store::ObjectId(0),
                blob_type: BlobType::ArrowVertexGroup,
                label: label.clone(),
                partition_id,
                size_bytes: 0,
                r2_key: format!("snap/v/{label}.arrow"),
                fields: HashMap::new(),
                created_at: now_ns,
            };
            let id = vineyard.put(meta, data.clone());
            vertex_labels_map.insert(label.clone(), id);
            chunk_keys.push(format!("snap/v/{label}.arrow"));
        } else {
            // Large label: split into chunks
            let chunks: Vec<&[yata_store::blocks::VertexBlock]> =
                group.vertices.chunks(CHUNK_SIZE).collect();
            for (i, chunk) in chunks.iter().enumerate() {
                let chunk_group = yata_store::blocks::LabelVertexGroup {
                    count: chunk.len() as u32,
                    label: label.clone(),
                    vertices: chunk.to_vec(),
                };
                let chunk_bytes = arrow_codec::encode_vertex_group(&chunk_group)
                    .map_err(|e| format!("encode chunk {label}/{i}: {e}"))?;
                let r2_key = format!("snap/v/{label}/chunk_{i:06}.arrow");
                let meta = ObjectMeta {
                    id: yata_store::ObjectId(0),
                    blob_type: BlobType::ArrowVertexGroup,
                    label: format!("{label}__chunk_{i}"),
                    partition_id,
                    size_bytes: 0,
                    r2_key: r2_key.clone(),
                    fields: HashMap::new(),
                    created_at: now_ns,
                };
                let id = vineyard.put(meta, Bytes::from(chunk_bytes));
                // First chunk becomes the label entry in manifest
                if i == 0 {
                    vertex_labels_map.insert(label.clone(), id);
                }
                chunk_keys.push(r2_key);
            }
        }
    }

    let mut edge_labels_map = HashMap::new();
    for (label, data) in &bundle.edge_groups {
        let meta = ObjectMeta {
            id: yata_store::ObjectId(0),
            blob_type: BlobType::ArrowEdgeGroup,
            label: label.clone(),
            partition_id,
            size_bytes: 0,
            r2_key: format!("snap/e/{label}.arrow"),
            fields: HashMap::new(),
            created_at: now_ns,
        };
        let id = vineyard.put(meta, data.clone());
        edge_labels_map.insert(label.clone(), id);
        chunk_keys.push(format!("snap/e/{label}.arrow"));
    }

    let manifest = FragmentManifest {
        vertex_labels: vertex_labels_map,
        edge_labels: edge_labels_map,
        csr_object: None,
        schema_object: None,
        partition_id,
        timestamp_ns: now_ns,
        vertex_count: bundle.manifest.vertex_count,
        edge_count: bundle.manifest.edge_count,
    };

    Ok((manifest, chunk_keys))
}

/// Restore a MutableCsrStore from Vineyard blobs referenced by a FragmentManifest.
pub fn restore_snapshot_from_vineyard(
    vineyard: &dyn VineyardStore,
    manifest: &FragmentManifest,
) -> Result<MutableCsrStore, String> {
    let mut store =
        MutableCsrStore::new_with_partition_id(yata_core::PartitionId::from(manifest.partition_id));

    // Load vertex groups
    for (label, &obj_id) in &manifest.vertex_labels {
        let data = vineyard
            .get_blob(obj_id)
            .ok_or_else(|| format!("vineyard blob missing for vertex label {label}"))?;
        let group = arrow_codec::decode_vertex_group(&data)
            .map_err(|e| format!("decode vertex group {label}: {e}"))?;
        for vb in &group.vertices {
            let mut props_owned = vb.props.clone();
            let global_vid = props_owned
                .iter()
                .position(|(k, _)| k == GLOBAL_VID_PROP_KEY)
                .and_then(|idx| match props_owned.remove(idx).1 {
                    PropValue::Int(v) if v >= 0 => Some(v as u64),
                    _ => None,
                });
            let props: Vec<(&str, PropValue)> = props_owned
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            let labels = if vb.labels.is_empty() {
                vec![group.label.clone()]
            } else {
                vb.labels.clone()
            };
            store.add_vertex_with_labels_and_optional_global_id(
                global_vid.map(Into::into),
                &labels,
                &props,
            );
        }
    }

    // Load edge groups
    for (label, &obj_id) in &manifest.edge_labels {
        let data = vineyard
            .get_blob(obj_id)
            .ok_or_else(|| format!("vineyard blob missing for edge label {label}"))?;
        let group = arrow_codec::decode_edge_group(&data)
            .map_err(|e| format!("decode edge group {label}: {e}"))?;
        for eb in &group.edges {
            let mut props_owned = eb.props.clone();
            let global_eid = props_owned
                .iter()
                .position(|(k, _)| k == GLOBAL_EID_PROP_KEY)
                .and_then(|idx| match props_owned.remove(idx).1 {
                    PropValue::Int(v) if v >= 0 => Some(v as u64),
                    _ => None,
                });
            let props: Vec<(&str, PropValue)> = props_owned
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            store.add_edge_with_optional_global_id(
                global_eid.map(Into::into),
                eb.src,
                eb.dst,
                &eb.label,
                &props,
            );
        }
    }

    store.commit();
    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_store::EdgeVineyard;

    #[test]
    fn test_serialize_empty_snapshot() {
        let store = MutableCsrStore::new();
        let bundle = serialize_snapshot(&store, 0).unwrap();
        assert_eq!(bundle.manifest.version, SNAPSHOT_FORMAT_VERSION);
        assert_eq!(bundle.manifest.partition_id, PartitionId::from(0));
        assert_eq!(bundle.schema.version, SNAPSHOT_SCHEMA_VERSION);
        assert_eq!(bundle.manifest.vertex_count, 0);
        assert_eq!(bundle.manifest.edge_count, 0);
        assert!(bundle.vertex_groups.is_empty());
        assert!(bundle.edge_groups.is_empty());
    }

    #[test]
    fn test_serialize_snapshot_with_data() {
        let mut store = MutableCsrStore::new_partition(7);
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Bob".into())),
                ("age", PropValue::Int(25)),
            ],
        );
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.add_edge(0, 1, "KNOWS", &[("since", PropValue::Int(2020))]);
        store.add_edge(0, 2, "WORKS_AT", &[]);
        store.commit();

        let bundle = serialize_snapshot(&store, 42).unwrap();
        assert_eq!(bundle.manifest.version, SNAPSHOT_FORMAT_VERSION);
        assert_eq!(bundle.manifest.partition_id, PartitionId::from(7));
        assert_eq!(bundle.schema.version, SNAPSHOT_SCHEMA_VERSION);
        assert_eq!(bundle.manifest.vertex_count, 3);
        assert_eq!(bundle.manifest.wal_lsn, 42);
        assert_eq!(bundle.vertex_groups.len(), 2); // Person, Company
        assert_eq!(bundle.edge_groups.len(), 2); // KNOWS, WORKS_AT
        assert!(!bundle.topology.is_empty());
    }

    #[test]
    fn test_snapshot_preserves_global_vid() {
        let mut store = MutableCsrStore::new();
        let labels = vec!["Person".to_string(), "VIP".to_string()];
        store.add_vertex_with_labels_and_global_id(
            99u64.into(),
            &labels,
            &[("name", PropValue::Str("Alice".into()))],
        );
        store.commit();

        let bundle = serialize_snapshot(&store, 7).unwrap();
        let person_group = bundle
            .vertex_groups
            .iter()
            .find(|(label, _)| label == "Person")
            .unwrap();
        let decoded = arrow_codec::decode_vertex_group(&person_group.1).unwrap();
        assert!(
            decoded.vertices[0]
                .props
                .iter()
                .any(|(k, _)| k == GLOBAL_VID_PROP_KEY)
        );
    }

    #[test]
    fn test_snapshot_restore_bundle_roundtrip_preserves_global_vid() {
        let mut store = MutableCsrStore::new_partition(5);
        let labels = vec!["Person".to_string(), "VIP".to_string()];
        let a = store.add_vertex_with_labels_and_global_id(
            99u64.into(),
            &labels,
            &[("name", PropValue::Str("Alice".into()))],
        );
        let b = store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_edge(a, b, "KNOWS", &[("since", PropValue::Int(2024))]);
        store.commit();

        let bundle = serialize_snapshot(&store, 11).unwrap();
        let restored = restore_snapshot_bundle(&bundle).unwrap();

        assert_eq!(restored.vertex_count(), 2);
        assert_eq!(restored.edge_count(), 1);
        assert_eq!(restored.partition_id_raw(), PartitionId::from(5));
        assert_eq!(restored.local_vid(99u64.into()).map(|v| v.get()), Some(0));
        assert_eq!(restored.global_vid(0).map(|v| v.get()), Some(99));
        assert_eq!(
            restored.vertex_prop(0, "name"),
            Some(PropValue::Str("Alice".into()))
        );
    }

    #[test]
    fn test_snapshot_preserves_global_eid() {
        let mut store = MutableCsrStore::new();
        let a = store.add_vertex_with_global_id(1u64.into(), "Person", &[]);
        let b = store.add_vertex_with_global_id(2u64.into(), "Person", &[]);
        store.add_edge_with_global_id(
            77u64.into(),
            a,
            b,
            "KNOWS",
            &[("since", PropValue::Int(2024))],
        );
        store.commit();

        let bundle = serialize_snapshot(&store, 9).unwrap();
        let edge_group = bundle
            .edge_groups
            .iter()
            .find(|(label, _)| label == "KNOWS")
            .unwrap();
        let decoded = arrow_codec::decode_edge_group(&edge_group.1).unwrap();
        assert!(
            decoded.edges[0]
                .props
                .iter()
                .any(|(k, _)| k == GLOBAL_EID_PROP_KEY)
        );
    }

    #[test]
    fn test_snapshot_restore_bundle_roundtrip_preserves_global_eid() {
        let mut store = MutableCsrStore::new();
        let a = store.add_vertex_with_global_id(1u64.into(), "Person", &[]);
        let b = store.add_vertex_with_global_id(2u64.into(), "Person", &[]);
        store.add_edge_with_global_id(
            77u64.into(),
            a,
            b,
            "KNOWS",
            &[("since", PropValue::Int(2024))],
        );
        store.commit();

        let bundle = serialize_snapshot(&store, 12).unwrap();
        let restored = restore_snapshot_bundle(&bundle).unwrap();

        assert_eq!(restored.edge_count(), 1);
        assert_eq!(restored.local_eid(77u64.into()).map(|e| e.get()), Some(0));
        assert_eq!(restored.global_eid(0).map(|e| e.get()), Some(77));
        assert_eq!(restored.edge_prop(0, "since"), Some(PropValue::Int(2024)));
    }

    #[test]
    fn test_snapshot_rejects_version_mismatch() {
        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[]);
        store.commit();

        let mut bundle = serialize_snapshot(&store, 1).unwrap();
        bundle.manifest.version = SNAPSHOT_FORMAT_VERSION + 1;
        assert!(restore_snapshot_bundle(&bundle).is_err());

        let mut bundle = serialize_snapshot(&store, 1).unwrap();
        bundle.schema.version = SNAPSHOT_SCHEMA_VERSION + 1;
        assert!(restore_snapshot_bundle(&bundle).is_err());
    }

    // ── Vineyard round-trip tests ───────────────────────────────────

    #[test]
    fn test_vineyard_snapshot_roundtrip() {
        let vineyard = EdgeVineyard::new(0);

        let mut store = MutableCsrStore::new_partition(3);
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Bob".into())),
                ("age", PropValue::Int(25)),
            ],
        );
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.add_edge(0, 1, "KNOWS", &[("since", PropValue::Int(2020))]);
        store.add_edge(0, 2, "WORKS_AT", &[]);
        store.commit();

        // CSR → Vineyard
        let manifest = serialize_snapshot_to_vineyard(&store, &vineyard, 42).unwrap();
        assert_eq!(manifest.vertex_labels.len(), 2);
        assert_eq!(manifest.edge_labels.len(), 2);
        assert_eq!(manifest.vertex_count, 3);
        assert_eq!(manifest.edge_count, 2);
        assert_eq!(manifest.partition_id, 3);
        assert!(vineyard.total_bytes() > 0);

        // Vineyard → CSR
        let restored = restore_snapshot_from_vineyard(&vineyard, &manifest).unwrap();
        assert_eq!(restored.vertex_count(), 3);
        assert_eq!(restored.edge_count(), 2);
        assert_eq!(restored.partition_id_raw(), PartitionId::from(3));
        // Check that all names exist (HashMap order means VID ordering is non-deterministic)
        let names: Vec<_> = (0..3)
            .filter_map(|vid| restored.vertex_prop(vid, "name"))
            .collect();
        assert!(names.contains(&PropValue::Str("Alice".into())));
        assert!(names.contains(&PropValue::Str("Bob".into())));
        assert!(names.contains(&PropValue::Str("GFTD".into())));
    }

    #[test]
    fn test_vineyard_snapshot_preserves_global_ids() {
        let vineyard = EdgeVineyard::new(0);

        let mut store = MutableCsrStore::new();
        let a = store.add_vertex_with_global_id(
            99u64.into(),
            "Person",
            &[("name", PropValue::Str("A".into()))],
        );
        let b = store.add_vertex_with_global_id(
            100u64.into(),
            "Person",
            &[("name", PropValue::Str("B".into()))],
        );
        store.add_edge_with_global_id(77u64.into(), a, b, "KNOWS", &[]);
        store.commit();

        let manifest = serialize_snapshot_to_vineyard(&store, &vineyard, 1).unwrap();
        let restored = restore_snapshot_from_vineyard(&vineyard, &manifest).unwrap();

        assert_eq!(restored.global_vid(0).map(|v| v.get()), Some(99));
        assert_eq!(restored.global_vid(1).map(|v| v.get()), Some(100));
        assert_eq!(restored.global_eid(0).map(|e| e.get()), Some(77));
    }

    #[test]
    fn test_vineyard_fragment_manifest_serde() {
        let vineyard = EdgeVineyard::new(0);
        let mut store = MutableCsrStore::new();
        store.add_vertex("X", &[]);
        store.commit();

        let manifest = serialize_snapshot_to_vineyard(&store, &vineyard, 0).unwrap();

        // FragmentManifest is serde-roundtrippable
        let json = serde_json::to_string(&manifest).unwrap();
        let deserialized: yata_store::FragmentManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(
            deserialized.vertex_labels.len(),
            manifest.vertex_labels.len()
        );
        assert_eq!(deserialized.partition_id, manifest.partition_id);
    }

    #[test]
    fn test_snapshot_persists_to_vineyard_and_restores() {
        // Create data, snapshot to Vineyard, create a new Vineyard, copy blobs,
        // and restore from the manifest to verify persistence roundtrip.
        let vineyard1 = EdgeVineyard::new(0);

        let mut store = MutableCsrStore::new_partition(2);
        store.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        store.add_vertex(
            "Person",
            &[("name", PropValue::Str("Bob".into()))],
        );
        store.add_vertex(
            "Company",
            &[("name", PropValue::Str("GFTD".into()))],
        );
        store.add_edge(0, 1, "KNOWS", &[("since", PropValue::Int(2024))]);
        store.commit();

        // Snapshot to first Vineyard
        let manifest = serialize_snapshot_to_vineyard(&store, &vineyard1, 55).unwrap();
        assert_eq!(manifest.vertex_count, 3);
        assert_eq!(manifest.edge_count, 1);

        // Simulate persistence: copy blobs to a second Vineyard (like R2 download)
        let vineyard2 = EdgeVineyard::new(0);
        let mut new_vertex_labels = std::collections::HashMap::new();
        for (label, &obj_id) in &manifest.vertex_labels {
            let data = vineyard1.get_blob(obj_id).unwrap();
            let meta = vineyard1.get_meta(obj_id).unwrap();
            let new_id = vineyard2.put(meta, data);
            new_vertex_labels.insert(label.clone(), new_id);
        }
        let mut new_edge_labels = std::collections::HashMap::new();
        for (label, &obj_id) in &manifest.edge_labels {
            let data = vineyard1.get_blob(obj_id).unwrap();
            let meta = vineyard1.get_meta(obj_id).unwrap();
            let new_id = vineyard2.put(meta, data);
            new_edge_labels.insert(label.clone(), new_id);
        }

        let new_manifest = yata_store::vineyard::FragmentManifest {
            vertex_labels: new_vertex_labels,
            edge_labels: new_edge_labels,
            csr_object: None,
            schema_object: None,
            partition_id: manifest.partition_id,
            timestamp_ns: manifest.timestamp_ns,
            vertex_count: manifest.vertex_count,
            edge_count: manifest.edge_count,
        };

        // Restore from second Vineyard
        let restored = restore_snapshot_from_vineyard(&vineyard2, &new_manifest).unwrap();
        assert_eq!(restored.vertex_count(), 3);
        assert_eq!(restored.edge_count(), 1);

        let names: Vec<_> = (0..3)
            .filter_map(|vid| restored.vertex_prop(vid, "name"))
            .collect();
        assert!(names.contains(&PropValue::Str("Alice".into())));
        assert!(names.contains(&PropValue::Str("Bob".into())));
        assert!(names.contains(&PropValue::Str("GFTD".into())));
    }

    #[test]
    fn test_snapshot_manifest_json_roundtrip_with_all_fields() {
        // Verify SnapshotManifest (R2-side) serializes and deserializes correctly
        let manifest = SnapshotManifest {
            version: SNAPSHOT_FORMAT_VERSION,
            partition_id: PartitionId::from(5),
            wal_lsn: 42,
            timestamp_ns: 1234567890,
            vertex_count: 100,
            edge_count: 50,
            vertex_labels: vec!["Person".into(), "Company".into()],
            edge_labels: vec!["KNOWS".into(), "WORKS_AT".into()],
            partition_count: 1,
            global_map_count: 10,
        };

        let json = serde_json::to_string(&manifest).unwrap();
        let deserialized: SnapshotManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.version, SNAPSHOT_FORMAT_VERSION);
        assert_eq!(deserialized.partition_id, PartitionId::from(5));
        assert_eq!(deserialized.wal_lsn, 42);
        assert_eq!(deserialized.vertex_count, 100);
        assert_eq!(deserialized.edge_count, 50);
        assert_eq!(deserialized.vertex_labels.len(), 2);
        assert_eq!(deserialized.edge_labels.len(), 2);
    }

    #[test]
    fn test_chunked_serialize_small_label() {
        // Labels with <= CHUNK_SIZE vertices should produce single blob (no chunking)
        let mut store = MutableCsrStore::new();
        for i in 0..10 {
            store.add_vertex("SmallLabel", &[
                ("id", PropValue::Int(i)),
                ("sensitivity_ord", PropValue::Int(0)),
            ]);
        }
        store.commit();

        let vineyard = yata_store::vineyard::EdgeVineyard::new(256);
        let (manifest, chunk_keys) = serialize_snapshot_chunked_to_vineyard(&store, &vineyard, 0)
            .expect("chunked serialize should succeed");

        assert_eq!(manifest.vertex_count, 10);
        assert!(manifest.vertex_labels.contains_key("SmallLabel"));
        // Small label → single blob, key = snap/v/SmallLabel.arrow
        assert!(chunk_keys.iter().any(|k| k == "snap/v/SmallLabel.arrow"),
            "small label should produce single blob: {:?}", chunk_keys);
    }

    #[test]
    fn test_chunked_serialize_roundtrip() {
        // Verify chunked serialize → restore roundtrip preserves data
        let mut store = MutableCsrStore::new();
        for i in 0..50 {
            store.add_vertex("TestLabel", &[
                ("rkey", PropValue::Str(format!("rkey_{i}"))),
                ("sensitivity_ord", PropValue::Int(0)),
                ("owner_hash", PropValue::Int(12345)),
            ]);
        }
        store.add_edge(0, 1, "KNOWS", &[]);
        store.commit();

        let vineyard = yata_store::vineyard::EdgeVineyard::new(256);
        let (manifest, _chunk_keys) = serialize_snapshot_chunked_to_vineyard(&store, &vineyard, 0)
            .expect("chunked serialize should succeed");

        // Restore from vineyard
        let restored = restore_snapshot_from_vineyard(&vineyard, &manifest)
            .expect("restore should succeed");

        assert_eq!(restored.vertex_count_raw(), 50);
        // Check a property survived roundtrip
        let prop = restored.vertex_prop(0, "rkey");
        assert!(prop.is_some(), "property should survive roundtrip");
    }
}
