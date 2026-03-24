//! Graph → MDAG serialization: MutableCsrStore → root CID in CAS.
//!
//! Data blocks (vertex/edge groups): Arrow IPC columnar format.
//! Metadata blocks (root, schema, index): CBOR.
//! R2-optimized: all vertices/edges of a label are inlined in one Arrow IPC CAS blob.
//! A full commit produces: #vertex_labels + #edge_labels + schema + root CAS objects.

use bytes::Bytes;
use ed25519_dalek::SigningKey;
use std::collections::{BTreeMap, HashSet};
use yata_cas::CasStore;
use yata_core::Blake3Hash;
use yata_grin::*;
use yata_store::MutableCsrStore;

use crate::blocks::*;
use crate::error::Result;
use crate::verify::sign_root;

/// Serialize a MutableCsrStore graph into MDAG blocks stored in CAS.
/// Returns the root CID identifying the entire graph state.
///
/// CAS object count = #vertex_labels + #edge_labels + 1 (schema) + 1 (root) + #index_keys.
pub async fn commit_graph(
    store: &MutableCsrStore,
    cas: &dyn CasStore,
    parent: Option<Blake3Hash>,
    message: &str,
    signing_key: &SigningKey,
    signer_did: &str,
) -> Result<Blake3Hash> {
    commit_graph_indexed(store, cas, parent, message, &[], signing_key, signer_did).await
}

/// Serialize with secondary property indexes for federated query.
/// `index_keys` specifies which property keys to index (e.g. ["email", "node_id"]).
pub async fn commit_graph_indexed(
    store: &MutableCsrStore,
    cas: &dyn CasStore,
    parent: Option<Blake3Hash>,
    message: &str,
    index_keys: &[&str],
    signing_key: &SigningKey,
    signer_did: &str,
) -> Result<Blake3Hash> {
    // ── 1. Collect vertices by label ────────────────────────────────────
    let all_vids = store.scan_all_vertices();
    let mut vertex_by_label: BTreeMap<String, Vec<VertexBlock>> = BTreeMap::new();

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
        let primary_label = labels.into_iter().next().unwrap_or_default();
        vertex_by_label
            .entry(primary_label)
            .or_default()
            .push(block);
    }

    // ── 2. Build label vertex groups (1 CAS put per label) ─────────────
    let mut vertex_group_cids: Vec<Blake3Hash> = Vec::new();
    let mut vertex_group_label_cids: Vec<(String, Blake3Hash)> = Vec::new();
    for (label, mut vertices) in vertex_by_label {
        vertices.sort_by_key(|v| v.vid);
        let group = LabelVertexGroup {
            count: vertices.len() as u32,
            label: label.clone(),
            vertices,
        };
        let cid = put_arrow_vertex_group(cas, &group).await?;
        vertex_group_label_cids.push((label, cid.clone()));
        vertex_group_cids.push(cid);
    }

    // ── 3. Collect edges by label ───────────────────────────────────────
    let mut edge_by_label: BTreeMap<String, Vec<EdgeBlock>> = BTreeMap::new();
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
                edge_by_label.entry(elabel.clone()).or_default().push(block);
            }
        }
    }

    // ── 4. Build label edge groups (1 CAS put per label) ────────────────
    let mut edge_group_cids: Vec<Blake3Hash> = Vec::new();
    for (label, mut edges) in edge_by_label {
        edges.sort_by(|a, b| (a.src, a.dst, a.edge_id).cmp(&(b.src, b.dst, b.edge_id)));
        let group = LabelEdgeGroup {
            count: edges.len() as u32,
            label,
            edges,
        };
        edge_group_cids.push(put_arrow_edge_group(cas, &group).await?);
    }

    // ── 5. Schema block ────────────────────────────────────────────────
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

    // ── 6. Property indexes (federated query support) ───────────────────
    let index_keys_set: HashSet<&str> = index_keys.iter().copied().collect();
    let index_cids = if !index_keys_set.is_empty() {
        build_property_indexes(cas, &vertex_group_label_cids, &index_keys_set).await?
    } else {
        Vec::new()
    };

    // ── 7. Root block ──────────────────────────────────────────────────
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    let mut root = GraphRootBlock {
        version: store.version(),
        partition_id: store.partition_id_raw(),
        parent,
        schema_cid,
        vertex_groups: vertex_group_cids,
        edge_groups: edge_group_cids,
        vertex_count: store.vertex_count() as u32,
        edge_count: store.edge_count() as u32,
        timestamp_ns: now_ns,
        message: message.to_string(),
        index_cids,
        vex_cid: None,
        vineyard_ids: None,
        signer_did: String::new(),
        signature: Vec::new(),
    };
    sign_root(&mut root, signing_key, signer_did);
    put_block(cas, &root).await
}

/// Delta commit: only re-serialize label groups whose vertices/edges changed.
/// Reuses CIDs from the previous root block for unchanged groups.
/// Falls back to full commit_graph() if no previous root.
pub async fn delta_commit_graph(
    store: &MutableCsrStore,
    cas: &dyn CasStore,
    prev_root_cid: Option<Blake3Hash>,
    message: &str,
    dirty_vertex_labels: &[String],
    dirty_edge_labels: &[String],
    signing_key: &SigningKey,
    signer_did: &str,
) -> Result<Blake3Hash> {
    let prev_root = if let Some(ref cid) = prev_root_cid {
        match cas.get(cid).await {
            Ok(Some(data)) => yata_cbor::decode::<GraphRootBlock>(&data).ok(),
            _ => None,
        }
    } else {
        None
    };

    let prev_root = match prev_root {
        Some(r) => r,
        None => {
            return commit_graph(store, cas, prev_root_cid, message, signing_key, signer_did).await;
        }
    };

    // Load previous label→CID maps for reuse
    let mut prev_vg_map: BTreeMap<String, Blake3Hash> = BTreeMap::new();
    for cid in &prev_root.vertex_groups {
        if let Ok(Some(data)) = cas.get(cid).await {
            let group = if crate::arrow_codec::is_arrow_block(&data) {
                crate::arrow_codec::decode_vertex_group(&data).ok()
            } else {
                yata_cbor::decode::<LabelVertexGroup>(&data).ok()
            };
            if let Some(g) = group {
                prev_vg_map.insert(g.label.clone(), cid.clone());
            }
        }
    }
    let mut prev_eg_map: BTreeMap<String, Blake3Hash> = BTreeMap::new();
    for cid in &prev_root.edge_groups {
        if let Ok(Some(data)) = cas.get(cid).await {
            let group = if crate::arrow_codec::is_arrow_block(&data) {
                crate::arrow_codec::decode_edge_group(&data).ok()
            } else {
                yata_cbor::decode::<LabelEdgeGroup>(&data).ok()
            };
            if let Some(g) = group {
                prev_eg_map.insert(g.label.clone(), cid.clone());
            }
        }
    }

    let dirty_v_set: std::collections::HashSet<&str> =
        dirty_vertex_labels.iter().map(|s| s.as_str()).collect();
    let dirty_e_set: std::collections::HashSet<&str> =
        dirty_edge_labels.iter().map(|s| s.as_str()).collect();

    // ── Vertex groups: reuse unchanged, re-serialize dirty ──
    let all_vids = store.scan_all_vertices();
    let mut vertex_by_label: BTreeMap<String, Vec<VertexBlock>> = BTreeMap::new();

    for vid in &all_vids {
        let labels = <MutableCsrStore as Property>::vertex_labels(store, *vid);
        let primary_label = labels.first().cloned().unwrap_or_default();

        if !dirty_v_set.contains(primary_label.as_str()) {
            continue;
        }

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
        vertex_by_label
            .entry(primary_label)
            .or_default()
            .push(block);
    }

    let mut vertex_group_cids: Vec<Blake3Hash> = Vec::new();
    let all_vlabels = <MutableCsrStore as Schema>::vertex_labels(store);
    for label in &all_vlabels {
        if let Some(mut vertices) = vertex_by_label.remove(label.as_str()) {
            vertices.sort_by_key(|v| v.vid);
            let group = LabelVertexGroup {
                count: vertices.len() as u32,
                label: label.clone(),
                vertices,
            };
            vertex_group_cids.push(put_arrow_vertex_group(cas, &group).await?);
        } else if let Some(prev_cid) = prev_vg_map.get(label.as_str()) {
            vertex_group_cids.push(prev_cid.clone());
        }
    }

    // ── Edge groups: reuse unchanged, re-serialize dirty ──
    let mut edge_by_label: BTreeMap<String, Vec<EdgeBlock>> = BTreeMap::new();
    let edge_labels = <MutableCsrStore as Schema>::edge_labels(store);

    for elabel in &edge_labels {
        if !dirty_e_set.contains(elabel.as_str()) {
            continue;
        }
        for vid in &all_vids {
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
                edge_by_label.entry(elabel.clone()).or_default().push(block);
            }
        }
    }

    let mut edge_group_cids: Vec<Blake3Hash> = Vec::new();
    for label in &edge_labels {
        if let Some(mut edges) = edge_by_label.remove(label.as_str()) {
            edges.sort_by(|a, b| (a.src, a.dst, a.edge_id).cmp(&(b.src, b.dst, b.edge_id)));
            let group = LabelEdgeGroup {
                count: edges.len() as u32,
                label: label.clone(),
                edges,
            };
            edge_group_cids.push(put_arrow_edge_group(cas, &group).await?);
        } else if let Some(prev_cid) = prev_eg_map.get(label.as_str()) {
            edge_group_cids.push(prev_cid.clone());
        }
    }

    // ── Schema + Root ──
    let pks: Vec<(String, String)> = all_vlabels
        .iter()
        .filter_map(|l| store.vertex_primary_key(l).map(|pk| (l.clone(), pk)))
        .collect();
    let schema = SchemaBlock {
        vertex_labels: all_vlabels,
        edge_labels,
        vertex_primary_keys: pks,
    };
    let schema_cid = put_block(cas, &schema).await?;

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    let mut root = GraphRootBlock {
        version: store.version(),
        partition_id: store.partition_id_raw(),
        parent: prev_root_cid,
        schema_cid,
        vertex_groups: vertex_group_cids,
        edge_groups: edge_group_cids,
        vertex_count: store.vertex_count() as u32,
        edge_count: store.edge_count() as u32,
        timestamp_ns: now_ns,
        message: message.to_string(),
        index_cids: Vec::new(),
        vex_cid: None,
        vineyard_ids: None,
        signer_did: String::new(),
        signature: Vec::new(),
    };
    sign_root(&mut root, signing_key, signer_did);
    put_block(cas, &root).await
}

/// Cached variant of delta_commit_graph: uses pre-built label→CID maps
/// instead of fetching previous root + all label groups from CAS.
/// Returns (root_cid, updated_vg_map, updated_eg_map) for the caller to cache.
pub async fn delta_commit_graph_cached(
    store: &MutableCsrStore,
    cas: &dyn CasStore,
    prev_root_cid: Option<Blake3Hash>,
    message: &str,
    dirty_vertex_labels: &[String],
    dirty_edge_labels: &[String],
    signing_key: &SigningKey,
    signer_did: &str,
    prev_vg_map: &BTreeMap<String, Blake3Hash>,
    prev_eg_map: &BTreeMap<String, Blake3Hash>,
) -> std::result::Result<
    (
        Blake3Hash,
        BTreeMap<String, Blake3Hash>,
        BTreeMap<String, Blake3Hash>,
    ),
    crate::error::MdagError,
> {
    // If no cached maps, fall back to full commit (first commit or cold start)
    if prev_vg_map.is_empty() && prev_eg_map.is_empty() && prev_root_cid.is_some() {
        // Fall back to the original (uncached) path for first delta after restart
        let cid = delta_commit_graph(
            store,
            cas,
            prev_root_cid,
            message,
            dirty_vertex_labels,
            dirty_edge_labels,
            signing_key,
            signer_did,
        )
        .await?;
        // Build cache from the new root we just created
        let (vg, eg) = build_label_cid_maps_from_root(cas, &cid).await;
        return Ok((cid, vg, eg));
    }

    let dirty_v_set: HashSet<&str> = dirty_vertex_labels.iter().map(|s| s.as_str()).collect();
    let dirty_e_set: HashSet<&str> = dirty_edge_labels.iter().map(|s| s.as_str()).collect();

    // ── Vertex groups: reuse cached CIDs for unchanged, re-serialize dirty ──
    let all_vids = store.scan_all_vertices();
    let mut vertex_by_label: BTreeMap<String, Vec<VertexBlock>> = BTreeMap::new();
    for vid in &all_vids {
        let labels = <MutableCsrStore as Property>::vertex_labels(store, *vid);
        let primary_label = labels.first().cloned().unwrap_or_default();
        if !dirty_v_set.contains(primary_label.as_str()) {
            continue;
        }
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
        vertex_by_label
            .entry(primary_label)
            .or_default()
            .push(VertexBlock {
                vid: *vid,
                labels: labels.clone(),
                props,
            });
    }

    let mut new_vg_map = prev_vg_map.clone();
    let mut vertex_group_cids: Vec<Blake3Hash> = Vec::new();
    let all_vlabels = <MutableCsrStore as Schema>::vertex_labels(store);
    for label in &all_vlabels {
        if let Some(mut vertices) = vertex_by_label.remove(label.as_str()) {
            vertices.sort_by_key(|v| v.vid);
            let group = LabelVertexGroup {
                count: vertices.len() as u32,
                label: label.clone(),
                vertices,
            };
            let cid = put_arrow_vertex_group(cas, &group).await?;
            new_vg_map.insert(label.clone(), cid.clone());
            vertex_group_cids.push(cid);
        } else if let Some(prev_cid) = prev_vg_map.get(label.as_str()) {
            vertex_group_cids.push(prev_cid.clone());
        }
    }

    // ── Edge groups: reuse cached CIDs for unchanged, re-serialize dirty ──
    let mut edge_by_label: BTreeMap<String, Vec<EdgeBlock>> = BTreeMap::new();
    let edge_labels = <MutableCsrStore as Schema>::edge_labels(store);
    for elabel in &edge_labels {
        if !dirty_e_set.contains(elabel.as_str()) {
            continue;
        }
        for vid in &all_vids {
            let neighbors = store.out_neighbors_by_label(*vid, elabel);
            for n in neighbors {
                let prop_keys = store.edge_prop_keys(elabel);
                let mut props: Vec<(String, PropValue)> = prop_keys
                    .iter()
                    .filter_map(|k| store.edge_prop(n.edge_id, k).map(|v| (k.clone(), v)))
                    .collect();
                props.sort_by(|a, b| a.0.cmp(&b.0));
                edge_by_label
                    .entry(elabel.clone())
                    .or_default()
                    .push(EdgeBlock {
                        edge_id: n.edge_id,
                        src: *vid,
                        dst: n.vid,
                        label: elabel.clone(),
                        props,
                    });
            }
        }
    }

    let mut new_eg_map = prev_eg_map.clone();
    let mut edge_group_cids: Vec<Blake3Hash> = Vec::new();
    for label in &edge_labels {
        if let Some(mut edges) = edge_by_label.remove(label.as_str()) {
            edges.sort_by(|a, b| (a.src, a.dst, a.edge_id).cmp(&(b.src, b.dst, b.edge_id)));
            let group = LabelEdgeGroup {
                count: edges.len() as u32,
                label: label.clone(),
                edges,
            };
            let cid = put_arrow_edge_group(cas, &group).await?;
            new_eg_map.insert(label.clone(), cid.clone());
            edge_group_cids.push(cid);
        } else if let Some(prev_cid) = prev_eg_map.get(label.as_str()) {
            edge_group_cids.push(prev_cid.clone());
        }
    }

    // ── Schema + Root ──
    let pks: Vec<(String, String)> = all_vlabels
        .iter()
        .filter_map(|l| store.vertex_primary_key(l).map(|pk| (l.clone(), pk)))
        .collect();
    let schema = SchemaBlock {
        vertex_labels: all_vlabels,
        edge_labels,
        vertex_primary_keys: pks,
    };
    let schema_cid = put_block(cas, &schema).await?;

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    let mut root = GraphRootBlock {
        version: store.version(),
        partition_id: store.partition_id_raw(),
        parent: prev_root_cid,
        schema_cid,
        vertex_groups: vertex_group_cids,
        edge_groups: edge_group_cids,
        vertex_count: store.vertex_count() as u32,
        edge_count: store.edge_count() as u32,
        timestamp_ns: now_ns,
        message: message.to_string(),
        index_cids: Vec::new(),
        vex_cid: None,
        vineyard_ids: None,
        signer_did: String::new(),
        signature: Vec::new(),
    };
    sign_root(&mut root, signing_key, signer_did);
    let root_cid = put_block(cas, &root).await?;
    Ok((root_cid, new_vg_map, new_eg_map))
}

/// Build label→CID maps from a root block (for cache warm-up after uncached commit).
async fn build_label_cid_maps_from_root(
    cas: &dyn CasStore,
    root_cid: &Blake3Hash,
) -> (BTreeMap<String, Blake3Hash>, BTreeMap<String, Blake3Hash>) {
    let mut vg_map = BTreeMap::new();
    let mut eg_map = BTreeMap::new();
    let root_data = match cas.get(root_cid).await {
        Ok(Some(data)) => data,
        _ => return (vg_map, eg_map),
    };
    let root: GraphRootBlock = match yata_cbor::decode(&root_data) {
        Ok(r) => r,
        Err(_) => return (vg_map, eg_map),
    };
    for cid in &root.vertex_groups {
        if let Ok(Some(data)) = cas.get(cid).await {
            let group = if crate::arrow_codec::is_arrow_block(&data) {
                crate::arrow_codec::decode_vertex_group(&data).ok()
            } else {
                yata_cbor::decode::<LabelVertexGroup>(&data).ok()
            };
            if let Some(g) = group {
                vg_map.insert(g.label.clone(), cid.clone());
            }
        }
    }
    for cid in &root.edge_groups {
        if let Ok(Some(data)) = cas.get(cid).await {
            let group = if crate::arrow_codec::is_arrow_block(&data) {
                crate::arrow_codec::decode_edge_group(&data).ok()
            } else {
                yata_cbor::decode::<LabelEdgeGroup>(&data).ok()
            };
            if let Some(g) = group {
                eg_map.insert(g.label.clone(), cid.clone());
            }
        }
    }
    (vg_map, eg_map)
}

/// Build property indexes from already-committed vertex groups.
/// Scans each LabelVertexGroup CAS blob and collects prop_value → IndexVertexRef entries.
async fn build_property_indexes(
    cas: &dyn CasStore,
    vertex_group_label_cids: &[(String, Blake3Hash)],
    index_keys: &HashSet<&str>,
) -> Result<Vec<Blake3Hash>> {
    // key → value → Vec<IndexVertexRef>
    let mut indexes: BTreeMap<String, BTreeMap<String, Vec<IndexVertexRef>>> = BTreeMap::new();
    for key in index_keys {
        indexes.insert(key.to_string(), BTreeMap::new());
    }

    for (label, group_cid) in vertex_group_label_cids {
        let data = cas
            .get(group_cid)
            .await?
            .ok_or_else(|| crate::error::MdagError::NotFound(group_cid.hex()))?;
        let group: LabelVertexGroup =
            if data.len() >= 4 && &data[..4] == crate::arrow_codec::ARROW_MAGIC {
                crate::arrow_codec::decode_vertex_group(&data)?
            } else {
                yata_cbor::decode(&data)
                    .map_err(|e| crate::error::MdagError::Deserialize(e.to_string()))?
            };

        for vb in &group.vertices {
            for (pk, pv) in &vb.props {
                if !index_keys.contains(pk.as_str()) {
                    continue;
                }
                let val_str = match pv {
                    PropValue::Str(s) => s.clone(),
                    PropValue::Int(i) => i.to_string(),
                    PropValue::Float(f) => f.to_string(),
                    PropValue::Bool(b) => b.to_string(),
                    PropValue::Null => continue,
                };
                let vref = IndexVertexRef {
                    group_cid: group_cid.clone(),
                    label: label.clone(),
                    vid: vb.vid,
                };
                indexes
                    .get_mut(pk)
                    .unwrap()
                    .entry(val_str)
                    .or_default()
                    .push(vref);
            }
        }
    }

    let mut cids = Vec::new();
    for (key, entries_map) in indexes {
        // Sort entries within each value for deterministic CID
        let mut entries: Vec<(String, Vec<IndexVertexRef>)> = entries_map.into_iter().collect();
        for (_, refs) in &mut entries {
            refs.sort();
        }
        let block = PropertyIndexBlock { key, entries };
        cids.push(put_block(cas, &block).await?);
    }
    Ok(cids)
}

/// CBOR-encode a metadata block and store in CAS. Used for root/schema/index blocks only.
/// Data blocks (vertex/edge groups) use `put_arrow_vertex_group` / `put_arrow_edge_group`.
async fn put_block<T: serde::Serialize>(cas: &dyn CasStore, block: &T) -> Result<Blake3Hash> {
    let cbor = yata_cbor::encode(block)?;
    let hash = cas.put(Bytes::from(cbor)).await?;
    Ok(hash)
}

/// Arrow IPC-encode a vertex group and store in CAS.
async fn put_arrow_vertex_group(
    cas: &dyn CasStore,
    group: &LabelVertexGroup,
) -> Result<Blake3Hash> {
    let ipc_bytes = crate::arrow_codec::encode_vertex_group(group)?;
    let hash = cas.put(Bytes::from(ipc_bytes)).await?;
    Ok(hash)
}

/// Arrow IPC-encode an edge group and store in CAS.
async fn put_arrow_edge_group(cas: &dyn CasStore, group: &LabelEdgeGroup) -> Result<Blake3Hash> {
    let ipc_bytes = crate::arrow_codec::encode_edge_group(group)?;
    let hash = cas.put(Bytes::from(ipc_bytes)).await?;
    Ok(hash)
}

/// Commit graph from Vineyard blobs — avoids double-encoding.
/// Reads already-encoded Arrow IPC blobs from Vineyard and stores them in CAS directly.
pub async fn commit_graph_from_vineyard(
    vineyard: &dyn yata_store::VineyardStore,
    manifest: &yata_store::FragmentManifest,
    cas: &dyn CasStore,
    parent: Option<Blake3Hash>,
    message: &str,
    signing_key: &SigningKey,
    signer_did: &str,
) -> Result<Blake3Hash> {
    use std::collections::HashMap;

    // Store vertex group blobs in CAS
    let mut vertex_cids = Vec::new();
    let mut vineyard_id_map = HashMap::new();
    for (label, &obj_id) in &manifest.vertex_labels {
        let data = vineyard.get_blob(obj_id).ok_or_else(|| {
            crate::error::MdagError::NotFound(format!("vineyard blob: vertex {label}"))
        })?;
        let cid = cas.put(data).await?;
        vertex_cids.push(cid);
        vineyard_id_map.insert(label.clone(), obj_id.0);
    }

    // Store edge group blobs in CAS
    let mut edge_cids = Vec::new();
    for (label, &obj_id) in &manifest.edge_labels {
        let data = vineyard.get_blob(obj_id).ok_or_else(|| {
            crate::error::MdagError::NotFound(format!("vineyard blob: edge {label}"))
        })?;
        let cid = cas.put(data).await?;
        edge_cids.push(cid);
        vineyard_id_map.insert(label.clone(), obj_id.0);
    }

    // Schema block
    let schema = SchemaBlock {
        vertex_labels: manifest.vertex_labels.keys().cloned().collect(),
        edge_labels: manifest.edge_labels.keys().cloned().collect(),
        vertex_primary_keys: Vec::new(),
    };
    let schema_bytes = yata_cbor::encode(&schema)?;
    let schema_cid = cas.put(Bytes::from(schema_bytes)).await?;

    // Root block
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    let mut root = GraphRootBlock {
        version: 3,
        partition_id: yata_core::PartitionId::from(manifest.partition_id),
        parent,
        schema_cid,
        vertex_groups: vertex_cids,
        edge_groups: edge_cids,
        vertex_count: manifest.vertex_count as u32,
        edge_count: manifest.edge_count as u32,
        timestamp_ns: now_ns,
        message: message.to_string(),
        index_cids: Vec::new(),
        vex_cid: None,
        vineyard_ids: Some(vineyard_id_map),
        signer_did: signer_did.to_string(),
        signature: Vec::new(),
    };

    sign_root(&mut root, signing_key, signer_did);

    let root_bytes = yata_cbor::encode(&root)?;
    let root_cid = cas.put(Bytes::from(root_bytes)).await?;

    tracing::info!(
        root = %root_cid.hex(),
        v_groups = root.vertex_groups.len(),
        e_groups = root.edge_groups.len(),
        "MDAG commit from Vineyard"
    );

    Ok(root_cid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    use yata_cas::LocalCasStore;

    fn test_key() -> (SigningKey, &'static str) {
        (SigningKey::generate(&mut OsRng), "did:key:z6MkTest")
    }

    #[tokio::test]
    async fn test_commit_empty_graph() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let store = MutableCsrStore::new();
        let (sk, did) = test_key();
        let root_cid = commit_graph(&store, &cas, None, "empty", &sk, did)
            .await
            .unwrap();
        let data = cas.get(&root_cid).await.unwrap().unwrap();
        let root: GraphRootBlock = yata_cbor::decode(&data).unwrap();
        assert_eq!(root.vertex_count, 0);
        assert_eq!(root.edge_count, 0);
        assert!(root.parent.is_none());
        assert_eq!(root.signer_did, did);
        assert_eq!(root.signature.len(), 64);
    }

    #[tokio::test]
    async fn test_commit_single_vertex() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.commit();

        let (sk, did) = test_key();
        let root_cid = commit_graph(&store, &cas, None, "one vertex", &sk, did)
            .await
            .unwrap();
        let data = cas.get(&root_cid).await.unwrap().unwrap();
        let root: GraphRootBlock = yata_cbor::decode(&data).unwrap();
        assert_eq!(root.vertex_count, 1);
        assert_eq!(root.vertex_groups.len(), 1);

        // Verify inline vertex in group
        let group_data = cas.get(&root.vertex_groups[0]).await.unwrap().unwrap();
        let group = crate::arrow_codec::decode_vertex_group(&group_data).unwrap();
        assert_eq!(group.vertices.len(), 1);
        assert_eq!(group.vertices[0].vid, 0);
    }

    #[tokio::test]
    async fn test_commit_multi_label() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.commit();

        let (sk, did) = test_key();
        let root_cid = commit_graph(&store, &cas, None, "two labels", &sk, did)
            .await
            .unwrap();
        let data = cas.get(&root_cid).await.unwrap().unwrap();
        let root: GraphRootBlock = yata_cbor::decode(&data).unwrap();
        assert_eq!(root.vertex_count, 2);
        assert_eq!(root.vertex_groups.len(), 2);
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

        let (sk, did) = test_key();
        let root_cid = commit_graph(&store, &cas, None, "with edge", &sk, did)
            .await
            .unwrap();
        let data = cas.get(&root_cid).await.unwrap().unwrap();
        let root: GraphRootBlock = yata_cbor::decode(&data).unwrap();
        assert_eq!(root.vertex_count, 2);
        assert_eq!(root.edge_count, 1);
        assert_eq!(root.edge_groups.len(), 1);

        // Verify inline edge in group
        let group_data = cas.get(&root.edge_groups[0]).await.unwrap().unwrap();
        let group = crate::arrow_codec::decode_edge_group(&group_data).unwrap();
        assert_eq!(group.edges.len(), 1);
        assert_eq!(group.edges[0].label, "KNOWS");
    }

    #[tokio::test]
    async fn test_cas_object_count_1k_graph() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let mut store = MutableCsrStore::new();
        for i in 0u32..1000 {
            let label = match i % 3 {
                0 => "Person",
                1 => "Company",
                _ => "Product",
            };
            store.add_vertex(label, &[("name", PropValue::Str(format!("n{i}")))]);
        }
        for i in 0u32..3000 {
            let src = i % 1000;
            let dst = (i * 7 + 3) % 1000;
            if src != dst {
                store.add_edge(
                    src,
                    dst,
                    "REL",
                    &[("w", PropValue::Float(i as f64 * 0.001))],
                );
            }
        }
        store.commit();

        let (sk, did) = test_key();
        let root_cid = commit_graph(&store, &cas, None, "1k", &sk, did)
            .await
            .unwrap();
        let data = cas.get(&root_cid).await.unwrap().unwrap();
        let root: GraphRootBlock = yata_cbor::decode(&data).unwrap();

        // 3 vertex labels + 1 edge label + 1 schema + 1 root = 6 CAS objects
        let total_objects = root.vertex_groups.len() + root.edge_groups.len() + 2;
        assert_eq!(
            total_objects, 6,
            "1K V / 3K E should produce exactly 6 CAS objects"
        );
        println!("CAS objects for 1K V / 3K E: {total_objects} (was ~4,000+ before)");
    }

    // ── delta_commit partial label reuse ─────────────────────────────

    #[tokio::test]
    async fn test_delta_commit_partial_label_reuse() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.add_vertex("Product", &[("name", PropValue::Str("Widget".into()))]);
        store.commit();

        let (sk, did) = test_key();
        let root1_cid = commit_graph(&store, &cas, None, "initial", &sk, did)
            .await
            .unwrap();
        let data1 = cas.get(&root1_cid).await.unwrap().unwrap();
        let root1: GraphRootBlock = yata_cbor::decode(&data1).unwrap();
        assert_eq!(root1.vertex_groups.len(), 3);

        store.set_vertex_prop(0, "name", PropValue::Str("Alice Updated".into()));
        store.commit();

        let root2_cid = delta_commit_graph(
            &store,
            &cas,
            Some(root1_cid.clone()),
            "update person",
            &["Person".into()],
            &[],
            &sk,
            did,
        )
        .await
        .unwrap();
        let data2 = cas.get(&root2_cid).await.unwrap().unwrap();
        let root2: GraphRootBlock = yata_cbor::decode(&data2).unwrap();

        assert_eq!(root2.vertex_groups.len(), 3);
        let unchanged_count = root1
            .vertex_groups
            .iter()
            .filter(|cid| root2.vertex_groups.contains(cid))
            .count();
        assert_eq!(
            unchanged_count, 2,
            "Company + Product CIDs must be reused, got {unchanged_count} reused"
        );
    }

    #[tokio::test]
    async fn test_delta_commit_no_dirty_labels() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.commit();

        let (sk, did) = test_key();
        let root1_cid = commit_graph(&store, &cas, None, "init", &sk, did)
            .await
            .unwrap();
        let root2_cid = delta_commit_graph(
            &store,
            &cas,
            Some(root1_cid.clone()),
            "no change",
            &[],
            &[],
            &sk,
            did,
        )
        .await
        .unwrap();

        let data1 = cas.get(&root1_cid).await.unwrap().unwrap();
        let data2 = cas.get(&root2_cid).await.unwrap().unwrap();
        let r1: GraphRootBlock = yata_cbor::decode(&data1).unwrap();
        let r2: GraphRootBlock = yata_cbor::decode(&data2).unwrap();

        assert_eq!(
            r1.vertex_groups, r2.vertex_groups,
            "no dirty labels → vertex CIDs must be identical"
        );
    }

    #[tokio::test]
    async fn test_delta_commit_all_dirty() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let mut store = MutableCsrStore::new();
        store.add_vertex("A", &[("v", PropValue::Int(1))]);
        store.add_vertex("B", &[("v", PropValue::Int(2))]);
        store.commit();

        let (sk, did) = test_key();
        let root1_cid = commit_graph(&store, &cas, None, "init", &sk, did)
            .await
            .unwrap();
        store.set_vertex_prop(0, "v", PropValue::Int(10));
        store.set_vertex_prop(1, "v", PropValue::Int(20));
        store.commit();

        let root2_cid = delta_commit_graph(
            &store,
            &cas,
            Some(root1_cid.clone()),
            "all dirty",
            &["A".into(), "B".into()],
            &[],
            &sk,
            did,
        )
        .await
        .unwrap();

        let data2 = cas.get(&root2_cid).await.unwrap().unwrap();
        let r2: GraphRootBlock = yata_cbor::decode(&data2).unwrap();
        assert_eq!(r2.vertex_count, 2);
    }

    #[tokio::test]
    async fn test_commit_chain_parent() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCasStore::new(dir.path().join("cas")).await.unwrap();
        let mut store = MutableCsrStore::new();
        store.add_vertex("X", &[]);
        store.commit();

        let (sk, did) = test_key();
        let cid1 = commit_graph(&store, &cas, None, "first", &sk, did)
            .await
            .unwrap();
        store.add_vertex("X", &[]);
        store.commit();
        let cid2 = commit_graph(&store, &cas, Some(cid1.clone()), "second", &sk, did)
            .await
            .unwrap();

        let data2 = cas.get(&cid2).await.unwrap().unwrap();
        let r2: GraphRootBlock = yata_cbor::decode(&data2).unwrap();
        assert_eq!(
            r2.parent,
            Some(cid1.clone()),
            "second commit must reference first as parent"
        );
    }
}
