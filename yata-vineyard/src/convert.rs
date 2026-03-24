//! Conversion between MutableCsrStore and ArrowFragment.
//!
//! MutableCsrStore (mutable, in-memory) ↔ ArrowFragment (immutable, serializable).
//! ArrowFragment uses NbrUnit zero-copy CSR which is ~2x faster for neighbor traversal.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;

use yata_grin::{Mutable, PropValue, Property, Scannable, Schema as GrinSchema};

use crate::fragment::ArrowFragment;
use crate::nbr::{self, NbrUnit64};
use crate::schema::{PropertyGraphSchema, SchemaEntry};

/// Convert a MutableCsrStore to an ArrowFragment.
///
/// Extracts all vertices/edges per label into Arrow columnar format,
/// builds bidirectional CSR with packed NbrUnit for zero-copy traversal.
pub fn csr_to_fragment<S>(store: &S, partition_id: u32) -> ArrowFragment
where
    S: GrinSchema + Property + Scannable + yata_grin::Topology,
{
    let v_labels = GrinSchema::vertex_labels(store);
    let e_labels = GrinSchema::edge_labels(store);

    // Build schema
    let mut schema = PropertyGraphSchema::new();
    let mut vlabel_map: HashMap<String, i32> = HashMap::new();
    let mut elabel_map: HashMap<String, i32> = HashMap::new();

    for label in &v_labels {
        let id = schema.add_vertex_label(label);
        // Discover property keys from first vertex of this label
        let vids = Scannable::scan_vertices_by_label(store, label);
        if let Some(&first_vid) = vids.first() {
            let prop_keys = store.vertex_prop_keys(label);
            for key in &prop_keys {
                if let Some(val) = store.vertex_prop(first_vid, key) {
                    let dt = prop_value_to_arrow_type(&val);
                    schema.vertex_entry_mut(id).unwrap().add_prop(key, &dt);
                }
            }
        }
        vlabel_map.insert(label.clone(), id);
    }

    for label in &e_labels {
        let id = schema.add_edge_label(label);
        elabel_map.insert(label.clone(), id);
    }

    let mut frag = ArrowFragment::new(partition_id, 1, true, schema);

    // ── Vertex tables ──
    for label in &v_labels {
        let vids = Scannable::scan_vertices_by_label(store, label);
        let prop_keys = store.vertex_prop_keys(label);

        // Build columnar arrays
        let mut columns: Vec<(&str, Vec<PropValue>)> = prop_keys
            .iter()
            .map(|k| (k.as_str(), Vec::with_capacity(vids.len())))
            .collect();

        for &vid in &vids {
            for (i, key) in prop_keys.iter().enumerate() {
                let val = store
                    .vertex_prop(vid, key)
                    .unwrap_or(PropValue::Null);
                columns[i].1.push(val);
            }
        }

        let (fields, arrays): (Vec<_>, Vec<_>) = columns
            .iter()
            .map(|(name, vals)| prop_values_to_arrow(name, vals))
            .unzip();

        if !fields.is_empty() && !vids.is_empty() {
            let schema = Arc::new(Schema::new(fields));
            if let Ok(batch) = RecordBatch::try_new(schema, arrays) {
                frag.vertex_tables.push(batch);
            }
        } else {
            // Push empty batch to maintain index alignment
            frag.vertex_tables.push(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        let vlabel_id = *vlabel_map.get(label.as_str()).unwrap() as usize;
        frag.ivnums[vlabel_id] = vids.len() as u64;
    }

    // ── Edge tables + CSR topology ──
    // Build per-(elabel, vlabel) adjacency lists
    // vid_to_local: maps global vid to per-label local index
    let mut vid_to_local: HashMap<String, HashMap<u32, u64>> = HashMap::new();
    for label in &v_labels {
        let vids = Scannable::scan_vertices_by_label(store, label);
        let local_map: HashMap<u32, u64> = vids
            .iter()
            .enumerate()
            .map(|(i, &vid)| (vid, i as u64))
            .collect();
        vid_to_local.insert(label.clone(), local_map);
    }

    // Collect edges per edge label
    for elabel in &e_labels {
        let elabel_id = *elabel_map.get(elabel.as_str()).unwrap() as usize;

        // Collect all edges with this label
        let mut edges: Vec<(u32, u32, u32)> = Vec::new(); // (src, dst, eid)
        let edge_count = store.edge_count();
        for eid in 0..edge_count {
            if let Some((src, dst, label)) = store.edge_endpoints(eid) {
                if label == *elabel {
                    edges.push((src, dst, eid));
                }
            }
        }

        // For each source vertex label, build CSR
        for (vlabel_idx, vlabel) in v_labels.iter().enumerate() {
            let local_map = vid_to_local.get(vlabel.as_str()).unwrap();
            let vcount = local_map.len();
            if vcount == 0 {
                continue;
            }

            // Outgoing edges from vertices of this label
            let mut oe_adj: Vec<Vec<NbrUnit64>> = vec![Vec::new(); vcount];
            let mut ie_adj: Vec<Vec<NbrUnit64>> = vec![Vec::new(); vcount];

            for &(src, dst, eid) in &edges {
                if let Some(&src_local) = local_map.get(&src) {
                    oe_adj[src_local as usize].push(NbrUnit64::new(dst as u64, eid as u64));
                }
                if let Some(&dst_local) = local_map.get(&dst) {
                    ie_adj[dst_local as usize].push(NbrUnit64::new(src as u64, eid as u64));
                }
            }

            // Build CSR offsets + packed NbrUnit bytes (outgoing)
            let (oe_offsets, oe_bytes) = build_csr_from_adj(&oe_adj);
            if !oe_bytes.is_empty() {
                frag.oe_offsets[elabel_id][vlabel_idx] = Some(oe_offsets);
                frag.oe_nbrs[elabel_id][vlabel_idx] = Some(oe_bytes);
            }

            // Build CSR (incoming)
            let (ie_offsets, ie_bytes) = build_csr_from_adj(&ie_adj);
            if !ie_bytes.is_empty() {
                frag.ie_offsets[elabel_id][vlabel_idx] = Some(ie_offsets);
                frag.ie_nbrs[elabel_id][vlabel_idx] = Some(ie_bytes);
            }
        }

        // Edge property table (placeholder — edge props stored per-eid, not per-label yet)
        frag.edge_tables.push(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }

    frag
}

/// Build CSR offset array + packed NbrUnit bytes from adjacency lists.
fn build_csr_from_adj(adj: &[Vec<NbrUnit64>]) -> (Int64Array, Bytes) {
    let mut offsets = Vec::with_capacity(adj.len() + 1);
    let mut all_units: Vec<NbrUnit64> = Vec::new();
    let mut offset = 0i64;

    for neighbors in adj {
        offsets.push(offset);
        all_units.extend_from_slice(neighbors);
        offset += neighbors.len() as i64;
    }
    offsets.push(offset);

    let bytes = if all_units.is_empty() {
        Bytes::new()
    } else {
        Bytes::copy_from_slice(nbr::nbr_units_to_bytes(&all_units))
    };

    (Int64Array::from(offsets), bytes)
}

/// Map PropValue to Arrow DataType.
fn prop_value_to_arrow_type(v: &PropValue) -> DataType {
    match v {
        PropValue::Int(_) => DataType::Int64,
        PropValue::Float(_) => DataType::Float64,
        PropValue::Str(_) => DataType::Utf8,
        PropValue::Bool(_) => DataType::Boolean,
        PropValue::Null => DataType::Utf8,
        PropValue::UInt(_) => DataType::UInt64,
        PropValue::Bytes(_) => DataType::Utf8, // base64 fallback
    }
}

/// Convert a column of PropValues to an Arrow array.
fn prop_values_to_arrow(name: &str, vals: &[PropValue]) -> (Field, ArrayRef) {
    if vals.is_empty() {
        return (
            Field::new(name, DataType::Utf8, true),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef,
        );
    }

    // Detect type from first non-null value
    let dt = vals
        .iter()
        .find(|v| !matches!(v, PropValue::Null))
        .map(prop_value_to_arrow_type)
        .unwrap_or(DataType::Utf8);

    let array: ArrayRef = match dt {
        DataType::Int64 => {
            let arr: Vec<Option<i64>> = vals
                .iter()
                .map(|v| match v {
                    PropValue::Int(n) => Some(*n),
                    _ => None,
                })
                .collect();
            Arc::new(Int64Array::from(arr))
        }
        DataType::UInt64 => {
            let arr: Vec<Option<u64>> = vals
                .iter()
                .map(|v| match v {
                    PropValue::UInt(n) => Some(*n),
                    PropValue::Int(n) if *n >= 0 => Some(*n as u64),
                    _ => None,
                })
                .collect();
            Arc::new(UInt64Array::from(arr))
        }
        DataType::Float64 => {
            let arr: Vec<Option<f64>> = vals
                .iter()
                .map(|v| match v {
                    PropValue::Float(f) => Some(f.0),
                    _ => None,
                })
                .collect();
            Arc::new(Float64Array::from(arr))
        }
        DataType::Boolean => {
            let arr: Vec<Option<bool>> = vals
                .iter()
                .map(|v| match v {
                    PropValue::Bool(b) => Some(*b),
                    _ => None,
                })
                .collect();
            Arc::new(BooleanArray::from(arr))
        }
        _ => {
            // Fallback: everything as string
            let arr: Vec<Option<String>> = vals
                .iter()
                .map(|v| match v {
                    PropValue::Str(s) => Some(s.clone()),
                    PropValue::Int(n) => Some(n.to_string()),
                    PropValue::Float(f) => Some(f.0.to_string()),
                    PropValue::Bool(b) => Some(b.to_string()),
                    PropValue::Null => None,
                    PropValue::UInt(n) => Some(n.to_string()),
                    PropValue::Bytes(b) => Some(format!("{:?}", b)),
                })
                .collect();
            Arc::new(StringArray::from(arr))
        }
    };

    (Field::new(name, dt, true), array)
}
