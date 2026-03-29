//! ArrowWalStore: I/O utility for loading compacted Arrow IPC WAL segments.
//!
//! Loads a compacted WAL segment from disk via mmap (zero kernel→user copy)
//! or from raw bytes. Provides vertex data access for cold start recovery.
//!
//! NOT a GraphStoreEnum variant — this is a loading utility, not a query store.
//! Cold start path: ArrowWalStore::from_file() → iterate entries → wal_apply → MutableCsrStore.
//!
//! The mmap optimization avoids R2 fetch + heap allocation for the initial load.
//! Arrow FileReader reads column buffers directly from mmap'd pages.

use std::collections::HashMap;
use std::path::Path;

use arrow::array::{Array, RecordBatch, StringArray, UInt8Array, UInt64Array};
use yata_grin::*;

/// A graph store backed by mmap'd Arrow IPC WAL data.
///
/// Zero-copy for property reads: Arrow column buffers reference mmap'd pages.
/// The OS page cache manages eviction — no application-level memory management.
pub struct ArrowWalStore {
    /// Per-VID vertex data, indexed by position in the compacted segment.
    /// VID = array index (0-based, dense).
    labels: Vec<String>,
    pk_keys: Vec<String>,
    pk_values: Vec<String>,

    /// Property columns: the Arrow RecordBatch rows, stored as (key, value) pairs per VID.
    /// Parsed from props_json column on load (Phase 3).
    /// Phase 4 will serve these directly from mmap'd Arrow MapArray columns.
    vertex_props: Vec<HashMap<String, PropValue>>,

    /// Label → list of VIDs (for scan operations).
    label_index: HashMap<String, Vec<u32>>,

    /// (label, pk_key) → { pk_value → VID } for O(1) equality lookup.
    prop_eq_index: HashMap<(String, String), HashMap<String, u32>>,

    /// All known vertex labels (sorted, deduped).
    known_labels: Vec<String>,

    /// Total vertex count.
    count: usize,

    /// The mmap handle (kept alive to prevent unmap).
    _mmap: Option<memmap2::Mmap>,
}

impl ArrowWalStore {
    /// Create an empty ArrowWalStore.
    pub fn new() -> Self {
        Self {
            labels: Vec::new(),
            pk_keys: Vec::new(),
            pk_values: Vec::new(),
            vertex_props: Vec::new(),
            label_index: HashMap::new(),
            prop_eq_index: HashMap::new(),
            known_labels: Vec::new(),
            count: 0,
            _mmap: None,
        }
    }

    /// Load from a compacted Arrow IPC File on disk via mmap.
    ///
    /// The file is mmap'd (zero copy from disk to virtual memory).
    /// Arrow FileReader reads column data directly from mmap'd pages.
    /// Property values are parsed from props_json column (one alloc per entry).
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let file = std::fs::File::open(path)
            .map_err(|e| format!("failed to open compacted segment: {e}"))?;

        // Safety: read-only mmap, file not modified during lifetime.
        let mmap = unsafe { memmap2::Mmap::map(&file) }
            .map_err(|e| format!("mmap failed: {e}"))?;

        let mut store = Self::from_arrow_ipc_bytes(&mmap)?;
        store._mmap = Some(mmap);
        Ok(store)
    }

    /// Load from Arrow IPC bytes (for testing or network-received data).
    pub fn from_arrow_ipc_bytes(data: &[u8]) -> Result<Self, String> {
        if data.is_empty() {
            return Ok(Self::new());
        }

        let cursor = std::io::Cursor::new(data);
        let reader = arrow::ipc::reader::FileReader::try_new(cursor, None)
            .map_err(|e| format!("Arrow FileReader failed: {e}"))?;

        let mut labels = Vec::new();
        let mut pk_keys = Vec::new();
        let mut pk_values = Vec::new();
        let mut vertex_props = Vec::new();
        let mut label_index: HashMap<String, Vec<u32>> = HashMap::new();
        let mut prop_eq_index: HashMap<(String, String), HashMap<String, u32>> = HashMap::new();

        let mut vid = 0u32;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| format!("batch read failed: {e}"))?;
            let n = batch.num_rows();

            let op_col = batch.column_by_name("op")
                .and_then(|c| c.as_any().downcast_ref::<UInt8Array>());
            let label_col = batch.column_by_name("label")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let pk_key_col = batch.column_by_name("pk_key")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let pk_value_col = batch.column_by_name("pk_value")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let props_col = batch.column_by_name("props_json")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let (op_col, label_col, pk_key_col, pk_value_col) =
                match (op_col, label_col, pk_key_col, pk_value_col) {
                    (Some(o), Some(l), Some(k), Some(v)) => (o, l, k, v),
                    _ => return Err("missing required WAL columns".into()),
                };

            for i in 0..n {
                // Skip Delete entries (compaction should have removed them,
                // but guard against partial compaction).
                if op_col.value(i) == 1 {
                    continue;
                }

                let label = label_col.value(i).to_string();
                let pk_key = pk_key_col.value(i).to_string();
                let pk_value = pk_value_col.value(i).to_string();

                // Parse properties from JSON column
                let props: HashMap<String, PropValue> = if let Some(pc) = props_col {
                    if !pc.is_null(i) {
                        parse_props_json(pc.value(i))
                    } else {
                        HashMap::new()
                    }
                } else {
                    HashMap::new()
                };

                // Build indexes
                label_index.entry(label.clone()).or_default().push(vid);
                prop_eq_index
                    .entry((label.clone(), pk_key.clone()))
                    .or_default()
                    .insert(pk_value.clone(), vid);

                labels.push(label);
                pk_keys.push(pk_key);
                pk_values.push(pk_value);
                vertex_props.push(props);
                vid += 1;
            }
        }

        let mut known_labels: Vec<String> = label_index.keys().cloned().collect();
        known_labels.sort();

        let count = vid as usize;

        Ok(Self {
            labels,
            pk_keys,
            pk_values,
            vertex_props,
            label_index,
            prop_eq_index,
            known_labels,
            count,
            _mmap: None,
        })
    }

    /// Number of vertices.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Find VID by primary key.
    pub fn find_vid_by_pk(&self, label: &str, pk_key: &str, pk_value: &str) -> Option<u32> {
        self.prop_eq_index
            .get(&(label.to_string(), pk_key.to_string()))
            .and_then(|m| m.get(pk_value).copied())
    }
}

/// Parse a JSON object string into PropValue map.
fn parse_props_json(json: &str) -> HashMap<String, PropValue> {
    let map: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(json).unwrap_or_default();
    map.into_iter()
        .map(|(k, v)| {
            let pv = match &v {
                serde_json::Value::String(s) => PropValue::Str(s.clone()),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        PropValue::Int(i)
                    } else {
                        PropValue::Float(n.as_f64().unwrap_or(0.0))
                    }
                }
                serde_json::Value::Bool(b) => PropValue::Bool(*b),
                serde_json::Value::Null => PropValue::Null,
                _ => PropValue::Str(v.to_string()),
            };
            (k, pv)
        })
        .collect()
}

// ── GRIN trait implementations (read-only, vertex-only) ──
// These enable direct querying of the store without wal_apply,
// though the primary use is as a loading utility for cold start.

impl Topology for ArrowWalStore {
    fn vertex_count(&self) -> usize {
        self.count
    }

    fn edge_count(&self) -> usize {
        0 // WAL entries are vertex-only
    }

    fn has_vertex(&self, vid: u32) -> bool {
        (vid as usize) < self.count
    }

    fn out_degree(&self, _vid: u32) -> usize {
        0 // No edge data in WAL
    }

    fn in_degree(&self, _vid: u32) -> usize {
        0
    }

    fn out_neighbors(&self, _vid: u32) -> Vec<Neighbor> {
        Vec::new()
    }

    fn in_neighbors(&self, _vid: u32) -> Vec<Neighbor> {
        Vec::new()
    }

    fn out_neighbors_by_label(&self, _vid: u32, _edge_label: &str) -> Vec<Neighbor> {
        Vec::new()
    }

    fn in_neighbors_by_label(&self, _vid: u32, _edge_label: &str) -> Vec<Neighbor> {
        Vec::new()
    }
}

impl Property for ArrowWalStore {
    fn vertex_labels(&self, vid: u32) -> Vec<String> {
        if let Some(label) = self.labels.get(vid as usize) {
            vec![label.clone()]
        } else {
            Vec::new()
        }
    }

    fn vertex_prop(&self, vid: u32, key: &str) -> Option<PropValue> {
        self.vertex_props
            .get(vid as usize)
            .and_then(|props| props.get(key).cloned())
    }

    fn edge_prop(&self, _edge_id: u32, _key: &str) -> Option<PropValue> {
        None // No edge data in WAL
    }

    fn vertex_prop_keys(&self, label: &str) -> Vec<String> {
        // Collect all property keys from vertices with this label
        let mut keys = std::collections::HashSet::new();
        if let Some(vids) = self.label_index.get(label) {
            for &vid in vids {
                if let Some(props) = self.vertex_props.get(vid as usize) {
                    keys.extend(props.keys().cloned());
                }
            }
        }
        let mut result: Vec<String> = keys.into_iter().collect();
        result.sort();
        result
    }

    fn edge_prop_keys(&self, _label: &str) -> Vec<String> {
        Vec::new()
    }

    fn vertex_all_props(&self, vid: u32) -> HashMap<String, PropValue> {
        self.vertex_props
            .get(vid as usize)
            .cloned()
            .unwrap_or_default()
    }
}

impl Schema for ArrowWalStore {
    fn vertex_labels(&self) -> Vec<String> {
        self.known_labels.clone()
    }

    fn edge_labels(&self) -> Vec<String> {
        Vec::new() // No edges in WAL
    }

    fn vertex_primary_key(&self, _label: &str) -> Option<String> {
        Some("rkey".to_string()) // Convention: all WAL entries use "rkey"
    }
}

impl Scannable for ArrowWalStore {
    fn scan_vertices(&self, label: &str, predicate: &Predicate) -> Vec<u32> {
        let vids = match self.label_index.get(label) {
            Some(vids) => vids,
            None => return Vec::new(),
        };

        match predicate {
            Predicate::True => vids.clone(),
            Predicate::Eq(key, value) => {
                // O(N) scan over label's vertices
                vids.iter()
                    .copied()
                    .filter(|&vid| {
                        self.vertex_props
                            .get(vid as usize)
                            .and_then(|p| p.get(key.as_str()))
                            == Some(value)
                    })
                    .collect()
            }
            _ => {
                // General predicate: O(N) scan
                vids.iter()
                    .copied()
                    .filter(|&vid| {
                        if let Some(props) = self.vertex_props.get(vid as usize) {
                            eval_predicate(predicate, props)
                        } else {
                            false
                        }
                    })
                    .collect()
            }
        }
    }

    fn scan_vertices_by_label(&self, label: &str) -> Vec<u32> {
        self.label_index.get(label).cloned().unwrap_or_default()
    }

    fn scan_all_vertices(&self) -> Vec<u32> {
        (0..self.count as u32).collect()
    }
}

/// Evaluate a predicate against a property map.
fn eval_predicate(pred: &Predicate, props: &HashMap<String, PropValue>) -> bool {
    match pred {
        Predicate::True => true,
        Predicate::Eq(k, v) => props.get(k.as_str()) == Some(v),
        Predicate::Neq(k, v) => props.get(k.as_str()) != Some(v),
        Predicate::Lt(k, v) => props.get(k.as_str()).map_or(false, |pv| prop_lt(pv, v)),
        Predicate::Gt(k, v) => props.get(k.as_str()).map_or(false, |pv| prop_gt(pv, v)),
        Predicate::In(k, vals) => props.get(k.as_str()).map_or(false, |pv| vals.contains(pv)),
        Predicate::StartsWith(k, prefix) => {
            props.get(k.as_str()).map_or(false, |pv| {
                if let PropValue::Str(s) = pv { s.starts_with(prefix.as_str()) } else { false }
            })
        }
        Predicate::And(a, b) => eval_predicate(a, props) && eval_predicate(b, props),
        Predicate::Or(a, b) => eval_predicate(a, props) || eval_predicate(b, props),
    }
}

fn prop_lt(a: &PropValue, b: &PropValue) -> bool {
    match (a, b) {
        (PropValue::Int(a), PropValue::Int(b)) => a < b,
        (PropValue::Float(a), PropValue::Float(b)) => a < b,
        (PropValue::Str(a), PropValue::Str(b)) => a < b,
        _ => false,
    }
}

fn prop_gt(a: &PropValue, b: &PropValue) -> bool {
    match (a, b) {
        (PropValue::Int(a), PropValue::Int(b)) => a > b,
        (PropValue::Float(a), PropValue::Float(b)) => a > b,
        (PropValue::Str(a), PropValue::Str(b)) => a > b,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build Arrow IPC File bytes from test entries.
    fn build_test_arrow_ipc(entries: &[(u64, &str, &str, &str, Vec<(String, PropValue)>)]) -> Vec<u8> {
        use arrow::array::{ArrayRef, UInt64Array, UInt8Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("seq", DataType::UInt64, false),
            Field::new("op", DataType::UInt8, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("pk_key", DataType::Utf8, false),
            Field::new("pk_value", DataType::Utf8, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("props_json", DataType::Utf8, true),
        ]));

        let n = entries.len();
        let seq: Vec<u64> = entries.iter().map(|(s, ..)| *s).collect();
        let op: Vec<u8> = entries.iter().map(|(_, _, _, o, _)| if *o == "delete" { 1u8 } else { 0u8 }).collect();
        let labels: Vec<&str> = entries.iter().map(|(_, l, ..)| *l).collect();
        let pk_keys: Vec<&str> = vec!["rkey"; n];
        let pk_values: Vec<&str> = entries.iter().map(|(_, _, p, ..)| *p).collect();
        let ts: Vec<u64> = entries.iter().map(|(s, ..)| 1000 + s).collect();
        let props: Vec<String> = entries.iter().map(|(_, _, _, _, p)| {
            let mut map = serde_json::Map::new();
            for (k, v) in p {
                let jv = match v {
                    PropValue::Str(s) => serde_json::Value::String(s.clone()),
                    PropValue::Int(n) => serde_json::Value::Number((*n).into()),
                    PropValue::Float(f) => serde_json::json!(*f),
                    PropValue::Bool(b) => serde_json::Value::Bool(*b),
                    PropValue::Null => serde_json::Value::Null,
                };
                map.insert(k.clone(), jv);
            }
            serde_json::to_string(&map).unwrap()
        }).collect();
        let props_refs: Vec<&str> = props.iter().map(|s| s.as_str()).collect();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(seq)),
            Arc::new(UInt8Array::from(op)),
            Arc::new(StringArray::from(labels)),
            Arc::new(StringArray::from(pk_keys)),
            Arc::new(StringArray::from(pk_values)),
            Arc::new(UInt64Array::from(ts)),
            Arc::new(StringArray::from(props_refs)),
        ];

        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

        let buf = std::io::Cursor::new(Vec::new());
        let mut writer = arrow::ipc::writer::FileWriter::try_new(buf, schema.as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        writer.into_inner().unwrap().into_inner()
    }

    #[test]
    fn test_empty_store() {
        let store = ArrowWalStore::new();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        assert_eq!(store.vertex_count(), 0);
        assert_eq!(store.edge_count(), 0);
        assert!(Schema::vertex_labels(&store).is_empty());
    }

    #[test]
    fn test_load_from_bytes() {
        let data = build_test_arrow_ipc(&[
            (1, "Post", "pk_1", "upsert", vec![
                ("name".into(), PropValue::Str("alice".into())),
            ]),
            (2, "Like", "pk_2", "upsert", vec![
                ("target".into(), PropValue::Str("pk_1".into())),
            ]),
        ]);

        let store = ArrowWalStore::from_arrow_ipc_bytes(&data).unwrap();
        assert_eq!(store.len(), 2);
        assert_eq!(store.vertex_count(), 2);

        // Check labels
        let labels = Schema::vertex_labels(&store);
        assert!(labels.contains(&"Post".to_string()));
        assert!(labels.contains(&"Like".to_string()));

        // Check vertex properties
        assert_eq!(store.vertex_prop(0, "name"), Some(PropValue::Str("alice".into())));
        assert_eq!(store.vertex_prop(1, "target"), Some(PropValue::Str("pk_1".into())));

        // Check label scan
        let posts = store.scan_vertices_by_label("Post");
        assert_eq!(posts, vec![0]);
        let likes = store.scan_vertices_by_label("Like");
        assert_eq!(likes, vec![1]);
    }

    #[test]
    fn test_property_lookup() {
        let data = build_test_arrow_ipc(&[
            (1, "Post", "pk_1", "upsert", vec![
                ("name".into(), PropValue::Str("alice".into())),
                ("age".into(), PropValue::Int(30)),
                ("active".into(), PropValue::Bool(true)),
            ]),
        ]);

        let store = ArrowWalStore::from_arrow_ipc_bytes(&data).unwrap();

        assert_eq!(store.vertex_prop(0, "name"), Some(PropValue::Str("alice".into())));
        assert_eq!(store.vertex_prop(0, "age"), Some(PropValue::Int(30)));
        assert_eq!(store.vertex_prop(0, "active"), Some(PropValue::Bool(true)));
        assert_eq!(store.vertex_prop(0, "nonexistent"), None);
        assert_eq!(store.vertex_prop(999, "name"), None);
    }

    #[test]
    fn test_vertex_labels() {
        let data = build_test_arrow_ipc(&[
            (1, "Post", "pk_1", "upsert", vec![]),
        ]);
        let store = ArrowWalStore::from_arrow_ipc_bytes(&data).unwrap();
        assert_eq!(Property::vertex_labels(&store, 0), vec!["Post".to_string()]);
        assert!(Property::vertex_labels(&store, 999).is_empty());
    }

    #[test]
    fn test_scan_with_predicate() {
        let data = build_test_arrow_ipc(&[
            (1, "Post", "pk_1", "upsert", vec![("age".into(), PropValue::Int(30))]),
            (2, "Post", "pk_2", "upsert", vec![("age".into(), PropValue::Int(25))]),
            (3, "Post", "pk_3", "upsert", vec![("age".into(), PropValue::Int(35))]),
        ]);
        let store = ArrowWalStore::from_arrow_ipc_bytes(&data).unwrap();

        // Eq predicate
        let result = store.scan_vertices("Post", &Predicate::Eq("age".into(), PropValue::Int(30)));
        assert_eq!(result, vec![0]);

        // True predicate (all)
        let all = store.scan_vertices("Post", &Predicate::True);
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_find_by_pk() {
        let data = build_test_arrow_ipc(&[
            (1, "Post", "pk_1", "upsert", vec![("name".into(), PropValue::Str("alice".into()))]),
            (2, "Post", "pk_2", "upsert", vec![("name".into(), PropValue::Str("bob".into()))]),
        ]);
        let store = ArrowWalStore::from_arrow_ipc_bytes(&data).unwrap();

        assert_eq!(store.find_vid_by_pk("Post", "rkey", "pk_1"), Some(0));
        assert_eq!(store.find_vid_by_pk("Post", "rkey", "pk_2"), Some(1));
        assert_eq!(store.find_vid_by_pk("Post", "rkey", "pk_99"), None);
    }

    #[test]
    fn test_skip_delete_entries() {
        let data = build_test_arrow_ipc(&[
            (1, "Post", "pk_1", "upsert", vec![("name".into(), PropValue::Str("alice".into()))]),
            (2, "Post", "pk_2", "delete", vec![]),
        ]);
        let store = ArrowWalStore::from_arrow_ipc_bytes(&data).unwrap();
        // Delete entry should be skipped
        assert_eq!(store.len(), 1);
        assert_eq!(store.vertex_prop(0, "name"), Some(PropValue::Str("alice".into())));
    }

    #[test]
    fn test_vertex_all_props() {
        let data = build_test_arrow_ipc(&[
            (1, "Post", "pk_1", "upsert", vec![
                ("name".into(), PropValue::Str("alice".into())),
                ("age".into(), PropValue::Int(30)),
            ]),
        ]);
        let store = ArrowWalStore::from_arrow_ipc_bytes(&data).unwrap();
        let all = store.vertex_all_props(0);
        assert_eq!(all.len(), 2);
        assert_eq!(all.get("name"), Some(&PropValue::Str("alice".into())));
        assert_eq!(all.get("age"), Some(&PropValue::Int(30)));
    }

    #[test]
    fn test_vertex_prop_keys() {
        let data = build_test_arrow_ipc(&[
            (1, "Post", "pk_1", "upsert", vec![
                ("name".into(), PropValue::Str("alice".into())),
                ("age".into(), PropValue::Int(30)),
            ]),
            (2, "Post", "pk_2", "upsert", vec![
                ("name".into(), PropValue::Str("bob".into())),
                ("score".into(), PropValue::Float(9.5)),
            ]),
        ]);
        let store = ArrowWalStore::from_arrow_ipc_bytes(&data).unwrap();
        let keys = store.vertex_prop_keys("Post");
        assert!(keys.contains(&"name".to_string()));
        assert!(keys.contains(&"age".to_string()));
        assert!(keys.contains(&"score".to_string()));
    }

    #[test]
    fn test_edge_operations_return_empty() {
        let store = ArrowWalStore::new();
        assert_eq!(store.edge_count(), 0);
        assert!(store.out_neighbors(0).is_empty());
        assert!(store.in_neighbors(0).is_empty());
        assert!(store.edge_prop(0, "x").is_none());
        assert!(store.edge_prop_keys("REL").is_empty());
        assert!(Schema::edge_labels(&store).is_empty());
    }

    #[test]
    fn test_mmap_from_file() {
        let data = build_test_arrow_ipc(&[
            (1, "Post", "pk_1", "upsert", vec![("name".into(), PropValue::Str("alice".into()))]),
        ]);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compacted.arrow");
        std::fs::write(&path, &data).unwrap();

        let store = ArrowWalStore::from_file(&path).unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(store.vertex_prop(0, "name"), Some(PropValue::Str("alice".into())));
        // mmap handle should be held
        assert!(store._mmap.is_some());
    }
}

