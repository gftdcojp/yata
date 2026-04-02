use std::collections::{HashMap, HashSet};

use arrow::array::{Array, BooleanArray, StringArray, UInt8Array};
use arrow::record_batch::RecordBatch;
use yata_grin::{GraphStore, Mutable, Neighbor, Predicate, PropValue, Property, Scannable, Schema, Topology};

#[derive(Debug, Clone, Default)]
pub struct LanceReadStore {
    vertex_labels_by_vid: Vec<Vec<String>>,
    vertex_props: Vec<HashMap<String, PropValue>>,
    vertex_alive: Vec<bool>,
    edge_props: Vec<HashMap<String, PropValue>>,
    edge_labels: Vec<String>,
    out_adj: Vec<Vec<Neighbor>>,
    in_adj: Vec<Vec<Neighbor>>,
    label_index: HashMap<String, Vec<u32>>,
    known_vertex_labels: Vec<String>,
    known_edge_labels: Vec<String>,
    vertex_pk: HashMap<String, String>,
}

impl LanceReadStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_live_batches(
        vertex_batches: &[RecordBatch],
        edge_out_batches: &[RecordBatch],
    ) -> Result<Self, String> {
        let mut vid_map = HashMap::<String, u32>::new();
        let mut store = Self::default();

        for batch in vertex_batches {
            let labels = str_col(batch, 1)?;
            let pk_keys = str_col(batch, 2)?;
            let pk_values = str_col(batch, 3)?;
            let vids = str_col(batch, 4)?;
            let alive = bool_col(batch, 5)?;
            let repos = optional_str_col(batch, 7)?;
            let rkeys = optional_str_col(batch, 8)?;
            let owner_dids = optional_str_col(batch, 9)?;
            let props_json = optional_str_col(batch, 11)?;

            for row in 0..batch.num_rows() {
                let label = labels.value(row).to_string();
                let pk_key = pk_keys.value(row).to_string();
                let pk_value = pk_values.value(row).to_string();
                let raw_vid = vids.value(row).to_string();
                let is_alive = alive.value(row);

                let local_vid = store.vertex_alive.len() as u32;
                vid_map.insert(raw_vid, local_vid);
                store.vertex_alive.push(is_alive);
                store.vertex_labels_by_vid.push(vec![label.clone()]);

                if !store.known_vertex_labels.contains(&label) {
                    store.known_vertex_labels.push(label.clone());
                }
                store.vertex_pk.entry(label.clone()).or_insert(pk_key.clone());
                if is_alive {
                    store.label_index.entry(label).or_default().push(local_vid);
                }

                let mut props = parse_props_json(props_json[row].as_deref())?;
                props.entry(pk_key).or_insert(PropValue::Str(pk_value.clone()));
                props.entry("pk_value".to_string()).or_insert(PropValue::Str(pk_value));
                if let Some(repo) = &repos[row] {
                    props.entry("repo".to_string()).or_insert(PropValue::Str(repo.clone()));
                }
                if let Some(rkey) = &rkeys[row] {
                    props.entry("rkey".to_string()).or_insert(PropValue::Str(rkey.clone()));
                }
                if let Some(owner_did) = &owner_dids[row] {
                    props.entry("owner_did".to_string()).or_insert(PropValue::Str(owner_did.clone()));
                }
                store.vertex_props.push(props);
            }
        }

        store.out_adj = vec![Vec::new(); store.vertex_alive.len()];
        store.in_adj = vec![Vec::new(); store.vertex_alive.len()];

        for batch in edge_out_batches {
            let edge_labels = str_col(batch, 1)?;
            let eids = str_col(batch, 4)?;
            let src_vids = str_col(batch, 5)?;
            let dst_vids = str_col(batch, 6)?;
            let alive = bool_col(batch, 9)?;
            let props_json = optional_str_col(batch, 12)?;

            for row in 0..batch.num_rows() {
                if !alive.value(row) {
                    continue;
                }
                let src = match vid_map.get(src_vids.value(row)) {
                    Some(v) => *v,
                    None => continue,
                };
                let dst = match vid_map.get(dst_vids.value(row)) {
                    Some(v) => *v,
                    None => continue,
                };
                let edge_label = edge_labels.value(row).to_string();
                if !store.known_edge_labels.contains(&edge_label) {
                    store.known_edge_labels.push(edge_label.clone());
                }
                let mut props = parse_props_json(props_json[row].as_deref())?;
                props.entry("eid".to_string())
                    .or_insert(PropValue::Str(eids.value(row).to_string()));
                let edge_id = store.edge_labels.len() as u32;
                store.edge_labels.push(edge_label.clone());
                store.edge_props.push(props);
                store.out_adj[src as usize].push(Neighbor {
                    vid: dst,
                    edge_id,
                    edge_label: edge_label.clone(),
                });
                store.in_adj[dst as usize].push(Neighbor {
                    vid: src,
                    edge_id,
                    edge_label,
                });
            }
        }

        Ok(store)
    }

    /// Build read store from Lance-schema batches.
    /// Schema: seq(u64), op(u8), label(utf8), pk_key(utf8), pk_value(utf8), timestamp_ms(u64), props_json(utf8)
    pub fn from_lance_batches(batches: &[RecordBatch]) -> Result<Self, String> {
        let mut store = Self::default();
        // PK-dedup: last-writer-wins by (label, pk_key, pk_value) triple
        let mut pk_map: HashMap<(String, String, String), usize> = HashMap::new();

        for batch in batches {
            if batch.num_columns() < 7 { continue; }
            let op_col = batch.column(1).as_any().downcast_ref::<UInt8Array>()
                .ok_or("column 1 (op) is not UInt8")?;
            let labels = str_col(batch, 2)?;
            let pk_keys = str_col(batch, 3)?;
            let pk_values = str_col(batch, 4)?;
            let props_json = optional_str_col(batch, 6)?;

            for row in 0..batch.num_rows() {
                let op = op_col.value(row);
                let label = labels.value(row).to_string();
                let pk_key = pk_keys.value(row).to_string();
                let pk_value = pk_values.value(row).to_string();
                let is_alive = op == 0; // 0=upsert, 1=delete

                let dedup_key = (label.clone(), pk_key.clone(), pk_value.clone());
                if let Some(&existing_idx) = pk_map.get(&dedup_key) {
                    // Overwrite existing entry (last-writer-wins)
                    store.vertex_alive[existing_idx] = is_alive;
                    let mut props = parse_props_json(props_json[row].as_deref())?;
                    props.entry(pk_key.clone()).or_insert(PropValue::Str(pk_value.clone()));
                    props.entry("pk_value".to_string()).or_insert(PropValue::Str(pk_value));
                    store.vertex_props[existing_idx] = props;
                    continue;
                }

                let local_vid = store.vertex_alive.len();
                pk_map.insert(dedup_key, local_vid);
                store.vertex_alive.push(is_alive);
                store.vertex_labels_by_vid.push(vec![label.clone()]);

                if !store.known_vertex_labels.contains(&label) {
                    store.known_vertex_labels.push(label.clone());
                }
                store.vertex_pk.entry(label.clone()).or_insert(pk_key.clone());

                let mut props = parse_props_json(props_json[row].as_deref())?;
                props.entry(pk_key).or_insert(PropValue::Str(pk_value.clone()));
                props.entry("pk_value".to_string()).or_insert(PropValue::Str(pk_value));
                store.vertex_props.push(props);
            }
        }

        // Rebuild label_index from final alive state (after PK dedup)
        for (vid, (labels, alive)) in store.vertex_labels_by_vid.iter().zip(store.vertex_alive.iter()).enumerate() {
            if *alive {
                for label in labels {
                    store.label_index.entry(label.clone()).or_default().push(vid as u32);
                }
            }
        }

        store.out_adj = vec![Vec::new(); store.vertex_alive.len()];
        store.in_adj = vec![Vec::new(); store.vertex_alive.len()];
        Ok(store)
    }

    pub fn merge_vertex_by_pk(
        &mut self,
        label: &str,
        pk_key: &str,
        pk_value: &PropValue,
        props: &[(&str, PropValue)],
    ) -> u32 {
        if let Some(existing) = self.find_vertex_by_pk(label, pk_key, pk_value) {
            if let Some(prop_map) = self.vertex_props.get_mut(existing as usize) {
                for (k, v) in props {
                    prop_map.insert((*k).to_string(), v.clone());
                }
                prop_map.entry(pk_key.to_string()).or_insert(pk_value.clone());
            }
            if let Some(alive) = self.vertex_alive.get_mut(existing as usize) {
                *alive = true;
            }
            return existing;
        }

        let vid = self.vertex_alive.len() as u32;
        self.vertex_alive.push(true);
        self.vertex_labels_by_vid.push(vec![label.to_string()]);
        self.label_index.entry(label.to_string()).or_default().push(vid);
        if !self.known_vertex_labels.contains(&label.to_string()) {
            self.known_vertex_labels.push(label.to_string());
        }
        self.vertex_pk
            .entry(label.to_string())
            .or_insert_with(|| pk_key.to_string());

        let mut prop_map = HashMap::new();
        prop_map.insert(pk_key.to_string(), pk_value.clone());
        for (k, v) in props {
            prop_map.insert((*k).to_string(), v.clone());
        }
        self.vertex_props.push(prop_map);
        self.out_adj.push(Vec::new());
        self.in_adj.push(Vec::new());
        vid
    }

    pub fn delete_vertex_by_pk(&mut self, label: &str, pk_key: &str, pk_value: &PropValue) -> bool {
        let Some(vid) = self.find_vertex_by_pk(label, pk_key, pk_value) else {
            return false;
        };
        if let Some(alive) = self.vertex_alive.get_mut(vid as usize) {
            *alive = false;
        }
        if let Some(vids) = self.label_index.get_mut(label) {
            vids.retain(|&v| v != vid);
        }
        true
    }

    pub fn add_edge_cache_entry(
        &mut self,
        src: u32,
        dst: u32,
        edge_id: u32,
        edge_label: String,
        props: HashMap<String, PropValue>,
    ) {
        if self.out_adj.len() <= src as usize {
            self.out_adj.resize(src as usize + 1, Vec::new());
        }
        if self.in_adj.len() <= dst as usize {
            self.in_adj.resize(dst as usize + 1, Vec::new());
        }
        if self.edge_labels.len() <= edge_id as usize {
            self.edge_labels.resize(edge_id as usize + 1, String::new());
            self.edge_props.resize(edge_id as usize + 1, HashMap::new());
        }
        self.edge_labels[edge_id as usize] = edge_label.clone();
        self.edge_props[edge_id as usize] = props;
        if !self.known_edge_labels.contains(&edge_label) {
            self.known_edge_labels.push(edge_label.clone());
        }
        self.out_adj[src as usize].push(Neighbor {
            vid: dst,
            edge_id,
            edge_label: edge_label.clone(),
        });
        self.in_adj[dst as usize].push(Neighbor {
            vid: src,
            edge_id,
            edge_label,
        });
    }

    pub fn find_vertex_by_pk(&self, label: &str, pk_key: &str, pk_value: &PropValue) -> Option<u32> {
        self.label_index.get(label)?.iter().copied().find(|&vid| {
            self.vertex_alive.get(vid as usize).copied().unwrap_or(false)
                && self
                    .vertex_props
                    .get(vid as usize)
                    .and_then(|props| props.get(pk_key))
                    == Some(pk_value)
        })
    }
}

impl Mutable for LanceReadStore {
    fn add_vertex(&mut self, label: &str, props: &[(&str, PropValue)]) -> u32 {
        let pk_key = props
            .iter()
            .find_map(|(k, _)| matches!(*k, "rkey" | "id" | "name").then_some(*k))
            .unwrap_or("_vid");
        let fallback = PropValue::Str(format!("v{}", self.vertex_alive.len()));
        let pk_value = props
            .iter()
            .find(|(k, _)| *k == pk_key)
            .map(|(_, v)| v.clone())
            .unwrap_or(fallback);
        self.merge_vertex_by_pk(label, pk_key, &pk_value, props)
    }

    fn add_edge(&mut self, src: u32, dst: u32, label: &str, props: &[(&str, PropValue)]) -> u32 {
        let edge_id = self.edge_labels.len() as u32;
        let mut prop_map: HashMap<String, PropValue> = props
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        prop_map
            .entry("eid".to_string())
            .or_insert_with(|| PropValue::Str(format!("e{edge_id}")));
        self.add_edge_cache_entry(src, dst, edge_id, label.to_string(), prop_map);
        edge_id
    }

    fn set_vertex_prop(&mut self, vid: u32, key: &str, value: PropValue) {
        if let Some(props) = self.vertex_props.get_mut(vid as usize) {
            props.insert(key.to_string(), value);
        }
    }

    fn delete_vertex(&mut self, vid: u32) {
        if let Some(alive) = self.vertex_alive.get_mut(vid as usize) {
            *alive = false;
        }
        for vids in self.label_index.values_mut() {
            vids.retain(|&v| v != vid);
        }
    }

    fn delete_edge(&mut self, edge_id: u32) {
        if let Some(label) = self.edge_labels.get_mut(edge_id as usize) {
            label.clear();
        }
        if let Some(props) = self.edge_props.get_mut(edge_id as usize) {
            props.clear();
        }
        for neighbors in &mut self.out_adj {
            neighbors.retain(|n| n.edge_id != edge_id);
        }
        for neighbors in &mut self.in_adj {
            neighbors.retain(|n| n.edge_id != edge_id);
        }
        self.known_edge_labels.retain(|label| !label.is_empty());
    }

    fn commit(&mut self) -> u64 {
        0
    }
}

impl Topology for LanceReadStore {
    fn vertex_count(&self) -> usize {
        self.vertex_alive.iter().filter(|&&v| v).count()
    }

    fn edge_count(&self) -> usize {
        self.edge_labels.len()
    }

    fn has_vertex(&self, vid: u32) -> bool {
        self.vertex_alive.get(vid as usize).copied().unwrap_or(false)
    }

    fn out_degree(&self, vid: u32) -> usize {
        self.out_adj.get(vid as usize).map(|v| v.len()).unwrap_or(0)
    }

    fn in_degree(&self, vid: u32) -> usize {
        self.in_adj.get(vid as usize).map(|v| v.len()).unwrap_or(0)
    }

    fn out_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        self.out_adj.get(vid as usize).cloned().unwrap_or_default()
    }

    fn in_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        self.in_adj.get(vid as usize).cloned().unwrap_or_default()
    }

    fn out_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        self.out_neighbors(vid)
            .into_iter()
            .filter(|n| n.edge_label == edge_label)
            .collect()
    }

    fn in_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        self.in_neighbors(vid)
            .into_iter()
            .filter(|n| n.edge_label == edge_label)
            .collect()
    }
}

impl Property for LanceReadStore {
    fn vertex_labels(&self, vid: u32) -> Vec<String> {
        self.vertex_labels_by_vid
            .get(vid as usize)
            .cloned()
            .unwrap_or_default()
    }

    fn vertex_prop(&self, vid: u32, key: &str) -> Option<PropValue> {
        self.vertex_props.get(vid as usize)?.get(key).cloned()
    }

    fn edge_prop(&self, edge_id: u32, key: &str) -> Option<PropValue> {
        self.edge_props.get(edge_id as usize)?.get(key).cloned()
    }

    fn vertex_prop_keys(&self, label: &str) -> Vec<String> {
        let mut keys = HashSet::new();
        for &vid in self.label_index.get(label).into_iter().flatten() {
            if let Some(props) = self.vertex_props.get(vid as usize) {
                keys.extend(props.keys().cloned());
            }
        }
        let mut keys: Vec<String> = keys.into_iter().collect();
        keys.sort();
        keys
    }

    fn edge_prop_keys(&self, label: &str) -> Vec<String> {
        let mut keys = HashSet::new();
        for (idx, edge_label) in self.edge_labels.iter().enumerate() {
            if edge_label == label {
                keys.extend(self.edge_props[idx].keys().cloned());
            }
        }
        let mut keys: Vec<String> = keys.into_iter().collect();
        keys.sort();
        keys
    }

    fn vertex_all_props(&self, vid: u32) -> HashMap<String, PropValue> {
        self.vertex_props.get(vid as usize).cloned().unwrap_or_default()
    }
}

impl Schema for LanceReadStore {
    fn vertex_labels(&self) -> Vec<String> {
        self.known_vertex_labels.clone()
    }

    fn edge_labels(&self) -> Vec<String> {
        self.known_edge_labels.clone()
    }

    fn vertex_primary_key(&self, label: &str) -> Option<String> {
        self.vertex_pk.get(label).cloned()
    }
}

impl Scannable for LanceReadStore {
    fn scan_vertices(&self, label: &str, predicate: &Predicate) -> Vec<u32> {
        self.scan_vertices_by_label(label)
            .into_iter()
            .filter(|&vid| predicate_matches_vertex(self, vid, predicate))
            .collect()
    }

    fn scan_vertices_by_label(&self, label: &str) -> Vec<u32> {
        self.label_index.get(label).cloned().unwrap_or_default()
    }

    fn scan_all_vertices(&self) -> Vec<u32> {
        self.vertex_alive
            .iter()
            .enumerate()
            .filter_map(|(vid, alive)| if *alive { Some(vid as u32) } else { None })
            .collect()
    }
}

fn predicate_matches_vertex<S: GraphStore>(store: &S, vid: u32, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::True => true,
        Predicate::Eq(key, val) => store.vertex_prop(vid, key).as_ref() == Some(val),
        Predicate::Neq(key, val) => store.vertex_prop(vid, key).as_ref() != Some(val),
        Predicate::Lt(key, val) => store
            .vertex_prop(vid, key)
            .is_some_and(|v| prop_value_cmp(&v, val).is_lt()),
        Predicate::Gt(key, val) => store
            .vertex_prop(vid, key)
            .is_some_and(|v| prop_value_cmp(&v, val).is_gt()),
        Predicate::In(key, vals) => store
            .vertex_prop(vid, key)
            .is_some_and(|v| vals.contains(&v)),
        Predicate::StartsWith(key, prefix) => match store.vertex_prop(vid, key) {
            Some(PropValue::Str(s)) => s.starts_with(prefix),
            _ => false,
        },
        Predicate::And(a, b) => predicate_matches_vertex(store, vid, a) && predicate_matches_vertex(store, vid, b),
        Predicate::Or(a, b) => predicate_matches_vertex(store, vid, a) || predicate_matches_vertex(store, vid, b),
    }
}

fn prop_value_cmp(a: &PropValue, b: &PropValue) -> std::cmp::Ordering {
    match (a, b) {
        (PropValue::Int(x), PropValue::Int(y)) => x.cmp(y),
        (PropValue::Float(x), PropValue::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (PropValue::Str(x), PropValue::Str(y)) => x.cmp(y),
        (PropValue::Bool(x), PropValue::Bool(y)) => x.cmp(y),
        (PropValue::Null, PropValue::Null) => std::cmp::Ordering::Equal,
        (PropValue::Null, _) => std::cmp::Ordering::Less,
        (_, PropValue::Null) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}

fn parse_props_json(raw: Option<&str>) -> Result<HashMap<String, PropValue>, String> {
    let Some(raw) = raw else {
        return Ok(HashMap::new());
    };
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    let serde_json::Value::Object(map) = value else {
        return Ok(HashMap::new());
    };
    Ok(map
        .into_iter()
        .map(|(k, v)| (k, json_to_prop(v)))
        .collect())
}

fn json_to_prop(v: serde_json::Value) -> PropValue {
    match v {
        serde_json::Value::Null => PropValue::Null,
        serde_json::Value::Bool(v) => PropValue::Bool(v),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                PropValue::Float(f)
            } else {
                PropValue::Null
            }
        }
        serde_json::Value::String(s) => PropValue::Str(s),
        other => PropValue::Str(other.to_string()),
    }
}

fn str_col<'a>(batch: &'a RecordBatch, idx: usize) -> Result<&'a StringArray, String> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("column {idx} is not Utf8"))
}

fn bool_col<'a>(batch: &'a RecordBatch, idx: usize) -> Result<&'a BooleanArray, String> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| format!("column {idx} is not Boolean"))
}

fn optional_str_col(batch: &RecordBatch, idx: usize) -> Result<Vec<Option<String>>, String> {
    let col = str_col(batch, idx)?;
    Ok((0..batch.num_rows())
        .map(|row| (!col.is_null(row)).then(|| col.value(row).to_string()))
        .collect())
}

// ── ArrowStore: zero-copy LanceDB read store ──────────────────────────
// Holds Arrow RecordBatches directly. No eager props_json parsing.
// Dedup index built in O(N) scanning only label/pk_value/op columns.
// Props parsed lazily on vertex_prop() access.

/// Row location within batches.
#[derive(Clone, Copy)]
struct RowLoc {
    batch: u16,
    row: u32,
}

/// Batch storage: either in-memory Vec or spilled to disk IPC file.
enum BatchStorage {
    /// All batches held in RAM (default, fast path).
    InMemory(Vec<RecordBatch>),
    /// Batches spilled to a local Arrow IPC file; loaded on demand per batch index.
    /// The file path and batch count are stored. Individual batches are read via
    /// Arrow IPC FileReader seeking to the batch offset.
    Spilled {
        path: std::path::PathBuf,
        batch_count: usize,
        /// Cache of recently accessed batches (LRU-ish, small fixed size).
        cache: std::sync::Mutex<HashMap<u16, RecordBatch>>,
    },
}

impl BatchStorage {
    /// Get a batch by index. For InMemory, this is O(1). For Spilled, reads from IPC file.
    fn get(&self, idx: u16) -> Option<RecordBatch> {
        match self {
            BatchStorage::InMemory(batches) => batches.get(idx as usize).cloned(),
            BatchStorage::Spilled { path, batch_count, cache, .. } => {
                if idx as usize >= *batch_count { return None; }
                // Check cache first
                if let Some(batch) = cache.lock().unwrap().get(&idx) {
                    return Some(batch.clone());
                }
                // Read from IPC file
                let file = std::fs::File::open(path).ok()?;
                let reader = arrow::ipc::reader::FileReader::try_new(file, None).ok()?;
                let batch = reader.into_iter().nth(idx as usize)?.ok()?;
                // Cache it (evict if cache too large)
                let mut c = cache.lock().unwrap();
                if c.len() >= 8 {
                    // Evict oldest (simple: clear all)
                    c.clear();
                }
                c.insert(idx, batch.clone());
                Some(batch)
            }
        }
    }

}

/// Zero-copy read store backed by Arrow RecordBatches from LanceDB.
pub struct ArrowStore {
    storage: BatchStorage,
    /// vid → row location (only alive, after PK dedup)
    rows: Vec<RowLoc>,
    label_index: HashMap<String, Vec<u32>>,
    vertex_labels: Vec<String>,
    known_labels: Vec<String>,
    pk_keys: HashMap<String, String>,
    edge_props: Vec<HashMap<String, PropValue>>,
    edge_labels: Vec<String>,
    known_edge_labels: Vec<String>,
    out_adj: Vec<Vec<Neighbor>>,
    in_adj: Vec<Vec<Neighbor>>,
    /// Lazy props cache: vid → parsed props
    props_cache: std::sync::Mutex<HashMap<u32, HashMap<String, PropValue>>>,
}

impl ArrowStore {
    /// Build from Lance-schema batches. O(N) scan of label/pk_value/op columns only.
    ///
    /// Supports both legacy 7-col schema (seq, op, label, pk_key, pk_value, ts, props_json)
    /// and new 10-col Format D schema (op, label, pk_value, ts, repo, owner_did, name, app_id, rkey, val_json).
    /// Auto-detects schema by column count and first column type.
    pub fn from_batches(batches: Vec<RecordBatch>) -> Result<Self, String> {
        let (rows, label_index, vertex_labels, known_labels, pk_keys) =
            Self::build_dedup_index(&batches)?;

        Ok(Self {
            storage: BatchStorage::InMemory(batches),
            rows,
            label_index,
            vertex_labels,
            known_labels,
            pk_keys,
            props_cache: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Build from batches with disk spill when total size exceeds `budget_bytes`.
    ///
    /// If total batch size is within budget, behaves identically to `from_batches()`.
    /// Otherwise, writes batches to a temporary Arrow IPC file in `spill_dir` and
    /// reads them back on demand. The dedup index (small) stays in RAM.
    pub fn from_batches_with_spill(
        batches: Vec<RecordBatch>,
        budget_bytes: u64,
        spill_dir: &str,
    ) -> Result<Self, String> {
        // Estimate total batch size from Arrow buffer sizes
        let total_bytes: u64 = batches.iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        // Build dedup index first (always in memory — small relative to data)
        let (rows, label_index, vertex_labels, known_labels, pk_keys) =
            Self::build_dedup_index(&batches)?;

        let storage = if total_bytes <= budget_bytes || batches.is_empty() {
            BatchStorage::InMemory(batches)
        } else {
            // Write to IPC file
            std::fs::create_dir_all(spill_dir)
                .map_err(|e| format!("create spill dir: {e}"))?;
            let path = std::path::PathBuf::from(spill_dir)
                .join(format!("arrowstore-{}.ipc", std::process::id()));
            let file = std::fs::File::create(&path)
                .map_err(|e| format!("create spill file: {e}"))?;
            let batch_schema = batches[0].schema();
            let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &batch_schema)
                .map_err(|e| format!("IPC writer: {e}"))?;
            let batch_count = batches.len();
            for batch in &batches {
                writer.write(batch).map_err(|e| format!("IPC write: {e}"))?;
            }
            writer.finish().map_err(|e| format!("IPC finish: {e}"))?;
            drop(batches); // Free RAM
            tracing::info!(
                total_mb = total_bytes / (1024 * 1024),
                batch_count,
                path = %path.display(),
                "ArrowStore spilled to disk"
            );
            BatchStorage::Spilled {
                path,
                batch_count,
                cache: std::sync::Mutex::new(HashMap::new()),
            }
        };

        Ok(Self {
            storage,
            rows,
            label_index,
            vertex_labels,
            known_labels,
            pk_keys,
            props_cache: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Build dedup index from batches (shared between from_batches and from_batches_with_spill).
    fn build_dedup_index(
        batches: &[RecordBatch],
    ) -> Result<(
        Vec<RowLoc>,
        HashMap<String, Vec<u32>>,
        Vec<String>,
        Vec<String>,
        HashMap<String, String>,
    ), String> {
        let mut dedup: HashMap<(String, String), (u16, u32, bool)> = HashMap::new();
        for (bi, batch) in batches.iter().enumerate() {
            if batch.num_columns() < 3 { continue; }

            // Detect schema: Format D has op(UInt8) at col 0, legacy has seq(UInt64) at col 0
            let is_format_d = batch.schema().field(0).data_type() == &arrow::datatypes::DataType::UInt8;

            let (op_idx, label_idx, pk_val_idx) = if is_format_d {
                (0usize, 1usize, 2usize)
            } else {
                (1usize, 2usize, 4usize)
            };

            let op_col = batch.column(op_idx).as_any().downcast_ref::<UInt8Array>()
                .ok_or(format!("column {op_idx} (op) is not UInt8"))?;
            let label_col = str_col(batch, label_idx)?;
            let pk_val_col = str_col(batch, pk_val_idx)?;

            for row in 0..batch.num_rows() {
                let key = (
                    label_col.value(row).to_string(),
                    pk_val_col.value(row).to_string(),
                );
                dedup.insert(key, (bi as u16, row as u32, op_col.value(row) == 0));
            }
        }

        let mut rows = Vec::new();
        let mut label_index: HashMap<String, Vec<u32>> = HashMap::new();
        let mut vertex_labels = Vec::new();
        let mut known_labels = Vec::new();
        let mut pk_keys: HashMap<String, String> = HashMap::new();

        for ((label, _), (bi, ri, alive)) in &dedup {
            if !*alive { continue; }
            let vid = rows.len() as u32;
            rows.push(RowLoc { batch: *bi, row: *ri });
            vertex_labels.push(label.clone());
            label_index.entry(label.clone()).or_default().push(vid);
            if !known_labels.contains(label) { known_labels.push(label.clone()); }
            pk_keys.entry(label.clone()).or_insert_with(|| "rkey".to_string());
        }

        Ok((rows, label_index, vertex_labels, known_labels, pk_keys))
    }

    /// Get a batch from storage (in-memory or disk).
    fn get_batch(&self, idx: u16) -> Option<RecordBatch> {
        self.storage.get(idx)
    }

    fn col_str(&self, vid: u32, col_idx: usize) -> Option<String> {
        let loc = self.rows.get(vid as usize)?;
        let batch = self.get_batch(loc.batch)?;
        let col = batch.column(col_idx).as_any().downcast_ref::<StringArray>()?;
        Some(col.value(loc.row as usize).to_string())
    }

    fn ensure_props(&self, vid: u32) -> Option<()> {
        if self.props_cache.lock().unwrap().contains_key(&vid) { return Some(()); }
        let loc = self.rows.get(vid as usize)?;
        let batch = self.get_batch(loc.batch)?;
        // Format D: val_json at col 9. Legacy: props_json at col 6.
        let is_format_d = batch.schema().field(0).data_type() == &arrow::datatypes::DataType::UInt8;
        let json_col_idx = if is_format_d { 9 } else { 6 };
        if json_col_idx >= batch.num_columns() { return Some(()); }
        let col = batch.column(json_col_idx).as_any().downcast_ref::<StringArray>()?;
        if col.is_null(loc.row as usize) {
            self.props_cache.lock().unwrap().insert(vid, HashMap::new());
            return Some(());
        }
        let parsed = parse_props_json(Some(col.value(loc.row as usize))).ok()?;
        self.props_cache.lock().unwrap().insert(vid, parsed);
        Some(())
    }

    pub fn vertex_count(&self) -> usize { self.rows.len() }

    pub fn find_vertex_by_pk(&self, label: &str, _pk_key: &str, pk_value: &PropValue) -> Option<u32> {
        let expected = match pk_value { PropValue::Str(s) => s.as_str(), _ => return None };
        self.label_index.get(label)?.iter().copied().find(|&vid| {
            // Detect schema to find pk_value column index
            let loc = match self.rows.get(vid as usize) { Some(l) => l, None => return false };
            let batch = match self.get_batch(loc.batch) { Some(b) => b, None => return false };
            let is_format_d = batch.schema().field(0).data_type() == &arrow::datatypes::DataType::UInt8;
            let pk_col = if is_format_d { 2 } else { 4 };
            self.col_str(vid, pk_col).as_deref() == Some(expected)
        })
    }
}

impl Topology for ArrowStore {
    fn vertex_count(&self) -> usize { self.rows.len() }
    fn edge_count(&self) -> usize { 0 }
    fn has_vertex(&self, vid: u32) -> bool { (vid as usize) < self.rows.len() }
    fn out_degree(&self, _vid: u32) -> usize { 0 }
    fn in_degree(&self, _vid: u32) -> usize { 0 }
    fn out_neighbors(&self, _vid: u32) -> Vec<Neighbor> { Vec::new() }
    fn in_neighbors(&self, _vid: u32) -> Vec<Neighbor> { Vec::new() }
    fn out_neighbors_by_label(&self, _vid: u32, _edge_label: &str) -> Vec<Neighbor> { Vec::new() }
    fn in_neighbors_by_label(&self, _vid: u32, _edge_label: &str) -> Vec<Neighbor> { Vec::new() }
}

impl Property for ArrowStore {
    fn vertex_labels(&self, vid: u32) -> Vec<String> {
        self.vertex_labels.get(vid as usize).into_iter().cloned().collect()
    }

    fn vertex_prop(&self, vid: u32, key: &str) -> Option<PropValue> {
        // Detect schema: Format D has op(UInt8) at col 0
        let loc = self.rows.get(vid as usize)?;
        let batch = self.get_batch(loc.batch)?;
        let is_format_d = batch.schema().field(0).data_type() == &arrow::datatypes::DataType::UInt8;

        if is_format_d {
            // Format D: op=0, label=1, pk_value=2, ts=3, repo=4, owner_did=5, name=6, app_id=7, rkey=8, val_json=9
            match key {
                "pk_value" | "rkey" => return self.col_str(vid, 2).map(PropValue::Str), // pk_value is rkey
                "label" => return self.col_str(vid, 1).map(PropValue::Str),
                "repo" => return self.col_str(vid, 4).map(PropValue::Str),
                "owner_did" => return self.col_str(vid, 5).map(PropValue::Str),
                "name" => return self.col_str(vid, 6).map(PropValue::Str),
                "app_id" | "_app_id" => return self.col_str(vid, 7).map(PropValue::Str),
                _ => {}
            }
        } else {
            // Legacy 7-col: seq=0, op=1, label=2, pk_key=3, pk_value=4, ts=5, props_json=6
            match key {
                "rkey" | "pk_value" => return self.col_str(vid, 4).map(PropValue::Str),
                "pk_key" => return self.col_str(vid, 3).map(PropValue::Str),
                "label" => return self.col_str(vid, 2).map(PropValue::Str),
                _ => {}
            }
        }
        // Slow path: parse val_json/props_json (cached per-vid)
        self.ensure_props(vid);
        self.props_cache.lock().unwrap().get(&vid)?.get(key).cloned()
    }

    fn edge_prop(&self, _edge_id: u32, _key: &str) -> Option<PropValue> { None }

    fn vertex_prop_keys(&self, label: &str) -> Vec<String> {
        let mut keys = HashSet::new();
        for &vid in self.label_index.get(label).into_iter().flatten() {
            self.ensure_props(vid);
            if let Some(props) = self.props_cache.lock().unwrap().get(&vid) {
                keys.extend(props.keys().cloned());
            }
        }
        keys.into_iter().collect()
    }

    fn edge_prop_keys(&self, _label: &str) -> Vec<String> { Vec::new() }

    fn vertex_all_props(&self, vid: u32) -> HashMap<String, PropValue> {
        self.ensure_props(vid);
        self.props_cache.lock().unwrap().get(&vid).cloned().unwrap_or_default()
    }
}

impl Schema for ArrowStore {
    fn vertex_labels(&self) -> Vec<String> { self.known_labels.clone() }
    fn edge_labels(&self) -> Vec<String> { Vec::new() }
    fn vertex_primary_key(&self, label: &str) -> Option<String> { self.pk_keys.get(label).cloned() }
}

impl Drop for ArrowStore {
    fn drop(&mut self) {
        if let BatchStorage::Spilled { ref path, .. } = self.storage {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Scannable for ArrowStore {
    fn scan_vertices(&self, label: &str, predicate: &Predicate) -> Vec<u32> {
        self.scan_vertices_by_label(label)
            .into_iter()
            .filter(|&vid| predicate_matches_vertex(self, vid, predicate))
            .collect()
    }
    fn scan_vertices_by_label(&self, label: &str) -> Vec<u32> {
        self.label_index.get(label).cloned().unwrap_or_default()
    }
    fn scan_all_vertices(&self) -> Vec<u32> {
        (0..self.rows.len() as u32).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BooleanArray, StringArray, UInt32Array, UInt64Array};

    use super::*;

    fn vertex_batch() -> RecordBatch {
        let schema = Arc::new(crate::vertex_live_schema().clone());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from(vec![0u32, 0])),
                Arc::new(StringArray::from(vec!["Person", "Person"])),
                Arc::new(StringArray::from(vec!["rkey", "rkey"])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(StringArray::from(vec!["v0", "v1"])),
                Arc::new(BooleanArray::from(vec![true, true])),
                Arc::new(UInt64Array::from(vec![1u64, 2])),
                Arc::new(StringArray::from(vec![Some("bench"), Some("bench")])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
                Arc::new(StringArray::from(vec![None::<String>, None::<String>])),
                Arc::new(UInt64Array::from(vec![1u64, 2])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"name":"Alice","age":30}"#),
                    Some(r#"{"name":"Bob","age":25}"#),
                ])),
            ],
        )
        .unwrap()
    }

    fn edge_batch() -> RecordBatch {
        let schema = Arc::new(crate::edge_live_out_schema().clone());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from(vec![0u32])),
                Arc::new(StringArray::from(vec!["KNOWS"])),
                Arc::new(StringArray::from(vec!["eid"])),
                Arc::new(StringArray::from(vec!["e0"])),
                Arc::new(StringArray::from(vec!["e0"])),
                Arc::new(StringArray::from(vec!["v0"])),
                Arc::new(StringArray::from(vec!["v1"])),
                Arc::new(StringArray::from(vec![Some("Person")])),
                Arc::new(StringArray::from(vec![Some("Person")])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(StringArray::from(vec![Some(r#"{"since":2020}"#)])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_from_live_batches_scan_and_expand() {
        let store = LanceReadStore::from_live_batches(&[vertex_batch()], &[edge_batch()]).unwrap();
        let vids = store.scan_vertices_by_label("Person");
        assert_eq!(vids.len(), 2);
        assert_eq!(store.vertex_prop(0, "name"), Some(PropValue::Str("Alice".into())));
        let out = store.out_neighbors_by_label(0, "KNOWS");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].vid, 1);
        assert_eq!(store.edge_prop(out[0].edge_id, "since"), Some(PropValue::Int(2020)));
    }

    #[test]
    fn test_predicate_scan() {
        let store = LanceReadStore::from_live_batches(&[vertex_batch()], &[edge_batch()]).unwrap();
        let vids = store.scan_vertices("Person", &Predicate::Gt("age".into(), PropValue::Int(26)));
        assert_eq!(vids, vec![0]);
    }
}
