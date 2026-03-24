//! ArrowGraphStore: Vineyard-native graph store backed by Arrow RecordBatch.
//!
//! Replaces MutableCsrStore as the primary query engine.
//! Arrow IPC blobs from Vineyard are used directly — no CSR conversion.
//!
//! Design:
//! - Vertex data: per-label Arrow RecordBatch (columnar, zero-copy on MmapVineyard)
//! - Edge data: per-label Arrow RecordBatch with _src/_dst columns
//! - Topology: lightweight CSR built from _src/_dst columns only (no prop copy)
//! - PK index: HashMap for O(1) merge_by_pk
//! - Pending mutations: WAL buffer, merged into Arrow on commit

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::ipc::reader::StreamReader;
use arrow_array::{Array, RecordBatch, StringArray, Int64Array, Float64Array, ArrayRef};
use arrow_array::builder::StringBuilder;
use arrow_schema::{Schema as ArrowSchema, Field, DataType};
use std::sync::Arc;

use yata_core::PartitionId;
use yata_grin::{self, Topology, Property, Neighbor, PropValue, Predicate};

/// Lightweight CSR segment for topology only (no property storage).
struct CsrSegment {
    /// out_offsets[vid] .. out_offsets[vid+1] = range in targets
    out_offsets: Vec<u32>,
    /// target VIDs (packed)
    out_targets: Vec<u32>,
    /// in_offsets[vid] .. in_offsets[vid+1] = range in sources
    in_offsets: Vec<u32>,
    /// source VIDs (packed)
    in_sources: Vec<u32>,
    /// edge IDs corresponding to out_targets
    out_edge_ids: Vec<u32>,
    /// edge IDs corresponding to in_sources
    in_edge_ids: Vec<u32>,
}

/// Pending vertex mutation (not yet in Arrow).
struct PendingVertex {
    label: String,
    props: HashMap<String, PropValue>,
}

/// Pending edge mutation.
struct PendingEdge {
    label: String,
    src: u32,
    dst: u32,
    props: HashMap<String, PropValue>,
}

/// ArrowGraphStore: GRIN-compliant graph store backed by Arrow RecordBatch.
pub struct ArrowGraphStore {
    /// Per-label vertex RecordBatch (from Arrow IPC, zero-copy on mmap).
    vertex_batches: HashMap<String, Vec<RecordBatch>>,
    /// Per-label edge RecordBatch.
    edge_batches: HashMap<String, Vec<RecordBatch>>,
    /// Per-label lightweight CSR for topology.
    csr: HashMap<String, CsrSegment>,
    /// VID → (label, batch_index, row_index) for O(1) property lookup.
    vid_index: HashMap<u32, (String, usize, usize)>,
    /// PK index: (label, pk_key) → { pk_value → vid }.
    pk_index: HashMap<(String, String), HashMap<String, u32>>,
    /// Pending vertex mutations (WAL buffer).
    pending_vertices: Vec<PendingVertex>,
    /// Pending property updates for existing vertices.
    pending_props: HashMap<u32, HashMap<String, PropValue>>,
    /// Next VID.
    next_vid: AtomicU64,
    /// Next EID.
    next_eid: AtomicU64,
    /// All known vertex labels.
    vertex_label_set: Vec<String>,
    /// All known edge labels.
    edge_label_set: Vec<String>,
    /// Partition ID.
    partition_id: PartitionId,
    /// Loaded labels (to avoid re-loading).
    loaded_labels: std::collections::HashSet<String>,
}

impl ArrowGraphStore {
    pub fn new(partition_id: PartitionId) -> Self {
        Self {
            vertex_batches: HashMap::new(),
            edge_batches: HashMap::new(),
            csr: HashMap::new(),
            vid_index: HashMap::new(),
            pk_index: HashMap::new(),
            pending_vertices: Vec::new(),
            pending_props: HashMap::new(),
            next_vid: AtomicU64::new(0),
            next_eid: AtomicU64::new(0),
            vertex_label_set: Vec::new(),
            edge_label_set: Vec::new(),
            partition_id,
            loaded_labels: std::collections::HashSet::new(),
        }
    }

    /// Load a vertex label from Arrow IPC bytes (Vineyard blob).
    /// Zero-decode: directly stores RecordBatch.
    pub fn load_vertex_label(&mut self, label: &str, ipc_bytes: &[u8]) -> Result<usize, String> {
        let cursor = std::io::Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| format!("arrow ipc decode: {e}"))?;

        let mut batches = Vec::new();
        let mut count = 0usize;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| format!("arrow batch: {e}"))?;
            let nrows = batch.num_rows();

            // Build vid_index for each row
            for row in 0..nrows {
                let vid = self.next_vid.fetch_add(1, Ordering::Relaxed) as u32;
                self.vid_index.insert(vid, (label.to_string(), batches.len(), row));

                // Build PK index from all string columns
                for col_idx in 0..batch.num_columns() {
                    let field = batch.schema().field(col_idx);
                    if let Some(arr) = batch.column(col_idx).as_any().downcast_ref::<StringArray>() {
                        if !arr.is_null(row) {
                            let val = arr.value(row).to_string();
                            self.pk_index
                                .entry((label.to_string(), field.name().clone()))
                                .or_default()
                                .insert(val, vid);
                        }
                    }
                }
            }

            count += nrows;
            batches.push(batch);
        }

        if !self.vertex_label_set.contains(&label.to_string()) {
            self.vertex_label_set.push(label.to_string());
        }
        self.vertex_batches.insert(label.to_string(), batches);
        self.loaded_labels.insert(label.to_string());
        Ok(count)
    }

    /// Load an edge label from Arrow IPC bytes. Builds lightweight CSR.
    pub fn load_edge_label(&mut self, label: &str, ipc_bytes: &[u8]) -> Result<usize, String> {
        let cursor = std::io::Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| format!("arrow ipc decode: {e}"))?;

        let mut batches = Vec::new();
        let mut edges: Vec<(u32, u32, u32)> = Vec::new(); // (src, dst, eid)

        for batch_result in reader {
            let batch = batch_result.map_err(|e| format!("arrow batch: {e}"))?;
            let schema = batch.schema();

            // Find _src and _dst columns
            let src_idx = schema.fields().iter().position(|f| f.name() == "_src");
            let dst_idx = schema.fields().iter().position(|f| f.name() == "_dst");

            if let (Some(si), Some(di)) = (src_idx, dst_idx) {
                let src_col = batch.column(si).as_any().downcast_ref::<Int64Array>();
                let dst_col = batch.column(di).as_any().downcast_ref::<Int64Array>();

                if let (Some(srcs), Some(dsts)) = (src_col, dst_col) {
                    for row in 0..batch.num_rows() {
                        let src = srcs.value(row) as u32;
                        let dst = dsts.value(row) as u32;
                        let eid = self.next_eid.fetch_add(1, Ordering::Relaxed) as u32;
                        edges.push((src, dst, eid));
                    }
                }
            }

            batches.push(batch);
        }

        // Build lightweight CSR from edges
        let max_vid = self.next_vid.load(Ordering::Relaxed) as u32;
        let csr = build_csr_from_edges(&edges, max_vid);

        if !self.edge_label_set.contains(&label.to_string()) {
            self.edge_label_set.push(label.to_string());
        }
        self.edge_batches.insert(label.to_string(), batches);
        self.csr.insert(label.to_string(), csr);
        self.loaded_labels.insert(label.to_string());
        Ok(edges.len())
    }

    /// O(1) merge by primary key. Returns VID.
    pub fn merge_by_pk(
        &mut self,
        label: &str,
        pk_key: &str,
        pk_value: &str,
        props: &[(&str, PropValue)],
    ) -> u32 {
        let key = (label.to_string(), pk_key.to_string());

        // O(1) lookup
        if let Some(vid) = self.pk_index.get(&key).and_then(|m| m.get(pk_value).copied()) {
            // Update existing vertex
            let updates = self.pending_props.entry(vid).or_default();
            for (k, v) in props {
                updates.insert(k.to_string(), v.clone());
            }
            return vid;
        }

        // New vertex
        let vid = self.next_vid.fetch_add(1, Ordering::Relaxed) as u32;
        let mut prop_map = HashMap::new();
        prop_map.insert(pk_key.to_string(), PropValue::Str(pk_value.to_string()));
        for (k, v) in props {
            prop_map.insert(k.to_string(), v.clone());
        }
        self.pending_vertices.push(PendingVertex {
            label: label.to_string(),
            props: prop_map,
        });
        self.pk_index
            .entry(key)
            .or_default()
            .insert(pk_value.to_string(), vid);

        if !self.vertex_label_set.contains(&label.to_string()) {
            self.vertex_label_set.push(label.to_string());
        }

        // Index pending vertex in vid_index (batch_index = usize::MAX as sentinel)
        self.vid_index.insert(vid, (label.to_string(), usize::MAX, self.pending_vertices.len() - 1));

        vid
    }

    /// Delete by primary key. Returns true if found.
    pub fn delete_by_pk(&mut self, label: &str, pk_key: &str, pk_value: &str) -> bool {
        let key = (label.to_string(), pk_key.to_string());
        if let Some(vid) = self.pk_index.get_mut(&key).and_then(|m| m.remove(pk_value)) {
            self.vid_index.remove(&vid);
            true
        } else {
            false
        }
    }

    /// Commit pending mutations. Flushes WAL to Arrow RecordBatch.
    pub fn commit(&mut self) {
        if self.pending_vertices.is_empty() && self.pending_props.is_empty() {
            return;
        }

        // Group pending vertices by label
        let mut by_label: HashMap<String, Vec<&PendingVertex>> = HashMap::new();
        for pv in &self.pending_vertices {
            by_label.entry(pv.label.clone()).or_default().push(pv);
        }

        // For each label, create a new RecordBatch from pending vertices
        for (label, vertices) in by_label {
            if vertices.is_empty() { continue; }

            // Collect all property keys
            let mut all_keys: Vec<String> = Vec::new();
            for v in &vertices {
                for k in v.props.keys() {
                    if !all_keys.contains(k) {
                        all_keys.push(k.clone());
                    }
                }
            }
            all_keys.sort();

            // Build Arrow arrays
            let mut builders: Vec<StringBuilder> = all_keys.iter().map(|_| StringBuilder::new()).collect();
            for v in &vertices {
                for (i, key) in all_keys.iter().enumerate() {
                    match v.props.get(key) {
                        Some(PropValue::Str(s)) => builders[i].append_value(s),
                        Some(PropValue::Int(n)) => builders[i].append_value(&n.to_string()),
                        Some(PropValue::Float(f)) => builders[i].append_value(&f.to_string()),
                        Some(PropValue::Bool(b)) => builders[i].append_value(&b.to_string()),
                        _ => builders[i].append_null(),
                    }
                }
            }

            let fields: Vec<Field> = all_keys.iter().map(|k| Field::new(k, DataType::Utf8, true)).collect();
            let schema = Arc::new(ArrowSchema::new(fields));
            let arrays: Vec<ArrayRef> = builders.into_iter().map(|mut b| Arc::new(b.finish()) as ArrayRef).collect();

            if let Ok(batch) = RecordBatch::try_new(schema, arrays) {
                self.vertex_batches.entry(label).or_default().push(batch);
            }
        }

        self.pending_vertices.clear();
        // Note: pending_props are applied lazily on read (see vertex_prop)
    }

    /// Check if a label is loaded.
    pub fn is_label_loaded(&self, label: &str) -> bool {
        self.loaded_labels.contains(label)
    }

    /// Get all loaded labels.
    pub fn loaded_labels(&self) -> &std::collections::HashSet<String> {
        &self.loaded_labels
    }

    /// Get a vertex property, checking pending updates first.
    fn get_vertex_prop(&self, vid: u32, key: &str) -> Option<PropValue> {
        // Check pending updates first
        if let Some(updates) = self.pending_props.get(vid) {
            if let Some(val) = updates.get(key) {
                return Some(val.clone());
            }
        }

        let (label, batch_idx, row_idx) = self.vid_index.get(&vid)?;

        // Sentinel: pending vertex (not yet in Arrow)
        if *batch_idx == usize::MAX {
            let pv = self.pending_vertices.get(*row_idx)?;
            return pv.props.get(key).cloned();
        }

        let batches = self.vertex_batches.get(label.as_str())?;
        let batch = batches.get(*batch_idx)?;
        let schema = batch.schema();
        let col_idx = schema.fields().iter().position(|f| f.name() == key)?;
        let col = batch.column(col_idx);

        if col.is_null(*row_idx) {
            return None;
        }

        // Try string first (most common)
        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            return Some(PropValue::Str(arr.value(*row_idx).to_string()));
        }
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            return Some(PropValue::Int(arr.value(*row_idx)));
        }
        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            return Some(PropValue::Float(arr.value(*row_idx)));
        }

        None
    }
}

// ── GRIN Trait Implementations ──

impl Topology for ArrowGraphStore {
    fn vertex_count(&self) -> usize {
        self.vid_index.len()
    }

    fn edge_count(&self) -> usize {
        self.next_eid.load(Ordering::Relaxed) as usize
    }

    fn has_vertex(&self, vid: u32) -> bool {
        self.vid_index.contains_key(&vid)
    }

    fn out_degree(&self, vid: u32) -> usize {
        self.csr.values()
            .map(|seg| {
                let v = vid as usize;
                if v + 1 < seg.out_offsets.len() {
                    (seg.out_offsets[v + 1] - seg.out_offsets[v]) as usize
                } else { 0 }
            })
            .sum()
    }

    fn in_degree(&self, vid: u32) -> usize {
        self.csr.values()
            .map(|seg| {
                let v = vid as usize;
                if v + 1 < seg.in_offsets.len() {
                    (seg.in_offsets[v + 1] - seg.in_offsets[v]) as usize
                } else { 0 }
            })
            .sum()
    }

    fn out_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        let mut result = Vec::new();
        for (label, seg) in &self.csr {
            let v = vid as usize;
            if v + 1 < seg.out_offsets.len() {
                let start = seg.out_offsets[v] as usize;
                let end = seg.out_offsets[v + 1] as usize;
                for i in start..end {
                    result.push(Neighbor {
                        vid: seg.out_targets[i],
                        edge_id: seg.out_edge_ids[i],
                        edge_label: label.clone(),
                    });
                }
            }
        }
        result
    }

    fn in_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        let mut result = Vec::new();
        for (label, seg) in &self.csr {
            let v = vid as usize;
            if v + 1 < seg.in_offsets.len() {
                let start = seg.in_offsets[v] as usize;
                let end = seg.in_offsets[v + 1] as usize;
                for i in start..end {
                    result.push(Neighbor {
                        vid: seg.in_sources[i],
                        edge_id: seg.in_edge_ids[i],
                        edge_label: label.clone(),
                    });
                }
            }
        }
        result
    }

    fn out_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        let seg = match self.csr.get(edge_label) { Some(s) => s, None => return vec![] };
        let v = vid as usize;
        if v + 1 >= seg.out_offsets.len() { return vec![]; }
        let start = seg.out_offsets[v] as usize;
        let end = seg.out_offsets[v + 1] as usize;
        (start..end).map(|i| Neighbor {
            vid: seg.out_targets[i],
            edge_id: seg.out_edge_ids[i],
            edge_label: edge_label.to_string(),
        }).collect()
    }

    fn in_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        let seg = match self.csr.get(edge_label) { Some(s) => s, None => return vec![] };
        let v = vid as usize;
        if v + 1 >= seg.in_offsets.len() { return vec![]; }
        let start = seg.in_offsets[v] as usize;
        let end = seg.in_offsets[v + 1] as usize;
        (start..end).map(|i| Neighbor {
            vid: seg.in_sources[i],
            edge_id: seg.in_edge_ids[i],
            edge_label: edge_label.to_string(),
        }).collect()
    }
}

impl Property for ArrowGraphStore {
    fn vertex_labels(&self, vid: u32) -> Vec<String> {
        self.vid_index.get(&vid)
            .map(|(label, _, _)| vec![label.clone()])
            .unwrap_or_default()
    }

    fn vertex_prop(&self, vid: u32, key: &str) -> Option<PropValue> {
        self.get_vertex_prop(vid, key)
    }

    fn edge_prop(&self, _edge_id: u32, _key: &str) -> Option<PropValue> {
        // Edge properties from Arrow batches (TODO: wire edge prop lookup)
        None
    }

    fn vertex_prop_keys(&self, label: &str) -> Vec<String> {
        self.vertex_batches.get(label)
            .and_then(|batches| batches.first())
            .map(|batch| batch.schema().fields().iter().map(|f| f.name().clone()).collect())
            .unwrap_or_default()
    }

    fn edge_prop_keys(&self, label: &str) -> Vec<String> {
        self.edge_batches.get(label)
            .and_then(|batches| batches.first())
            .map(|batch| batch.schema().fields().iter()
                .filter(|f| f.name() != "_src" && f.name() != "_dst")
                .map(|f| f.name().clone()).collect())
            .unwrap_or_default()
    }

    fn vertex_all_props(&self, vid: u32) -> HashMap<String, PropValue> {
        let mut result = HashMap::new();
        if let Some((label, batch_idx, row_idx)) = self.vid_index.get(&vid) {
            if *batch_idx == usize::MAX {
                // Pending vertex
                if let Some(pv) = self.pending_vertices.get(*row_idx) {
                    result = pv.props.clone();
                }
            } else if let Some(batches) = self.vertex_batches.get(label.as_str()) {
                if let Some(batch) = batches.get(*batch_idx) {
                    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                        let col = batch.column(col_idx);
                        if !col.is_null(*row_idx) {
                            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                                result.insert(field.name().clone(), PropValue::Str(arr.value(*row_idx).to_string()));
                            } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                                result.insert(field.name().clone(), PropValue::Int(arr.value(*row_idx)));
                            } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                                result.insert(field.name().clone(), PropValue::Float(arr.value(*row_idx)));
                            }
                        }
                    }
                }
            }
            // Apply pending updates
            if let Some(updates) = self.pending_props.get(vid) {
                for (k, v) in updates {
                    result.insert(k.clone(), v.clone());
                }
            }
        }
        result
    }
}

impl yata_grin::Schema for ArrowGraphStore {
    fn vertex_labels(&self) -> Vec<String> {
        self.vertex_label_set.clone()
    }

    fn edge_labels(&self) -> Vec<String> {
        self.edge_label_set.clone()
    }

    fn vertex_primary_key(&self, _label: &str) -> Option<String> {
        Some("rkey".to_string()) // AT Protocol convention
    }
}

// ── CSR builder from edges ──

fn build_csr_from_edges(edges: &[(u32, u32, u32)], max_vid: u32) -> CsrSegment {
    let n = max_vid as usize + 1;
    let mut out_deg = vec![0u32; n];
    let mut in_deg = vec![0u32; n];

    for &(src, dst, _) in edges {
        if (src as usize) < n { out_deg[src as usize] += 1; }
        if (dst as usize) < n { in_deg[dst as usize] += 1; }
    }

    // Build offsets
    let mut out_offsets = vec![0u32; n + 1];
    let mut in_offsets = vec![0u32; n + 1];
    for i in 0..n {
        out_offsets[i + 1] = out_offsets[i] + out_deg[i];
        in_offsets[i + 1] = in_offsets[i] + in_deg[i];
    }

    let out_total = *out_offsets.last().unwrap() as usize;
    let in_total = *in_offsets.last().unwrap() as usize;
    let mut out_targets = vec![0u32; out_total];
    let mut out_edge_ids = vec![0u32; out_total];
    let mut in_sources = vec![0u32; in_total];
    let mut in_edge_ids = vec![0u32; in_total];

    // Reset counters for fill
    let mut out_pos = out_offsets[..n].to_vec();
    let mut in_pos = in_offsets[..n].to_vec();

    for &(src, dst, eid) in edges {
        let si = src as usize;
        let di = dst as usize;
        if si < n {
            let pos = out_pos[si] as usize;
            out_targets[pos] = dst;
            out_edge_ids[pos] = eid;
            out_pos[si] += 1;
        }
        if di < n {
            let pos = in_pos[di] as usize;
            in_sources[pos] = src;
            in_edge_ids[pos] = eid;
            in_pos[di] += 1;
        }
    }

    CsrSegment { out_offsets, out_targets, in_offsets, in_sources, out_edge_ids, in_edge_ids }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow_codec;
    use crate::blocks::{VertexBlock, LabelVertexGroup};

    #[test]
    fn test_merge_and_read() {
        let mut store = ArrowGraphStore::new(PartitionId(0));

        let vid1 = store.merge_by_pk("Post", "rkey", "abc123", &[
            ("collection", PropValue::Str("app.bsky.feed.post".into())),
            ("value_b64", PropValue::Str("dGVzdA==".into())),
            ("repo", PropValue::Str("did:web:news.gftd.ai".into())),
        ]);

        let vid2 = store.merge_by_pk("Post", "rkey", "def456", &[
            ("collection", PropValue::Str("app.bsky.feed.post".into())),
            ("value_b64", PropValue::Str("dGVzdDI=".into())),
        ]);

        assert_ne!(vid1, vid2);
        assert_eq!(store.vertex_count(), 2);

        // Update existing
        let vid1_again = store.merge_by_pk("Post", "rkey", "abc123", &[
            ("quality_score", PropValue::Str("85".into())),
        ]);
        assert_eq!(vid1, vid1_again);
        assert_eq!(store.vertex_count(), 2); // no new vertex

        // Read property
        assert_eq!(
            store.vertex_prop(vid1, "repo"),
            Some(PropValue::Str("did:web:news.gftd.ai".into()))
        );
        // Read updated property
        assert_eq!(
            store.vertex_prop(vid1, "quality_score"),
            Some(PropValue::Str("85".into()))
        );
    }

    #[test]
    fn test_load_from_arrow_ipc() {
        // Encode a vertex group to Arrow IPC
        let group = LabelVertexGroup {
            label: "Article".into(),
            vertices: vec![
                VertexBlock {
                    global_vid: 0,
                    labels: vec!["Article".into()],
                    props: [
                        ("rkey".into(), PropValue::Str("r1".into())),
                        ("title".into(), PropValue::Str("Test Article".into())),
                    ].into_iter().collect(),
                },
                VertexBlock {
                    global_vid: 1,
                    labels: vec!["Article".into()],
                    props: [
                        ("rkey".into(), PropValue::Str("r2".into())),
                        ("title".into(), PropValue::Str("Another Article".into())),
                    ].into_iter().collect(),
                },
            ],
        };

        let ipc_bytes = arrow_codec::encode_vertex_group(&group).expect("encode");
        let mut store = ArrowGraphStore::new(PartitionId(0));
        let count = store.load_vertex_label("Article", &ipc_bytes).expect("load");
        assert_eq!(count, 2);

        // Verify we can read properties directly from Arrow
        assert_eq!(store.vertex_count(), 2);
        assert!(store.vertex_labels().contains(&"Article".to_string()));

        // PK lookup should work
        let vid = store.merge_by_pk("Article", "rkey", "r1", &[]);
        assert_eq!(store.vertex_prop(vid, "title"), Some(PropValue::Str("Test Article".into())));
    }

    #[test]
    fn test_delete_by_pk() {
        let mut store = ArrowGraphStore::new(PartitionId(0));
        store.merge_by_pk("Post", "rkey", "to_delete", &[
            ("value", PropValue::Str("gone".into())),
        ]);
        assert_eq!(store.vertex_count(), 1);
        assert!(store.delete_by_pk("Post", "rkey", "to_delete"));
        assert_eq!(store.vertex_count(), 0);
    }
}
