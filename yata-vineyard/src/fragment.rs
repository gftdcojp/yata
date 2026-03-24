//! ArrowFragment: Vineyard-compatible property graph fragment in pure Rust.
//!
//! Reference: v6d ArrowFragment<OID_T=int64, VID_T=uint64>
//!
//! Layout:
//! - Per vertex label: Arrow RecordBatch (property columns, row index = local VID)
//! - Per edge label: Arrow RecordBatch (property columns, row index = local EID)
//! - Per (vertex_label, edge_label): CSR topology
//!   - offsets: Int64Array (length = |V| + 1)
//!   - nbr_units: packed NbrUnit<u64,u64> bytes
//! - Bidirectional: both outgoing (oe) and incoming (ie) CSR

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use std::sync::Arc;

use crate::blob::{BlobStore, ObjectId, ObjectMeta};
use crate::nbr::{NbrUnit64, NBR_UNIT_64_SIZE};
use crate::schema::PropertyGraphSchema;

/// Vineyard-compatible ArrowFragment.
///
/// Stores a single partition (fid) of a property graph using Arrow columnar format.
/// CSR topology uses packed NbrUnit arrays (Vineyard binary-compatible).
#[derive(Clone)]
pub struct ArrowFragment {
    /// Fragment ID within the distributed graph.
    pub fid: u32,
    /// Total number of fragments.
    pub fnum: u32,
    /// Whether the graph is directed.
    pub directed: bool,
    /// Graph schema (vertex/edge labels + property definitions).
    pub schema: PropertyGraphSchema,

    /// Per vertex label: Arrow RecordBatch (columns = properties, rows = vertices).
    /// Index = vertex label_id from schema.
    pub vertex_tables: Vec<RecordBatch>,

    /// Per vertex label: inner vertex count (vertices owned by this fragment).
    pub ivnums: Vec<u64>,

    /// Per edge label: Arrow RecordBatch (columns = properties, rows = edges).
    /// Index = edge label_id from schema.
    pub edge_tables: Vec<RecordBatch>,

    /// Outgoing CSR offsets: oe_offsets[edge_label_id][vertex_label_id] = Int64Array.
    /// offsets[vid] = start index in nbr_units, offsets[vid+1] = end index.
    pub oe_offsets: Vec<Vec<Option<Int64Array>>>,

    /// Outgoing CSR adjacency: oe_nbrs[edge_label_id][vertex_label_id] = packed NbrUnit bytes.
    pub oe_nbrs: Vec<Vec<Option<Bytes>>>,

    /// Incoming CSR offsets (same structure as oe_offsets).
    pub ie_offsets: Vec<Vec<Option<Int64Array>>>,

    /// Incoming CSR adjacency (same structure as oe_nbrs).
    pub ie_nbrs: Vec<Vec<Option<Bytes>>>,
}

impl ArrowFragment {
    /// Create an empty fragment.
    pub fn new(fid: u32, fnum: u32, directed: bool, schema: PropertyGraphSchema) -> Self {
        let vlabel_count = schema.vertex_entries.len();
        let elabel_count = schema.edge_entries.len();

        let empty_offset_vecs = || vec![vec![None; vlabel_count]; elabel_count];
        let empty_nbr_vecs = || -> Vec<Vec<Option<Bytes>>> { vec![vec![None; vlabel_count]; elabel_count] };

        Self {
            fid,
            fnum,
            directed,
            schema,
            vertex_tables: Vec::new(),
            ivnums: vec![0; vlabel_count],
            edge_tables: Vec::new(),
            oe_offsets: empty_offset_vecs(),
            oe_nbrs: empty_nbr_vecs(),
            ie_offsets: empty_offset_vecs(),
            ie_nbrs: empty_nbr_vecs(),
        }
    }

    /// Number of vertex labels.
    pub fn vertex_label_num(&self) -> usize {
        self.schema.vertex_entries.len()
    }

    /// Number of edge labels.
    pub fn edge_label_num(&self) -> usize {
        self.schema.edge_entries.len()
    }

    /// Inner vertex count for a label.
    pub fn inner_vertex_num(&self, vlabel_id: usize) -> u64 {
        self.ivnums.get(vlabel_id).copied().unwrap_or(0)
    }

    /// Total edge count (sum all oe_nbrs lengths).
    pub fn edge_num(&self) -> u64 {
        let mut count = 0u64;
        for elabel_nbrs in &self.oe_nbrs {
            for maybe_nbrs in elabel_nbrs {
                if let Some(nbrs) = maybe_nbrs {
                    count += (nbrs.len() / NBR_UNIT_64_SIZE) as u64;
                }
            }
        }
        count
    }

    /// Get vertex property RecordBatch for a label.
    pub fn vertex_table(&self, vlabel_id: usize) -> Option<&RecordBatch> {
        self.vertex_tables.get(vlabel_id)
    }

    /// Get edge property RecordBatch for a label.
    pub fn edge_table(&self, elabel_id: usize) -> Option<&RecordBatch> {
        self.edge_tables.get(elabel_id)
    }

    /// Get outgoing neighbors of a vertex (CSR traversal).
    /// Returns slice of packed NbrUnit64.
    pub fn out_neighbors(
        &self,
        vlabel_id: usize,
        elabel_id: usize,
        vid: u64,
    ) -> Option<&[NbrUnit64]> {
        let offsets = self.oe_offsets.get(elabel_id)?.get(vlabel_id)?.as_ref()?;
        let nbrs_bytes = self.oe_nbrs.get(elabel_id)?.get(vlabel_id)?.as_ref()?;

        let start = offsets.value(vid as usize) as usize;
        let end = offsets.value(vid as usize + 1) as usize;
        if start >= end {
            return Some(&[]);
        }

        let byte_start = start * NBR_UNIT_64_SIZE;
        let byte_end = end * NBR_UNIT_64_SIZE;
        if byte_end > nbrs_bytes.len() {
            return None;
        }

        crate::nbr::bytes_to_nbr_units(&nbrs_bytes[byte_start..byte_end])
    }

    /// Get incoming neighbors of a vertex (CSR traversal).
    pub fn in_neighbors(
        &self,
        vlabel_id: usize,
        elabel_id: usize,
        vid: u64,
    ) -> Option<&[NbrUnit64]> {
        let offsets = self.ie_offsets.get(elabel_id)?.get(vlabel_id)?.as_ref()?;
        let nbrs_bytes = self.ie_nbrs.get(elabel_id)?.get(vlabel_id)?.as_ref()?;

        let start = offsets.value(vid as usize) as usize;
        let end = offsets.value(vid as usize + 1) as usize;
        if start >= end {
            return Some(&[]);
        }

        let byte_start = start * NBR_UNIT_64_SIZE;
        let byte_end = end * NBR_UNIT_64_SIZE;
        if byte_end > nbrs_bytes.len() {
            return None;
        }

        crate::nbr::bytes_to_nbr_units(&nbrs_bytes[byte_start..byte_end])
    }

    // ── Serialization to BlobStore ──────────────────────────────────

    /// Serialize fragment to blob store + ObjectMeta.
    ///
    /// R2 key layout:
    /// ```text
    /// fragment/{fid}/meta.json         — ObjectMeta
    /// fragment/{fid}/schema.json       — PropertyGraphSchema
    /// fragment/{fid}/v/{label_id}/table.arrow   — vertex RecordBatch
    /// fragment/{fid}/e/{label_id}/table.arrow   — edge RecordBatch
    /// fragment/{fid}/e/{elabel_id}/oe_offsets/{vlabel_id}.arrow
    /// fragment/{fid}/e/{elabel_id}/oe_nbrs/{vlabel_id}.bin
    /// fragment/{fid}/e/{elabel_id}/ie_offsets/{vlabel_id}.arrow
    /// fragment/{fid}/e/{elabel_id}/ie_nbrs/{vlabel_id}.bin
    /// ```
    pub fn serialize(&self, store: &dyn BlobStore) -> ObjectMeta {
        let mut meta = ObjectMeta::new(
            ObjectId::new(self.fid as u64),
            "vineyard::ArrowFragment<int64,uint64>",
        );
        meta.set_field("fid", self.fid as i64);
        meta.set_field("fnum", self.fnum as i64);
        meta.set_field("directed", self.directed);
        meta.set_field(
            "vertex_label_num",
            self.schema.vertex_entries.len() as i64,
        );
        meta.set_field("edge_label_num", self.schema.edge_entries.len() as i64);

        // Schema
        let schema_json = serde_json::to_vec(&self.schema).unwrap_or_default();
        store.put("schema", Bytes::from(schema_json));
        meta.add_blob("schema");

        // Vertex tables
        for (i, batch) in self.vertex_tables.iter().enumerate() {
            let name = format!("vertex_table_{}", i);
            let ipc_bytes = yata_arrow::batch_to_ipc(batch).unwrap_or_default();
            store.put(&name, ipc_bytes);
            meta.add_blob(&name);
        }

        // ivnums
        let ivnums_bytes = self
            .ivnums
            .iter()
            .flat_map(|n| n.to_le_bytes())
            .collect::<Vec<u8>>();
        store.put("ivnums", Bytes::from(ivnums_bytes));
        meta.add_blob("ivnums");

        // Edge tables
        for (i, batch) in self.edge_tables.iter().enumerate() {
            let name = format!("edge_table_{}", i);
            let ipc_bytes = yata_arrow::batch_to_ipc(batch).unwrap_or_default();
            store.put(&name, ipc_bytes);
            meta.add_blob(&name);
        }

        // CSR topology
        for (elabel_id, vlabel_offsets) in self.oe_offsets.iter().enumerate() {
            for (vlabel_id, maybe_offsets) in vlabel_offsets.iter().enumerate() {
                if let Some(offsets) = maybe_offsets {
                    let name = format!("oe_offsets_{}_{}", elabel_id, vlabel_id);
                    let offsets_batch = offsets_to_batch(offsets);
                    let ipc = yata_arrow::batch_to_ipc(&offsets_batch).unwrap_or_default();
                    store.put(&name, ipc);
                    meta.add_blob(&name);
                }
                if let Some(nbrs) = self.oe_nbrs.get(elabel_id).and_then(|v| v.get(vlabel_id)).and_then(|n| n.as_ref()) {
                    let name = format!("oe_nbrs_{}_{}", elabel_id, vlabel_id);
                    store.put(&name, nbrs.clone());
                    meta.add_blob(&name);
                }
            }
        }

        for (elabel_id, vlabel_offsets) in self.ie_offsets.iter().enumerate() {
            for (vlabel_id, maybe_offsets) in vlabel_offsets.iter().enumerate() {
                if let Some(offsets) = maybe_offsets {
                    let name = format!("ie_offsets_{}_{}", elabel_id, vlabel_id);
                    let offsets_batch = offsets_to_batch(offsets);
                    let ipc = yata_arrow::batch_to_ipc(&offsets_batch).unwrap_or_default();
                    store.put(&name, ipc);
                    meta.add_blob(&name);
                }
                if let Some(nbrs) = self.ie_nbrs.get(elabel_id).and_then(|v| v.get(vlabel_id)).and_then(|n| n.as_ref()) {
                    let name = format!("ie_nbrs_{}_{}", elabel_id, vlabel_id);
                    store.put(&name, nbrs.clone());
                    meta.add_blob(&name);
                }
            }
        }

        meta
    }

    /// Deserialize fragment from blob store + ObjectMeta.
    pub fn deserialize(meta: &ObjectMeta, store: &dyn BlobStore) -> Result<Self, FragmentError> {
        let fid = meta
            .get_field("fid")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as u32;
        let fnum = meta
            .get_field("fnum")
            .and_then(|v| v.as_i64())
            .unwrap_or(1) as u32;
        let directed = meta
            .get_field("directed")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let vlabel_num = meta
            .get_field("vertex_label_num")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;
        let elabel_num = meta
            .get_field("edge_label_num")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;

        // Schema
        if !meta.blobs.contains_key("schema") {
            return Err(FragmentError::MissingBlob("schema".to_string()));
        }
        let schema_bytes = store
            .get("schema")
            .ok_or(FragmentError::MissingBlob("schema blob data".to_string()))?;
        let schema: PropertyGraphSchema = serde_json::from_slice(&schema_bytes)
            .map_err(|e| FragmentError::SchemaParseError(e.to_string()))?;

        // Vertex tables
        let mut vertex_tables = Vec::with_capacity(vlabel_num);
        for i in 0..vlabel_num {
            let name = format!("vertex_table_{}", i);
            if meta.blobs.contains_key(&name) {
                let ipc = store
                    .get(&name)
                    .ok_or(FragmentError::MissingBlob(name.clone()))?;
                let batch =
                    yata_arrow::ipc_to_batch(&ipc).map_err(|e| FragmentError::ArrowError(e.to_string()))?;
                vertex_tables.push(batch);
            }
        }

        // ivnums
        let mut ivnums = vec![0u64; vlabel_num];
        if let Some(data) = store.get("ivnums") {
            for (i, chunk) in data.chunks_exact(8).enumerate() {
                if i < vlabel_num {
                    ivnums[i] = u64::from_le_bytes(chunk.try_into().unwrap_or([0; 8]));
                }
            }
        }

        // Edge tables
        let mut edge_tables = Vec::with_capacity(elabel_num);
        for i in 0..elabel_num {
            let name = format!("edge_table_{}", i);
            if meta.blobs.contains_key(&name) {
                let ipc = store
                    .get(&name)
                    .ok_or(FragmentError::MissingBlob(name.clone()))?;
                let batch =
                    yata_arrow::ipc_to_batch(&ipc).map_err(|e| FragmentError::ArrowError(e.to_string()))?;
                edge_tables.push(batch);
            }
        }

        // CSR topology
        let mut oe_offsets = vec![vec![None; vlabel_num]; elabel_num];
        let mut oe_nbrs: Vec<Vec<Option<Bytes>>> = vec![vec![None; vlabel_num]; elabel_num];
        let mut ie_offsets = vec![vec![None; vlabel_num]; elabel_num];
        let mut ie_nbrs: Vec<Vec<Option<Bytes>>> = vec![vec![None; vlabel_num]; elabel_num];

        for elabel_id in 0..elabel_num {
            for vlabel_id in 0..vlabel_num {
                let name = format!("oe_offsets_{}_{}", elabel_id, vlabel_id);
                if meta.blobs.contains_key(&name) {
                    if let Some(ipc) = store.get(&name) {
                        if let Ok(batch) = yata_arrow::ipc_to_batch(&ipc) {
                            if let Some(col) = batch.column_by_name("offsets") {
                                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                                    oe_offsets[elabel_id][vlabel_id] = Some(arr.clone());
                                }
                            }
                        }
                    }
                }

                let name = format!("oe_nbrs_{}_{}", elabel_id, vlabel_id);
                if let Some(data) = store.get(&name) {
                    oe_nbrs[elabel_id][vlabel_id] = Some(data);
                }

                let name = format!("ie_offsets_{}_{}", elabel_id, vlabel_id);
                if meta.blobs.contains_key(&name) {
                    if let Some(ipc) = store.get(&name) {
                        if let Ok(batch) = yata_arrow::ipc_to_batch(&ipc) {
                            if let Some(col) = batch.column_by_name("offsets") {
                                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                                    ie_offsets[elabel_id][vlabel_id] = Some(arr.clone());
                                }
                            }
                        }
                    }
                }

                let name = format!("ie_nbrs_{}_{}", elabel_id, vlabel_id);
                if let Some(data) = store.get(&name) {
                    ie_nbrs[elabel_id][vlabel_id] = Some(data);
                }
            }
        }

        Ok(Self {
            fid,
            fnum,
            directed,
            schema,
            vertex_tables,
            ivnums,
            edge_tables,
            oe_offsets,
            oe_nbrs,
            ie_offsets,
            ie_nbrs,
        })
    }
}

/// Convert Int64Array offsets to a single-column RecordBatch for IPC serialization.
fn offsets_to_batch(offsets: &Int64Array) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "offsets",
        DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(offsets.clone()) as ArrayRef])
        .expect("offsets batch creation should not fail")
}

#[derive(thiserror::Error, Debug)]
pub enum FragmentError {
    #[error("missing blob: {0}")]
    MissingBlob(String),
    #[error("blob not found in store: {0}")]
    BlobNotFound(String),
    #[error("schema parse error: {0}")]
    SchemaParseError(String),
    #[error("arrow error: {0}")]
    ArrowError(String),
}
