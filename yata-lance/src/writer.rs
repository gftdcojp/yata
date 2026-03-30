use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, StringArray, UInt32Array, UInt64Array};
use arrow::record_batch::RecordBatch;

use crate::catalog::GraphCatalog;
use crate::manifest::{GraphManifest, ManifestStore};
use crate::schema::{
    edge_live_in_schema,
    edge_live_out_schema,
    edge_log_schema,
    vertex_live_schema,
    vertex_log_schema,
};
use crate::OpenedGraphDatasets;

#[derive(Debug, Clone)]
pub struct VertexMutation {
    pub partition_id: u32,
    pub seq: u64,
    pub tx_id: String,
    pub op: String,
    pub label: String,
    pub pk_key: String,
    pub pk_value: String,
    pub vid: String,
    pub repo: Option<String>,
    pub rkey: Option<String>,
    pub owner_did: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub tombstone: bool,
    pub props_json: Option<String>,
    pub props_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EdgeMutation {
    pub partition_id: u32,
    pub seq: u64,
    pub tx_id: String,
    pub op: String,
    pub edge_label: String,
    pub pk_key: String,
    pub pk_value: String,
    pub eid: String,
    pub src_vid: String,
    pub dst_vid: String,
    pub src_label: Option<String>,
    pub dst_label: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub tombstone: bool,
    pub props_json: Option<String>,
    pub props_hash: Option<String>,
}

fn vertex_log_batch(m: &VertexMutation) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(vertex_log_schema().clone()),
        vec![
            Arc::new(UInt32Array::from(vec![m.partition_id])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![m.seq])),
            Arc::new(StringArray::from(vec![m.tx_id.clone()])),
            Arc::new(StringArray::from(vec![m.op.clone()])),
            Arc::new(StringArray::from(vec![m.label.clone()])),
            Arc::new(StringArray::from(vec![m.pk_key.clone()])),
            Arc::new(StringArray::from(vec![m.pk_value.clone()])),
            Arc::new(StringArray::from(vec![m.vid.clone()])),
            Arc::new(StringArray::from(vec![m.repo.clone()])),
            Arc::new(StringArray::from(vec![m.rkey.clone()])),
            Arc::new(StringArray::from(vec![m.owner_did.clone()])),
            Arc::new(UInt64Array::from(vec![m.created_at_ms])),
            Arc::new(UInt64Array::from(vec![m.updated_at_ms])),
            Arc::new(BooleanArray::from(vec![m.tombstone])),
            Arc::new(StringArray::from(vec![m.props_json.clone()])),
            Arc::new(StringArray::from(vec![m.props_hash.clone()])),
        ],
    )
    .expect("vertex log batch")
}

fn vertex_live_batch(m: &VertexMutation) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(vertex_live_schema().clone()),
        vec![
            Arc::new(UInt32Array::from(vec![m.partition_id])) as ArrayRef,
            Arc::new(StringArray::from(vec![m.label.clone()])),
            Arc::new(StringArray::from(vec![m.pk_key.clone()])),
            Arc::new(StringArray::from(vec![m.pk_value.clone()])),
            Arc::new(StringArray::from(vec![m.vid.clone()])),
            Arc::new(BooleanArray::from(vec![!m.tombstone])),
            Arc::new(UInt64Array::from(vec![m.seq])),
            Arc::new(StringArray::from(vec![m.repo.clone()])),
            Arc::new(StringArray::from(vec![m.rkey.clone()])),
            Arc::new(StringArray::from(vec![m.owner_did.clone()])),
            Arc::new(UInt64Array::from(vec![m.updated_at_ms])),
            Arc::new(StringArray::from(vec![m.props_json.clone()])),
        ],
    )
    .expect("vertex live batch")
}

fn edge_log_batch(m: &EdgeMutation) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(edge_log_schema().clone()),
        vec![
            Arc::new(UInt32Array::from(vec![m.partition_id])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![m.seq])),
            Arc::new(StringArray::from(vec![m.tx_id.clone()])),
            Arc::new(StringArray::from(vec![m.op.clone()])),
            Arc::new(StringArray::from(vec![m.edge_label.clone()])),
            Arc::new(StringArray::from(vec![m.pk_key.clone()])),
            Arc::new(StringArray::from(vec![m.pk_value.clone()])),
            Arc::new(StringArray::from(vec![m.eid.clone()])),
            Arc::new(StringArray::from(vec![m.src_vid.clone()])),
            Arc::new(StringArray::from(vec![m.dst_vid.clone()])),
            Arc::new(StringArray::from(vec![m.src_label.clone()])),
            Arc::new(StringArray::from(vec![m.dst_label.clone()])),
            Arc::new(UInt64Array::from(vec![m.created_at_ms])),
            Arc::new(UInt64Array::from(vec![m.updated_at_ms])),
            Arc::new(BooleanArray::from(vec![m.tombstone])),
            Arc::new(StringArray::from(vec![m.props_json.clone()])),
            Arc::new(StringArray::from(vec![m.props_hash.clone()])),
        ],
    )
    .expect("edge log batch")
}

fn edge_live_out_batch(m: &EdgeMutation) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(edge_live_out_schema().clone()),
        vec![
            Arc::new(UInt32Array::from(vec![m.partition_id])) as ArrayRef,
            Arc::new(StringArray::from(vec![m.edge_label.clone()])),
            Arc::new(StringArray::from(vec![m.pk_key.clone()])),
            Arc::new(StringArray::from(vec![m.pk_value.clone()])),
            Arc::new(StringArray::from(vec![m.eid.clone()])),
            Arc::new(StringArray::from(vec![m.src_vid.clone()])),
            Arc::new(StringArray::from(vec![m.dst_vid.clone()])),
            Arc::new(StringArray::from(vec![m.src_label.clone()])),
            Arc::new(StringArray::from(vec![m.dst_label.clone()])),
            Arc::new(BooleanArray::from(vec![!m.tombstone])),
            Arc::new(UInt64Array::from(vec![m.seq])),
            Arc::new(UInt64Array::from(vec![m.updated_at_ms])),
            Arc::new(StringArray::from(vec![m.props_json.clone()])),
        ],
    )
    .expect("edge live out batch")
}

fn edge_live_in_batch(m: &EdgeMutation) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(edge_live_in_schema().clone()),
        vec![
            Arc::new(UInt32Array::from(vec![m.partition_id])) as ArrayRef,
            Arc::new(StringArray::from(vec![m.edge_label.clone()])),
            Arc::new(StringArray::from(vec![m.pk_key.clone()])),
            Arc::new(StringArray::from(vec![m.pk_value.clone()])),
            Arc::new(StringArray::from(vec![m.eid.clone()])),
            Arc::new(StringArray::from(vec![m.dst_vid.clone()])),
            Arc::new(StringArray::from(vec![m.src_vid.clone()])),
            Arc::new(StringArray::from(vec![m.src_label.clone()])),
            Arc::new(StringArray::from(vec![m.dst_label.clone()])),
            Arc::new(BooleanArray::from(vec![!m.tombstone])),
            Arc::new(UInt64Array::from(vec![m.seq])),
            Arc::new(UInt64Array::from(vec![m.updated_at_ms])),
            Arc::new(StringArray::from(vec![m.props_json.clone()])),
        ],
    )
    .expect("edge live in batch")
}

pub async fn apply_vertex_mutation<S: ManifestStore>(
    datasets: &mut OpenedGraphDatasets,
    store: &S,
    current_manifest: &GraphManifest,
    mutation: &VertexMutation,
) -> Result<GraphManifest, String> {
    datasets
        .vertex_log
        .append(vec![vertex_log_batch(mutation)], Arc::new(vertex_log_schema().clone()))
        .await
        .map_err(|e| e.to_string())?;
    datasets
        .vertex_live
        .append(vec![vertex_live_batch(mutation)], Arc::new(vertex_live_schema().clone()))
        .await
        .map_err(|e| e.to_string())?;

    let mut next = GraphCatalog::create_manifest(
        current_manifest.partition_id,
        current_manifest.version + 1,
        current_manifest.seq.min.min(mutation.seq),
        current_manifest.seq.max.max(mutation.seq),
        current_manifest.tables.vertex_log.uri.rsplit_once('/').map(|(p, _)| p).unwrap_or(""),
    );
    next.dirty_labels = vec![mutation.label.clone()];
    next.generated_at_ms = Some(mutation.updated_at_ms);
    GraphCatalog::publish(store, &next)?;
    Ok(next)
}

pub async fn apply_edge_mutation<S: ManifestStore>(
    datasets: &mut OpenedGraphDatasets,
    store: &S,
    current_manifest: &GraphManifest,
    mutation: &EdgeMutation,
) -> Result<GraphManifest, String> {
    datasets
        .edge_log
        .append(vec![edge_log_batch(mutation)], Arc::new(edge_log_schema().clone()))
        .await
        .map_err(|e| e.to_string())?;
    datasets
        .edge_live_out
        .append(vec![edge_live_out_batch(mutation)], Arc::new(edge_live_out_schema().clone()))
        .await
        .map_err(|e| e.to_string())?;
    datasets
        .edge_live_in
        .append(vec![edge_live_in_batch(mutation)], Arc::new(edge_live_in_schema().clone()))
        .await
        .map_err(|e| e.to_string())?;

    let mut next = GraphCatalog::create_manifest(
        current_manifest.partition_id,
        current_manifest.version + 1,
        current_manifest.seq.min.min(mutation.seq),
        current_manifest.seq.max.max(mutation.seq),
        current_manifest.tables.edge_log.uri.rsplit_once('/').map(|(p, _)| p).unwrap_or(""),
    );
    next.dirty_labels = vec![mutation.edge_label.clone()];
    next.generated_at_ms = Some(mutation.updated_at_ms);
    GraphCatalog::publish(store, &next)?;
    Ok(next)
}
