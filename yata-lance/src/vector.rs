use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float32Array, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use indexmap::IndexMap;
use lance::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use lance::index::vector::VectorIndexParams;
use lance_index::{DatasetIndexExt, IndexType};
use lance_linalg::distance::MetricType;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use yata_cypher::{NodeRef, Value};

const VECTOR_COLUMN: &str = "embedding";
const VID_COLUMN: &str = "vid_hash";
const NODE_COLUMN: &str = "node_json";
const DISTANCE_COLUMN: &str = "_distance";

#[derive(thiserror::Error, Debug)]
pub enum VectorStoreError {
    #[error("storage error: {0}")]
    Storage(String),
}

pub type VectorStoreResult<T> = std::result::Result<T, VectorStoreError>;

pub struct YataVectorStore {
    dataset_uri: Option<String>,
    dim: RwLock<Option<usize>>,
    entries: RwLock<IndexMap<u64, StoredVector>>,
    dataset: RwLock<Option<Dataset>>,
    index_enabled: RwLock<bool>,
}

#[derive(Clone, Debug)]
struct StoredVector {
    embedding: Vec<f32>,
    node: NodeRef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedNodeRef {
    id: String,
    labels: Vec<String>,
    props: Vec<(String, PersistedValue)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum PersistedValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    List(Vec<PersistedValue>),
    Map(Vec<(String, PersistedValue)>),
}

impl YataVectorStore {
    pub async fn new(base_uri: impl Into<String>) -> Self {
        let base = base_uri.into();
        let dataset_uri = vector_dataset_uri(&base);
        let (dim, entries, dataset) = if let Some(uri) = dataset_uri.as_deref() {
            match Dataset::open(uri).await {
                Ok(ds) => match load_entries(&ds).await {
                    Ok((dim, entries)) => (dim, entries, Some(ds)),
                    Err(e) => {
                        tracing::warn!("vector dataset load failed: {e}");
                        (None, IndexMap::new(), None)
                    }
                },
                Err(_) => (None, IndexMap::new(), None),
            }
        } else {
            (None, IndexMap::new(), None)
        };
        Self {
            dataset_uri,
            dim: RwLock::new(dim),
            entries: RwLock::new(entries),
            dataset: RwLock::new(dataset),
            index_enabled: RwLock::new(false),
        }
    }

    pub async fn write_vertices_with_embeddings(
        &self,
        nodes: &[NodeRef],
        embedding_key: &str,
        dim: usize,
    ) -> VectorStoreResult<()> {
        if nodes.is_empty() {
            return Ok(());
        }

        {
            let mut stored_dim = self.dim.write().await;
            match *stored_dim {
                Some(existing) if existing != dim => {
                    return Err(VectorStoreError::Storage(format!(
                        "embedding dim mismatch: expected {existing}, got {dim}"
                    )));
                }
                None => *stored_dim = Some(dim),
                _ => {}
            }
        }

        {
            let mut entries = self.entries.write().await;
            for node in nodes {
                if let Some(val) = node.props.get(embedding_key) {
                    if let Some(vec) = extract_f32_vec(val) {
                        if vec.len() != dim {
                            continue;
                        }
                        entries.insert(
                            fxhash_vid(&node.id),
                            StoredVector {
                                embedding: vec,
                                node: node.clone(),
                            },
                        );
                    }
                }
            }
        }

        self.flush_dataset().await
    }

    pub async fn vector_search_vertices(
        &self,
        query_vector: Vec<f32>,
        limit: usize,
        label_filter: Option<&str>,
        _prop_filter: Option<&str>,
    ) -> VectorStoreResult<Vec<(NodeRef, f32)>> {
        let stored_dim = *self.dim.read().await;
        if let Some(expected) = stored_dim {
            if expected != query_vector.len() {
                return Err(VectorStoreError::Storage(format!(
                    "query dim mismatch: expected {expected}, got {}",
                    query_vector.len()
                )));
            }
        }

        let dataset = self.ensure_dataset().await?;
        let query = Float32Array::from(query_vector);
        let batches = dataset
            .scan()
            .nearest(VECTOR_COLUMN, &query, limit.max(1))
            .map_err(|e| VectorStoreError::Storage(format!("nearest: {e}")))?
            .prefilter(true)
            .refine(2)
            .try_into_stream()
            .await
            .map_err(|e| VectorStoreError::Storage(format!("nearest stream: {e}")))?;
        let batches: Vec<RecordBatch> = futures::TryStreamExt::try_collect(batches)
            .await
            .map_err(|e| VectorStoreError::Storage(format!("nearest collect: {e}")))?;

        let mut out = Vec::new();
        for batch in batches {
            let node_json = batch
                .column_by_name(NODE_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| VectorStoreError::Storage("missing node_json column".into()))?;
            let distances = batch
                .column_by_name(DISTANCE_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
                .ok_or_else(|| VectorStoreError::Storage("missing _distance column".into()))?;

            for row in 0..batch.num_rows() {
                let node = parse_node_ref(node_json.value(row))?;
                if let Some(label) = label_filter {
                    if !node.labels.iter().any(|l| l == label) {
                        continue;
                    }
                }
                out.push((node, distances.value(row)));
                if out.len() >= limit {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    pub async fn create_embedding_index(&self) -> VectorStoreResult<()> {
        {
            let mut indexed = self.index_enabled.write().await;
            *indexed = true;
        }
        self.rebuild_index().await
    }

    pub async fn vector_count(&self) -> usize {
        self.entries.read().await.len()
    }

    async fn ensure_dataset(&self) -> VectorStoreResult<Dataset> {
        if let Some(dataset) = self.dataset.read().await.clone() {
            return Ok(dataset);
        }
        self.flush_dataset().await?;
        self.dataset
            .read()
            .await
            .clone()
            .ok_or_else(|| VectorStoreError::Storage("vector dataset is not available".into()))
    }

    async fn flush_dataset(&self) -> VectorStoreResult<()> {
        let dim = *self.dim.read().await;
        let Some(dim) = dim else {
            return Ok(());
        };
        let Some(uri) = self.dataset_uri.as_deref() else {
            return Ok(());
        };
        let entries = self.entries.read().await;
        let schema = Arc::new(vector_schema(dim));
        let batch = build_batch(&entries, schema.clone(), dim)?;
        let params = WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        let ds = Dataset::write(reader, uri, Some(params))
            .await
            .map_err(|e| VectorStoreError::Storage(format!("write vector dataset: {e}")))?;
        {
            let mut dataset = self.dataset.write().await;
            *dataset = Some(ds);
        }
        if *self.index_enabled.read().await {
            self.rebuild_index().await?;
        }
        Ok(())
    }

    async fn rebuild_index(&self) -> VectorStoreResult<()> {
        let dim = *self.dim.read().await;
        let Some(dim) = dim else {
            return Ok(());
        };
        let count = self.entries.read().await.len();
        if count == 0 {
            return Ok(());
        }
        let mut dataset_guard = self.dataset.write().await;
        let Some(dataset) = dataset_guard.as_mut() else {
            return Ok(());
        };
        let params = vector_index_params(dim, count);
        dataset
            .create_index(
                &[VECTOR_COLUMN],
                IndexType::Vector,
                Some("yata_vector_index".to_string()),
                &params,
                true,
            )
            .await
            .map_err(|e| VectorStoreError::Storage(format!("create vector index: {e}")))?;
        Ok(())
    }
}

fn vector_dataset_uri(base: &str) -> Option<String> {
    if base.is_empty() || base == "." {
        return None;
    }
    if base.contains("://") {
        return Some(format!("{}/vectors.lance", base.trim_end_matches('/')));
    }
    let path = PathBuf::from(base).join("vectors.lance");
    path.to_str().map(ToOwned::to_owned)
}

fn vector_schema(dim: usize) -> Schema {
    Schema::new(vec![
        Field::new(VID_COLUMN, DataType::UInt64, false),
        Field::new(NODE_COLUMN, DataType::Utf8, false),
        Field::new(
            VECTOR_COLUMN,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            false,
        ),
    ])
}

fn build_batch(
    entries: &IndexMap<u64, StoredVector>,
    schema: Arc<Schema>,
    dim: usize,
) -> VectorStoreResult<RecordBatch> {
    let vids = UInt64Array::from(entries.keys().copied().collect::<Vec<_>>());
    let nodes = StringArray::from(
        entries
            .values()
            .map(|entry| format_node_ref(&entry.node))
            .collect::<Result<Vec<_>, _>>()?,
    );
    let flat = Float32Array::from(
        entries
            .values()
            .flat_map(|entry| entry.embedding.iter().copied())
            .collect::<Vec<_>>(),
    );
    let vectors = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        dim as i32,
        Arc::new(flat) as ArrayRef,
        None,
    )
        .map_err(|e| VectorStoreError::Storage(format!("build embedding array: {e}")))?;
    RecordBatch::try_new(
        schema,
        vec![Arc::new(vids), Arc::new(nodes), Arc::new(vectors)],
    )
    .map_err(|e| VectorStoreError::Storage(format!("build vector batch: {e}")))
}

async fn load_entries(ds: &Dataset) -> VectorStoreResult<(Option<usize>, IndexMap<u64, StoredVector>)> {
    let batches = ds
        .scan()
        .try_into_stream()
        .await
        .map_err(|e| VectorStoreError::Storage(format!("scan vector dataset: {e}")))?;
    let batches: Vec<RecordBatch> = futures::TryStreamExt::try_collect(batches)
        .await
        .map_err(|e| VectorStoreError::Storage(format!("collect vector dataset: {e}")))?;

    let mut dim = None;
    let mut entries = IndexMap::new();
    for batch in batches {
        let vids = batch
            .column_by_name(VID_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| VectorStoreError::Storage("missing vid_hash column".into()))?;
        let nodes = batch
            .column_by_name(NODE_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| VectorStoreError::Storage("missing node_json column".into()))?;
        let vectors = batch
            .column_by_name(VECTOR_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeListArray>())
            .ok_or_else(|| VectorStoreError::Storage("missing embedding column".into()))?;
        dim.get_or_insert(vectors.value_length() as usize);

        for row in 0..batch.num_rows() {
            let vector = vectors.value(row);
            let values = vector
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| VectorStoreError::Storage("embedding values not Float32".into()))?;
            entries.insert(
                vids.value(row),
                StoredVector {
                    embedding: values.values().to_vec(),
                    node: parse_node_ref(nodes.value(row))?,
                },
            );
        }
    }
    Ok((dim, entries))
}

fn vector_index_params(dim: usize, count: usize) -> VectorIndexParams {
    if count >= 256 && dim % 8 == 0 {
        let partitions = (count / 64).clamp(1, 64);
        VectorIndexParams::ivf_pq(partitions, 8, 8, MetricType::L2, 50)
    } else {
        VectorIndexParams::ivf_flat(1, MetricType::L2)
    }
}

fn fxhash_vid(vid: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in vid.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn extract_f32_vec(val: &Value) -> Option<Vec<f32>> {
    if let Value::List(items) = val {
        items
            .iter()
            .map(|v| match v {
                Value::Float(f) => Some(*f as f32),
                Value::Int(i) => Some(*i as f32),
                _ => None,
            })
            .collect()
    } else {
        None
    }
}

fn format_node_ref(node: &NodeRef) -> VectorStoreResult<String> {
    serde_json::to_string(&PersistedNodeRef::from(node))
        .map_err(|e| VectorStoreError::Storage(format!("encode node ref: {e}")))
}

fn parse_node_ref(json: &str) -> VectorStoreResult<NodeRef> {
    let node: PersistedNodeRef = serde_json::from_str(json)
        .map_err(|e| VectorStoreError::Storage(format!("decode node ref: {e}")))?;
    Ok(node.into())
}

impl From<&NodeRef> for PersistedNodeRef {
    fn from(value: &NodeRef) -> Self {
        Self {
            id: value.id.clone(),
            labels: value.labels.clone(),
            props: value
                .props
                .iter()
                .map(|(k, v)| (k.clone(), PersistedValue::from(v)))
                .collect(),
        }
    }
}

impl From<PersistedNodeRef> for NodeRef {
    fn from(value: PersistedNodeRef) -> Self {
        Self {
            id: value.id,
            labels: value.labels,
            props: value
                .props
                .into_iter()
                .map(|(k, v)| (k, Value::from(v)))
                .collect(),
        }
    }
}

impl From<&Value> for PersistedValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(v) => Self::Bool(*v),
            Value::Int(v) => Self::Int(*v),
            Value::Float(v) => Self::Float(*v),
            Value::Str(v) => Self::Str(v.clone()),
            Value::List(values) => Self::List(values.iter().map(Self::from).collect()),
            Value::Map(values) => Self::Map(
                values
                    .iter()
                    .map(|(k, v)| (k.clone(), PersistedValue::from(v)))
                    .collect(),
            ),
            Value::Node(node) => Self::Map(vec![
                ("id".into(), Self::Str(node.id.clone())),
                (
                    "labels".into(),
                    Self::List(node.labels.iter().cloned().map(Self::Str).collect()),
                ),
                (
                    "props".into(),
                    Self::Map(
                        node.props
                            .iter()
                            .map(|(k, v)| (k.clone(), PersistedValue::from(v)))
                            .collect(),
                    ),
                ),
            ]),
            Value::Rel(rel) => Self::Map(vec![
                ("id".into(), Self::Str(rel.id.clone())),
                ("rel_type".into(), Self::Str(rel.rel_type.clone())),
                ("src".into(), Self::Str(rel.src.clone())),
                ("dst".into(), Self::Str(rel.dst.clone())),
                (
                    "props".into(),
                    Self::Map(
                        rel.props
                            .iter()
                            .map(|(k, v)| (k.clone(), PersistedValue::from(v)))
                            .collect(),
                    ),
                ),
            ]),
        }
    }
}

impl From<PersistedValue> for Value {
    fn from(value: PersistedValue) -> Self {
        match value {
            PersistedValue::Null => Self::Null,
            PersistedValue::Bool(v) => Self::Bool(v),
            PersistedValue::Int(v) => Self::Int(v),
            PersistedValue::Float(v) => Self::Float(v),
            PersistedValue::Str(v) => Self::Str(v),
            PersistedValue::List(values) => Self::List(values.into_iter().map(Value::from).collect()),
            PersistedValue::Map(values) => {
                Self::Map(values.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn persists_and_reloads_lance_vector_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = YataVectorStore::new(dir.path().to_string_lossy().to_string()).await;

        let mut props = indexmap::IndexMap::new();
        props.insert("emb".into(), Value::List(vec![Value::Float(1.0), Value::Float(0.0)]));
        props.insert("name".into(), Value::Str("alpha".into()));
        let node = NodeRef {
            id: "n1".into(),
            labels: vec!["Doc".into()],
            props,
        };
        store
            .write_vertices_with_embeddings(&[node], "emb", 2)
            .await
            .unwrap();
        store.create_embedding_index().await.unwrap();

        let reloaded = YataVectorStore::new(dir.path().to_string_lossy().to_string()).await;
        let results = reloaded
            .vector_search_vertices(vec![1.0, 0.0], 1, Some("Doc"), None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.id, "n1");
    }
}
