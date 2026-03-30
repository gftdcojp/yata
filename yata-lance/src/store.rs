//! LanceGraphStore — Lance Dataset wrapper for COO sorted graph persistence.
//!
//! Provides append-only write (Lance fragments) + scan (label filter) + compaction.
//! Replaces custom Arrow IPC segments + CompactionManifest with Lance built-in.

use arrow::record_batch::RecordBatch;
use arrow_array::{
    Int64Array, StringArray, UInt32Array, UInt8Array,
};
use lance::Dataset;
use std::sync::Arc;

use crate::schema::VERTICES_SCHEMA;

/// Lance-backed graph persistence store.
pub struct LanceGraphStore {
    uri: String,
    store: Arc<dyn object_store::ObjectStore>,
}

impl LanceGraphStore {
    /// Create a new LanceGraphStore pointed at the given Lance URI.
    pub fn new(uri: String, store: Arc<dyn object_store::ObjectStore>) -> Self {
        Self { uri, store }
    }

    /// Append vertex records to the Lance vertices table.
    /// Each call creates a new Lance fragment (immutable, append-only).
    pub async fn append_vertices(
        &self,
        rows: Vec<VertexRow>,
    ) -> Result<(), String> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = vertex_rows_to_batch(&rows)?;
        let params = lance::dataset::WriteParams {
            mode: lance::dataset::WriteMode::Append,
            ..Default::default()
        };
        let reader = lance::io::RecordBatchStream::new(
            vec![batch],
            VERTICES_SCHEMA.clone(),
        );
        // Try open existing, fall back to create
        match Dataset::open(&self.uri).await {
            Ok(ds) => {
                ds.append(reader, None).await.map_err(|e| e.to_string())?;
            }
            Err(_) => {
                Dataset::write(reader, &self.uri, Some(params))
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }

    /// Scan vertices by label. Returns matching rows.
    pub async fn scan_by_label(
        &self,
        label: &str,
        limit: usize,
    ) -> Result<Vec<VertexRow>, String> {
        let ds = Dataset::open(&self.uri)
            .await
            .map_err(|e| e.to_string())?;
        let mut scanner = ds.scan();
        scanner
            .filter(&format!("label = '{}'", label.replace('\'', "''")))
            .map_err(|e| e.to_string())?;
        scanner.limit(Some(limit as i64), None).map_err(|e| e.to_string())?;

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .map_err(|e| e.to_string())?
            .try_collect()
            .await
            .map_err(|e| e.to_string())?;

        let mut rows = Vec::new();
        for batch in &batches {
            rows.extend(batch_to_vertex_rows(batch)?);
        }
        Ok(rows)
    }

    /// Run Lance built-in compaction (merge small fragments + GC old versions).
    pub async fn compact(&self) -> Result<(), String> {
        let ds = Dataset::open(&self.uri)
            .await
            .map_err(|e| e.to_string())?;
        ds.compact_files(Default::default(), None)
            .await
            .map_err(|e| e.to_string())?;
        ds.cleanup_old_versions(0, None)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Delete vertices by label + rkey (Lance deletion vector).
    pub async fn delete_vertex(&self, label: &str, rkey: &str) -> Result<(), String> {
        let ds = Dataset::open(&self.uri)
            .await
            .map_err(|e| e.to_string())?;
        ds.delete(&format!(
            "label = '{}' AND rkey = '{}'",
            label.replace('\'', "''"),
            rkey.replace('\'', "''")
        ))
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Get Lance dataset version (manifest version count).
    pub async fn version(&self) -> Result<u64, String> {
        let ds = Dataset::open(&self.uri)
            .await
            .map_err(|e| e.to_string())?;
        Ok(ds.version().version)
    }
}

/// Flat vertex row for conversion to/from RecordBatch.
#[derive(Debug, Clone)]
pub struct VertexRow {
    pub label: String,
    pub rkey: String,
    pub collection: String,
    pub value_b64: String,
    pub repo: String,
    pub updated_at: i64,
    pub sensitivity_ord: u8,
    pub owner_hash: u32,
}

fn vertex_rows_to_batch(rows: &[VertexRow]) -> Result<RecordBatch, String> {
    let label: StringArray = rows.iter().map(|r| Some(r.label.as_str())).collect();
    let rkey: StringArray = rows.iter().map(|r| Some(r.rkey.as_str())).collect();
    let collection: StringArray = rows.iter().map(|r| Some(r.collection.as_str())).collect();
    let value_b64: StringArray = rows.iter().map(|r| Some(r.value_b64.as_str())).collect();
    let repo: StringArray = rows.iter().map(|r| Some(r.repo.as_str())).collect();
    let updated_at: Int64Array = rows.iter().map(|r| Some(r.updated_at)).collect();
    let sensitivity_ord: UInt8Array = rows.iter().map(|r| Some(r.sensitivity_ord)).collect();
    let owner_hash: UInt32Array = rows.iter().map(|r| Some(r.owner_hash)).collect();

    RecordBatch::try_new(
        VERTICES_SCHEMA.clone(),
        vec![
            Arc::new(label),
            Arc::new(rkey),
            Arc::new(collection),
            Arc::new(value_b64),
            Arc::new(repo),
            Arc::new(updated_at),
            Arc::new(sensitivity_ord),
            Arc::new(owner_hash),
        ],
    )
    .map_err(|e| e.to_string())
}

fn batch_to_vertex_rows(batch: &RecordBatch) -> Result<Vec<VertexRow>, String> {
    let label = batch.column(0).as_any().downcast_ref::<StringArray>().ok_or("label column")?;
    let rkey = batch.column(1).as_any().downcast_ref::<StringArray>().ok_or("rkey column")?;
    let collection = batch.column(2).as_any().downcast_ref::<StringArray>().ok_or("collection column")?;
    let value_b64 = batch.column(3).as_any().downcast_ref::<StringArray>().ok_or("value_b64 column")?;
    let repo = batch.column(4).as_any().downcast_ref::<StringArray>().ok_or("repo column")?;
    let updated_at = batch.column(5).as_any().downcast_ref::<Int64Array>().ok_or("updated_at column")?;
    let sensitivity_ord = batch.column(6).as_any().downcast_ref::<UInt8Array>().ok_or("sensitivity_ord column")?;
    let owner_hash = batch.column(7).as_any().downcast_ref::<UInt32Array>().ok_or("owner_hash column")?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        rows.push(VertexRow {
            label: label.value(i).to_string(),
            rkey: rkey.value(i).to_string(),
            collection: collection.value(i).to_string(),
            value_b64: value_b64.value(i).to_string(),
            repo: repo.value(i).to_string(),
            updated_at: updated_at.value(i),
            sensitivity_ord: sensitivity_ord.value(i),
            owner_hash: owner_hash.value(i),
        });
    }
    Ok(rows)
}
