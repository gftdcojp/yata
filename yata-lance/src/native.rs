//! Native Lance v2 format read/write using lance-file crate.
//!
//! Uses `object_store::memory::InMemory` as a buffer to serialize Lance format bytes
//! that can then be uploaded to R2 via yata-s3. This decouples Lance's async object_store
//! from yata's sync S3 client.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_array::{Int64Array, StringArray, UInt32Array, UInt8Array};
use arrow_schema::Schema;
use bytes::Bytes;
use futures::StreamExt;
use lance_core::cache::LanceCache;
use lance_core::datatypes::Schema as LanceSchema;
use lance_encoding::decoder::FilterExpression;
use lance_file::reader::{FileReader, FileReaderOptions};
use lance_file::writer::{FileWriter, FileWriterOptions};
use lance_io::object_store::ObjectStore as LanceObjectStore;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use lance_io::ReadBatchParams;
use object_store::path::Path;

use crate::schema::VERTICES_SCHEMA;
use crate::table::VertexRow;

/// Serialize an arbitrary RecordBatch to Lance v2 native format bytes.
///
/// This is the general-purpose function used by the engine's compaction path.
/// The batch schema is preserved in the Lance file metadata.
pub async fn serialize_batch_lance(batch: &RecordBatch) -> Result<Bytes, String> {
    let store = LanceObjectStore::memory();
    let path = Path::from("fragment.lance");
    let lance_schema =
        LanceSchema::try_from(batch.schema().as_ref()).map_err(|e| e.to_string())?;

    FileWriter::create_file_with_batches(
        &store,
        &path,
        lance_schema,
        std::iter::once(batch.clone()),
        FileWriterOptions::default(),
    )
    .await
    .map_err(|e| e.to_string())?;

    let get_result = store.inner.get(&path).await.map_err(|e| e.to_string())?;
    get_result.bytes().await.map_err(|e| e.to_string())
}

/// Deserialize Lance v2 native format bytes back to a RecordBatch.
pub async fn deserialize_batch_lance(data: Bytes) -> Result<RecordBatch, String> {
    let store = LanceObjectStore::memory();
    let path = Path::from("fragment.lance");
    store
        .inner
        .put(&path, data.clone().into())
        .await
        .map_err(|e| e.to_string())?;

    let cache = LanceCache::no_cache();
    let scheduler = ScanScheduler::new(
        Arc::new(store),
        SchedulerConfig::new(64 * 1024 * 1024),
    );
    let file_scheduler = scheduler
        .open_file(&path, &CachedFileSize::new(data.len() as u64))
        .await
        .map_err(|e| e.to_string())?;

    let reader = FileReader::try_open(
        file_scheduler,
        None,
        Arc::default(),
        &cache,
        FileReaderOptions::default(),
    )
    .await
    .map_err(|e| e.to_string())?;

    let mut stream = reader
        .read_stream(
            ReadBatchParams::RangeFull,
            65536,
            16,
            FilterExpression::no_filter(),
        )
        .map_err(|e| e.to_string())?;

    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(result) = stream.next().await {
        batches.push(result.map_err(|e| e.to_string())?);
    }

    if batches.is_empty() {
        return Err("empty Lance file".into());
    }
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }
    // Concat multiple batches
    arrow::compute::concat_batches(&batches[0].schema(), &batches).map_err(|e| e.to_string())
}

/// Serialize vertex rows to Lance v2 native format bytes (in-memory).
///
/// Returns the raw bytes of a single Lance file that can be uploaded to R2.
pub async fn serialize_lance(rows: &[VertexRow]) -> Result<Bytes, String> {
    let batch = vertex_rows_to_batch(rows)?;
    let store = LanceObjectStore::memory();
    let path = Path::from("fragment.lance");
    let lance_schema =
        LanceSchema::try_from(VERTICES_SCHEMA.as_ref()).map_err(|e| e.to_string())?;

    FileWriter::create_file_with_batches(
        &store,
        &path,
        lance_schema,
        std::iter::once(batch),
        FileWriterOptions::default(),
    )
    .await
    .map_err(|e| e.to_string())?;

    // Extract the bytes from the in-memory store
    let get_result = store
        .inner
        .get(&path)
        .await
        .map_err(|e| e.to_string())?;
    let data = get_result.bytes().await.map_err(|e| e.to_string())?;
    Ok(data)
}

/// Deserialize Lance v2 native format bytes back to vertex rows.
pub async fn deserialize_lance(data: Bytes) -> Result<Vec<VertexRow>, String> {
    // Use lance_io's memory store, then write the data bytes into it
    let store = LanceObjectStore::memory();
    let path = Path::from("fragment.lance");
    store
        .inner
        .put(&path, data.clone().into())
        .await
        .map_err(|e| e.to_string())?;

    let cache = LanceCache::no_cache();
    let scheduler = ScanScheduler::new(
        Arc::new(store),
        SchedulerConfig::new(64 * 1024 * 1024),
    );
    let file_scheduler = scheduler
        .open_file(&path, &CachedFileSize::new(data.len() as u64))
        .await
        .map_err(|e| e.to_string())?;

    let reader = FileReader::try_open(
        file_scheduler,
        None,
        Arc::default(),
        &cache,
        FileReaderOptions::default(),
    )
    .await
    .map_err(|e| e.to_string())?;

    let mut stream = reader
        .read_stream(
            ReadBatchParams::RangeFull,
            4096,
            16,
            FilterExpression::no_filter(),
        )
        .map_err(|e| e.to_string())?;

    let mut rows = Vec::new();
    while let Some(result) = stream.next().await {
        let batch = result.map_err(|e| e.to_string())?;
        rows.extend(batch_to_vertex_rows(&batch)?);
    }
    Ok(rows)
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
    let label = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("label col")?;
    let rkey = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("rkey col")?;
    let collection = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("collection col")?;
    let value_b64 = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("value_b64 col")?;
    let repo = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("repo col")?;
    let updated_at = batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("updated_at col")?;
    let sensitivity_ord = batch
        .column(6)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or("sensitivity_ord col")?;
    let owner_hash = batch
        .column(7)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or("owner_hash col")?;

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

/// Build a RecordBatch from vertex rows (public for engine compaction).
pub fn rows_to_batch(rows: &[VertexRow]) -> Result<RecordBatch, String> {
    vertex_rows_to_batch(rows)
}

/// Convert a Schema to a LanceSchema.
pub fn to_lance_schema(schema: &Schema) -> Result<LanceSchema, String> {
    LanceSchema::try_from(schema).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_rows() -> Vec<VertexRow> {
        vec![
            VertexRow {
                label: "Post".into(),
                rkey: "abc123".into(),
                collection: "app.bsky.feed.post".into(),
                value_b64: "eyJ0ZXh0IjoiaGVsbG8ifQ==".into(),
                repo: "did:web:test.gftd.ai".into(),
                updated_at: 1711900000000,
                sensitivity_ord: 0,
                owner_hash: 12345,
            },
            VertexRow {
                label: "Like".into(),
                rkey: "def456".into(),
                collection: "app.bsky.feed.like".into(),
                value_b64: "e30=".into(),
                repo: "did:web:test.gftd.ai".into(),
                updated_at: 1711900001000,
                sensitivity_ord: 0,
                owner_hash: 12345,
            },
        ]
    }

    #[tokio::test]
    async fn test_lance_roundtrip() {
        let rows = test_rows();
        let data = serialize_lance(&rows).await.unwrap();
        assert!(data.len() > 0, "Lance file should have data");

        let decoded = deserialize_lance(data).await.unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].label, "Post");
        assert_eq!(decoded[0].rkey, "abc123");
        assert_eq!(decoded[1].label, "Like");
        assert_eq!(decoded[1].updated_at, 1711900001000);
    }

    #[tokio::test]
    async fn test_lance_vs_arrow_ipc_size() {
        let rows = test_rows();

        // Lance native
        let lance_data = serialize_lance(&rows).await.unwrap();

        // Arrow IPC (legacy)
        let arrow_data = crate::table::LanceTable::serialize_fragment(&rows).unwrap();

        println!(
            "Lance v2: {} bytes, Arrow IPC: {} bytes, ratio: {:.1}x",
            lance_data.len(),
            arrow_data.len(),
            arrow_data.len() as f64 / lance_data.len() as f64,
        );
    }

    #[tokio::test]
    async fn test_lance_empty() {
        let rows: Vec<VertexRow> = vec![];
        // Empty batch should still work
        let result = serialize_lance(&rows).await;
        // Lance may error on empty — that's acceptable
        if let Ok(data) = result {
            let decoded = deserialize_lance(data).await.unwrap();
            assert_eq!(decoded.len(), 0);
        }
    }
}
