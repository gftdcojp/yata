//! NATS JetStream Object Store with Arrow IPC manifest publishing.
//!
//! Dual-write architecture:
//! 1. JetStream Object Store — binary blob storage (get/put)
//! 2. Arrow IPC batch on `yata.arrow.blobs` — manifest metadata for Lance
//!
//! Every put_object publishes an Arrow RecordBatch matching `yata_blobs`
//! schema so a Lance sink subscriber can index manifests without polling.

use arrow::array::{Array, Int64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::sync::Arc;
use yata_core::{
    Blake3Hash, ChunkRef, ObjectId, ObjectManifest, ObjectMeta, ObjectStorage, Result, YataError,
};

const OBJECT_STORE_BUCKET: &str = "yata_objects";
/// Arrow subject for blob manifests (Lance sink subscribes here).
const ARROW_BLOBS_SUBJECT: &str = "yata.arrow.blobs";
/// Lance table name carried in header.
const LANCE_TABLE: &str = "yata_blobs";

pub struct NatsObjectStore {
    js: async_nats::jetstream::Context,
}

impl NatsObjectStore {
    pub fn new(js: async_nats::jetstream::Context) -> Self {
        Self { js }
    }

    async fn ensure_store(
        &self,
    ) -> std::result::Result<async_nats::jetstream::object_store::ObjectStore, YataError> {
        let config = async_nats::jetstream::object_store::Config {
            bucket: OBJECT_STORE_BUCKET.into(),
            ..Default::default()
        };
        self.js
            .create_object_store(config)
            .await
            .map_err(|e| YataError::Storage(format!("object store ensure: {e}")))
    }

    /// Publish object manifest as Arrow IPC batch for Lance ingestion.
    async fn publish_arrow_manifest(&self, manifest: &ObjectManifest) -> Result<()> {
        let batch = manifest_to_batch(manifest)?;
        let ipc_bytes =
            yata_arrow::batch_to_ipc(&batch).map_err(|e| YataError::Arrow(e.to_string()))?;

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Yata-Lance-Table", LANCE_TABLE);
        headers.insert("Yata-Arrow-Rows", "1");

        // Best-effort: don't fail the object write if Arrow publish fails
        let _ = self
            .js
            .publish_with_headers(ARROW_BLOBS_SUBJECT.to_string(), headers, ipc_bytes)
            .await;

        Ok(())
    }
}

/// Arrow schema matching `yata_blobs` Lance table.
fn blobs_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("object_id", DataType::Utf8, false),
        Field::new("content_hash", DataType::Utf8, false),
        Field::new("size_bytes", DataType::UInt64, false),
        Field::new("media_type", DataType::Utf8, false),
        Field::new("schema_id", DataType::Utf8, true),
        Field::new("chunk_count", DataType::UInt32, false),
        Field::new("created_at_ns", DataType::Int64, false),
    ]))
}

/// Encode an ObjectManifest as an Arrow RecordBatch.
fn manifest_to_batch(m: &ObjectManifest) -> Result<RecordBatch> {
    let schema = blobs_schema();
    let schema_id: Option<&str> = m.schema_id.as_ref().map(|s| s.0.as_str());
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![m.object_id.to_string().as_str()])) as Arc<dyn Array>,
            Arc::new(StringArray::from(vec![m.content_hash.hex().as_str()])),
            Arc::new(UInt64Array::from(vec![m.size_bytes])),
            Arc::new(StringArray::from(vec![m.media_type.as_str()])),
            Arc::new(StringArray::from(vec![schema_id])),
            Arc::new(UInt32Array::from(vec![m.chunks.len() as u32])),
            Arc::new(Int64Array::from(vec![m.created_at_ns])),
        ],
    )
    .map_err(|e| YataError::Arrow(e.to_string()))
}

/// Encode multiple ObjectManifests as an Arrow RecordBatch.
pub fn manifests_to_batch(manifests: &[ObjectManifest]) -> Result<RecordBatch> {
    let schema = blobs_schema();
    let ids: Vec<String> = manifests.iter().map(|m| m.object_id.to_string()).collect();
    let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
    let hashes: Vec<String> = manifests.iter().map(|m| m.content_hash.hex()).collect();
    let hash_refs: Vec<&str> = hashes.iter().map(|s| s.as_str()).collect();
    let sizes: Vec<u64> = manifests.iter().map(|m| m.size_bytes).collect();
    let media: Vec<&str> = manifests.iter().map(|m| m.media_type.as_str()).collect();
    let schemas: Vec<Option<&str>> = manifests
        .iter()
        .map(|m| m.schema_id.as_ref().map(|s| s.0.as_str()))
        .collect();
    let chunks: Vec<u32> = manifests.iter().map(|m| m.chunks.len() as u32).collect();
    let ts: Vec<i64> = manifests.iter().map(|m| m.created_at_ns).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(id_refs)) as Arc<dyn Array>,
            Arc::new(StringArray::from(hash_refs)),
            Arc::new(UInt64Array::from(sizes)),
            Arc::new(StringArray::from(media)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(UInt32Array::from(chunks)),
            Arc::new(Int64Array::from(ts)),
        ],
    )
    .map_err(|e| YataError::Arrow(e.to_string()))
}

#[async_trait]
impl ObjectStorage for NatsObjectStore {
    async fn put_object(&self, data: bytes::Bytes, meta: ObjectMeta) -> Result<ObjectManifest> {
        let store = self.ensure_store().await?;
        let object_id = ObjectId::new();
        let content_hash = Blake3Hash::of(&data);
        let size_bytes = data.len() as u64;

        // Store metadata as JSON in the object description
        let meta_json =
            serde_json::to_string(&meta).map_err(|e| YataError::Serialization(e.to_string()))?;

        let obj_meta = async_nats::jetstream::object_store::ObjectMetadata {
            name: object_id.to_string(),
            description: Some(meta_json),
            ..Default::default()
        };

        // Write binary data to NATS Object Store
        let mut reader = tokio::io::BufReader::new(std::io::Cursor::new(data.to_vec()));
        store
            .put(obj_meta, &mut reader)
            .await
            .map_err(|e| YataError::Storage(format!("object put: {e}")))?;

        let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let hash_copy = content_hash.clone();
        let manifest = ObjectManifest {
            object_id,
            content_hash,
            size_bytes,
            chunks: vec![ChunkRef {
                seq: 0,
                hash: hash_copy,
                size_bytes,
                offset: 0,
            }],
            media_type: meta.media_type,
            schema_id: meta.schema_id,
            lineage: meta.lineage,
            created_at_ns: ts_ns,
        };

        // Dual-write: publish manifest as Arrow IPC for Lance indexing
        self.publish_arrow_manifest(&manifest).await?;

        Ok(manifest)
    }

    async fn get_object(&self, id: &ObjectId) -> Result<bytes::Bytes> {
        let store = self.ensure_store().await?;

        let mut obj = store
            .get(&id.to_string())
            .await
            .map_err(|e| YataError::NotFound(format!("object get {id}: {e}")))?;

        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        obj.read_to_end(&mut buf)
            .await
            .map_err(|e| YataError::Io(e))?;

        Ok(bytes::Bytes::from(buf))
    }

    async fn head_object(&self, id: &ObjectId) -> Result<Option<ObjectManifest>> {
        let store = self.ensure_store().await?;

        match store.info(&id.to_string()).await {
            Ok(info) => {
                let meta: ObjectMeta = info
                    .description
                    .as_deref()
                    .and_then(|d| serde_json::from_str(d).ok())
                    .unwrap_or(ObjectMeta {
                        media_type: "application/octet-stream".into(),
                        schema_id: None,
                        lineage: vec![],
                    });

                Ok(Some(ObjectManifest {
                    object_id: id.clone(),
                    content_hash: Blake3Hash([0u8; 32]),
                    size_bytes: info.size as u64,
                    chunks: vec![],
                    media_type: meta.media_type,
                    schema_id: meta.schema_id,
                    lineage: meta.lineage,
                    created_at_ns: 0,
                }))
            }
            Err(_) => Ok(None),
        }
    }

    async fn pin_object(&self, _id: &ObjectId) -> Result<()> {
        Ok(())
    }

    async fn gc(&self) -> Result<u64> {
        Ok(0)
    }
}
