//! NATS JetStream KV implementation with Arrow IPC payload.
//!
//! Dual-write architecture:
//! 1. JetStream KV bucket — fast point reads, watch, history
//! 2. Arrow IPC batch on `yata.arrow.kv_history` — Lance-compatible streaming
//!
//! Every mutation (put/delete) publishes an Arrow RecordBatch matching
//! `yata_kv_history` schema so that a Lance sink subscriber can directly
//! ingest without conversion.

use arrow::array::{Array, Int64Array, LargeBinaryArray, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;
use yata_core::{
    BucketId, KvAck, KvEntry, KvEvent, KvOp, KvPutRequest, KvStore, Result, Revision, YataError,
};

fn time_to_nanos(t: time::OffsetDateTime) -> i64 {
    t.unix_timestamp_nanos() as i64
}

/// Arrow subject for KV history (Lance sink subscribes here).
const ARROW_KV_SUBJECT: &str = "yata.arrow.kv_history";
/// Lance table name carried in header.
const LANCE_TABLE: &str = "yata_kv_history";

pub struct NatsKvStore {
    js: async_nats::jetstream::Context,
}

impl NatsKvStore {
    pub fn new(js: async_nats::jetstream::Context) -> Self {
        Self { js }
    }

    fn bucket_name(bucket: &BucketId) -> String {
        format!("yata_kv_{}", bucket.0.replace('.', "_").replace('-', "_"))
    }

    async fn ensure_bucket(
        &self,
        bucket: &BucketId,
    ) -> std::result::Result<async_nats::jetstream::kv::Store, YataError> {
        let name = Self::bucket_name(bucket);
        let config = async_nats::jetstream::kv::Config {
            bucket: name,
            history: 64,
            ..Default::default()
        };
        self.js
            .create_key_value(config)
            .await
            .map_err(|e| YataError::Storage(format!("kv ensure_bucket: {e}")))
    }

    /// Publish KV entry as Arrow IPC batch to NATS for Lance ingestion.
    async fn publish_arrow_kv(&self, entry: &KvEntry) -> Result<()> {
        let batch = kv_entry_to_batch(entry)?;
        let ipc_bytes =
            yata_arrow::batch_to_ipc(&batch).map_err(|e| YataError::Arrow(e.to_string()))?;

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Yata-Lance-Table", LANCE_TABLE);
        headers.insert("Yata-Arrow-Rows", "1");

        // Best-effort: don't fail the KV write if Arrow publish fails
        let _ = self
            .js
            .publish_with_headers(ARROW_KV_SUBJECT.to_string(), headers, ipc_bytes)
            .await;

        Ok(())
    }
}

/// Arrow schema matching `yata_kv_history` Lance table.
fn kv_history_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("bucket", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("revision", DataType::UInt64, false),
        Field::new("value_bytes", DataType::LargeBinary, true),
        Field::new("ts_ns", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]))
}

/// Encode a single KvEntry as an Arrow RecordBatch.
fn kv_entry_to_batch(entry: &KvEntry) -> Result<RecordBatch> {
    let schema = kv_history_schema();
    let op_str = match entry.op {
        KvOp::Put => "put",
        KvOp::Delete => "delete",
        KvOp::Purge => "purge",
    };
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entry.bucket.0.as_str()])) as Arc<dyn Array>,
            Arc::new(StringArray::from(vec![entry.key.as_str()])),
            Arc::new(UInt64Array::from(vec![entry.revision.0])),
            Arc::new(LargeBinaryArray::from_opt_vec(vec![Some(
                entry.value.as_ref(),
            )])),
            Arc::new(Int64Array::from(vec![entry.ts_ns])),
            Arc::new(StringArray::from(vec![op_str])),
        ],
    )
    .map_err(|e| YataError::Arrow(e.to_string()))
}

/// Encode multiple KvEntries as an Arrow RecordBatch.
pub fn kv_entries_to_batch(entries: &[KvEntry]) -> Result<RecordBatch> {
    let schema = kv_history_schema();
    let buckets: Vec<&str> = entries.iter().map(|e| e.bucket.0.as_str()).collect();
    let keys: Vec<&str> = entries.iter().map(|e| e.key.as_str()).collect();
    let revisions: Vec<u64> = entries.iter().map(|e| e.revision.0).collect();
    let values: Vec<Option<&[u8]>> = entries.iter().map(|e| Some(e.value.as_ref())).collect();
    let ts_ns: Vec<i64> = entries.iter().map(|e| e.ts_ns).collect();
    let ops: Vec<&str> = entries
        .iter()
        .map(|e| match e.op {
            KvOp::Put => "put",
            KvOp::Delete => "delete",
            KvOp::Purge => "purge",
        })
        .collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(buckets)) as Arc<dyn Array>,
            Arc::new(StringArray::from(keys)),
            Arc::new(UInt64Array::from(revisions)),
            Arc::new(LargeBinaryArray::from_opt_vec(values)),
            Arc::new(Int64Array::from(ts_ns)),
            Arc::new(StringArray::from(ops)),
        ],
    )
    .map_err(|e| YataError::Arrow(e.to_string()))
}

#[async_trait]
impl KvStore for NatsKvStore {
    async fn put(&self, req: KvPutRequest) -> Result<KvAck> {
        let store = self.ensure_bucket(&req.bucket).await?;

        let revision = if let Some(expected) = req.expected_revision {
            store
                .update(&req.key, req.value.clone(), expected.0)
                .await
                .map_err(|e| YataError::Storage(format!("kv update (CAS): {e}")))?
        } else {
            store
                .put(&req.key, req.value.clone())
                .await
                .map_err(|e| YataError::Storage(format!("kv put: {e}")))?
        };

        let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // Dual-write: Arrow IPC for Lance ingestion
        let entry = KvEntry {
            bucket: req.bucket,
            key: req.key,
            revision: Revision(revision),
            value: req.value.to_vec(),
            ts_ns,
            op: KvOp::Put,
            ttl_expires_at_ns: None,
        };
        self.publish_arrow_kv(&entry).await?;

        Ok(KvAck {
            revision: Revision(revision),
            ts_ns,
        })
    }

    async fn get(&self, bucket: &BucketId, key: &str) -> Result<Option<KvEntry>> {
        let store = self.ensure_bucket(bucket).await?;

        match store.entry(key).await {
            Ok(Some(entry)) => {
                if matches!(
                    entry.operation,
                    async_nats::jetstream::kv::Operation::Delete
                        | async_nats::jetstream::kv::Operation::Purge
                ) {
                    return Ok(None);
                }
                Ok(Some(KvEntry {
                    bucket: bucket.clone(),
                    key: entry.key,
                    revision: Revision(entry.revision),
                    value: entry.value.to_vec(),
                    ts_ns: time_to_nanos(entry.created),
                    op: KvOp::Put,
                    ttl_expires_at_ns: None,
                }))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(YataError::Storage(format!("kv get: {e}"))),
        }
    }

    async fn delete(
        &self,
        bucket: &BucketId,
        key: &str,
        _expected_revision: Option<Revision>,
    ) -> Result<KvAck> {
        let store = self.ensure_bucket(bucket).await?;

        store
            .delete(key)
            .await
            .map_err(|e| YataError::Storage(format!("kv delete: {e}")))?;

        let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // Dual-write: Arrow IPC for Lance ingestion
        let entry = KvEntry {
            bucket: bucket.clone(),
            key: key.to_owned(),
            revision: Revision(0),
            value: vec![],
            ts_ns,
            op: KvOp::Delete,
            ttl_expires_at_ns: None,
        };
        self.publish_arrow_kv(&entry).await?;

        Ok(KvAck {
            revision: Revision(0),
            ts_ns,
        })
    }

    async fn watch(
        &self,
        bucket: &BucketId,
        prefix: &str,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = KvEvent> + Send>>> {
        let store = self.ensure_bucket(bucket).await?;
        let bucket_id = bucket.clone();

        let watcher = if prefix.is_empty() {
            store
                .watch_all()
                .await
                .map_err(|e| YataError::Storage(format!("kv watch_all: {e}")))?
        } else {
            store
                .watch(prefix)
                .await
                .map_err(|e| YataError::Storage(format!("kv watch: {e}")))?
        };

        let mapped = watcher.filter_map(move |entry_result| {
            let bucket_id = bucket_id.clone();
            async move {
                let entry = entry_result.ok()?;
                let is_delete = matches!(
                    entry.operation,
                    async_nats::jetstream::kv::Operation::Delete
                        | async_nats::jetstream::kv::Operation::Purge
                );
                let op = match entry.operation {
                    async_nats::jetstream::kv::Operation::Put => KvOp::Put,
                    async_nats::jetstream::kv::Operation::Delete => KvOp::Delete,
                    async_nats::jetstream::kv::Operation::Purge => KvOp::Purge,
                };
                Some(KvEvent {
                    entry: KvEntry {
                        bucket: bucket_id,
                        key: entry.key,
                        revision: Revision(entry.revision),
                        value: entry.value.to_vec(),
                        ts_ns: time_to_nanos(entry.created),
                        op,
                        ttl_expires_at_ns: None,
                    },
                    is_delete,
                })
            }
        });

        Ok(Box::pin(mapped))
    }

    async fn history(&self, bucket: &BucketId, key: &str) -> Result<Vec<KvEntry>> {
        let store = self.ensure_bucket(bucket).await?;

        let mut history_stream = store
            .history(key)
            .await
            .map_err(|e| YataError::Storage(format!("kv history: {e}")))?;

        let mut entries = Vec::new();
        while let Some(entry_result) = history_stream.next().await {
            let entry =
                entry_result.map_err(|e| YataError::Storage(format!("kv history entry: {e}")))?;
            let op = match entry.operation {
                async_nats::jetstream::kv::Operation::Put => KvOp::Put,
                async_nats::jetstream::kv::Operation::Delete => KvOp::Delete,
                async_nats::jetstream::kv::Operation::Purge => KvOp::Purge,
            };
            entries.push(KvEntry {
                bucket: bucket.clone(),
                key: entry.key,
                revision: Revision(entry.revision),
                value: entry.value.to_vec(),
                ts_ns: time_to_nanos(entry.created),
                op,
                ttl_expires_at_ns: None,
            });
        }

        Ok(entries)
    }
}
