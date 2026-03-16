#![allow(dead_code)]

pub use schema::*;
pub use sink::*;

// ---- schema -------------------------------------------------------------

pub mod schema {
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn ts_us_utc() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    }

    /// Schema for `yata_messages` table.
    pub fn messages_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("message_id", DataType::Utf8, false),
            Field::new("stream_id", DataType::Utf8, false),
            Field::new("subject", DataType::Utf8, false),
            Field::new("seq", DataType::UInt64, false),
            Field::new("schema_id", DataType::Utf8, true),
            Field::new("content_hash", DataType::Utf8, false),
            Field::new("payload_kind", DataType::Utf8, false),
            Field::new("payload_ref_str", DataType::Utf8, true),
            Field::new("ocel_event_type", DataType::Utf8, true),
            Field::new("ts_ns", DataType::Int64, false),
            Field::new("headers_json", DataType::Utf8, true),
        ]))
    }

    /// Schema for `yata_events` (OCEL events).
    pub fn ocel_events_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("timestamp", ts_us_utc(), false),
            Field::new("attrs_json", DataType::Utf8, true),
            Field::new("message_id", DataType::Utf8, true),
            Field::new("stream_id", DataType::Utf8, true),
            Field::new("seq", DataType::UInt64, true),
        ]))
    }

    /// Schema for `yata_objects` (OCEL objects).
    pub fn ocel_objects_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("object_id", DataType::Utf8, false),
            Field::new("object_type", DataType::Utf8, false),
            Field::new("attrs_json", DataType::Utf8, true),
            Field::new("state_hash", DataType::Utf8, true),
            Field::new("valid_from", ts_us_utc(), false),
            Field::new("valid_to", ts_us_utc(), true),
        ]))
    }

    /// Schema for `yata_event_object_edges`.
    pub fn event_object_edges_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("object_id", DataType::Utf8, false),
            Field::new("qualifier", DataType::Utf8, false),
            Field::new("role", DataType::Utf8, true),
        ]))
    }

    /// Schema for `yata_object_object_edges`.
    pub fn object_object_edges_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("src_object_id", DataType::Utf8, false),
            Field::new("dst_object_id", DataType::Utf8, false),
            Field::new("rel_type", DataType::Utf8, false),
            Field::new("qualifier", DataType::Utf8, true),
            Field::new("valid_from", ts_us_utc(), false),
            Field::new("valid_to", ts_us_utc(), true),
        ]))
    }

    /// Schema for `yata_kv_history`.
    pub fn kv_history_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("bucket", DataType::Utf8, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("revision", DataType::UInt64, false),
            Field::new("value_bytes", DataType::LargeBinary, true),
            Field::new("ts_ns", DataType::Int64, false),
            Field::new("op", DataType::Utf8, false),
            Field::new("ttl_expires_at_ns", DataType::Int64, true),
        ]))
    }

    /// Schema for `yata_blobs` (object manifest index).
    pub fn blobs_schema() -> Arc<Schema> {
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
}

// ---- sink ---------------------------------------------------------------

pub mod sink {
    use super::schema::*;
    use arrow::record_batch::{RecordBatch, RecordBatchIterator};
    use arrow_array::{
        Array, BinaryArray, Int64Array, LargeBinaryArray, StringArray, TimestampMicrosecondArray,
        UInt32Array, UInt64Array,
    };
    use arrow_schema::Schema;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use yata_core::{KvEntry, KvOp, Result, YataError};
    use yata_ocel::{OcelEvent, OcelEventObjectEdge, OcelObject, OcelObjectObjectEdge};

    #[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
    pub struct SyncStats {
        pub messages_written: u64,
        pub events_written: u64,
        pub objects_written: u64,
        pub edges_written: u64,
        pub bytes_written: u64,
        pub last_sync_ns: i64,
    }

    #[async_trait]
    pub trait LanceSink: Send + Sync + 'static {
        async fn write_messages(&self, batches: Vec<RecordBatch>) -> Result<usize>;
        async fn write_ocel_events(&self, events: &[OcelEvent]) -> Result<usize>;
        async fn write_ocel_objects(&self, objects: &[OcelObject]) -> Result<usize>;
        async fn write_e2o_edges(&self, edges: &[OcelEventObjectEdge]) -> Result<usize>;
        async fn write_o2o_edges(&self, edges: &[OcelObjectObjectEdge]) -> Result<usize>;
        async fn write_kv_history(&self, entries: &[KvEntry]) -> Result<usize>;
        async fn sync_stats(&self) -> Result<SyncStats>;
    }

    pub struct LocalLanceSink {
        conn: lancedb::Connection,
        stats: RwLock<SyncStats>,
    }

    impl LocalLanceSink {
        /// Access the underlying lancedb Connection (for SQL queries / upserts).
        pub fn connection(&self) -> &lancedb::Connection {
            &self.conn
        }

        pub async fn new(base_uri: impl Into<String>) -> Result<Self> {
            let base_uri = base_uri.into();
            // Ensure base directory exists for local paths
            if !base_uri.starts_with("s3://") && !base_uri.starts_with("gs://") {
                tokio::fs::create_dir_all(&base_uri)
                    .await
                    .map_err(YataError::Io)?;
            }
            let conn = lancedb::connect(&base_uri)
                .execute()
                .await
                .map_err(|e| YataError::Storage(e.to_string()))?;
            Ok(Self {
                conn,
                stats: RwLock::new(SyncStats::default()),
            })
        }

        async fn write_batches(
            &self,
            table: &str,
            schema: Arc<Schema>,
            batches: Vec<RecordBatch>,
        ) -> Result<usize> {
            if batches.is_empty() {
                return Ok(0);
            }
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            if total_rows == 0 {
                return Ok(0);
            }
            let reader = Box::new(RecordBatchIterator::new(
                batches.into_iter().map(Ok::<_, arrow::error::ArrowError>),
                schema,
            ));
            match self.conn.open_table(table).execute().await {
                Ok(tbl) => {
                    tbl.add(reader)
                        .execute()
                        .await
                        .map_err(|e| YataError::Storage(e.to_string()))?;
                }
                Err(_) => {
                    self.conn
                        .create_table(table, reader)
                        .execute()
                        .await
                        .map_err(|e| YataError::Storage(e.to_string()))?;
                }
            }
            Ok(total_rows)
        }
    }

    #[async_trait]
    impl LanceSink for LocalLanceSink {
        async fn write_messages(&self, batches: Vec<RecordBatch>) -> Result<usize> {
            let n = self
                .write_batches("yata_messages", messages_schema(), batches)
                .await?;
            let mut stats = self.stats.write().await;
            stats.messages_written += n as u64;
            Ok(n)
        }

        async fn write_ocel_events(&self, events: &[OcelEvent]) -> Result<usize> {
            if events.is_empty() {
                return Ok(0);
            }
            let batch = ocel_events_to_batch(events)?;
            let n = self
                .write_batches("yata_events", ocel_events_schema(), vec![batch])
                .await?;
            let mut stats = self.stats.write().await;
            stats.events_written += n as u64;
            Ok(n)
        }

        async fn write_ocel_objects(&self, objects: &[OcelObject]) -> Result<usize> {
            if objects.is_empty() {
                return Ok(0);
            }
            let batch = ocel_objects_to_batch(objects)?;
            let n = self
                .write_batches("yata_objects", ocel_objects_schema(), vec![batch])
                .await?;
            let mut stats = self.stats.write().await;
            stats.objects_written += n as u64;
            Ok(n)
        }

        async fn write_e2o_edges(&self, edges: &[OcelEventObjectEdge]) -> Result<usize> {
            if edges.is_empty() {
                return Ok(0);
            }
            let batch = e2o_edges_to_batch(edges)?;
            let n = self
                .write_batches(
                    "yata_event_object_edges",
                    event_object_edges_schema(),
                    vec![batch],
                )
                .await?;
            let mut stats = self.stats.write().await;
            stats.edges_written += n as u64;
            Ok(n)
        }

        async fn write_o2o_edges(&self, edges: &[OcelObjectObjectEdge]) -> Result<usize> {
            if edges.is_empty() {
                return Ok(0);
            }
            let batch = o2o_edges_to_batch(edges)?;
            let n = self
                .write_batches(
                    "yata_object_object_edges",
                    object_object_edges_schema(),
                    vec![batch],
                )
                .await?;
            let mut stats = self.stats.write().await;
            stats.edges_written += n as u64;
            Ok(n)
        }

        async fn write_kv_history(&self, entries: &[KvEntry]) -> Result<usize> {
            if entries.is_empty() {
                return Ok(0);
            }
            let batch = kv_entries_to_batch(entries)?;
            let n = self
                .write_batches("yata_kv_history", kv_history_schema(), vec![batch])
                .await?;
            Ok(n)
        }

        async fn sync_stats(&self) -> Result<SyncStats> {
            let stats = self.stats.read().await;
            Ok(stats.clone())
        }
    }

    pub fn ocel_events_to_batch(events: &[OcelEvent]) -> Result<RecordBatch> {
        let schema = ocel_events_schema();
        let event_ids: Vec<&str> = events.iter().map(|e| e.event_id.as_str()).collect();
        let event_types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
        let timestamps: Vec<i64> = events
            .iter()
            .map(|e| e.timestamp.timestamp_micros())
            .collect();
        let attrs_json: Vec<Option<String>> = events
            .iter()
            .map(|e| serde_json::to_string(&e.attrs).ok())
            .collect();
        let message_ids: Vec<Option<&str>> =
            events.iter().map(|e| e.message_id.as_deref()).collect();
        let stream_ids: Vec<Option<&str>> =
            events.iter().map(|e| e.stream_id.as_deref()).collect();
        let seqs: Vec<Option<u64>> = events.iter().map(|e| e.seq).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(event_ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(event_types)),
                Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("UTC")),
                Arc::new(StringArray::from(
                    attrs_json.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(message_ids)),
                Arc::new(StringArray::from(stream_ids)),
                Arc::new(UInt64Array::from(seqs)),
            ],
        )
        .map_err(|e| YataError::Arrow(e.to_string()))
    }

    pub fn ocel_objects_to_batch(objects: &[OcelObject]) -> Result<RecordBatch> {
        let schema = ocel_objects_schema();
        let object_ids: Vec<&str> = objects.iter().map(|o| o.object_id.as_str()).collect();
        let object_types: Vec<&str> = objects.iter().map(|o| o.object_type.as_str()).collect();
        let attrs_json: Vec<Option<String>> = objects
            .iter()
            .map(|o| serde_json::to_string(&o.attrs).ok())
            .collect();
        let state_hashes: Vec<Option<&str>> =
            objects.iter().map(|o| o.state_hash.as_deref()).collect();
        let valid_froms: Vec<i64> = objects
            .iter()
            .map(|o| o.valid_from.timestamp_micros())
            .collect();
        let valid_tos: Vec<Option<i64>> = objects
            .iter()
            .map(|o| o.valid_to.map(|t| t.timestamp_micros()))
            .collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(object_ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(object_types)),
                Arc::new(StringArray::from(
                    attrs_json.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(state_hashes)),
                Arc::new(TimestampMicrosecondArray::from(valid_froms).with_timezone("UTC")),
                Arc::new(TimestampMicrosecondArray::from(valid_tos).with_timezone("UTC")),
            ],
        )
        .map_err(|e| YataError::Arrow(e.to_string()))
    }

    pub fn e2o_edges_to_batch(edges: &[OcelEventObjectEdge]) -> Result<RecordBatch> {
        let schema = event_object_edges_schema();
        let event_ids: Vec<&str> = edges.iter().map(|e| e.event_id.as_str()).collect();
        let object_ids: Vec<&str> = edges.iter().map(|e| e.object_id.as_str()).collect();
        let qualifiers: Vec<&str> = edges.iter().map(|e| e.qualifier.as_str()).collect();
        let roles: Vec<Option<&str>> = edges.iter().map(|e| e.role.as_deref()).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(event_ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(object_ids)),
                Arc::new(StringArray::from(qualifiers)),
                Arc::new(StringArray::from(roles)),
            ],
        )
        .map_err(|e| YataError::Arrow(e.to_string()))
    }

    pub fn o2o_edges_to_batch(edges: &[OcelObjectObjectEdge]) -> Result<RecordBatch> {
        let schema = object_object_edges_schema();
        let src_ids: Vec<&str> = edges.iter().map(|e| e.src_object_id.as_str()).collect();
        let dst_ids: Vec<&str> = edges.iter().map(|e| e.dst_object_id.as_str()).collect();
        let rel_types: Vec<&str> = edges.iter().map(|e| e.rel_type.as_str()).collect();
        let qualifiers: Vec<Option<&str>> =
            edges.iter().map(|e| e.qualifier.as_deref()).collect();
        let valid_froms: Vec<i64> = edges
            .iter()
            .map(|e| e.valid_from.timestamp_micros())
            .collect();
        let valid_tos: Vec<Option<i64>> = edges
            .iter()
            .map(|e| e.valid_to.map(|t| t.timestamp_micros()))
            .collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(src_ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(dst_ids)),
                Arc::new(StringArray::from(rel_types)),
                Arc::new(StringArray::from(qualifiers)),
                Arc::new(TimestampMicrosecondArray::from(valid_froms).with_timezone("UTC")),
                Arc::new(TimestampMicrosecondArray::from(valid_tos).with_timezone("UTC")),
            ],
        )
        .map_err(|e| YataError::Arrow(e.to_string()))
    }

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
        let ttl_expires: Vec<Option<i64>> = entries
            .iter()
            .map(|e| e.ttl_expires_at_ns)
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
                Arc::new(Int64Array::from(ttl_expires)),
            ],
        )
        .map_err(|e| YataError::Arrow(e.to_string()))
    }
}
