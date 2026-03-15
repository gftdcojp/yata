//! NATS → Lance bridge (consumer side).
//!
//! Subscribes to `yata.arrow.>` JetStream subjects and writes decoded
//! Arrow RecordBatches to LanceDB tables using the `Yata-Lance-Table` header.
//!
//! ## Flow
//!
//! ```text
//! Producer (flush_lance / graph write / KV / Object)
//!   → Arrow IPC batch → NATS subject yata.arrow.<table>
//!       ↓
//! NatsLanceWriter (this module)
//!   → subscribe yata.arrow.>
//!   → decode_arrow_payload()
//!   → lance_table_from_headers() → table name
//!   → LanceDB table.add(batch)
//! ```

use crate::arrow_payload;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use yata_core::YataError;

/// NATS JetStream stream name for Arrow payloads.
const ARROW_STREAM: &str = "YATA_ARROW";
/// Subject filter for all Arrow payloads.
const ARROW_SUBJECT_FILTER: &str = "yata.arrow.>";

/// Consumer that bridges NATS Arrow payloads to LanceDB.
pub struct NatsLanceWriter {
    js: async_nats::jetstream::Context,
    lance_conn: lancedb::Connection,
}

impl NatsLanceWriter {
    pub fn new(js: async_nats::jetstream::Context, lance_conn: lancedb::Connection) -> Self {
        Self { js, lance_conn }
    }

    /// Ensure the JetStream stream exists for Arrow payloads.
    async fn ensure_stream(&self) -> Result<async_nats::jetstream::stream::Stream, YataError> {
        let config = async_nats::jetstream::stream::Config {
            name: ARROW_STREAM.into(),
            subjects: vec![ARROW_SUBJECT_FILTER.into()],
            retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
            storage: async_nats::jetstream::stream::StorageType::File,
            ..Default::default()
        };
        self.js
            .get_or_create_stream(config)
            .await
            .map_err(|e| YataError::Storage(format!("arrow stream ensure: {e}")))
    }

    /// Run the consumer loop. Blocks forever, writing incoming Arrow batches to Lance.
    pub async fn run(&self) -> Result<(), YataError> {
        let stream = self.ensure_stream().await?;

        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("yata-lance-writer".into()),
                filter_subject: ARROW_SUBJECT_FILTER.into(),
                ..Default::default()
            })
            .await
            .map_err(|e| YataError::Storage(format!("consumer create: {e}")))?;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| YataError::Storage(format!("consumer messages: {e}")))?;

        tracing::info!("NatsLanceWriter: consuming from {}", ARROW_SUBJECT_FILTER);

        while let Some(msg_result) = messages.next().await {
            let msg = match msg_result {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("NatsLanceWriter: message error: {e}");
                    continue;
                }
            };

            let table_name = msg
                .headers
                .as_ref()
                .and_then(arrow_payload::lance_table_from_headers);

            let table_name = match table_name {
                Some(t) => t,
                None => {
                    // Derive table name from subject: yata.arrow.<table>
                    let subj = msg.subject.as_str();
                    subj.strip_prefix("yata.arrow.")
                        .unwrap_or("unknown")
                        .to_owned()
                }
            };

            match arrow_payload::decode_arrow_payload(&msg.payload) {
                Ok(batch) => {
                    if let Err(e) = self.write_batch(&table_name, batch).await {
                        tracing::warn!(
                            table = %table_name,
                            "NatsLanceWriter: lance write error: {e}"
                        );
                        // Don't ack — message will be redelivered
                        continue;
                    }
                    // Ack on success
                    if let Err(e) = msg.ack().await {
                        tracing::warn!("NatsLanceWriter: ack error: {e}");
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        table = %table_name,
                        "NatsLanceWriter: arrow decode error: {e}"
                    );
                    // Ack to discard malformed messages
                    let _ = msg.ack().await;
                }
            }
        }

        Ok(())
    }

    /// Write a single Arrow RecordBatch to a Lance table.
    async fn write_batch(&self, table: &str, batch: RecordBatch) -> Result<(), YataError> {
        let schema = batch.schema();
        let reader = arrow::record_batch::RecordBatchIterator::new(
            std::iter::once(Ok::<_, arrow::error::ArrowError>(batch)),
            schema,
        );

        match self.lance_conn.open_table(table).execute().await {
            Ok(tbl) => {
                tbl.add(reader)
                    .execute()
                    .await
                    .map_err(|e| YataError::Storage(format!("lance add: {e}")))?;
            }
            Err(_) => {
                self.lance_conn
                    .create_table(table, reader)
                    .execute()
                    .await
                    .map_err(|e| YataError::Storage(format!("lance create_table: {e}")))?;
            }
        }

        Ok(())
    }
}
