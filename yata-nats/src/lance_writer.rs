//! NATS → Lance bridge (consumer side).
//!
//! Subscribes to `yata.arrow.>` JetStream subjects and writes decoded
//! Arrow RecordBatches to LanceDB tables using the `Yata-Lance-Table` header.
//!
//! ## Batch Accumulation (CRITICAL)
//!
//! 1-row/batch is prohibited — Arrow IPC schema overhead ~1.9KB/batch means
//! 1-row writes achieve only 2% efficiency. This consumer accumulates batches
//! per table and flushes when either threshold is met:
//!   - Row count >= 4096
//!   - Time since last flush >= 1 second
//!
//! Table handles are cached to avoid repeated `open_table()` calls.

use crate::arrow_payload;
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{Duration, Instant};
use yata_core::YataError;

/// NATS JetStream stream name for Arrow payloads.
const ARROW_STREAM: &str = "YATA_ARROW";
/// Subject filter for all Arrow payloads.
const ARROW_SUBJECT_FILTER: &str = "yata.arrow.>";
/// Flush when accumulated rows per table exceed this.
const BATCH_ROW_THRESHOLD: usize = 4096;
/// Flush interval for tables that haven't reached row threshold.
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// Per-table accumulation buffer.
struct TableBuffer {
    batches: Vec<RecordBatch>,
    row_count: usize,
    last_flush: Instant,
}

impl TableBuffer {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            row_count: 0,
            last_flush: Instant::now(),
        }
    }

    fn push(&mut self, batch: RecordBatch) {
        self.row_count += batch.num_rows();
        self.batches.push(batch);
    }

    fn should_flush(&self) -> bool {
        self.row_count >= BATCH_ROW_THRESHOLD || self.last_flush.elapsed() >= FLUSH_INTERVAL
    }

    fn drain(&mut self) -> Vec<RecordBatch> {
        self.row_count = 0;
        self.last_flush = Instant::now();
        std::mem::take(&mut self.batches)
    }
}

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
    ///
    /// Batches are accumulated per table and flushed at 4096 rows or 1s intervals.
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

        let mut buffers: HashMap<String, TableBuffer> = HashMap::new();
        let mut table_cache: HashMap<String, lancedb::Table> = HashMap::new();
        let mut pending_acks: HashMap<String, Vec<async_nats::jetstream::Message>> = HashMap::new();
        let mut flush_timer = tokio::time::interval(FLUSH_INTERVAL);

        loop {
            tokio::select! {
                msg_opt = messages.next() => {
                    let msg = match msg_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            tracing::warn!("NatsLanceWriter: message error: {e}");
                            continue;
                        }
                        None => break,
                    };

                    let table_name = msg
                        .headers
                        .as_ref()
                        .and_then(arrow_payload::lance_table_from_headers)
                        .unwrap_or_else(|| {
                            let subj = msg.subject.as_str();
                            subj.strip_prefix("yata.arrow.")
                                .unwrap_or("unknown")
                                .to_owned()
                        });

                    match arrow_payload::decode_arrow_payload(&msg.payload) {
                        Ok(batch) => {
                            let buf = buffers
                                .entry(table_name.clone())
                                .or_insert_with(TableBuffer::new);
                            buf.push(batch);

                            pending_acks
                                .entry(table_name.clone())
                                .or_default()
                                .push(msg);

                            if buf.should_flush() {
                                Self::flush_table(
                                    &self.lance_conn,
                                    &mut table_cache,
                                    &mut buffers,
                                    &mut pending_acks,
                                    &table_name,
                                ).await;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                table = %table_name,
                                "NatsLanceWriter: arrow decode error: {e}"
                            );
                            let _ = msg.ack().await;
                        }
                    }
                }

                _ = flush_timer.tick() => {
                    let tables: Vec<String> = buffers.keys().cloned().collect();
                    for table_name in tables {
                        let should = buffers
                            .get(&table_name)
                            .map(|b| !b.batches.is_empty() && b.last_flush.elapsed() >= FLUSH_INTERVAL)
                            .unwrap_or(false);
                        if should {
                            Self::flush_table(
                                &self.lance_conn,
                                &mut table_cache,
                                &mut buffers,
                                &mut pending_acks,
                                &table_name,
                            ).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Flush accumulated batches for a single table to Lance, then ack messages.
    async fn flush_table(
        conn: &lancedb::Connection,
        table_cache: &mut HashMap<String, lancedb::Table>,
        buffers: &mut HashMap<String, TableBuffer>,
        pending_acks: &mut HashMap<String, Vec<async_nats::jetstream::Message>>,
        table_name: &str,
    ) {
        let batches = match buffers.get_mut(table_name) {
            Some(buf) if !buf.batches.is_empty() => buf.drain(),
            _ => return,
        };

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return;
        }

        let schema = batches[0].schema();
        let merged = match concat_batches(&schema, &batches) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(
                    table = %table_name,
                    "NatsLanceWriter: concat_batches error: {e}"
                );
                return;
            }
        };

        if let Err(e) = Self::write_merged(conn, table_cache, table_name, merged).await {
            tracing::warn!(
                table = %table_name,
                rows = total_rows,
                "NatsLanceWriter: lance write error: {e}"
            );
            return;
        }

        tracing::debug!(
            table = %table_name,
            rows = total_rows,
            "NatsLanceWriter: flushed"
        );

        if let Some(acks) = pending_acks.remove(table_name) {
            for msg in acks {
                let _ = msg.ack().await;
            }
        }
    }

    /// Write a merged RecordBatch to Lance, using table handle cache.
    async fn write_merged(
        conn: &lancedb::Connection,
        table_cache: &mut HashMap<String, lancedb::Table>,
        table_name: &str,
        batch: RecordBatch,
    ) -> Result<(), YataError> {
        let schema = batch.schema();
        let reader = arrow::record_batch::RecordBatchIterator::new(
            std::iter::once(Ok::<_, arrow::error::ArrowError>(batch)),
            schema,
        );

        if let Some(tbl) = table_cache.get(table_name) {
            match tbl.add(reader).execute().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::debug!(
                        table = %table_name,
                        "cached table handle stale, re-opening: {e}"
                    );
                    table_cache.remove(table_name);
                }
            }
        }

        match conn.open_table(table_name).execute().await {
            Ok(tbl) => {
                tbl.add(reader)
                    .execute()
                    .await
                    .map_err(|e| YataError::Storage(format!("lance add: {e}")))?;
                table_cache.insert(table_name.to_owned(), tbl);
            }
            Err(_) => {
                let tbl = conn
                    .create_table(table_name, reader)
                    .execute()
                    .await
                    .map_err(|e| YataError::Storage(format!("lance create_table: {e}")))?;
                table_cache.insert(table_name.to_owned(), tbl);
            }
        }

        Ok(())
    }
}
