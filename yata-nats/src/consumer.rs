//! Consumer group API for NATS JetStream.
//!
//! Wraps NATS pull consumers with durable names, providing a stream of
//! decoded Arrow RecordBatches with explicit ack handles.

use crate::arrow_payload;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use std::pin::Pin;
use yata_core::{ConsumerConfig, ConsumerInfo, YataError};

const ARROW_STREAM: &str = "YATA_ARROW";

/// Handle for acknowledging a consumed message.
pub struct AckHandle {
    inner: async_nats::jetstream::Message,
}

impl AckHandle {
    pub async fn ack(self) -> Result<(), YataError> {
        self.inner
            .ack()
            .await
            .map_err(|e| YataError::Storage(format!("ack: {e}")))
    }

    pub async fn nak(self) -> Result<(), YataError> {
        self.inner
            .ack_with(async_nats::jetstream::AckKind::Nak(None))
            .await
            .map_err(|e| YataError::Storage(format!("nak: {e}")))
    }
}

/// A consumed Arrow batch with its table name and ack handle.
pub struct ConsumedBatch {
    pub table: String,
    pub batch: RecordBatch,
    pub ack: AckHandle,
}

/// Consumer group manager backed by NATS JetStream.
pub struct NatsConsumerGroup {
    js: async_nats::jetstream::Context,
}

impl NatsConsumerGroup {
    pub fn new(js: async_nats::jetstream::Context) -> Self {
        Self { js }
    }

    /// Create a durable pull consumer and return a stream of decoded Arrow batches.
    pub async fn subscribe(
        &self,
        config: ConsumerConfig,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Result<ConsumedBatch, YataError>> + Send>>, YataError>
    {
        let stream = self
            .js
            .get_stream(ARROW_STREAM)
            .await
            .map_err(|e| YataError::Storage(format!("get stream: {e}")))?;

        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(config.group_name.clone()),
                filter_subject: config.filter_subject.clone(),
                ack_wait: std::time::Duration::from_secs(config.ack_wait_secs),
                max_batch: config.max_batch as i64,
                ..Default::default()
            })
            .await
            .map_err(|e| YataError::Storage(format!("create consumer: {e}")))?;

        let messages = consumer
            .messages()
            .await
            .map_err(|e| YataError::Storage(format!("consumer messages: {e}")))?;

        let stream = messages.filter_map(move |msg_result| async move {
            let msg = match msg_result {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("consumer group message error: {e}");
                    return None;
                }
            };

            let table = msg
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
                Ok(batch) => Some(Ok(ConsumedBatch {
                    table,
                    batch,
                    ack: AckHandle { inner: msg },
                })),
                Err(e) => {
                    tracing::warn!(table = %table, "consumer group decode error: {e}");
                    let _ = msg.ack().await;
                    None
                }
            }
        });

        Ok(Box::pin(stream))
    }

    /// Delete a consumer group by name.
    pub async fn delete(&self, group_name: &str) -> Result<(), YataError> {
        let stream = self
            .js
            .get_stream(ARROW_STREAM)
            .await
            .map_err(|e| YataError::Storage(format!("get stream: {e}")))?;

        stream
            .delete_consumer(group_name)
            .await
            .map_err(|e| YataError::Storage(format!("delete consumer: {e}")))?;
        Ok(())
    }

    /// List all consumer groups on the Arrow stream.
    pub async fn list(&self) -> Result<Vec<ConsumerInfo>, YataError> {
        let stream = self
            .js
            .get_stream(ARROW_STREAM)
            .await
            .map_err(|e| YataError::Storage(format!("get stream: {e}")))?;

        let mut consumers = stream.consumers();
        let mut result = Vec::new();

        while let Some(info) = consumers.next().await {
            match info {
                Ok(info) => {
                    result.push(ConsumerInfo {
                        group_name: info.name.clone(),
                        filter_subject: info
                            .config
                            .filter_subject
                            .clone(),
                        num_pending: info.num_pending,
                        num_ack_pending: info.num_ack_pending as u64,
                    });
                }
                Err(e) => {
                    tracing::warn!("list consumers error: {e}");
                }
            }
        }

        Ok(result)
    }
}
