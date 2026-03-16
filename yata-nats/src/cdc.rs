//! CDC (Change Data Capture) stream over NATS JetStream.
//!
//! Publishes CDC events to `yata.cdc.<table>` subjects as JSON.
//! Consumers can subscribe to track changes across Lance tables.

use futures::StreamExt;
use std::pin::Pin;
use yata_core::{CdcEvent, CdcOp, YataError};

const CDC_STREAM: &str = "YATA_CDC";
const CDC_SUBJECT_PREFIX: &str = "yata.cdc.";
const CDC_SUBJECT_FILTER: &str = "yata.cdc.>";

/// Publishes CDC events to NATS.
pub struct NatsCdcPublisher {
    js: async_nats::jetstream::Context,
}

impl NatsCdcPublisher {
    pub fn new(js: async_nats::jetstream::Context) -> Self {
        Self { js }
    }

    /// Ensure the CDC JetStream stream exists.
    pub async fn ensure_stream(&self) -> Result<(), YataError> {
        let config = async_nats::jetstream::stream::Config {
            name: CDC_STREAM.into(),
            subjects: vec![CDC_SUBJECT_FILTER.into()],
            retention: async_nats::jetstream::stream::RetentionPolicy::Interest,
            storage: async_nats::jetstream::stream::StorageType::File,
            ..Default::default()
        };
        self.js
            .get_or_create_stream(config)
            .await
            .map_err(|e| YataError::Storage(format!("cdc stream ensure: {e}")))?;
        Ok(())
    }

    /// Publish a CDC event for a table operation.
    pub async fn publish(&self, table: &str, op: CdcOp, row_count: usize) -> Result<(), YataError> {
        let event = CdcEvent {
            table: table.to_owned(),
            op,
            row_count,
            ts_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        };

        let subject = format!("{}{}", CDC_SUBJECT_PREFIX, table);
        let payload = serde_json::to_vec(&event)
            .map_err(|e| YataError::Serialization(e.to_string()))?;

        self.js
            .publish(subject, payload.into())
            .await
            .map_err(|e| YataError::Storage(format!("cdc publish: {e}")))?;

        Ok(())
    }
}

/// Subscribes to CDC events from NATS.
pub struct NatsCdcConsumer {
    js: async_nats::jetstream::Context,
}

impl NatsCdcConsumer {
    pub fn new(js: async_nats::jetstream::Context) -> Self {
        Self { js }
    }

    /// Subscribe to CDC events, optionally filtered to a specific table.
    pub async fn subscribe(
        &self,
        group_name: &str,
        table_filter: Option<&str>,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Result<CdcEvent, YataError>> + Send>>, YataError>
    {
        let stream = self
            .js
            .get_stream(CDC_STREAM)
            .await
            .map_err(|e| YataError::Storage(format!("get cdc stream: {e}")))?;

        let filter = match table_filter {
            Some(table) => format!("{}{}", CDC_SUBJECT_PREFIX, table),
            None => CDC_SUBJECT_FILTER.to_owned(),
        };

        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(group_name.to_owned()),
                filter_subject: filter,
                ..Default::default()
            })
            .await
            .map_err(|e| YataError::Storage(format!("cdc consumer create: {e}")))?;

        let messages = consumer
            .messages()
            .await
            .map_err(|e| YataError::Storage(format!("cdc consumer messages: {e}")))?;

        let event_stream = messages.filter_map(|msg_result| async move {
            let msg = match msg_result {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("CDC consumer message error: {e}");
                    return None;
                }
            };

            let event: CdcEvent = match serde_json::from_slice(&msg.payload) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("CDC decode error: {e}");
                    let _ = msg.ack().await;
                    return None;
                }
            };

            let _ = msg.ack().await;
            Some(Ok(event))
        });

        Ok(Box::pin(event_stream))
    }
}
