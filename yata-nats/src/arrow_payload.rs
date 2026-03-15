//! Arrow IPC payload transport over NATS.
//!
//! Publishes Arrow RecordBatches as IPC stream bytes over NATS subjects.
//! Subscribers can decode these directly into LanceDB-compatible batches.
//!
//! Subject convention: `yata.arrow.<table>` for Lance table writes,
//! `yata.arrow.<stream>.<subject>` for stream events.
//!
//! ## Wire format
//!
//! NATS message payload = Arrow IPC stream bytes (same as `yata_arrow::batch_to_ipc`).
//! Headers carry schema fingerprint + row count for zero-parse routing.

use arrow::record_batch::RecordBatch;
use yata_core::{Blake3Hash, YataError};

/// Header: Arrow schema fingerprint (blake3 of schema bytes).
const HDR_SCHEMA_HASH: &str = "Yata-Arrow-Schema";
/// Header: row count (for routing/monitoring without deserializing).
const HDR_ROW_COUNT: &str = "Yata-Arrow-Rows";
/// Header: target Lance table name (for sink routing).
const HDR_LANCE_TABLE: &str = "Yata-Lance-Table";

/// Publishes Arrow RecordBatches over NATS JetStream.
pub struct NatsArrowPublisher {
    js: async_nats::jetstream::Context,
}

impl NatsArrowPublisher {
    pub fn new(js: async_nats::jetstream::Context) -> Self {
        Self { js }
    }

    /// Publish a RecordBatch to a NATS subject as Arrow IPC bytes.
    ///
    /// The batch is directly LanceDB-ingestible by the subscriber.
    pub async fn publish_batch(
        &self,
        subject: &str,
        batch: &RecordBatch,
        lance_table: Option<&str>,
    ) -> std::result::Result<u64, YataError> {
        let ipc_bytes =
            yata_arrow::batch_to_ipc(batch).map_err(|e| YataError::Arrow(e.to_string()))?;

        let schema_hash = Blake3Hash::of(&ipc_bytes);

        let mut headers = async_nats::HeaderMap::new();
        headers.insert(HDR_SCHEMA_HASH, schema_hash.hex().as_str());
        headers.insert(HDR_ROW_COUNT, batch.num_rows().to_string().as_str());
        if let Some(table) = lance_table {
            headers.insert(HDR_LANCE_TABLE, table);
        }

        let ack = self
            .js
            .publish_with_headers(subject.to_string(), headers, ipc_bytes.clone())
            .await
            .map_err(|e| YataError::Storage(format!("arrow publish: {e}")))?
            .await
            .map_err(|e| YataError::Storage(format!("arrow publish ack: {e}")))?;

        Ok(ack.sequence)
    }

    /// Publish multiple batches as a single logical write.
    /// Each batch is a separate NATS message on the same subject.
    pub async fn publish_batches(
        &self,
        subject: &str,
        batches: &[RecordBatch],
        lance_table: Option<&str>,
    ) -> std::result::Result<Vec<u64>, YataError> {
        let mut seqs = Vec::with_capacity(batches.len());
        for batch in batches {
            let seq = self.publish_batch(subject, batch, lance_table).await?;
            seqs.push(seq);
        }
        Ok(seqs)
    }
}

/// Decode a NATS message payload as an Arrow RecordBatch.
pub fn decode_arrow_payload(payload: &[u8]) -> std::result::Result<RecordBatch, YataError> {
    yata_arrow::ipc_to_batch(payload).map_err(|e| YataError::Arrow(e.to_string()))
}

/// Extract Lance table target from NATS message headers (if present).
pub fn lance_table_from_headers(headers: &async_nats::HeaderMap) -> Option<String> {
    headers.get(HDR_LANCE_TABLE).map(|v| v.to_string())
}

/// Extract row count from headers (for zero-parse monitoring).
pub fn row_count_from_headers(headers: &async_nats::HeaderMap) -> Option<usize> {
    headers
        .get(HDR_ROW_COUNT)
        .and_then(|v| v.to_string().parse().ok())
}
