#![allow(dead_code)]

//! Arrow C Data bridge and IPC serialization helpers for YATA.
//!
//! Transport fast-path uses Arrow IPC bytes.
//! C Data / C Device interfaces are Phase 2 (stubs provided).

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::reader::StreamReader;
use bytes::Bytes;
use std::sync::Arc;
use std::io::Cursor;
use yata_core::{Blake3Hash, ObjectMeta, PayloadRef, YataError};

pub type Result<T> = std::result::Result<T, ArrowError>;

#[derive(thiserror::Error, Debug)]
pub enum ArrowError {
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("empty batch")]
    EmptyBatch,
}

impl From<ArrowError> for YataError {
    fn from(e: ArrowError) -> Self {
        YataError::Arrow(e.to_string())
    }
}

/// Handle to an Arrow RecordBatch, with pre-computed content hash.
#[derive(Clone, Debug)]
pub struct ArrowBatchHandle {
    pub batch: RecordBatch,
    pub schema: Arc<Schema>,
    pub ipc_bytes: Bytes,
    pub content_hash: Blake3Hash,
    pub row_count: usize,
}

impl ArrowBatchHandle {
    /// Create from a RecordBatch. Serializes to IPC immediately.
    pub fn from_batch(batch: RecordBatch) -> Result<Self> {
        let schema = batch.schema();
        let row_count = batch.num_rows();
        let ipc_bytes = batch_to_ipc(&batch)?;
        let content_hash = ipc_hash(&ipc_bytes);
        Ok(Self {
            batch,
            schema,
            ipc_bytes,
            content_hash,
            row_count,
        })
    }

    /// Create from existing IPC bytes (e.g., received from network).
    pub fn from_ipc(ipc_bytes: Bytes) -> Result<Self> {
        let batch = ipc_to_batch(&ipc_bytes)?;
        let schema = batch.schema();
        let row_count = batch.num_rows();
        let content_hash = ipc_hash(&ipc_bytes);
        Ok(Self {
            batch,
            schema,
            ipc_bytes,
            content_hash,
            row_count,
        })
    }

    /// Convert to a PayloadRef::ArrowIpc.
    pub fn as_payload_ref(&self) -> PayloadRef {
        PayloadRef::ArrowIpc(self.ipc_bytes.clone())
    }
}

/// Serialize a RecordBatch to Arrow IPC stream format.
pub fn batch_to_ipc(batch: &RecordBatch) -> Result<Bytes> {
    let mut buf = Vec::new();
    let options = IpcWriteOptions::default();
    let mut writer = StreamWriter::try_new_with_options(&mut buf, batch.schema().as_ref(), options)?;
    writer.write(batch)?;
    writer.finish()?;
    drop(writer);
    Ok(Bytes::from(buf))
}

/// Deserialize Arrow IPC stream bytes to a RecordBatch.
pub fn ipc_to_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)?;
    match reader.next() {
        Some(Ok(batch)) => Ok(batch),
        Some(Err(e)) => Err(ArrowError::Arrow(e)),
        None => Err(ArrowError::EmptyBatch),
    }
}

/// Compute Blake3 hash of IPC bytes.
pub fn ipc_hash(bytes: &[u8]) -> Blake3Hash {
    Blake3Hash::of(bytes)
}

/// Schema registry — maps SchemaId to Arc<Schema>.
/// In Phase 1, this is an in-process HashMap. Phase 2: external registry.
pub struct SchemaRegistry {
    inner: std::collections::HashMap<String, Arc<Schema>>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            inner: std::collections::HashMap::new(),
        }
    }

    pub fn register(&mut self, id: &str, schema: Arc<Schema>) {
        self.inner.insert(id.to_owned(), schema);
    }

    pub fn get(&self, id: &str) -> Option<&Arc<Schema>> {
        self.inner.get(id)
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}
