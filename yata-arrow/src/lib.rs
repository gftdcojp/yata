#![allow(dead_code)]

//! Arrow IPC serialization helpers for YATA.
//!
//! Two IPC formats:
//! - Stream format (StreamWriter/StreamReader): framed, for in-memory/network payloads.
//! - File format  (FileWriter/FileReader):      seekable, enables mmap zero-copy reads from disk.
//!
//! Zero-copy guidance:
//! - `Bytes::from(Vec<u8>)` is O(1) — no copy (transfers Vec heap ownership).
//! - `Cursor<&[u8]>` over an existing `Bytes` borrows without copy.
//! - `read_ipc_file_mmap` maps a file into virtual address space; no kernel→user copy.
//! - `LazyArrowBatchHandle` defers IPC serialization until first call to `ipc_bytes()`.

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter, FileWriter};
use arrow::ipc::reader::{StreamReader, FileReader};
use bytes::Bytes;
use std::sync::Arc;
use once_cell::sync::OnceCell;
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

// ── Arrow IPC File format (seekable, mmap-compatible) ────────────────────────

/// Serialize a RecordBatch to Arrow IPC **File** format.
///
/// File format is seekable (uses absolute offsets), enabling zero-copy mmap reads.
/// Use this when the bytes will be persisted to disk or served via `read_ipc_file_mmap`.
pub fn batch_to_ipc_file(batch: &RecordBatch) -> Result<Bytes> {
    let mut buf = Cursor::new(Vec::new());
    let options = IpcWriteOptions::default();
    let mut writer = FileWriter::try_new_with_options(&mut buf, batch.schema().as_ref(), options)?;
    writer.write(batch)?;
    writer.finish()?;
    drop(writer);
    Ok(Bytes::from(buf.into_inner()))
}

/// Deserialize all RecordBatches from Arrow IPC File format bytes.
pub fn ipc_file_to_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let cursor = Cursor::new(bytes);
    let reader = FileReader::try_new(cursor, None)?;
    reader.map(|r| r.map_err(ArrowError::Arrow)).collect()
}

/// Open an Arrow IPC File on disk via mmap and return all batches.
///
/// The OS page cache backs the mapping — no kernel→user copy on read.
/// Arrow buffers are initialised from the mmap'd pages (one copy into `Buffer`).
pub fn read_ipc_file_mmap(path: &std::path::Path) -> Result<Vec<RecordBatch>> {
    let file = std::fs::File::open(path)?;
    // Safety: we only read the mapping and do not mutate it.
    let mmap = unsafe { memmap2::Mmap::map(&file) }?;
    let cursor = Cursor::new(&mmap[..]);
    let reader = FileReader::try_new(cursor, None)?;
    reader.map(|r| r.map_err(ArrowError::Arrow)).collect()
}

// ── Lazy handle — defers IPC serialization ────────────────────────────────────

/// A RecordBatch handle that defers IPC serialization until first access.
///
/// Preferred over `ArrowBatchHandle` when the batch may be consumed by the
/// Arrow Flight `do_get` path directly (Lance scan → IPC stream), avoiding
/// an unnecessary eager serialize-then-deserialize round-trip.
pub struct LazyArrowBatchHandle {
    pub batch: RecordBatch,
    pub schema: Arc<Schema>,
    pub row_count: usize,
    ipc_bytes: OnceCell<Bytes>,
}

impl LazyArrowBatchHandle {
    pub fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        let row_count = batch.num_rows();
        Self { batch, schema, row_count, ipc_bytes: OnceCell::new() }
    }

    /// Returns IPC stream bytes, serializing on first call (subsequent calls are free).
    pub fn ipc_bytes(&self) -> Result<&Bytes> {
        if self.ipc_bytes.get().is_none() {
            let bytes = batch_to_ipc(&self.batch)?;
            let _ = self.ipc_bytes.set(bytes);
        }
        Ok(self.ipc_bytes.get().expect("just initialised"))
    }

    pub fn content_hash(&self) -> Result<Blake3Hash> {
        Ok(ipc_hash(self.ipc_bytes()?))
    }

    pub fn as_payload_ref(&self) -> Result<PayloadRef> {
        Ok(PayloadRef::ArrowIpc(self.ipc_bytes()?.clone()))
    }
}

impl std::fmt::Debug for LazyArrowBatchHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyArrowBatchHandle")
            .field("row_count", &self.row_count)
            .field("serialized", &self.ipc_bytes.get().is_some())
            .finish()
    }
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
