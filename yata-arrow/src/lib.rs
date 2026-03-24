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
use arrow::ipc::reader::{FileReader, StreamReader};
use arrow::ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use once_cell::sync::OnceCell;
use std::io::Cursor;
use std::sync::Arc;
use yata_core::{Blake3Hash, PayloadRef, YataError};

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
    let mut writer =
        StreamWriter::try_new_with_options(&mut buf, batch.schema().as_ref(), options)?;
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
/// Arrow Flight `do_get` path directly (scan → IPC stream), avoiding
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
        Self {
            batch,
            schema,
            row_count,
            ipc_bytes: OnceCell::new(),
        }
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

/// Schema compatibility mode for evolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaCompatibility {
    /// New schema can only add nullable fields (readers of old data work).
    Backward,
    /// New schema can only remove fields (writers of old data work).
    Forward,
    /// Both backward and forward compatible.
    Full,
    /// No compatibility check.
    None,
}

/// Versioned schema entry.
#[derive(Clone, Debug)]
pub struct SchemaEntry {
    pub schema: Arc<Schema>,
    pub version: u32,
}

/// Schema registry — maps SchemaId to versioned schemas.
///
/// Supports compatibility checking on registration.
/// Built-in schemas are pre-registered at broker startup.
pub struct SchemaRegistry {
    inner: std::collections::HashMap<String, SchemaEntry>,
    compatibility: SchemaCompatibility,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            inner: std::collections::HashMap::new(),
            compatibility: SchemaCompatibility::Backward,
        }
    }

    pub fn with_compatibility(mut self, compat: SchemaCompatibility) -> Self {
        self.compatibility = compat;
        self
    }

    /// Register a schema. Returns Err if incompatible with the existing version.
    pub fn register(&mut self, id: &str, schema: Arc<Schema>) -> std::result::Result<u32, String> {
        if let Some(existing) = self.inner.get(id) {
            self.check_compatibility(&existing.schema, &schema)?;
            let new_version = existing.version + 1;
            self.inner.insert(
                id.to_owned(),
                SchemaEntry {
                    schema,
                    version: new_version,
                },
            );
            Ok(new_version)
        } else {
            self.inner
                .insert(id.to_owned(), SchemaEntry { schema, version: 1 });
            Ok(1)
        }
    }

    pub fn get(&self, id: &str) -> Option<&Arc<Schema>> {
        self.inner.get(id).map(|e| &e.schema)
    }

    pub fn version(&self, id: &str) -> Option<u32> {
        self.inner.get(id).map(|e| e.version)
    }

    pub fn ids(&self) -> Vec<&str> {
        self.inner.keys().map(|s| s.as_str()).collect()
    }

    /// Check if a RecordBatch is compatible with a registered schema.
    pub fn validate_batch(
        &self,
        schema_id: &str,
        batch: &RecordBatch,
    ) -> std::result::Result<(), String> {
        let registered = self
            .get(schema_id)
            .ok_or_else(|| format!("schema not registered: {schema_id}"))?;

        for field in registered.fields() {
            if batch.schema().field_with_name(field.name()).is_err() && !field.is_nullable() {
                return Err(format!("batch missing required field: {}", field.name()));
            }
        }
        Ok(())
    }

    fn check_compatibility(&self, old: &Schema, new: &Schema) -> std::result::Result<(), String> {
        match self.compatibility {
            SchemaCompatibility::None => Ok(()),
            SchemaCompatibility::Backward => {
                // New schema can add nullable fields, cannot remove or change existing.
                for old_field in old.fields() {
                    match new.field_with_name(old_field.name()) {
                        Ok(new_field) => {
                            if new_field.data_type() != old_field.data_type() {
                                return Err(format!(
                                    "backward incompatible: field '{}' type changed from {:?} to {:?}",
                                    old_field.name(),
                                    old_field.data_type(),
                                    new_field.data_type()
                                ));
                            }
                        }
                        Err(_) => {
                            return Err(format!(
                                "backward incompatible: field '{}' removed",
                                old_field.name()
                            ));
                        }
                    }
                }
                // New fields must be nullable
                for new_field in new.fields() {
                    if old.field_with_name(new_field.name()).is_err() && !new_field.is_nullable() {
                        return Err(format!(
                            "backward incompatible: new field '{}' must be nullable",
                            new_field.name()
                        ));
                    }
                }
                Ok(())
            }
            SchemaCompatibility::Forward => {
                // New schema can remove fields, cannot add required.
                for new_field in new.fields() {
                    if let Ok(old_field) = old.field_with_name(new_field.name()) {
                        if new_field.data_type() != old_field.data_type() {
                            return Err(format!(
                                "forward incompatible: field '{}' type changed",
                                old_field.name()
                            ));
                        }
                    }
                }
                Ok(())
            }
            SchemaCompatibility::Full => {
                // Both backward + forward: no removals, new fields must be nullable, no type changes.
                self.check_compatibility_inner(old, new, "full")
            }
        }
    }

    fn check_compatibility_inner(
        &self,
        old: &Schema,
        new: &Schema,
        label: &str,
    ) -> std::result::Result<(), String> {
        for old_field in old.fields() {
            match new.field_with_name(old_field.name()) {
                Ok(new_field) => {
                    if new_field.data_type() != old_field.data_type() {
                        return Err(format!(
                            "{label} incompatible: field '{}' type changed",
                            old_field.name()
                        ));
                    }
                }
                Err(_) => {
                    return Err(format!(
                        "{label} incompatible: field '{}' removed",
                        old_field.name()
                    ));
                }
            }
        }
        for new_field in new.fields() {
            if old.field_with_name(new_field.name()).is_err() && !new_field.is_nullable() {
                return Err(format!(
                    "{label} incompatible: new field '{}' must be nullable",
                    new_field.name()
                ));
            }
        }
        Ok(())
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                Arc::new(Int64Array::from(vec![30, 25])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_ipc_stream_roundtrip() {
        let batch = sample_batch();
        let ipc = batch_to_ipc(&batch).unwrap();
        let decoded = ipc_to_batch(&ipc).unwrap();
        assert_eq!(decoded.num_rows(), 2);
        assert_eq!(decoded.num_columns(), 2);
    }

    #[test]
    fn test_ipc_file_roundtrip() {
        let batch = sample_batch();
        let ipc = batch_to_ipc_file(&batch).unwrap();
        let batches = ipc_file_to_batches(&ipc).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_ipc_hash_deterministic() {
        let batch = sample_batch();
        let ipc1 = batch_to_ipc(&batch).unwrap();
        let ipc2 = batch_to_ipc(&batch).unwrap();
        assert_eq!(ipc_hash(&ipc1), ipc_hash(&ipc2));
    }

    #[test]
    fn test_arrow_batch_handle() {
        let batch = sample_batch();
        let handle = ArrowBatchHandle::from_batch(batch).unwrap();
        assert_eq!(handle.row_count, 2);
        assert_eq!(handle.content_hash.hex().len(), 64);
    }

    #[test]
    fn test_arrow_batch_handle_from_ipc() {
        let batch = sample_batch();
        let ipc = batch_to_ipc(&batch).unwrap();
        let handle = ArrowBatchHandle::from_ipc(ipc.clone()).unwrap();
        assert_eq!(handle.row_count, 2);
        assert_eq!(handle.ipc_bytes, ipc);
    }

    #[test]
    fn test_lazy_handle() {
        let batch = sample_batch();
        let lazy = LazyArrowBatchHandle::new(batch.clone());
        assert_eq!(lazy.row_count, 2);
        let ipc = lazy.ipc_bytes().unwrap();
        let decoded = ipc_to_batch(ipc).unwrap();
        assert_eq!(decoded.num_rows(), 2);
    }

    #[test]
    fn test_ipc_to_batch_empty() {
        let result = ipc_to_batch(&[]);
        assert!(result.is_err());
    }

    // ── UTF-8 tests ──────────────────────────────────────────────────────

    fn japanese_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["田中太郎", "佐藤花子", "鈴木一郎"])),
                Arc::new(StringArray::from(vec!["東京都", "大阪府", "北海道"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_ipc_roundtrip_japanese_strings() {
        let batch = japanese_batch();
        let ipc = batch_to_ipc(&batch).unwrap();
        let decoded = ipc_to_batch(&ipc).unwrap();
        assert_eq!(decoded.num_rows(), 3);
        let names = decoded
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "田中太郎");
        assert_eq!(names.value(1), "佐藤花子");
        assert_eq!(names.value(2), "鈴木一郎");
        let cities = decoded
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(cities.value(0), "東京都");
        assert_eq!(cities.value(1), "大阪府");
        assert_eq!(cities.value(2), "北海道");
    }

    #[test]
    fn test_ipc_roundtrip_emoji_mixed_cjk() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "content",
            DataType::Utf8,
            false,
        )]));
        let strings = vec![
            "Hello 世界 🌍",
            "こんにちは 🎌 Rust",
            "漢字かなカナ ASCII 混在 🚀✨",
            "🇯🇵 日本語テスト 🏯",
            "café résumé naïve",
            "中文简体 繁體中文 한국어",
        ];
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(strings.clone()))])
                .unwrap();
        let ipc = batch_to_ipc(&batch).unwrap();
        let decoded = ipc_to_batch(&ipc).unwrap();
        assert_eq!(decoded.num_rows(), strings.len());
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for (i, expected) in strings.iter().enumerate() {
            assert_eq!(col.value(i), *expected, "mismatch at row {i}");
        }
    }

    #[test]
    fn test_ipc_hash_determinism_non_ascii() {
        let batch = japanese_batch();
        let ipc1 = batch_to_ipc(&batch).unwrap();
        let ipc2 = batch_to_ipc(&batch).unwrap();
        let h1 = ipc_hash(&ipc1);
        let h2 = ipc_hash(&ipc2);
        assert_eq!(h1, h2, "hash must be deterministic for non-ASCII data");
        assert_eq!(h1.hex().len(), 64);

        // Emoji batch hash determinism
        let schema = Arc::new(Schema::new(vec![Field::new(
            "emoji",
            DataType::Utf8,
            false,
        )]));
        let emoji_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["🔥🚀✨", "日本語🎌"]))],
        )
        .unwrap();
        let e1 = batch_to_ipc(&emoji_batch).unwrap();
        let e2 = batch_to_ipc(&emoji_batch).unwrap();
        assert_eq!(ipc_hash(&e1), ipc_hash(&e2));

        // Different content must produce different hashes
        assert_ne!(h1, ipc_hash(&e1));
    }

    #[test]
    fn test_ipc_roundtrip_large_utf8_strings() {
        // Build strings >1KB each with mixed UTF-8
        let base_jp = "吾輩は猫である。名前はまだ無い。"; // 45 bytes UTF-8
        let base_emoji = "🌸🗾🏯🎌🍣🍱🎎🎏🎐🎑"; // 40 bytes UTF-8
        let large_jp: String = base_jp.repeat(40); // ~1800 bytes
        let large_emoji: String = base_emoji.repeat(30); // ~1200 bytes
        let large_mixed = format!(
            "{}---ASCII padding here---{}",
            "漢字".repeat(100),
            "α β γ δ ε ζ η θ ι κ ".repeat(20),
        ); // well over 1KB

        assert!(large_jp.len() > 1024, "large_jp must be >1KB");
        assert!(large_emoji.len() > 1024, "large_emoji must be >1KB");
        assert!(large_mixed.len() > 1024, "large_mixed must be >1KB");

        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                large_jp.as_str(),
                large_emoji.as_str(),
                large_mixed.as_str(),
            ]))],
        )
        .unwrap();

        // Stream format roundtrip
        let ipc = batch_to_ipc(&batch).unwrap();
        let decoded = ipc_to_batch(&ipc).unwrap();
        assert_eq!(decoded.num_rows(), 3);
        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), large_jp);
        assert_eq!(col.value(1), large_emoji);
        assert_eq!(col.value(2), large_mixed);

        // File format roundtrip
        let ipc_file = batch_to_ipc_file(&batch).unwrap();
        let file_batches = ipc_file_to_batches(&ipc_file).unwrap();
        assert_eq!(file_batches.len(), 1);
        let fcol = file_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(fcol.value(0), large_jp);
        assert_eq!(fcol.value(1), large_emoji);
        assert_eq!(fcol.value(2), large_mixed);

        // ArrowBatchHandle roundtrip with large UTF-8
        let handle = ArrowBatchHandle::from_batch(batch).unwrap();
        assert_eq!(handle.row_count, 3);
        let rt = ArrowBatchHandle::from_ipc(handle.ipc_bytes.clone()).unwrap();
        assert_eq!(rt.content_hash, handle.content_hash);
    }
}
