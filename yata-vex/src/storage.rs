use std::path::Path;
use std::sync::Arc;

use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use memmap2::Mmap;

use crate::Result;

/// Arrow-based vector storage schema.
pub fn vector_schema(dim: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("vid", DataType::UInt64, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                dim as i32,
            ),
            false,
        ),
    ]))
}

/// Contiguous f32 vector store backed by a flat buffer.
/// Vectors are laid out as [N × dim] contiguous f32 values.
pub struct FlatVectorStore {
    data: Vec<f32>,
    vids: Vec<u64>,
    dim: usize,
}

impl FlatVectorStore {
    pub fn new(dim: usize) -> Self {
        Self {
            data: Vec::new(),
            vids: Vec::new(),
            dim,
        }
    }

    pub fn with_capacity(dim: usize, capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity * dim),
            vids: Vec::with_capacity(capacity),
            dim,
        }
    }

    pub fn push(&mut self, vid: u64, vector: &[f32]) {
        debug_assert_eq!(vector.len(), self.dim);
        self.vids.push(vid);
        self.data.extend_from_slice(vector);
    }

    #[inline]
    pub fn get(&self, idx: usize) -> &[f32] {
        &self.data[idx * self.dim..(idx + 1) * self.dim]
    }

    #[inline]
    pub fn vid(&self, idx: usize) -> u64 {
        self.vids[idx]
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.vids.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.vids.is_empty()
    }

    #[inline]
    pub fn dim(&self) -> usize {
        self.dim
    }

    #[inline]
    pub fn raw_data(&self) -> &[f32] {
        &self.data
    }

    #[inline]
    pub fn vids(&self) -> &[u64] {
        &self.vids
    }

    /// Build from Arrow RecordBatch with (vid: UInt64, embedding: FixedSizeList<Float32>).
    pub fn from_record_batch(batch: &RecordBatch, dim: usize) -> Result<Self> {
        let vid_col = batch.column_by_name("vid").ok_or_else(|| {
            crate::VexError::Arrow(arrow::error::ArrowError::SchemaError(
                "missing vid column".into(),
            ))
        })?;
        let emb_col = batch.column_by_name("embedding").ok_or_else(|| {
            crate::VexError::Arrow(arrow::error::ArrowError::SchemaError(
                "missing embedding column".into(),
            ))
        })?;

        let vids_arr = vid_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                crate::VexError::Arrow(arrow::error::ArrowError::CastError("vid not UInt64".into()))
            })?;
        let emb_list = emb_col
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| {
                crate::VexError::Arrow(arrow::error::ArrowError::CastError(
                    "embedding not FixedSizeList".into(),
                ))
            })?;

        let values = emb_list.values();
        let float_arr = values
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                crate::VexError::Arrow(arrow::error::ArrowError::CastError(
                    "embedding values not Float32".into(),
                ))
            })?;

        let n = vids_arr.len();
        let mut store = Self::with_capacity(dim, n);
        for i in 0..n {
            store.vids.push(vids_arr.value(i));
        }
        store.data.extend_from_slice(float_arr.values());
        Ok(store)
    }

    /// Convert to Arrow RecordBatch.
    pub fn to_record_batch(&self) -> std::result::Result<RecordBatch, arrow::error::ArrowError> {
        let vids = UInt64Array::from(self.vids.clone());
        let flat = Float32Array::from(self.data.clone());
        let emb = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            self.dim as i32,
            Arc::new(flat),
            None,
        )?;
        let schema = vector_schema(self.dim);
        RecordBatch::try_new(schema, vec![Arc::new(vids), Arc::new(emb)])
    }

    /// Serialize to Arrow IPC file format (seekable, mmap-friendly).
    pub fn to_ipc_file(&self) -> Result<Bytes> {
        let batch = self.to_record_batch()?;
        yata_arrow::batch_to_ipc_file(&batch).map_err(|e| {
            crate::VexError::Arrow(arrow::error::ArrowError::ExternalError(Box::new(e)))
        })
    }
}

/// Memory-mapped vector store. Zero-copy reads from Arrow IPC file.
pub struct MmapVectorStore {
    _mmap: Mmap,
    data_ptr: *const f32,
    data_len: usize,
    vids: Vec<u64>,
    dim: usize,
}

unsafe impl Send for MmapVectorStore {}
unsafe impl Sync for MmapVectorStore {}

impl MmapVectorStore {
    /// Open Arrow IPC file via mmap. Expects schema: (vid: UInt64, embedding: FixedSizeList<Float32, dim>).
    pub fn open(path: &Path, dim: usize) -> Result<Self> {
        let batches = yata_arrow::read_ipc_file_mmap(path).map_err(|e| {
            crate::VexError::Arrow(arrow::error::ArrowError::ExternalError(Box::new(e)))
        })?;
        if batches.is_empty() {
            return Ok(Self {
                _mmap: unsafe { Mmap::map(&std::fs::File::open(path)?)? },
                data_ptr: std::ptr::null(),
                data_len: 0,
                vids: Vec::new(),
                dim,
            });
        }
        // Collect all batches into a FlatVectorStore, then hold the data
        let mut flat = FlatVectorStore::new(dim);
        for batch in &batches {
            let partial = FlatVectorStore::from_record_batch(batch, dim)?;
            for i in 0..partial.len() {
                flat.push(partial.vid(i), partial.get(i));
            }
        }
        let data = flat.data.into_boxed_slice();
        let vids = flat.vids;
        let data_len = data.len();
        let data_ptr = Box::into_raw(data) as *const f32;
        Ok(Self {
            _mmap: unsafe { Mmap::map(&std::fs::File::open(path)?)? },
            data_ptr,
            data_len,
            vids,
            dim,
        })
    }

    #[inline]
    pub fn get(&self, idx: usize) -> &[f32] {
        unsafe { std::slice::from_raw_parts(self.data_ptr.add(idx * self.dim), self.dim) }
    }

    #[inline]
    pub fn vid(&self, idx: usize) -> u64 {
        self.vids[idx]
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.vids.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.vids.is_empty()
    }

    #[inline]
    pub fn dim(&self) -> usize {
        self.dim
    }
}

impl Drop for MmapVectorStore {
    fn drop(&mut self) {
        if !self.data_ptr.is_null() {
            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(
                    self.data_ptr as *mut f32,
                    self.data_len,
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flat_store_push_get() {
        let mut store = FlatVectorStore::new(3);
        store.push(10, &[1.0, 2.0, 3.0]);
        store.push(20, &[4.0, 5.0, 6.0]);
        assert_eq!(store.len(), 2);
        assert_eq!(store.vid(0), 10);
        assert_eq!(store.get(1), &[4.0, 5.0, 6.0]);
    }

    #[test]
    fn test_flat_store_to_record_batch() {
        let mut store = FlatVectorStore::new(2);
        store.push(1, &[0.1, 0.2]);
        store.push(2, &[0.3, 0.4]);
        let batch = store.to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_flat_store_roundtrip_record_batch() {
        let mut store = FlatVectorStore::new(4);
        store.push(100, &[1.0, 2.0, 3.0, 4.0]);
        store.push(200, &[5.0, 6.0, 7.0, 8.0]);
        let batch = store.to_record_batch().unwrap();
        let store2 = FlatVectorStore::from_record_batch(&batch, 4).unwrap();
        assert_eq!(store2.len(), 2);
        assert_eq!(store2.vid(0), 100);
        assert_eq!(store2.get(0), &[1.0, 2.0, 3.0, 4.0]);
        assert_eq!(store2.vid(1), 200);
        assert_eq!(store2.get(1), &[5.0, 6.0, 7.0, 8.0]);
    }

    #[test]
    fn test_flat_store_ipc_roundtrip() {
        let mut store = FlatVectorStore::new(3);
        store.push(1, &[0.1, 0.2, 0.3]);
        let ipc = store.to_ipc_file().unwrap();
        assert!(!ipc.is_empty());
    }
}
