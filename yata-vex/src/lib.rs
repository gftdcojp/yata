pub mod distance;
pub mod ivf_pq;
pub mod lazy;
pub mod quantize;
pub mod storage;
pub mod vamana;

use arrow_array::RecordBatch;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum VexError {
    #[error("not enough vectors: need {need}, got {got}")]
    NotEnoughVectors { need: usize, got: usize },
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
    #[error("index not built")]
    IndexNotBuilt,
    #[error("empty index")]
    EmptyIndex,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

pub type Result<T> = std::result::Result<T, VexError>;

impl From<VexError> for yata_core::YataError {
    fn from(e: VexError) -> Self {
        yata_core::YataError::Storage(e.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DistanceMetric {
    L2,
    Cosine,
    DotProduct,
}

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub vid: u64,
    pub distance: f32,
}

pub trait VectorIndex: Send + Sync {
    fn search(&self, query: &[f32], k: usize, nprobes: usize) -> Result<Vec<SearchResult>>;
    fn search_with_filter(
        &self,
        query: &[f32],
        k: usize,
        nprobes: usize,
        filter: &dyn Fn(u64) -> bool,
    ) -> Result<Vec<SearchResult>>;
    fn dim(&self) -> usize;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn metric(&self) -> DistanceMetric;
}

pub trait VectorIndexBuilder: Send {
    fn build(self) -> Result<Box<dyn VectorIndex>>;
}

/// Threshold below which brute-force is used instead of IVF_PQ.
pub const BRUTE_FORCE_THRESHOLD: usize = 1000;

pub fn to_record_batch(
    results: &[SearchResult],
    _dim: usize,
) -> std::result::Result<RecordBatch, arrow::error::ArrowError> {
    use arrow_array::{Float32Array, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let vids = UInt64Array::from(results.iter().map(|r| r.vid).collect::<Vec<_>>());
    let distances = Float32Array::from(results.iter().map(|r| r.distance).collect::<Vec<_>>());

    let schema = Arc::new(Schema::new(vec![
        Field::new("vid", DataType::UInt64, false),
        Field::new("_distance", DataType::Float32, false),
    ]));
    RecordBatch::try_new(schema, vec![Arc::new(vids), Arc::new(distances)])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_result_to_record_batch() {
        let results = vec![
            SearchResult {
                vid: 1,
                distance: 0.1,
            },
            SearchResult {
                vid: 5,
                distance: 0.5,
            },
        ];
        let batch = to_record_batch(&results, 4).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }
}
