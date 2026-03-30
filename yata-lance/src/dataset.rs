//! YataDataset — Lance Dataset wrapper for yata graph persistence.
//!
//! Provides create/open/append/scan/compact operations on a Lance dataset
//! stored at an S3/R2/local URI. All versioning, manifest management, fragment
//! tracking, and compaction are handled by Lance internally.

use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use std::sync::Arc;

/// Wrapper around a Lance Dataset for yata graph vertices.
pub struct YataDataset {
    ds: Dataset,
}

impl YataDataset {
    /// Create a new empty dataset with the given schema.
    pub async fn create_empty(
        uri: &str,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<Self, lance::Error> {
        let empty = RecordBatch::new_empty(schema.clone());
        let reader = RecordBatchIterator::new(vec![Ok(empty)].into_iter(), schema);
        let ds = Dataset::write(reader, uri, Some(WriteParams::default())).await?;
        Ok(Self { ds })
    }

    /// Open an existing dataset at the given URI.
    ///
    /// URI can be:
    /// - Local: `/path/to/dataset`
    /// - S3/R2: `s3://bucket/prefix/dataset`
    pub async fn open(uri: &str) -> Result<Self, lance::Error> {
        let ds = Dataset::open(uri).await?;
        Ok(Self { ds })
    }

    /// Create a new dataset (or overwrite) with initial batches.
    pub async fn create(
        uri: &str,
        batches: Vec<RecordBatch>,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<Self, lance::Error> {
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        let ds = Dataset::write(reader, uri, Some(WriteParams::default())).await?;
        Ok(Self { ds })
    }

    /// Append batches to the dataset (creates a new version).
    pub async fn append(
        &mut self,
        batches: Vec<RecordBatch>,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<(), lance::Error> {
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        self.ds.append(reader, None).await
    }

    /// Overwrite the dataset with new batches (creates a new version).
    pub async fn overwrite(
        &mut self,
        batches: Vec<RecordBatch>,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<(), lance::Error> {
        let params = WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        let new_ds = Dataset::write(
            reader,
            self.ds.uri(),
            Some(params),
        )
        .await?;
        self.ds = new_ds;
        Ok(())
    }

    /// Scan the full dataset, returning all rows as RecordBatches.
    pub async fn scan_all(&self) -> Result<Vec<RecordBatch>, lance::Error> {
        let stream = self.ds.scan().try_into_stream().await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok(batches)
    }

    /// Scan with a SQL filter expression (e.g. `"label = 'Post'"`).
    pub async fn scan_filter(&self, filter: &str) -> Result<Vec<RecordBatch>, lance::Error> {
        let mut scanner = self.ds.scan();
        scanner.filter(filter)?;
        let stream = scanner.try_into_stream().await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok(batches)
    }

    /// Count rows, optionally with a filter.
    pub async fn count_rows(&self, filter: Option<&str>) -> Result<usize, lance::Error> {
        self.ds
            .count_rows(filter.map(|s| s.to_string()))
            .await
    }

    /// Compact fragments (merge small files). Delegates to Lance's built-in compaction.
    pub async fn compact(&mut self) -> Result<lance::dataset::optimize::CompactionMetrics, lance::Error> {
        let metrics = lance::dataset::optimize::compact_files(
            &mut self.ds,
            lance::dataset::optimize::CompactionOptions::default(),
            None,
        )
        .await?;
        self.ds.checkout_latest().await?;
        Ok(metrics)
    }

    /// Get the current dataset version.
    pub fn version(&self) -> u64 {
        self.ds.manifest().version
    }

    /// Get the dataset URI.
    pub fn uri(&self) -> &str {
        self.ds.uri()
    }

    /// Get a reference to the inner Lance Dataset.
    pub fn inner(&self) -> &Dataset {
        &self.ds
    }
}

pub async fn create_vertex_log_dataset(uri: &str) -> Result<YataDataset, lance::Error> {
    YataDataset::create_empty(
        uri,
        Arc::new(crate::schema::vertex_log_schema().clone()),
    )
    .await
}

pub async fn create_edge_log_dataset(uri: &str) -> Result<YataDataset, lance::Error> {
    YataDataset::create_empty(
        uri,
        Arc::new(crate::schema::edge_log_schema().clone()),
    )
    .await
}

pub async fn create_vertex_live_dataset(uri: &str) -> Result<YataDataset, lance::Error> {
    YataDataset::create_empty(
        uri,
        Arc::new(crate::schema::vertex_live_schema().clone()),
    )
    .await
}

pub async fn create_edge_live_out_dataset(uri: &str) -> Result<YataDataset, lance::Error> {
    YataDataset::create_empty(
        uri,
        Arc::new(crate::schema::edge_live_out_schema().clone()),
    )
    .await
}

pub async fn create_edge_live_in_dataset(uri: &str) -> Result<YataDataset, lance::Error> {
    YataDataset::create_empty(
        uri,
        Arc::new(crate::schema::edge_live_in_schema().clone()),
    )
    .await
}

pub async fn open_datasets_from_manifest(
    manifest: &crate::manifest::GraphManifest,
) -> Result<crate::manifest::OpenedGraphDatasets, lance::Error> {
    Ok(crate::manifest::OpenedGraphDatasets {
        vertex_log: YataDataset::open(&manifest.tables.vertex_log.uri).await?,
        edge_log: YataDataset::open(&manifest.tables.edge_log.uri).await?,
        vertex_live: YataDataset::open(&manifest.tables.vertex_live.uri).await?,
        edge_live_out: YataDataset::open(&manifest.tables.edge_live_out.uri).await?,
        edge_live_in: YataDataset::open(&manifest.tables.edge_live_in.uri).await?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, UInt64Array, UInt8Array};
    use arrow::datatypes::{DataType, Field, Schema};

    fn wal_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("seq", DataType::UInt64, false),
            Field::new("op", DataType::UInt8, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("pk_key", DataType::Utf8, false),
            Field::new("pk_value", DataType::Utf8, false),
            Field::new("timestamp_ms", DataType::UInt64, false),
            Field::new("props_json", DataType::Utf8, true),
        ]))
    }

    fn test_batch(schema: &Arc<Schema>, seq: &[u64], labels: &[&str], rkeys: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(seq.to_vec())),
                Arc::new(UInt8Array::from(vec![0u8; seq.len()])),
                Arc::new(StringArray::from(labels.to_vec())),
                Arc::new(StringArray::from(vec!["rkey"; seq.len()])),
                Arc::new(StringArray::from(rkeys.to_vec())),
                Arc::new(UInt64Array::from(vec![1711900000000u64; seq.len()])),
                Arc::new(StringArray::from(vec!["{}"; seq.len()])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_create_and_scan() {
        let tmpdir = tempfile::tempdir().unwrap();
        let uri = tmpdir.path().to_str().unwrap();
        let schema = wal_schema();
        let batch = test_batch(&schema, &[1, 2, 3], &["Post", "Like", "Post"], &["a", "b", "c"]);

        let ds = YataDataset::create(uri, vec![batch], schema).await.unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), 3);
        assert_eq!(ds.version(), 1);
    }

    #[tokio::test]
    async fn test_append_and_reopen() {
        let tmpdir = tempfile::tempdir().unwrap();
        let uri = tmpdir.path().to_str().unwrap();
        let schema = wal_schema();

        let mut ds = YataDataset::create(
            uri,
            vec![test_batch(&schema, &[1, 2], &["Post", "Like"], &["a", "b"])],
            schema.clone(),
        )
        .await
        .unwrap();

        ds.append(
            vec![test_batch(&schema, &[3], &["Follow"], &["c"])],
            schema,
        )
        .await
        .unwrap();

        assert_eq!(ds.count_rows(None).await.unwrap(), 3);
        assert_eq!(ds.version(), 2);

        // Re-open
        let ds2 = YataDataset::open(uri).await.unwrap();
        assert_eq!(ds2.count_rows(None).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_scan_filter() {
        let tmpdir = tempfile::tempdir().unwrap();
        let uri = tmpdir.path().to_str().unwrap();
        let schema = wal_schema();
        let batch = test_batch(&schema, &[1, 2, 3], &["Post", "Like", "Post"], &["a", "b", "c"]);

        let ds = YataDataset::create(uri, vec![batch], schema).await.unwrap();
        let posts = ds.count_rows(Some("label = 'Post'")).await.unwrap();
        assert_eq!(posts, 2);
    }
}
