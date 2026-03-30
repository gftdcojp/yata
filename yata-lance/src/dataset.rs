//! YataDb — LanceDB-backed persistence for yata graph.
//!
//! Uses lancedb::Connection + Table for all storage operations.
//! LanceDB handles versioning, manifest, fragments, compaction, and S3/R2 internally.

use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use std::sync::Arc;

/// LanceDB connection wrapper for yata graph persistence.
pub struct YataDb {
    db: lancedb::Connection,
}

/// Single table handle (vertices or edges).
pub struct YataTable {
    table: lancedb::Table,
}

impl YataDb {
    /// Connect to a local LanceDB database.
    pub async fn connect_local(path: &str) -> Result<Self, lancedb::Error> {
        let db = lancedb::connect(path).execute().await?;
        Ok(Self { db })
    }

    /// Connect to an S3/R2-backed LanceDB database.
    ///
    /// `uri` should be `s3://bucket/prefix` or `s3+ddb://bucket/prefix` for DynamoDB commit store.
    /// R2 is S3-compatible, so use `s3://bucket/prefix` with R2 endpoint.
    pub async fn connect_s3(
        uri: &str,
        endpoint: &str,
        access_key_id: &str,
        secret_access_key: &str,
        region: &str,
    ) -> Result<Self, lancedb::Error> {
        let db = lancedb::connect(uri)
            .storage_option("aws_access_key_id", access_key_id)
            .storage_option("aws_secret_access_key", secret_access_key)
            .storage_option("aws_endpoint", endpoint)
            .storage_option("aws_region", region)
            .storage_option("aws_virtual_hosted_style_request", "false")
            .execute()
            .await?;
        Ok(Self { db })
    }

    /// Connect from YATA_S3_* env vars. Returns None if not configured.
    pub async fn connect_from_env(prefix: &str) -> Option<Self> {
        let endpoint = std::env::var("YATA_S3_ENDPOINT").ok()?;
        let bucket = std::env::var("YATA_S3_BUCKET").unwrap_or_default();
        let key_id = std::env::var("YATA_S3_ACCESS_KEY_ID")
            .or_else(|_| std::env::var("YATA_S3_KEY_ID"))
            .unwrap_or_default();
        let secret = std::env::var("YATA_S3_SECRET_ACCESS_KEY")
            .or_else(|_| std::env::var("YATA_S3_SECRET_KEY"))
            .or_else(|_| std::env::var("YATA_S3_APPLICATION_KEY"))
            .unwrap_or_default();
        let region = std::env::var("YATA_S3_REGION").unwrap_or_else(|_| "auto".to_string());
        if endpoint.is_empty() || bucket.is_empty() || key_id.is_empty() || secret.is_empty() {
            return None;
        }
        let uri = format!("s3://{bucket}/{prefix}");
        tracing::info!(%uri, %endpoint, "connecting to LanceDB on R2");
        Self::connect_s3(&uri, &endpoint, &key_id, &secret, &region)
            .await
            .ok()
    }

    /// Open an existing table.
    pub async fn open_table(&self, name: &str) -> Result<YataTable, lancedb::Error> {
        let table = self.db.open_table(name).execute().await?;
        Ok(YataTable { table })
    }

    /// Create a new table with initial data.
    pub async fn create_table(
        &self,
        name: &str,
        batch: RecordBatch,
    ) -> Result<YataTable, lancedb::Error> {
        let table = self.db.create_table(name, batch).execute().await?;
        Ok(YataTable { table })
    }

    /// Create an empty table with a given schema.
    pub async fn create_empty_table(
        &self,
        name: &str,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<YataTable, lancedb::Error> {
        let table = self.db.create_empty_table(name, schema).execute().await?;
        Ok(YataTable { table })
    }

    /// List all table names.
    pub async fn table_names(&self) -> Result<Vec<String>, lancedb::Error> {
        self.db.table_names().execute().await
    }

    /// Get the underlying LanceDB connection.
    pub fn inner(&self) -> &lancedb::Connection {
        &self.db
    }
}

impl YataTable {
    /// Append a batch to the table (creates a new version).
    pub async fn add(&self, batch: RecordBatch) -> Result<(), lancedb::Error> {
        self.table.add(batch).execute().await?;
        Ok(())
    }

    /// Scan the full table, returning all rows.
    pub async fn scan_all(&self) -> Result<Vec<RecordBatch>, lancedb::Error> {
        let stream = self.table.query().execute().await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok(batches)
    }

    /// Scan with a SQL filter expression (e.g. `"label = 'Post'"`).
    pub async fn scan_filter(&self, filter: &str) -> Result<Vec<RecordBatch>, lancedb::Error> {
        let stream = self
            .table
            .query()
            .only_if(filter)
            .execute()
            .await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok(batches)
    }

    /// Count rows, optionally with a filter.
    pub async fn count_rows(&self, filter: Option<&str>) -> Result<usize, lancedb::Error> {
        self.table.count_rows(filter.map(|s| s.to_string())).await
    }

    /// Compact fragments (merge small files). Delegates to LanceDB built-in optimization.
    pub async fn compact(&self) -> Result<lancedb::table::OptimizeStats, lancedb::Error> {
        self.table
            .optimize(lancedb::table::OptimizeAction::Compact {
                options: Default::default(),
                remap_options: None,
            })
            .await
    }

    /// Get the current table version.
    pub async fn version(&self) -> Result<u64, lancedb::Error> {
        self.table.version().await
    }

    /// Get table name.
    pub fn name(&self) -> &str {
        self.table.name()
    }

    /// Get the underlying LanceDB table.
    pub fn inner(&self) -> &lancedb::Table {
        &self.table
    }
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

    fn test_batch(
        schema: &Arc<Schema>,
        seq: &[u64],
        labels: &[&str],
        rkeys: &[&str],
    ) -> RecordBatch {
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
        let db = YataDb::connect_local(tmpdir.path().to_str().unwrap())
            .await
            .unwrap();
        let schema = wal_schema();
        let batch = test_batch(&schema, &[1, 2, 3], &["Post", "Like", "Post"], &["a", "b", "c"]);

        let table = db.create_table("vertices", batch).await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_add_and_reopen() {
        let tmpdir = tempfile::tempdir().unwrap();
        let path = tmpdir.path().to_str().unwrap();
        let db = YataDb::connect_local(path).await.unwrap();
        let schema = wal_schema();

        let table = db
            .create_table(
                "vertices",
                test_batch(&schema, &[1, 2], &["Post", "Like"], &["a", "b"]),
            )
            .await
            .unwrap();

        table
            .add(test_batch(&schema, &[3], &["Follow"], &["c"]))
            .await
            .unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 3);

        // Re-open
        let db2 = YataDb::connect_local(path).await.unwrap();
        let table2 = db2.open_table("vertices").await.unwrap();
        assert_eq!(table2.count_rows(None).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_scan_filter() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db = YataDb::connect_local(tmpdir.path().to_str().unwrap())
            .await
            .unwrap();
        let schema = wal_schema();
        let batch = test_batch(&schema, &[1, 2, 3], &["Post", "Like", "Post"], &["a", "b", "c"]);

        let table = db.create_table("vertices", batch).await.unwrap();
        let posts = table.count_rows(Some("label = 'Post'")).await.unwrap();
        assert_eq!(posts, 2);
    }
}
