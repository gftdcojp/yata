//! Backward-compatibility shims for yata-engine migration.
//!
//! These types wrap the new LanceDB API to match the old API signatures
//! used by yata-engine. They will be removed once engine.rs is fully migrated.

use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Compat shim: wraps YataDb + YataTable to match old YataDataset API.
pub struct YataDataset {
    db: crate::YataDb,
    table: Option<crate::YataTable>,
}

impl YataDataset {
    /// Open an existing dataset (table "vertices") at the given path.
    pub async fn open(uri: &str) -> Result<Self, lancedb::Error> {
        let db = crate::YataDb::connect_local(uri).await?;
        let table = db.open_table("vertices").await.ok();
        Ok(Self { db, table })
    }

    /// Open with S3/R2 credentials from env.
    pub async fn open_from_env(prefix: &str) -> Option<Self> {
        let db = crate::YataDb::connect_from_env(prefix).await?;
        let table = db.open_table("vertices").await.ok();
        Some(Self { db, table })
    }

    /// Create a new dataset with initial batches.
    pub async fn create(
        uri: &str,
        batches: Vec<RecordBatch>,
        _schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<Self, lancedb::Error> {
        let db = crate::YataDb::connect_local(uri).await?;
        let batch = batches.into_iter().next().ok_or_else(|| {
            lancedb::Error::InvalidInput { message: "empty batches".into() }
        })?;
        let table = db.create_table("vertices", batch).await?;
        Ok(Self { db, table: Some(table) })
    }

    /// Create with S3/R2 credentials.
    pub async fn create_with_store(
        _uri: &str,
        batches: Vec<RecordBatch>,
        _schema: Arc<arrow::datatypes::Schema>,
        prefix: &str,
    ) -> Result<Self, lancedb::Error> {
        let db = crate::YataDb::connect_from_env(prefix).await.ok_or_else(|| {
            lancedb::Error::InvalidInput { message: "S3 env not configured".into() }
        })?;
        let batch = batches.into_iter().next().ok_or_else(|| {
            lancedb::Error::InvalidInput { message: "empty batches".into() }
        })?;
        let table = db.create_table("vertices", batch).await?;
        Ok(Self { db, table: Some(table) })
    }

    /// Append batches.
    pub async fn append(
        &mut self,
        batches: Vec<RecordBatch>,
        _schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<(), lancedb::Error> {
        if let Some(ref tbl) = self.table {
            for batch in batches {
                tbl.add(batch).await?;
            }
            Ok(())
        } else {
            Err(lancedb::Error::InvalidInput { message: "no table open".into() })
        }
    }

    /// Scan all rows.
    pub async fn scan_all(&self) -> Result<Vec<RecordBatch>, lancedb::Error> {
        if let Some(ref tbl) = self.table {
            tbl.scan_all().await
        } else {
            Ok(Vec::new())
        }
    }

    /// Scan with filter.
    pub async fn scan_filter(&self, filter: &str) -> Result<Vec<RecordBatch>, lancedb::Error> {
        if let Some(ref tbl) = self.table {
            tbl.scan_filter(filter).await
        } else {
            Ok(Vec::new())
        }
    }

    /// Count rows.
    pub async fn count_rows(&self, filter: Option<&str>) -> Result<usize, lancedb::Error> {
        if let Some(ref tbl) = self.table {
            tbl.count_rows(filter).await
        } else {
            Ok(0)
        }
    }

    /// Compact.
    pub async fn compact(&mut self) -> Result<CompactMetrics, lancedb::Error> {
        if let Some(ref tbl) = self.table {
            let stats = tbl.compact().await?;
            Ok(CompactMetrics {
                fragments_removed: stats.compaction.as_ref().map(|c| c.fragments_removed).unwrap_or(0),
                fragments_added: stats.compaction.as_ref().map(|c| c.fragments_added).unwrap_or(0),
            })
        } else {
            Ok(CompactMetrics { fragments_removed: 0, fragments_added: 0 })
        }
    }

    /// Version.
    pub async fn version(&self) -> u64 {
        if let Some(ref tbl) = self.table {
            tbl.version().await.unwrap_or(0)
        } else {
            0
        }
    }

    /// Overwrite with new data.
    pub async fn overwrite(
        &mut self,
        batches: Vec<RecordBatch>,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<(), lancedb::Error> {
        // Drop old table, create new
        let batch = batches.into_iter().next().ok_or_else(|| {
            lancedb::Error::InvalidInput { message: "empty batches".into() }
        })?;
        let table = self.db.create_table("vertices", batch).await?;
        self.table = Some(table);
        let _ = schema;
        Ok(())
    }
}

/// Compat metrics for compaction.
pub struct CompactMetrics {
    pub fragments_removed: usize,
    pub fragments_added: usize,
}

/// Compat stub: replaces the old vector store.
/// Vector search will be re-implemented using lancedb Table::search() in next phase.
pub struct YataVectorStore;

impl YataVectorStore {
    pub async fn new(_data_dir: &str) -> Self {
        Self
    }

    pub async fn write_vertices_with_embeddings(
        &self,
        _nodes: &[yata_cypher::NodeRef],
        _embedding_key: &str,
        _dim: usize,
    ) -> Result<(), String> {
        Ok(()) // stub — lancedb table.add() will replace
    }

    pub async fn vector_search_vertices(
        &self,
        _query: Vec<f32>,
        _limit: usize,
        _label_filter: Option<&str>,
        _prop_filter: Option<&str>,
    ) -> Result<Vec<(yata_cypher::NodeRef, f32)>, String> {
        Ok(Vec::new()) // stub — lancedb table.search().nearest_to() will replace
    }

    pub async fn create_embedding_index(&self) -> Result<(), String> {
        Ok(()) // stub — lancedb table.create_index() will replace
    }
}

/// Compat stub: replaces the old UreqObjectStore.
/// Engine code that references this will be migrated to use YataDb::connect_from_env().
pub struct UreqObjectStore;

impl UreqObjectStore {
    pub fn from_env() -> Option<Self> {
        if std::env::var("YATA_S3_ENDPOINT").is_ok() {
            Some(Self)
        } else {
            None
        }
    }

    pub fn client(&self) -> &Self {
        self
    }
}
