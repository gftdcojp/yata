//! LanceTable — Lance-compatible table backed by Arrow IPC fragments on R2.
//!
//! Fragment layout (R2):
//!   lance/{table_name}/{pid}/fragments/{version:020}-{fragment_id:06}.arrow
//!   lance/{table_name}/{pid}/manifest-{inverted:020}.json
//!   lance/{table_name}/{pid}/deletion/{version:020}-{fragment_id:06}.bin
//!
//! Each append creates a new immutable fragment (Arrow IPC File).
//! Deletes write a deletion bitmap for the affected fragment.
//! Compaction merges fragments + applies deletion vectors.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_array::{Int64Array, StringArray, UInt32Array, UInt8Array};
use arrow_ipc::writer::FileWriter;
use arrow_ipc::reader::FileReader;
use arrow_schema::Schema;
use bytes::Bytes;

use crate::schema::VERTICES_SCHEMA;

/// A flat vertex row for Arrow IPC serialization.
#[derive(Debug, Clone)]
pub struct VertexRow {
    pub label: String,
    pub rkey: String,
    pub collection: String,
    pub value_b64: String,
    pub repo: String,
    pub updated_at: i64,
    pub sensitivity_ord: u8,
    pub owner_hash: u32,
}

/// A single immutable fragment (Arrow IPC file) in the table.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Fragment {
    pub id: u32,
    pub version: u64,
    pub r2_key: String,
    pub row_count: usize,
    pub byte_size: usize,
    pub blake3_hex: String,
    /// Labels contained in this fragment (for pruning).
    pub labels: Vec<String>,
}

/// Lance-compatible table manifest (versioned, immutable per version).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableManifest {
    pub table_name: String,
    pub partition_id: u32,
    pub version: u64,
    pub fragments: Vec<Fragment>,
    /// Fragment IDs with active deletion vectors.
    pub deletions: HashMap<u32, DeletionInfo>,
    pub total_rows: usize,
    pub created_at_ms: u64,
}

/// Deletion vector metadata for a fragment.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeletionInfo {
    pub r2_key: String,
    pub deleted_count: usize,
}

/// Lance-compatible table backed by Arrow IPC fragments on R2.
pub struct LanceTable {
    pub table_name: String,
    pub partition_id: u32,
    pub schema: Arc<Schema>,
    prefix: String,
}

impl LanceTable {
    /// Create a new LanceTable pointed at an R2 prefix.
    pub fn new(table_name: &str, partition_id: u32, r2_prefix: &str) -> Self {
        let prefix = format!("{}lance/{}/{}/", r2_prefix, table_name, partition_id);
        Self {
            table_name: table_name.to_string(),
            partition_id,
            schema: VERTICES_SCHEMA.clone(),
            prefix,
        }
    }

    /// Serialize vertex rows to an Arrow IPC fragment (immutable bytes).
    pub fn serialize_fragment(rows: &[VertexRow]) -> Result<Bytes, String> {
        let batch = vertex_rows_to_batch(rows)?;
        let mut buf = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut buf, &VERTICES_SCHEMA)
                .map_err(|e| e.to_string())?;
            writer.write(&batch).map_err(|e| e.to_string())?;
            writer.finish().map_err(|e| e.to_string())?;
        }
        Ok(Bytes::from(buf))
    }

    /// Deserialize an Arrow IPC fragment to vertex rows.
    pub fn deserialize_fragment(data: &[u8]) -> Result<Vec<VertexRow>, String> {
        let cursor = std::io::Cursor::new(data);
        let reader = FileReader::try_new(cursor, None).map_err(|e| e.to_string())?;
        let mut rows = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| e.to_string())?;
            rows.extend(batch_to_vertex_rows(&batch)?);
        }
        Ok(rows)
    }

    /// Build a Fragment metadata entry.
    pub fn build_fragment(
        &self,
        version: u64,
        fragment_id: u32,
        data: &Bytes,
        labels: Vec<String>,
        row_count: usize,
    ) -> Fragment {
        let blake3_hex = blake3::hash(data).to_hex().to_string();
        Fragment {
            id: fragment_id,
            version,
            r2_key: format!("{}fragments/{:020}-{:06}.arrow", self.prefix, version, fragment_id),
            row_count,
            byte_size: data.len(),
            blake3_hex,
            labels,
        }
    }

    /// Build the manifest R2 key (inverted versioning for O(1) latest lookup).
    pub fn manifest_key(&self, version: u64) -> String {
        let inverted = u64::MAX - version;
        format!("{}manifest-{:020}.json", self.prefix, inverted)
    }

    /// Build a new manifest from existing + new fragment.
    pub fn build_manifest(
        &self,
        prev: Option<&TableManifest>,
        new_fragments: Vec<Fragment>,
    ) -> TableManifest {
        let version = prev.map(|m| m.version + 1).unwrap_or(1);
        let mut fragments = prev.map(|m| m.fragments.clone()).unwrap_or_default();
        let mut total_rows = prev.map(|m| m.total_rows).unwrap_or(0);
        for f in &new_fragments {
            total_rows += f.row_count;
        }
        fragments.extend(new_fragments);
        let deletions = prev.map(|m| m.deletions.clone()).unwrap_or_default();
        TableManifest {
            table_name: self.table_name.clone(),
            partition_id: self.partition_id,
            version,
            fragments,
            deletions,
            total_rows,
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
}

// ── Arrow RecordBatch conversion ──

fn vertex_rows_to_batch(rows: &[VertexRow]) -> Result<RecordBatch, String> {
    let label: StringArray = rows.iter().map(|r| Some(r.label.as_str())).collect();
    let rkey: StringArray = rows.iter().map(|r| Some(r.rkey.as_str())).collect();
    let collection: StringArray = rows.iter().map(|r| Some(r.collection.as_str())).collect();
    let value_b64: StringArray = rows.iter().map(|r| Some(r.value_b64.as_str())).collect();
    let repo: StringArray = rows.iter().map(|r| Some(r.repo.as_str())).collect();
    let updated_at: Int64Array = rows.iter().map(|r| Some(r.updated_at)).collect();
    let sensitivity_ord: UInt8Array = rows.iter().map(|r| Some(r.sensitivity_ord)).collect();
    let owner_hash: UInt32Array = rows.iter().map(|r| Some(r.owner_hash)).collect();

    RecordBatch::try_new(
        VERTICES_SCHEMA.clone(),
        vec![
            Arc::new(label),
            Arc::new(rkey),
            Arc::new(collection),
            Arc::new(value_b64),
            Arc::new(repo),
            Arc::new(updated_at),
            Arc::new(sensitivity_ord),
            Arc::new(owner_hash),
        ],
    )
    .map_err(|e| e.to_string())
}

fn batch_to_vertex_rows(batch: &RecordBatch) -> Result<Vec<VertexRow>, String> {
    let label = batch.column(0).as_any().downcast_ref::<StringArray>().ok_or("label col")?;
    let rkey = batch.column(1).as_any().downcast_ref::<StringArray>().ok_or("rkey col")?;
    let collection = batch.column(2).as_any().downcast_ref::<StringArray>().ok_or("collection col")?;
    let value_b64 = batch.column(3).as_any().downcast_ref::<StringArray>().ok_or("value_b64 col")?;
    let repo = batch.column(4).as_any().downcast_ref::<StringArray>().ok_or("repo col")?;
    let updated_at = batch.column(5).as_any().downcast_ref::<Int64Array>().ok_or("updated_at col")?;
    let sensitivity_ord = batch.column(6).as_any().downcast_ref::<UInt8Array>().ok_or("sensitivity_ord col")?;
    let owner_hash = batch.column(7).as_any().downcast_ref::<UInt32Array>().ok_or("owner_hash col")?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        rows.push(VertexRow {
            label: label.value(i).to_string(),
            rkey: rkey.value(i).to_string(),
            collection: collection.value(i).to_string(),
            value_b64: value_b64.value(i).to_string(),
            repo: repo.value(i).to_string(),
            updated_at: updated_at.value(i),
            sensitivity_ord: sensitivity_ord.value(i),
            owner_hash: owner_hash.value(i),
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_fragment() {
        let rows = vec![
            VertexRow {
                label: "Post".into(),
                rkey: "abc123".into(),
                collection: "app.bsky.feed.post".into(),
                value_b64: "eyJ0ZXh0IjoiaGVsbG8ifQ==".into(),
                repo: "did:web:test.gftd.ai".into(),
                updated_at: 1711900000000,
                sensitivity_ord: 0,
                owner_hash: 12345,
            },
            VertexRow {
                label: "Like".into(),
                rkey: "def456".into(),
                collection: "app.bsky.feed.like".into(),
                value_b64: "e30=".into(),
                repo: "did:web:test.gftd.ai".into(),
                updated_at: 1711900001000,
                sensitivity_ord: 0,
                owner_hash: 12345,
            },
        ];
        let data = LanceTable::serialize_fragment(&rows).unwrap();
        let decoded = LanceTable::deserialize_fragment(&data).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].label, "Post");
        assert_eq!(decoded[0].rkey, "abc123");
        assert_eq!(decoded[1].label, "Like");
        assert_eq!(decoded[1].updated_at, 1711900001000);
    }

    #[test]
    fn test_manifest_versioning() {
        let table = LanceTable::new("vertices", 0, "yata/");
        let m1 = table.build_manifest(None, vec![
            table.build_fragment(1, 0, &Bytes::from("test"), vec!["Post".into()], 100),
        ]);
        assert_eq!(m1.version, 1);
        assert_eq!(m1.total_rows, 100);
        assert_eq!(m1.fragments.len(), 1);

        let m2 = table.build_manifest(Some(&m1), vec![
            table.build_fragment(2, 1, &Bytes::from("test2"), vec!["Like".into()], 50),
        ]);
        assert_eq!(m2.version, 2);
        assert_eq!(m2.total_rows, 150);
        assert_eq!(m2.fragments.len(), 2);
    }

    #[test]
    fn test_manifest_key_inverted() {
        let table = LanceTable::new("vertices", 0, "yata/");
        let k1 = table.manifest_key(1);
        let k2 = table.manifest_key(2);
        // Inverted: higher version = lexicographically smaller key → O(1) latest via ListObjects
        assert!(k1 > k2);
    }
}
