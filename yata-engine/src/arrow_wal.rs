//! Arrow WAL — batch conversion between WalEntry and Arrow RecordBatch.
//!
//! Used by engine.rs: wal_entries_to_batch (WAL flush → LanceDB table.add).

use arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use crate::wal::{WalEntry, WalOp};

/// Canonical Arrow schema for WAL segments.
pub fn wal_arrow_schema() -> Arc<Schema> {
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

#[inline]
fn op_to_u8(op: WalOp) -> u8 {
    match op {
        WalOp::Upsert => 0,
        WalOp::Delete => 1,
    }
}

/// Convert WAL entries to an Arrow RecordBatch.
pub fn wal_entries_to_batch(entries: &[WalEntry]) -> Result<RecordBatch, String> {
    if entries.is_empty() {
        return Err("empty entries".into());
    }
    let schema = wal_arrow_schema();

    let seq_values: Vec<u64> = entries.iter().map(|e| e.seq).collect();
    let op_values: Vec<u8> = entries.iter().map(|e| op_to_u8(e.op)).collect();
    let label_values: Vec<&str> = entries.iter().map(|e| e.label.as_str()).collect();
    let pk_key_values: Vec<&str> = entries.iter().map(|e| e.pk_key.as_str()).collect();
    let pk_value_values: Vec<&str> = entries.iter().map(|e| e.pk_value.as_str()).collect();
    let ts_values: Vec<u64> = entries.iter().map(|e| e.timestamp_ms).collect();
    let props_values: Vec<String> = entries
        .iter()
        .map(|e| {
            let json_map = crate::wal::props_to_json_map(&e.props);
            serde_json::to_string(&json_map).unwrap_or_default()
        })
        .collect();
    let props_refs: Vec<&str> = props_values.iter().map(|s| s.as_str()).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(seq_values)),
        Arc::new(UInt8Array::from(op_values)),
        Arc::new(StringArray::from(label_values)),
        Arc::new(StringArray::from(pk_key_values)),
        Arc::new(StringArray::from(pk_value_values)),
        Arc::new(UInt64Array::from(ts_values)),
        Arc::new(StringArray::from(props_refs)),
    ];

    RecordBatch::try_new(schema, columns)
        .map_err(|e| format!("Arrow RecordBatch build failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_roundtrip() {
        let entries = vec![WalEntry {
            seq: 1, op: WalOp::Upsert,
            label: "Post".to_string(),
            pk_key: "rkey".to_string(),
            pk_value: "pk_1".to_string(),
            props: vec![
                ("name".to_string(), yata_grin::PropValue::Str("alice".to_string())),
                ("age".to_string(), yata_grin::PropValue::Int(30)),
            ],
            timestamp_ms: 1001,
        }];
        let batch = wal_entries_to_batch(&entries).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_op_encoding() {
        assert_eq!(op_to_u8(WalOp::Upsert), 0);
        assert_eq!(op_to_u8(WalOp::Delete), 1);
    }
}
