//! Arrow IPC WAL — batch conversion between WalEntry and Arrow RecordBatch.
//!
//! Used by:
//! - engine.rs: wal_entries_to_batch (WAL flush → LanceDB table.add)
//! - engine.rs: batch_to_wal_entries (LanceDB scan → wal_apply)
//! - rest.rs: serialize/deserialize_segment_arrow (WAL replica transport)

use arrow::array::{
    Array, ArrayRef, RecordBatch, StringArray, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use base64::Engine as _;
use bytes::Bytes;
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

#[inline]
fn u8_to_op(v: u8) -> WalOp {
    match v {
        1 => WalOp::Delete,
        _ => WalOp::Upsert,
    }
}

fn json_to_prop_value(v: &serde_json::Value) -> yata_grin::PropValue {
    match v {
        serde_json::Value::String(s) => {
            if let Some(encoded) = s.strip_prefix("b64:") {
                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded) {
                    return yata_grin::PropValue::Binary(bytes);
                }
            }
            yata_grin::PropValue::Str(s.clone())
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                yata_grin::PropValue::Int(i)
            } else {
                yata_grin::PropValue::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::Bool(b) => yata_grin::PropValue::Bool(*b),
        serde_json::Value::Null => yata_grin::PropValue::Null,
        _ => yata_grin::PropValue::Str(v.to_string()),
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

/// Convert an Arrow RecordBatch back to WalEntry vector.
pub fn batch_to_wal_entries(batch: &RecordBatch) -> Result<Vec<WalEntry>, String> {
    let n = batch.num_rows();
    let seq_col = batch.column_by_name("seq").and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or("missing 'seq' column")?;
    let op_col = batch.column_by_name("op").and_then(|c| c.as_any().downcast_ref::<UInt8Array>())
        .ok_or("missing 'op' column")?;
    let label_col = batch.column_by_name("label").and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or("missing 'label' column")?;
    let pk_key_col = batch.column_by_name("pk_key").and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or("missing 'pk_key' column")?;
    let pk_value_col = batch.column_by_name("pk_value").and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or("missing 'pk_value' column")?;
    let ts_col = batch.column_by_name("timestamp_ms").and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or("missing 'timestamp_ms' column")?;
    let props_col = batch.column_by_name("props_json").and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or("missing 'props_json' column")?;

    let mut entries = Vec::with_capacity(n);
    for i in 0..n {
        let props: Vec<(String, yata_grin::PropValue)> = if props_col.is_null(i) {
            Vec::new()
        } else {
            let json_map: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(props_col.value(i)).unwrap_or_default();
            json_map.into_iter().map(|(k, v)| (k, json_to_prop_value(&v))).collect()
        };
        entries.push(WalEntry {
            seq: seq_col.value(i),
            op: u8_to_op(op_col.value(i)),
            label: label_col.value(i).to_string(),
            pk_key: pk_key_col.value(i).to_string(),
            pk_value: pk_value_col.value(i).to_string(),
            timestamp_ms: ts_col.value(i),
            props,
        });
    }
    Ok(entries)
}

/// Serialize WAL entries to Arrow IPC File format bytes.
/// Used by WAL replica transport (walTailArrow/walApplyArrow endpoints).
pub fn serialize_segment_arrow(entries: &[WalEntry]) -> Result<Bytes, String> {
    if entries.is_empty() {
        return Ok(Bytes::new());
    }
    let batch = wal_entries_to_batch(entries)?;
    let schema = wal_arrow_schema();
    let buf = std::io::Cursor::new(Vec::with_capacity(entries.len() * 128));
    let mut writer = arrow::ipc::writer::FileWriter::try_new(buf, schema.as_ref())
        .map_err(|e| format!("Arrow FileWriter init failed: {e}"))?;
    writer.write(&batch).map_err(|e| format!("Arrow FileWriter write failed: {e}"))?;
    writer.finish().map_err(|e| format!("Arrow FileWriter finish failed: {e}"))?;
    let buf = writer.into_inner().map_err(|e| format!("Arrow FileWriter into_inner failed: {e}"))?;
    Ok(Bytes::from(buf.into_inner()))
}

/// Deserialize WAL entries from Arrow IPC File format bytes.
/// Used by WAL replica transport (walTailArrow/walApplyArrow endpoints).
pub fn deserialize_segment_arrow(data: &[u8]) -> Result<Vec<WalEntry>, String> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let cursor = std::io::Cursor::new(data);
    let reader = arrow::ipc::reader::FileReader::try_new(cursor, None)
        .map_err(|e| format!("Arrow FileReader open failed: {e}"))?;
    let mut entries = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("Arrow batch read failed: {e}"))?;
        entries.extend(batch_to_wal_entries(&batch)?);
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(seq: u64, label: &str, pk: &str, op: WalOp) -> WalEntry {
        WalEntry {
            seq, op,
            label: label.to_string(),
            pk_key: "rkey".to_string(),
            pk_value: pk.to_string(),
            props: vec![
                ("name".to_string(), yata_grin::PropValue::Str("alice".to_string())),
                ("age".to_string(), yata_grin::PropValue::Int(30)),
            ],
            timestamp_ms: 1000 + seq,
        }
    }

    #[test]
    fn test_roundtrip_arrow_segment() {
        let entries = vec![
            make_entry(1, "Post", "pk_1", WalOp::Upsert),
            make_entry(2, "Like", "pk_2", WalOp::Delete),
        ];
        let data = serialize_segment_arrow(&entries).expect("serialize");
        let parsed = deserialize_segment_arrow(&data).expect("deserialize");
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].seq, 1);
        assert_eq!(parsed[0].label, "Post");
        assert_eq!(parsed[1].op, WalOp::Delete);
    }

    #[test]
    fn test_empty_entries() {
        let data = serialize_segment_arrow(&[]).expect("serialize empty");
        assert!(data.is_empty());
        let parsed = deserialize_segment_arrow(&data).expect("deserialize empty");
        assert!(parsed.is_empty());
    }

    #[test]
    fn test_batch_roundtrip() {
        let entries = vec![make_entry(1, "Post", "pk_1", WalOp::Upsert)];
        let batch = wal_entries_to_batch(&entries).unwrap();
        let recovered = batch_to_wal_entries(&batch).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].seq, 1);
        // Props order may differ after JSON roundtrip — compare as sets
        let mut orig: Vec<_> = entries[0].props.clone();
        let mut recv: Vec<_> = recovered[0].props.clone();
        orig.sort_by(|a, b| a.0.cmp(&b.0));
        recv.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(orig, recv);
    }

    #[test]
    fn test_op_encoding() {
        assert_eq!(op_to_u8(WalOp::Upsert), 0);
        assert_eq!(op_to_u8(WalOp::Delete), 1);
        assert_eq!(u8_to_op(0), WalOp::Upsert);
        assert_eq!(u8_to_op(1), WalOp::Delete);
        assert_eq!(u8_to_op(255), WalOp::Upsert);
    }
}
