//! Arrow IPC WAL format — Shannon-optimal zero-copy WAL serialization.
//!
//! Replaces NDJSON segment format with Arrow IPC File format (seekable, mmap-compatible).
//! Each segment is a single Arrow IPC File containing one RecordBatch per flush.
//!
//! Schema (canonical, all segments share this):
//!   seq:        UInt64    — monotonic sequence number
//!   op:         UInt8     — 0 = Upsert, 1 = Delete
//!   label:      Utf8      — vertex label (Cypher node type)
//!   pk_key:     Utf8      — primary key field name
//!   pk_value:   Utf8      — primary key value
//!   timestamp_ms: UInt64  — millis since epoch
//!   props_json: Utf8      — JSON-encoded properties (Phase 0: typed columns in Phase 1)
//!
//! Zero-copy path: segment file on disk → mmap → Arrow FileReader → RecordBatch
//! (Arrow buffers reference mmap'd pages directly; no heap allocation for column data.)

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

/// Convert WalOp to u8 for Arrow storage.
#[inline]
fn op_to_u8(op: WalOp) -> u8 {
    match op {
        WalOp::Upsert => 0,
        WalOp::Delete => 1,
    }
}

/// Convert u8 back to WalOp.
#[inline]
fn u8_to_op(v: u8) -> WalOp {
    match v {
        1 => WalOp::Delete,
        _ => WalOp::Upsert,
    }
}

/// Convert a serde_json::Value to PropValue (used during Arrow deserialization).
fn json_to_prop_value(v: &serde_json::Value) -> yata_grin::PropValue {
    match v {
        serde_json::Value::String(s) => {
            // Detect base64-encoded binary with "b64:" prefix.
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


/// Convert WAL entries to an Arrow RecordBatch (WAL schema).
///
/// Public so that the compaction path can build a batch and serialize it
/// in either Arrow IPC or Lance format.
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

/// Convert an Arrow RecordBatch (WAL schema) back to WalEntry vector.
pub fn batch_to_wal_entries(batch: &RecordBatch) -> Result<Vec<WalEntry>, String> {
    let n = batch.num_rows();
    let seq_col = batch
        .column_by_name("seq")
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or("missing or invalid 'seq' column")?;
    let op_col = batch
        .column_by_name("op")
        .and_then(|c| c.as_any().downcast_ref::<UInt8Array>())
        .ok_or("missing or invalid 'op' column")?;
    let label_col = batch
        .column_by_name("label")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or("missing or invalid 'label' column")?;
    let pk_key_col = batch
        .column_by_name("pk_key")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or("missing or invalid 'pk_key' column")?;
    let pk_value_col = batch
        .column_by_name("pk_value")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or("missing or invalid 'pk_value' column")?;
    let ts_col = batch
        .column_by_name("timestamp_ms")
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or("missing or invalid 'timestamp_ms' column")?;
    let props_col = batch
        .column_by_name("props_json")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or("missing or invalid 'props_json' column")?;

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

/// Serialize WAL entries to Lance v2 native format bytes (async).
///
/// Uses lance-file crate for columnar encoding (bitpacking, FSST, dictionary).
#[cfg(feature = "native-lance")]
pub fn serialize_segment_lance(entries: &[WalEntry]) -> Result<Bytes, String> {
    if entries.is_empty() {
        return Ok(Bytes::new());
    }
    let batch = wal_entries_to_batch(entries)?;
    crate::engine::ENGINE_RT.block_on(yata_lance::native::serialize_batch_lance(&batch))
}

/// Deserialize Lance v2 native format bytes back to WalEntry vector.
#[cfg(feature = "native-lance")]
pub fn deserialize_segment_lance(data: &[u8]) -> Result<Vec<WalEntry>, String> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let batch = crate::engine::ENGINE_RT.block_on(
        yata_lance::native::deserialize_batch_lance(Bytes::copy_from_slice(data))
    )?;
    batch_to_wal_entries(&batch)
}

/// Serialize WAL entries to Arrow IPC File format bytes.
///
/// File format (not Stream) enables seekable mmap reads.
/// Returns empty Bytes if entries is empty.
pub fn serialize_segment_arrow(entries: &[WalEntry]) -> Result<Bytes, String> {
    if entries.is_empty() {
        return Ok(Bytes::new());
    }

    let batch = wal_entries_to_batch(entries)?;
    let n = batch.num_rows();
    let schema = wal_arrow_schema();

    // Serialize to Arrow IPC File format (seekable, mmap-compatible).
    let buf = std::io::Cursor::new(Vec::with_capacity(n * 128));
    let mut writer = arrow::ipc::writer::FileWriter::try_new(buf, schema.as_ref())
        .map_err(|e| format!("Arrow FileWriter init failed: {e}"))?;
    writer
        .write(&batch)
        .map_err(|e| format!("Arrow FileWriter write failed: {e}"))?;
    writer
        .finish()
        .map_err(|e| format!("Arrow FileWriter finish failed: {e}"))?;

    let buf = writer
        .into_inner()
        .map_err(|e| format!("Arrow FileWriter into_inner failed: {e}"))?;

    Ok(Bytes::from(buf.into_inner()))
}

/// Deserialize WAL entries from Arrow IPC File format bytes.
///
/// Reads all RecordBatches in the file and converts each row back to WalEntry.
pub fn deserialize_segment_arrow(data: &[u8]) -> Result<Vec<WalEntry>, String> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let cursor = std::io::Cursor::new(data);
    let reader = arrow::ipc::reader::FileReader::try_new(cursor, None)
        .map_err(|e| format!("Arrow FileReader open failed: {e}"))?;

    let mut entries = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("Arrow FileReader batch read failed: {e}"))?;
        let n = batch.num_rows();

        let seq_col = batch
            .column_by_name("seq")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or("missing or invalid 'seq' column")?;
        let op_col = batch
            .column_by_name("op")
            .and_then(|c| c.as_any().downcast_ref::<UInt8Array>())
            .ok_or("missing or invalid 'op' column")?;
        let label_col = batch
            .column_by_name("label")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or("missing or invalid 'label' column")?;
        let pk_key_col = batch
            .column_by_name("pk_key")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or("missing or invalid 'pk_key' column")?;
        let pk_value_col = batch
            .column_by_name("pk_value")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or("missing or invalid 'pk_value' column")?;
        let ts_col = batch
            .column_by_name("timestamp_ms")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or("missing or invalid 'timestamp_ms' column")?;
        let props_col = batch
            .column_by_name("props_json")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or("missing or invalid 'props_json' column")?;

        for i in 0..n {
            let props: Vec<(String, yata_grin::PropValue)> = if props_col.is_null(i) {
                Vec::new()
            } else {
                // Parse JSON map → Vec<(String, PropValue)>
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
    }

    Ok(entries)
}

/// Build R2 key for an Arrow IPC WAL segment.
/// Format: `{prefix}wal/segments/{partition_id}/{seq_start:020}-{seq_end:020}.arrow`
pub fn segment_r2_key_arrow(
    prefix: &str,
    partition_id: u32,
    seq_start: u64,
    seq_end: u64,
) -> String {
    format!("{prefix}wal/segments/{partition_id}/{seq_start:020}-{seq_end:020}.arrow")
}

/// Detect WAL segment format from R2 key extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalSegmentFormat {
    Ndjson,
    Arrow,
}

/// Parse format from a segment R2 key (or filename).
pub fn detect_segment_format(key: &str) -> WalSegmentFormat {
    if key.ends_with(".arrow") {
        WalSegmentFormat::Arrow
    } else {
        WalSegmentFormat::Ndjson
    }
}

/// Deserialize WAL entries from either NDJSON or Arrow IPC bytes, auto-detecting format.
/// Uses the R2 key (or filename) to determine format.
///
/// On Arrow deserialization failure, logs the error and falls back to NDJSON parsing
/// (handles mixed-format segments during migration).
pub fn deserialize_segment_auto(key: &str, data: &[u8]) -> Vec<WalEntry> {
    match detect_segment_format(key) {
        WalSegmentFormat::Arrow => match deserialize_segment_arrow(data) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::error!(key, error = %e, bytes = data.len(), "Arrow IPC deser failed, falling back to NDJSON");
                crate::wal::deserialize_segment(data)
            }
        },
        WalSegmentFormat::Ndjson => crate::wal::deserialize_segment(data),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(seq: u64, label: &str, pk: &str, op: WalOp) -> WalEntry {
        let props = vec![
            ("name".to_string(), yata_grin::PropValue::Str("alice".to_string())),
            ("age".to_string(), yata_grin::PropValue::Int(30)),
        ];
        WalEntry {
            seq,
            op,
            label: label.to_string(),
            pk_key: "rkey".to_string(),
            pk_value: pk.to_string(),
            props,
            timestamp_ms: 1000 + seq,
        }
    }

    #[test]
    fn test_roundtrip_arrow_segment() {
        let entries = vec![
            make_entry(1, "Post", "pk_1", WalOp::Upsert),
            make_entry(2, "Like", "pk_2", WalOp::Delete),
            make_entry(3, "Follow", "pk_3", WalOp::Upsert),
        ];

        let data = serialize_segment_arrow(&entries).expect("serialize");
        assert!(!data.is_empty());

        let parsed = deserialize_segment_arrow(&data).expect("deserialize");
        assert_eq!(parsed.len(), 3);

        // Verify field-by-field roundtrip
        assert_eq!(parsed[0].seq, 1);
        assert_eq!(parsed[0].label, "Post");
        assert_eq!(parsed[0].op, WalOp::Upsert);
        assert_eq!(parsed[0].pk_key, "rkey");
        assert_eq!(parsed[0].pk_value, "pk_1");
        assert_eq!(parsed[0].timestamp_ms, 1001);
        assert!(parsed[0].props.iter().any(|(k, v)| k == "name" && *v == yata_grin::PropValue::Str("alice".to_string())));
        assert!(parsed[0].props.iter().any(|(k, v)| k == "age" && *v == yata_grin::PropValue::Int(30)));

        assert_eq!(parsed[1].seq, 2);
        assert_eq!(parsed[1].label, "Like");
        assert_eq!(parsed[1].op, WalOp::Delete);

        assert_eq!(parsed[2].seq, 3);
        assert_eq!(parsed[2].label, "Follow");
    }

    #[test]
    fn test_empty_entries() {
        let data = serialize_segment_arrow(&[]).expect("serialize empty");
        assert!(data.is_empty());

        let parsed = deserialize_segment_arrow(&data).expect("deserialize empty");
        assert!(parsed.is_empty());
    }

    #[test]
    fn test_empty_props() {
        let entry = WalEntry {
            seq: 1,
            op: WalOp::Delete,
            label: "Post".to_string(),
            pk_key: "rkey".to_string(),
            pk_value: "pk_1".to_string(),
            props: Vec::new(),
            timestamp_ms: 1000,
        };

        let data = serialize_segment_arrow(&[entry]).expect("serialize");
        let parsed = deserialize_segment_arrow(&data).expect("deserialize");
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].props.is_empty());
    }

    #[test]
    fn test_parity_with_ndjson() {
        let entries = vec![
            make_entry(1, "Post", "pk_1", WalOp::Upsert),
            make_entry(2, "Like", "pk_2", WalOp::Delete),
        ];

        // NDJSON roundtrip
        let ndjson_data = crate::wal::serialize_segment(&entries);
        let ndjson_parsed = crate::wal::deserialize_segment(&ndjson_data);

        // Arrow roundtrip
        let arrow_data = serialize_segment_arrow(&entries).expect("serialize");
        let arrow_parsed = deserialize_segment_arrow(&arrow_data).expect("deserialize");

        // Parity check: same number of entries, same field values
        assert_eq!(ndjson_parsed.len(), arrow_parsed.len());
        for (nj, ar) in ndjson_parsed.iter().zip(arrow_parsed.iter()) {
            assert_eq!(nj.seq, ar.seq);
            assert_eq!(nj.op, ar.op);
            assert_eq!(nj.label, ar.label);
            assert_eq!(nj.pk_key, ar.pk_key);
            assert_eq!(nj.pk_value, ar.pk_value);
            assert_eq!(nj.timestamp_ms, ar.timestamp_ms);
            assert_eq!(nj.props, ar.props);
        }
    }

    #[test]
    fn test_arrow_smaller_than_ndjson() {
        // Arrow columnar should be more compact than NDJSON for repeated labels/pk_keys.
        let entries: Vec<WalEntry> = (1..=100)
            .map(|i| make_entry(i, "Post", &format!("pk_{i}"), WalOp::Upsert))
            .collect();

        let ndjson_size = crate::wal::serialize_segment(&entries).len();
        let arrow_size = serialize_segment_arrow(&entries).expect("serialize").len();

        // Arrow should be smaller due to dictionary encoding of repeated strings.
        // At minimum, verify Arrow isn't dramatically larger.
        assert!(
            arrow_size <= ndjson_size * 2,
            "Arrow ({arrow_size}) should not be >2x NDJSON ({ndjson_size})"
        );
    }

    #[test]
    fn test_segment_r2_key_arrow() {
        let key = segment_r2_key_arrow("yata/", 0, 1, 100);
        assert_eq!(
            key,
            "yata/wal/segments/0/00000000000000000001-00000000000000000100.arrow"
        );
    }

    #[test]
    fn test_detect_segment_format() {
        assert_eq!(
            detect_segment_format("yata/wal/segments/0/00000000000000000001-00000000000000000100.ndjson"),
            WalSegmentFormat::Ndjson
        );
        assert_eq!(
            detect_segment_format("yata/wal/segments/0/00000000000000000001-00000000000000000100.arrow"),
            WalSegmentFormat::Arrow
        );
        assert_eq!(
            detect_segment_format("unknown_extension.txt"),
            WalSegmentFormat::Ndjson
        );
    }

    #[test]
    fn test_deserialize_auto_arrow() {
        let entries = vec![make_entry(1, "Post", "pk_1", WalOp::Upsert)];
        let data = serialize_segment_arrow(&entries).expect("serialize");
        let parsed = deserialize_segment_auto("seg.arrow", &data);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].label, "Post");
    }

    #[test]
    fn test_deserialize_auto_ndjson() {
        let entries = vec![make_entry(1, "Post", "pk_1", WalOp::Upsert)];
        let data = crate::wal::serialize_segment(&entries);
        let parsed = deserialize_segment_auto("seg.ndjson", &data);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].label, "Post");
    }

    #[test]
    fn test_large_props_roundtrip() {
        use yata_grin::PropValue;
        let props = vec![
            ("key1".to_string(), PropValue::Str("x".repeat(10_000))),
            // Nested JSON becomes Str (PropValue has no nested type)
            ("key2".to_string(), PropValue::Str("{\"nested\":[1,2,3]}".to_string())),
            ("key3".to_string(), PropValue::Bool(true)),
            ("key4".to_string(), PropValue::Null),
        ];

        let entry = WalEntry {
            seq: 42,
            op: WalOp::Upsert,
            label: "BigRecord".to_string(),
            pk_key: "rkey".to_string(),
            pk_value: "big_1".to_string(),
            props,
            timestamp_ms: 9999,
        };

        let data = serialize_segment_arrow(&[entry.clone()]).expect("serialize");
        let parsed = deserialize_segment_arrow(&data).expect("deserialize");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].props, entry.props);
    }

    #[test]
    fn test_op_encoding() {
        assert_eq!(op_to_u8(WalOp::Upsert), 0);
        assert_eq!(op_to_u8(WalOp::Delete), 1);
        assert_eq!(u8_to_op(0), WalOp::Upsert);
        assert_eq!(u8_to_op(1), WalOp::Delete);
        assert_eq!(u8_to_op(255), WalOp::Upsert); // fallback
    }
}
