//! WAL (Write-Ahead Log) — in-memory ring buffer for graph mutations.
//!
//! WalEntry is the atomic unit of graph mutation (upsert/delete).
//! WalRingBuffer is a fixed-capacity ring buffer for recent entries.
//! Props stored as Vec<(String, PropValue)> — zero JSON overhead on write/apply.

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use yata_grin::PropValue;

/// Operation type for a WAL entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WalOp {
    Upsert,
    Delete,
}

/// A single WAL entry representing a graph mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub seq: u64,
    pub op: WalOp,
    pub label: String,
    pub pk_key: String,
    pub pk_value: String,
    #[serde(with = "props_serde")]
    pub props: Vec<(String, PropValue)>,
    pub timestamp_ms: u64,
}

/// Custom serde for Vec<(String, PropValue)> <-> flat JSON object.
mod props_serde {
    use base64::Engine as _;
    use serde::{Deserializer, Serializer, Deserialize};
    use serde::ser::SerializeMap;
    use yata_grin::PropValue;

    pub fn serialize<S: Serializer>(props: &[(String, PropValue)], ser: S) -> Result<S::Ok, S::Error> {
        let mut map = ser.serialize_map(Some(props.len()))?;
        for (k, v) in props {
            match v {
                PropValue::Null => map.serialize_entry(k, &serde_json::Value::Null)?,
                PropValue::Bool(b) => map.serialize_entry(k, b)?,
                PropValue::Int(n) => map.serialize_entry(k, n)?,
                PropValue::Float(f) => map.serialize_entry(k, f)?,
                PropValue::Str(s) => map.serialize_entry(k, s)?,
                PropValue::Binary(bytes) => {
                    let encoded = format!("b64:{}", base64::engine::general_purpose::STANDARD.encode(bytes));
                    map.serialize_entry(k, &encoded)?;
                }
            }
        }
        map.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Vec<(String, PropValue)>, D::Error> {
        let map = serde_json::Map::<String, serde_json::Value>::deserialize(de)?;
        Ok(map.into_iter().map(|(k, v)| (k, json_value_to_prop_value(&v))).collect())
    }

    fn json_value_to_prop_value(v: &serde_json::Value) -> PropValue {
        match v {
            serde_json::Value::String(s) => {
                if let Some(encoded) = s.strip_prefix("b64:") {
                    if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded) {
                        return PropValue::Binary(bytes);
                    }
                }
                PropValue::Str(s.clone())
            }
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() { PropValue::Int(i) }
                else { PropValue::Float(n.as_f64().unwrap_or(0.0)) }
            }
            serde_json::Value::Bool(b) => PropValue::Bool(*b),
            serde_json::Value::Null => PropValue::Null,
            _ => PropValue::Str(v.to_string()),
        }
    }
}

/// Convert PropValue slice to serde_json::Map (used by arrow_wal batch conversion).
pub fn props_to_json_map(props: &[(String, PropValue)]) -> serde_json::Map<String, serde_json::Value> {
    let mut m = serde_json::Map::new();
    for (k, v) in props {
        let jv = match v {
            PropValue::Str(s) => serde_json::Value::String(s.clone()),
            PropValue::Int(n) => serde_json::Value::Number((*n).into()),
            PropValue::Float(f) => serde_json::json!(*f),
            PropValue::Bool(b) => serde_json::Value::Bool(*b),
            PropValue::Null => serde_json::Value::Null,
            PropValue::Binary(bytes) => {
                serde_json::Value::String(format!("b64:{}", base64::engine::general_purpose::STANDARD.encode(bytes)))
            }
        };
        m.insert(k.clone(), jv);
    }
    m
}

/// Fixed-capacity ring buffer for WAL entries.
///
/// Append is O(1). Tail read (after_seq) is O(log N) via binary search.
pub struct WalRingBuffer {
    entries: VecDeque<WalEntry>,
    capacity: usize,
    seq_counter: AtomicU64,
    oldest_evicted_seq: u64,
}

impl WalRingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity),
            capacity,
            seq_counter: AtomicU64::new(0),
            oldest_evicted_seq: 0,
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.seq_counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn head_seq(&self) -> u64 {
        self.seq_counter.load(Ordering::SeqCst)
    }

    pub fn oldest_seq(&self) -> u64 {
        self.entries.front().map(|e| e.seq).unwrap_or(0)
    }

    pub fn oldest_evicted(&self) -> u64 {
        self.oldest_evicted_seq
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn append(&mut self, entry: WalEntry) {
        if self.entries.len() >= self.capacity {
            if let Some(evicted) = self.entries.pop_front() {
                self.oldest_evicted_seq = evicted.seq;
            }
        }
        self.entries.push_back(entry);
    }

    pub fn tail(&self, after_seq: u64, limit: usize) -> Option<Vec<WalEntry>> {
        if self.entries.is_empty() {
            return Some(Vec::new());
        }
        let oldest = self.entries.front().unwrap().seq;
        if after_seq > 0 && after_seq < oldest && after_seq < self.oldest_evicted_seq {
            return None;
        }
        let start_idx = if after_seq == 0 { 0 }
        else { self.entries.partition_point(|e| e.seq <= after_seq) };
        let end_idx = std::cmp::min(start_idx + limit, self.entries.len());
        Some(self.entries.range(start_idx..end_idx).cloned().collect())
    }

    pub fn entries_up_to(&self, up_to_seq: u64) -> Vec<WalEntry> {
        self.entries.iter().take_while(|e| e.seq <= up_to_seq).cloned().collect()
    }

    pub fn set_head_seq(&self, seq: u64) {
        self.seq_counter.store(seq, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(seq: u64, label: &str, pk: &str, op: WalOp) -> WalEntry {
        WalEntry { seq, op, label: label.to_string(), pk_key: "rkey".to_string(),
            pk_value: pk.to_string(), props: Vec::new(), timestamp_ms: 1000 + seq }
    }

    #[test]
    fn test_append_and_tail() {
        let mut buf = WalRingBuffer::new(100);
        for i in 1..=10 {
            let mut entry = make_entry(buf.next_seq(), "Post", &format!("pk_{i}"), WalOp::Upsert);
            entry.seq = i;
            buf.seq_counter.store(i, Ordering::SeqCst);
            buf.append(entry);
        }
        let all = buf.tail(0, 100).unwrap();
        assert_eq!(all.len(), 10);
        let after5 = buf.tail(5, 100).unwrap();
        assert_eq!(after5.len(), 5);
        assert_eq!(after5[0].seq, 6);
    }

    #[test]
    fn test_eviction_and_gap() {
        let mut buf = WalRingBuffer::new(5);
        for i in 1..=8u64 {
            buf.seq_counter.store(i, Ordering::SeqCst);
            buf.append(make_entry(i, "Post", &format!("pk_{i}"), WalOp::Upsert));
        }
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.oldest_seq(), 4);
        assert!(buf.tail(3, 100).is_some());
        assert!(buf.tail(1, 100).is_none());
    }

    #[test]
    fn test_empty_buffer() {
        let buf = WalRingBuffer::new(10);
        assert_eq!(buf.head_seq(), 0);
        assert!(buf.is_empty());
        assert!(buf.tail(0, 100).unwrap().is_empty());
    }

    #[test]
    fn test_delete_op_serialization() {
        let entry = make_entry(1, "Post", "pk_1", WalOp::Delete);
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"delete\""));
        let parsed: WalEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.op, WalOp::Delete);
    }
}
