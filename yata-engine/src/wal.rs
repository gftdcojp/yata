//! WAL (Write-Ahead Log) for Kafka-style projection architecture.
//!
//! Write Container: merge_record → WAL append + CSR merge → R2 segment flush.
//! Read Container:  WAL apply → incremental CSR merge → serve queries.
//!
//! WAL entries are stored in an in-memory ring buffer with monotonic sequence numbers.
//! The YataRPC Worker coordinator pushes entries from Write → Read containers.
//! R2 segments provide durability for cold start recovery.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

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
    /// Monotonically increasing sequence number (unique per write container).
    pub seq: u64,
    /// Operation type.
    pub op: WalOp,
    /// Vertex label (Cypher node type).
    pub label: String,
    /// Primary key field name (usually "rkey").
    pub pk_key: String,
    /// Primary key value.
    pub pk_value: String,
    /// Properties as JSON key-value pairs. Empty for Delete ops.
    pub props: serde_json::Map<String, serde_json::Value>,
    /// Timestamp (millis since epoch).
    pub timestamp_ms: u64,
}

/// Fixed-capacity ring buffer for WAL entries.
///
/// Append is O(1). Tail read (after_seq) is O(log N) via binary search on seq.
/// When capacity is exceeded, oldest entries are dropped (consumers must fall back to R2 segments).
pub struct WalRingBuffer {
    entries: VecDeque<WalEntry>,
    capacity: usize,
    seq_counter: AtomicU64,
    /// Sequence number of the oldest entry ever evicted (for gap detection).
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

    /// Allocate the next sequence number.
    pub fn next_seq(&self) -> u64 {
        self.seq_counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Current head sequence (latest written).
    pub fn head_seq(&self) -> u64 {
        self.seq_counter.load(Ordering::SeqCst)
    }

    /// Oldest sequence still in the buffer (0 if empty).
    pub fn oldest_seq(&self) -> u64 {
        self.entries.front().map(|e| e.seq).unwrap_or(0)
    }

    /// Oldest sequence that was evicted from the buffer.
    /// If `after_seq < oldest_evicted_seq`, the consumer has a gap and must use R2 segments.
    pub fn oldest_evicted(&self) -> u64 {
        self.oldest_evicted_seq
    }

    /// Number of entries currently buffered.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Append an entry. If at capacity, evicts the oldest.
    pub fn append(&mut self, entry: WalEntry) {
        if self.entries.len() >= self.capacity {
            if let Some(evicted) = self.entries.pop_front() {
                self.oldest_evicted_seq = evicted.seq;
            }
        }
        self.entries.push_back(entry);
    }

    /// Read entries with seq > after_seq, up to `limit`.
    /// Returns empty vec if after_seq >= head_seq (fully caught up).
    /// Returns None if after_seq < oldest available (gap — consumer must use R2 segments).
    pub fn tail(&self, after_seq: u64, limit: usize) -> Option<Vec<WalEntry>> {
        if self.entries.is_empty() {
            return Some(Vec::new());
        }

        let oldest = self.entries.front().unwrap().seq;
        // Gap detection: consumer's position is before our oldest available AND entries were evicted
        // between the consumer's position and our oldest. If after_seq >= oldest_evicted_seq,
        // the consumer is at the eviction boundary — no gap (entries are contiguous from after_seq+1).
        if after_seq > 0 && after_seq < oldest && after_seq < self.oldest_evicted_seq {
            return None; // Gap — consumer needs R2 segments
        }

        // Binary search for the first entry with seq > after_seq
        let start_idx = if after_seq == 0 {
            0
        } else {
            self.entries
                .partition_point(|e| e.seq <= after_seq)
        };

        let end_idx = std::cmp::min(start_idx + limit, self.entries.len());
        Some(
            self.entries
                .range(start_idx..end_idx)
                .cloned()
                .collect(),
        )
    }

    /// Drain entries with seq <= up_to_seq (for R2 segment flush).
    /// Returns drained entries. Does NOT actually remove from buffer (entries stay for tail reads).
    /// Use this to get entries for serialization, then call `advance_eviction(seq)` after R2 upload.
    pub fn entries_up_to(&self, up_to_seq: u64) -> Vec<WalEntry> {
        self.entries
            .iter()
            .take_while(|e| e.seq <= up_to_seq)
            .cloned()
            .collect()
    }

    /// Set the head sequence (used during cold start to resume from checkpoint).
    pub fn set_head_seq(&self, seq: u64) {
        self.seq_counter.store(seq, Ordering::SeqCst);
    }
}

/// R2 segment metadata (stored alongside segment data).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSegmentMeta {
    /// Partition ID that produced this segment.
    pub partition_id: u32,
    /// First sequence number in this segment.
    pub seq_start: u64,
    /// Last sequence number in this segment.
    pub seq_end: u64,
    /// Number of entries.
    pub entry_count: u64,
    /// Timestamp of segment creation (millis since epoch).
    pub created_at_ms: u64,
}

/// Serialize WAL entries to JSON bytes (for R2 segment upload).
/// Format: newline-delimited JSON (NDJSON) for streaming parse.
pub fn serialize_segment(entries: &[WalEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(entries.len() * 256);
    for entry in entries {
        if let Ok(line) = serde_json::to_vec(entry) {
            buf.extend_from_slice(&line);
            buf.push(b'\n');
        }
    }
    buf
}

/// Deserialize WAL entries from NDJSON bytes (for R2 segment read).
pub fn deserialize_segment(data: &[u8]) -> Vec<WalEntry> {
    let mut entries = Vec::new();
    for line in data.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        if let Ok(entry) = serde_json::from_slice::<WalEntry>(line) {
            entries.push(entry);
        }
    }
    entries
}

/// Build R2 key for a WAL segment.
/// Format: `{prefix}wal/segments/{partition_id}/{seq_start:020}-{seq_end:020}.ndjson`
pub fn segment_r2_key(prefix: &str, partition_id: u32, seq_start: u64, seq_end: u64) -> String {
    format!(
        "{prefix}wal/segments/{partition_id}/{seq_start:020}-{seq_end:020}.ndjson"
    )
}

/// Build R2 key for checkpoint metadata.
/// Format: `{prefix}wal/checkpoints/{partition_id}/latest.json`
pub fn checkpoint_meta_r2_key(prefix: &str, partition_id: u32) -> String {
    format!("{prefix}wal/checkpoints/{partition_id}/latest.json")
}

/// Checkpoint metadata: records the WAL seq at which the ArrowFragment snapshot was taken.
/// Read containers use this to know where to start WAL replay from after loading a checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMeta {
    pub partition_id: u32,
    pub checkpoint_seq: u64,
    pub vertex_count: u64,
    pub edge_count: u64,
    pub created_at_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(seq: u64, label: &str, pk: &str, op: WalOp) -> WalEntry {
        WalEntry {
            seq,
            op,
            label: label.to_string(),
            pk_key: "rkey".to_string(),
            pk_value: pk.to_string(),
            props: serde_json::Map::new(),
            timestamp_ms: 1000 + seq,
        }
    }

    #[test]
    fn test_append_and_tail() {
        let mut buf = WalRingBuffer::new(100);
        for i in 1..=10 {
            let mut entry = make_entry(buf.next_seq(), "Post", &format!("pk_{i}"), WalOp::Upsert);
            entry.seq = i; // fix seq
            buf.seq_counter.store(i, Ordering::SeqCst);
            buf.append(entry);
        }

        // Tail from 0 = all entries
        let all = buf.tail(0, 100).unwrap();
        assert_eq!(all.len(), 10);
        assert_eq!(all[0].seq, 1);
        assert_eq!(all[9].seq, 10);

        // Tail from 5 = entries 6..10
        let after5 = buf.tail(5, 100).unwrap();
        assert_eq!(after5.len(), 5);
        assert_eq!(after5[0].seq, 6);

        // Tail from 10 = empty (caught up)
        let caught_up = buf.tail(10, 100).unwrap();
        assert!(caught_up.is_empty());

        // Tail with limit
        let limited = buf.tail(0, 3).unwrap();
        assert_eq!(limited.len(), 3);
    }

    #[test]
    fn test_eviction_and_gap_detection() {
        let mut buf = WalRingBuffer::new(5);
        for i in 1..=8u64 {
            buf.seq_counter.store(i, Ordering::SeqCst);
            buf.append(make_entry(i, "Post", &format!("pk_{i}"), WalOp::Upsert));
        }

        // Buffer should have entries 4..8 (capacity 5, evicted 1..3)
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.oldest_seq(), 4);
        assert_eq!(buf.oldest_evicted(), 3);
        assert_eq!(buf.head_seq(), 8);

        // Tail from 3 = entries 4..8 (no gap, 3 is exactly the eviction boundary)
        let result = buf.tail(3, 100).unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].seq, 4);

        // Tail from 1 = gap (entry 2 was evicted)
        let gap = buf.tail(1, 100);
        assert!(gap.is_none());

        // Tail from 0 with no eviction tracking = returns all available
        let mut fresh_buf = WalRingBuffer::new(5);
        for i in 1..=3u64 {
            fresh_buf.seq_counter.store(i, Ordering::SeqCst);
            fresh_buf.append(make_entry(i, "Post", &format!("pk_{i}"), WalOp::Upsert));
        }
        let all = fresh_buf.tail(0, 100).unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_serialize_deserialize_segment() {
        let entries = vec![
            make_entry(1, "Post", "pk_1", WalOp::Upsert),
            make_entry(2, "Like", "pk_2", WalOp::Delete),
        ];
        let data = serialize_segment(&entries);
        let parsed = deserialize_segment(&data);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].seq, 1);
        assert_eq!(parsed[0].label, "Post");
        assert_eq!(parsed[1].op, WalOp::Delete);
    }

    #[test]
    fn test_segment_r2_key() {
        let key = segment_r2_key("yata/", 0, 1, 100);
        assert_eq!(key, "yata/wal/segments/0/00000000000000000001-00000000000000000100.ndjson");
    }

    #[test]
    fn test_entries_up_to() {
        let mut buf = WalRingBuffer::new(100);
        for i in 1..=5u64 {
            buf.seq_counter.store(i, Ordering::SeqCst);
            buf.append(make_entry(i, "Post", &format!("pk_{i}"), WalOp::Upsert));
        }
        let up_to_3 = buf.entries_up_to(3);
        assert_eq!(up_to_3.len(), 3);
        assert_eq!(up_to_3[2].seq, 3);
    }

    #[test]
    fn test_empty_buffer() {
        let buf = WalRingBuffer::new(10);
        assert_eq!(buf.head_seq(), 0);
        assert_eq!(buf.oldest_seq(), 0);
        assert!(buf.is_empty());
        let result = buf.tail(0, 100).unwrap();
        assert!(result.is_empty());
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
