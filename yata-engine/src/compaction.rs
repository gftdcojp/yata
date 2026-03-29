//! L1 Compaction — Kafka-style PK-dedup log rewrite.
//!
//! Reads WAL segments (Arrow IPC or NDJSON), deduplicates entries by (label, pk_value)
//! keeping only the latest seq per key, outputs a compacted Arrow IPC segment.
//!
//! This replaces ArrowFragment snapshot as the persistence mechanism:
//! - Snapshot = serialize entire CSR → ArrowFragment blobs (O(state_size), OOM risk)
//! - Compaction = merge WAL segments → dedup → single Arrow IPC file (O(WAL_size), streaming)
//!
//! The compacted segment uses the same Arrow IPC File format as WAL segments,
//! enabling uniform mmap-based zero-copy reads (Phase 3 MmapCsrView).
//!
//! Compaction is idempotent: compacting an already-compacted segment produces the same output.

use std::collections::HashMap;

use bytes::Bytes;
use yata_grin::PropValue;

use crate::wal::{WalEntry, WalOp};

/// Compact multiple WAL entry batches into a single deduplicated batch.
///
/// Dedup key: (label, pk_key, pk_value). For entries with the same key,
/// only the entry with the highest seq is kept.
/// Delete entries with the highest seq remove the key entirely.
///
/// Returns compacted entries sorted by seq (ascending).
pub fn compact_entries(batches: &[&[WalEntry]]) -> Vec<WalEntry> {
    // Key: (label, pk_key, pk_value) → latest WalEntry
    let mut latest: HashMap<(String, String, String), WalEntry> = HashMap::new();

    for batch in batches {
        for entry in *batch {
            let key = (
                entry.label.clone(),
                entry.pk_key.clone(),
                entry.pk_value.clone(),
            );
            match latest.get(&key) {
                Some(existing) if existing.seq >= entry.seq => {
                    // Keep existing (higher or equal seq)
                }
                _ => {
                    latest.insert(key, entry.clone());
                }
            }
        }
    }

    // Remove entries where the latest op is Delete (tombstone compaction).
    latest.retain(|_, entry| entry.op != WalOp::Delete);

    // Sort by seq ascending for deterministic output.
    let mut result: Vec<WalEntry> = latest.into_values().collect();
    result.sort_by_key(|e| e.seq);
    result
}

/// Compact WAL segments from raw bytes (auto-detecting format per segment).
///
/// Each (key, data) pair is a segment: key is the R2 key (for format detection),
/// data is the raw bytes.
///
/// Returns compacted entries as Arrow IPC File bytes, plus metadata.
pub fn compact_segments(segments: &[(&str, &[u8])]) -> Result<CompactionResult, String> {
    // Parse all segments
    let mut all_entries: Vec<Vec<WalEntry>> = Vec::with_capacity(segments.len());
    let mut min_seq = u64::MAX;
    let mut max_seq = 0u64;
    let mut total_input_entries = 0usize;

    for (key, data) in segments {
        let entries = crate::arrow_wal::deserialize_segment_auto(key, data);
        for e in &entries {
            min_seq = min_seq.min(e.seq);
            max_seq = max_seq.max(e.seq);
        }
        total_input_entries += entries.len();
        all_entries.push(entries);
    }

    if total_input_entries == 0 {
        return Ok(CompactionResult {
            data: Bytes::new(),
            min_seq: 0,
            max_seq: 0,
            input_entries: 0,
            output_entries: 0,
            labels: Vec::new(),
        });
    }

    // Compact
    let refs: Vec<&[WalEntry]> = all_entries.iter().map(|v| v.as_slice()).collect();
    let compacted = compact_entries(&refs);

    // Collect label inventory
    let mut labels: Vec<String> = compacted.iter().map(|e| e.label.clone()).collect();
    labels.sort();
    labels.dedup();

    let output_entries = compacted.len();

    // Serialize to Arrow IPC
    let data = crate::arrow_wal::serialize_segment_arrow(&compacted)?;

    Ok(CompactionResult {
        data,
        min_seq,
        max_seq,
        input_entries: total_input_entries,
        output_entries,
        labels,
    })
}

/// Result of a compaction operation.
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// Compacted Arrow IPC File bytes.
    pub data: Bytes,
    /// Minimum seq across all input segments.
    pub min_seq: u64,
    /// Maximum seq across all input segments.
    pub max_seq: u64,
    /// Total entries across all input segments (before dedup).
    pub input_entries: usize,
    /// Entries in compacted output (after dedup + tombstone removal).
    pub output_entries: usize,
    /// Sorted list of unique labels present in compacted output.
    pub labels: Vec<String>,
}

/// Manifest for compacted state in R2.
///
/// R2 key: `{prefix}log/compacted/{partition_id}/manifest.json`
///
/// The manifest tracks the compacted segment and the WAL seq range it covers.
/// Cold start: load compacted segment → replay WAL segments after compacted_seq.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompactionManifest {
    /// Partition ID.
    pub partition_id: u32,
    /// Version for forward compat.
    pub version: u32,
    /// R2 key of the compacted segment (Arrow IPC).
    pub compacted_segment_key: String,
    /// Maximum WAL seq included in the compacted segment.
    /// WAL replay starts from compacted_seq + 1.
    pub compacted_seq: u64,
    /// Number of entries in the compacted segment.
    pub entry_count: usize,
    /// Sorted list of labels present in the compacted segment.
    pub labels: Vec<String>,
    /// Timestamp of compaction (millis since epoch).
    pub created_at_ms: u64,
    /// Size of compacted segment in bytes.
    pub segment_bytes: usize,
}

/// Build R2 key for the compaction manifest.
pub fn manifest_r2_key(prefix: &str, partition_id: u32) -> String {
    format!("{prefix}log/compacted/{partition_id}/manifest.json")
}

/// Build R2 key for a compacted segment.
pub fn compacted_segment_r2_key(
    prefix: &str,
    partition_id: u32,
    max_seq: u64,
) -> String {
    format!(
        "{prefix}log/compacted/{partition_id}/compacted_{max_seq:020}.arrow"
    )
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Build a CompactionManifest from a CompactionResult.
pub fn build_manifest(
    partition_id: u32,
    prefix: &str,
    result: &CompactionResult,
) -> CompactionManifest {
    CompactionManifest {
        partition_id,
        version: 1,
        compacted_segment_key: compacted_segment_r2_key(prefix, partition_id, result.max_seq),
        compacted_seq: result.max_seq,
        entry_count: result.output_entries,
        labels: result.labels.clone(),
        created_at_ms: now_ms(),
        segment_bytes: result.data.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make(seq: u64, label: &str, pk: &str, op: WalOp, name: &str) -> WalEntry {
        WalEntry {
            seq,
            op,
            label: label.to_string(),
            pk_key: "rkey".to_string(),
            pk_value: pk.to_string(),
            props: vec![("name".to_string(), PropValue::Str(name.to_string()))],
            timestamp_ms: 1000 + seq,
        }
    }

    fn make_delete(seq: u64, label: &str, pk: &str) -> WalEntry {
        WalEntry {
            seq,
            op: WalOp::Delete,
            label: label.to_string(),
            pk_key: "rkey".to_string(),
            pk_value: pk.to_string(),
            props: Vec::new(),
            timestamp_ms: 1000 + seq,
        }
    }

    #[test]
    fn test_compact_dedup_by_pk() {
        let batch = vec![
            make(1, "Post", "pk_1", WalOp::Upsert, "alice_v1"),
            make(2, "Post", "pk_1", WalOp::Upsert, "alice_v2"),
            make(3, "Post", "pk_1", WalOp::Upsert, "alice_v3"),
        ];
        let result = compact_entries(&[&batch]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].seq, 3);
        assert_eq!(
            result[0].props.iter().find(|(k, _)| k == "name").map(|(_, v)| v.clone()),
            Some(PropValue::Str("alice_v3".to_string()))
        );
    }

    #[test]
    fn test_compact_delete_removes_entry() {
        let batch = vec![
            make(1, "Post", "pk_1", WalOp::Upsert, "alice"),
            make_delete(2, "Post", "pk_1"),
        ];
        let result = compact_entries(&[&batch]);
        assert!(result.is_empty(), "Delete should remove the entry");
    }

    #[test]
    fn test_compact_upsert_after_delete() {
        let batch = vec![
            make(1, "Post", "pk_1", WalOp::Upsert, "alice"),
            make_delete(2, "Post", "pk_1"),
            make(3, "Post", "pk_1", WalOp::Upsert, "alice_revived"),
        ];
        let result = compact_entries(&[&batch]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].seq, 3);
    }

    #[test]
    fn test_compact_multi_label() {
        let batch = vec![
            make(1, "Post", "pk_1", WalOp::Upsert, "post1"),
            make(2, "Like", "pk_2", WalOp::Upsert, "like1"),
            make(3, "Post", "pk_1", WalOp::Upsert, "post1_v2"),
            make(4, "Follow", "pk_3", WalOp::Upsert, "follow1"),
        ];
        let result = compact_entries(&[&batch]);
        assert_eq!(result.len(), 3);
        // Post pk_1 should be v2 (seq 3)
        let post = result.iter().find(|e| e.label == "Post").unwrap();
        assert_eq!(post.seq, 3);
    }

    #[test]
    fn test_compact_multi_batch() {
        let batch1 = vec![
            make(1, "Post", "pk_1", WalOp::Upsert, "v1"),
            make(2, "Post", "pk_2", WalOp::Upsert, "b_v1"),
        ];
        let batch2 = vec![
            make(3, "Post", "pk_1", WalOp::Upsert, "v2"),
            make(4, "Post", "pk_3", WalOp::Upsert, "c_v1"),
        ];
        let result = compact_entries(&[&batch1, &batch2]);
        assert_eq!(result.len(), 3); // pk_1(v2), pk_2(v1), pk_3(v1)
        let pk1 = result.iter().find(|e| e.pk_value == "pk_1").unwrap();
        assert_eq!(pk1.seq, 3);
    }

    #[test]
    fn test_compact_empty() {
        let result = compact_entries(&[&[]]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_compact_idempotent() {
        let batch = vec![
            make(1, "Post", "pk_1", WalOp::Upsert, "alice"),
            make(2, "Like", "pk_2", WalOp::Upsert, "bob"),
        ];
        let first = compact_entries(&[&batch]);
        let second = compact_entries(&[&first]);
        assert_eq!(first.len(), second.len());
        for (a, b) in first.iter().zip(second.iter()) {
            assert_eq!(a.seq, b.seq);
            assert_eq!(a.label, b.label);
            assert_eq!(a.pk_value, b.pk_value);
            assert_eq!(a.props, b.props);
        }
    }

    #[test]
    fn test_compact_sorted_by_seq() {
        let batch = vec![
            make(5, "Post", "pk_5", WalOp::Upsert, "e"),
            make(1, "Post", "pk_1", WalOp::Upsert, "a"),
            make(3, "Post", "pk_3", WalOp::Upsert, "c"),
        ];
        let result = compact_entries(&[&batch]);
        let seqs: Vec<u64> = result.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![1, 3, 5]);
    }

    #[test]
    fn test_compact_segments_roundtrip() {
        let entries1 = vec![
            make(1, "Post", "pk_1", WalOp::Upsert, "v1"),
            make(2, "Post", "pk_2", WalOp::Upsert, "v1"),
        ];
        let entries2 = vec![
            make(3, "Post", "pk_1", WalOp::Upsert, "v2"),
            make_delete(4, "Post", "pk_2"),
        ];

        let seg1 = crate::arrow_wal::serialize_segment_arrow(&entries1).unwrap();
        let seg2 = crate::arrow_wal::serialize_segment_arrow(&entries2).unwrap();

        let result = compact_segments(&[
            ("seg1.arrow", &seg1),
            ("seg2.arrow", &seg2),
        ]).unwrap();

        assert_eq!(result.input_entries, 4);
        assert_eq!(result.output_entries, 1); // pk_1 v2 only (pk_2 deleted)
        assert_eq!(result.min_seq, 1);
        assert_eq!(result.max_seq, 4);
        assert_eq!(result.labels, vec!["Post"]);

        // Verify the compacted segment can be deserialized
        let parsed = crate::arrow_wal::deserialize_segment_arrow(&result.data).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].pk_value, "pk_1");
        assert_eq!(parsed[0].seq, 3);
    }

    #[test]
    fn test_compact_segments_mixed_format() {
        let entries = vec![make(1, "Post", "pk_1", WalOp::Upsert, "v1")];

        // One NDJSON, one Arrow
        let ndjson = crate::wal::serialize_segment(&entries);
        let arrow = crate::arrow_wal::serialize_segment_arrow(&entries).unwrap();

        let result = compact_segments(&[
            ("seg1.ndjson", &ndjson),
            ("seg2.arrow", &arrow),
        ]).unwrap();

        // Same entry appears in both — dedup to 1
        assert_eq!(result.output_entries, 1);
    }

    #[test]
    fn test_compact_segments_empty() {
        let result = compact_segments(&[]).unwrap();
        assert_eq!(result.output_entries, 0);
        assert!(result.data.is_empty());
    }

    #[test]
    fn test_manifest_r2_keys() {
        assert_eq!(
            manifest_r2_key("yata/", 0),
            "yata/log/compacted/0/manifest.json"
        );
        assert_eq!(
            compacted_segment_r2_key("yata/", 0, 12345),
            "yata/log/compacted/0/compacted_00000000000000012345.arrow"
        );
    }

    #[test]
    fn test_build_manifest() {
        let result = CompactionResult {
            data: Bytes::from(vec![1, 2, 3]),
            min_seq: 1,
            max_seq: 100,
            input_entries: 50,
            output_entries: 30,
            labels: vec!["Post".to_string(), "Like".to_string()],
        };
        let manifest = build_manifest(0, "yata/", &result);
        assert_eq!(manifest.partition_id, 0);
        assert_eq!(manifest.compacted_seq, 100);
        assert_eq!(manifest.entry_count, 30);
        assert_eq!(manifest.segment_bytes, 3);
        assert_eq!(manifest.labels, vec!["Post", "Like"]);
        assert_eq!(manifest.version, 1);
        assert!(manifest.compacted_segment_key.ends_with(".arrow"));
    }

    #[test]
    fn test_manifest_serde_roundtrip() {
        let manifest = CompactionManifest {
            partition_id: 0,
            version: 1,
            compacted_segment_key: "log/compacted/0/compacted_00100.arrow".to_string(),
            compacted_seq: 100,
            entry_count: 50,
            labels: vec!["Post".to_string()],
            created_at_ms: 1234567890,
            segment_bytes: 4096,
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: CompactionManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.compacted_seq, 100);
        assert_eq!(parsed.entry_count, 50);
    }
}
