//! L1 Compaction — Kafka-style PK-dedup log rewrite.
//!
//! Reads WAL segments (Arrow IPC or NDJSON), deduplicates entries by (label, pk_value)
//! keeping only the latest seq per key, outputs per-label compacted Arrow IPC segments.
//!
//! Per-label compaction: only dirty labels are re-compacted on each cycle.
//! Clean labels retain their existing compacted segment (zero R2 I/O).
//!
//! The compacted segments use the same Arrow IPC File format as WAL segments,
//! enabling uniform mmap-based zero-copy reads (Phase 3 MmapCsrView).
//!
//! Compaction is idempotent: compacting an already-compacted segment produces the same output.

use std::collections::{HashMap, HashSet};

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

/// Partition WAL entries by label, then compact each label independently.
/// Returns a map of label → compacted entries.
pub fn compact_entries_by_label(batches: &[&[WalEntry]]) -> HashMap<String, Vec<WalEntry>> {
    // Group by label first
    let mut by_label: HashMap<String, Vec<&WalEntry>> = HashMap::new();
    for batch in batches {
        for entry in *batch {
            by_label.entry(entry.label.clone()).or_default().push(entry);
        }
    }

    let mut result: HashMap<String, Vec<WalEntry>> = HashMap::new();
    for (label, entries) in by_label {
        // PK-dedup within this label
        let mut latest: HashMap<(&str, &str), &WalEntry> = HashMap::new();
        for entry in &entries {
            match latest.get(&(entry.pk_key.as_str(), entry.pk_value.as_str())) {
                Some(existing) if existing.seq >= entry.seq => {}
                _ => { latest.insert((entry.pk_key.as_str(), entry.pk_value.as_str()), entry); }
            }
        }
        // Remove tombstones
        let mut compacted: Vec<WalEntry> = latest.into_values()
            .filter(|e| e.op != WalOp::Delete)
            .cloned()
            .collect();
        compacted.sort_by_key(|e| e.seq);
        if !compacted.is_empty() {
            result.insert(label, compacted);
        }
    }
    result
}

/// Per-label compaction result for a single label.
#[derive(Debug, Clone)]
pub struct LabelCompactionResult {
    pub label: String,
    pub data: Bytes,
    pub max_seq: u64,
    pub entry_count: usize,
}

/// Compact WAL segments per-label. Only processes entries for the given dirty labels.
/// Returns per-label compacted Arrow IPC segments.
pub fn compact_segments_by_label(
    segments: &[(&str, &[u8])],
    dirty_labels: &HashSet<String>,
) -> Result<Vec<LabelCompactionResult>, String> {
    // Parse all segments
    let mut all_entries: Vec<Vec<WalEntry>> = Vec::with_capacity(segments.len());
    for (key, data) in segments {
        let entries = crate::arrow_wal::deserialize_segment_auto(key, data);
        all_entries.push(entries);
    }

    // Flatten and filter to dirty labels only
    let dirty_entries: Vec<WalEntry> = all_entries.into_iter()
        .flatten()
        .filter(|e| dirty_labels.contains(&e.label))
        .collect();

    if dirty_entries.is_empty() {
        return Ok(Vec::new());
    }

    let refs = vec![dirty_entries.as_slice()];
    let by_label = compact_entries_by_label(&refs);

    let mut results = Vec::with_capacity(by_label.len());
    for (label, entries) in by_label {
        let max_seq = entries.iter().map(|e| e.seq).max().unwrap_or(0);
        let entry_count = entries.len();
        let data = crate::arrow_wal::serialize_segment_arrow(&entries)?;
        results.push(LabelCompactionResult { label, data, max_seq, entry_count });
    }
    results.sort_by(|a, b| a.label.cmp(&b.label));
    Ok(results)
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

/// V2 manifest with per-label compacted segments.
/// Backward-compatible: v1 fields retained, v2 adds `label_segments`.
///
/// R2 key: `{prefix}log/compacted/{partition_id}/manifest.json`
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompactionManifest {
    /// Partition ID.
    pub partition_id: u32,
    /// Version for forward compat. v1 = monolithic, v2 = per-label.
    pub version: u32,
    /// R2 key of the compacted segment (v1 monolithic, empty in v2).
    #[serde(default)]
    pub compacted_segment_key: String,
    /// Maximum WAL seq included in the compacted segment.
    /// WAL replay starts from compacted_seq + 1.
    pub compacted_seq: u64,
    /// Number of entries in the compacted segment (v1) or total across all labels (v2).
    pub entry_count: usize,
    /// Sorted list of labels present in the compacted segment.
    pub labels: Vec<String>,
    /// Timestamp of compaction (millis since epoch).
    pub created_at_ms: u64,
    /// Size of compacted segment in bytes (v1) or total across all labels (v2).
    pub segment_bytes: usize,
    /// Per-label compacted segment state (v2). Empty in v1.
    #[serde(default)]
    pub label_segments: HashMap<String, LabelSegmentState>,
}

/// Per-label compacted segment metadata.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LabelSegmentState {
    /// R2 key of the per-label compacted segment.
    pub key: String,
    /// Max WAL seq included in this label's compacted segment.
    pub max_seq: u64,
    /// Number of entries.
    pub entry_count: usize,
    /// Segment size in bytes.
    pub segment_bytes: usize,
    /// Blake3 hash of the segment data (hex-encoded). Empty for legacy segments.
    #[serde(default)]
    pub blake3_hex: String,
}

/// Compute Blake3 hash of data, return hex string.
pub fn blake3_hex(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

/// Verify Blake3 checksum. Returns Ok if checksum matches or is empty (legacy).
/// Returns Err with details if mismatch.
pub fn verify_blake3(data: &[u8], expected_hex: &str, label: &str) -> Result<(), String> {
    if expected_hex.is_empty() {
        return Ok(()); // legacy segment without checksum
    }
    let actual = blake3_hex(data);
    if actual != expected_hex {
        return Err(format!(
            "Blake3 checksum mismatch for label {label}: expected {expected_hex}, got {actual}"
        ));
    }
    Ok(())
}

/// Build R2 key for the compaction manifest.
pub fn manifest_r2_key(prefix: &str, partition_id: u32) -> String {
    format!("{prefix}log/compacted/{partition_id}/manifest.json")
}

/// Build R2 key for a compacted segment (v1 monolithic).
pub fn compacted_segment_r2_key(
    prefix: &str,
    partition_id: u32,
    max_seq: u64,
) -> String {
    format!(
        "{prefix}log/compacted/{partition_id}/compacted_{max_seq:020}.arrow"
    )
}

/// Build R2 key for a per-label compacted segment (v2).
pub fn label_compacted_r2_key(prefix: &str, partition_id: u32, label: &str) -> String {
    format!("{prefix}log/compacted/{partition_id}/label/{label}.arrow")
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Build a CompactionManifest from a CompactionResult (v1 compat).
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
        label_segments: HashMap::new(),
    }
}

/// Build a v2 CompactionManifest from per-label results.
pub fn build_manifest_v2(
    partition_id: u32,
    prefix: &str,
    global_max_seq: u64,
    label_results: &[LabelCompactionResult],
    existing: &HashMap<String, LabelSegmentState>,
) -> CompactionManifest {
    let mut label_segments = existing.clone();
    let mut total_entries = 0usize;
    let mut total_bytes = 0usize;

    for lr in label_results {
        let key = label_compacted_r2_key(prefix, partition_id, &lr.label);
        let checksum = blake3_hex(&lr.data);
        label_segments.insert(lr.label.clone(), LabelSegmentState {
            key,
            max_seq: lr.max_seq,
            entry_count: lr.entry_count,
            segment_bytes: lr.data.len(),
            blake3_hex: checksum,
        });
    }

    let mut all_labels: Vec<String> = label_segments.keys().cloned().collect();
    all_labels.sort();

    for state in label_segments.values() {
        total_entries += state.entry_count;
        total_bytes += state.segment_bytes;
    }

    CompactionManifest {
        partition_id,
        version: 2,
        compacted_segment_key: String::new(),
        compacted_seq: global_max_seq,
        entry_count: total_entries,
        labels: all_labels,
        created_at_ms: now_ms(),
        segment_bytes: total_bytes,
        label_segments,
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
            label_segments: HashMap::new(),
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: CompactionManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.compacted_seq, 100);
        assert_eq!(parsed.entry_count, 50);
        assert!(parsed.label_segments.is_empty());
    }

    #[test]
    fn test_manifest_v1_deserialize_compat() {
        // v1 JSON without label_segments field — should deserialize with default
        let json = r#"{"partition_id":0,"version":1,"compacted_segment_key":"k","compacted_seq":50,"entry_count":10,"labels":["Post"],"created_at_ms":0,"segment_bytes":100}"#;
        let parsed: CompactionManifest = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.version, 1);
        assert!(parsed.label_segments.is_empty());
    }

    #[test]
    fn test_manifest_v2_serde_roundtrip() {
        let mut label_segments = HashMap::new();
        label_segments.insert("Post".to_string(), LabelSegmentState {
            key: "log/compacted/0/label/Post.arrow".to_string(),
            max_seq: 100,
            entry_count: 30,
            segment_bytes: 2048,
            blake3_hex: "abc123".to_string(),
        });
        let manifest = CompactionManifest {
            partition_id: 0,
            version: 2,
            compacted_segment_key: String::new(),
            compacted_seq: 100,
            entry_count: 30,
            labels: vec!["Post".to_string()],
            created_at_ms: 1234567890,
            segment_bytes: 2048,
            label_segments,
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: CompactionManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.version, 2);
        assert_eq!(parsed.label_segments.len(), 1);
        assert_eq!(parsed.label_segments["Post"].entry_count, 30);
    }

    #[test]
    fn test_compact_entries_by_label() {
        let batch = vec![
            make(1, "Post", "pk_1", WalOp::Upsert, "v1"),
            make(2, "Like", "pk_2", WalOp::Upsert, "v1"),
            make(3, "Post", "pk_1", WalOp::Upsert, "v2"),
            make(4, "Follow", "pk_3", WalOp::Upsert, "v1"),
            make_delete(5, "Like", "pk_2"),
        ];
        let result = compact_entries_by_label(&[&batch]);
        assert_eq!(result.len(), 2, "Like should be removed by delete");
        assert!(result.contains_key("Post"));
        assert!(result.contains_key("Follow"));
        assert!(!result.contains_key("Like"));
        assert_eq!(result["Post"].len(), 1);
        assert_eq!(result["Post"][0].seq, 3);
    }

    #[test]
    fn test_compact_segments_by_label_filters_dirty() {
        let entries = vec![
            make(1, "Post", "pk_1", WalOp::Upsert, "v1"),
            make(2, "Like", "pk_2", WalOp::Upsert, "v1"),
            make(3, "Follow", "pk_3", WalOp::Upsert, "v1"),
        ];
        let seg = crate::arrow_wal::serialize_segment_arrow(&entries).unwrap();

        let dirty: HashSet<String> = ["Post".to_string()].into_iter().collect();
        let results = compact_segments_by_label(&[("seg.arrow", &seg)], &dirty).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].label, "Post");
        assert_eq!(results[0].entry_count, 1);
    }

    #[test]
    fn test_label_compacted_r2_key() {
        assert_eq!(
            label_compacted_r2_key("yata/", 0, "Post"),
            "yata/log/compacted/0/label/Post.arrow"
        );
    }

    #[test]
    fn test_build_manifest_v2() {
        let results = vec![
            LabelCompactionResult {
                label: "Post".to_string(),
                data: Bytes::from(vec![1, 2]),
                max_seq: 10,
                entry_count: 5,
            },
        ];
        let existing = HashMap::new();
        let manifest = build_manifest_v2(0, "yata/", 10, &results, &existing);
        assert_eq!(manifest.version, 2);
        assert!(manifest.compacted_segment_key.is_empty());
        assert_eq!(manifest.label_segments.len(), 1);
        assert_eq!(manifest.label_segments["Post"].entry_count, 5);
        assert_eq!(manifest.entry_count, 5);
        assert_eq!(manifest.segment_bytes, 2);
    }

    #[test]
    fn test_build_manifest_v2_merges_existing() {
        let mut existing = HashMap::new();
        existing.insert("Like".to_string(), LabelSegmentState {
            key: "yata/log/compacted/0/label/Like.arrow".to_string(),
            max_seq: 5,
            entry_count: 3,
            segment_bytes: 100,
            blake3_hex: String::new(),
        });
        let results = vec![
            LabelCompactionResult {
                label: "Post".to_string(),
                data: Bytes::from(vec![1, 2]),
                max_seq: 10,
                entry_count: 5,
            },
        ];
        let manifest = build_manifest_v2(0, "yata/", 10, &results, &existing);
        assert_eq!(manifest.label_segments.len(), 2);
        assert_eq!(manifest.labels, vec!["Like", "Post"]);
        assert_eq!(manifest.entry_count, 8); // 3 + 5
    }

    #[test]
    fn test_blake3_hex_deterministic() {
        let data = b"hello yata";
        let h1 = blake3_hex(data);
        let h2 = blake3_hex(data);
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64); // 256-bit = 64 hex chars
    }

    #[test]
    fn test_verify_blake3_match() {
        let data = b"test segment data";
        let hash = blake3_hex(data);
        assert!(verify_blake3(data, &hash, "Post").is_ok());
    }

    #[test]
    fn test_verify_blake3_mismatch() {
        let data = b"test segment data";
        let result = verify_blake3(data, "0000000000000000000000000000000000000000000000000000000000000000", "Post");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("checksum mismatch"));
    }

    #[test]
    fn test_verify_blake3_empty_skips() {
        let data = b"any data";
        assert!(verify_blake3(data, "", "Legacy").is_ok());
    }

    #[test]
    fn test_build_manifest_v2_includes_blake3() {
        let entries = vec![make(1, "Post", "pk_1", WalOp::Upsert, "v1")];
        let data = crate::arrow_wal::serialize_segment_arrow(&entries).unwrap();
        let expected_hash = blake3_hex(&data);

        let results = vec![LabelCompactionResult {
            label: "Post".to_string(),
            data,
            max_seq: 1,
            entry_count: 1,
        }];
        let manifest = build_manifest_v2(0, "yata/", 1, &results, &HashMap::new());
        assert_eq!(manifest.label_segments["Post"].blake3_hex, expected_hash);
        assert!(!expected_hash.is_empty());
    }

    #[test]
    fn test_manifest_v2_blake3_serde_backward_compat() {
        // v2 JSON without blake3_hex field — should deserialize with default empty string
        let json = r#"{"key":"k","max_seq":50,"entry_count":10,"segment_bytes":100}"#;
        let parsed: LabelSegmentState = serde_json::from_str(json).unwrap();
        assert!(parsed.blake3_hex.is_empty());
    }
}
