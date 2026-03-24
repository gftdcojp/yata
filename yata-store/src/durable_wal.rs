//! DurableWal: rkyv-based write-ahead log for graph mutations.
//!
//! Disk format per entry: `[4B LE length][rkyv bytes][4B CRC32]`
//! fsync after each append/batch for durability.

use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use yata_core::{DurablePropValue, DurableWalEntry, DurableWalOp};
use yata_grin::{Mutable, PropValue, Property, Topology};

use crate::MutableCsrStore;

const WAL_FILE_NAME: &str = "graph.wal";

pub struct DurableWal {
    file: BufWriter<std::fs::File>,
    path: PathBuf,
    lsn: u64,
    committed_lsn: u64,
}

impl DurableWal {
    /// Return the WAL file path (for diagnostics).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Open or create a WAL file in `dir`.
    /// Scans existing entries to find the last LSN.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let path = dir.join(WAL_FILE_NAME);

        // Scan existing entries to find last LSN
        let committed_path = dir.join("committed_lsn");
        let persisted_committed = if committed_path.exists() {
            std::fs::read_to_string(&committed_path)
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap_or(0)
        } else {
            0
        };
        let (lsn, committed_lsn) = if path.exists() {
            let entries = Self::scan_file(&path, 0)?;
            let max_lsn = entries.iter().map(|e| e.lsn).max().unwrap_or(0);
            (max_lsn, persisted_committed)
        } else {
            (0, persisted_committed)
        };

        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let file = BufWriter::new(file);

        Ok(Self {
            file,
            path,
            lsn,
            committed_lsn,
        })
    }

    /// Append a single operation. fsync'd before returning.
    /// Returns the assigned LSN.
    pub fn append(&mut self, op: DurableWalOp) -> io::Result<u64> {
        self.lsn += 1;
        let entry = DurableWalEntry {
            lsn: self.lsn,
            term: 0,
            ts_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            shard_id: 0,
            op,
        };
        self.write_entry(&entry)?;
        self.sync()?;
        Ok(self.lsn)
    }

    /// Append multiple operations with a single fsync.
    /// Returns the LSN of the last entry.
    pub fn append_batch(&mut self, ops: Vec<DurableWalOp>) -> io::Result<u64> {
        if ops.is_empty() {
            return Ok(self.lsn);
        }
        let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        for op in ops {
            self.lsn += 1;
            let entry = DurableWalEntry {
                lsn: self.lsn,
                term: 0,
                ts_ns,
                shard_id: 0,
                op,
            };
            self.write_entry(&entry)?;
        }
        self.sync()?;
        Ok(self.lsn)
    }

    /// Mark entries up to `lsn` as committed (persisted to R2).
    /// Persists committed_lsn to disk so it survives Container restart.
    pub fn mark_committed(&mut self, lsn: u64) {
        self.committed_lsn = lsn;
        // Persist committed_lsn to a sidecar file for crash recovery
        let committed_path = self.path.parent().unwrap_or(Path::new(".")).join("committed_lsn");
        let _ = std::fs::write(&committed_path, lsn.to_string());
    }

    /// Current LSN.
    pub fn lsn(&self) -> u64 {
        self.lsn
    }

    /// Current committed LSN.
    pub fn committed_lsn(&self) -> u64 {
        self.committed_lsn
    }

    /// Truncate the WAL: rewrite keeping only entries > committed_lsn.
    pub fn truncate(&mut self) -> io::Result<()> {
        let remaining = Self::scan_file(&self.path, self.committed_lsn)?;

        // Write remaining entries to a temp file, then rename
        let tmp_path = self.path.with_extension("wal.tmp");
        {
            let tmp_file = std::fs::File::create(&tmp_path)?;
            let mut tmp_writer = BufWriter::new(tmp_file);
            for entry in &remaining {
                Self::write_entry_to(&mut tmp_writer, entry)?;
            }
            tmp_writer.flush()?;
            tmp_writer.get_ref().sync_all()?;
        }

        std::fs::rename(&tmp_path, &self.path)?;

        // Reopen for appending
        let file = std::fs::OpenOptions::new().append(true).open(&self.path)?;
        self.file = BufWriter::new(file);

        tracing::debug!(
            committed = self.committed_lsn,
            remaining = remaining.len(),
            "WAL truncated"
        );
        Ok(())
    }

    /// Scan a WAL file for entries after `after_lsn`.
    /// Verifies CRC for each entry; skips corrupt entries.
    pub fn scan_after(path: impl AsRef<Path>, after_lsn: u64) -> io::Result<Vec<DurableWalEntry>> {
        Self::scan_file(path.as_ref(), after_lsn)
    }

    /// Replay WAL entries into a MutableCsrStore.
    pub fn replay_into(entries: &[DurableWalEntry], csr: &mut MutableCsrStore) {
        for entry in entries {
            match &entry.op {
                DurableWalOp::CreateVertex {
                    node_id,
                    global_vid,
                    labels,
                    props,
                } => {
                    let mut prop_pairs: Vec<(&str, PropValue)> =
                        vec![("_id", PropValue::Str(node_id.clone()))];
                    let converted: Vec<(String, PropValue)> = props
                        .iter()
                        .map(|(k, v)| (k.clone(), durable_to_prop(v)))
                        .collect();
                    for (k, v) in &converted {
                        prop_pairs.push((k.as_str(), v.clone()));
                    }
                    let fallback_label = "_default".to_string();
                    let effective_labels = if labels.is_empty() {
                        std::slice::from_ref(&fallback_label)
                    } else {
                        labels.as_slice()
                    };
                    csr.add_vertex_with_labels_and_optional_global_id(
                        *global_vid,
                        effective_labels,
                        &prop_pairs,
                    );
                }
                DurableWalOp::CreateEdge {
                    edge_id: _edge_id,
                    global_eid,
                    src,
                    dst,
                    rel_type,
                    props,
                } => {
                    if let (Some(src_vid), Some(dst_vid)) =
                        (find_vid_by_id(csr, src), find_vid_by_id(csr, dst))
                    {
                        let converted: Vec<(String, PropValue)> = props
                            .iter()
                            .map(|(k, v)| (k.clone(), durable_to_prop(v)))
                            .collect();
                        let prop_pairs: Vec<(&str, PropValue)> = converted
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.clone()))
                            .collect();
                        csr.add_edge_with_optional_global_id(
                            *global_eid,
                            src_vid,
                            dst_vid,
                            rel_type,
                            &prop_pairs,
                        );
                    }
                }
                DurableWalOp::DeleteVertex { node_id } => {
                    if let Some(vid) = find_vid_by_id(csr, node_id) {
                        csr.delete_vertex(vid);
                    }
                }
                DurableWalOp::DeleteEdge { edge_id: _ } => {
                    // Edge deletion by string ID requires scan — skip for now
                }
                DurableWalOp::SetProp {
                    node_id,
                    key,
                    value,
                } => {
                    if let Some(vid) = find_vid_by_id(csr, node_id) {
                        csr.set_vertex_prop(vid, key, durable_to_prop(value));
                    }
                }
                DurableWalOp::Commit { .. } => {}
            }
        }
    }

    // ── Internal ──

    fn write_entry(&mut self, entry: &DurableWalEntry) -> io::Result<()> {
        Self::write_entry_to(&mut self.file, entry)
    }

    fn write_entry_to(
        writer: &mut BufWriter<std::fs::File>,
        entry: &DurableWalEntry,
    ) -> io::Result<()> {
        let rkyv_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(entry)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let len = rkyv_bytes.len() as u32;
        let crc = crc32fast::hash(&rkyv_bytes);

        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(&rkyv_bytes)?;
        writer.write_all(&crc.to_le_bytes())?;
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.flush()?;
        // Use sync_data() on Linux (fdatasync ~10-50µs) instead of
        // sync_all() (fsync ~5-10ms on macOS due to F_FULLFSYNC).
        // Data durability is sufficient for WAL — metadata (mtime/size)
        // sync is not required for crash recovery.
        #[cfg(target_os = "linux")]
        {
            self.file.get_ref().sync_data()
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.file.get_ref().sync_all()
        }
    }

    fn scan_file(path: &Path, after_lsn: u64) -> io::Result<Vec<DurableWalEntry>> {
        let mut entries = Vec::new();
        let mut file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(entries),
            Err(e) => return Err(e),
        };

        let file_len = file.metadata()?.len();
        let mut pos = 0u64;

        while pos + 8 <= file_len {
            // Read length
            let mut len_buf = [0u8; 4];
            file.seek(SeekFrom::Start(pos))?;
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            // Bounds check
            if pos + 4 + len as u64 + 4 > file_len {
                tracing::warn!(pos, len, "WAL: truncated entry, stopping scan");
                break;
            }

            // Read rkyv bytes
            let mut rkyv_buf = vec![0u8; len];
            if file.read_exact(&mut rkyv_buf).is_err() {
                break;
            }

            // Read and verify CRC
            let mut crc_buf = [0u8; 4];
            if file.read_exact(&mut crc_buf).is_err() {
                break;
            }
            let stored_crc = u32::from_le_bytes(crc_buf);
            let computed_crc = crc32fast::hash(&rkyv_buf);
            if stored_crc != computed_crc {
                tracing::warn!(pos, "WAL: CRC mismatch, stopping scan");
                break;
            }

            // Deserialize
            match rkyv::from_bytes::<DurableWalEntry, rkyv::rancor::Error>(&rkyv_buf) {
                Ok(entry) => {
                    if entry.lsn > after_lsn {
                        entries.push(entry);
                    }
                }
                Err(e) => {
                    tracing::warn!(pos, error = %e, "WAL: deserialize failed, stopping scan");
                    break;
                }
            }

            pos += 4 + len as u64 + 4;
        }

        Ok(entries)
    }
}

/// Convert DurablePropValue to PropValue.
fn durable_to_prop(v: &DurablePropValue) -> PropValue {
    match v {
        DurablePropValue::Null => PropValue::Null,
        DurablePropValue::Bool(b) => PropValue::Bool(*b),
        DurablePropValue::Int(i) => PropValue::Int(*i),
        DurablePropValue::Float(f) => PropValue::Float(*f),
        DurablePropValue::Str(s) => PropValue::Str(s.clone()),
    }
}

/// Find a vertex ID by its `_id` string property.
fn find_vid_by_id(csr: &MutableCsrStore, node_id: &str) -> Option<u32> {
    for vid in 0..csr.vertex_count() as u32 {
        if let Some(PropValue::Str(ref id)) = csr.vertex_prop(vid, "_id") {
            if id == node_id {
                return Some(vid);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_core::{DurablePropValue, GlobalVid, LocalVid};

    #[test]
    fn test_append_and_scan() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = DurableWal::open(dir.path()).unwrap();

        let lsn1 = wal
            .append(DurableWalOp::CreateVertex {
                node_id: "v1".into(),
                global_vid: None,
                labels: vec!["Person".into()],
                props: vec![("name".into(), DurablePropValue::Str("Alice".into()))],
            })
            .unwrap();
        assert_eq!(lsn1, 1);

        let lsn2 = wal
            .append(DurableWalOp::CreateVertex {
                node_id: "v2".into(),
                global_vid: None,
                labels: vec!["Person".into()],
                props: vec![("name".into(), DurablePropValue::Str("Bob".into()))],
            })
            .unwrap();
        assert_eq!(lsn2, 2);

        let entries = DurableWal::scan_after(dir.path().join(WAL_FILE_NAME), 0).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].lsn, 1);
        assert_eq!(entries[1].lsn, 2);
    }

    #[test]
    fn test_append_batch() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = DurableWal::open(dir.path()).unwrap();

        let ops = vec![
            DurableWalOp::CreateVertex {
                node_id: "v1".into(),
                global_vid: None,
                labels: vec!["A".into()],
                props: vec![],
            },
            DurableWalOp::CreateVertex {
                node_id: "v2".into(),
                global_vid: None,
                labels: vec!["B".into()],
                props: vec![],
            },
            DurableWalOp::CreateEdge {
                edge_id: "e1".into(),
                global_eid: Some(501u64.into()),
                src: "v1".into(),
                dst: "v2".into(),
                rel_type: "KNOWS".into(),
                props: vec![],
            },
        ];
        let lsn = wal.append_batch(ops).unwrap();
        assert_eq!(lsn, 3);

        let entries = DurableWal::scan_after(dir.path().join(WAL_FILE_NAME), 0).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_scan_after_filter() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = DurableWal::open(dir.path()).unwrap();

        for i in 0..5 {
            wal.append(DurableWalOp::CreateVertex {
                node_id: format!("v{i}"),
                global_vid: None,
                labels: vec![],
                props: vec![],
            })
            .unwrap();
        }

        let entries = DurableWal::scan_after(dir.path().join(WAL_FILE_NAME), 3).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].lsn, 4);
        assert_eq!(entries[1].lsn, 5);
    }

    #[test]
    fn test_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = DurableWal::open(dir.path()).unwrap();

        for i in 0..5 {
            wal.append(DurableWalOp::CreateVertex {
                node_id: format!("v{i}"),
                global_vid: None,
                labels: vec![],
                props: vec![],
            })
            .unwrap();
        }

        wal.mark_committed(3);
        wal.truncate().unwrap();

        let entries = DurableWal::scan_after(dir.path().join(WAL_FILE_NAME), 0).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].lsn, 4);
        assert_eq!(entries[1].lsn, 5);
    }

    #[test]
    fn test_reopen_continues_lsn() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut wal = DurableWal::open(dir.path()).unwrap();
            wal.append(DurableWalOp::Commit {
                message: "first".into(),
            })
            .unwrap();
            wal.append(DurableWalOp::Commit {
                message: "second".into(),
            })
            .unwrap();
        }
        // Reopen
        let mut wal = DurableWal::open(dir.path()).unwrap();
        assert_eq!(wal.lsn(), 2);
        let lsn = wal
            .append(DurableWalOp::Commit {
                message: "third".into(),
            })
            .unwrap();
        assert_eq!(lsn, 3);
    }

    #[test]
    fn test_corrupt_crc_stops_scan() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join(WAL_FILE_NAME);
        {
            let mut wal = DurableWal::open(dir.path()).unwrap();
            wal.append(DurableWalOp::Commit {
                message: "ok".into(),
            })
            .unwrap();
            wal.append(DurableWalOp::Commit {
                message: "will be corrupted".into(),
            })
            .unwrap();
        }
        // Corrupt last 4 bytes (CRC of second entry)
        let mut data = std::fs::read(&wal_path).unwrap();
        let len = data.len();
        data[len - 1] ^= 0xFF;
        std::fs::write(&wal_path, &data).unwrap();

        let entries = DurableWal::scan_after(&wal_path, 0).unwrap();
        assert_eq!(
            entries.len(),
            1,
            "corrupt CRC should stop scan after first valid entry"
        );
    }

    #[test]
    fn test_replay_into_csr() {
        let entries = vec![
            DurableWalEntry {
                lsn: 1,
                term: 0,
                ts_ns: 0,
                shard_id: 0,
                op: DurableWalOp::CreateVertex {
                    node_id: "alice".into(),
                    global_vid: Some(101u64.into()),
                    labels: vec!["Person".into()],
                    props: vec![("name".into(), DurablePropValue::Str("Alice".into()))],
                },
            },
            DurableWalEntry {
                lsn: 2,
                term: 0,
                ts_ns: 0,
                shard_id: 0,
                op: DurableWalOp::CreateVertex {
                    node_id: "bob".into(),
                    global_vid: None,
                    labels: vec!["Person".into()],
                    props: vec![("name".into(), DurablePropValue::Str("Bob".into()))],
                },
            },
            DurableWalEntry {
                lsn: 3,
                term: 0,
                ts_ns: 0,
                shard_id: 0,
                op: DurableWalOp::CreateEdge {
                    edge_id: "e1".into(),
                    global_eid: Some(501u64.into()),
                    src: "alice".into(),
                    dst: "bob".into(),
                    rel_type: "KNOWS".into(),
                    props: vec![],
                },
            },
        ];

        let mut csr = MutableCsrStore::new();
        DurableWal::replay_into(&entries, &mut csr);

        assert_eq!(csr.vertex_count(), 2);

        // Verify props were set
        let alice_vid = find_vid_by_id(&csr, "alice").unwrap();
        assert_eq!(
            csr.local_vid(GlobalVid::from(101u64)),
            Some(LocalVid::from(alice_vid))
        );
        assert_eq!(
            csr.vertex_prop(alice_vid, "name"),
            Some(PropValue::Str("Alice".into()))
        );
    }

    #[test]
    fn test_empty_batch() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = DurableWal::open(dir.path()).unwrap();
        let lsn = wal.append_batch(vec![]).unwrap();
        assert_eq!(lsn, 0);
    }

    #[test]
    fn test_utf8_japanese_props_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = DurableWal::open(dir.path()).unwrap();

        let japanese_name = "田中太郎";
        let japanese_desc = "東京都港区にある会社です。";

        wal.append(DurableWalOp::CreateVertex {
            node_id: "jp1".into(),
            global_vid: None,
            labels: vec!["会社".into()],
            props: vec![
                ("名前".into(), DurablePropValue::Str(japanese_name.into())),
                ("説明".into(), DurablePropValue::Str(japanese_desc.into())),
            ],
        })
        .unwrap();

        wal.append(DurableWalOp::SetProp {
            node_id: "jp1".into(),
            key: "住所".into(),
            value: DurablePropValue::Str("東京都渋谷区神宮前1-2-3".into()),
        })
        .unwrap();

        // Scan and verify UTF-8 integrity
        let entries = DurableWal::scan_after(dir.path().join(WAL_FILE_NAME), 0).unwrap();
        assert_eq!(entries.len(), 2);

        match &entries[0].op {
            DurableWalOp::CreateVertex {
                node_id,
                global_vid,
                labels,
                props,
            } => {
                assert_eq!(node_id, "jp1");
                assert_eq!(*global_vid, None);
                assert_eq!(labels[0], "会社");
                assert_eq!(props[0].0, "名前");
                assert_eq!(props[0].1, DurablePropValue::Str(japanese_name.into()));
                assert_eq!(props[1].0, "説明");
                assert_eq!(props[1].1, DurablePropValue::Str(japanese_desc.into()));
            }
            other => panic!("expected CreateVertex, got {:?}", other),
        }

        match &entries[1].op {
            DurableWalOp::SetProp {
                node_id,
                key,
                value,
            } => {
                assert_eq!(node_id, "jp1");
                assert_eq!(key, "住所");
                assert_eq!(
                    *value,
                    DurablePropValue::Str("東京都渋谷区神宮前1-2-3".into())
                );
            }
            other => panic!("expected SetProp, got {:?}", other),
        }

        // Replay into CSR and verify
        let mut csr = MutableCsrStore::new();
        DurableWal::replay_into(&entries, &mut csr);
        assert_eq!(csr.vertex_count(), 1);

        let vid = find_vid_by_id(&csr, "jp1").unwrap();
        assert_eq!(
            csr.vertex_prop(vid, "名前"),
            Some(PropValue::Str(japanese_name.into()))
        );
        assert_eq!(
            csr.vertex_prop(vid, "説明"),
            Some(PropValue::Str(japanese_desc.into()))
        );
        assert_eq!(
            csr.vertex_prop(vid, "住所"),
            Some(PropValue::Str("東京都渋谷区神宮前1-2-3".into()))
        );
    }

    #[test]
    fn test_utf8_emoji_props_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = DurableWal::open(dir.path()).unwrap();

        let emoji_val = "Hello 🌸🎌🗾 World 🇯🇵";
        let mixed_emoji = "Status: ✅ Done! 🎉🚀💯";

        wal.append(DurableWalOp::CreateVertex {
            node_id: "emoji1".into(),
            global_vid: None,
            labels: vec!["Status".into()],
            props: vec![
                ("message".into(), DurablePropValue::Str(emoji_val.into())),
                ("status".into(), DurablePropValue::Str(mixed_emoji.into())),
            ],
        })
        .unwrap();

        // Reopen WAL to test persistence across open/close
        drop(wal);
        let entries = DurableWal::scan_after(dir.path().join(WAL_FILE_NAME), 0).unwrap();
        assert_eq!(entries.len(), 1);

        match &entries[0].op {
            DurableWalOp::CreateVertex { props, .. } => {
                assert_eq!(props[0].1, DurablePropValue::Str(emoji_val.into()));
                assert_eq!(props[1].1, DurablePropValue::Str(mixed_emoji.into()));
            }
            other => panic!("expected CreateVertex, got {:?}", other),
        }

        // Replay and verify
        let mut csr = MutableCsrStore::new();
        DurableWal::replay_into(&entries, &mut csr);

        let vid = find_vid_by_id(&csr, "emoji1").unwrap();
        assert_eq!(
            csr.vertex_prop(vid, "message"),
            Some(PropValue::Str(emoji_val.into()))
        );
        assert_eq!(
            csr.vertex_prop(vid, "status"),
            Some(PropValue::Str(mixed_emoji.into()))
        );
    }

    #[test]
    fn test_utf8_large_japanese_text_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = DurableWal::open(dir.path()).unwrap();

        // Build a >1KB Japanese text string
        // Each iteration: "段落N: 吾輩は猫である。名前はまだ無い。どこで生まれたかとんと見当がつかぬ。" (~100+ bytes in UTF-8)
        let mut large_text = String::new();
        for i in 0..20 {
            large_text.push_str(&format!(
                "段落{}: 吾輩は猫である。名前はまだ無い。どこで生まれたかとんと見当がつかぬ。\n",
                i
            ));
        }
        assert!(
            large_text.len() > 1024,
            "test text must be >1KB, got {} bytes",
            large_text.len()
        );
        let original_bytes = large_text.as_bytes().to_vec();

        wal.append(DurableWalOp::CreateVertex {
            node_id: "novel1".into(),
            global_vid: None,
            labels: vec!["文書".into()],
            props: vec![("本文".into(), DurablePropValue::Str(large_text.clone()))],
        })
        .unwrap();

        // Scan back and verify byte-perfect roundtrip
        let entries = DurableWal::scan_after(dir.path().join(WAL_FILE_NAME), 0).unwrap();
        assert_eq!(entries.len(), 1);

        match &entries[0].op {
            DurableWalOp::CreateVertex { props, .. } => {
                if let DurablePropValue::Str(recovered) = &props[0].1 {
                    assert_eq!(
                        recovered.as_bytes(),
                        original_bytes.as_slice(),
                        "byte-perfect roundtrip failed for large Japanese text"
                    );
                    assert_eq!(recovered.len(), large_text.len());
                } else {
                    panic!("expected Str prop value");
                }
            }
            other => panic!("expected CreateVertex, got {:?}", other),
        }

        // Replay into CSR and verify byte-perfect
        let mut csr = MutableCsrStore::new();
        DurableWal::replay_into(&entries, &mut csr);

        let vid = find_vid_by_id(&csr, "novel1").unwrap();
        match csr.vertex_prop(vid, "本文") {
            Some(PropValue::Str(ref s)) => {
                assert_eq!(
                    s.as_bytes(),
                    original_bytes.as_slice(),
                    "CSR replay byte-perfect roundtrip failed"
                );
            }
            other => panic!("expected Str prop, got {:?}", other),
        }
    }
}
