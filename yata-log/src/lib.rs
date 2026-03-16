#![allow(dead_code)]

pub use local_log::LocalLog;
pub use payload_store::PayloadStore;

// ---- config -----------------------------------------------------------------

pub mod config {
    /// Configuration for log segment rotation and compaction.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct LogConfig {
        /// Max bytes per segment file before rotation (default: 64 MB).
        pub max_segment_bytes: u64,
        /// Number of segments to retain per stream (default: 8).
        pub retention_count: usize,
    }

    impl Default for LogConfig {
        fn default() -> Self {
            Self {
                max_segment_bytes: 64 * 1024 * 1024,
                retention_count: 8,
            }
        }
    }
}

// ---- segment ----------------------------------------------------------------

pub mod segment {
    use std::path::{Path, PathBuf};
    use yata_core::{Sequence, StreamId};

    /// A single segment file: <base_dir>/<stream_id>/<seq_start>.log
    /// Format per entry: [4-byte len LE][CBOR-encoded LogEntry][4-byte crc32]
    pub struct Segment {
        pub path: PathBuf,
        pub stream: StreamId,
        pub start_seq: Sequence,
    }

    impl Segment {
        pub fn create(
            base_dir: &Path,
            stream: &StreamId,
            start_seq: Sequence,
        ) -> tokio::io::Result<Self> {
            let stream_dir = base_dir.join(sanitize_stream_id(&stream.0));
            let path = stream_dir.join(format!("{:020}.log", start_seq.0));
            Ok(Self {
                path,
                stream: stream.clone(),
                start_seq,
            })
        }

        pub fn open(path: PathBuf) -> tokio::io::Result<Self> {
            let file_stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| tokio::io::Error::new(tokio::io::ErrorKind::InvalidInput, "invalid segment filename"))?;
            let start_seq = file_stem
                .parse::<u64>()
                .map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::InvalidInput, e))?;
            let stream_dir = path
                .parent()
                .ok_or_else(|| tokio::io::Error::new(tokio::io::ErrorKind::InvalidInput, "no parent dir"))?;
            let stream_id = stream_dir
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_owned();
            Ok(Self {
                path,
                stream: StreamId(stream_id),
                start_seq: Sequence(start_seq),
            })
        }
    }

    pub fn sanitize_stream_id(id: &str) -> String {
        id.replace('/', "_").replace(':', "_")
    }
}

// ---- local_log --------------------------------------------------------------

pub mod local_log {
    use super::config::LogConfig;
    use super::segment::{sanitize_stream_id, Segment};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::pin::Pin;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Mutex;
    use yata_core::{
        Ack, AppendLog, Blake3Hash, Envelope, LogEntry, MessageId, PayloadKind, PayloadRef,
        PublishRequest, Result, Sequence, StreamId, Subject, YataError,
    };

    struct StreamState {
        last_seq: u64,
        writer: tokio::fs::File,
        writer_path: PathBuf,
        /// Bytes written to current segment file.
        segment_bytes: u64,
    }

    pub struct LocalLog {
        base_dir: PathBuf,
        streams: Mutex<HashMap<String, StreamState>>,
        config: LogConfig,
    }

    impl LocalLog {
        pub async fn new(base_dir: impl Into<PathBuf>) -> Result<Self> {
            Self::with_config(base_dir, LogConfig::default()).await
        }

        pub async fn with_config(base_dir: impl Into<PathBuf>, config: LogConfig) -> Result<Self> {
            let base_dir = base_dir.into();
            tokio::fs::create_dir_all(&base_dir).await?;
            Ok(Self {
                base_dir,
                streams: Mutex::new(HashMap::new()),
                config,
            })
        }

        /// Recover last_seq for all streams by scanning segment files.
        pub async fn recover(&self) -> Result<()> {
            let mut read_dir = match tokio::fs::read_dir(&self.base_dir).await {
                Ok(rd) => rd,
                Err(_) => return Ok(()),
            };
            let mut streams = self.streams.lock().await;
            while let Ok(Some(entry)) = read_dir.next_entry().await {
                let stream_dir = entry.path();
                if !stream_dir.is_dir() {
                    continue;
                }
                let stream_id = stream_dir
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_owned();
                let mut seg_files: Vec<PathBuf> = Vec::new();
                let mut sd = match tokio::fs::read_dir(&stream_dir).await {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                while let Ok(Some(f)) = sd.next_entry().await {
                    let p = f.path();
                    if p.extension().and_then(|e| e.to_str()) == Some("log") {
                        seg_files.push(p);
                    }
                }
                seg_files.sort();
                if let Some(latest) = seg_files.last() {
                    let last_seq = scan_last_seq(latest).await.unwrap_or(0);
                    let seg_bytes = tokio::fs::metadata(latest)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0);
                    let writer = tokio::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(latest)
                        .await?;
                    streams.insert(
                        stream_id,
                        StreamState {
                            last_seq,
                            writer,
                            writer_path: latest.clone(),
                            segment_bytes: seg_bytes,
                        },
                    );
                }
            }
            Ok(())
        }

        /// Compact old segments: retain only the latest `retention_count` per stream.
        /// Returns the number of segment files removed.
        pub async fn compact(&self) -> Result<usize> {
            let mut removed = 0usize;
            let mut read_dir = match tokio::fs::read_dir(&self.base_dir).await {
                Ok(rd) => rd,
                Err(_) => return Ok(0),
            };

            while let Ok(Some(entry)) = read_dir.next_entry().await {
                let stream_dir = entry.path();
                if !stream_dir.is_dir() {
                    continue;
                }

                let mut seg_files: Vec<PathBuf> = Vec::new();
                let mut sd = match tokio::fs::read_dir(&stream_dir).await {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                while let Ok(Some(f)) = sd.next_entry().await {
                    let p = f.path();
                    if p.extension().and_then(|e| e.to_str()) == Some("log") {
                        seg_files.push(p);
                    }
                }
                seg_files.sort();

                if seg_files.len() > self.config.retention_count {
                    let to_remove = seg_files.len() - self.config.retention_count;
                    for path in &seg_files[..to_remove] {
                        if let Err(e) = tokio::fs::remove_file(path).await {
                            tracing::warn!("compact: failed to remove {:?}: {}", path, e);
                        } else {
                            removed += 1;
                        }
                    }
                }
            }

            if removed > 0 {
                tracing::info!(removed, "log compaction: removed old segments");
            }
            Ok(removed)
        }

        async fn ensure_stream<'a>(
            streams: &'a mut HashMap<String, StreamState>,
            base_dir: &Path,
            stream: &StreamId,
        ) -> Result<&'a mut StreamState> {
            let key = sanitize_stream_id(&stream.0);
            if !streams.contains_key(&key) {
                let stream_dir = base_dir.join(&key);
                tokio::fs::create_dir_all(&stream_dir).await?;
                let seg = Segment::create(base_dir, stream, Sequence(1))?;
                let writer = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&seg.path)
                    .await?;
                streams.insert(
                    key.clone(),
                    StreamState {
                        last_seq: 0,
                        writer,
                        writer_path: seg.path,
                        segment_bytes: 0,
                    },
                );
            }
            Ok(streams.get_mut(&key).unwrap())
        }

        /// Rotate the current segment: close writer, create new segment file.
        async fn rotate_segment(
            state: &mut StreamState,
            base_dir: &Path,
            stream: &StreamId,
        ) -> Result<()> {
            state.writer.flush().await?;
            let next_start = Sequence(state.last_seq + 1);
            let seg = Segment::create(base_dir, stream, next_start)?;
            if let Some(parent) = seg.path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let writer = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&seg.path)
                .await?;
            state.writer = writer;
            state.writer_path = seg.path;
            state.segment_bytes = 0;
            tracing::debug!(
                stream = %stream,
                seq = next_start.0,
                "segment rotated"
            );
            Ok(())
        }
    }

    #[async_trait]
    impl AppendLog for LocalLog {
        async fn append(&self, req: PublishRequest) -> Result<Ack> {
            let mut streams = self.streams.lock().await;
            let state =
                Self::ensure_stream(&mut streams, &self.base_dir, &req.stream).await?;

            // Sequence conflict check
            if let Some(expected) = req.expected_last_seq {
                if state.last_seq != expected.0 {
                    return Err(YataError::SeqConflict {
                        expected: Some(expected.0),
                        actual: state.last_seq,
                    });
                }
            }

            // Check if rotation needed before write
            if state.segment_bytes >= self.config.max_segment_bytes {
                Self::rotate_segment(state, &self.base_dir, &req.stream).await?;
            }

            let next_seq = state.last_seq + 1;
            let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

            let envelope_cbor = serialize_envelope(&req.envelope)?;
            let envelope_hash = Blake3Hash::of(&envelope_cbor);

            let entry = LogEntry {
                seq: Sequence(next_seq),
                stream_id: req.stream.clone(),
                subject: req.subject.clone(),
                ts_ns,
                envelope_hash: envelope_hash.clone(),
                payload_kind: req.payload.kind(),
                payload_ref_str: req.payload.to_ref_str(),
                headers: req.envelope.headers.clone(),
            };

            let bytes_written = write_entry(&mut state.writer, &entry).await?;
            state.last_seq = next_seq;
            state.segment_bytes += bytes_written as u64;

            Ok(Ack {
                message_id: req.envelope.message_id.clone(),
                stream_id: req.stream.clone(),
                seq: Sequence(next_seq),
                ts_ns,
            })
        }

        async fn read_from(
            &self,
            stream: &StreamId,
            from_seq: Sequence,
        ) -> Result<Pin<Box<dyn futures::Stream<Item = Result<LogEntry>> + Send>>> {
            let key = sanitize_stream_id(&stream.0);
            let stream_dir = self.base_dir.join(&key);

            let mut seg_files: Vec<PathBuf> = Vec::new();
            if let Ok(mut rd) = tokio::fs::read_dir(&stream_dir).await {
                while let Ok(Some(entry)) = rd.next_entry().await {
                    let p = entry.path();
                    if p.extension().and_then(|e| e.to_str()) == Some("log") {
                        seg_files.push(p);
                    }
                }
            }
            seg_files.sort();

            let entries = read_entries_from_segments(seg_files, from_seq).await?;
            let stream_out = futures::stream::iter(entries.into_iter().map(Ok));
            Ok(Box::pin(stream_out))
        }

        async fn last_seq(&self, stream: &StreamId) -> Result<Option<Sequence>> {
            let key = sanitize_stream_id(&stream.0);
            let streams = self.streams.lock().await;
            if let Some(state) = streams.get(&key) {
                if state.last_seq == 0 {
                    return Ok(None);
                }
                return Ok(Some(Sequence(state.last_seq)));
            }
            // Not in memory — scan
            let stream_dir = self.base_dir.join(&key);
            let mut seg_files: Vec<PathBuf> = Vec::new();
            if let Ok(mut rd) = tokio::fs::read_dir(&stream_dir).await {
                while let Ok(Some(entry)) = rd.next_entry().await {
                    let p = entry.path();
                    if p.extension().and_then(|e| e.to_str()) == Some("log") {
                        seg_files.push(p);
                    }
                }
            }
            seg_files.sort();
            if let Some(latest) = seg_files.last() {
                let last = scan_last_seq(latest).await.unwrap_or(0);
                if last == 0 {
                    return Ok(None);
                }
                return Ok(Some(Sequence(last)));
            }
            Ok(None)
        }
    }

    /// Write a LogEntry as [4-byte len LE][CBOR bytes][4-byte crc32].
    /// Returns total bytes written (len header + cbor + crc).
    async fn write_entry(file: &mut tokio::fs::File, entry: &LogEntry) -> Result<usize> {
        let mut cbor_buf = Vec::new();
        ciborium::into_writer(entry, &mut cbor_buf)
            .map_err(|e| YataError::Serialization(e.to_string()))?;
        let crc = crc32fast::hash(&cbor_buf);
        let len = cbor_buf.len() as u32;
        file.write_all(&len.to_le_bytes()).await?;
        file.write_all(&cbor_buf).await?;
        file.write_all(&crc.to_le_bytes()).await?;
        file.flush().await?;
        Ok(4 + cbor_buf.len() + 4)
    }

    fn serialize_envelope(env: &Envelope) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        ciborium::into_writer(env, &mut buf)
            .map_err(|e| YataError::Serialization(e.to_string()))?;
        Ok(buf)
    }

    async fn scan_last_seq(path: &Path) -> std::io::Result<u64> {
        let data = tokio::fs::read(path).await?;
        let mut cursor = std::io::Cursor::new(data.as_slice());
        let mut last_seq = 0u64;
        loop {
            let mut len_buf = [0u8; 4];
            if std::io::Read::read_exact(&mut cursor, &mut len_buf).is_err() {
                break;
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut cbor_buf = vec![0u8; len];
            if std::io::Read::read_exact(&mut cursor, &mut cbor_buf).is_err() {
                break;
            }
            let mut crc_buf = [0u8; 4];
            if std::io::Read::read_exact(&mut cursor, &mut crc_buf).is_err() {
                break;
            }
            if let Ok(entry) = ciborium::from_reader::<LogEntry, _>(std::io::Cursor::new(&cbor_buf)) {
                last_seq = entry.seq.0;
            }
        }
        Ok(last_seq)
    }

    async fn read_entries_from_segments(
        seg_files: Vec<PathBuf>,
        from_seq: Sequence,
    ) -> Result<Vec<LogEntry>> {
        let mut results = Vec::new();
        for path in seg_files {
            let data = tokio::fs::read(&path).await?;
            let mut cursor = std::io::Cursor::new(data.as_slice());
            loop {
                let mut len_buf = [0u8; 4];
                if std::io::Read::read_exact(&mut cursor, &mut len_buf).is_err() {
                    break;
                }
                let len = u32::from_le_bytes(len_buf) as usize;
                let mut cbor_buf = vec![0u8; len];
                if std::io::Read::read_exact(&mut cursor, &mut cbor_buf).is_err() {
                    break;
                }
                let mut crc_buf = [0u8; 4];
                if std::io::Read::read_exact(&mut cursor, &mut crc_buf).is_err() {
                    break;
                }
                let stored_crc = u32::from_le_bytes(crc_buf);
                let computed_crc = crc32fast::hash(&cbor_buf);
                if stored_crc != computed_crc {
                    tracing::warn!("CRC mismatch in segment {:?}, skipping entry", path);
                    continue;
                }
                match ciborium::from_reader::<LogEntry, _>(std::io::Cursor::new(&cbor_buf)) {
                    Ok(entry) => {
                        if entry.seq >= from_seq {
                            results.push(entry);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("failed to decode log entry: {}", e);
                    }
                }
            }
        }
        Ok(results)
    }
}

// ---- payload_store ----------------------------------------------------------

pub mod payload_store {
    use bytes::Bytes;
    use yata_core::Blake3Hash;

    pub struct PayloadStore {
        base_dir: std::path::PathBuf,
    }

    impl PayloadStore {
        pub async fn new(base_dir: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
            let base_dir = base_dir.into();
            tokio::fs::create_dir_all(&base_dir).await?;
            Ok(Self { base_dir })
        }

        pub async fn put(&self, data: &Bytes) -> std::io::Result<Blake3Hash> {
            let hash = Blake3Hash::of(data);
            let path = self.path_for(&hash);
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            if !path.exists() {
                tokio::fs::write(&path, data).await?;
            }
            Ok(hash)
        }

        pub async fn get(&self, hash: &Blake3Hash) -> std::io::Result<Option<Bytes>> {
            let path = self.path_for(hash);
            match tokio::fs::read(&path).await {
                Ok(data) => Ok(Some(Bytes::from(data))),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            }
        }

        pub fn path_for(&self, hash: &Blake3Hash) -> std::path::PathBuf {
            let hex = hash.hex();
            self.base_dir
                .join(&hex[..2])
                .join(&hex[2..4])
                .join(&hex)
        }
    }
}

#[cfg(test)]
mod tests;
