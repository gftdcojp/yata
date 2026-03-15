#![allow(dead_code)]

//! KV bucket: a typed, revisioned key-value store backed by an append-only log.
//!
//! Each bucket maps to a dedicated stream: `_kv.<bucket_id>`.
//! Entries are LogEntry with payload_kind=InlineBytes.
//! Current value for each key is maintained in an in-memory snapshot.

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use yata_core::{
    AppendLog, Blake3Hash, BucketId, Envelope, KvAck, KvEntry, KvEvent, KvOp, KvPutRequest,
    KvStore, PayloadKind, PayloadRef, PublishRequest, Result, Revision, SchemaId, Sequence,
    StreamId, Subject, YataError,
};
use yata_log::{LocalLog, PayloadStore};

const WATCHER_CAPACITY: usize = 256;

pub struct KvBucketStore {
    log: Arc<LocalLog>,
    payload_store: Arc<PayloadStore>,
    snapshots: RwLock<HashMap<String, HashMap<String, KvEntry>>>,
    watchers: RwLock<HashMap<String, broadcast::Sender<KvEvent>>>,
    /// Buffered entries waiting to be flushed to Lance / B2 by the Broker.
    pending_sync: Mutex<Vec<KvEntry>>,
}

impl KvBucketStore {
    pub async fn new(log: Arc<LocalLog>, payload_store: Arc<PayloadStore>) -> Result<Self> {
        Ok(Self {
            log,
            payload_store,
            snapshots: RwLock::new(HashMap::new()),
            watchers: RwLock::new(HashMap::new()),
            pending_sync: Mutex::new(Vec::new()),
        })
    }

    /// Drain and return all KvEntries buffered since the last call.
    /// The Broker flushes these to Lance (yata_kv_history) on each sync cycle.
    pub fn drain_pending(&self) -> Vec<KvEntry> {
        self.pending_sync.lock().unwrap().drain(..).collect()
    }

    pub async fn load_snapshot(&self, bucket: &BucketId) -> Result<()> {
        let stream_id = Self::stream_id_for(bucket);
        let stream = self.log.read_from(&stream_id, Sequence(1)).await?;
        tokio::pin!(stream);

        let mut snap: HashMap<String, KvEntry> = HashMap::new();
        while let Some(entry_result) = stream.next().await {
            let log_entry = entry_result?;
            if log_entry.payload_kind != PayloadKind::InlineBytes {
                continue;
            }
            let hash_hex = log_entry.payload_ref_str
                .strip_prefix("inline:")
                .unwrap_or("");
            let hash: Blake3Hash = match hash_hex.parse() {
                Ok(h) => h,
                Err(_) => continue,
            };
            let bytes = match self.payload_store.get(&hash).await.map_err(YataError::Io)? {
                Some(b) => b,
                None => continue,
            };
            let kv_entry: KvEntry = match ciborium::from_reader(std::io::Cursor::new(&bytes[..])) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("failed to decode KV entry during replay: {}", e);
                    continue;
                }
            };
            match kv_entry.op {
                KvOp::Put => {
                    snap.insert(kv_entry.key.clone(), kv_entry);
                }
                KvOp::Delete | KvOp::Purge => {
                    snap.remove(&kv_entry.key);
                }
            }
        }
        let mut snapshots = self.snapshots.write().await;
        snapshots.insert(bucket.0.clone(), snap);
        Ok(())
    }

    fn stream_id_for(bucket: &BucketId) -> StreamId {
        StreamId(format!("_kv.{}", bucket.0))
    }

    async fn get_or_init_watcher(&self, bucket: &BucketId) -> broadcast::Sender<KvEvent> {
        let mut watchers = self.watchers.write().await;
        watchers
            .entry(bucket.0.clone())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(WATCHER_CAPACITY);
                tx
            })
            .clone()
    }

    async fn current_revision(&self, bucket: &BucketId, key: &str) -> Revision {
        let snapshots = self.snapshots.read().await;
        if let Some(bucket_snap) = snapshots.get(&bucket.0) {
            if let Some(entry) = bucket_snap.get(key) {
                return entry.revision;
            }
        }
        Revision(0)
    }
}

#[async_trait]
impl KvStore for KvBucketStore {
    async fn put(&self, req: KvPutRequest) -> Result<KvAck> {
        // Check expected revision
        let current_rev = self.current_revision(&req.bucket, &req.key).await;
        if let Some(expected) = req.expected_revision {
            if current_rev != expected {
                return Err(YataError::RevisionConflict {
                    expected: expected.0,
                    actual: current_rev.0,
                });
            }
        }

        let next_rev = Revision(current_rev.0 + 1);
        let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        let kv_entry = KvEntry {
            bucket: req.bucket.clone(),
            key: req.key.clone(),
            revision: next_rev,
            value: req.value.to_vec(),
            ts_ns,
            op: KvOp::Put,
        };

        // Encode as CBOR payload and persist for replay
        let mut cbor_buf = Vec::new();
        ciborium::into_writer(&kv_entry, &mut cbor_buf)
            .map_err(|e| YataError::Serialization(e.to_string()))?;
        let cbor_bytes = Bytes::from(cbor_buf);
        self.payload_store.put(&cbor_bytes).await.map_err(YataError::Io)?;
        let payload = PayloadRef::InlineBytes(cbor_bytes);
        let content_hash = payload.content_hash();
        let stream_id = Self::stream_id_for(&req.bucket);
        let subject = Subject(format!("{}.{}", req.bucket.0, req.key));
        let schema_id = SchemaId("yata.kv.entry".to_string());
        let envelope = Envelope::new(subject.clone(), schema_id, content_hash);

        let publish_req = PublishRequest {
            stream: stream_id,
            subject,
            envelope,
            payload,
            expected_last_seq: None,
        };
        self.log.append(publish_req).await?;

        // Update snapshot
        {
            let mut snapshots = self.snapshots.write().await;
            let bucket_snap = snapshots
                .entry(req.bucket.0.clone())
                .or_insert_with(HashMap::new);
            bucket_snap.insert(req.key.clone(), kv_entry.clone());
        }

        // Buffer for Lance / B2 flush
        self.pending_sync.lock().unwrap().push(kv_entry.clone());

        // Broadcast
        let tx = self.get_or_init_watcher(&req.bucket).await;
        let _ = tx.send(KvEvent {
            entry: kv_entry,
            is_delete: false,
        });

        Ok(KvAck {
            revision: next_rev,
            ts_ns,
        })
    }

    async fn get(&self, bucket: &BucketId, key: &str) -> Result<Option<KvEntry>> {
        let snapshots = self.snapshots.read().await;
        if let Some(bucket_snap) = snapshots.get(&bucket.0) {
            return Ok(bucket_snap.get(key).cloned());
        }
        Ok(None)
    }

    async fn delete(
        &self,
        bucket: &BucketId,
        key: &str,
        expected_revision: Option<Revision>,
    ) -> Result<KvAck> {
        let current_rev = self.current_revision(bucket, key).await;
        if let Some(expected) = expected_revision {
            if current_rev != expected {
                return Err(YataError::RevisionConflict {
                    expected: expected.0,
                    actual: current_rev.0,
                });
            }
        }

        let next_rev = Revision(current_rev.0 + 1);
        let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        let kv_entry = KvEntry {
            bucket: bucket.clone(),
            key: key.to_owned(),
            revision: next_rev,
            value: Vec::new(),
            ts_ns,
            op: KvOp::Delete,
        };

        let mut cbor_buf = Vec::new();
        ciborium::into_writer(&kv_entry, &mut cbor_buf)
            .map_err(|e| YataError::Serialization(e.to_string()))?;
        let cbor_bytes = Bytes::from(cbor_buf);
        self.payload_store.put(&cbor_bytes).await.map_err(YataError::Io)?;
        let payload = PayloadRef::InlineBytes(cbor_bytes);
        let content_hash = payload.content_hash();
        let stream_id = Self::stream_id_for(bucket);
        let subject = Subject(format!("{}.{}", bucket.0, key));
        let schema_id = SchemaId("yata.kv.entry".to_string());
        let envelope = Envelope::new(subject.clone(), schema_id, content_hash);

        let publish_req = PublishRequest {
            stream: stream_id,
            subject,
            envelope,
            payload,
            expected_last_seq: None,
        };
        self.log.append(publish_req).await?;

        // Update snapshot
        {
            let mut snapshots = self.snapshots.write().await;
            if let Some(bucket_snap) = snapshots.get_mut(&bucket.0) {
                bucket_snap.remove(key);
            }
        }

        // Buffer for Lance / B2 flush
        self.pending_sync.lock().unwrap().push(kv_entry.clone());

        // Broadcast
        let tx = self.get_or_init_watcher(bucket).await;
        let _ = tx.send(KvEvent {
            entry: kv_entry,
            is_delete: true,
        });

        Ok(KvAck {
            revision: next_rev,
            ts_ns,
        })
    }

    async fn watch(
        &self,
        bucket: &BucketId,
        prefix: &str,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = KvEvent> + Send>>> {
        let tx = self.get_or_init_watcher(bucket).await;
        let rx = tx.subscribe();
        let prefix = prefix.to_owned();
        let stream = BroadcastStream::new(rx)
            .filter_map(move |r| {
                let prefix = prefix.clone();
                async move {
                    match r {
                        Ok(event) if event.entry.key.starts_with(&prefix) => Some(event),
                        _ => None,
                    }
                }
            });
        Ok(Box::pin(stream))
    }

    async fn history(&self, bucket: &BucketId, key: &str) -> Result<Vec<KvEntry>> {
        let stream_id = Self::stream_id_for(bucket);
        let stream = self.log.read_from(&stream_id, Sequence(1)).await?;
        tokio::pin!(stream);

        let key = key.to_owned();
        let mut history = Vec::new();
        while let Some(entry_result) = stream.next().await {
            let log_entry = entry_result?;
            if log_entry.payload_kind != PayloadKind::InlineBytes {
                continue;
            }
            let hash_hex = log_entry.payload_ref_str
                .strip_prefix("inline:")
                .unwrap_or("");
            let hash: Blake3Hash = match hash_hex.parse() {
                Ok(h) => h,
                Err(_) => continue,
            };
            let bytes = match self.payload_store.get(&hash).await.map_err(YataError::Io)? {
                Some(b) => b,
                None => continue,
            };
            let kv_entry: KvEntry = match ciborium::from_reader(std::io::Cursor::new(&bytes[..])) {
                Ok(e) => e,
                Err(_) => continue,
            };
            if kv_entry.key == key {
                history.push(kv_entry);
            }
        }
        Ok(history)
    }
}

#[cfg(test)]
mod tests;

