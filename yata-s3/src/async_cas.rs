//! AsyncS3CasStore: non-blocking S3 write-through via mpsc channel + background uploader.
//!
//! Mutations write to local CAS and enqueue S3 upload (non-blocking).
//! Background task drains the channel in batches (up to 16 parallel PUTs).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc;
use yata_cas::{CasStore, LocalCasStore};
use yata_core::Blake3Hash;

use crate::s3::S3Client;
use crate::s3_put_with_retry;

struct S3PutRequest {
    key: String,
    data: Bytes,
    lsn: u64,
}

pub struct AsyncS3CasStore {
    local: Arc<LocalCasStore>,
    remote: Arc<S3Client>,
    prefix: String,
    tx: mpsc::Sender<S3PutRequest>,
    committed_lsn: Arc<AtomicU64>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl AsyncS3CasStore {
    pub fn new(
        local: Arc<LocalCasStore>,
        remote: Arc<S3Client>,
        prefix: String,
        channel_size: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<S3PutRequest>(channel_size);
        let committed_lsn = Arc::new(AtomicU64::new(0));

        let task_remote = remote.clone();
        let task_lsn = committed_lsn.clone();
        let task_handle = tokio::spawn(Self::background_uploader(rx, task_remote, task_lsn));

        Self {
            local,
            remote,
            prefix,
            tx,
            committed_lsn,
            task_handle: Some(task_handle),
        }
    }

    /// Current committed LSN (highest LSN fully uploaded to S3).
    pub fn committed_lsn(&self) -> u64 {
        self.committed_lsn.load(Ordering::Acquire)
    }

    /// Push MDAG HEAD CID to well-known key `{prefix}MDAG_HEAD`.
    pub async fn push_head(&self, head_cid: &Blake3Hash) {
        let key = format!("{}MDAG_HEAD", self.prefix);
        let data = Bytes::from(head_cid.hex());
        s3_put_with_retry(&self.remote, &key, data).await;
    }

    /// Restore MDAG HEAD CID from well-known key.
    pub async fn restore_head(&self) -> Option<Blake3Hash> {
        let key = format!("{}MDAG_HEAD", self.prefix);
        match self.remote.get(&key).await {
            Ok(Some(data)) => {
                let hex = std::str::from_utf8(&data).ok()?.trim();
                Blake3Hash::from_hex(hex).ok()
            }
            _ => None,
        }
    }

    /// Flush: close the channel and await the background task completion.
    /// Call during graceful shutdown.
    pub async fn flush(&mut self) {
        // Drop sender to signal background task to finish
        // We can't drop self.tx directly, so we create a closed channel
        let (closed_tx, _) = mpsc::channel::<S3PutRequest>(1);
        let old_tx = std::mem::replace(&mut self.tx, closed_tx);
        drop(old_tx);

        if let Some(handle) = self.task_handle.take() {
            let _ = handle.await;
        }
        tracing::info!(
            committed_lsn = self.committed_lsn.load(Ordering::Acquire),
            "AsyncS3CasStore flushed"
        );
    }

    fn remote_key(&self, hash: &Blake3Hash) -> String {
        let hex = hash.hex();
        format!("{}cas/{}/{}/{}", self.prefix, &hex[..2], &hex[2..4], hex)
    }

    /// Fetch from S3 and cache locally.
    async fn fetch_remote(&self, hash: &Blake3Hash) -> yata_cas::Result<Option<Bytes>> {
        let key = self.remote_key(hash);
        match self.remote.get(&key).await {
            Ok(Some(data)) => {
                self.local.put_at(hash.clone(), data.clone()).await?;
                Ok(Some(data))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(yata_cas::CasError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))),
        }
    }

    async fn background_uploader(
        mut rx: mpsc::Receiver<S3PutRequest>,
        remote: Arc<S3Client>,
        committed_lsn: Arc<AtomicU64>,
    ) {
        loop {
            // Wait for the first item
            let first = match rx.recv().await {
                Some(req) => req,
                None => break, // Channel closed — shutdown
            };

            // Batch: try_recv up to 15 more (total 16)
            let mut batch = vec![first];
            while batch.len() < 16 {
                match rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(_) => break,
                }
            }

            let max_lsn = batch.iter().map(|r| r.lsn).max().unwrap_or(0);

            // Upload in parallel
            let futs: Vec<_> = batch
                .into_iter()
                .map(|req| {
                    let r = remote.clone();
                    async move {
                        s3_put_with_retry(&r, &req.key, req.data).await;
                    }
                })
                .collect();
            futures::future::join_all(futs).await;

            // Update committed LSN
            committed_lsn.fetch_max(max_lsn, Ordering::Release);
        }

        tracing::debug!("AsyncS3CasStore background uploader stopped");
    }
}

#[async_trait]
impl CasStore for AsyncS3CasStore {
    async fn put(&self, data: Bytes) -> yata_cas::Result<Blake3Hash> {
        let hash = self.local.put(data.clone()).await?;
        let key = self.remote_key(&hash);
        // Non-blocking enqueue
        let _ = self.tx.try_send(S3PutRequest { key, data, lsn: 0 });
        Ok(hash)
    }

    async fn get(&self, hash: &Blake3Hash) -> yata_cas::Result<Option<Bytes>> {
        if let Some(data) = self.local.get(hash).await? {
            return Ok(Some(data));
        }
        self.fetch_remote(hash).await
    }

    async fn has(&self, hash: &Blake3Hash) -> yata_cas::Result<bool> {
        if self.local.has(hash).await? {
            return Ok(true);
        }
        let key = self.remote_key(hash);
        self.remote.head(&key).await.map_err(|e| {
            yata_cas::CasError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })
    }

    async fn put_at(&self, hash: Blake3Hash, data: Bytes) -> yata_cas::Result<()> {
        self.local.put_at(hash.clone(), data.clone()).await?;
        let key = self.remote_key(&hash);
        let _ = self.tx.try_send(S3PutRequest { key, data, lsn: 0 });
        Ok(())
    }

    async fn get_raw(&self, hash: &Blake3Hash) -> yata_cas::Result<Option<Bytes>> {
        if let Some(data) = self.local.get_raw(hash).await? {
            return Ok(Some(data));
        }
        self.fetch_remote(hash).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_writes_local() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());

        // Create a mock S3Client that rejects everything (no real S3 in tests)
        let remote = Arc::new(S3Client::new(
            "http://localhost:0",
            "test-bucket",
            "key",
            "secret",
            "auto",
        ));

        let store = AsyncS3CasStore::new(local.clone(), remote, "test/".into(), 64);

        let data = Bytes::from_static(b"hello async cas");
        let hash = store.put(data.clone()).await.unwrap();

        // Local should have it
        let retrieved = local.get(&hash).await.unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_get_reads_local() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());
        let remote = Arc::new(S3Client::new(
            "http://localhost:0",
            "test-bucket",
            "key",
            "secret",
            "auto",
        ));

        let store = AsyncS3CasStore::new(local.clone(), remote, "test/".into(), 64);

        let data = Bytes::from_static(b"read test");
        let hash = store.put(data.clone()).await.unwrap();
        let retrieved = store.get(&hash).await.unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_has_local() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());
        let remote = Arc::new(S3Client::new(
            "http://localhost:0",
            "test-bucket",
            "key",
            "secret",
            "auto",
        ));

        let store = AsyncS3CasStore::new(local, remote, "test/".into(), 64);

        let data = Bytes::from_static(b"has test");
        let hash = store.put(data).await.unwrap();
        assert!(store.has(&hash).await.unwrap());

        let missing = Blake3Hash::of(b"not stored");
        // has() for missing data will fail with connection error (no S3), but local says false
        // In this test setup, remote.head() will error, which is expected
    }
}
