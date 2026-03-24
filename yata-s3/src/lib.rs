#![allow(dead_code)]

//! S3-compatible cold storage adapter (R2) via reqwest + SigV4.
//!
//! Syncs CAS chunks and object manifests from a LocalObjectStore to S3.
//! Uses reqwest (standard HTTP) instead of object_store crate to work
//! in CF Containers where outbound HTTPS is proxied through Workers fetch API.

pub mod async_cas;
pub mod s3;

use async_trait::async_trait;
use s3::S3Client;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;
use yata_object::cas::CasStore;
use yata_core::{
    Blake3Hash, ObjectId, ObjectManifest, ObjectMeta, ObjectStorage, Result, YataError,
};
use yata_object::LocalObjectStore;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct S3Config {
    /// S3-compatible endpoint URL.
    /// R2: https://{account_id}.r2.cloudflarestorage.com
    pub endpoint: String,
    /// Bucket name
    pub bucket: String,
    /// Access key ID
    pub key_id: String,
    /// Secret access key
    pub application_key: String,
    /// Region (R2: auto)
    pub region: String,
    /// Object key prefix, e.g. "yata/"
    pub prefix: String,
    /// Eager sync: await remote upload instead of fire-and-forget tokio::spawn.
    /// Use for CF Containers where ephemeral disk may be lost on sleep/evict.
    #[serde(default)]
    pub eager: bool,
}

impl S3Config {
    pub fn build_client(&self) -> S3Client {
        S3Client::new(
            &self.endpoint,
            &self.bucket,
            &self.key_id,
            &self.application_key,
            &self.region,
        )
    }
}

pub struct S3Sync {
    store: Arc<S3Client>,
    config: S3Config,
    local: Arc<LocalObjectStore>,
    cas: Arc<dyn CasStore>,
}

impl S3Sync {
    pub fn new(config: S3Config, local: Arc<LocalObjectStore>) -> Result<Self> {
        let cas = local.cas().clone();
        Self::with_cas(config, local, cas)
    }

    pub fn with_cas(
        config: S3Config,
        local: Arc<LocalObjectStore>,
        cas: Arc<dyn CasStore>,
    ) -> Result<Self> {
        let store = Arc::new(config.build_client());
        Ok(Self {
            store,
            config,
            local,
            cas,
        })
    }

    pub fn remote_store(&self) -> &Arc<S3Client> {
        &self.store
    }

    pub fn prefix(&self) -> &str {
        &self.config.prefix
    }

    pub fn is_eager(&self) -> bool {
        self.config.eager
    }

    /// Push MDAG HEAD CID to well-known key `{prefix}MDAG_HEAD`.
    pub async fn push_head(&self, head_cid: &Blake3Hash) {
        let key = format!("{}MDAG_HEAD", self.config.prefix);
        let data = bytes::Bytes::from(head_cid.hex());
        s3_put_with_retry(&self.store, &key, data).await;
    }

    /// Restore MDAG HEAD CID from well-known key.
    pub async fn restore_head(&self) -> Option<Blake3Hash> {
        let key = format!("{}MDAG_HEAD", self.config.prefix);
        match self.store.get(&key).await {
            Ok(Some(data)) => {
                let hex = std::str::from_utf8(&data).ok()?.trim();
                Blake3Hash::from_hex(hex).ok()
            }
            _ => None,
        }
    }

    /// Sync all unsynced chunks and manifests.
    /// Returns (chunks_uploaded, manifests_uploaded).
    pub async fn sync_all(&self) -> Result<(u64, u64)> {
        let manifests_dir = self
            .local
            .manifest_path(&ObjectId::new())
            .parent()
            .unwrap()
            .to_owned();
        let base_dir = manifests_dir.parent().unwrap().to_owned();
        let manifests_dir = base_dir.join("manifests");

        let mut chunks_uploaded = 0u64;
        let mut manifests_uploaded = 0u64;

        if let Ok(mut rd) = tokio::fs::read_dir(&manifests_dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) != Some("cbor") {
                    continue;
                }
                let stem = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_owned();
                if let Ok(id) = stem.parse::<Uuid>() {
                    let object_id = ObjectId(id);
                    if self.sync_manifest(&object_id).await? {
                        manifests_uploaded += 1;
                    }
                }
            }
        }

        let cas_dir = base_dir.join("cas");
        chunks_uploaded += self.sync_cas_dir_recursive(&cas_dir).await?;

        Ok((chunks_uploaded, manifests_uploaded))
    }

    async fn sync_cas_dir_recursive(&self, dir: &std::path::Path) -> Result<u64> {
        let mut uploaded = 0u64;
        let mut rd = match tokio::fs::read_dir(dir).await {
            Ok(r) => r,
            Err(_) => return Ok(0),
        };
        while let Ok(Some(entry)) = rd.next_entry().await {
            let path = entry.path();
            if path.is_dir() {
                uploaded += Box::pin(self.sync_cas_dir_recursive(&path)).await?;
            } else {
                let name = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_owned();
                if let Ok(hash) = name.parse::<Blake3Hash>() {
                    if self.sync_chunk(&hash).await? {
                        uploaded += 1;
                    }
                }
            }
        }
        Ok(uploaded)
    }

    /// Upload a single chunk if not already present remotely.
    pub async fn sync_chunk(&self, hash: &Blake3Hash) -> Result<bool> {
        let key = self.chunk_key(hash);
        if self
            .store
            .head(&key)
            .await
            .map_err(|e| YataError::Storage(e.to_string()))?
        {
            return Ok(false);
        }
        let data = self
            .cas
            .get_raw(hash)
            .await
            .map_err(|e| YataError::Storage(e.to_string()))?
            .ok_or_else(|| YataError::NotFound(format!("chunk {}", hash.hex())))?;
        self.store
            .put(&key, data)
            .await
            .map_err(|e| YataError::Storage(e.to_string()))?;
        tracing::debug!("synced chunk {} to S3", hash.hex());
        Ok(true)
    }

    /// Upload a manifest if not already present remotely.
    pub async fn sync_manifest(&self, id: &ObjectId) -> Result<bool> {
        let key = self.manifest_key(id);
        if self
            .store
            .head(&key)
            .await
            .map_err(|e| YataError::Storage(e.to_string()))?
        {
            return Ok(false);
        }
        let local_path = self.local.manifest_path(id);
        let data = tokio::fs::read(&local_path).await.map_err(YataError::Io)?;
        self.store
            .put(&key, bytes::Bytes::from(data))
            .await
            .map_err(|e| YataError::Storage(e.to_string()))?;
        tracing::debug!("synced manifest {} to S3", id);
        Ok(true)
    }

    /// Restore a manifest and its chunks from S3 to local.
    pub async fn restore_manifest(&self, id: &ObjectId) -> Result<ObjectManifest> {
        let key = self.manifest_key(id);
        let data = self
            .store
            .get(&key)
            .await
            .map_err(|e| YataError::Storage(e.to_string()))?
            .ok_or_else(|| YataError::NotFound(format!("manifest {id}")))?;

        let manifest: ObjectManifest = ciborium::from_reader(std::io::Cursor::new(&data[..]))
            .map_err(|e| YataError::Serialization(e.to_string()))?;

        let local_manifest_path = self.local.manifest_path(id);
        if let Some(parent) = local_manifest_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&local_manifest_path, &data[..]).await?;

        for chunk in &manifest.chunks {
            if self
                .cas
                .has(&chunk.hash)
                .await
                .map_err(|e| YataError::Storage(e.to_string()))?
            {
                continue;
            }
            let chunk_key = self.chunk_key(&chunk.hash);
            let chunk_data = self
                .store
                .get(&chunk_key)
                .await
                .map_err(|e| YataError::Storage(e.to_string()))?
                .ok_or_else(|| YataError::NotFound(format!("chunk {}", chunk.hash.hex())))?;
            self.cas
                .put_at(chunk.hash.clone(), chunk_data)
                .await
                .map_err(|e| YataError::Storage(e.to_string()))?;
        }

        Ok(manifest)
    }

    /// Recursively sync all files under `local_dir` to S3 under `remote_prefix`.
    pub async fn sync_dir(&self, local_dir: &Path, remote_prefix: &str) -> Result<u64> {
        self.sync_dir_recursive(local_dir, remote_prefix).await
    }

    fn sync_dir_recursive<'a>(
        &'a self,
        local_dir: &'a Path,
        remote_prefix: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64>> + Send + 'a>> {
        Box::pin(async move {
            let mut uploaded = 0u64;
            let mut rd = match tokio::fs::read_dir(local_dir).await {
                Ok(r) => r,
                Err(_) => return Ok(0),
            };
            while let Ok(Some(entry)) = rd.next_entry().await {
                let path = entry.path();
                let name = entry.file_name();
                let name_str = name.to_string_lossy().into_owned();
                if path.is_dir() {
                    let sub = format!("{}{}/", remote_prefix, name_str);
                    uploaded += self.sync_dir_recursive(&path, &sub).await?;
                } else {
                    let key = format!("{}{}", remote_prefix, name_str);
                    if self
                        .store
                        .head(&key)
                        .await
                        .map_err(|e| YataError::Storage(e.to_string()))?
                    {
                        continue;
                    }
                    let data = tokio::fs::read(&path).await.map_err(YataError::Io)?;
                    self.store
                        .put(&key, bytes::Bytes::from(data))
                        .await
                        .map_err(|e| YataError::Storage(e.to_string()))?;
                    uploaded += 1;
                }
            }
            Ok(uploaded)
        })
    }

    /// Upload a completed WAL segment file.
    pub async fn push_wal_segment(&self, segment_path: &Path) -> Result<()> {
        let file_name = segment_path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| YataError::Storage("invalid segment path".into()))?;
        let stream_dir = segment_path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        let key = format!(
            "{}graph-wal/{}/{}",
            self.config.prefix, stream_dir, file_name
        );

        if self
            .store
            .head(&key)
            .await
            .map_err(|e| YataError::Storage(e.to_string()))?
        {
            return Ok(());
        }

        let data = tokio::fs::read(segment_path).await.map_err(YataError::Io)?;
        self.store
            .put(&key, bytes::Bytes::from(data))
            .await
            .map_err(|e| YataError::Storage(e.to_string()))?;
        tracing::info!(
            segment = file_name,
            stream = stream_dir,
            "pushed WAL segment to S3"
        );
        Ok(())
    }

    /// List and download WAL segments from S3 for disaster recovery.
    pub async fn restore_wal_segments(
        &self,
        stream_id: &str,
        local_dir: &Path,
    ) -> Result<Vec<std::path::PathBuf>> {
        let prefix = format!("{}graph-wal/{}/", self.config.prefix, stream_id);

        let objects = self
            .store
            .list(&prefix)
            .await
            .map_err(|e| YataError::Storage(e.to_string()))?;

        let stream_dir = local_dir.join(stream_id);
        tokio::fs::create_dir_all(&stream_dir).await?;

        let mut paths = Vec::new();
        for obj in objects {
            if let Some(file_name) = obj.key.rsplit('/').next() {
                if file_name.is_empty() {
                    continue;
                }
                let local_path = stream_dir.join(file_name);
                if !local_path.exists() {
                    let data = self
                        .store
                        .get(&obj.key)
                        .await
                        .map_err(|e| YataError::Storage(e.to_string()))?
                        .ok_or_else(|| YataError::NotFound(format!("WAL segment {}", obj.key)))?;
                    tokio::fs::write(&local_path, &data).await?;
                }
                paths.push(local_path);
            }
        }

        paths.sort();
        Ok(paths)
    }

    pub fn chunk_key(&self, hash: &Blake3Hash) -> String {
        let hex = hash.hex();
        format!(
            "{}cas/{}/{}/{}",
            self.config.prefix,
            &hex[..2],
            &hex[2..4],
            hex
        )
    }

    fn manifest_key(&self, id: &ObjectId) -> String {
        format!("{}manifests/{}.cbor", self.config.prefix, id)
    }
}

// ── S3 upload utility ────────────────────────────────────────────────────────

/// Upload with exponential backoff retry (3 attempts).
/// Used by AsyncS3CasStore and S3Sync.
pub async fn s3_put_with_retry(remote: &Arc<S3Client>, key: &str, data: bytes::Bytes) {
    let delays = [1, 4, 16];
    for (attempt, delay_secs) in delays.iter().enumerate() {
        match remote.put(key, data.clone()).await {
            Ok(_) => return,
            Err(e) => {
                if attempt + 1 < delays.len() {
                    tracing::warn!(
                        "S3 upload attempt {}/{} failed for {}: {e}, retrying in {delay_secs}s",
                        attempt + 1,
                        delays.len(),
                        key
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(*delay_secs)).await;
                } else {
                    tracing::error!(
                        "S3 upload FAILED after {} attempts for {}: {e}",
                        delays.len(),
                        key
                    );
                }
            }
        }
    }
}

// ── TieredObjectStore ─────────────────────────────────────────────────────────
//
// Implements ObjectStorage: writes go to local first, then syncs to S3.
// Reads fall back to S3 restore when the local chunk/manifest is missing.

pub struct TieredObjectStore {
    local: Arc<LocalObjectStore>,
    s3: Arc<S3Sync>,
    eager: bool,
}

impl TieredObjectStore {
    pub fn new(local: Arc<LocalObjectStore>, s3: Arc<S3Sync>) -> Self {
        Self {
            local,
            s3,
            eager: false,
        }
    }

    pub fn new_eager(local: Arc<LocalObjectStore>, s3: Arc<S3Sync>, eager: bool) -> Self {
        Self { local, s3, eager }
    }
}

#[async_trait]
impl ObjectStorage for TieredObjectStore {
    async fn put_object(&self, data: bytes::Bytes, meta: ObjectMeta) -> Result<ObjectManifest> {
        let manifest = self.local.put_object(data, meta).await?;
        let s3 = self.s3.clone();
        let manifest_id = manifest.object_id.clone();
        let chunks = manifest.chunks.clone();
        let sync_fn = async move {
            for chunk in &chunks {
                if let Err(e) = s3.sync_chunk(&chunk.hash).await {
                    tracing::warn!("S3 chunk sync failed hash={}: {}", chunk.hash.hex(), e);
                }
            }
            if let Err(e) = s3.sync_manifest(&manifest_id).await {
                tracing::warn!("S3 manifest sync failed id={}: {}", manifest_id, e);
            }
        };
        if self.eager {
            sync_fn.await;
        } else {
            tokio::spawn(sync_fn);
        }
        Ok(manifest)
    }

    async fn get_object(&self, id: &ObjectId) -> Result<bytes::Bytes> {
        match self.local.get_object(id).await {
            Ok(data) => Ok(data),
            Err(YataError::NotFound(_)) => {
                self.s3.restore_manifest(id).await?;
                self.local.get_object(id).await
            }
            Err(e) => Err(e),
        }
    }

    async fn head_object(&self, id: &ObjectId) -> Result<Option<ObjectManifest>> {
        if let Some(m) = self.local.head_object(id).await? {
            return Ok(Some(m));
        }
        match self.s3.restore_manifest(id).await {
            Ok(m) => Ok(Some(m)),
            Err(YataError::Storage(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn pin_object(&self, id: &ObjectId) -> Result<()> {
        self.local.pin_object(id).await
    }

    async fn gc(&self) -> Result<u64> {
        self.local.gc().await
    }
}
