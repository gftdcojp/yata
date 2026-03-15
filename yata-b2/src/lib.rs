#![allow(dead_code)]

//! Backblaze B2 sync adapter using S3-compatible API via object_store.
//!
//! Syncs CAS chunks and object manifests from a LocalObjectStore to B2.
//! Uses manifest-first approach — no reliance on S3 ACL/tags.

use async_trait::async_trait;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, PutPayload};
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;
use yata_core::{
    Blake3Hash, ObjectId, ObjectManifest, ObjectMeta, ObjectStorage, Result, YataError,
};
use yata_object::LocalObjectStore;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct B2Config {
    /// B2 endpoint URL, e.g. https://s3.us-west-004.backblazeb2.com
    pub endpoint: String,
    /// B2 bucket name
    pub bucket: String,
    /// B2 application key ID (AWS_ACCESS_KEY_ID)
    pub key_id: String,
    /// B2 application key (AWS_SECRET_ACCESS_KEY)
    pub application_key: String,
    /// B2 region, e.g. us-west-004
    pub region: String,
    /// Prefix under which YATA objects are stored, e.g. "yata/"
    pub prefix: String,
}

pub struct B2Sync {
    store: Arc<dyn ObjectStore>,
    config: B2Config,
    local: Arc<LocalObjectStore>,
}

impl B2Sync {
    pub fn new(config: B2Config, local: Arc<LocalObjectStore>) -> Result<Self> {
        let store = AmazonS3Builder::new()
            .with_endpoint(&config.endpoint)
            .with_bucket_name(&config.bucket)
            .with_access_key_id(&config.key_id)
            .with_secret_access_key(&config.application_key)
            .with_region(&config.region)
            .build()
            .map_err(|e| YataError::Storage(e.to_string()))?;
        Ok(Self {
            store: Arc::new(store),
            config,
            local,
        })
    }

    /// Sync all unsynced chunks and manifests to B2.
    /// Returns (chunks_uploaded, manifests_uploaded).
    pub async fn sync_all(&self) -> Result<(u64, u64)> {
        let manifests_dir = self.local.manifest_path(&ObjectId::new()).parent().unwrap().to_owned();
        // Actually derive from base path via a known manifest
        let base_dir = manifests_dir.parent().unwrap().to_owned();
        let manifests_dir = base_dir.join("manifests");
        let cas_dir = base_dir.join("cas");

        let mut chunks_uploaded = 0u64;
        let mut manifests_uploaded = 0u64;

        // Sync manifests
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

        // Sync chunks
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

    /// Upload a single chunk if not already present on B2.
    pub async fn sync_chunk(&self, hash: &Blake3Hash) -> Result<bool> {
        let key = self.chunk_key(hash);
        // Check if already present
        if self.store.head(&key).await.is_ok() {
            return Ok(false);
        }
        let local_path = self.local.chunk_path(hash);
        let data = tokio::fs::read(&local_path).await.map_err(YataError::Io)?;
        let payload = PutPayload::from_bytes(bytes::Bytes::from(data));
        self.store
            .put(&key, payload)
            .await
            .map_err(|e| YataError::Storage(e.to_string()))?;
        tracing::debug!("synced chunk {} to B2", hash.hex());
        Ok(true)
    }

    /// Upload a manifest if not already present on B2.
    pub async fn sync_manifest(&self, id: &ObjectId) -> Result<bool> {
        let key = self.manifest_key(id);
        // Check if already present
        if self.store.head(&key).await.is_ok() {
            return Ok(false);
        }
        let local_path = self.local.manifest_path(id);
        let data = tokio::fs::read(&local_path)
            .await
            .map_err(YataError::Io)?;
        let payload = PutPayload::from_bytes(bytes::Bytes::from(data));
        self.store
            .put(&key, payload)
            .await
            .map_err(|e| YataError::Storage(e.to_string()))?;
        tracing::debug!("synced manifest {} to B2", id);
        Ok(true)
    }

    /// Restore a manifest and its chunks from B2 to local.
    pub async fn restore_manifest(&self, id: &ObjectId) -> Result<ObjectManifest> {
        let key = self.manifest_key(id);
        let get_result = self
            .store
            .get(&key)
            .await
            .map_err(|e| YataError::Storage(e.to_string()))?;
        let data = get_result
            .bytes()
            .await
            .map_err(|e| YataError::Storage(e.to_string()))?;

        let manifest: ObjectManifest =
            ciborium::from_reader(std::io::Cursor::new(&data[..]))
                .map_err(|e| YataError::Serialization(e.to_string()))?;

        // Write manifest locally
        let local_manifest_path = self.local.manifest_path(id);
        if let Some(parent) = local_manifest_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&local_manifest_path, &data[..]).await?;

        // Restore chunks
        for chunk in &manifest.chunks {
            let chunk_key = self.chunk_key(&chunk.hash);
            let local_chunk_path = self.local.chunk_path(&chunk.hash);
            if local_chunk_path.exists() {
                continue;
            }
            let chunk_result = self
                .store
                .get(&chunk_key)
                .await
                .map_err(|e| YataError::Storage(e.to_string()))?;
            let chunk_data = chunk_result
                .bytes()
                .await
                .map_err(|e| YataError::Storage(e.to_string()))?;
            if let Some(parent) = local_chunk_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(&local_chunk_path, &chunk_data[..]).await?;
        }

        Ok(manifest)
    }

    /// Recursively sync all files under `local_dir` to B2 under `remote_prefix`.
    /// Files already present on B2 are skipped. Returns number of files uploaded.
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
                    let key = OsPath::from(format!("{}{}", remote_prefix, name_str));
                    if self.store.head(&key).await.is_ok() {
                        continue;
                    }
                    let data = tokio::fs::read(&path).await.map_err(YataError::Io)?;
                    let payload = PutPayload::from_bytes(bytes::Bytes::from(data));
                    self.store
                        .put(&key, payload)
                        .await
                        .map_err(|e| YataError::Storage(e.to_string()))?;
                    uploaded += 1;
                }
            }
            Ok(uploaded)
        })
    }

    fn chunk_key(&self, hash: &Blake3Hash) -> OsPath {
        let hex = hash.hex();
        OsPath::from(format!(
            "{}cas/{}/{}/{}",
            self.config.prefix,
            &hex[..2],
            &hex[2..4],
            hex
        ))
    }

    fn manifest_key(&self, id: &ObjectId) -> OsPath {
        OsPath::from(format!("{}manifests/{}.cbor", self.config.prefix, id))
    }
}

// ── TieredObjectStore ─────────────────────────────────────────────────────────
//
// Implements ObjectStorage: writes go to local first, then async-syncs to B2.
// Reads fall back to B2 restore when the local chunk/manifest is missing.

pub struct TieredObjectStore {
    local: Arc<LocalObjectStore>,
    b2: Arc<B2Sync>,
}

impl TieredObjectStore {
    pub fn new(local: Arc<LocalObjectStore>, b2: Arc<B2Sync>) -> Self {
        Self { local, b2 }
    }
}

#[async_trait]
impl ObjectStorage for TieredObjectStore {
    async fn put_object(&self, data: bytes::Bytes, meta: ObjectMeta) -> Result<ObjectManifest> {
        let manifest = self.local.put_object(data, meta).await?;
        let b2 = self.b2.clone();
        let manifest_id = manifest.object_id.clone();
        let chunks = manifest.chunks.clone();
        tokio::spawn(async move {
            for chunk in &chunks {
                if let Err(e) = b2.sync_chunk(&chunk.hash).await {
                    tracing::warn!("b2 chunk sync failed hash={}: {}", chunk.hash.hex(), e);
                }
            }
            if let Err(e) = b2.sync_manifest(&manifest_id).await {
                tracing::warn!("b2 manifest sync failed id={}: {}", manifest_id, e);
            }
        });
        Ok(manifest)
    }

    async fn get_object(&self, id: &ObjectId) -> Result<bytes::Bytes> {
        match self.local.get_object(id).await {
            Ok(data) => Ok(data),
            Err(YataError::NotFound(_)) => {
                self.b2.restore_manifest(id).await?;
                self.local.get_object(id).await
            }
            Err(e) => Err(e),
        }
    }

    async fn head_object(&self, id: &ObjectId) -> Result<Option<ObjectManifest>> {
        if let Some(m) = self.local.head_object(id).await? {
            return Ok(Some(m));
        }
        match self.b2.restore_manifest(id).await {
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

