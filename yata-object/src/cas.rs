//! Content-addressed chunk storage.
//!
//! `CasStore` stores arbitrary byte blobs keyed by Blake3 hash.
//! Layout: `<base>/<hash[0..2]>/<hash[2..4]>/<full_hex_hash>`

use async_trait::async_trait;
use bytes::Bytes;
use yata_core::Blake3Hash;

#[derive(thiserror::Error, Debug)]
pub enum CasError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },
}

pub type Result<T> = std::result::Result<T, CasError>;

/// Content-addressed chunk store.
#[async_trait]
pub trait CasStore: Send + Sync + 'static {
    async fn put(&self, data: Bytes) -> Result<Blake3Hash>;
    async fn get(&self, hash: &Blake3Hash) -> Result<Option<Bytes>>;
    async fn has(&self, hash: &Blake3Hash) -> Result<bool>;

    async fn put_at(&self, hash: Blake3Hash, data: Bytes) -> Result<()> {
        let _ = hash;
        let _ = data;
        Err(CasError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "put_at not supported",
        )))
    }

    async fn get_raw(&self, hash: &Blake3Hash) -> Result<Option<Bytes>> {
        self.get(hash).await
    }
}

/// Local filesystem CAS store.
pub struct LocalCasStore {
    base_dir: std::path::PathBuf,
}

impl LocalCasStore {
    pub async fn new(base_dir: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        let base_dir = base_dir.into();
        tokio::fs::create_dir_all(&base_dir).await?;
        Ok(Self { base_dir })
    }

    pub fn chunk_path(&self, hash: &Blake3Hash) -> std::path::PathBuf {
        let hex = hash.hex();
        self.base_dir.join(&hex[..2]).join(&hex[2..4]).join(&hex)
    }
}

#[async_trait]
impl CasStore for LocalCasStore {
    async fn put(&self, data: Bytes) -> Result<Blake3Hash> {
        let hash = Blake3Hash::of(&data);
        let path = self.chunk_path(&hash);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if !path.exists() {
            tokio::fs::write(&path, &data).await?;
        }
        Ok(hash)
    }

    async fn get(&self, hash: &Blake3Hash) -> Result<Option<Bytes>> {
        let path = self.chunk_path(hash);
        match tokio::fs::read(&path).await {
            Ok(data) => {
                let actual = Blake3Hash::of(&data);
                if actual != *hash {
                    return Err(CasError::HashMismatch {
                        expected: hash.hex(),
                        actual: actual.hex(),
                    });
                }
                Ok(Some(Bytes::from(data)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CasError::Io(e)),
        }
    }

    async fn has(&self, hash: &Blake3Hash) -> Result<bool> {
        Ok(self.chunk_path(hash).exists())
    }

    async fn put_at(&self, hash: Blake3Hash, data: Bytes) -> Result<()> {
        let path = self.chunk_path(&hash);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, &data).await?;
        Ok(())
    }

    async fn get_raw(&self, hash: &Blake3Hash) -> Result<Option<Bytes>> {
        let path = self.chunk_path(hash);
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Some(Bytes::from(data))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CasError::Io(e)),
        }
    }
}
