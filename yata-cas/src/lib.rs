#![allow(dead_code)]

//! Content-addressed chunk storage for YATA.
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
    /// Store `data`, returning its Blake3 hash.
    async fn put(&self, data: Bytes) -> Result<Blake3Hash>;

    /// Retrieve chunk by hash. Returns `None` if not found.
    async fn get(&self, hash: &Blake3Hash) -> Result<Option<Bytes>>;

    /// Check existence without loading data.
    async fn has(&self, hash: &Blake3Hash) -> Result<bool>;
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
        // Only write if not already present (dedup)
        if !path.exists() {
            tokio::fs::write(&path, &data).await?;
        }
        Ok(hash)
    }

    async fn get(&self, hash: &Blake3Hash) -> Result<Option<Bytes>> {
        let path = self.chunk_path(hash);
        match tokio::fs::read(&path).await {
            Ok(data) => {
                // Verify hash on read
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_get_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalCasStore::new(dir.path()).await.unwrap();

        let data = Bytes::from_static(b"hello yata cas");
        let hash = store.put(data.clone()).await.unwrap();
        let retrieved = store.get(&hash).await.unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_has() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalCasStore::new(dir.path()).await.unwrap();

        let hash = Blake3Hash::of(b"not stored");
        assert!(!store.has(&hash).await.unwrap());

        store.put(Bytes::from_static(b"not stored")).await.unwrap();
        assert!(store.has(&hash).await.unwrap());
    }

    #[tokio::test]
    async fn test_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalCasStore::new(dir.path()).await.unwrap();

        let data = Bytes::from_static(b"same bytes");
        let h1 = store.put(data.clone()).await.unwrap();
        let h2 = store.put(data.clone()).await.unwrap();
        assert_eq!(h1, h2);
    }

    #[tokio::test]
    async fn test_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalCasStore::new(dir.path()).await.unwrap();

        let hash = Blake3Hash::of(b"ghost");
        let result = store.get(&hash).await.unwrap();
        assert!(result.is_none());
    }
}
