#![allow(dead_code)]

//! Local content-addressed object store.
//!
//! Directory layout:
//! ```text
//! cas/
//!   <hash[0..2]>/<hash[2..4]>/<full_hex_hash>   # chunk blobs
//! manifests/
//!   <object_id>.cbor                              # ObjectManifest as CBOR
//! pins/
//!   <object_id>                                   # presence = pinned
//! ```
//!
//! Small objects (< INLINE_THRESHOLD=256KB) are stored as single chunk.
//! Larger objects use content-defined chunking (fixed 4MB for Phase 1).

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use yata_core::{
    Blake3Hash, ChunkRef, ObjectId, ObjectManifest, ObjectMeta, ObjectStorage, Result,
    YataError,
};

pub const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024;
pub const INLINE_THRESHOLD: usize = 256 * 1024;

pub struct LocalObjectStore {
    base_dir: PathBuf,
    chunk_size: usize,
}

impl LocalObjectStore {
    pub async fn new(base_dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        Self::new_with_chunk_size(base_dir, DEFAULT_CHUNK_SIZE).await
    }

    pub async fn new_with_chunk_size(
        base_dir: impl Into<PathBuf>,
        chunk_size: usize,
    ) -> std::io::Result<Self> {
        let base_dir = base_dir.into();
        tokio::fs::create_dir_all(base_dir.join("cas")).await?;
        tokio::fs::create_dir_all(base_dir.join("manifests")).await?;
        tokio::fs::create_dir_all(base_dir.join("pins")).await?;
        Ok(Self { base_dir, chunk_size })
    }

    async fn write_chunk(&self, data: &[u8]) -> std::io::Result<ChunkRef> {
        let hash = Blake3Hash::of(data);
        let path = self.chunk_path(&hash);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if !path.exists() {
            tokio::fs::write(&path, data).await?;
        }
        Ok(ChunkRef {
            seq: 0, // will be set by caller
            hash,
            size_bytes: data.len() as u64,
            offset: 0, // will be set by caller
        })
    }

    async fn read_chunk(&self, chunk: &ChunkRef) -> std::io::Result<Bytes> {
        let path = self.chunk_path(&chunk.hash);
        let data = tokio::fs::read(&path).await?;
        Ok(Bytes::from(data))
    }

    pub fn chunk_path(&self, hash: &Blake3Hash) -> PathBuf {
        let hex = hash.hex();
        self.base_dir
            .join("cas")
            .join(&hex[..2])
            .join(&hex[2..4])
            .join(&hex)
    }

    pub fn manifest_path(&self, id: &ObjectId) -> PathBuf {
        self.base_dir.join("manifests").join(format!("{}.cbor", id))
    }

    pub fn pin_path(&self, id: &ObjectId) -> PathBuf {
        self.base_dir.join("pins").join(id.to_string())
    }

    async fn read_manifest_internal(&self, id: &ObjectId) -> Result<Option<ObjectManifest>> {
        let path = self.manifest_path(id);
        match tokio::fs::read(&path).await {
            Ok(data) => {
                let manifest: ObjectManifest = ciborium::from_reader(std::io::Cursor::new(&data))
                    .map_err(|e| YataError::Serialization(e.to_string()))?;
                Ok(Some(manifest))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(YataError::Io(e)),
        }
    }

    async fn write_manifest_internal(&self, manifest: &ObjectManifest) -> Result<()> {
        let path = self.manifest_path(&manifest.object_id);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut buf = Vec::new();
        ciborium::into_writer(manifest, &mut buf)
            .map_err(|e| YataError::Serialization(e.to_string()))?;
        tokio::fs::write(&path, &buf).await?;
        Ok(())
    }
}

#[async_trait]
impl ObjectStorage for LocalObjectStore {
    async fn put_object(&self, data: Bytes, meta: ObjectMeta) -> Result<ObjectManifest> {
        let overall_hash = Blake3Hash::of(&data);
        let total_size = data.len() as u64;
        let object_id = ObjectId::new();
        let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        let chunks_data: Vec<&[u8]> = if data.len() <= self.chunk_size {
            vec![&data]
        } else {
            data.chunks(self.chunk_size).collect()
        };

        let mut chunks = Vec::new();
        let mut offset = 0u64;
        for (seq, chunk_data) in chunks_data.iter().enumerate() {
            let mut chunk_ref = self.write_chunk(chunk_data).await?;
            chunk_ref.seq = seq as u32;
            chunk_ref.offset = offset;
            offset += chunk_ref.size_bytes;
            chunks.push(chunk_ref);
        }

        let manifest = ObjectManifest {
            object_id,
            content_hash: overall_hash,
            size_bytes: total_size,
            chunks,
            media_type: meta.media_type,
            schema_id: meta.schema_id,
            lineage: meta.lineage,
            created_at_ns: ts_ns,
        };
        self.write_manifest_internal(&manifest).await?;
        Ok(manifest)
    }

    async fn get_object(&self, id: &ObjectId) -> Result<bytes::Bytes> {
        let manifest = self
            .read_manifest_internal(id)
            .await?
            .ok_or_else(|| YataError::NotFound(id.to_string()))?;

        let mut data = Vec::with_capacity(manifest.size_bytes as usize);
        let mut sorted_chunks = manifest.chunks.clone();
        sorted_chunks.sort_by_key(|c| c.seq);
        for chunk in &sorted_chunks {
            let chunk_data = self.read_chunk(chunk).await?;
            data.extend_from_slice(&chunk_data);
        }
        Ok(Bytes::from(data))
    }

    async fn head_object(&self, id: &ObjectId) -> Result<Option<ObjectManifest>> {
        self.read_manifest_internal(id).await
    }

    async fn pin_object(&self, id: &ObjectId) -> Result<()> {
        let path = self.pin_path(id);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        // Touch pin file
        tokio::fs::write(&path, b"").await?;
        Ok(())
    }

    async fn gc(&self) -> Result<u64> {
        // Collect all pinned object IDs
        let pins_dir = self.base_dir.join("pins");
        let mut pinned_ids: HashSet<String> = HashSet::new();
        if let Ok(mut rd) = tokio::fs::read_dir(&pins_dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let name = entry.file_name();
                if let Some(s) = name.to_str() {
                    pinned_ids.insert(s.to_owned());
                }
            }
        }

        // Collect all chunk hashes referenced by pinned manifests
        let manifests_dir = self.base_dir.join("manifests");
        let mut referenced_chunks: HashSet<String> = HashSet::new();
        if let Ok(mut rd) = tokio::fs::read_dir(&manifests_dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let path = entry.path();
                let stem = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_owned();
                if !pinned_ids.contains(&stem) {
                    continue;
                }
                if let Ok(data) = tokio::fs::read(&path).await {
                    if let Ok(manifest) = ciborium::from_reader::<ObjectManifest, _>(
                        std::io::Cursor::new(&data),
                    ) {
                        for chunk in &manifest.chunks {
                            referenced_chunks.insert(chunk.hash.hex());
                        }
                    }
                }
            }
        }

        // Delete unreferenced chunks
        let cas_dir = self.base_dir.join("cas");
        let mut deleted = 0u64;
        deleted += gc_dir_recursive(&cas_dir, &referenced_chunks).await;
        Ok(deleted)
    }
}

async fn gc_dir_recursive(dir: &Path, referenced: &HashSet<String>) -> u64 {
    let mut deleted = 0u64;
    if let Ok(mut rd) = tokio::fs::read_dir(dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            let path = entry.path();
            if path.is_dir() {
                deleted += Box::pin(gc_dir_recursive(&path, referenced)).await;
            } else {
                let name = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_owned();
                if !referenced.contains(&name) {
                    if tokio::fs::remove_file(&path).await.is_ok() {
                        deleted += 1;
                    }
                }
            }
        }
    }
    deleted
}

#[cfg(test)]
mod tests;
