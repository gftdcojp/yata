use bytes::Bytes;
use yata_core::{ObjectMeta, ObjectStorage};
use crate::LocalObjectStore;

fn default_meta() -> ObjectMeta {
    ObjectMeta {
        media_type: "application/octet-stream".to_owned(),
        schema_id: None,
        lineage: vec![],
    }
}

#[tokio::test]
async fn test_put_get_small_object() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(dir.path()).await.unwrap();

    let data = Bytes::from_static(b"small object content");
    let manifest = store.put_object(data.clone(), default_meta()).await.unwrap();

    assert_eq!(manifest.size_bytes, data.len() as u64);
    assert_eq!(manifest.chunks.len(), 1);

    let retrieved = store.get_object(&manifest.object_id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_head_object() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(dir.path()).await.unwrap();

    let data = Bytes::from_static(b"some data");
    let manifest = store.put_object(data, default_meta()).await.unwrap();

    let head = store.head_object(&manifest.object_id).await.unwrap();
    assert!(head.is_some());
    assert_eq!(head.unwrap().object_id, manifest.object_id);
}

#[tokio::test]
async fn test_head_missing_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(dir.path()).await.unwrap();

    let id = yata_core::ObjectId::new();
    let result = store.head_object(&id).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_put_large_object_multiple_chunks() {
    let dir = tempfile::tempdir().unwrap();
    // Use small chunk size to force multiple chunks
    let store = LocalObjectStore::new_with_chunk_size(dir.path(), 1024).await.unwrap();

    // 5KB object → 5 chunks of 1KB
    let data = Bytes::from(vec![42u8; 5 * 1024]);
    let manifest = store.put_object(data.clone(), default_meta()).await.unwrap();

    assert_eq!(manifest.size_bytes, data.len() as u64);
    assert_eq!(manifest.chunks.len(), 5);

    let retrieved = store.get_object(&manifest.object_id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_content_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(dir.path()).await.unwrap();

    let data = Bytes::from_static(b"dedup content");
    let m1 = store.put_object(data.clone(), default_meta()).await.unwrap();
    let m2 = store.put_object(data.clone(), default_meta()).await.unwrap();

    // Same content hash, different object IDs (manifests are per put), same chunk hashes
    assert_eq!(m1.content_hash, m2.content_hash);
    assert_ne!(m1.object_id, m2.object_id);
}

#[tokio::test]
async fn test_pin_and_gc() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(dir.path()).await.unwrap();

    let data = Bytes::from_static(b"gc test data");
    let manifest = store.put_object(data, default_meta()).await.unwrap();

    // Pin the object
    store.pin_object(&manifest.object_id).await.unwrap();

    // GC should return 0 deleted (object is pinned)
    let deleted = store.gc().await.unwrap();
    assert_eq!(deleted, 0);

    // Object still accessible
    let retrieved = store.get_object(&manifest.object_id).await.unwrap();
    assert!(!retrieved.is_empty());
}
