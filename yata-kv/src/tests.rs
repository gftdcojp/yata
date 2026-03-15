use bytes::Bytes;
use std::sync::Arc;
use yata_core::{BucketId, KvOp, KvPutRequest, KvStore, Revision};
use yata_log::{LocalLog, PayloadStore};
use crate::KvBucketStore;

async fn make_store(dir: &tempfile::TempDir) -> Arc<KvBucketStore> {
    let log = Arc::new(LocalLog::new(dir.path().join("log")).await.unwrap());
    let ps = Arc::new(PayloadStore::new(dir.path().join("kv_payloads")).await.unwrap());
    Arc::new(KvBucketStore::new(log, ps).await.unwrap())
}

#[tokio::test]
async fn test_put_and_get() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir).await;

    let req = KvPutRequest {
        bucket: BucketId::from("config"),
        key: "foo".to_owned(),
        value: Bytes::from_static(b"bar"),
        expected_revision: None,
        ttl_secs: None,
    };
    let ack = store.put(req).await.unwrap();
    assert_eq!(ack.revision, Revision(1));

    let entry = store.get(&BucketId::from("config"), "foo").await.unwrap().unwrap();
    assert_eq!(entry.value, b"bar");
    assert_eq!(entry.revision, Revision(1));
    assert!(matches!(entry.op, KvOp::Put));
}

#[tokio::test]
async fn test_put_overwrite_increments_revision() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir).await;

    let bucket = BucketId::from("config");

    store.put(KvPutRequest {
        bucket: bucket.clone(),
        key: "key1".to_owned(),
        value: Bytes::from_static(b"v1"),
        expected_revision: None,
        ttl_secs: None,
    }).await.unwrap();

    let ack2 = store.put(KvPutRequest {
        bucket: bucket.clone(),
        key: "key1".to_owned(),
        value: Bytes::from_static(b"v2"),
        expected_revision: None,
        ttl_secs: None,
    }).await.unwrap();
    assert_eq!(ack2.revision, Revision(2));

    let entry = store.get(&bucket, "key1").await.unwrap().unwrap();
    assert_eq!(entry.value, b"v2");
}

#[tokio::test]
async fn test_revision_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir).await;

    let bucket = BucketId::from("b");
    store.put(KvPutRequest {
        bucket: bucket.clone(),
        key: "k".to_owned(),
        value: Bytes::from_static(b"v"),
        expected_revision: None,
        ttl_secs: None,
    }).await.unwrap();

    let result = store.put(KvPutRequest {
        bucket: bucket.clone(),
        key: "k".to_owned(),
        value: Bytes::from_static(b"v2"),
        expected_revision: Some(Revision(99)),
        ttl_secs: None,
    }).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_get_missing_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir).await;
    let result = store.get(&BucketId::from("b"), "no-such-key").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_delete() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir).await;

    let bucket = BucketId::from("b");
    store.put(KvPutRequest {
        bucket: bucket.clone(),
        key: "k".to_owned(),
        value: Bytes::from_static(b"v"),
        expected_revision: None,
        ttl_secs: None,
    }).await.unwrap();

    store.delete(&bucket, "k", None).await.unwrap();
    let result = store.get(&bucket, "k").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_multiple_keys_in_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir).await;

    let bucket = BucketId::from("b");
    for i in 0..5u8 {
        store.put(KvPutRequest {
            bucket: bucket.clone(),
            key: format!("key{}", i),
            value: Bytes::from(vec![i]),
            expected_revision: None,
            ttl_secs: None,
        }).await.unwrap();
    }

    for i in 0..5u8 {
        let entry = store.get(&bucket, &format!("key{}", i)).await.unwrap().unwrap();
        assert_eq!(entry.value, vec![i]);
    }
}

/// Core replay test: data written to one KvBucketStore instance must be
/// recoverable by a new instance pointing at the same directory after restart.
#[tokio::test]
async fn test_replay_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let bucket = BucketId::from("config");

    // Write phase
    {
        let store = make_store(&dir).await;
        store.put(KvPutRequest {
            bucket: bucket.clone(),
            key: "setting".to_owned(),
            value: Bytes::from_static(b"enabled"),
            expected_revision: None,
            ttl_secs: None,
        }).await.unwrap();
        store.put(KvPutRequest {
            bucket: bucket.clone(),
            key: "setting".to_owned(),
            value: Bytes::from_static(b"disabled"),
            expected_revision: None,
            ttl_secs: None,
        }).await.unwrap();
        store.put(KvPutRequest {
            bucket: bucket.clone(),
            key: "other".to_owned(),
            value: Bytes::from_static(b"value"),
            expected_revision: None,
            ttl_secs: None,
        }).await.unwrap();
        store.delete(&bucket, "other", None).await.unwrap();
    }

    // Restart: new instance, no in-memory state
    {
        let log = Arc::new(LocalLog::new(dir.path().join("log")).await.unwrap());
        log.recover().await.unwrap();
        let ps = Arc::new(PayloadStore::new(dir.path().join("kv_payloads")).await.unwrap());
        let store = Arc::new(KvBucketStore::new(log, ps).await.unwrap());

        store.load_snapshot(&bucket).await.unwrap();

        // Latest value for "setting" should be "disabled" (revision 2)
        let entry = store.get(&bucket, "setting").await.unwrap();
        assert!(entry.is_some(), "setting should be present after replay");
        assert_eq!(entry.unwrap().value, b"disabled");

        // "other" was deleted — should be absent
        let deleted = store.get(&bucket, "other").await.unwrap();
        assert!(deleted.is_none(), "deleted key should not appear after replay");
    }
}

#[tokio::test]
async fn test_history() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(&dir).await;
    let bucket = BucketId::from("b");

    for i in 0..3u8 {
        store.put(KvPutRequest {
            bucket: bucket.clone(),
            key: "k".to_owned(),
            value: Bytes::from(vec![i]),
            expected_revision: None,
            ttl_secs: None,
        }).await.unwrap();
    }

    let history = store.history(&bucket, "k").await.unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].value, vec![0]);
    assert_eq!(history[2].value, vec![2]);
}
