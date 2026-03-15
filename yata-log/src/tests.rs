use bytes::Bytes;
use futures::StreamExt;
use yata_core::{
    AppendLog, Blake3Hash, Envelope, PayloadRef, PublishRequest, SchemaId, Sequence, StreamId,
    Subject,
};
use crate::LocalLog;

fn make_request(stream: &str, subject: &str, payload: &[u8]) -> PublishRequest {
    let stream_id = StreamId::from(stream);
    let subject_id = Subject::from(subject);
    let payload_bytes = Bytes::copy_from_slice(payload);
    let hash = Blake3Hash::of(payload);
    let schema_id = SchemaId::from("test.schema");
    let envelope = Envelope::new(subject_id.clone(), schema_id, hash);
    PublishRequest {
        stream: stream_id,
        subject: subject_id,
        envelope,
        payload: PayloadRef::InlineBytes(payload_bytes),
        expected_last_seq: None,
    }
}

#[tokio::test]
async fn test_append_and_read_single() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::new(dir.path()).await.unwrap();

    let req = make_request("events", "test.subject", b"hello world");
    let ack = log.append(req).await.unwrap();
    assert_eq!(ack.seq, Sequence(1));

    let stream_id = StreamId::from("events");
    let mut stream = log.read_from(&stream_id, Sequence(1)).await.unwrap();
    let entry = stream.next().await.unwrap().unwrap();
    assert_eq!(entry.seq, Sequence(1));
    assert_eq!(entry.stream_id, StreamId::from("events"));
}

#[tokio::test]
async fn test_append_multiple_and_read_from() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::new(dir.path()).await.unwrap();

    for i in 0..5u64 {
        let req = make_request("mystream", "sub", format!("msg{}", i).as_bytes());
        let ack = log.append(req).await.unwrap();
        assert_eq!(ack.seq, Sequence(i + 1));
    }

    let stream_id = StreamId::from("mystream");
    let entries: Vec<_> = log
        .read_from(&stream_id, Sequence(3))
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert_eq!(entries.len(), 3); // seq 3, 4, 5
    assert_eq!(entries[0].as_ref().unwrap().seq, Sequence(3));
}

#[tokio::test]
async fn test_last_seq() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::new(dir.path()).await.unwrap();

    let stream_id = StreamId::from("mystream");
    assert!(log.last_seq(&stream_id).await.unwrap().is_none());

    log.append(make_request("mystream", "s", b"a")).await.unwrap();
    log.append(make_request("mystream", "s", b"b")).await.unwrap();

    let last = log.last_seq(&stream_id).await.unwrap();
    assert_eq!(last, Some(Sequence(2)));
}

#[tokio::test]
async fn test_seq_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let log = LocalLog::new(dir.path()).await.unwrap();

    log.append(make_request("s", "sub", b"first")).await.unwrap();

    let mut req = make_request("s", "sub", b"second");
    req.expected_last_seq = Some(Sequence(99)); // wrong
    let result = log.append(req).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_payload_store_put_get() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::PayloadStore::new(dir.path()).await.unwrap();

    let data = Bytes::from_static(b"test payload");
    let hash = store.put(&data).await.unwrap();

    let retrieved = store.get(&hash).await.unwrap().unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_payload_store_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let store = crate::PayloadStore::new(dir.path()).await.unwrap();

    let data = Bytes::from_static(b"same content");
    let h1 = store.put(&data).await.unwrap();
    let h2 = store.put(&data).await.unwrap();
    assert_eq!(h1, h2);
}
