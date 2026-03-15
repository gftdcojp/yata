use std::sync::Arc;
use arrow_array::{Int64Array, StringArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use yata_arrow::ArrowBatchHandle;
use yata_core::{AppendLog, BucketId, KvOp, KvPutRequest, KvStore, ObjectMeta, ObjectStorage, Sequence, StreamId};
use yata_ocel::{OcelEventDraft, OcelProjector};
use yata_server::{Broker, BrokerBackend, BrokerConfig};
use yata_client::YataClient;

async fn test_broker(dir: &tempfile::TempDir) -> Arc<Broker> {
    let config = BrokerConfig {
        data_dir: dir.path().join("data"),
        lance_uri: dir.path().join("lance").to_str().unwrap().to_owned(),
        lance_flush_interval_ms: 60_000, // no auto-flush in tests
    };
    Arc::new(Broker::new(config).await.unwrap())
}

fn make_batch() -> ArrowBatchHandle {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["row1", "row2", "row3"])),
            Arc::new(Int64Array::from(vec![10i64, 20, 30])),
        ],
    ).unwrap();
    ArrowBatchHandle::from_batch(batch).unwrap()
}

#[tokio::test]
async fn test_publish_arrow_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let broker = test_broker(&dir).await;

    let batch = make_batch();
    let ack = broker.publish_arrow("events", "test.event", batch).await.unwrap();
    assert_eq!(ack.seq, Sequence(1));

    let stream_id = StreamId::from("events");
    let last = broker.log.last_seq(&stream_id).await.unwrap();
    assert_eq!(last, Some(Sequence(1)));
}

#[tokio::test]
async fn test_broker_kv_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let broker = test_broker(&dir).await;

    // put
    broker.kv.put(KvPutRequest {
        bucket: BucketId::from("cfg"),
        key: "setting.debug".to_owned(),
        value: Bytes::from_static(b"true"),
        expected_revision: None,
        ttl_secs: None,
    }).await.unwrap();

    // get
    let entry = broker.kv.get(&BucketId::from("cfg"), "setting.debug").await.unwrap().unwrap();
    assert_eq!(entry.value, b"true");
    assert!(matches!(entry.op, KvOp::Put));

    // overwrite
    broker.kv.put(KvPutRequest {
        bucket: BucketId::from("cfg"),
        key: "setting.debug".to_owned(),
        value: Bytes::from_static(b"false"),
        expected_revision: None,
        ttl_secs: None,
    }).await.unwrap();

    let entry2 = broker.kv.get(&BucketId::from("cfg"), "setting.debug").await.unwrap().unwrap();
    assert_eq!(entry2.value, b"false");
}

#[tokio::test]
async fn test_broker_object_store() {
    let dir = tempfile::tempdir().unwrap();
    let broker = test_broker(&dir).await;

    let data = Bytes::from_static(b"object store test payload");
    let manifest = broker.objects.put_object(data.clone(), ObjectMeta {
        media_type: "text/plain".to_owned(),
        schema_id: None,
        lineage: vec![],
    }).await.unwrap();

    let retrieved = broker.objects.get_object(&manifest.object_id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_broker_ocel_event() {
    let dir = tempfile::tempdir().unwrap();
    let broker = test_broker(&dir).await;

    let event = OcelEventDraft::new("OrderCreated")
        .attr("order_id", serde_json::json!("ord-123"))
        .touches("ord-123", "Order");

    let ack = broker.publish_ocel_event("ocel.events", event, None).await.unwrap();
    assert_eq!(ack.seq, Sequence(1));

    let snap = broker.ocel.snapshot().await.unwrap();
    assert_eq!(snap.events.len(), 1);
    assert_eq!(snap.events[0].event_type, "OrderCreated");
    assert_eq!(snap.event_object_edges.len(), 1);
}

#[tokio::test]
async fn test_client_backend_publish_and_kv() {
    let dir = tempfile::tempdir().unwrap();
    let broker = test_broker(&dir).await;
    let client = BrokerBackend::new(broker).into_client();

    // publish arrow
    let ack = client.publish_arrow("stream1", "subject1", make_batch(), Default::default()).await.unwrap();
    assert_eq!(ack.seq, Sequence(1));

    // kv round-trip
    client.kv_put("bucket", "mykey", Bytes::from_static(b"myval"), None).await.unwrap();
    let entry = client.kv_get("bucket", "mykey").await.unwrap().unwrap();
    assert_eq!(entry.value, b"myval");
}

#[tokio::test]
async fn test_client_ocel_and_export() {
    let dir = tempfile::tempdir().unwrap();
    let broker = test_broker(&dir).await;
    let client = BrokerBackend::new(broker).into_client();

    client.publish_ocel_event(
        "ocel.events",
        OcelEventDraft::new("PaymentCaptured")
            .attr("amount", serde_json::json!(50.0))
            .touches("inv-1", "Invoice"),
        None,
    ).await.unwrap();

    let json = client.export_ocel_json("ocel.events").await.unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    let events = parsed["events"].as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["type"], "PaymentCaptured");
}

#[tokio::test]
async fn test_client_object_store() {
    let dir = tempfile::tempdir().unwrap();
    let broker = test_broker(&dir).await;
    let client = BrokerBackend::new(broker).into_client();

    let data = Bytes::from(b"binary payload data".to_vec());
    let manifest = client.put_object(data.clone(), ObjectMeta {
        media_type: "application/octet-stream".to_owned(),
        schema_id: None,
        lineage: vec![],
    }).await.unwrap();

    let retrieved = client.get_object(&manifest.object_id).await.unwrap();
    assert_eq!(retrieved, data);
}
