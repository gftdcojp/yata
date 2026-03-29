use bytes::Bytes;
use std::sync::Arc;
use yata_core::ObjectMeta;
use yata_core::OcelEventDraft;
use yata_server::{Broker, BrokerConfig};

async fn test_broker(dir: &tempfile::TempDir) -> Arc<Broker> {
    let config = BrokerConfig {
        data_dir: dir.path().join("data"),
        ..Default::default()
    };
    Arc::new(Broker::new(config).await.unwrap())
}

async fn test_broker_with_graph(dir: &tempfile::TempDir) -> Arc<Broker> {
    let config = BrokerConfig {
        data_dir: dir.path().join("data"),
        graph_uri: Some(dir.path().join("graph").to_str().unwrap().to_owned()),
        ..Default::default()
    };
    Arc::new(Broker::new(config).await.unwrap())
}

#[tokio::test]
async fn test_broker_object_store() {
    let dir = tempfile::tempdir().unwrap();
    let broker = test_broker(&dir).await;

    let data = Bytes::from_static(b"object store test payload");
    let manifest = broker
        .objects
        .put_object(
            data.clone(),
            ObjectMeta {
                media_type: "text/plain".to_owned(),
                schema_id: None,
                lineage: vec![],
            },
        )
        .await
        .unwrap();

    let retrieved = broker
        .objects
        .get_object(&manifest.object_id)
        .await
        .unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_broker_ocel_event_graph() {
    let dir = tempfile::tempdir().unwrap();
    let broker = test_broker_with_graph(&dir).await;

    let event = OcelEventDraft::new("OrderCreated")
        .attr("order_id", serde_json::json!("ord-123"))
        .touches("ord-123", "Order");

    let ack = broker
        .publish_ocel_event("ocel.events", event, None)
        .await
        .unwrap();
    assert!(ack.ts_ns > 0);

    // Verify graph node was created
    let json = broker.export_ocel_json().await.unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(parsed.is_array());
}

#[tokio::test]
async fn test_client_object_store() {
    let dir = tempfile::tempdir().unwrap();
    let broker = test_broker(&dir).await;

    let data = Bytes::from(b"binary payload data".to_vec());
    let manifest = broker
        .objects
        .put_object(
            data.clone(),
            ObjectMeta {
                media_type: "application/octet-stream".to_owned(),
                schema_id: None,
                lineage: vec![],
            },
        )
        .await
        .unwrap();

    let retrieved = broker.objects.get_object(&manifest.object_id).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_broker_graph_integration() {
    let dir = tempfile::tempdir().unwrap();
    let broker = test_broker_with_graph(&dir).await;
    assert!(broker.graph.is_some());

    let dir2 = tempfile::tempdir().unwrap();
    let broker2 = test_broker(&dir2).await;
    assert!(broker2.graph.is_none());
}
