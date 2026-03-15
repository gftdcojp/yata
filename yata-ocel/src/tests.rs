use yata_core::Result;
use crate::{
    MemoryOcelProjector, OcelEvent, OcelEventDraft, OcelEventObjectEdge, OcelObject,
    OcelObjectObjectEdge, OcelProjector, export_json,
};

fn make_event(id: &str, event_type: &str) -> OcelEvent {
    OcelEvent {
        event_id: id.to_owned(),
        event_type: event_type.to_owned(),
        timestamp: chrono::Utc::now(),
        attrs: indexmap::IndexMap::new(),
        message_id: None,
        stream_id: None,
        seq: None,
    }
}

fn make_object(id: &str, object_type: &str) -> OcelObject {
    OcelObject {
        object_id: id.to_owned(),
        object_type: object_type.to_owned(),
        attrs: indexmap::IndexMap::new(),
        state_hash: None,
        valid_from: chrono::Utc::now(),
        valid_to: None,
    }
}

#[tokio::test]
async fn test_project_events_and_snapshot() {
    let proj = MemoryOcelProjector::new();

    proj.project_event(make_event("e1", "OrderCreated")).await.unwrap();
    proj.project_event(make_event("e2", "PaymentCaptured")).await.unwrap();

    let snap = proj.snapshot().await.unwrap();
    assert_eq!(snap.events.len(), 2);
    assert_eq!(snap.events[0].event_id, "e1");
    assert_eq!(snap.events[1].event_id, "e2");
}

#[tokio::test]
async fn test_project_objects_upsert() {
    let proj = MemoryOcelProjector::new();

    proj.project_object(make_object("o1", "Order")).await.unwrap();
    // Update same object
    let mut updated = make_object("o1", "Order");
    updated.attrs.insert("status".to_owned(), serde_json::json!("paid"));
    proj.project_object(updated).await.unwrap();

    let snap = proj.snapshot().await.unwrap();
    assert_eq!(snap.objects.len(), 1);
    assert_eq!(snap.objects[0].attrs.get("status").unwrap(), &serde_json::json!("paid"));
}

#[tokio::test]
async fn test_e2o_edges() {
    let proj = MemoryOcelProjector::new();
    proj.project_event(make_event("e1", "OrderCreated")).await.unwrap();
    proj.project_object(make_object("o1", "Order")).await.unwrap();

    proj.add_e2o(OcelEventObjectEdge {
        event_id: "e1".to_owned(),
        object_id: "o1".to_owned(),
        qualifier: "created".to_owned(),
        role: None,
    }).await.unwrap();

    let snap = proj.snapshot().await.unwrap();
    assert_eq!(snap.event_object_edges.len(), 1);
    assert_eq!(snap.event_object_edges[0].event_id, "e1");
}

#[tokio::test]
async fn test_o2o_edges() {
    let proj = MemoryOcelProjector::new();
    proj.project_object(make_object("o1", "Order")).await.unwrap();
    proj.project_object(make_object("o2", "Invoice")).await.unwrap();

    proj.add_o2o(OcelObjectObjectEdge {
        src_object_id: "o1".to_owned(),
        dst_object_id: "o2".to_owned(),
        rel_type: "HAS_INVOICE".to_owned(),
        qualifier: None,
        valid_from: chrono::Utc::now(),
        valid_to: None,
    }).await.unwrap();

    let snap = proj.snapshot().await.unwrap();
    assert_eq!(snap.object_object_edges.len(), 1);
}

#[tokio::test]
async fn test_export_json_valid() {
    let proj = MemoryOcelProjector::new();
    proj.project_event(make_event("e1", "OrderCreated")).await.unwrap();
    proj.project_object(make_object("o1", "Order")).await.unwrap();

    let snap = proj.snapshot().await.unwrap();
    let json_str = export_json(&snap).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();

    assert!(parsed.get("events").is_some());
    assert!(parsed.get("objects").is_some());
    let events = parsed["events"].as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["id"], "e1");
}

#[tokio::test]
async fn test_ocel_event_draft_builder() {
    let draft = OcelEventDraft::new("PaymentCaptured")
        .attr("amount", serde_json::json!(100.0))
        .touches("order-1", "Order")
        .touches_with_role("invoice-1", "Invoice", "source");

    assert_eq!(draft.event_type, "PaymentCaptured");
    assert_eq!(draft.attrs.get("amount").unwrap(), &serde_json::json!(100.0));
    assert_eq!(draft.object_refs.len(), 2);
    assert_eq!(draft.object_refs[1].role, Some("source".to_owned()));
}
