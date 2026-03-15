#![allow(dead_code)]

pub use types::*;
pub use draft::*;
pub use projector::*;
pub use export::*;

// ---- types --------------------------------------------------------------

pub mod types {
    /// OCEL v2 event.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct OcelEvent {
        pub event_id: String,
        pub event_type: String,
        pub timestamp: chrono::DateTime<chrono::Utc>,
        pub attrs: indexmap::IndexMap<String, serde_json::Value>,
        pub message_id: Option<String>,
        pub stream_id: Option<String>,
        pub seq: Option<u64>,
    }

    /// OCEL v2 object.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct OcelObject {
        pub object_id: String,
        pub object_type: String,
        pub attrs: indexmap::IndexMap<String, serde_json::Value>,
        pub state_hash: Option<String>,
        pub valid_from: chrono::DateTime<chrono::Utc>,
        pub valid_to: Option<chrono::DateTime<chrono::Utc>>,
    }

    /// OCEL v2 event→object edge (E2O).
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct OcelEventObjectEdge {
        pub event_id: String,
        pub object_id: String,
        pub qualifier: String,
        pub role: Option<String>,
    }

    /// OCEL v2 object→object edge (O2O).
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct OcelObjectObjectEdge {
        pub src_object_id: String,
        pub dst_object_id: String,
        pub rel_type: String,
        pub qualifier: Option<String>,
        pub valid_from: chrono::DateTime<chrono::Utc>,
        pub valid_to: Option<chrono::DateTime<chrono::Utc>>,
    }

    /// Complete OCEL v2 log.
    #[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
    pub struct OcelLog {
        pub events: Vec<OcelEvent>,
        pub objects: Vec<OcelObject>,
        pub event_object_edges: Vec<OcelEventObjectEdge>,
        pub object_object_edges: Vec<OcelObjectObjectEdge>,
    }
}

// ---- draft --------------------------------------------------------------

pub mod draft {
    use yata_core::OcelObjectRef;

    #[derive(Clone, Debug, Default)]
    pub struct OcelEventDraft {
        pub event_type: String,
        pub attrs: indexmap::IndexMap<String, serde_json::Value>,
        pub object_refs: Vec<OcelObjectRef>,
    }

    impl OcelEventDraft {
        pub fn new(event_type: impl Into<String>) -> Self {
            Self {
                event_type: event_type.into(),
                attrs: indexmap::IndexMap::new(),
                object_refs: Vec::new(),
            }
        }

        pub fn attr(
            mut self,
            key: impl Into<String>,
            value: impl Into<serde_json::Value>,
        ) -> Self {
            self.attrs.insert(key.into(), value.into());
            self
        }

        pub fn touches(
            mut self,
            object_id: impl Into<String>,
            object_type: impl Into<String>,
        ) -> Self {
            self.object_refs.push(OcelObjectRef {
                object_id: object_id.into(),
                object_type: object_type.into(),
                qualifier: None,
                role: None,
            });
            self
        }

        pub fn touches_with_role(
            mut self,
            object_id: impl Into<String>,
            object_type: impl Into<String>,
            role: impl Into<String>,
        ) -> Self {
            self.object_refs.push(OcelObjectRef {
                object_id: object_id.into(),
                object_type: object_type.into(),
                qualifier: None,
                role: Some(role.into()),
            });
            self
        }
    }
}

// ---- projector ----------------------------------------------------------

pub mod projector {
    use super::types::*;
    use async_trait::async_trait;
    use tokio::sync::RwLock;
    use yata_core::Result;

    #[async_trait]
    pub trait OcelProjector: Send + Sync + 'static {
        async fn project_event(&self, event: OcelEvent) -> Result<()>;
        async fn project_object(&self, object: OcelObject) -> Result<()>;
        async fn add_e2o(&self, edge: OcelEventObjectEdge) -> Result<()>;
        async fn add_o2o(&self, edge: OcelObjectObjectEdge) -> Result<()>;
        async fn snapshot(&self) -> Result<OcelLog>;
    }

    /// Simple in-memory OCEL projector (Phase 1).
    pub struct MemoryOcelProjector {
        log: RwLock<OcelLog>,
    }

    impl MemoryOcelProjector {
        pub fn new() -> Self {
            Self {
                log: RwLock::new(OcelLog::default()),
            }
        }
    }

    impl Default for MemoryOcelProjector {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl OcelProjector for MemoryOcelProjector {
        async fn project_event(&self, event: OcelEvent) -> Result<()> {
            let mut log = self.log.write().await;
            log.events.push(event);
            Ok(())
        }

        async fn project_object(&self, object: OcelObject) -> Result<()> {
            let mut log = self.log.write().await;
            // Update existing or insert
            if let Some(existing) = log.objects.iter_mut().find(|o| o.object_id == object.object_id) {
                *existing = object;
            } else {
                log.objects.push(object);
            }
            Ok(())
        }

        async fn add_e2o(&self, edge: OcelEventObjectEdge) -> Result<()> {
            let mut log = self.log.write().await;
            log.event_object_edges.push(edge);
            Ok(())
        }

        async fn add_o2o(&self, edge: OcelObjectObjectEdge) -> Result<()> {
            let mut log = self.log.write().await;
            log.object_object_edges.push(edge);
            Ok(())
        }

        async fn snapshot(&self) -> Result<OcelLog> {
            let log = self.log.read().await;
            Ok(log.clone())
        }
    }
}

// ---- export -------------------------------------------------------------

pub mod export {
    use super::types::OcelLog;
    use tokio::io::AsyncWriteExt;
    use yata_core::{Result, YataError};

    /// Export OcelLog to OCEL 2.0 JSON format.
    pub fn export_json(log: &OcelLog) -> serde_json::Result<String> {
        let doc = ocel_to_json_doc(log);
        serde_json::to_string_pretty(&doc)
    }

    /// Export OcelLog to OCEL 2.0 JSON format, writing to a writer.
    pub async fn export_json_to<W: tokio::io::AsyncWrite + Unpin>(
        log: &OcelLog,
        writer: &mut W,
    ) -> Result<()> {
        let json = export_json(log)
            .map_err(|e| YataError::Serialization(e.to_string()))?;
        writer
            .write_all(json.as_bytes())
            .await
            .map_err(YataError::Io)?;
        Ok(())
    }

    fn ocel_to_json_doc(log: &OcelLog) -> serde_json::Value {
        use serde_json::{json, Value};

        // Build event types and object types from data
        let mut event_types: std::collections::HashMap<String, serde_json::Value> =
            std::collections::HashMap::new();
        for ev in &log.events {
            event_types
                .entry(ev.event_type.clone())
                .or_insert_with(|| json!({"attributes": []}));
        }

        let mut object_types: std::collections::HashMap<String, serde_json::Value> =
            std::collections::HashMap::new();
        for obj in &log.objects {
            object_types
                .entry(obj.object_type.clone())
                .or_insert_with(|| json!({"attributes": []}));
        }

        let events_json: Vec<Value> = log
            .events
            .iter()
            .map(|ev| {
                let e2o: Vec<Value> = log
                    .event_object_edges
                    .iter()
                    .filter(|e| e.event_id == ev.event_id)
                    .map(|e| {
                        json!({
                            "objectId": e.object_id,
                            "qualifier": e.qualifier,
                        })
                    })
                    .collect();
                json!({
                    "id": ev.event_id,
                    "type": ev.event_type,
                    "time": ev.timestamp.to_rfc3339(),
                    "attributes": ev.attrs,
                    "relationships": e2o,
                })
            })
            .collect();

        let objects_json: Vec<Value> = log
            .objects
            .iter()
            .map(|obj| {
                let o2o: Vec<Value> = log
                    .object_object_edges
                    .iter()
                    .filter(|e| e.src_object_id == obj.object_id)
                    .map(|e| {
                        json!({
                            "objectId": e.dst_object_id,
                            "qualifier": e.rel_type,
                        })
                    })
                    .collect();
                json!({
                    "id": obj.object_id,
                    "type": obj.object_type,
                    "attributes": obj.attrs,
                    "relationships": o2o,
                })
            })
            .collect();

        json!({
            "objectTypes": object_types,
            "eventTypes": event_types,
            "objects": objects_json,
            "events": events_json,
        })
    }
}

#[cfg(test)]
mod tests;
