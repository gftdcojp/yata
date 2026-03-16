use arrow_schema::Schema;
use std::collections::HashMap;
use std::sync::Arc;
use yata_lance::schema::{
    blobs_schema, event_object_edges_schema, kv_history_schema, messages_schema,
    object_object_edges_schema, ocel_events_schema, ocel_objects_schema,
};

pub const KNOWN_TABLES: &[&str] = &[
    "yata_messages",
    "yata_events",
    "yata_objects",
    "yata_event_object_edges",
    "yata_object_object_edges",
    "yata_kv_history",
    "yata_blobs",
];

pub struct YataTableCatalog {
    pub base_uri: String,
    schemas: HashMap<&'static str, Arc<Schema>>,
}

impl YataTableCatalog {
    pub fn new(base_uri: impl Into<String>) -> Self {
        let mut schemas: HashMap<&'static str, Arc<Schema>> = HashMap::new();
        schemas.insert("yata_messages", messages_schema());
        schemas.insert("yata_events", ocel_events_schema());
        schemas.insert("yata_objects", ocel_objects_schema());
        schemas.insert("yata_event_object_edges", event_object_edges_schema());
        schemas.insert("yata_object_object_edges", object_object_edges_schema());
        schemas.insert("yata_kv_history", kv_history_schema());
        schemas.insert("yata_blobs", blobs_schema());
        Self {
            base_uri: base_uri.into(),
            schemas,
        }
    }

    pub fn dataset_uri(&self, table: &str) -> String {
        format!("{}/{}", self.base_uri.trim_end_matches('/'), table)
    }

    pub fn schema(&self, table: &str) -> Option<Arc<Schema>> {
        self.schemas.get(table).cloned()
    }

    pub fn tables(&self) -> &[&'static str] {
        KNOWN_TABLES
    }
}
