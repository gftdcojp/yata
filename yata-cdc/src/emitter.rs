//! CDC emitter: WalEntry → GraphCdcEvent → broadcast channel.

use yata_core::{CdcEvent, CdcOp};
use yata_store::WalEntry;

/// Graph-specific CDC event with vertex/edge details.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GraphCdcEvent {
    /// Base CDC event (table, op, row_count, ts_ns).
    pub base: CdcEvent,
    /// Vertex ID (for vertex ops) or edge ID (for edge ops).
    pub entity_id: Option<String>,
    /// Label (vertex label or edge rel_type).
    pub label: Option<String>,
    /// Affected property key (for SetProperty ops).
    pub property_key: Option<String>,
    /// WAL version that produced this event.
    pub version: u64,
}

/// Broadcast-based CDC emitter. Subscribers receive GraphCdcEvent in real-time.
pub struct CdcEmitter {
    tx: tokio::sync::broadcast::Sender<GraphCdcEvent>,
}

impl CdcEmitter {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(capacity);
        Self { tx }
    }

    /// Subscribe to CDC events.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<GraphCdcEvent> {
        self.tx.subscribe()
    }

    /// Number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }

    /// Convert a batch of WalEntry to GraphCdcEvents and emit.
    /// Returns the number of events emitted (excluding Commit entries).
    pub fn emit_wal_batch(&self, entries: &[WalEntry], version: u64) -> usize {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        let mut count = 0;
        for entry in entries {
            if let Some(event) = wal_entry_to_cdc(entry, version, now_ns) {
                // Ignore send errors (no subscribers = events dropped)
                let _ = self.tx.send(event);
                count += 1;
            }
        }
        count
    }
}

fn wal_entry_to_cdc(entry: &WalEntry, version: u64, ts_ns: i64) -> Option<GraphCdcEvent> {
    match entry {
        WalEntry::AddVertex { vid, label, .. } => Some(GraphCdcEvent {
            base: CdcEvent {
                table: "graph_vertices".into(),
                op: CdcOp::Insert,
                row_count: 1,
                ts_ns,
            },
            entity_id: Some(format!("{vid}")),
            label: Some(label.clone()),
            property_key: None,
            version,
        }),
        WalEntry::AddEdge {
            edge_id, label, ..
        } => Some(GraphCdcEvent {
            base: CdcEvent {
                table: "graph_edges".into(),
                op: CdcOp::Insert,
                row_count: 1,
                ts_ns,
            },
            entity_id: Some(format!("{edge_id}")),
            label: Some(label.clone()),
            property_key: None,
            version,
        }),
        WalEntry::DeleteVertex { vid } => Some(GraphCdcEvent {
            base: CdcEvent {
                table: "graph_vertices".into(),
                op: CdcOp::Delete,
                row_count: 1,
                ts_ns,
            },
            entity_id: Some(format!("{vid}")),
            label: None,
            property_key: None,
            version,
        }),
        WalEntry::DeleteEdge { edge_id } => Some(GraphCdcEvent {
            base: CdcEvent {
                table: "graph_edges".into(),
                op: CdcOp::Delete,
                row_count: 1,
                ts_ns,
            },
            entity_id: Some(format!("{edge_id}")),
            label: None,
            property_key: None,
            version,
        }),
        WalEntry::SetProperty { vid, key, .. } => Some(GraphCdcEvent {
            base: CdcEvent {
                table: "graph_vertices".into(),
                op: CdcOp::Update,
                row_count: 1,
                ts_ns,
            },
            entity_id: Some(format!("{vid}")),
            label: None,
            property_key: Some(key.clone()),
            version,
        }),
        WalEntry::Commit { .. } => None, // Skip commit markers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_grin::PropValue;

    #[test]
    fn test_wal_to_cdc_add_vertex() {
        let entry = WalEntry::AddVertex {
            vid: 42,
            label: "Person".into(),
            props: vec![("name".into(), PropValue::Str("Alice".into()))],
        };
        let event = wal_entry_to_cdc(&entry, 1, 1000).unwrap();
        assert_eq!(event.base.table, "graph_vertices");
        assert!(matches!(event.base.op, CdcOp::Insert));
        assert_eq!(event.entity_id, Some("42".into()));
        assert_eq!(event.label, Some("Person".into()));
        assert_eq!(event.version, 1);
    }

    #[test]
    fn test_wal_to_cdc_delete_edge() {
        let entry = WalEntry::DeleteEdge { edge_id: 7 };
        let event = wal_entry_to_cdc(&entry, 5, 2000).unwrap();
        assert_eq!(event.base.table, "graph_edges");
        assert!(matches!(event.base.op, CdcOp::Delete));
        assert_eq!(event.entity_id, Some("7".into()));
    }

    #[test]
    fn test_wal_to_cdc_set_property() {
        let entry = WalEntry::SetProperty {
            vid: 3,
            key: "age".into(),
            value: PropValue::Int(30),
        };
        let event = wal_entry_to_cdc(&entry, 2, 3000).unwrap();
        assert!(matches!(event.base.op, CdcOp::Update));
        assert_eq!(event.property_key, Some("age".into()));
    }

    #[test]
    fn test_wal_to_cdc_commit_skipped() {
        let entry = WalEntry::Commit { version: 10 };
        assert!(wal_entry_to_cdc(&entry, 10, 4000).is_none());
    }

    #[tokio::test]
    async fn test_emitter_broadcast() {
        let emitter = CdcEmitter::new(64);
        let mut rx = emitter.subscribe();

        let entries = vec![
            WalEntry::AddVertex {
                vid: 1,
                label: "Person".into(),
                props: vec![],
            },
            WalEntry::AddEdge {
                edge_id: 1,
                src: 1,
                dst: 2,
                label: "KNOWS".into(),
                props: vec![],
            },
            WalEntry::Commit { version: 1 },
        ];

        let emitted = emitter.emit_wal_batch(&entries, 1);
        assert_eq!(emitted, 2); // Commit skipped

        let ev1 = rx.recv().await.unwrap();
        assert_eq!(ev1.base.table, "graph_vertices");

        let ev2 = rx.recv().await.unwrap();
        assert_eq!(ev2.base.table, "graph_edges");
    }

    #[test]
    fn test_emitter_no_subscribers() {
        let emitter = CdcEmitter::new(64);
        // No subscribers — should not panic
        let entries = vec![WalEntry::AddVertex {
            vid: 1,
            label: "Test".into(),
            props: vec![],
        }];
        let emitted = emitter.emit_wal_batch(&entries, 1);
        assert_eq!(emitted, 1);
    }
}
