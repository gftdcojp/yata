use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::types::{YataRequest, YataResponse};

#[async_trait]
pub trait StateMachineApplier: Send + Sync + 'static {
    async fn apply_publish(
        &self, topic: &str, partition: u32, message_id: &str,
        schema_id: Option<&str>, content_hash: &str, headers_json: Option<&str>,
        ts_ns: i64, payload_ipc: &[u8],
    ) -> Result<(u64, i64), String>;

    async fn apply_create_topic(
        &self, name: &str, num_partitions: u32, replication_factor: u32, retention_hours: Option<u64>,
    ) -> Result<(), String>;

    async fn apply_delete_topic(&self, name: &str) -> Result<(), String>;

    async fn apply_commit_offset(
        &self, group: &str, topic: &str, partition: u32, offset: u64,
    ) -> Result<(), String>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub request: YataRequest,
}

#[derive(Debug)]
pub struct MemLogStore {
    inner: Mutex<LogStoreInner>,
}

#[derive(Debug, Default)]
struct LogStoreInner {
    log: BTreeMap<u64, LogEntry>,
    last_applied: u64,
    current_term: u64,
    voted_for: Option<u64>,
}

impl Default for MemLogStore {
    fn default() -> Self {
        Self { inner: Mutex::new(LogStoreInner::default()) }
    }
}

impl MemLogStore {
    pub fn new() -> Self { Self::default() }

    pub async fn append(&self, entry: LogEntry) {
        self.inner.lock().await.log.insert(entry.index, entry);
    }

    pub async fn get(&self, index: u64) -> Option<LogEntry> {
        self.inner.lock().await.log.get(&index).cloned()
    }

    pub async fn last_index(&self) -> u64 {
        self.inner.lock().await.log.keys().next_back().copied().unwrap_or(0)
    }

    pub async fn last_term(&self) -> u64 {
        self.inner.lock().await.log.values().next_back().map(|e| e.term).unwrap_or(0)
    }

    pub async fn entries_from(&self, start: u64) -> Vec<LogEntry> {
        self.inner.lock().await.log.range(start..).map(|(_, v)| v.clone()).collect()
    }

    pub async fn truncate_from(&self, index: u64) {
        let mut inner = self.inner.lock().await;
        let keys: Vec<u64> = inner.log.range(index..).map(|(k, _)| *k).collect();
        for k in keys { inner.log.remove(&k); }
    }

    pub async fn set_term(&self, term: u64) { self.inner.lock().await.current_term = term; }
    pub async fn current_term(&self) -> u64 { self.inner.lock().await.current_term }
    pub async fn set_voted_for(&self, node: Option<u64>) { self.inner.lock().await.voted_for = node; }
    pub async fn voted_for(&self) -> Option<u64> { self.inner.lock().await.voted_for }
    pub async fn last_applied(&self) -> u64 { self.inner.lock().await.last_applied }
    pub async fn set_last_applied(&self, index: u64) { self.inner.lock().await.last_applied = index; }
}

pub struct YataStateMachine {
    applier: Arc<dyn StateMachineApplier>,
}

impl YataStateMachine {
    pub fn new(applier: Arc<dyn StateMachineApplier>) -> Self { Self { applier } }

    pub async fn apply(&self, req: &YataRequest) -> YataResponse {
        match req {
            YataRequest::Publish { topic, partition, message_id, schema_id, content_hash, headers_json, ts_ns, payload_ipc } => {
                match self.applier.apply_publish(topic, *partition, message_id, schema_id.as_deref(), content_hash, headers_json.as_deref(), *ts_ns, payload_ipc).await {
                    Ok((offset, ts_ns)) => YataResponse::Publish { offset, ts_ns },
                    Err(e) => YataResponse::Error(e),
                }
            }
            YataRequest::CreateTopic { name, num_partitions, replication_factor, retention_hours } => {
                match self.applier.apply_create_topic(name, *num_partitions, *replication_factor, *retention_hours).await {
                    Ok(()) => YataResponse::TopicCreated,
                    Err(e) => YataResponse::Error(e),
                }
            }
            YataRequest::DeleteTopic { name } => {
                match self.applier.apply_delete_topic(name).await {
                    Ok(()) => YataResponse::TopicDeleted,
                    Err(e) => YataResponse::Error(e),
                }
            }
            YataRequest::CommitOffset { group, topic, partition, offset } => {
                match self.applier.apply_commit_offset(group, topic, *partition, *offset).await {
                    Ok(()) => YataResponse::OffsetCommitted,
                    Err(e) => YataResponse::Error(e),
                }
            }
        }
    }
}
