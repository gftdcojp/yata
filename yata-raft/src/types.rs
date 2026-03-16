use std::fmt;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum YataRequest {
    Publish {
        topic: String,
        partition: u32,
        message_id: String,
        schema_id: Option<String>,
        content_hash: String,
        headers_json: Option<String>,
        ts_ns: i64,
        payload_ipc: Vec<u8>,
    },
    CreateTopic {
        name: String,
        num_partitions: u32,
        replication_factor: u32,
        retention_hours: Option<u64>,
    },
    DeleteTopic { name: String },
    CommitOffset {
        group: String,
        topic: String,
        partition: u32,
        offset: u64,
    },
}

impl fmt::Display for YataRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Publish { topic, partition, message_id, .. } => {
                write!(f, "Publish({}/{} msg={})", topic, partition, message_id)
            }
            Self::CreateTopic { name, num_partitions, .. } => {
                write!(f, "CreateTopic({} p={})", name, num_partitions)
            }
            Self::DeleteTopic { name } => write!(f, "DeleteTopic({})", name),
            Self::CommitOffset { group, topic, partition, offset } => {
                write!(f, "CommitOffset({}/{}/{} @{})", group, topic, partition, offset)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum YataResponse {
    Publish { offset: u64, ts_ns: i64 },
    TopicCreated,
    TopicDeleted,
    OffsetCommitted,
    Error(String),
}

impl fmt::Display for YataResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Publish { offset, ts_ns } => write!(f, "Publish(off={} ts={})", offset, ts_ns),
            Self::TopicCreated => write!(f, "TopicCreated"),
            Self::TopicDeleted => write!(f, "TopicDeleted"),
            Self::OffsetCommitted => write!(f, "OffsetCommitted"),
            Self::Error(e) => write!(f, "Error({})", e),
        }
    }
}
