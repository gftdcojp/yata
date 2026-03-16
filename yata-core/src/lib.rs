#![allow(dead_code)]

// ---- Forward declarations for inline modules ---------------------------
// All modules are defined inline below; pub use re-exports at crate root.

pub use self::ids::*;
pub use self::hash::*;
pub use self::envelope::*;
pub use self::payload::*;
pub use self::log::*;
pub use self::object::*;
pub use self::kv::*;
pub use self::ack::*;
pub use self::publish::*;
pub use self::error::*;
pub use self::traits::{AppendLog, KvStore, ObjectStorage};

// ---- ids ---------------------------------------------------------------

pub mod ids {
    use std::fmt;

    #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct StreamId(pub String);

    impl fmt::Display for StreamId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl From<&str> for StreamId {
        fn from(s: &str) -> Self { Self(s.to_owned()) }
    }
    impl From<String> for StreamId {
        fn from(s: String) -> Self { Self(s) }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct Subject(pub String);

    impl fmt::Display for Subject {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl From<&str> for Subject {
        fn from(s: &str) -> Self { Self(s.to_owned()) }
    }
    impl From<String> for Subject {
        fn from(s: String) -> Self { Self(s) }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct BucketId(pub String);

    impl fmt::Display for BucketId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl From<&str> for BucketId {
        fn from(s: &str) -> Self { Self(s.to_owned()) }
    }
    impl From<String> for BucketId {
        fn from(s: String) -> Self { Self(s) }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct ObjectId(pub uuid::Uuid);

    impl ObjectId {
        pub fn new() -> Self { Self(uuid::Uuid::new_v4()) }
    }

    impl Default for ObjectId {
        fn default() -> Self { Self::new() }
    }

    impl fmt::Display for ObjectId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl From<uuid::Uuid> for ObjectId {
        fn from(u: uuid::Uuid) -> Self { Self(u) }
    }

    impl std::str::FromStr for ObjectId {
        type Err = uuid::Error;
        fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
            Ok(Self(uuid::Uuid::parse_str(s)?))
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct MessageId(pub uuid::Uuid);

    impl MessageId {
        pub fn new() -> Self { Self(uuid::Uuid::new_v4()) }
    }

    impl Default for MessageId {
        fn default() -> Self { Self::new() }
    }

    impl fmt::Display for MessageId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct SchemaId(pub String);

    impl fmt::Display for SchemaId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl From<&str> for SchemaId {
        fn from(s: &str) -> Self { Self(s.to_owned()) }
    }
    impl From<String> for SchemaId {
        fn from(s: String) -> Self { Self(s) }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
    pub struct Sequence(pub u64);

    impl Sequence {
        pub fn next(self) -> Self { Self(self.0 + 1) }
        pub fn as_u64(self) -> u64 { self.0 }
    }

    impl fmt::Display for Sequence {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl From<u64> for Sequence {
        fn from(v: u64) -> Self { Self(v) }
    }

    impl std::ops::Add<u64> for Sequence {
        type Output = Self;
        fn add(self, rhs: u64) -> Self { Self(self.0 + rhs) }
    }

    impl std::ops::Sub<u64> for Sequence {
        type Output = Self;
        fn sub(self, rhs: u64) -> Self { Self(self.0.saturating_sub(rhs)) }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
    pub struct Revision(pub u64);

    impl Revision {
        pub fn next(self) -> Self { Self(self.0 + 1) }
        pub fn as_u64(self) -> u64 { self.0 }
    }

    impl fmt::Display for Revision {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl From<u64> for Revision {
        fn from(v: u64) -> Self { Self(v) }
    }

    impl std::ops::Add<u64> for Revision {
        type Output = Self;
        fn add(self, rhs: u64) -> Self { Self(self.0 + rhs) }
    }
}

// ---- hash ---------------------------------------------------------------

pub mod hash {
    #[derive(Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct Blake3Hash(pub [u8; 32]);

    impl Blake3Hash {
        pub fn of(data: &[u8]) -> Self {
            let h = blake3::hash(data);
            Self(*h.as_bytes())
        }

        pub fn hex(&self) -> String {
            self.0.iter().map(|b| format!("{:02x}", b)).collect()
        }
    }

    impl std::fmt::Display for Blake3Hash {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.hex())
        }
    }

    impl std::fmt::Debug for Blake3Hash {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Blake3Hash({})", self.hex())
        }
    }

    impl std::str::FromStr for Blake3Hash {
        type Err = String;
        fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
            if s.len() != 64 {
                return Err(format!("expected 64 hex chars, got {}", s.len()));
            }
            let mut bytes = [0u8; 32];
            for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
                let hex_str = std::str::from_utf8(chunk).map_err(|e| e.to_string())?;
                bytes[i] = u8::from_str_radix(hex_str, 16).map_err(|e| e.to_string())?;
            }
            Ok(Self(bytes))
        }
    }
}

// ---- envelope -----------------------------------------------------------

pub mod envelope {
    use crate::ids::{MessageId, SchemaId, Subject};
    use crate::hash::Blake3Hash;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct OcelObjectRef {
        pub object_id: String,
        pub object_type: String,
        pub qualifier: Option<String>,
        pub role: Option<String>,
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct Envelope {
        pub message_id: MessageId,
        pub subject: Subject,
        pub schema_id: SchemaId,
        pub content_hash: Blake3Hash,
        pub causality: Vec<MessageId>,
        pub ocel_event_type: Option<String>,
        pub ocel_object_refs: Vec<OcelObjectRef>,
        pub headers: indexmap::IndexMap<String, String>,
        pub ts_ns: i64,
    }

    impl Envelope {
        pub fn new(subject: Subject, schema_id: SchemaId, content_hash: Blake3Hash) -> Self {
            Self {
                message_id: MessageId::new(),
                subject,
                schema_id,
                content_hash,
                causality: Vec::new(),
                ocel_event_type: None,
                ocel_object_refs: Vec::new(),
                headers: indexmap::IndexMap::new(),
                ts_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            }
        }
    }
}

// ---- payload ------------------------------------------------------------

pub mod payload {
    use crate::hash::Blake3Hash;
    use crate::ids::ObjectId;

    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub enum PayloadKind {
        InlineBytes,
        ArrowIpc,
        Blob,
        Manifest,
    }

    #[derive(Clone, Debug)]
    pub enum PayloadRef {
        InlineBytes(bytes::Bytes),
        ArrowIpc(bytes::Bytes),
        Blob(ObjectId),
        Manifest(ObjectId),
    }

    impl PayloadRef {
        pub fn kind(&self) -> PayloadKind {
            match self {
                PayloadRef::InlineBytes(_) => PayloadKind::InlineBytes,
                PayloadRef::ArrowIpc(_) => PayloadKind::ArrowIpc,
                PayloadRef::Blob(_) => PayloadKind::Blob,
                PayloadRef::Manifest(_) => PayloadKind::Manifest,
            }
        }

        pub fn content_hash(&self) -> Blake3Hash {
            match self {
                PayloadRef::InlineBytes(b) => Blake3Hash::of(b),
                PayloadRef::ArrowIpc(b) => Blake3Hash::of(b),
                PayloadRef::Blob(id) => Blake3Hash::of(id.to_string().as_bytes()),
                PayloadRef::Manifest(id) => Blake3Hash::of(id.to_string().as_bytes()),
            }
        }

        pub fn size_bytes(&self) -> usize {
            match self {
                PayloadRef::InlineBytes(b) => b.len(),
                PayloadRef::ArrowIpc(b) => b.len(),
                PayloadRef::Blob(_) => 0,
                PayloadRef::Manifest(_) => 0,
            }
        }

        pub fn to_ref_str(&self) -> String {
            match self {
                PayloadRef::InlineBytes(b) => format!("inline:{}", Blake3Hash::of(b).hex()),
                PayloadRef::ArrowIpc(b) => format!("arrow:{}", Blake3Hash::of(b).hex()),
                PayloadRef::Blob(id) => format!("blob:{}", id),
                PayloadRef::Manifest(id) => format!("manifest:{}", id),
            }
        }
    }
}

// ---- log ----------------------------------------------------------------

pub mod log {
    use crate::ids::{Sequence, StreamId, Subject};
    use crate::hash::Blake3Hash;
    use crate::payload::PayloadKind;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct LogEntry {
        pub seq: Sequence,
        pub stream_id: StreamId,
        pub subject: Subject,
        pub ts_ns: i64,
        pub envelope_hash: Blake3Hash,
        pub payload_kind: PayloadKind,
        pub payload_ref_str: String,
        pub headers: indexmap::IndexMap<String, String>,
    }
}

// ---- object -------------------------------------------------------------

pub mod object {
    use crate::ids::{ObjectId, SchemaId};
    use crate::hash::Blake3Hash;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct ChunkRef {
        pub seq: u32,
        pub hash: Blake3Hash,
        pub size_bytes: u64,
        pub offset: u64,
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct ObjectManifest {
        pub object_id: ObjectId,
        pub content_hash: Blake3Hash,
        pub size_bytes: u64,
        pub chunks: Vec<ChunkRef>,
        pub media_type: String,
        pub schema_id: Option<SchemaId>,
        pub lineage: Vec<Blake3Hash>,
        pub created_at_ns: i64,
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct ObjectMeta {
        pub media_type: String,
        pub schema_id: Option<SchemaId>,
        pub lineage: Vec<Blake3Hash>,
    }
}

// ---- kv -----------------------------------------------------------------

pub mod kv {
    use crate::ids::{BucketId, Revision};

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct KvEntry {
        pub bucket: BucketId,
        pub key: String,
        pub revision: Revision,
        pub value: Vec<u8>,
        pub ts_ns: i64,
        pub op: KvOp,
        /// Absolute expiry timestamp (nanoseconds since epoch).
        /// `None` means the entry never expires.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub ttl_expires_at_ns: Option<i64>,
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub enum KvOp {
        Put,
        Delete,
        Purge,
    }

    #[derive(Clone, Debug)]
    pub struct KvPutRequest {
        pub bucket: BucketId,
        pub key: String,
        pub value: bytes::Bytes,
        pub expected_revision: Option<Revision>,
        pub ttl_secs: Option<u64>,
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct KvAck {
        pub revision: Revision,
        pub ts_ns: i64,
    }

    #[derive(Clone, Debug)]
    pub struct KvEvent {
        pub entry: KvEntry,
        pub is_delete: bool,
    }
}

// ---- ack ----------------------------------------------------------------

pub mod ack {
    use crate::ids::{MessageId, Sequence, StreamId};

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct Ack {
        pub message_id: MessageId,
        pub stream_id: StreamId,
        pub seq: Sequence,
        pub ts_ns: i64,
    }
}

// ---- publish ------------------------------------------------------------

pub mod publish {
    use crate::ids::{Sequence, StreamId, Subject};
    use crate::envelope::Envelope;
    use crate::payload::PayloadRef;

    #[derive(Clone, Debug)]
    pub struct PublishRequest {
        pub stream: StreamId,
        pub subject: Subject,
        pub envelope: Envelope,
        pub payload: PayloadRef,
        pub expected_last_seq: Option<Sequence>,
    }
}

// ---- traits -------------------------------------------------------------

pub mod traits {
    use crate::error::Result;
    use crate::ids::{BucketId, ObjectId, Revision, Sequence, StreamId};
    use crate::log::LogEntry;
    use crate::kv::{KvAck, KvEntry, KvEvent, KvPutRequest};
    use crate::object::{ObjectManifest, ObjectMeta};
    use crate::ack::Ack;
    use crate::publish::PublishRequest;
    use async_trait::async_trait;
    use std::pin::Pin;

    /// Durable append-only log.
    #[async_trait]
    pub trait AppendLog: Send + Sync + 'static {
        async fn append(&self, req: PublishRequest) -> Result<Ack>;
        async fn read_from(
            &self,
            stream: &StreamId,
            from_seq: Sequence,
        ) -> Result<Pin<Box<dyn futures::Stream<Item = Result<LogEntry>> + Send>>>;
        async fn last_seq(&self, stream: &StreamId) -> Result<Option<Sequence>>;
    }

    /// KV bucket store.
    #[async_trait]
    pub trait KvStore: Send + Sync + 'static {
        async fn put(&self, req: KvPutRequest) -> Result<KvAck>;
        async fn get(&self, bucket: &BucketId, key: &str) -> Result<Option<KvEntry>>;
        async fn delete(
            &self,
            bucket: &BucketId,
            key: &str,
            expected_revision: Option<Revision>,
        ) -> Result<KvAck>;
        async fn watch(
            &self,
            bucket: &BucketId,
            prefix: &str,
        ) -> Result<Pin<Box<dyn futures::Stream<Item = KvEvent> + Send>>>;
        async fn history(&self, bucket: &BucketId, key: &str) -> Result<Vec<KvEntry>>;
    }

    /// Content-addressed object store.
    #[async_trait]
    pub trait ObjectStorage: Send + Sync + 'static {
        async fn put_object(
            &self,
            data: bytes::Bytes,
            meta: ObjectMeta,
        ) -> Result<ObjectManifest>;
        async fn get_object(&self, id: &ObjectId) -> Result<bytes::Bytes>;
        async fn head_object(&self, id: &ObjectId) -> Result<Option<ObjectManifest>>;
        async fn pin_object(&self, id: &ObjectId) -> Result<()>;
        async fn gc(&self) -> Result<u64>;
    }
}

// ---- consumer -----------------------------------------------------------

pub mod consumer {
    /// Configuration for creating a consumer group subscription.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct ConsumerConfig {
        /// Durable consumer group name.
        pub group_name: String,
        /// NATS subject filter (e.g., "yata.arrow.>").
        pub filter_subject: String,
        /// Max batch size per pull.
        pub max_batch: usize,
        /// Ack wait timeout (seconds) before message redelivery.
        pub ack_wait_secs: u64,
    }

    impl Default for ConsumerConfig {
        fn default() -> Self {
            Self {
                group_name: String::new(),
                filter_subject: "yata.arrow.>".into(),
                max_batch: 100,
                ack_wait_secs: 30,
            }
        }
    }

    /// Info about an existing consumer group.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct ConsumerInfo {
        pub group_name: String,
        pub filter_subject: String,
        pub num_pending: u64,
        pub num_ack_pending: u64,
    }
}

pub use self::consumer::*;

// ---- cdc ----------------------------------------------------------------

pub mod cdc {
    /// CDC operation type.
    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub enum CdcOp {
        Insert,
        Update,
        Delete,
    }

    /// A single CDC event describing a change to a Lance table.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct CdcEvent {
        /// Table that was modified.
        pub table: String,
        /// Operation type.
        pub op: CdcOp,
        /// Number of rows affected.
        pub row_count: usize,
        /// Timestamp (nanoseconds since epoch).
        pub ts_ns: i64,
    }
}

pub use self::cdc::*;

// ---- error --------------------------------------------------------------

pub mod error {
    #[derive(thiserror::Error, Debug)]
    pub enum YataError {
        #[error("not found: {0}")]
        NotFound(String),
        #[error("revision conflict: expected {expected}, got {actual}")]
        RevisionConflict { expected: u64, actual: u64 },
        #[error("sequence conflict: expected last_seq {expected:?}, actual {actual}")]
        SeqConflict { expected: Option<u64>, actual: u64 },
        #[error("io error: {0}")]
        Io(#[from] std::io::Error),
        #[error("serialization error: {0}")]
        Serialization(String),
        #[error("storage error: {0}")]
        Storage(String),
        #[error("arrow error: {0}")]
        Arrow(String),
    }

    pub type Result<T> = std::result::Result<T, YataError>;
}
