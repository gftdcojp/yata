//! Core types, error handling, and storage traits for the yata broker.
//!
//! Defines [`YataError`], [`PayloadRef`], and foundational async traits
//! ([`AppendLog`], [`KvStore`], [`ObjectStorage`]) that all storage backends implement.

#![allow(dead_code)]

// ---- Forward declarations for inline modules ---------------------------
// All modules are defined inline below; pub use re-exports at crate root.

pub use self::ack::*;
pub use self::envelope::*;
pub use self::error::*;
pub use self::graph_identity::*;
pub use self::hash::*;
pub use self::ids::*;
pub use self::log::*;
pub use self::object::*;
pub use self::payload::*;
pub use self::publish::*;
pub use self::traits::{AppendLog, ObjectStorage, WrpcBroker};

// ---- ids ---------------------------------------------------------------

pub mod ids {
    use std::fmt;

    macro_rules! numeric_id {
        ($name:ident, $inner:ty) => {
            #[derive(
                Clone,
                Copy,
                Debug,
                Default,
                PartialEq,
                Eq,
                PartialOrd,
                Ord,
                Hash,
                serde::Serialize,
                serde::Deserialize,
                rkyv::Archive,
                rkyv::Serialize,
                rkyv::Deserialize,
            )]
            #[rkyv(derive(Debug))]
            pub struct $name(pub $inner);

            impl $name {
                pub fn new(value: $inner) -> Self {
                    Self(value)
                }
                pub fn get(self) -> $inner {
                    self.0
                }
            }

            impl fmt::Display for $name {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}", self.0)
                }
            }

            impl From<$inner> for $name {
                fn from(value: $inner) -> Self {
                    Self(value)
                }
            }
        };
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct StreamId(pub String);

    impl fmt::Display for StreamId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl From<&str> for StreamId {
        fn from(s: &str) -> Self {
            Self(s.to_owned())
        }
    }
    impl From<String> for StreamId {
        fn from(s: String) -> Self {
            Self(s)
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct Subject(pub String);

    impl fmt::Display for Subject {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl From<&str> for Subject {
        fn from(s: &str) -> Self {
            Self(s.to_owned())
        }
    }
    impl From<String> for Subject {
        fn from(s: String) -> Self {
            Self(s)
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
    pub struct ObjectId(pub uuid::Uuid);

    impl ObjectId {
        pub fn new() -> Self {
            Self(uuid::Uuid::new_v4())
        }
    }

    impl Default for ObjectId {
        fn default() -> Self {
            Self::new()
        }
    }

    impl fmt::Display for ObjectId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl From<uuid::Uuid> for ObjectId {
        fn from(u: uuid::Uuid) -> Self {
            Self(u)
        }
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
        pub fn new() -> Self {
            Self(uuid::Uuid::new_v4())
        }
    }

    impl Default for MessageId {
        fn default() -> Self {
            Self::new()
        }
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
        fn from(s: &str) -> Self {
            Self(s.to_owned())
        }
    }
    impl From<String> for SchemaId {
        fn from(s: String) -> Self {
            Self(s)
        }
    }

    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Hash,
        serde::Serialize,
        serde::Deserialize,
    )]
    pub struct Sequence(pub u64);

    impl Sequence {
        pub fn next(self) -> Self {
            Self(self.0 + 1)
        }
        pub fn as_u64(self) -> u64 {
            self.0
        }
    }

    impl fmt::Display for Sequence {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl From<u64> for Sequence {
        fn from(v: u64) -> Self {
            Self(v)
        }
    }

    impl std::ops::Add<u64> for Sequence {
        type Output = Self;
        fn add(self, rhs: u64) -> Self {
            Self(self.0 + rhs)
        }
    }

    impl std::ops::Sub<u64> for Sequence {
        type Output = Self;
        fn sub(self, rhs: u64) -> Self {
            Self(self.0.saturating_sub(rhs))
        }
    }

    numeric_id!(PartitionId, u32);
    numeric_id!(LocalVid, u32);
    numeric_id!(LocalEid, u32);
    numeric_id!(GlobalVid, u64);
    numeric_id!(GlobalEid, u64);

    // ── Global/Local ID encoding ────────────────────────────────────
    //
    // GlobalVid layout (u64):
    //   [16-bit partition_id][48-bit local_vid]
    //   → up to 65,536 partitions × 281 trillion local vertices per partition
    //
    // GlobalEid layout (u64):
    //   [16-bit partition_id][48-bit local_eid]
    //   → same structure as GlobalVid

    const PARTITION_BITS: u32 = 16;
    const LOCAL_BITS: u32 = 48;
    const LOCAL_MASK: u64 = (1u64 << LOCAL_BITS) - 1;
    const PARTITION_MAX: u32 = (1u32 << PARTITION_BITS) - 1;

    impl GlobalVid {
        /// Encode a partition + local vertex ID into a global ID.
        /// Panics if partition_id exceeds 16-bit range or local_vid exceeds 48-bit range.
        pub fn encode(partition: PartitionId, local: LocalVid) -> Self {
            debug_assert!(
                partition.0 <= PARTITION_MAX,
                "partition_id overflow: {}",
                partition.0
            );
            debug_assert!(
                (local.0 as u64) <= LOCAL_MASK,
                "local_vid overflow: {}",
                local.0
            );
            Self(((partition.0 as u64) << LOCAL_BITS) | (local.0 as u64))
        }

        /// Decode the partition ID from a global vertex ID.
        pub fn partition(self) -> PartitionId {
            PartitionId((self.0 >> LOCAL_BITS) as u32)
        }

        /// Decode the local vertex ID from a global vertex ID.
        pub fn local(self) -> LocalVid {
            LocalVid((self.0 & LOCAL_MASK) as u32)
        }

        /// Split into (partition, local) tuple.
        pub fn split(self) -> (PartitionId, LocalVid) {
            (self.partition(), self.local())
        }

        /// Create from a raw local u32 in partition 0 (single-partition compat).
        pub fn from_local(vid: u32) -> Self {
            Self(vid as u64)
        }
    }

    impl From<LocalVid> for GlobalVid {
        /// Widening conversion: LocalVid → GlobalVid (partition 0).
        fn from(v: LocalVid) -> Self {
            Self(v.0 as u64)
        }
    }

    impl GlobalEid {
        /// Encode a partition + local edge ID into a global ID.
        pub fn encode(partition: PartitionId, local: LocalEid) -> Self {
            debug_assert!(partition.0 <= PARTITION_MAX);
            debug_assert!((local.0 as u64) <= LOCAL_MASK);
            Self(((partition.0 as u64) << LOCAL_BITS) | (local.0 as u64))
        }

        pub fn partition(self) -> PartitionId {
            PartitionId((self.0 >> LOCAL_BITS) as u32)
        }

        pub fn local(self) -> LocalEid {
            LocalEid((self.0 & LOCAL_MASK) as u32)
        }

        pub fn split(self) -> (PartitionId, LocalEid) {
            (self.partition(), self.local())
        }

        pub fn from_local(eid: u32) -> Self {
            Self(eid as u64)
        }
    }

    impl From<LocalEid> for GlobalEid {
        fn from(e: LocalEid) -> Self {
            Self(e.0 as u64)
        }
    }

    /// Maximum local ID value (48-bit).
    pub const LOCAL_ID_MAX: u64 = LOCAL_MASK;
    /// Maximum partition ID value (16-bit).
    pub const PARTITION_ID_MAX: u32 = PARTITION_MAX;
}

// ---- hash ---------------------------------------------------------------

// ---- graph_identity -----------------------------------------------------

pub mod graph_identity {
    use crate::{GlobalEid, GlobalVid};

    pub const SNAPSHOT_FORMAT_VERSION: u32 = 1;
    pub const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
    pub const GLOBAL_VID_PROP_KEY: &str = "_global_vid";
    pub const GLOBAL_EID_PROP_KEY: &str = "_global_eid";

    pub fn parse_global_vid_str(node_id: &str) -> Option<GlobalVid> {
        node_id
            .strip_prefix('g')
            .and_then(|raw| raw.parse::<u64>().ok())
            .map(GlobalVid::from)
    }

    pub fn parse_global_eid_str(edge_id: &str) -> Option<GlobalEid> {
        edge_id
            .strip_prefix('e')
            .and_then(|raw| raw.parse::<u64>().ok())
            .map(GlobalEid::from)
    }
}

pub mod hash {
    #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
    pub struct Blake3Hash(pub [u8; 32]);

    impl Blake3Hash {
        pub fn of(data: &[u8]) -> Self {
            let h = blake3::hash(data);
            Self(*h.as_bytes())
        }

        pub fn hex(&self) -> String {
            self.0.iter().map(|b| format!("{:02x}", b)).collect()
        }

        pub fn from_hex(hex: &str) -> std::result::Result<Self, String> {
            if hex.len() != 64 {
                return Err(format!("expected 64 hex chars, got {}", hex.len()));
            }
            let mut bytes = [0u8; 32];
            for i in 0..32 {
                bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
                    .map_err(|e| format!("invalid hex at byte {i}: {e}"))?;
            }
            Ok(Self(bytes))
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
    use crate::hash::Blake3Hash;
    use crate::ids::{MessageId, SchemaId, Subject};

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
    use crate::hash::Blake3Hash;
    use crate::ids::{Sequence, StreamId, Subject};
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
    use crate::hash::Blake3Hash;
    use crate::ids::{ObjectId, SchemaId};

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
    use crate::envelope::Envelope;
    use crate::ids::{Sequence, StreamId, Subject};
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
    use crate::ack::Ack;
    use crate::error::Result;
    use crate::ids::{ObjectId, Sequence, StreamId};
    use crate::log::LogEntry;
    use crate::object::{ObjectManifest, ObjectMeta};
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

    /// Broker handle for wRPC transport layer.
    /// Provides the minimal interface yata-wrpc needs without pulling in
    /// the full Broker (yata-server) and its heavyweight deps.
    #[async_trait]
    pub trait WrpcBroker: Send + Sync + 'static {
        /// Store bytes in CAS, returning Blake3 hash.
        async fn cas_put(&self, data: bytes::Bytes) -> Result<crate::Blake3Hash>;
        /// Per-app single-writer: always true (MDAG-native replication).
        fn is_leader(&self) -> bool;
    }

    /// Content-addressed object store.
    #[async_trait]
    pub trait ObjectStorage: Send + Sync + 'static {
        async fn put_object(&self, data: bytes::Bytes, meta: ObjectMeta) -> Result<ObjectManifest>;
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

pub use self::durable_wal_types::*;
pub use self::graph_wal::*;

// ---- durable_wal_types --------------------------------------------------

pub mod durable_wal_types {
    /// Property value for durable WAL entries (rkyv-native mirror of yata_grin::PropValue).
    /// Cannot add rkyv derives to zero-dep yata-grin, so we maintain a parallel type.
    #[derive(
        Clone,
        Debug,
        PartialEq,
        serde::Serialize,
        serde::Deserialize,
        rkyv::Archive,
        rkyv::Serialize,
        rkyv::Deserialize,
    )]
    #[rkyv(compare(PartialEq), derive(Debug))]
    pub enum DurablePropValue {
        Null,
        Bool(bool),
        Int(i64),
        Float(f64),
        Str(String),
        /// Raw binary data (vector weights, safetensors, etc.).
        Binary(Vec<u8>),
    }

    /// Durable WAL operation — mirrors graph mutations for crash recovery.
    #[derive(
        Clone,
        Debug,
        serde::Serialize,
        serde::Deserialize,
        rkyv::Archive,
        rkyv::Serialize,
        rkyv::Deserialize,
    )]
    #[rkyv(derive(Debug))]
    pub enum DurableWalOp {
        CreateVertex {
            node_id: String,
            global_vid: Option<crate::GlobalVid>,
            labels: Vec<String>,
            props: Vec<(String, DurablePropValue)>,
        },
        CreateEdge {
            edge_id: String,
            global_eid: Option<crate::GlobalEid>,
            src: String,
            dst: String,
            rel_type: String,
            props: Vec<(String, DurablePropValue)>,
        },
        DeleteVertex {
            node_id: String,
        },
        DeleteEdge {
            edge_id: String,
        },
        SetProp {
            node_id: String,
            key: String,
            value: DurablePropValue,
        },
        Commit {
            message: String,
        },
    }

    /// A single durable WAL entry — the fsync'd unit of graph mutation.
    #[derive(
        Clone,
        Debug,
        serde::Serialize,
        serde::Deserialize,
        rkyv::Archive,
        rkyv::Serialize,
        rkyv::Deserialize,
    )]
    #[rkyv(derive(Debug))]
    pub struct DurableWalEntry {
        /// Monotonically increasing log sequence number.
        pub lsn: u64,
        /// Raft term (Phase 2 — 0 for single-node).
        pub term: u64,
        /// Timestamp in nanoseconds.
        pub ts_ns: i64,
        /// Shard ID (Phase 3 — 0 for single-shard).
        pub shard_id: u16,
        /// The mutation operation.
        pub op: DurableWalOp,
    }
}

// ---- graph_wal ----------------------------------------------------------

pub mod graph_wal {
    use std::collections::HashMap;

    /// Graph mutation operation for WAL persistence.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub enum GraphWalOp {
        CreateNode {
            node_id: String,
            labels: Vec<String>,
            /// Property values as JSON-encoded strings.
            props: HashMap<String, String>,
        },
        CreateEdge {
            edge_id: String,
            src: String,
            dst: String,
            rel_type: String,
            props: HashMap<String, String>,
        },
        UpdateNodeProps {
            node_id: String,
            props: HashMap<String, String>,
        },
        DeleteNode {
            node_id: String,
        },
        DeleteEdge {
            edge_id: String,
        },
    }

    /// A single graph WAL entry — the durable unit of graph mutation.
    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    pub struct GraphWalEntry {
        /// Monotonically increasing sequence within the graph WAL stream.
        pub seq: u64,
        /// Timestamp in nanoseconds.
        pub ts_ns: i64,
        /// The mutation operation.
        pub op: GraphWalOp,
        /// CSR version after this mutation was applied.
        pub csr_version: u64,
        /// Optional RLS org_id that scopes this mutation.
        pub org_id: Option<String>,
    }
}

// ---- ocel_draft ---------------------------------------------------------

pub mod ocel_draft {
    use crate::envelope::OcelObjectRef;

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

        pub fn attr(mut self, key: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
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

pub use self::ocel_draft::*;

// ---- error --------------------------------------------------------------

pub mod error {
    #[derive(thiserror::Error, Debug)]
    pub enum YataError {
        #[error("not found: {0}")]
        NotFound(String),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blake3_hash_of() {
        let h = Blake3Hash::of(b"hello");
        assert_eq!(h.hex().len(), 64);
        assert_eq!(Blake3Hash::of(b"hello"), Blake3Hash::of(b"hello"));
        assert_ne!(Blake3Hash::of(b"hello"), Blake3Hash::of(b"world"));
    }

    #[test]
    fn test_blake3_hash_hex_roundtrip() {
        let h = Blake3Hash::of(b"test data");
        let hex = h.hex();
        let parsed: Blake3Hash = hex.parse().unwrap();
        assert_eq!(h, parsed);
    }

    #[test]
    fn test_blake3_hash_from_str_invalid() {
        assert!("not_a_hash".parse::<Blake3Hash>().is_err());
        assert!("zz".parse::<Blake3Hash>().is_err());
    }

    #[test]
    fn test_stream_id_display() {
        let id = StreamId::from("my-stream");
        assert_eq!(format!("{id}"), "my-stream");
    }

    #[test]
    fn test_subject_from_string() {
        let s = Subject::from("topic.events".to_string());
        assert_eq!(s.0, "topic.events");
    }

    #[test]
    fn test_object_id_unique() {
        let a = ObjectId::new();
        let b = ObjectId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn test_sequence_next() {
        let s = Sequence(5);
        assert_eq!(s.next().as_u64(), 6);
    }

    #[test]
    fn test_parse_global_graph_ids() {
        assert_eq!(parse_global_vid_str("g42").map(|id| id.get()), Some(42));
        assert_eq!(parse_global_eid_str("e77").map(|id| id.get()), Some(77));
        assert_eq!(parse_global_vid_str("node-42"), None);
        assert_eq!(parse_global_eid_str("edge-77"), None);
    }

    #[test]
    fn test_envelope_new() {
        let e = Envelope::new(
            Subject::from("test"),
            SchemaId::from("s1"),
            Blake3Hash::of(b"data"),
        );
        assert!(e.ts_ns > 0);
        assert_eq!(e.subject.0, "test");
    }

    #[test]
    fn test_payload_ref_inline() {
        let p = PayloadRef::InlineBytes(bytes::Bytes::from_static(b"hello"));
        assert_eq!(p.kind(), PayloadKind::InlineBytes);
        assert_eq!(p.size_bytes(), 5);
    }

    #[test]
    fn test_payload_ref_content_hash() {
        let p = PayloadRef::InlineBytes(bytes::Bytes::from_static(b"data"));
        let h = p.content_hash();
        assert_eq!(h, Blake3Hash::of(b"data"));
    }

    #[test]
    fn test_payload_ref_to_ref_str() {
        let p = PayloadRef::InlineBytes(bytes::Bytes::from_static(b"data"));
        let s = p.to_ref_str();
        assert!(s.starts_with("inline:"));
    }

    #[test]
    fn test_schema_id_display() {
        let s = SchemaId::from("my.schema.v1");
        assert_eq!(format!("{s}"), "my.schema.v1");
    }

    // ── Global/Local ID encoding tests ──

    #[test]
    fn test_global_vid_encode_decode() {
        let p = PartitionId::new(42);
        let l = LocalVid::new(12345);
        let g = GlobalVid::encode(p, l);
        assert_eq!(g.partition(), p);
        assert_eq!(g.local(), l);
        let (pp, ll) = g.split();
        assert_eq!(pp, p);
        assert_eq!(ll, l);
    }

    #[test]
    fn test_global_vid_partition_zero() {
        let g = GlobalVid::from_local(999);
        assert_eq!(g.partition(), PartitionId::new(0));
        assert_eq!(g.local(), LocalVid::new(999));
    }

    #[test]
    fn test_global_vid_from_local_vid() {
        let lv = LocalVid::new(42);
        let gv: GlobalVid = lv.into();
        assert_eq!(gv.0, 42);
        assert_eq!(gv.partition(), PartitionId::new(0));
        assert_eq!(gv.local(), LocalVid::new(42));
    }

    #[test]
    fn test_global_vid_max_partition() {
        let p = PartitionId::new(ids::PARTITION_ID_MAX);
        let l = LocalVid::new(0);
        let g = GlobalVid::encode(p, l);
        assert_eq!(g.partition(), p);
        assert_eq!(g.local(), l);
    }

    #[test]
    fn test_global_vid_max_local() {
        let p = PartitionId::new(0);
        let l = LocalVid::new(u32::MAX);
        let g = GlobalVid::encode(p, l);
        assert_eq!(g.partition(), p);
        assert_eq!(g.local(), l);
    }

    #[test]
    fn test_global_eid_encode_decode() {
        let p = PartitionId::new(7);
        let l = LocalEid::new(999999);
        let g = GlobalEid::encode(p, l);
        assert_eq!(g.partition(), p);
        assert_eq!(g.local(), l);
    }

    #[test]
    fn test_global_vid_different_partitions_differ() {
        let g1 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(1));
        let g2 = GlobalVid::encode(PartitionId::new(1), LocalVid::new(1));
        assert_ne!(g1, g2);
    }

    #[test]
    fn test_global_vid_serde_roundtrip() {
        let g = GlobalVid::encode(PartitionId::new(5), LocalVid::new(42));
        let json = serde_json::to_string(&g).unwrap();
        let g2: GlobalVid = serde_json::from_str(&json).unwrap();
        assert_eq!(g, g2);
    }
}
