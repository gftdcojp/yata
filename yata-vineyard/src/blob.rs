//! Vineyard-compatible blob storage abstraction.
//!
//! Vineyard stores objects as: Blob (raw bytes in shared memory) + ObjectMeta (JSON in etcd).
//! This module provides the same separation with pluggable backends:
//! - CF: R2 for blobs, in-process HashMap for metadata

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Vineyard ObjectID (uint64, same as v6d).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectId(pub u64);

impl ObjectId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "o{:016x}", self.0)
    }
}

/// Blob ID (content-addressed by Blake3 hash).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobId(pub [u8; 32]);

impl BlobId {
    pub fn from_data(data: &[u8]) -> Self {
        Self(blake3::hash(data).into())
    }
}

impl std::fmt::Display for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex_encode(&self.0))
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Metadata value (Vineyard's JSON tree value types).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetaValue {
    String(String),
    Int(i64),
    UInt(u64),
    Float(f64),
    Bool(bool),
    Null,
}

impl MetaValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            MetaValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            MetaValue::Int(n) => Some(*n),
            MetaValue::UInt(n) => Some(*n as i64),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            MetaValue::UInt(n) => Some(*n),
            MetaValue::Int(n) => Some(*n as u64),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            MetaValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

impl From<String> for MetaValue {
    fn from(s: String) -> Self {
        MetaValue::String(s)
    }
}

impl From<&str> for MetaValue {
    fn from(s: &str) -> Self {
        MetaValue::String(s.to_string())
    }
}

impl From<i64> for MetaValue {
    fn from(n: i64) -> Self {
        MetaValue::Int(n)
    }
}

impl From<u64> for MetaValue {
    fn from(n: u64) -> Self {
        MetaValue::UInt(n)
    }
}

impl From<bool> for MetaValue {
    fn from(b: bool) -> Self {
        MetaValue::Bool(b)
    }
}

/// Vineyard-compatible object metadata (maps to etcd JSON tree).
///
/// In Vineyard, each object has a hierarchical metadata tree:
/// ```json
/// {
///   "id": "o0000000000000001",
///   "typename": "vineyard::ArrowFragment<int64,uint64>",
///   "fields": { "fid": 0, "directed": true, ... },
///   "members": {
///     "vertex_tables_0": { ... nested ObjectMeta ... },
///     ...
///   }
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub id: ObjectId,
    /// Vineyard type name (e.g., "vineyard::NumericArray<int64>").
    pub typename: String,
    /// Scalar fields.
    pub fields: HashMap<String, MetaValue>,
    /// Nested object references (member name → child ObjectMeta).
    pub members: HashMap<String, ObjectMeta>,
    /// Associated blob IDs (buffer name → BlobId).
    pub blobs: HashMap<String, BlobId>,
}

impl ObjectMeta {
    pub fn new(id: ObjectId, typename: &str) -> Self {
        Self {
            id,
            typename: typename.to_string(),
            fields: HashMap::new(),
            members: HashMap::new(),
            blobs: HashMap::new(),
        }
    }

    pub fn set_field(&mut self, key: &str, value: impl Into<MetaValue>) {
        self.fields.insert(key.to_string(), value.into());
    }

    pub fn get_field(&self, key: &str) -> Option<&MetaValue> {
        self.fields.get(key)
    }

    pub fn add_member(&mut self, name: &str, meta: ObjectMeta) {
        self.members.insert(name.to_string(), meta);
    }

    pub fn get_member(&self, name: &str) -> Option<&ObjectMeta> {
        self.members.get(name)
    }

    pub fn add_blob(&mut self, name: &str, blob_id: BlobId) {
        self.blobs.insert(name.to_string(), blob_id);
    }
}

/// Blob storage trait. CF: R2 backend.
pub trait BlobStore: Send + Sync {
    /// Get blob data by ID.
    fn get(&self, id: &BlobId) -> Option<Bytes>;
    /// Store blob data, returns its content-addressed BlobId.
    fn put(&self, data: Bytes) -> BlobId;
    /// Delete a blob.
    fn delete(&self, id: &BlobId) -> bool;
    /// Total stored bytes.
    fn total_bytes(&self) -> u64;
}

/// In-memory blob store (for testing and single-process use).
#[derive(Clone)]
pub struct MemoryBlobStore {
    blobs: Arc<Mutex<HashMap<BlobId, Bytes>>>,
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self {
            blobs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for MemoryBlobStore {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobStore for MemoryBlobStore {
    fn get(&self, id: &BlobId) -> Option<Bytes> {
        self.blobs.lock().unwrap().get(id).cloned()
    }

    fn put(&self, data: Bytes) -> BlobId {
        let id = BlobId::from_data(&data);
        self.blobs.lock().unwrap().insert(id, data);
        id
    }

    fn delete(&self, id: &BlobId) -> bool {
        self.blobs.lock().unwrap().remove(id).is_some()
    }

    fn total_bytes(&self) -> u64 {
        self.blobs
            .lock()
            .unwrap()
            .values()
            .map(|b| b.len() as u64)
            .sum()
    }
}
