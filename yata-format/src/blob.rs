//! Name-based blob storage for YataFragment persistence.
//!
//! Blobs are stored by name (e.g., "vertex_table_0", "oe_offsets_0_0").
//! R2 key = `snap/fragment/{name}`. No content-addressing (CAS removed).

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Fragment ObjectID (uint64).
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

/// Metadata value (JSON tree value types).
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

/// Object metadata: maps blob names to their storage keys.
///
/// Unlike CAS (content-addressed), blobs are name-addressed.
/// R2 key = `snap/fragment/{name}`. No hash computation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub id: ObjectId,
    /// Fragment type name.
    pub typename: String,
    /// Scalar fields.
    pub fields: HashMap<String, MetaValue>,
    /// Nested object references (member name → child ObjectMeta).
    pub members: HashMap<String, ObjectMeta>,
    /// Blob names (logical name → storage name). Same key for put/get.
    pub blobs: HashMap<String, String>,
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

    pub fn add_blob(&mut self, name: &str) {
        self.blobs.insert(name.to_string(), name.to_string());
    }
}

/// Name-based blob store. Blobs identified by string name, not content hash.
pub trait BlobStore: Send + Sync {
    /// Get blob data by name.
    fn get(&self, name: &str) -> Option<Bytes>;
    /// Store blob data by name.
    fn put(&self, name: &str, data: Bytes);
    /// Delete a blob by name.
    fn delete(&self, name: &str) -> bool;
    /// Total stored bytes.
    fn total_bytes(&self) -> u64;
}

/// In-memory blob store (for testing and single-process use).
#[derive(Clone)]
pub struct MemoryBlobStore {
    blobs: Arc<Mutex<HashMap<String, Bytes>>>,
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
    fn get(&self, name: &str) -> Option<Bytes> {
        self.blobs.lock().unwrap().get(name).cloned()
    }

    fn put(&self, name: &str, data: Bytes) {
        self.blobs.lock().unwrap().insert(name.to_string(), data);
    }

    fn delete(&self, name: &str) -> bool {
        self.blobs.lock().unwrap().remove(name).is_some()
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
