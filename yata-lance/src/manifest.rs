use serde::{Deserialize, Serialize};
use yata_s3::s3::{S3Client, S3Error};

use crate::store::UreqObjectStore;

use crate::schema::{
    EDGE_LIVE_IN_TABLE,
    EDGE_LIVE_OUT_TABLE,
    EDGE_LOG_TABLE,
    GRAPH_FORMAT,
    VERTEX_LIVE_TABLE,
    VERTEX_LOG_TABLE,
};
use crate::YataDataset;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphManifestTableRef {
    pub table_name: String,
    pub uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphManifestTables {
    pub vertex_log: GraphManifestTableRef,
    pub edge_log: GraphManifestTableRef,
    pub vertex_live: GraphManifestTableRef,
    pub edge_live_out: GraphManifestTableRef,
    pub edge_live_in: GraphManifestTableRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphManifestSeqRange {
    pub min: u64,
    pub max: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphManifest {
    pub manifest_version: u32,
    pub graph_format: String,
    pub contract_version: u32,
    pub partition_id: u32,
    pub version: u64,
    pub key_layout: GraphManifestKeyLayout,
    pub tables: GraphManifestTables,
    pub seq: GraphManifestSeqRange,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dirty_labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphManifestKeyLayout {
    pub manifest_prefix: String,
    pub latest_key: String,
    pub pointer_key: String,
    pub versioned_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphManifestLatestPointer {
    pub graph_format: String,
    pub contract_version: u32,
    pub partition_id: u32,
    pub version: u64,
    pub versioned_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_ms: Option<u64>,
}

pub struct OpenedGraphDatasets {
    pub vertex_log: YataDataset,
    pub edge_log: YataDataset,
    pub vertex_live: YataDataset,
    pub edge_live_out: YataDataset,
    pub edge_live_in: YataDataset,
}

pub trait ManifestStore {
    type Error;

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Self::Error>;
    fn put(&self, key: &str, value: &[u8]) -> Result<(), Self::Error>;
}

pub struct S3ManifestStore {
    client: S3Client,
    prefix: String,
}

impl S3ManifestStore {
    pub fn new(client: S3Client, prefix: impl Into<String>) -> Self {
        Self {
            client,
            prefix: prefix.into().trim_end_matches('/').to_string(),
        }
    }

    fn object_key(&self, key: &str) -> String {
        let key = key.trim_start_matches('/');
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }
}

impl ManifestStore for S3ManifestStore {
    type Error = S3Error;

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        let key = self.object_key(key);
        self.client
            .get_sync(&key)
            .map(|opt| opt.map(|b| b.to_vec()))
    }

    fn put(&self, key: &str, value: &[u8]) -> Result<(), Self::Error> {
        let key = self.object_key(key);
        self.client
            .put_sync(&key, bytes::Bytes::copy_from_slice(value))
    }
}

/// ManifestStore backed by UreqObjectStore (object_store::ObjectStore trait).
/// Provides the same interface as S3ManifestStore but using the standardized ObjectStore API.
pub struct ObjectStoreManifestStore {
    store: std::sync::Arc<UreqObjectStore>,
    prefix: String,
}

impl ObjectStoreManifestStore {
    pub fn new(store: std::sync::Arc<UreqObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into().trim_end_matches('/').to_string(),
        }
    }

    fn object_key(&self, key: &str) -> String {
        let key = key.trim_start_matches('/');
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }

    pub fn store(&self) -> &std::sync::Arc<UreqObjectStore> {
        &self.store
    }
}

impl ManifestStore for ObjectStoreManifestStore {
    type Error = String;

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        let key = self.object_key(key);
        self.store
            .client()
            .get_sync(&key)
            .map(|opt| opt.map(|b| b.to_vec()))
            .map_err(|e| e.to_string())
    }

    fn put(&self, key: &str, value: &[u8]) -> Result<(), Self::Error> {
        let key = self.object_key(key);
        self.store
            .client()
            .put_sync(&key, bytes::Bytes::copy_from_slice(value))
            .map_err(|e| e.to_string())
    }
}

impl GraphManifest {
    pub fn new(partition_id: u32, version: u64, seq_min: u64, seq_max: u64, base_uri: &str) -> Self {
        let base_uri = base_uri.trim_end_matches('/');
        let key_layout = GraphManifestKeyLayout::for_partition(partition_id, version);
        Self {
            manifest_version: 1,
            graph_format: GRAPH_FORMAT.to_string(),
            contract_version: 1,
            partition_id,
            version,
            key_layout,
            tables: GraphManifestTables {
                vertex_log: GraphManifestTableRef::new(VERTEX_LOG_TABLE, format!("{base_uri}/{VERTEX_LOG_TABLE}")),
                edge_log: GraphManifestTableRef::new(EDGE_LOG_TABLE, format!("{base_uri}/{EDGE_LOG_TABLE}")),
                vertex_live: GraphManifestTableRef::new(VERTEX_LIVE_TABLE, format!("{base_uri}/{VERTEX_LIVE_TABLE}")),
                edge_live_out: GraphManifestTableRef::new(EDGE_LIVE_OUT_TABLE, format!("{base_uri}/{EDGE_LIVE_OUT_TABLE}")),
                edge_live_in: GraphManifestTableRef::new(EDGE_LIVE_IN_TABLE, format!("{base_uri}/{EDGE_LIVE_IN_TABLE}")),
            },
            seq: GraphManifestSeqRange { min: seq_min, max: seq_max },
            dirty_labels: Vec::new(),
            generated_at_ms: None,
        }
    }
}

impl GraphManifestKeyLayout {
    pub fn for_partition(partition_id: u32, version: u64) -> Self {
        let manifest_prefix = format!("manifests/partitions/{partition_id}");
        let latest_key = format!("{manifest_prefix}/latest.json");
        let pointer_key = latest_key.clone();
        let versioned_key = format!("{manifest_prefix}/manifest-{version:020}.json");
        Self {
            manifest_prefix,
            latest_key,
            pointer_key,
            versioned_key,
        }
    }
}

impl GraphManifestLatestPointer {
    pub fn from_manifest(manifest: &GraphManifest) -> Self {
        Self {
            graph_format: manifest.graph_format.clone(),
            contract_version: manifest.contract_version,
            partition_id: manifest.partition_id,
            version: manifest.version,
            versioned_key: manifest.key_layout.versioned_key.clone(),
            etag: manifest
                .tables
                .vertex_live
                .etag
                .clone()
                .or_else(|| manifest.tables.edge_live_out.etag.clone()),
            updated_at_ms: manifest.generated_at_ms,
        }
    }
}

impl GraphManifestTableRef {
    pub fn new(table_name: &str, uri: String) -> Self {
        Self {
            table_name: table_name.to_string(),
            uri,
            version: None,
            etag: None,
        }
    }
}

pub fn render_manifest_json(manifest: &GraphManifest) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec_pretty(manifest)
}

pub fn parse_manifest_json(data: &[u8]) -> Result<GraphManifest, serde_json::Error> {
    serde_json::from_slice(data)
}

pub fn render_latest_pointer_json(ptr: &GraphManifestLatestPointer) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec_pretty(ptr)
}

pub fn parse_latest_pointer_json(data: &[u8]) -> Result<GraphManifestLatestPointer, serde_json::Error> {
    serde_json::from_slice(data)
}

pub fn save_manifest<S: ManifestStore>(
    store: &S,
    key: &str,
    manifest: &GraphManifest,
) -> Result<(), String> {
    let bytes = render_manifest_json(manifest).map_err(|e| e.to_string())?;
    store.put(key, &bytes).map_err(|_| "manifest put failed".to_string())
}

pub fn load_manifest<S: ManifestStore>(store: &S, key: &str) -> Result<Option<GraphManifest>, String> {
    let bytes = match store.get(key).map_err(|_| "manifest get failed".to_string())? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    parse_manifest_json(&bytes)
        .map(Some)
        .map_err(|e| e.to_string())
}

pub fn load_latest_pointer<S: ManifestStore>(
    store: &S,
    key: &str,
) -> Result<Option<GraphManifestLatestPointer>, String> {
    let bytes = match store.get(key).map_err(|_| "latest pointer get failed".to_string())? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    parse_latest_pointer_json(&bytes)
        .map(Some)
        .map_err(|e| e.to_string())
}

pub fn load_manifest_via_latest<S: ManifestStore>(
    store: &S,
    latest_key: &str,
) -> Result<Option<GraphManifest>, String> {
    let pointer = match load_latest_pointer(store, latest_key)? {
        Some(pointer) => pointer,
        None => return Ok(None),
    };
    load_manifest(store, &pointer.versioned_key)
}

pub fn publish_manifest<S: ManifestStore>(
    store: &S,
    manifest: &GraphManifest,
) -> Result<(), String> {
    save_manifest(store, &manifest.key_layout.versioned_key, manifest)?;
    let ptr = GraphManifestLatestPointer::from_manifest(manifest);
    let bytes = render_latest_pointer_json(&ptr).map_err(|e| e.to_string())?;
    store
        .put(&manifest.key_layout.pointer_key, &bytes)
        .map_err(|_| "manifest pointer put failed".to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_roundtrip() {
        let mut manifest = GraphManifest::new(0, 7, 1, 99, "r2://bucket/yata/partitions/0");
        manifest.dirty_labels = vec!["Post".into(), "Profile".into()];
        manifest.generated_at_ms = Some(1_711_900_000_000);

        let bytes = render_manifest_json(&manifest).unwrap();
        let parsed = parse_manifest_json(&bytes).unwrap();
        assert_eq!(parsed, manifest);
    }
}
