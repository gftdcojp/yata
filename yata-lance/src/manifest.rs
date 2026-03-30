use serde::{Deserialize, Serialize};

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
    pub tables: GraphManifestTables,
    pub seq: GraphManifestSeqRange,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dirty_labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_at_ms: Option<u64>,
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

impl GraphManifest {
    pub fn new(partition_id: u32, version: u64, seq_min: u64, seq_max: u64, base_uri: &str) -> Self {
        let base_uri = base_uri.trim_end_matches('/');
        Self {
            manifest_version: 1,
            graph_format: GRAPH_FORMAT.to_string(),
            contract_version: 1,
            partition_id,
            version,
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
