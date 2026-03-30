use crate::dataset::open_datasets_from_manifest;
use crate::manifest::{
    GraphManifest,
    OpenedGraphDatasets,
    ManifestStore,
    load_manifest_via_latest,
    publish_manifest,
};

pub struct GraphCatalog;

impl GraphCatalog {
    pub fn latest_key(partition_id: u32) -> String {
        format!("manifests/partitions/{partition_id}/latest.json")
    }

    pub fn versioned_key(partition_id: u32, version: u64) -> String {
        format!("manifests/partitions/{partition_id}/manifest-{version:020}.json")
    }

    pub fn create_manifest(
        partition_id: u32,
        version: u64,
        seq_min: u64,
        seq_max: u64,
        base_uri: &str,
    ) -> GraphManifest {
        GraphManifest::new(partition_id, version, seq_min, seq_max, base_uri)
    }

    pub fn publish<S: ManifestStore>(store: &S, manifest: &GraphManifest) -> Result<(), String> {
        publish_manifest(store, manifest)
    }

    pub fn load_latest<S: ManifestStore>(
        store: &S,
        partition_id: u32,
    ) -> Result<Option<GraphManifest>, String> {
        load_manifest_via_latest(store, &Self::latest_key(partition_id))
    }

    pub async fn open_latest<S: ManifestStore>(
        store: &S,
        partition_id: u32,
    ) -> Result<Option<OpenedGraphDatasets>, String> {
        let manifest = match Self::load_latest(store, partition_id)? {
            Some(manifest) => manifest,
            None => return Ok(None),
        };
        open_datasets_from_manifest(&manifest)
            .await
            .map(Some)
            .map_err(|e| e.to_string())
    }
}
