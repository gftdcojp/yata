//! Per-label graph store — thin delegation to GraphStore.
//!
//! No-op wrapper since graph data lives in-memory.

/// Per-label store (no-op wrapper).
pub struct PerLabelStore;

impl PerLabelStore {
    pub async fn new(_base_uri: &str) -> Result<Self, String> {
        Ok(Self)
    }

    pub async fn write_vertices_by_label(
        &self,
        _nodes: &[yata_cypher::NodeRef],
    ) -> Result<(), String> {
        Ok(())
    }

    pub async fn load_vertices_by_label(
        &self,
        _label: &str,
    ) -> Result<Vec<yata_cypher::NodeRef>, String> {
        Ok(Vec::new())
    }

    pub async fn vertex_label_tables(&self) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }

    pub async fn table_version(&self, _label: &str) -> Result<u64, String> {
        Ok(0)
    }
}
