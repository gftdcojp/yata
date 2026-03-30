//! Delta writer — graph persistence is handled outside GraphStore.
//! This module provides vector search embedding writes only (via GraphStore/yata-vex).
//! Graph data writes (vertices/edges) are handled by engine.rs.

use yata_graph::GraphStore;

/// Write vertices with embeddings to the vector search index.
/// This is the ONLY write path through GraphStore.
pub async fn write_embeddings(
    store: &GraphStore,
    nodes: &[yata_cypher::NodeRef],
    embedding_key: &str,
    dim: usize,
) -> Result<usize, String> {
    store
        .write_vertices_with_embeddings(nodes, embedding_key, dim)
        .await
        .map_err(|e| format!("write embeddings: {e}"))?;
    Ok(nodes.len())
}
