use std::collections::{HashMap, HashSet};
use yata_cypher::types::Value;
use yata_cypher::Graph;
use yata_graph::{LanceGraphStore, QueryableGraph};

/// Delta stats after a write operation.
#[derive(Debug, Default)]
pub struct DeltaStats {
    pub new_vertices: usize,
    pub modified_vertices: usize,
    pub new_edges: usize,
}

/// Compute delta between before/after graph state and persist to Lance.
pub async fn write_delta(
    store: &LanceGraphStore,
    before_vids: &HashSet<String>,
    before_eids: &HashSet<String>,
    before_snapshot: &HashMap<String, String>,
    graph: &QueryableGraph,
    rls_org_id: Option<&str>,
) -> Result<DeltaStats, String> {
    let mut new_vertices: Vec<_> = graph
        .0
        .nodes()
        .into_iter()
        .filter(|n| !before_vids.contains(&n.id))
        .collect();

    let modified_vertices: Vec<_> = graph
        .0
        .nodes()
        .into_iter()
        .filter(|n| {
            before_vids.contains(&n.id)
                && before_snapshot
                    .get(&n.id)
                    .map_or(false, |old| *old != format!("{:?}{:?}", n.labels, n.props))
        })
        .collect();

    let new_edges: Vec<_> = graph
        .0
        .rels()
        .into_iter()
        .filter(|r| !before_eids.contains(&r.id))
        .collect();

    // Inject RLS org_id into new vertices
    if let Some(org_id) = rls_org_id {
        for node in &mut new_vertices {
            if !node.props.contains_key("org_id") {
                node.props
                    .insert("org_id".into(), Value::Str(org_id.to_string()));
            }
        }
    }

    let stats = DeltaStats {
        new_vertices: new_vertices.len(),
        modified_vertices: modified_vertices.len(),
        new_edges: new_edges.len(),
    };

    let all_dirty: Vec<_> = new_vertices.into_iter().chain(modified_vertices).collect();
    if !all_dirty.is_empty() {
        store
            .write_vertices(&all_dirty)
            .await
            .map_err(|e| format!("persist vertices: {e}"))?;
        tracing::debug!(count = all_dirty.len(), "engine: persisted dirty vertices");
    }
    if !new_edges.is_empty() {
        store
            .write_edges(&new_edges)
            .await
            .map_err(|e| format!("persist edges: {e}"))?;
        tracing::debug!(count = new_edges.len(), "engine: persisted new edges");
    }

    Ok(stats)
}
