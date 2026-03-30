use std::collections::HashMap;
use yata_core::PartitionId;
use yata_cypher::Graph;
use yata_graph::GraphStore;
use yata_grin::{Mutable, PropValue};
use yata_store::MutableCsrStore;

/// Convert yata_cypher::Value → yata_grin::PropValue for CSR property storage.
pub fn cypher_to_prop(v: &yata_cypher::types::Value) -> PropValue {
    match v {
        yata_cypher::types::Value::Null => PropValue::Null,
        yata_cypher::types::Value::Bool(b) => PropValue::Bool(*b),
        yata_cypher::types::Value::Int(i) => PropValue::Int(*i),
        yata_cypher::types::Value::Float(f) => PropValue::Float(*f),
        yata_cypher::types::Value::Str(s) => PropValue::Str(s.clone()),
        other => PropValue::Str(format!("{}", other)),
    }
}

/// Load all vertices/edges from GraphStore into a fresh MutableCsrStore.
pub async fn load_csr_from_graph(store: &GraphStore) -> Result<MutableCsrStore, String> {
    load_csr_from_graph_with_partition(store, PartitionId::from(0)).await
}

/// Load all vertices/edges from GraphStore into a fresh MutableCsrStore for a specific partition.
pub async fn load_csr_from_graph_with_partition(
    store: &GraphStore,
    partition_id: PartitionId,
) -> Result<MutableCsrStore, String> {
    let vertices = store.load_vertices().await.map_err(|e| e.to_string())?;
    let edges = store.load_edges().await.map_err(|e| e.to_string())?;

    let mut csr = MutableCsrStore::new_with_partition_id(partition_id);
    let mut vid_map: HashMap<String, u32> = HashMap::new();

    for node in &vertices {
        let props: Vec<(&str, PropValue)> = node
            .props
            .iter()
            .map(|(k, v): (&String, &yata_cypher::types::Value)| (k.as_str(), cypher_to_prop(v)))
            .collect();
        let mut all_props = props;
        all_props.push(("_vid", PropValue::Str(node.id.clone())));
        let vid = csr.add_vertex_with_labels(&node.labels, &all_props);
        vid_map.insert(node.id.clone(), vid);
    }

    for edge in &edges {
        let src = vid_map.get(&edge.src).copied();
        let dst = vid_map.get(&edge.dst).copied();
        if let (Some(s), Some(d)) = (src, dst) {
            let props: Vec<(&str, PropValue)> = edge
                .props
                .iter()
                .map(|(k, v): (&String, &yata_cypher::types::Value)| {
                    (k.as_str(), cypher_to_prop(v))
                })
                .collect();
            csr.add_edge(s, d, &edge.rel_type, &props);
        }
    }

    csr.commit();
    tracing::info!(
        vertices = vertices.len(),
        edges = edges.len(),
        "engine: CSR initialized from GraphStore"
    );
    Ok(csr)
}

/// Rebuild a CSR store from a post-mutation MemoryGraph.
pub fn rebuild_csr_from_graph(g: &yata_graph::QueryableGraph) -> MutableCsrStore {
    rebuild_csr_from_graph_with_partition(g, PartitionId::from(0))
}

/// Rebuild a CSR store from a post-mutation MemoryGraph for a specific partition.
pub fn rebuild_csr_from_graph_with_partition(
    g: &yata_graph::QueryableGraph,
    partition_id: PartitionId,
) -> MutableCsrStore {
    let nodes = g.0.nodes();
    let rels = g.0.rels();
    let mut csr = MutableCsrStore::new_with_partition_id(partition_id);
    let mut vid_map: HashMap<String, u32> = HashMap::new();

    for node in &nodes {
        let props: Vec<(&str, PropValue)> = node
            .props
            .iter()
            .map(|(k, v): (&String, &yata_cypher::types::Value)| (k.as_str(), cypher_to_prop(v)))
            .collect();
        let mut all_props = props;
        all_props.push(("_vid", PropValue::Str(node.id.clone())));
        let vid = csr.add_vertex_with_labels(&node.labels, &all_props);
        vid_map.insert(node.id.clone(), vid);
    }

    for rel in &rels {
        if let (Some(&s), Some(&d)) = (vid_map.get(&rel.src), vid_map.get(&rel.dst)) {
            let props: Vec<(&str, PropValue)> = rel
                .props
                .iter()
                .map(|(k, v): (&String, &yata_cypher::types::Value)| {
                    (k.as_str(), cypher_to_prop(v))
                })
                .collect();
            csr.add_edge(s, d, &rel.rel_type, &props);
        }
    }

    csr.commit();
    csr
}
