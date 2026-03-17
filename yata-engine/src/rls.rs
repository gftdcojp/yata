use yata_cypher::Graph;
use yata_cypher::types::Value;

/// Filter graph nodes by org_id for Row-Level Security.
/// Removes nodes that have an `org_id` property different from the given org_id.
/// Nodes without `org_id` are kept (schema/system nodes).
pub fn apply_rls_filter(g: &mut yata_cypher::MemoryGraph, org_id: &str) {
    let org_val = Value::Str(org_id.to_string());
    let remove_ids: std::collections::HashSet<String> = g
        .nodes()
        .iter()
        .filter(|n| matches!(n.props.get("org_id"), Some(v) if *v != org_val))
        .map(|n| n.id.clone())
        .collect();
    if remove_ids.is_empty() {
        return;
    }
    g.retain_nodes(|n| !remove_ids.contains(&n.id));
    g.retain_rels(|r| !remove_ids.contains(&r.src) && !remove_ids.contains(&r.dst));
}

/// Inject org_id into newly created vertices that don't have it.
pub fn inject_rls_on_new_vertices(
    vertices: &mut [yata_cypher::NodeRef],
    org_id: &str,
) {
    for node in vertices.iter_mut() {
        if !node.props.contains_key("org_id") {
            node.props
                .insert("org_id".into(), Value::Str(org_id.to_string()));
        }
    }
}
