use base64::Engine;
use std::collections::HashMap;

use indexmap::IndexMap;
use yata_core::PartitionId;
use yata_cypher::{Executor, Graph, MemoryGraph, NodeRef, RelRef};
use yata_grin::{GraphStore, Mutable, PropValue, Property, Topology};
use yata_store::MutableCsrStore;

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

pub fn prop_to_cypher(v: &PropValue) -> yata_cypher::types::Value {
    match v {
        PropValue::Null => yata_cypher::types::Value::Null,
        PropValue::Bool(b) => yata_cypher::types::Value::Bool(*b),
        PropValue::Int(i) => yata_cypher::types::Value::Int(*i),
        PropValue::Float(f) => yata_cypher::types::Value::Float(*f),
        PropValue::Str(s) => yata_cypher::types::Value::Str(s.clone()),
        PropValue::Binary(bytes) => {
            yata_cypher::types::Value::Str(format!("b64:{}", base64::engine::general_purpose::STANDARD.encode(bytes)))
        }
    }
}

fn cypher_to_json(v: &yata_cypher::Value) -> serde_json::Value {
    match v {
        yata_cypher::Value::Null => serde_json::Value::Null,
        yata_cypher::Value::Bool(b) => serde_json::Value::Bool(*b),
        yata_cypher::Value::Int(i) => serde_json::Value::Number((*i).into()),
        yata_cypher::Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        yata_cypher::Value::Str(s) => serde_json::Value::String(s.clone()),
        yata_cypher::Value::List(l) => {
            serde_json::Value::Array(l.iter().map(cypher_to_json).collect())
        }
        yata_cypher::Value::Map(m) => serde_json::Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), cypher_to_json(v)))
                .collect(),
        ),
        yata_cypher::Value::Node(n) => {
            let mut obj: serde_json::Map<String, serde_json::Value> = n
                .props
                .iter()
                .map(|(k, v)| (k.clone(), cypher_to_json(v)))
                .collect();
            obj.insert("__vid".into(), serde_json::Value::String(n.id.clone()));
            obj.insert(
                "__labels".into(),
                serde_json::Value::Array(
                    n.labels
                        .iter()
                        .map(|l| serde_json::Value::String(l.clone()))
                        .collect(),
                ),
            );
            serde_json::Value::Object(obj)
        }
        yata_cypher::Value::Rel(r) => {
            let mut obj: serde_json::Map<String, serde_json::Value> = r
                .props
                .iter()
                .map(|(k, v)| (k.clone(), cypher_to_json(v)))
                .collect();
            obj.insert("__eid".into(), serde_json::Value::String(r.id.clone()));
            obj.insert(
                "__type".into(),
                serde_json::Value::String(r.rel_type.clone()),
            );
            serde_json::Value::Object(obj)
        }
    }
}

fn json_to_cypher(v: &serde_json::Value) -> yata_cypher::Value {
    match v {
        serde_json::Value::Null => yata_cypher::Value::Null,
        serde_json::Value::Bool(b) => yata_cypher::Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                yata_cypher::Value::Int(i)
            } else {
                yata_cypher::Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => yata_cypher::Value::Str(s.clone()),
        serde_json::Value::Array(a) => {
            yata_cypher::Value::List(a.iter().map(json_to_cypher).collect())
        }
        serde_json::Value::Object(m) => {
            let mut map = IndexMap::new();
            for (k, v) in m {
                map.insert(k.clone(), json_to_cypher(v));
            }
            yata_cypher::Value::Map(map)
        }
    }
}

pub fn execute_query(
    graph: &mut MemoryGraph,
    cypher: &str,
    params: &[(String, String)],
) -> Result<Vec<Vec<(String, String)>>, yata_cypher::CypherError> {
    let query = yata_cypher::parse(cypher)?;
    let mut param_map = IndexMap::new();
    for (k, v) in params {
        let val: serde_json::Value =
            serde_json::from_str(v).unwrap_or(serde_json::Value::String(v.clone()));
        param_map.insert(k.clone(), json_to_cypher(&val));
    }
    let result = Executor::with_params(param_map).execute(&query, graph)?;
    let rows = result
        .rows
        .into_iter()
        .map(|row| {
            row.0
                .into_iter()
                .map(|(col, val)| {
                    let json = serde_json::to_string(&cypher_to_json(&val)).unwrap_or_default();
                    (col, json)
                })
                .collect()
        })
        .collect();
    Ok(rows)
}

pub fn rebuild_csr_from_memory_graph_with_partition(
    graph: &MemoryGraph,
    partition_id: PartitionId,
) -> MutableCsrStore {
    let nodes = graph.nodes();
    let rels = graph.rels();
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

pub fn rebuild_read_store_from_memory_graph(graph: &MemoryGraph) -> yata_lance::LanceReadStore {
    let nodes = graph.nodes();
    let rels = graph.rels();
    let mut store = yata_lance::LanceReadStore::default();
    let mut vid_map: HashMap<String, u32> = HashMap::new();

    for node in &nodes {
        let props: Vec<(&str, PropValue)> = node
            .props
            .iter()
            .map(|(k, v)| (k.as_str(), cypher_to_prop(v)))
            .collect();
        let label = node.labels.first().map(String::as_str).unwrap_or("Node");
        let pk_key = if node.props.contains_key("rkey") { "rkey" } else { "_vid" };
        let pk_value = node
            .props
            .get(pk_key)
            .map(cypher_to_prop)
            .unwrap_or_else(|| PropValue::Str(node.id.clone()));
        let vid = store.merge_vertex_by_pk(label, pk_key, &pk_value, &props);
        vid_map.insert(node.id.clone(), vid);
    }

    for rel in &rels {
        let (Some(&src), Some(&dst)) = (vid_map.get(&rel.src), vid_map.get(&rel.dst)) else {
            continue;
        };
        let edge_id = store.edge_count() as u32;
        let props: HashMap<String, PropValue> = rel
            .props
            .iter()
            .map(|(k, v)| (k.clone(), cypher_to_prop(v)))
            .collect();
        store.add_edge_cache_entry(
            src,
            dst,
            edge_id,
            rel.rel_type.clone(),
            props,
        );
    }

    store
}

pub fn memory_graph_from_store<S: GraphStore>(store: &S) -> MemoryGraph {
    let mut graph = MemoryGraph::new();
    let mut ids = HashMap::<u32, String>::new();

    for vid in store.scan_all_vertices() {
        let id = vertex_identity(store, vid);
        let labels = Property::vertex_labels(store, vid);
        let props = store
            .vertex_all_props(vid)
            .into_iter()
            .map(|(k, v)| (k, prop_to_cypher(&v)))
            .collect();
        graph.add_node(NodeRef { id: id.clone(), labels, props });
        ids.insert(vid, id);
    }

    for src in store.scan_all_vertices() {
        for neighbor in Topology::out_neighbors(store, src) {
            let Some(src_id) = ids.get(&src).cloned() else { continue };
            let Some(dst_id) = ids.get(&neighbor.vid).cloned() else { continue };
            let edge_id = match store.edge_prop(neighbor.edge_id, "eid") {
                Some(PropValue::Str(s)) => s,
                _ => format!("e{}", neighbor.edge_id),
            };
            let mut props = IndexMap::new();
            for key in store.edge_prop_keys(&neighbor.edge_label) {
                if let Some(v) = store.edge_prop(neighbor.edge_id, &key) {
                    props.insert(key, prop_to_cypher(&v));
                }
            }
            graph.add_rel(RelRef {
                id: edge_id,
                rel_type: neighbor.edge_label,
                src: src_id,
                dst: dst_id,
                props,
            });
        }
    }

    graph
}

fn vertex_identity<S: GraphStore>(store: &S, vid: u32) -> String {
    for key in ["_vid", "rkey", "pk_value", "did"] {
        if let Some(PropValue::Str(s)) = store.vertex_prop(vid, key) {
            return s;
        }
    }
    format!("v{vid}")
}
