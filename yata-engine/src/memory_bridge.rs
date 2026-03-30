use std::collections::HashMap;

use indexmap::IndexMap;
use yata_core::PartitionId;
use yata_cypher::{Executor, Graph, MemoryGraph};
use yata_grin::{Mutable, PropValue};
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
