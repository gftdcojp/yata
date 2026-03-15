use super::*;
use yata_cypher::{parse, Executor, Graph, MemoryGraph, NodeRef, RelRef};
use indexmap::IndexMap;

fn str_val(s: &str) -> yata_cypher::Value {
    yata_cypher::Value::Str(s.to_owned())
}

fn int_val(i: i64) -> yata_cypher::Value {
    yata_cypher::Value::Int(i)
}

#[tokio::test]
async fn test_write_and_load_vertices() {
    let dir = tempfile::tempdir().unwrap();
    let store = LanceGraphStore::new(dir.path().to_str().unwrap()).await.unwrap();

    let mut props = IndexMap::new();
    props.insert("name".into(), str_val("Alice"));
    props.insert("age".into(), int_val(30));

    let nodes = vec![NodeRef {
        id: "alice".into(),
        labels: vec!["Person".into()],
        props,
    }];
    store.write_vertices(&nodes).await.unwrap();

    let loaded = store.load_vertices().await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].id, "alice");
    assert_eq!(loaded[0].labels, vec!["Person".to_string()]);
    assert_eq!(loaded[0].props.get("name"), Some(&str_val("Alice")));
    assert_eq!(loaded[0].props.get("age"), Some(&int_val(30)));
}

#[tokio::test]
async fn test_write_and_load_edges_with_adj() {
    let dir = tempfile::tempdir().unwrap();
    let store = LanceGraphStore::new(dir.path().to_str().unwrap()).await.unwrap();

    let rels = vec![RelRef {
        id: "e1".into(),
        src: "alice".into(),
        dst: "bob".into(),
        rel_type: "KNOWS".into(),
        props: IndexMap::new(),
    }];
    store.write_edges(&rels).await.unwrap();

    let loaded = store.load_edges().await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].id, "e1");
    assert_eq!(loaded[0].src, "alice");
    assert_eq!(loaded[0].dst, "bob");
    assert_eq!(loaded[0].rel_type, "KNOWS");
}

#[tokio::test]
async fn test_to_memory_graph_and_cypher_match() {
    let dir = tempfile::tempdir().unwrap();
    let store = LanceGraphStore::new(dir.path().to_str().unwrap()).await.unwrap();

    // Write nodes
    let mut alice_props = IndexMap::new();
    alice_props.insert("name".into(), str_val("Alice"));
    let mut bob_props = IndexMap::new();
    bob_props.insert("name".into(), str_val("Bob"));

    store.write_vertices(&[
        NodeRef { id: "alice".into(), labels: vec!["Person".into()], props: alice_props },
        NodeRef { id: "bob".into(), labels: vec!["Person".into()], props: bob_props },
    ]).await.unwrap();

    store.write_edges(&[RelRef {
        id: "e1".into(),
        src: "alice".into(),
        dst: "bob".into(),
        rel_type: "KNOWS".into(),
        props: IndexMap::new(),
    }]).await.unwrap();

    // Load into MemoryGraph and run Cypher
    let mut g = store.to_memory_graph().await.unwrap();
    let q = parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name").unwrap();
    let ex = Executor::new();
    let result = ex.execute(&q, &mut g).unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].0.get("a.name"), Some(&str_val("Alice")));
    assert_eq!(result.rows[0].0.get("b.name"), Some(&str_val("Bob")));
}

#[tokio::test]
async fn test_cypher_create_then_persist() {
    // Tests the full write-back cycle:
    // 1. Create nodes via Cypher (MemoryGraph)
    // 2. Persist to LanceDB (write_vertices / write_edges)
    // 3. Reload and verify
    let dir = tempfile::tempdir().unwrap();
    let store = LanceGraphStore::new(dir.path().to_str().unwrap()).await.unwrap();

    // Step 1: CREATE via Cypher on MemoryGraph
    let mut g = MemoryGraph::new();
    let q = parse("CREATE (a:Company {name: 'GFTD'})-[:FOUNDED_BY]->(b:Person {name: 'Taro'})").unwrap();
    let ex = Executor::new();
    ex.execute(&q, &mut g).unwrap();

    // Step 2: Persist to Lance
    store.write_vertices(&g.nodes()).await.unwrap();
    store.write_edges(&g.rels()).await.unwrap();

    // Step 3: Reload and run MATCH
    let mut g2 = store.to_memory_graph().await.unwrap();
    let q2 = parse("MATCH (c:Company)-[:FOUNDED_BY]->(p:Person) RETURN c.name, p.name").unwrap();
    let result = ex.execute(&q2, &mut g2).unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].0.get("c.name"), Some(&str_val("GFTD")));
    assert_eq!(result.rows[0].0.get("p.name"), Some(&str_val("Taro")));
}

#[tokio::test]
async fn test_multi_label_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let store = LanceGraphStore::new(dir.path().to_str().unwrap()).await.unwrap();

    let mut props = IndexMap::new();
    props.insert("name".into(), str_val("Charlie"));

    store.write_vertices(&[NodeRef {
        id: "charlie".into(),
        labels: vec!["Person".into(), "Employee".into()],
        props,
    }]).await.unwrap();

    let loaded = store.load_vertices().await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert!(loaded[0].labels.contains(&"Person".to_string()));
    assert!(loaded[0].labels.contains(&"Employee".to_string()));
}

#[tokio::test]
async fn test_schemaless_mixed_prop_types() {
    // Verify that different prop types round-trip correctly
    let dir = tempfile::tempdir().unwrap();
    let store = LanceGraphStore::new(dir.path().to_str().unwrap()).await.unwrap();

    let mut props = IndexMap::new();
    props.insert("name".into(), str_val("Dave"));
    props.insert("score".into(), int_val(42));
    props.insert("ratio".into(), yata_cypher::Value::Float(3.14));
    props.insert("active".into(), yata_cypher::Value::Bool(true));
    props.insert("tags".into(), yata_cypher::Value::List(vec![
        str_val("rust"), str_val("graph"),
    ]));

    store.write_vertices(&[NodeRef {
        id: "dave".into(),
        labels: vec!["Node".into()],
        props,
    }]).await.unwrap();

    let loaded = store.load_vertices().await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].props.get("score"), Some(&int_val(42)));
    assert_eq!(loaded[0].props.get("active"), Some(&yata_cypher::Value::Bool(true)));
    // Float comparison
    if let Some(yata_cypher::Value::Float(f)) = loaded[0].props.get("ratio") {
        assert!((f - 3.14).abs() < 0.001);
    } else {
        panic!("expected Float for ratio");
    }
    // List comparison
    if let Some(yata_cypher::Value::List(l)) = loaded[0].props.get("tags") {
        assert_eq!(l.len(), 2);
        assert_eq!(l[0], str_val("rust"));
    } else {
        panic!("expected List for tags");
    }
}
