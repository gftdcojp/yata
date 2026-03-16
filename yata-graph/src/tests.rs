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
    let result = ex.execute(&q, &mut g.0).unwrap();

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
    let result = ex.execute(&q2, &mut g2.0).unwrap();

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

#[tokio::test]
async fn test_upsert_vertex_last_write_wins() {
    // Write same vid twice — load_vertices should return exactly 1 node (last write).
    let dir = tempfile::tempdir().unwrap();
    let store = LanceGraphStore::new(dir.path().to_str().unwrap()).await.unwrap();

    let mut props_v1 = IndexMap::new();
    props_v1.insert("name".into(), str_val("Alice"));
    props_v1.insert("age".into(), int_val(25));

    store.write_vertices(&[NodeRef {
        id: "alice".into(),
        labels: vec!["Person".into()],
        props: props_v1,
    }]).await.unwrap();

    // Second write — age updated to 30.
    let mut props_v2 = IndexMap::new();
    props_v2.insert("name".into(), str_val("Alice"));
    props_v2.insert("age".into(), int_val(30));

    store.write_vertices(&[NodeRef {
        id: "alice".into(),
        labels: vec!["Person".into()],
        props: props_v2,
    }]).await.unwrap();

    let loaded = store.load_vertices().await.unwrap();
    assert_eq!(loaded.len(), 1, "dedup: should have exactly 1 vertex");
    assert_eq!(loaded[0].id, "alice");
    assert_eq!(loaded[0].props.get("age"), Some(&int_val(30)), "last-write-wins: age=30");
}

#[tokio::test]
async fn test_upsert_edge_last_write_wins() {
    // Write same eid twice — load_edges should return exactly 1 edge.
    let dir = tempfile::tempdir().unwrap();
    let store = LanceGraphStore::new(dir.path().to_str().unwrap()).await.unwrap();

    store.write_edges(&[RelRef {
        id: "e1".into(),
        src: "alice".into(),
        dst: "bob".into(),
        rel_type: "KNOWS".into(),
        props: IndexMap::new(),
    }]).await.unwrap();

    let mut props2 = IndexMap::new();
    props2.insert("since".into(), int_val(2024));

    store.write_edges(&[RelRef {
        id: "e1".into(),
        src: "alice".into(),
        dst: "bob".into(),
        rel_type: "KNOWS".into(),
        props: props2,
    }]).await.unwrap();

    let loaded = store.load_edges().await.unwrap();
    assert_eq!(loaded.len(), 1, "dedup: should have exactly 1 edge");
    assert_eq!(loaded[0].id, "e1");
    assert_eq!(loaded[0].props.get("since"), Some(&int_val(2024)), "last-write-wins: since=2024");
}

#[tokio::test]
async fn test_write_vertices_with_embeddings() {
    let dir = tempfile::tempdir().unwrap();
    let store = LanceGraphStore::new(dir.path().to_str().unwrap()).await.unwrap();

    let nodes = vec![
        NodeRef {
            id: "d1".into(),
            labels: vec!["Doc".into()],
            props: {
                let mut p = IndexMap::new();
                p.insert("title".into(), str_val("rust"));
                p.insert("embedding".into(), yata_cypher::Value::List(vec![
                    yata_cypher::Value::Float(1.0),
                    yata_cypher::Value::Float(0.0),
                    yata_cypher::Value::Float(0.0),
                ]));
                p
            },
        },
        NodeRef {
            id: "d2".into(),
            labels: vec!["Doc".into()],
            props: {
                let mut p = IndexMap::new();
                p.insert("title".into(), str_val("python"));
                p.insert("embedding".into(), yata_cypher::Value::List(vec![
                    yata_cypher::Value::Float(0.0),
                    yata_cypher::Value::Float(1.0),
                    yata_cypher::Value::Float(0.0),
                ]));
                p
            },
        },
    ];

    store.write_vertices_with_embeddings(&nodes, "embedding", 3).await.unwrap();

    // Verify we can load them back (standard load won't include embedding column but props are preserved)
    let loaded = store.load_vertices().await.unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].props.get("title"), Some(&str_val("rust")));
    // embedding should NOT be in props_json (it was extracted to dedicated column)
    assert!(loaded[0].props.get("embedding").is_none(), "embedding should be in dedicated column, not props");
}

#[tokio::test]
async fn test_vector_search_vertices() {
    let dir = tempfile::tempdir().unwrap();
    let store = LanceGraphStore::new(dir.path().to_str().unwrap()).await.unwrap();

    let nodes = vec![
        NodeRef {
            id: "d1".into(),
            labels: vec!["Doc".into()],
            props: {
                let mut p = IndexMap::new();
                p.insert("title".into(), str_val("rust"));
                p.insert("embedding".into(), yata_cypher::Value::List(vec![
                    yata_cypher::Value::Float(1.0),
                    yata_cypher::Value::Float(0.0),
                ]));
                p
            },
        },
        NodeRef {
            id: "d2".into(),
            labels: vec!["Doc".into()],
            props: {
                let mut p = IndexMap::new();
                p.insert("title".into(), str_val("python"));
                p.insert("embedding".into(), yata_cypher::Value::List(vec![
                    yata_cypher::Value::Float(0.0),
                    yata_cypher::Value::Float(1.0),
                ]));
                p
            },
        },
    ];

    store.write_vertices_with_embeddings(&nodes, "embedding", 2).await.unwrap();

    // Search for vector closest to [1.0, 0.0]
    let results = store.vector_search_vertices(
        vec![1.0, 0.0],
        2,
        None,
        None,
    ).await.unwrap();
    assert!(!results.is_empty(), "should find at least 1 result");
    // First result should be d1 (closest to query)
    assert_eq!(results[0].0.id, "d1");
}

#[tokio::test]
async fn test_vector_search_cosine_ranking() {
    let dir = tempfile::tempdir().unwrap();
    let store = LanceGraphStore::new(dir.path().to_str().unwrap()).await.unwrap();

    let nodes = vec![
        NodeRef {
            id: "close".into(),
            labels: vec!["V".into()],
            props: {
                let mut p = IndexMap::new();
                p.insert("embedding".into(), yata_cypher::Value::List(vec![
                    yata_cypher::Value::Float(0.9),
                    yata_cypher::Value::Float(0.1),
                ]));
                p
            },
        },
        NodeRef {
            id: "far".into(),
            labels: vec!["V".into()],
            props: {
                let mut p = IndexMap::new();
                p.insert("embedding".into(), yata_cypher::Value::List(vec![
                    yata_cypher::Value::Float(0.0),
                    yata_cypher::Value::Float(1.0),
                ]));
                p
            },
        },
    ];

    store.write_vertices_with_embeddings(&nodes, "embedding", 2).await.unwrap();

    let results = store.vector_search_vertices(
        vec![1.0, 0.0],
        2,
        None,
        None,
    ).await.unwrap();
    assert_eq!(results.len(), 2);
    // "close" should have smaller distance than "far"
    assert!(results[0].1 < results[1].1, "close should have smaller distance: {} vs {}", results[0].1, results[1].1);
    assert_eq!(results[0].0.id, "close");
}
