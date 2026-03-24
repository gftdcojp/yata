use super::*;
use indexmap::IndexMap;
use yata_cypher::{Executor, Graph, MemoryGraph, NodeRef, RelRef, parse};

fn str_val(s: &str) -> yata_cypher::Value {
    yata_cypher::Value::Str(s.to_owned())
}

fn int_val(i: i64) -> yata_cypher::Value {
    yata_cypher::Value::Int(i)
}

#[tokio::test]
async fn test_write_and_load_vertices() {
    let store = GraphStore::new("memory://test").await.unwrap();

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
async fn test_write_and_load_edges() {
    let store = GraphStore::new("memory://test").await.unwrap();

    // Must write vertices first for MemoryGraph
    store
        .write_vertices(&[
            NodeRef {
                id: "alice".into(),
                labels: vec![],
                props: IndexMap::new(),
            },
            NodeRef {
                id: "bob".into(),
                labels: vec![],
                props: IndexMap::new(),
            },
        ])
        .await
        .unwrap();

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
    let store = GraphStore::new("memory://test").await.unwrap();

    let mut alice_props = IndexMap::new();
    alice_props.insert("name".into(), str_val("Alice"));
    let mut bob_props = IndexMap::new();
    bob_props.insert("name".into(), str_val("Bob"));

    store
        .write_vertices(&[
            NodeRef {
                id: "alice".into(),
                labels: vec!["Person".into()],
                props: alice_props,
            },
            NodeRef {
                id: "bob".into(),
                labels: vec!["Person".into()],
                props: bob_props,
            },
        ])
        .await
        .unwrap();

    store
        .write_edges(&[RelRef {
            id: "e1".into(),
            src: "alice".into(),
            dst: "bob".into(),
            rel_type: "KNOWS".into(),
            props: IndexMap::new(),
        }])
        .await
        .unwrap();

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
    let store = GraphStore::new("memory://test").await.unwrap();

    // Step 1: CREATE via Cypher on MemoryGraph
    let mut g = MemoryGraph::new();
    let q = parse("CREATE (a:Company {name: 'GFTD'})-[:FOUNDED_BY]->(b:Person {name: 'Taro'})")
        .unwrap();
    let ex = Executor::new();
    ex.execute(&q, &mut g).unwrap();

    // Step 2: Persist to in-memory store
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
    let store = GraphStore::new("memory://test").await.unwrap();

    let mut props = IndexMap::new();
    props.insert("name".into(), str_val("Charlie"));

    store
        .write_vertices(&[NodeRef {
            id: "charlie".into(),
            labels: vec!["Person".into(), "Employee".into()],
            props,
        }])
        .await
        .unwrap();

    let loaded = store.load_vertices().await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert!(loaded[0].labels.contains(&"Person".to_string()));
    assert!(loaded[0].labels.contains(&"Employee".to_string()));
}

#[tokio::test]
async fn test_schemaless_mixed_prop_types() {
    let store = GraphStore::new("memory://test").await.unwrap();

    let mut props = IndexMap::new();
    props.insert("name".into(), str_val("Dave"));
    props.insert("score".into(), int_val(42));
    props.insert("ratio".into(), yata_cypher::Value::Float(3.14));
    props.insert("active".into(), yata_cypher::Value::Bool(true));
    props.insert(
        "tags".into(),
        yata_cypher::Value::List(vec![str_val("rust"), str_val("graph")]),
    );

    store
        .write_vertices(&[NodeRef {
            id: "dave".into(),
            labels: vec!["Node".into()],
            props,
        }])
        .await
        .unwrap();

    let loaded = store.load_vertices().await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].props.get("score"), Some(&int_val(42)));
    assert_eq!(
        loaded[0].props.get("active"),
        Some(&yata_cypher::Value::Bool(true))
    );
    if let Some(yata_cypher::Value::Float(f)) = loaded[0].props.get("ratio") {
        assert!((f - 3.14).abs() < 0.001);
    } else {
        panic!("expected Float for ratio");
    }
    if let Some(yata_cypher::Value::List(l)) = loaded[0].props.get("tags") {
        assert_eq!(l.len(), 2);
        assert_eq!(l[0], str_val("rust"));
    } else {
        panic!("expected List for tags");
    }
}

#[tokio::test]
async fn test_upsert_vertex_last_write_wins() {
    let store = GraphStore::new("memory://test").await.unwrap();

    let mut props_v1 = IndexMap::new();
    props_v1.insert("name".into(), str_val("Alice"));
    props_v1.insert("age".into(), int_val(25));

    store
        .write_vertices(&[NodeRef {
            id: "alice".into(),
            labels: vec!["Person".into()],
            props: props_v1,
        }])
        .await
        .unwrap();

    let mut props_v2 = IndexMap::new();
    props_v2.insert("name".into(), str_val("Alice"));
    props_v2.insert("age".into(), int_val(30));

    store
        .write_vertices(&[NodeRef {
            id: "alice".into(),
            labels: vec!["Person".into()],
            props: props_v2,
        }])
        .await
        .unwrap();

    let loaded = store.load_vertices().await.unwrap();
    assert_eq!(loaded.len(), 1, "dedup: should have exactly 1 vertex");
    assert_eq!(loaded[0].id, "alice");
    assert_eq!(
        loaded[0].props.get("age"),
        Some(&int_val(30)),
        "last-write-wins: age=30"
    );
}

#[tokio::test]
async fn test_upsert_edge_last_write_wins() {
    let store = GraphStore::new("memory://test").await.unwrap();

    store
        .write_vertices(&[
            NodeRef {
                id: "alice".into(),
                labels: vec![],
                props: IndexMap::new(),
            },
            NodeRef {
                id: "bob".into(),
                labels: vec![],
                props: IndexMap::new(),
            },
        ])
        .await
        .unwrap();

    store
        .write_edges(&[RelRef {
            id: "e1".into(),
            src: "alice".into(),
            dst: "bob".into(),
            rel_type: "KNOWS".into(),
            props: IndexMap::new(),
        }])
        .await
        .unwrap();

    let mut props2 = IndexMap::new();
    props2.insert("since".into(), int_val(2024));

    store
        .write_edges(&[RelRef {
            id: "e1".into(),
            src: "alice".into(),
            dst: "bob".into(),
            rel_type: "KNOWS".into(),
            props: props2,
        }])
        .await
        .unwrap();

    let loaded = store.load_edges().await.unwrap();
    assert_eq!(loaded.len(), 1, "dedup: should have exactly 1 edge");
    assert_eq!(loaded[0].id, "e1");
    assert_eq!(
        loaded[0].props.get("since"),
        Some(&int_val(2024)),
        "last-write-wins: since=2024"
    );
}

#[tokio::test]
async fn test_write_vertices_with_embeddings() {
    let store = GraphStore::new("memory://test").await.unwrap();

    let nodes = vec![
        NodeRef {
            id: "d1".into(),
            labels: vec!["Doc".into()],
            props: {
                let mut p = IndexMap::new();
                p.insert("title".into(), str_val("rust"));
                p.insert(
                    "embedding".into(),
                    yata_cypher::Value::List(vec![
                        yata_cypher::Value::Float(1.0),
                        yata_cypher::Value::Float(0.0),
                        yata_cypher::Value::Float(0.0),
                    ]),
                );
                p
            },
        },
        NodeRef {
            id: "d2".into(),
            labels: vec!["Doc".into()],
            props: {
                let mut p = IndexMap::new();
                p.insert("title".into(), str_val("python"));
                p.insert(
                    "embedding".into(),
                    yata_cypher::Value::List(vec![
                        yata_cypher::Value::Float(0.0),
                        yata_cypher::Value::Float(1.0),
                        yata_cypher::Value::Float(0.0),
                    ]),
                );
                p
            },
        },
    ];

    store
        .write_vertices_with_embeddings(&nodes, "embedding", 3)
        .await
        .unwrap();

    let loaded = store.load_vertices().await.unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].props.get("title"), Some(&str_val("rust")));
    // embedding should NOT be in props (extracted to vector index)
    assert!(
        loaded[0].props.get("embedding").is_none(),
        "embedding should be in vector index, not props"
    );
}

// ── cached_query tests ──────────────────────────────────────────────────

#[tokio::test]
async fn test_cached_query_create_and_match() {
    let store = GraphStore::new("memory://test").await.unwrap();

    // CREATE via cached_query
    let result = store.cached_query(
        "CREATE (a:Person {name: \"Alice\", age: \"30\"}), (b:Person {name: \"Bob\", age: \"25\"}) RETURN a.name",
        &[],
    ).await.unwrap();
    assert!(!result.cache_hit);
    assert!(result.delta.is_some());
    assert_eq!(result.delta.unwrap().nodes_created, 2);

    // MATCH should find persisted data
    let result = store
        .cached_query("MATCH (n:Person) RETURN n.name, n.age", &[])
        .await
        .unwrap();
    assert!(!result.cache_hit); // first read after write = cache invalidated
    assert_eq!(result.rows.len(), 2);

    // Same query again = cache hit
    let result2 = store
        .cached_query("MATCH (n:Person) RETURN n.name, n.age", &[])
        .await
        .unwrap();
    assert!(result2.cache_hit);
    assert_eq!(result2.rows.len(), 2);
}

#[tokio::test]
async fn test_cached_query_create_with_edge_then_match_path() {
    let store = GraphStore::new("memory://test").await.unwrap();

    // CREATE nodes + edge
    store.cached_query(
        "CREATE (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}), (a)-[:KNOWS {since: \"2024\"}]->(b) RETURN a.name",
        &[],
    ).await.unwrap();

    // MATCH path pattern
    let result = store
        .cached_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 KNOWS relationship, got {}",
        result.rows.len()
    );
    let row = &result.rows[0];
    let a_name = row
        .iter()
        .find(|(c, _)| c == "a.name")
        .map(|(_, v)| v.as_str());
    let b_name = row
        .iter()
        .find(|(c, _)| c == "b.name")
        .map(|(_, v)| v.as_str());
    assert!(a_name.is_some(), "a.name should not be null");
    assert!(b_name.is_some(), "b.name should not be null");
    assert_ne!(a_name.unwrap(), "null", "a.name should not be 'null'");
    assert_ne!(b_name.unwrap(), "null", "b.name should not be 'null'");
}

#[tokio::test]
async fn test_cached_query_merge_set_persists() {
    let store = GraphStore::new("memory://test").await.unwrap();

    // CREATE initial node
    store
        .cached_query(
            "CREATE (x:Engineer {name: \"Sato\", skill: \"Rust\"}) RETURN x.name",
            &[],
        )
        .await
        .unwrap();

    // MERGE + SET new property
    let result = store
        .cached_query(
            "MERGE (x:Engineer {name: \"Sato\"}) SET x.level = \"senior\" RETURN x.name, x.level",
            &[],
        )
        .await
        .unwrap();
    assert!(!result.cache_hit);

    // Reload and verify the SET property persisted
    let result = store
        .cached_query(
            "MATCH (e:Engineer {name: \"Sato\"}) RETURN e.name, e.skill, e.level",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    let level = row
        .iter()
        .find(|(c, _)| c == "e.level")
        .map(|(_, v)| v.as_str());
    println!("MERGE SET test - row: {:?}", row);
    println!("level = {:?}", level);
}

#[tokio::test]
async fn test_cached_query_write_invalidates_cache() {
    let store = GraphStore::new("memory://test").await.unwrap();

    store
        .cached_query("CREATE (a:X {v: \"1\"}) RETURN a.v", &[])
        .await
        .unwrap();

    // Read -> cache miss (post-write invalidation)
    let r1 = store
        .cached_query("MATCH (n:X) RETURN n.v", &[])
        .await
        .unwrap();
    assert!(!r1.cache_hit);
    assert_eq!(r1.rows.len(), 1);

    // Same read -> cache hit
    let r2 = store
        .cached_query("MATCH (n:X) RETURN n.v", &[])
        .await
        .unwrap();
    assert!(r2.cache_hit);

    // Write -> invalidates
    store
        .cached_query("CREATE (b:X {v: \"2\"}) RETURN b.v", &[])
        .await
        .unwrap();

    // Read again -> cache miss, should see 2 nodes
    let r3 = store
        .cached_query("MATCH (n:X) RETURN n.v", &[])
        .await
        .unwrap();
    assert!(!r3.cache_hit);
    assert_eq!(r3.rows.len(), 2);
}

#[tokio::test]
async fn test_empty_store_returns_empty() {
    let store = GraphStore::new("memory://test").await.unwrap();
    let nodes = store.load_vertices().await.unwrap();
    assert!(nodes.is_empty());
    let edges = store.load_edges().await.unwrap();
    assert!(edges.is_empty());
}

#[tokio::test]
async fn test_optimize_noop() {
    let store = GraphStore::new("memory://test").await.unwrap();
    store.optimize().await.unwrap();
}

#[tokio::test]
async fn test_create_embedding_index_noop() {
    let store = GraphStore::new("memory://test").await.unwrap();
    store.create_embedding_index().await.unwrap();
}

// ── UTF-8 tests (QueryableGraph.query() → JSON serialization) ──────────

#[test]
fn test_utf8_queryable_graph_japanese_literal() {
    let mut qg = QueryableGraph(yata_cypher::MemoryGraph::new());
    qg.query(
        r#"CREATE (n:Article {id: "a1", title: "半導体市場が急拡大"})"#,
        &[],
    )
    .unwrap();

    let result = qg
        .query(
            r#"MATCH (n:Article {id: "a1"}) RETURN n.title AS title"#,
            &[],
        )
        .unwrap();

    assert_eq!(result.len(), 1);
    let (col, val) = &result[0][0];
    assert_eq!(col, "title");
    let parsed: String = serde_json::from_str(val).unwrap();
    assert_eq!(parsed, "半導体市場が急拡大");
}

#[test]
fn test_utf8_queryable_graph_with_params() {
    let mut qg = QueryableGraph(yata_cypher::MemoryGraph::new());
    let params = vec![
        ("id".to_string(), "\"a2\"".to_string()),
        ("title".to_string(), "\"こんにちは世界\"".to_string()),
    ];
    qg.query("CREATE (n:Article {id: $id, title: $title})", &params)
        .unwrap();

    let result = qg
        .query(
            r#"MATCH (n:Article {id: "a2"}) RETURN n.title AS title"#,
            &[],
        )
        .unwrap();

    assert_eq!(result.len(), 1);
    let parsed: String = serde_json::from_str(&result[0][0].1).unwrap();
    assert_eq!(parsed, "こんにちは世界");
}

#[test]
fn test_utf8_queryable_graph_large_content() {
    let mut qg = QueryableGraph(yata_cypher::MemoryGraph::new());
    let large_content = "人工知能向け半導体の世界市場が成長を続けている。".repeat(100);
    let params = vec![
        ("id".to_string(), "\"large1\"".to_string()),
        (
            "content".to_string(),
            serde_json::to_string(&large_content).unwrap(),
        ),
    ];
    qg.query("CREATE (n:Article {id: $id, content: $content})", &params)
        .unwrap();

    let result = qg
        .query(
            r#"MATCH (n:Article {id: "large1"}) RETURN n.content AS content"#,
            &[],
        )
        .unwrap();

    assert_eq!(result.len(), 1);
    let parsed: String = serde_json::from_str(&result[0][0].1).unwrap();
    assert_eq!(parsed, large_content);
}
