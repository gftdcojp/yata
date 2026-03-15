#[cfg(test)]
mod tests {
    use crate::executor::Executor;
    use crate::graph::{Graph, MemoryGraph};
    use crate::parser::parse;
    use crate::types::{NodeRef, RelRef, Value};

    fn make_graph() -> MemoryGraph {
        let mut g = MemoryGraph::new();
        g.add_node(NodeRef {
            id: "n1".into(),
            labels: vec!["Person".into()],
            props: [
                ("name".into(), Value::Str("Alice".into())),
                ("age".into(), Value::Int(30)),
            ]
            .into_iter()
            .collect(),
        });
        g.add_node(NodeRef {
            id: "n2".into(),
            labels: vec!["Person".into()],
            props: [
                ("name".into(), Value::Str("Bob".into())),
                ("age".into(), Value::Int(25)),
            ]
            .into_iter()
            .collect(),
        });
        g.add_node(NodeRef {
            id: "n3".into(),
            labels: vec!["Company".into()],
            props: [("name".into(), Value::Str("GFTD".into()))]
                .into_iter()
                .collect(),
        });
        g.add_rel(RelRef {
            id: "r1".into(),
            rel_type: "KNOWS".into(),
            src: "n1".into(),
            dst: "n2".into(),
            props: Default::default(),
        });
        g.add_rel(RelRef {
            id: "r2".into(),
            rel_type: "WORKS_AT".into(),
            src: "n1".into(),
            dst: "n3".into(),
            props: Default::default(),
        });
        g
    }

    /// Chain graph: n1 -> n2 -> n4 -> n5 (all FOLLOWS)
    fn make_chain_graph() -> MemoryGraph {
        let mut g = make_graph();
        g.add_node(NodeRef {
            id: "n4".into(),
            labels: vec!["Person".into()],
            props: [
                ("name".into(), Value::Str("Charlie".into())),
                ("age".into(), Value::Int(35)),
            ]
            .into_iter()
            .collect(),
        });
        g.add_node(NodeRef {
            id: "n5".into(),
            labels: vec!["Person".into()],
            props: [
                ("name".into(), Value::Str("Diana".into())),
                ("age".into(), Value::Int(28)),
            ]
            .into_iter()
            .collect(),
        });
        g.add_rel(RelRef {
            id: "r3".into(),
            rel_type: "FOLLOWS".into(),
            src: "n1".into(),
            dst: "n2".into(),
            props: Default::default(),
        });
        g.add_rel(RelRef {
            id: "r4".into(),
            rel_type: "FOLLOWS".into(),
            src: "n2".into(),
            dst: "n4".into(),
            props: Default::default(),
        });
        g.add_rel(RelRef {
            id: "r5".into(),
            rel_type: "FOLLOWS".into(),
            src: "n4".into(),
            dst: "n5".into(),
            props: Default::default(),
        });
        g
    }

    fn exec(cypher: &str, g: &mut MemoryGraph) -> crate::types::ResultSet {
        let q = parse(cypher).expect("parse failed");
        let ex = Executor::new();
        ex.execute(&q, g).expect("execute failed")
    }

    fn exec_with_params(
        cypher: &str,
        params: indexmap::IndexMap<String, Value>,
        g: &mut MemoryGraph,
    ) -> crate::types::ResultSet {
        let q = parse(cypher).expect("parse failed");
        let ex = Executor::with_params(params);
        ex.execute(&q, g).expect("execute failed")
    }

    // ========================================================================
    // Basic MATCH / RETURN
    // ========================================================================

    #[test]
    fn test_match_all_nodes() {
        let mut g = make_graph();
        let rs = exec("MATCH (n) RETURN n", &mut g);
        assert_eq!(rs.rows.len(), 3);
        assert!(rs.columns.contains(&"n".to_string()));
        for row in &rs.rows {
            assert!(matches!(row.0.get("n"), Some(Value::Node(_))));
        }
    }

    #[test]
    fn test_match_by_label() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person) RETURN n.name", &mut g);
        assert_eq!(rs.rows.len(), 2);
        let names: Vec<String> = rs
            .rows
            .iter()
            .filter_map(|row| match row.0.values().next() {
                Some(Value::Str(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert!(names.contains(&"Alice".to_string()));
        assert!(names.contains(&"Bob".to_string()));
    }

    #[test]
    fn test_match_by_prop() {
        let mut g = make_graph();
        let rs = exec(r#"MATCH (n:Person {name: "Alice"}) RETURN n.age"#, &mut g);
        assert_eq!(rs.rows.len(), 1);
        let age = rs.rows[0].0.values().next().cloned();
        assert_eq!(age, Some(Value::Int(30)));
    }

    #[test]
    fn test_match_relationship() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        let row = &rs.rows[0].0;
        let a_name = row.get("a.name").cloned();
        let b_name = row.get("b.name").cloned();
        assert_eq!(a_name, Some(Value::Str("Alice".into())));
        assert_eq!(b_name, Some(Value::Str("Bob".into())));
    }

    #[test]
    fn test_match_undirected_rel() {
        let mut g = make_graph();
        let rs = exec("MATCH (a)-[:KNOWS]-(b) RETURN a.name, b.name", &mut g);
        assert_eq!(rs.rows.len(), 2);
    }

    #[test]
    fn test_match_all_rels() {
        let mut g = make_graph();
        let rs = exec("MATCH (a)-[r]->(b) RETURN r", &mut g);
        assert_eq!(rs.rows.len(), 2);
    }

    #[test]
    fn test_match_multiple_patterns() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (a:Person {name: 'Alice'}), (c:Company) RETURN a.name, c.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.get("a.name").cloned(),
            Some(Value::Str("Alice".into()))
        );
        assert_eq!(
            rs.rows[0].0.get("c.name").cloned(),
            Some(Value::Str("GFTD".into()))
        );
    }

    #[test]
    fn test_return_star() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Company) RETURN *", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert!(rs.rows[0].0.contains_key("n"));
    }

    // ========================================================================
    // WHERE / Filters
    // ========================================================================

    #[test]
    fn test_where_filter() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person) WHERE n.age > 26 RETURN n.name", &mut g);
        assert_eq!(rs.rows.len(), 1);
        let name = rs.rows[0].0.values().next().cloned();
        assert_eq!(name, Some(Value::Str("Alice".into())));
    }

    #[test]
    fn test_where_and() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE n.age > 20 AND n.age < 28 RETURN n.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        let name = rs.rows[0].0.values().next().cloned();
        assert_eq!(name, Some(Value::Str("Bob".into())));
    }

    #[test]
    fn test_where_or() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE n.name = 'Alice' OR n.name = 'Bob' RETURN n.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 2);
    }

    #[test]
    fn test_where_xor() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE n.age > 27 XOR n.age < 27 RETURN n.name",
            &mut g,
        );
        // Alice(30) > 27 and NOT < 27: true XOR false = true
        // Bob(25) NOT > 27 and < 27: false XOR true = true
        assert_eq!(rs.rows.len(), 2);
    }

    #[test]
    fn test_where_not() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE NOT n.name = 'Alice' RETURN n.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("Bob".into()))
        );
    }

    #[test]
    fn test_where_in_list() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE n.name IN ['Alice', 'Charlie'] RETURN n.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("Alice".into()))
        );
    }

    #[test]
    fn test_is_null() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Company) RETURN n.age IS NULL", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Bool(true))
        );
    }

    #[test]
    fn test_is_not_null() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 2);
    }

    // ========================================================================
    // String predicates: STARTS WITH, ENDS WITH, CONTAINS, =~
    // ========================================================================

    #[test]
    fn test_starts_with() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE n.name STARTS WITH 'Al' RETURN n.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("Alice".into()))
        );
    }

    #[test]
    fn test_ends_with() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE n.name ENDS WITH 'ob' RETURN n.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("Bob".into()))
        );
    }

    #[test]
    fn test_contains_predicate() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE n.name CONTAINS 'lic' RETURN n.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("Alice".into()))
        );
    }

    #[test]
    fn test_regex_match() {
        let mut g = make_graph();
        let rs = exec(
            r#"MATCH (n:Person) WHERE n.name =~ "^A.*e$" RETURN n.name"#,
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("Alice".into()))
        );
    }

    #[test]
    fn test_regex_no_match() {
        let mut g = make_graph();
        let rs = exec(
            r#"MATCH (n:Person) WHERE n.name =~ "^Z.*" RETURN n.name"#,
            &mut g,
        );
        assert_eq!(rs.rows.len(), 0);
    }

    // ========================================================================
    // Aggregation
    // ========================================================================

    #[test]
    fn test_count_aggregation() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person) RETURN count(n)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        let count = rs.rows[0].0.values().next().cloned();
        assert_eq!(count, Some(Value::Int(2)));
    }

    #[test]
    fn test_count_star() {
        let mut g = make_graph();
        let rs = exec("MATCH (n) RETURN count(*)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(3))
        );
    }

    #[test]
    fn test_collect_aggregation() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person) RETURN collect(n.name)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        match rs.rows[0].0.values().next() {
            Some(Value::List(names)) => {
                assert_eq!(names.len(), 2);
            }
            _ => panic!("expected list from collect()"),
        }
    }

    #[test]
    fn test_sum_aggregation() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person) RETURN sum(n.age)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(55))
        );
    }

    #[test]
    fn test_avg_aggregation() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person) RETURN avg(n.age)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - 27.5).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_min_aggregation() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person) RETURN min(n.age)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(25))
        );
    }

    #[test]
    fn test_max_aggregation() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person) RETURN max(n.age)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(30))
        );
    }

    #[test]
    fn test_grouped_aggregation() {
        let mut g = make_graph();
        // Add another Person at the same Company
        exec(
            r#"CREATE (n:Person {name: "Eve", age: 22})"#,
            &mut g,
        );
        let rs = exec(
            "MATCH (n) RETURN labels(n) AS lbls, count(n) AS cnt",
            &mut g,
        );
        // Two groups: [Person] and [Company]
        assert!(rs.rows.len() >= 2);
    }

    // ========================================================================
    // ORDER BY / LIMIT / SKIP / DISTINCT
    // ========================================================================

    #[test]
    fn test_order_limit() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) RETURN n.name ORDER BY n.name ASC LIMIT 1",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        let name = rs.rows[0].0.values().next().cloned();
        assert_eq!(name, Some(Value::Str("Alice".into())));
    }

    #[test]
    fn test_order_desc() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) RETURN n.name ORDER BY n.age DESC",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 2);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("Alice".into()))
        );
        assert_eq!(
            rs.rows[1].0.values().next().cloned(),
            Some(Value::Str("Bob".into()))
        );
    }

    #[test]
    fn test_skip() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) RETURN n.name ORDER BY n.name ASC SKIP 1",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        let name = rs.rows[0].0.values().next().cloned();
        assert_eq!(name, Some(Value::Str("Bob".into())));
    }

    #[test]
    fn test_skip_and_limit() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n) RETURN n.name ORDER BY n.name ASC SKIP 1 LIMIT 1",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
    }

    #[test]
    fn test_distinct() {
        let mut g = make_graph();
        // Both KNOWS and WORKS_AT go from n1, so matching (a)-[]->(b) with RETURN DISTINCT a.name
        let rs = exec(
            "MATCH (a)-[]->(b) RETURN DISTINCT a.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("Alice".into()))
        );
    }

    // ========================================================================
    // CREATE / MERGE / SET / DELETE
    // ========================================================================

    #[test]
    fn test_create_node() {
        let mut g = make_graph();
        exec(r#"CREATE (n:Person {name: "Charlie"})"#, &mut g);
        let rs = exec(r#"MATCH (n:Person {name: "Charlie"}) RETURN n.name"#, &mut g);
        assert_eq!(rs.rows.len(), 1);
        let name = rs.rows[0].0.values().next().cloned();
        assert_eq!(name, Some(Value::Str("Charlie".into())));
    }

    #[test]
    fn test_create_relationship() {
        let mut g = make_graph();
        exec(
            "MATCH (a:Person {name: 'Alice'}), (c:Company) CREATE (a)-[:CEO_OF]->(c)",
            &mut g,
        );
        let rs = exec("MATCH (a)-[:CEO_OF]->(c) RETURN a.name, c.name", &mut g);
        assert_eq!(rs.rows.len(), 1);
    }

    #[test]
    fn test_merge_creates_when_missing() {
        let mut g = make_graph();
        exec(r#"MERGE (n:Person {name: "NewPerson"})"#, &mut g);
        let rs = exec(r#"MATCH (n:Person {name: "NewPerson"}) RETURN n"#, &mut g);
        assert_eq!(rs.rows.len(), 1);
    }

    #[test]
    fn test_merge_finds_existing() {
        let mut g = make_graph();
        let before = exec("MATCH (n:Person) RETURN count(n)", &mut g);
        exec(r#"MERGE (n:Person {name: "Alice"})"#, &mut g);
        let after = exec("MATCH (n:Person) RETURN count(n)", &mut g);
        assert_eq!(
            before.rows[0].0.values().next().cloned(),
            after.rows[0].0.values().next().cloned()
        );
    }

    #[test]
    fn test_set_property() {
        let mut g = make_graph();
        exec(
            "MATCH (n:Person {name: 'Bob'}) SET n.age = 26",
            &mut g,
        );
        let rs = exec("MATCH (n:Person {name: 'Bob'}) RETURN n.age", &mut g);
        assert_eq!(rs.rows[0].0.values().next().cloned(), Some(Value::Int(26)));
    }

    #[test]
    fn test_set_label() {
        let mut g = make_graph();
        exec("MATCH (n:Person {name: 'Alice'}) SET n:Employee", &mut g);
        let rs = exec("MATCH (n:Employee) RETURN n.name", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("Alice".into()))
        );
    }

    #[test]
    fn test_detach_delete() {
        let mut g = make_graph();
        exec("MATCH (n:Company) DETACH DELETE n", &mut g);
        let rs = exec("MATCH (n:Company) RETURN n", &mut g);
        assert_eq!(rs.rows.len(), 0);
        let rs2 = exec("MATCH (a)-[r:WORKS_AT]->(b) RETURN r", &mut g);
        assert_eq!(rs2.rows.len(), 0);
    }

    #[test]
    fn test_delete_relationship() {
        let mut g = make_graph();
        exec("MATCH (a)-[r:KNOWS]->(b) DELETE r", &mut g);
        let rs = exec("MATCH (a)-[r:KNOWS]->(b) RETURN r", &mut g);
        assert_eq!(rs.rows.len(), 0);
        // Nodes should still exist
        let rs2 = exec("MATCH (n:Person) RETURN count(n)", &mut g);
        assert_eq!(
            rs2.rows[0].0.values().next().cloned(),
            Some(Value::Int(2))
        );
    }

    // ========================================================================
    // OPTIONAL MATCH
    // ========================================================================

    #[test]
    fn test_optional_match_found() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (a:Person {name: 'Alice'}) OPTIONAL MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.get("b.name").cloned(),
            Some(Value::Str("Bob".into()))
        );
    }

    #[test]
    fn test_optional_match_not_found() {
        let mut g = make_graph();
        // Bob has no outgoing KNOWS, so OPTIONAL MATCH finds nothing
        // The original binding (a=Bob) is preserved
        let rs = exec(
            "MATCH (a:Person {name: 'Bob'}) OPTIONAL MATCH (a)-[:KNOWS]->(b) RETURN a.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.get("a.name").cloned(),
            Some(Value::Str("Bob".into()))
        );
    }

    // ========================================================================
    // WITH clause
    // ========================================================================

    #[test]
    fn test_with_clause() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WITH n WHERE n.age > 26 RETURN n.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("Alice".into()))
        );
    }

    #[test]
    fn test_with_alias() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WITH n.name AS personName RETURN personName",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 2);
        assert!(rs.columns.contains(&"personName".to_string()));
    }

    // ========================================================================
    // UNWIND
    // ========================================================================

    #[test]
    fn test_unwind() {
        let mut g = make_graph();
        let rs = exec("UNWIND [1, 2, 3] AS x RETURN x", &mut g);
        assert_eq!(rs.rows.len(), 3);
        let vals: Vec<Value> = rs
            .rows
            .iter()
            .map(|r| r.0.get("x").cloned().unwrap())
            .collect();
        assert_eq!(vals, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    }

    #[test]
    fn test_unwind_null() {
        let mut g = make_graph();
        let rs = exec("UNWIND null AS x RETURN x", &mut g);
        assert_eq!(rs.rows.len(), 0);
    }

    // ========================================================================
    // Variable-hop relationships
    // ========================================================================

    #[test]
    fn test_variable_hop_exact() {
        let mut g = make_chain_graph();
        // n1 -FOLLOWS-> n2 -FOLLOWS-> n4: exactly 2 hops
        let rs = exec(
            "MATCH (a:Person {name: 'Alice'})-[r:FOLLOWS*2]->(b) RETURN b.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.get("b.name").cloned(),
            Some(Value::Str("Charlie".into()))
        );
    }

    #[test]
    fn test_variable_hop_range() {
        let mut g = make_chain_graph();
        // n1 -FOLLOWS*1..3-> should reach n2(1), n4(2), n5(3)
        let rs = exec(
            "MATCH (a:Person {name: 'Alice'})-[:FOLLOWS*1..3]->(b) RETURN b.name",
            &mut g,
        );
        let names: Vec<String> = rs
            .rows
            .iter()
            .filter_map(|r| match r.0.get("b.name") {
                Some(Value::Str(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"Bob".into()));
        assert!(names.contains(&"Charlie".into()));
        assert!(names.contains(&"Diana".into()));
    }

    #[test]
    fn test_variable_hop_min_only() {
        let mut g = make_chain_graph();
        // *2.. (min=2, max defaults to max(2,10)=10): should reach n4(2), n5(3)
        let rs = exec(
            "MATCH (a:Person {name: 'Alice'})-[:FOLLOWS*2..3]->(b) RETURN b.name",
            &mut g,
        );
        let names: Vec<String> = rs
            .rows
            .iter()
            .filter_map(|r| match r.0.get("b.name") {
                Some(Value::Str(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert!(names.contains(&"Charlie".into()));
        assert!(names.contains(&"Diana".into()));
        assert!(!names.contains(&"Bob".into()));
    }

    #[test]
    fn test_variable_hop_returns_rel_list() {
        let mut g = make_chain_graph();
        let rs = exec(
            "MATCH (a:Person {name: 'Alice'})-[r:FOLLOWS*2]->(b) RETURN r",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        match rs.rows[0].0.get("r") {
            Some(Value::List(rels)) => {
                assert_eq!(rels.len(), 2);
                for v in rels {
                    assert!(matches!(v, Value::Rel(_)));
                }
            }
            other => panic!("expected list of rels, got {:?}", other),
        }
    }

    // ========================================================================
    // Functions: graph navigation
    // ========================================================================

    #[test]
    fn test_id_function() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person) RETURN id(n)", &mut g);
        assert_eq!(rs.rows.len(), 2);
        let ids: Vec<String> = rs
            .rows
            .iter()
            .filter_map(|row| match row.0.values().next() {
                Some(Value::Str(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert!(ids.contains(&"n1".to_string()));
        assert!(ids.contains(&"n2".to_string()));
    }

    #[test]
    fn test_labels_function() {
        let mut g = make_graph();
        let rs = exec("MATCH (n {name: 'Alice'}) RETURN labels(n)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        match rs.rows[0].0.values().next() {
            Some(Value::List(l)) => {
                assert_eq!(l.len(), 1);
                assert_eq!(l[0], Value::Str("Person".into()));
            }
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn test_type_function() {
        let mut g = make_graph();
        let rs = exec("MATCH (a)-[r]->(b) WHERE type(r) = 'KNOWS' RETURN type(r)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("KNOWS".into()))
        );
    }

    #[test]
    fn test_keys_function() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person {name: 'Alice'}) RETURN keys(n)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        match rs.rows[0].0.values().next() {
            Some(Value::List(k)) => {
                assert!(k.contains(&Value::Str("name".into())));
                assert!(k.contains(&Value::Str("age".into())));
            }
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn test_properties_function() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person {name: 'Alice'}) RETURN properties(n)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        match rs.rows[0].0.values().next() {
            Some(Value::Map(m)) => {
                assert_eq!(m.get("name"), Some(&Value::Str("Alice".into())));
                assert_eq!(m.get("age"), Some(&Value::Int(30)));
            }
            _ => panic!("expected map"),
        }
    }

    // ========================================================================
    // Functions: string
    // ========================================================================

    #[test]
    fn test_tolower() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person {name: 'Alice'}) RETURN toLower(n.name)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("alice".into()))
        );
    }

    #[test]
    fn test_toupper() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person {name: 'Alice'}) RETURN toUpper(n.name)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("ALICE".into()))
        );
    }

    #[test]
    fn test_substring() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person {name: 'Alice'}) RETURN substring(n.name, 1, 3)",
            &mut g,
        );
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("lic".into()))
        );
    }

    #[test]
    fn test_trim() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN trim('  hello  ')", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("hello".into()))
        );
    }

    #[test]
    fn test_replace() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN replace('hello world', 'world', 'cypher')", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("hello cypher".into()))
        );
    }

    #[test]
    fn test_left() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN left('hello', 3)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("hel".into()))
        );
    }

    #[test]
    fn test_right() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN right('hello', 3)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("llo".into()))
        );
    }

    #[test]
    fn test_split() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN split('a,b,c', ',')", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::List(l)) => {
                assert_eq!(l.len(), 3);
                assert_eq!(l[0], Value::Str("a".into()));
                assert_eq!(l[1], Value::Str("b".into()));
                assert_eq!(l[2], Value::Str("c".into()));
            }
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn test_reverse_string() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN reverse('hello')", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("olleh".into()))
        );
    }

    // ========================================================================
    // Functions: numeric
    // ========================================================================

    #[test]
    fn test_arithmetic_expr() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person {name: 'Alice'}) RETURN n.age + 5", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(35))
        );
    }

    #[test]
    fn test_modulo() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN 10 % 3", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(1))
        );
    }

    #[test]
    fn test_abs() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN abs(-42)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(42))
        );
    }

    #[test]
    fn test_tointeger() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN toInteger('42')", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(42))
        );
    }

    #[test]
    fn test_tofloat() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN toFloat('3.14')", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - 3.14).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_ceil() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN ceil(2.3)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Float(3.0))
        );
    }

    #[test]
    fn test_floor() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN floor(2.9)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Float(2.0))
        );
    }

    #[test]
    fn test_round() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN round(2.5)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Float(3.0))
        );
    }

    #[test]
    fn test_sign() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN sign(-5)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(-1))
        );
    }

    #[test]
    fn test_sqrt() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN sqrt(16)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - 4.0).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    // ========================================================================
    // Functions: list
    // ========================================================================

    #[test]
    fn test_head() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN head([1, 2, 3])", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(1))
        );
    }

    #[test]
    fn test_last() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN last([1, 2, 3])", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(3))
        );
    }

    #[test]
    fn test_tail() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN tail([1, 2, 3])", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::List(l)) => {
                assert_eq!(l.len(), 2);
                assert_eq!(l[0], Value::Int(2));
                assert_eq!(l[1], Value::Int(3));
            }
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn test_reverse_list() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN reverse([1, 2, 3])", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::List(l)) => {
                assert_eq!(l, &vec![Value::Int(3), Value::Int(2), Value::Int(1)]);
            }
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn test_range() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN range(1, 5)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::List(l)) => {
                assert_eq!(l.len(), 5);
                assert_eq!(l[0], Value::Int(1));
                assert_eq!(l[4], Value::Int(5));
            }
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn test_range_with_step() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN range(0, 10, 3)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::List(l)) => {
                assert_eq!(
                    l,
                    &vec![Value::Int(0), Value::Int(3), Value::Int(6), Value::Int(9)]
                );
            }
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn test_size_list() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN size([1, 2, 3])", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(3))
        );
    }

    #[test]
    fn test_size_string() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN size('hello')", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(5))
        );
    }

    // ========================================================================
    // Functions: control flow
    // ========================================================================

    #[test]
    fn test_coalesce() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Company) RETURN coalesce(n.age, 0)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(0))
        );
    }

    #[test]
    fn test_exists() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person {name: 'Alice'}) RETURN exists(n.age)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Bool(true))
        );
    }

    #[test]
    fn test_case_expression() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) RETURN CASE WHEN n.age > 27 THEN 'senior' ELSE 'junior' END AS category",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 2);
        let categories: Vec<String> = rs
            .rows
            .iter()
            .filter_map(|r| match r.0.get("category") {
                Some(Value::Str(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert!(categories.contains(&"senior".to_string()));
        assert!(categories.contains(&"junior".to_string()));
    }

    #[test]
    fn test_case_simple() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) RETURN CASE n.name WHEN 'Alice' THEN 'A' WHEN 'Bob' THEN 'B' END AS initial",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 2);
        let initials: Vec<String> = rs
            .rows
            .iter()
            .filter_map(|r| match r.0.get("initial") {
                Some(Value::Str(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        assert!(initials.contains(&"A".to_string()));
        assert!(initials.contains(&"B".to_string()));
    }

    // ========================================================================
    // Parameters
    // ========================================================================

    #[test]
    fn test_parameters() {
        let mut g = make_graph();
        let mut params = indexmap::IndexMap::new();
        params.insert("name".into(), Value::Str("Alice".into()));
        let rs = exec_with_params(
            "MATCH (n:Person {name: $name}) RETURN n.age",
            params,
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(30))
        );
    }

    // ========================================================================
    // Map / List literals
    // ========================================================================

    #[test]
    fn test_map_literal() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN {a: 1, b: 'hello'} AS m", &mut g);
        match rs.rows[0].0.get("m") {
            Some(Value::Map(m)) => {
                assert_eq!(m.get("a"), Some(&Value::Int(1)));
                assert_eq!(m.get("b"), Some(&Value::Str("hello".into())));
            }
            _ => panic!("expected map"),
        }
    }

    #[test]
    fn test_list_concat() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN [1, 2] + [3, 4]", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::List(l)) => {
                assert_eq!(l.len(), 4);
            }
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn test_string_concat() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN 'hello' + ' ' + 'world'", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("hello world".into()))
        );
    }

    // ========================================================================
    // Lexer edge cases
    // ========================================================================

    #[test]
    fn test_lexer_string_escape() {
        use crate::lexer::{Lexer, Token};
        let mut lex = Lexer::new(r#"'hello\nworld'"#);
        let tok = lex.next_token().unwrap();
        assert_eq!(tok, Token::StrLit("hello\nworld".into()));
    }

    #[test]
    fn test_lexer_backtick_ident() {
        use crate::lexer::{Lexer, Token};
        let mut lex = Lexer::new("`my label`");
        let tok = lex.next_token().unwrap();
        assert_eq!(tok, Token::Ident("my label".into()));
    }

    #[test]
    fn test_lexer_float() {
        use crate::lexer::{Lexer, Token};
        let mut lex = Lexer::new("3.14");
        let tok = lex.next_token().unwrap();
        assert!(matches!(tok, Token::FloatLit(_)));
        if let Token::FloatLit(f) = tok {
            assert!((f - 3.14).abs() < 1e-10);
        }
    }

    #[test]
    fn test_lexer_regex_match() {
        use crate::lexer::{Lexer, Token};
        let mut lex = Lexer::new("=~");
        let tok = lex.next_token().unwrap();
        assert_eq!(tok, Token::RegexMatch);
    }

    #[test]
    fn test_lexer_line_comment() {
        use crate::lexer::{Lexer, Token};
        let mut lex = Lexer::new("// comment\n42");
        let tok = lex.next_token().unwrap();
        assert_eq!(tok, Token::IntLit(42));
    }

    // ========================================================================
    // Error cases
    // ========================================================================

    #[test]
    fn test_parse_error_missing_paren() {
        let result = parse("MATCH (n RETURN n");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_unexpected_token() {
        let result = parse("FOOBAR (n) RETURN n");
        assert!(result.is_err());
    }

    #[test]
    fn test_exec_error_unbound_variable() {
        let mut g = make_graph();
        let q = parse("MATCH (n) RETURN x").unwrap();
        let ex = Executor::new();
        let result = ex.execute(&q, &mut g);
        assert!(result.is_err());
    }

    #[test]
    fn test_exec_error_division_by_zero() {
        let mut g = MemoryGraph::new();
        let q = parse("RETURN 1 / 0").unwrap();
        let ex = Executor::new();
        let result = ex.execute(&q, &mut g);
        assert!(result.is_err());
    }

    #[test]
    fn test_exec_error_invalid_regex() {
        let mut g = make_graph();
        let q = parse(r#"MATCH (n:Person) WHERE n.name =~ "[invalid" RETURN n"#).unwrap();
        let ex = Executor::new();
        let result = ex.execute(&q, &mut g);
        assert!(result.is_err());
    }

    // ========================================================================
    // OCEL integration
    // ========================================================================

    #[test]
    fn test_from_ocel() {
        use yata_ocel::{OcelObject, OcelObjectObjectEdge};
        let objects = vec![
            OcelObject {
                object_id: "o1".into(),
                object_type: "Order".into(),
                attrs: [("amount".into(), serde_json::json!(100))]
                    .into_iter()
                    .collect(),
                state_hash: None,
                valid_from: chrono::Utc::now(),
                valid_to: None,
            },
            OcelObject {
                object_id: "o2".into(),
                object_type: "Item".into(),
                attrs: [("sku".into(), serde_json::json!("ABC"))]
                    .into_iter()
                    .collect(),
                state_hash: None,
                valid_from: chrono::Utc::now(),
                valid_to: None,
            },
        ];
        let edges = vec![OcelObjectObjectEdge {
            src_object_id: "o1".into(),
            dst_object_id: "o2".into(),
            rel_type: "CONTAINS".into(),
            qualifier: None,
            valid_from: chrono::Utc::now(),
            valid_to: None,
        }];

        let mut g = MemoryGraph::from_ocel(&objects, &edges);
        let rs = exec("MATCH (o:Order)-[:CONTAINS]->(i:Item) RETURN o.amount, i.sku", &mut g);
        assert_eq!(rs.rows.len(), 1);
        let row = &rs.rows[0].0;
        assert_eq!(row.get("o.amount"), Some(&Value::Int(100)));
        assert_eq!(row.get("i.sku"), Some(&Value::Str("ABC".into())));
    }

    // ========================================================================
    // Float/Int mixed arithmetic
    // ========================================================================

    #[test]
    fn test_int_float_coercion() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN 3 + 1.5", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - 4.5).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_unary_neg() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN -10 + 3", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(-7))
        );
    }

    // ========================================================================
    // Property access on map
    // ========================================================================

    #[test]
    fn test_map_property_access() {
        let mut g = MemoryGraph::new();
        let rs = exec("WITH {x: 42, y: 'hi'} AS m RETURN m.x", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(42))
        );
    }

    // ========================================================================
    // toString / toInteger on various types
    // ========================================================================

    #[test]
    fn test_tostring_int() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN toString(42)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("42".into()))
        );
    }

    #[test]
    fn test_tointeger_float() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN toInteger(3.7)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(3))
        );
    }

    // ========================================================================
    // length() alias
    // ========================================================================

    #[test]
    fn test_length_function() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN length('hello')", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(5))
        );
    }

    // ========================================================================
    // Empty result set
    // ========================================================================

    #[test]
    fn test_empty_match() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:NonExistent) RETURN n", &mut g);
        assert_eq!(rs.rows.len(), 0);
    }

    #[test]
    fn test_count_empty() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:NonExistent) RETURN count(n)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(0))
        );
    }
}
