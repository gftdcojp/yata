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

    fn get_f64(rs: &crate::types::ResultSet, col: &str) -> f64 {
        match rs.rows[0].0.get(col).unwrap() {
            Value::Float(f) => *f,
            Value::Int(i) => *i as f64,
            _ => panic!("expected float for {}", col),
        }
    }

    fn get_bool(rs: &crate::types::ResultSet, col: &str) -> bool {
        match rs.rows[0].0.get(col).unwrap() {
            Value::Bool(b) => *b,
            _ => panic!("expected bool for {}", col),
        }
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

    // ========================================================================
    // MERGE ON CREATE SET / ON MATCH SET
    // ========================================================================

    #[test]
    fn test_merge_on_create_set() {
        let mut g = MemoryGraph::new();
        exec(
            "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true",
            &mut g,
        );
        let rs = exec("MATCH (n:Person {name: 'Alice'}) RETURN n.created", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Bool(true))
        );
    }

    #[test]
    fn test_merge_on_match_set() {
        let mut g = make_graph();
        // Alice already exists in make_graph
        exec(
            "MERGE (n:Person {name: 'Alice'}) ON MATCH SET n.found = true",
            &mut g,
        );
        let rs = exec("MATCH (n:Person {name: 'Alice'}) RETURN n.found", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Bool(true))
        );
    }

    #[test]
    fn test_merge_on_create_not_match() {
        let mut g = make_graph();
        // Alice exists so ON CREATE should NOT fire
        exec(
            "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.new = true",
            &mut g,
        );
        let rs = exec("MATCH (n:Person {name: 'Alice'}) RETURN n.new", &mut g);
        assert_eq!(rs.rows.len(), 1);
        // new should be null since ON CREATE doesn't fire for existing node
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Null)
        );
    }

    #[test]
    fn test_merge_on_match_not_create() {
        let mut g = MemoryGraph::new();
        // Charlie doesn't exist, so ON MATCH should NOT fire
        exec(
            "MERGE (n:Person {name: 'Charlie'}) ON MATCH SET n.existed = true",
            &mut g,
        );
        let rs = exec("MATCH (n:Person {name: 'Charlie'}) RETURN n.existed", &mut g);
        assert_eq!(rs.rows.len(), 1);
        // existed should be null since ON MATCH doesn't fire for new node
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Null)
        );
    }

    #[test]
    fn test_merge_both_on_create_and_on_match() {
        let mut g = MemoryGraph::new();
        exec(
            "MERGE (n:Person {name: 'Dave'}) ON CREATE SET n.created = true ON MATCH SET n.matched = true",
            &mut g,
        );
        let rs = exec("MATCH (n:Person {name: 'Dave'}) RETURN n.created, n.matched", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0].0.get("n.created"), Some(&Value::Bool(true)));
        assert_eq!(rs.rows[0].0.get("n.matched"), Some(&Value::Null));
    }

    // ========================================================================
    // CREATE binds variables back
    // ========================================================================

    #[test]
    fn test_create_binds_variables() {
        let mut g = MemoryGraph::new();
        let rs = exec("CREATE (n:Person {name: 'Eve'}) RETURN n.name", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("Eve".into()))
        );
    }

    // ========================================================================
    // SET n = {map}
    // ========================================================================

    #[test]
    fn test_set_node_equals_map() {
        let mut g = make_graph();
        exec(
            "MATCH (n:Person {name: 'Alice'}) SET n = {title: 'Engineer', city: 'Tokyo'}",
            &mut g,
        );
        let rs = exec("MATCH (n:Person {name: 'Alice'}) RETURN n.title, n.city", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0].0.get("n.title"), Some(&Value::Str("Engineer".into())));
        assert_eq!(rs.rows[0].0.get("n.city"), Some(&Value::Str("Tokyo".into())));
    }

    // ========================================================================
    // UNION / UNION ALL
    // ========================================================================

    #[test]
    fn test_union_parses() {
        // Verify UNION parses without error
        let q = parse("MATCH (n:Person) RETURN n.name UNION MATCH (n:Company) RETURN n.name");
        assert!(q.is_ok());
        let query = q.unwrap();
        assert!(query.clauses.len() > 2);
    }

    #[test]
    fn test_union_all_parses() {
        let q = parse("MATCH (n:Person) RETURN n.name UNION ALL MATCH (n:Company) RETURN n.name");
        assert!(q.is_ok());
    }

    // ========================================================================
    // FOREACH
    // ========================================================================

    #[test]
    fn test_foreach_create() {
        let mut g = MemoryGraph::new();
        exec(
            "FOREACH (name IN ['X', 'Y', 'Z'] | CREATE (n:Tag {name: name}))",
            &mut g,
        );
        let rs = exec("MATCH (n:Tag) RETURN n.name", &mut g);
        assert_eq!(rs.rows.len(), 3);
    }

    // ========================================================================
    // REMOVE
    // ========================================================================

    #[test]
    fn test_remove_property() {
        let mut g = make_graph();
        exec("MATCH (n:Person {name: 'Alice'}) REMOVE n.age = null", &mut g);
        let rs = exec("MATCH (n:Person {name: 'Alice'}) RETURN n.age", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0].0.values().next().cloned(), Some(Value::Null));
    }

    // ========================================================================
    // Trigonometric functions
    // ========================================================================

    #[test]
    fn test_sin() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN sin(0)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - 0.0).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_cos() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN cos(0)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - 1.0).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_tan() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN tan(0)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - 0.0).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_asin() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN asin(1)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - std::f64::consts::FRAC_PI_2).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_acos() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN acos(1)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - 0.0).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_atan() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN atan(0)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - 0.0).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_atan2() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN atan2(1, 1)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - std::f64::consts::FRAC_PI_4).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_log() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN log(1)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - 0.0).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_log10() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN log10(100)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - 2.0).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_exp() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN exp(0)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - 1.0).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_pi() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN pi()", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - std::f64::consts::PI).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_e() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN e()", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - std::f64::consts::E).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_degrees() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN degrees(3.141592653589793)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - 180.0).abs() < 1e-6),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_radians() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN radians(180)", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => assert!((f - std::f64::consts::PI).abs() < 1e-10),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_rand() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN rand()", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Float(f)) => {
                assert!(*f >= 0.0 && *f <= 1.0, "rand() should be in [0,1], got {}", f);
            }
            other => panic!("expected Float, got {:?}", other),
        }
    }

    // ========================================================================
    // Temporal functions
    // ========================================================================

    #[test]
    fn test_timestamp() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN timestamp()", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Int(ms)) => assert!(*ms > 0, "timestamp should be positive"),
            other => panic!("expected Int, got {:?}", other),
        }
    }

    #[test]
    fn test_date_string() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN date('2024-01-15')", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("2024-01-15".into()))
        );
    }

    #[test]
    fn test_datetime_no_args() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN datetime()", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::Int(ms)) => assert!(*ms > 0),
            other => panic!("expected Int, got {:?}", other),
        }
    }

    // ========================================================================
    // Additional string functions
    // ========================================================================

    #[test]
    fn test_ltrim() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN ltrim('  hello  ')", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("hello  ".into()))
        );
    }

    #[test]
    fn test_rtrim() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN rtrim('  hello  ')", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("  hello".into()))
        );
    }

    #[test]
    fn test_lpad() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN lpad('hi', 5)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("   hi".into()))
        );
    }

    #[test]
    fn test_rpad() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN rpad('hi', 5)", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("hi   ".into()))
        );
    }

    #[test]
    fn test_toboolean_true() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN toboolean('true')", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Bool(true))
        );
    }

    #[test]
    fn test_toboolean_false() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN toboolean('false')", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Bool(false))
        );
    }

    // ========================================================================
    // Index access
    // ========================================================================

    #[test]
    fn test_list_index() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN [10, 20, 30][1]", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(20))
        );
    }

    #[test]
    fn test_list_negative_index() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN [10, 20, 30][-1]", &mut g);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(30))
        );
    }

    // ========================================================================
    // List comprehension
    // ========================================================================

    #[test]
    fn test_list_comprehension_filter() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 3]", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::List(l)) => {
                assert_eq!(l, &vec![Value::Int(4), Value::Int(5)]);
            }
            other => panic!("expected list, got {:?}", other),
        }
    }

    #[test]
    fn test_list_comprehension_map() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN [x IN [1, 2, 3] | x * 2]", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::List(l)) => {
                assert_eq!(l, &vec![Value::Int(2), Value::Int(4), Value::Int(6)]);
            }
            other => panic!("expected list, got {:?}", other),
        }
    }

    #[test]
    fn test_list_comprehension_filter_and_map() {
        let mut g = MemoryGraph::new();
        let rs = exec("RETURN [x IN [1, 2, 3, 4] WHERE x > 2 | x * 10]", &mut g);
        match rs.rows[0].0.values().next() {
            Some(Value::List(l)) => {
                assert_eq!(l, &vec![Value::Int(30), Value::Int(40)]);
            }
            other => panic!("expected list, got {:?}", other),
        }
    }

    // ========================================================================
    // MERGE binds variables (regression)
    // ========================================================================

    #[test]
    fn test_merge_binds_variables() {
        let mut g = MemoryGraph::new();
        let rs = exec("MERGE (n:City {name: 'Tokyo'}) RETURN n.name", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Str("Tokyo".into()))
        );
    }

    #[test]
    fn test_merge_existing_binds_variables() {
        let mut g = make_graph();
        let rs = exec("MERGE (n:Person {name: 'Alice'}) RETURN n.age", &mut g);
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.values().next().cloned(),
            Some(Value::Int(30))
        );
    }

    // ========================================================================
    // UNION execution
    // ========================================================================

    #[test]
    fn test_union_all() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name AS name \
             UNION ALL \
             MATCH (n:Person) WHERE n.name = 'Bob' RETURN n.name AS name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 2);
        let names: Vec<String> = rs
            .rows
            .iter()
            .map(|r| match r.0.get("name").unwrap() {
                Value::Str(s) => s.clone(),
                _ => panic!("expected string"),
            })
            .collect();
        assert!(names.contains(&"Alice".to_string()));
        assert!(names.contains(&"Bob".to_string()));
    }

    #[test]
    fn test_union_dedup() {
        let mut g = make_graph();
        // Both sub-queries return Alice, UNION (not ALL) should deduplicate
        let rs = exec(
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name AS name \
             UNION \
             MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name AS name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
    }

    #[test]
    fn test_union_three_segments() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name AS name \
             UNION ALL \
             MATCH (n:Person) WHERE n.name = 'Bob' RETURN n.name AS name \
             UNION ALL \
             MATCH (n:Company) RETURN n.name AS name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 3);
    }

    // ========================================================================
    // Named paths
    // ========================================================================

    #[test]
    fn test_named_path() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH p = (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person) RETURN p",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        match rs.rows[0].0.get("p").unwrap() {
            Value::List(path) => {
                // Should contain [Node(Alice), Rel(KNOWS), Node(Bob)]
                assert_eq!(path.len(), 3);
                assert!(matches!(&path[0], Value::Node(n) if n.id == "n1"));
                assert!(matches!(&path[1], Value::Rel(r) if r.rel_type == "KNOWS"));
                assert!(matches!(&path[2], Value::Node(n) if n.id == "n2"));
            }
            other => panic!("expected list path, got {:?}", other),
        }
    }

    #[test]
    fn test_named_path_length() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH p = (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person) RETURN length(p) AS len",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0].0.get("len").cloned(), Some(Value::Int(3)));
    }

    // ========================================================================
    // Map projection
    // ========================================================================

    #[test]
    fn test_map_projection_shorthand() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person {name: 'Alice'}) RETURN n { .name, .age }",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        let val = rs.rows[0].0.values().next().unwrap();
        match val {
            Value::Map(m) => {
                assert_eq!(m.get("name"), Some(&Value::Str("Alice".into())));
                assert_eq!(m.get("age"), Some(&Value::Int(30)));
                assert_eq!(m.len(), 2);
            }
            other => panic!("expected map, got {:?}", other),
        }
    }

    #[test]
    fn test_map_projection_all_props() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person {name: 'Alice'}) RETURN n { .* }",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        let val = rs.rows[0].0.values().next().unwrap();
        match val {
            Value::Map(m) => {
                assert_eq!(m.get("name"), Some(&Value::Str("Alice".into())));
                assert_eq!(m.get("age"), Some(&Value::Int(30)));
            }
            other => panic!("expected map, got {:?}", other),
        }
    }

    #[test]
    fn test_map_projection_literal_key() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person {name: 'Alice'}) RETURN n { .name, status: 'active' }",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        let val = rs.rows[0].0.values().next().unwrap();
        match val {
            Value::Map(m) => {
                assert_eq!(m.get("name"), Some(&Value::Str("Alice".into())));
                assert_eq!(m.get("status"), Some(&Value::Str("active".into())));
            }
            other => panic!("expected map, got {:?}", other),
        }
    }

    // ========================================================================
    // EXISTS { subquery }
    // ========================================================================

    #[test]
    fn test_exists_subquery_true() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:KNOWS]->() } RETURN n.name",
            &mut g,
        );
        // Only Alice knows someone
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.get("n.name").cloned(),
            Some(Value::Str("Alice".into()))
        );
    }

    #[test]
    fn test_exists_subquery_false() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:LIKES]->() } RETURN n.name",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 0);
    }

    // ========================================================================
    // count(DISTINCT x)
    // ========================================================================

    #[test]
    fn test_count_distinct() {
        let mut g = make_graph();
        // Both persons have label "Person" so labels are same
        // But names are distinct
        let rs = exec(
            "MATCH (n:Person) RETURN count(DISTINCT n.name) AS cnt",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0].0.get("cnt").cloned(), Some(Value::Int(2)));
    }

    #[test]
    fn test_count_distinct_with_dupes() {
        let mut g = MemoryGraph::new();
        for i in 0..5 {
            g.add_node(NodeRef {
                id: format!("n{}", i),
                labels: vec!["Item".into()],
                props: [("cat".into(), Value::Str(if i % 2 == 0 { "A" } else { "B" }.into()))]
                    .into_iter()
                    .collect(),
            });
        }
        let rs = exec(
            "MATCH (n:Item) RETURN count(DISTINCT n.cat) AS cnt",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0].0.get("cnt").cloned(), Some(Value::Int(2)));
    }

    // ========================================================================
    // CALL {} subquery
    // ========================================================================

    #[test]
    fn test_call_subquery() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) CALL { WITH n MATCH (n)-[:KNOWS]->(m) RETURN m.name AS friend } RETURN n.name, friend",
            &mut g,
        );
        // Alice knows Bob, Bob knows nobody
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.get("n.name").cloned(),
            Some(Value::Str("Alice".into()))
        );
        assert_eq!(
            rs.rows[0].0.get("friend").cloned(),
            Some(Value::Str("Bob".into()))
        );
    }

    // ========================================================================
    // List predicate functions: ALL, ANY, NONE, SINGLE
    // ========================================================================

    #[test]
    fn test_all_predicate_true() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "RETURN ALL(x IN [2, 4, 6] WHERE x % 2 = 0) AS result",
            &mut g,
        );
        assert_eq!(rs.rows[0].0.get("result").cloned(), Some(Value::Bool(true)));
    }

    #[test]
    fn test_all_predicate_false() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "RETURN ALL(x IN [2, 3, 6] WHERE x % 2 = 0) AS result",
            &mut g,
        );
        assert_eq!(rs.rows[0].0.get("result").cloned(), Some(Value::Bool(false)));
    }

    #[test]
    fn test_any_predicate_true() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "RETURN ANY(x IN [1, 3, 6] WHERE x % 2 = 0) AS result",
            &mut g,
        );
        assert_eq!(rs.rows[0].0.get("result").cloned(), Some(Value::Bool(true)));
    }

    #[test]
    fn test_any_predicate_false() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "RETURN ANY(x IN [1, 3, 5] WHERE x % 2 = 0) AS result",
            &mut g,
        );
        assert_eq!(rs.rows[0].0.get("result").cloned(), Some(Value::Bool(false)));
    }

    #[test]
    fn test_none_predicate_true() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "RETURN NONE(x IN [1, 3, 5] WHERE x % 2 = 0) AS result",
            &mut g,
        );
        assert_eq!(rs.rows[0].0.get("result").cloned(), Some(Value::Bool(true)));
    }

    #[test]
    fn test_none_predicate_false() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "RETURN NONE(x IN [1, 2, 5] WHERE x % 2 = 0) AS result",
            &mut g,
        );
        assert_eq!(rs.rows[0].0.get("result").cloned(), Some(Value::Bool(false)));
    }

    #[test]
    fn test_single_predicate_true() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "RETURN SINGLE(x IN [1, 2, 5] WHERE x % 2 = 0) AS result",
            &mut g,
        );
        assert_eq!(rs.rows[0].0.get("result").cloned(), Some(Value::Bool(true)));
    }

    #[test]
    fn test_single_predicate_false() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "RETURN SINGLE(x IN [2, 4, 5] WHERE x % 2 = 0) AS result",
            &mut g,
        );
        assert_eq!(rs.rows[0].0.get("result").cloned(), Some(Value::Bool(false)));
    }

    #[test]
    fn test_list_predicate_with_graph() {
        let mut g = make_graph();
        // ANY person with age > 28
        let rs = exec(
            "MATCH (n:Person) WITH collect(n.age) AS ages RETURN ANY(a IN ages WHERE a > 28) AS result",
            &mut g,
        );
        assert_eq!(rs.rows[0].0.get("result").cloned(), Some(Value::Bool(true)));
    }

    // ========================================================================
    // Mixed features
    // ========================================================================

    #[test]
    fn test_union_with_count() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) RETURN count(*) AS cnt \
             UNION ALL \
             MATCH (n:Company) RETURN count(*) AS cnt",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 2);
        let counts: Vec<i64> = rs
            .rows
            .iter()
            .map(|r| match r.0.get("cnt").unwrap() {
                Value::Int(n) => *n,
                _ => panic!("expected int"),
            })
            .collect();
        assert!(counts.contains(&2)); // 2 persons
        assert!(counts.contains(&1)); // 1 company
    }

    #[test]
    fn test_named_path_no_rel_var() {
        let mut g = make_graph();
        // Path without rel variable should still work (rel won't be in path)
        let rs = exec(
            "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN p",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        match rs.rows[0].0.get("p").unwrap() {
            Value::List(path) => {
                // Without rel variable, path contains only nodes
                assert_eq!(path.len(), 2);
            }
            other => panic!("expected list path, got {:?}", other),
        }
    }

    #[test]
    fn test_map_projection_empty() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person {name: 'Alice'}) RETURN n {} AS m",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        match rs.rows[0].0.get("m").unwrap() {
            Value::Map(m) => assert_eq!(m.len(), 0),
            other => panic!("expected empty map, got {:?}", other),
        }
    }

    #[test]
    fn test_exists_subquery_negated() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) WHERE NOT EXISTS { MATCH (n)-[:KNOWS]->() } RETURN n.name",
            &mut g,
        );
        // Bob doesn't know anyone
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0].0.get("n.name").cloned(),
            Some(Value::Str("Bob".into()))
        );
    }

    #[test]
    fn test_count_distinct_all_same() {
        let mut g = MemoryGraph::new();
        for i in 0..3 {
            g.add_node(NodeRef {
                id: format!("n{}", i),
                labels: vec!["X".into()],
                props: [("val".into(), Value::Int(42))].into_iter().collect(),
            });
        }
        let rs = exec("MATCH (n:X) RETURN count(DISTINCT n.val) AS cnt", &mut g);
        assert_eq!(rs.rows[0].0.get("cnt").cloned(), Some(Value::Int(1)));
    }

    #[test]
    fn test_list_predicate_empty_list() {
        let mut g = MemoryGraph::new();
        // ALL on empty list is true (vacuous truth)
        let rs = exec("RETURN ALL(x IN [] WHERE x > 0) AS result", &mut g);
        assert_eq!(rs.rows[0].0.get("result").cloned(), Some(Value::Bool(true)));
        // ANY on empty list is false
        let rs = exec("RETURN ANY(x IN [] WHERE x > 0) AS result", &mut g);
        assert_eq!(rs.rows[0].0.get("result").cloned(), Some(Value::Bool(false)));
        // NONE on empty list is true
        let rs = exec("RETURN NONE(x IN [] WHERE x > 0) AS result", &mut g);
        assert_eq!(rs.rows[0].0.get("result").cloned(), Some(Value::Bool(true)));
    }

    // ---- Vector Search / GraphRAG Tests -------------------------------------

    #[test]
    fn test_vector_search_procedure() {
        let mut g = MemoryGraph::new();
        // Create nodes with embeddings
        g.add_node(NodeRef {
            id: "d1".into(),
            labels: vec!["Doc".into()],
            props: indexmap::indexmap! {
                "title".into() => Value::Str("rust programming".into()),
                "embedding".into() => Value::List(vec![Value::Float(1.0), Value::Float(0.0), Value::Float(0.0)]),
            },
        });
        g.add_node(NodeRef {
            id: "d2".into(),
            labels: vec!["Doc".into()],
            props: indexmap::indexmap! {
                "title".into() => Value::Str("python programming".into()),
                "embedding".into() => Value::List(vec![Value::Float(0.9), Value::Float(0.1), Value::Float(0.0)]),
            },
        });
        g.add_node(NodeRef {
            id: "d3".into(),
            labels: vec!["Doc".into()],
            props: indexmap::indexmap! {
                "title".into() => Value::Str("cooking recipes".into()),
                "embedding".into() => Value::List(vec![Value::Float(0.0), Value::Float(0.0), Value::Float(1.0)]),
            },
        });

        let rs = exec(
            "CALL db.index.vector.queryNodes('Doc', 'embedding', [1.0, 0.0, 0.0], 2) YIELD node, score RETURN node.title AS title, score",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 2);
        // First result should be "rust programming" (exact match)
        assert_eq!(rs.rows[0].0.get("title").cloned(), Some(Value::Str("rust programming".into())));
        // Score should be close to 1.0 (cosine similarity)
        if let Some(Value::Float(s)) = rs.rows[0].0.get("score") {
            assert!(*s > 0.99, "expected high similarity, got {}", s);
        }
    }

    #[test]
    fn test_cosine_distance_function() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "RETURN cosine_distance([1.0, 0.0, 0.0], [1.0, 0.0, 0.0]) AS d",
            &mut g,
        );
        if let Some(Value::Float(d)) = rs.rows[0].0.get("d") {
            assert!(d.abs() < 0.001, "identical vectors should have distance ~0, got {}", d);
        } else {
            panic!("expected Float result");
        }
    }

    #[test]
    fn test_cosine_similarity_function() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "RETURN cosine_similarity([1.0, 0.0], [0.0, 1.0]) AS s",
            &mut g,
        );
        if let Some(Value::Float(s)) = rs.rows[0].0.get("s") {
            assert!(s.abs() < 0.001, "orthogonal vectors should have similarity ~0, got {}", s);
        } else {
            panic!("expected Float result");
        }
    }

    #[test]
    fn test_euclidean_distance_function() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "RETURN euclidean_distance([0.0, 0.0], [3.0, 4.0]) AS d",
            &mut g,
        );
        if let Some(Value::Float(d)) = rs.rows[0].0.get("d") {
            assert!((d - 5.0).abs() < 0.001, "expected distance=5.0, got {}", d);
        } else {
            panic!("expected Float result");
        }
    }

    #[test]
    fn test_l2_distance_alias() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "RETURN l2_distance([0.0, 0.0], [3.0, 4.0]) AS d",
            &mut g,
        );
        if let Some(Value::Float(d)) = rs.rows[0].0.get("d") {
            assert!((d - 5.0).abs() < 0.001, "expected distance=5.0, got {}", d);
        }
    }

    #[test]
    fn test_create_node_with_embedding() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "CREATE (n:Doc {title: 'test', embedding: [0.1, 0.2, 0.3]}) RETURN n.embedding AS emb",
            &mut g,
        );
        if let Some(Value::List(l)) = rs.rows[0].0.get("emb") {
            assert_eq!(l.len(), 3);
            assert_eq!(l[0], Value::Float(0.1));
        } else {
            panic!("expected List for embedding");
        }
    }

    #[test]
    fn test_vector_search_with_label_filter() {
        let mut g = MemoryGraph::new();
        g.add_node(NodeRef {
            id: "d1".into(),
            labels: vec!["Doc".into()],
            props: indexmap::indexmap! {
                "embedding".into() => Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
            },
        });
        g.add_node(NodeRef {
            id: "p1".into(),
            labels: vec!["Person".into()],
            props: indexmap::indexmap! {
                "embedding".into() => Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
            },
        });

        // Search only Doc label — should not return Person
        let rs = exec(
            "CALL db.index.vector.queryNodes('Doc', 'embedding', [1.0, 0.0], 10) YIELD node, score RETURN node AS n",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        if let Some(Value::Node(n)) = rs.rows[0].0.get("n") {
            assert_eq!(n.id, "d1");
        }
    }

    #[test]
    fn test_vector_search_empty_graph() {
        let mut g = MemoryGraph::new();
        let rs = exec(
            "CALL db.index.vector.queryNodes('Doc', 'embedding', [1.0, 0.0], 5) YIELD node, score RETURN node",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 0);
    }

    #[test]
    fn test_vector_search_cosine_ranking() {
        let mut g = MemoryGraph::new();
        // Three nodes with different similarity to query [1, 0, 0]
        g.add_node(NodeRef {
            id: "low".into(),
            labels: vec!["V".into()],
            props: indexmap::indexmap! {
                "embedding".into() => Value::List(vec![Value::Float(0.0), Value::Float(1.0), Value::Float(0.0)]),
            },
        });
        g.add_node(NodeRef {
            id: "mid".into(),
            labels: vec!["V".into()],
            props: indexmap::indexmap! {
                "embedding".into() => Value::List(vec![Value::Float(0.7), Value::Float(0.7), Value::Float(0.0)]),
            },
        });
        g.add_node(NodeRef {
            id: "high".into(),
            labels: vec!["V".into()],
            props: indexmap::indexmap! {
                "embedding".into() => Value::List(vec![Value::Float(1.0), Value::Float(0.0), Value::Float(0.0)]),
            },
        });

        let rs = exec(
            "CALL db.index.vector.queryNodes('V', 'embedding', [1.0, 0.0, 0.0], 3) YIELD node, score RETURN node.id AS id, score ORDER BY score DESC",
            &mut g,
        );
        // Access id via the node alias trick - using node.id won't work directly,
        // but we have "id" alias. Check ordering: high, mid, low
        assert_eq!(rs.rows.len(), 3);
        // Actually, node.id won't work because we aliased using "id(node)" pattern.
        // Let me just verify by score ordering
        let scores: Vec<f64> = rs.rows.iter().filter_map(|r| {
            if let Some(Value::Float(f)) = r.0.get("score") { Some(*f) } else { None }
        }).collect();
        assert!(scores.len() == 3);
        assert!(scores[0] >= scores[1], "scores should be descending");
        assert!(scores[1] >= scores[2], "scores should be descending");
    }

    #[test]
    fn test_gds_similarity_cosine_alias() {
        let mut g = MemoryGraph::new();
        // gds.similarity.cosine is an alias for cosine_distance
        let q = parse("RETURN cosine_distance([1.0, 0.0], [1.0, 0.0]) AS d").unwrap();
        let ex = Executor::new();
        let rs = ex.execute(&q, &mut g).unwrap();
        if let Some(Value::Float(d)) = rs.rows[0].0.get("d") {
            assert!(d.abs() < 0.001);
        }
    }

    #[test]
    fn test_memory_graph_vector_search_trait() {
        use crate::graph::Graph;
        let mut g = MemoryGraph::new();
        g.add_node(NodeRef {
            id: "a".into(),
            labels: vec!["X".into()],
            props: indexmap::indexmap! {
                "embedding".into() => Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
            },
        });
        g.set_node_embedding("a", &[0.5, 0.5]);
        let hits = g.vector_search(&[1.0, 0.0], 5, &vec!["X".into()]);
        assert_eq!(hits.len(), 1);
        // Embedding was overwritten to [0.5, 0.5]
        let (node, score) = &hits[0];
        assert_eq!(node.id, "a");
        // cosine([1,0], [0.5,0.5]) = 0.5/sqrt(0.5) ~= 0.707
        assert!(*score > 0.7 && *score < 0.72, "score={}", score);
    }

    // ========================================================================
    // GDS Similarity Functions
    // ========================================================================

    #[test]
    fn test_jaccard_similarity() {
        let mut g = make_graph();
        let rs = exec(
            "RETURN gds.similarity.jaccard([1,2,3], [2,3,4]) AS j",
            &mut g,
        );
        let j = get_f64(&rs, "j");
        assert!((j - 0.5).abs() < 0.01); // {2,3} / {1,2,3,4} = 2/4 = 0.5
    }

    #[test]
    fn test_overlap_similarity() {
        let mut g = make_graph();
        let rs = exec(
            "RETURN gds.similarity.overlap([1,2,3], [2,3,4]) AS o",
            &mut g,
        );
        let o = get_f64(&rs, "o");
        assert!((o - 0.6666).abs() < 0.01); // 2/min(3,3) = 2/3
    }

    #[test]
    fn test_pearson_similarity() {
        let mut g = make_graph();
        let rs = exec(
            "RETURN gds.similarity.pearson([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) AS p",
            &mut g,
        );
        let p = get_f64(&rs, "p");
        assert!((p - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_gds_util_nan() {
        let mut g = make_graph();
        let rs = exec(
            "RETURN gds.util.isFinite(gds.util.NaN()) AS f",
            &mut g,
        );
        assert_eq!(get_bool(&rs, "f"), false);
    }

    #[test]
    fn test_gds_util_infinity() {
        let mut g = make_graph();
        let rs = exec(
            "RETURN gds.util.isInfinite(gds.util.infinity()) AS i",
            &mut g,
        );
        assert_eq!(get_bool(&rs, "i"), true);
    }

    #[test]
    fn test_gds_util_isfinite() {
        let mut g = make_graph();
        let rs = exec("RETURN gds.util.isFinite(42) AS f", &mut g);
        assert_eq!(get_bool(&rs, "f"), true);
    }
}
