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

    fn exec(cypher: &str, g: &mut MemoryGraph) -> crate::types::ResultSet {
        let q = parse(cypher).expect("parse failed");
        let ex = Executor::new();
        ex.execute(&q, g).expect("execute failed")
    }

    #[test]
    fn test_match_all_nodes() {
        let mut g = make_graph();
        let rs = exec("MATCH (n) RETURN n", &mut g);
        // Should return all 3 nodes
        assert_eq!(rs.rows.len(), 3);
        assert!(rs.columns.contains(&"n".to_string()));
        // All returned values should be nodes
        for row in &rs.rows {
            assert!(matches!(row.0.get("n"), Some(Value::Node(_))));
        }
    }

    #[test]
    fn test_match_by_label() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person) RETURN n.name", &mut g);
        assert_eq!(rs.rows.len(), 2);
        // Collect names
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
    fn test_where_filter() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person) WHERE n.age > 26 RETURN n.name", &mut g);
        assert_eq!(rs.rows.len(), 1);
        let name = rs.rows[0].0.values().next().cloned();
        assert_eq!(name, Some(Value::Str("Alice".into())));
    }

    #[test]
    fn test_count_aggregation() {
        let mut g = make_graph();
        let rs = exec("MATCH (n:Person) RETURN count(n)", &mut g);
        assert_eq!(rs.rows.len(), 1);
        let count = rs.rows[0].0.values().next().cloned();
        assert_eq!(count, Some(Value::Int(2)));
    }

    #[test]
    fn test_create_node() {
        let mut g = make_graph();
        exec(r#"CREATE (n:Person {name: "Charlie"})"#, &mut g);
        // Now query for Charlie
        let rs = exec(r#"MATCH (n:Person {name: "Charlie"}) RETURN n.name"#, &mut g);
        assert_eq!(rs.rows.len(), 1);
        let name = rs.rows[0].0.values().next().cloned();
        assert_eq!(name, Some(Value::Str("Charlie".into())));
    }

    #[test]
    fn test_order_limit() {
        let mut g = make_graph();
        let rs = exec(
            "MATCH (n:Person) RETURN n.name ORDER BY n.name ASC LIMIT 1",
            &mut g,
        );
        assert_eq!(rs.rows.len(), 1);
        let name = rs.rows[0].0.values().next().cloned();
        // Alice < Bob alphabetically
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
    fn test_match_undirected_rel() {
        let mut g = make_graph();
        // Undirected match should find KNOWS relationship from either end
        let rs = exec("MATCH (a)-[:KNOWS]-(b) RETURN a.name, b.name", &mut g);
        // Should find 2 rows: (Alice, Bob) and (Bob, Alice)
        assert_eq!(rs.rows.len(), 2);
    }

    #[test]
    fn test_match_all_rels() {
        let mut g = make_graph();
        let rs = exec("MATCH (a)-[r]->(b) RETURN r", &mut g);
        // n1->n2 (KNOWS) and n1->n3 (WORKS_AT)
        assert_eq!(rs.rows.len(), 2);
    }

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
    fn test_detach_delete() {
        let mut g = make_graph();
        exec("MATCH (n:Company) DETACH DELETE n", &mut g);
        let rs = exec("MATCH (n:Company) RETURN n", &mut g);
        assert_eq!(rs.rows.len(), 0);
        // WORKS_AT rel should also be gone
        let rs2 = exec("MATCH (a)-[r:WORKS_AT]->(b) RETURN r", &mut g);
        assert_eq!(rs2.rows.len(), 0);
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
    fn test_is_null() {
        let mut g = make_graph();
        // n3 (Company) has no 'age' property
        let rs = exec("MATCH (n:Company) RETURN n.age IS NULL", &mut g);
        assert_eq!(rs.rows.len(), 1);
        // n.age is null, IS NULL should be true — but we return the IS NULL expr itself
        // Actually parser returns Expr::IsNull, so eval returns Bool
        // The return item name is "value" since it's not a simple var/prop
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
}
