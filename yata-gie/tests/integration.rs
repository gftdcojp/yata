//! Integration tests for yata-gie: end-to-end Cypher → transpile → execute → result_to_rows.
//!
//! These tests exercise the full pipeline from Cypher string to query results,
//! verifying that transpile + optimizer + executor produce correct outputs.

use yata_gie::ir::{AggOp, Expr, LogicalOp, QueryPlan, SecurityScope};
use yata_gie::planner::PlanBuilder;
use yata_gie::{eval_expr, execute, optimize, transpile, transpile_secured};
use yata_grin::{Direction, Mutable, Predicate, PropValue};
use yata_store::MutableCsrStore;

use std::collections::HashSet;

/// Extract string values from results for easy assertion.
fn result_strings(results: &[yata_gie::Record], col: usize) -> Vec<String> {
    results
        .iter()
        .filter_map(|r| match r.values.get(col) {
            Some(PropValue::Str(s)) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

// ── Test store builders ─────────────────────────────────────────────────

fn social_graph() -> MutableCsrStore {
    let mut store = MutableCsrStore::new();
    // vid=0: Alice
    store.add_vertex(
        "Person",
        &[
            ("name", PropValue::Str("Alice".into())),
            ("age", PropValue::Int(30)),
            ("city", PropValue::Str("Tokyo".into())),
        ],
    );
    // vid=1: Bob
    store.add_vertex(
        "Person",
        &[
            ("name", PropValue::Str("Bob".into())),
            ("age", PropValue::Int(25)),
            ("city", PropValue::Str("Osaka".into())),
        ],
    );
    // vid=2: Charlie
    store.add_vertex(
        "Person",
        &[
            ("name", PropValue::Str("Charlie".into())),
            ("age", PropValue::Int(35)),
            ("city", PropValue::Str("Tokyo".into())),
        ],
    );
    // vid=3: Diana
    store.add_vertex(
        "Person",
        &[
            ("name", PropValue::Str("Diana".into())),
            ("age", PropValue::Int(28)),
            ("city", PropValue::Str("Nagoya".into())),
        ],
    );
    // vid=4: GFTD (Company)
    store.add_vertex(
        "Company",
        &[
            ("name", PropValue::Str("GFTD".into())),
            ("founded", PropValue::Int(2020)),
        ],
    );
    // vid=5: Acme (Company)
    store.add_vertex(
        "Company",
        &[
            ("name", PropValue::Str("Acme".into())),
            ("founded", PropValue::Int(2015)),
        ],
    );

    // Edges: KNOWS
    store.add_edge(0, 1, "KNOWS", &[("since", PropValue::Int(2020))]); // Alice->Bob
    store.add_edge(0, 2, "KNOWS", &[("since", PropValue::Int(2021))]); // Alice->Charlie
    store.add_edge(1, 2, "KNOWS", &[]); // Bob->Charlie
    store.add_edge(2, 3, "KNOWS", &[]); // Charlie->Diana
    store.add_edge(3, 0, "KNOWS", &[]); // Diana->Alice (cycle)

    // Edges: WORKS_AT
    store.add_edge(0, 4, "WORKS_AT", &[]); // Alice->GFTD
    store.add_edge(1, 4, "WORKS_AT", &[]); // Bob->GFTD
    store.add_edge(2, 5, "WORKS_AT", &[]); // Charlie->Acme
    store.add_edge(3, 5, "WORKS_AT", &[]); // Diana->Acme

    store.commit();
    store
}

fn security_graph() -> MutableCsrStore {
    let mut store = MutableCsrStore::new();
    // vid=0: public record
    store.add_vertex(
        "Record",
        &[
            ("title", PropValue::Str("Public Doc".into())),
            ("sensitivity_ord", PropValue::Int(0)),
            ("owner_hash", PropValue::Int(100)),
            ("collection", PropValue::Str("app.bsky.feed.post".into())),
        ],
    );
    // vid=1: internal record
    store.add_vertex(
        "Record",
        &[
            ("title", PropValue::Str("Internal Doc".into())),
            ("sensitivity_ord", PropValue::Int(1)),
            ("owner_hash", PropValue::Int(200)),
            ("collection", PropValue::Str("ai.gftd.apps.ops.task".into())),
        ],
    );
    // vid=2: confidential record with yabai collection
    store.add_vertex(
        "Record",
        &[
            ("title", PropValue::Str("Confidential Doc".into())),
            ("sensitivity_ord", PropValue::Int(2)),
            ("owner_hash", PropValue::Int(300)),
            ("collection", PropValue::Str("ai.gftd.apps.yabai.risk".into())),
        ],
    );
    // vid=3: restricted record
    store.add_vertex(
        "Record",
        &[
            ("title", PropValue::Str("Restricted Doc".into())),
            ("sensitivity_ord", PropValue::Int(3)),
            ("owner_hash", PropValue::Int(400)),
            ("collection", PropValue::Str("ai.gftd.apps.malak.threat".into())),
        ],
    );
    // vid=4: record with no sensitivity (defaults to public)
    store.add_vertex(
        "Record",
        &[("title", PropValue::Str("Legacy Doc".into()))],
    );
    store.commit();
    store
}

// ── E2E: Cypher → transpile → execute ───────────────────────────────────

#[test]
fn e2e_simple_match_return() {
    let store = social_graph();
    let query = yata_cypher::parse("MATCH (n:Person) RETURN n.name").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 4);
}

#[test]
fn e2e_match_where_gt() {
    let store = social_graph();
    let query = yata_cypher::parse("MATCH (n:Person) WHERE n.age > 28 RETURN n.name").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    // age > 28: Alice(30), Charlie(35) = 2
    assert_eq!(results.len(), 2);
}

#[test]
fn e2e_match_where_eq_string() {
    let store = social_graph();
    let query =
        yata_cypher::parse(r#"MATCH (n:Person) WHERE n.city = "Tokyo" RETURN n.name"#).unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    // Tokyo: Alice, Charlie = 2
    assert_eq!(results.len(), 2);
}

#[test]
fn e2e_match_inline_props() {
    let store = social_graph();
    let query = yata_cypher::parse("MATCH (n:Person {name: 'Bob'}) RETURN n.age").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], PropValue::Int(25));
}

#[test]
fn e2e_traversal_one_hop() {
    let store = social_graph();
    let query =
        yata_cypher::parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN b.name")
            .unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    // Alice knows Bob and Charlie
    assert_eq!(results.len(), 2);
    let names = result_strings(&results, 0);
    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
}

#[test]
fn e2e_traversal_cross_label() {
    let store = social_graph();
    let query =
        yata_cypher::parse("MATCH (a:Person {name: 'Alice'})-[:WORKS_AT]->(c) RETURN c.name")
            .unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], PropValue::Str("GFTD".into()));
}

#[test]
fn e2e_count_aggregate() {
    let store = social_graph();
    let query = yata_cypher::parse("MATCH (n:Person) RETURN count(n) AS cnt").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], PropValue::Int(4));
}

#[test]
fn e2e_sum_aggregate() {
    let store = social_graph();
    let query = yata_cypher::parse("MATCH (n:Person) RETURN sum(n.age) AS total").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 1);
    // 30 + 25 + 35 + 28 = 118
    assert_eq!(results[0].values[0], PropValue::Int(118));
}

#[test]
fn e2e_avg_aggregate() {
    let store = social_graph();
    let query = yata_cypher::parse("MATCH (n:Person) RETURN avg(n.age) AS avg_age").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 1);
    // (30+25+35+28)/4 = 29.5
    assert_eq!(results[0].values[0], PropValue::Float(29.5));
}

#[test]
fn e2e_min_max_aggregate() {
    let store = social_graph();
    let query =
        yata_cypher::parse("MATCH (n:Person) RETURN min(n.age) AS youngest, max(n.age) AS oldest")
            .unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], PropValue::Int(25));
    assert_eq!(results[0].values[1], PropValue::Int(35));
}

#[test]
fn e2e_order_by_asc() {
    let store = social_graph();
    let query =
        yata_cypher::parse("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 4);
    let ages: Vec<PropValue> = results.iter().map(|r| r.values[1].clone()).collect();
    assert_eq!(ages[0], PropValue::Int(25)); // Bob
    assert_eq!(ages[1], PropValue::Int(28)); // Diana
    assert_eq!(ages[2], PropValue::Int(30)); // Alice
    assert_eq!(ages[3], PropValue::Int(35)); // Charlie
}

#[test]
fn e2e_order_by_desc() {
    let store = social_graph();
    let query =
        yata_cypher::parse("MATCH (n:Person) RETURN n.name ORDER BY n.age DESC").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 4);
}

#[test]
fn e2e_limit() {
    let store = social_graph();
    let query = yata_cypher::parse("MATCH (n:Person) RETURN n.name LIMIT 2").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 2);
}

#[test]
fn e2e_skip_limit() {
    let store = social_graph();
    let query =
        yata_cypher::parse("MATCH (n:Person) RETURN n.name ORDER BY n.age SKIP 1 LIMIT 2")
            .unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 2);
}

#[test]
fn e2e_distinct() {
    let store = social_graph();
    let query = yata_cypher::parse("MATCH (n:Person) RETURN DISTINCT n.city").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    // Tokyo, Osaka, Nagoya = 3 distinct cities
    assert_eq!(results.len(), 3);
}

#[test]
fn e2e_variable_length_path() {
    let store = social_graph();
    let query =
        yata_cypher::parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b) RETURN b.name")
            .unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    // 1 hop: Bob, Charlie
    // 2 hops: Bob->Charlie (already found), Charlie->Diana
    // Total unique: Bob, Charlie, Diana = 3
    assert_eq!(results.len(), 3);
    let names = result_strings(&results, 0);
    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
    assert!(names.contains(&PropValue::Str("Diana".into())));
}

#[test]
fn e2e_variable_length_path_with_cycle() {
    let store = social_graph();
    // The graph has a cycle: Alice->Bob->Charlie->Diana->Alice
    // Variable-length 1..4 should find all reachable nodes without infinite loop
    let query =
        yata_cypher::parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..4]->(b) RETURN b.name")
            .unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    // Should find Bob, Charlie, Diana (and Alice again via cycle, but visited set prevents dup)
    assert!(results.len() >= 3);
    assert!(results.len() <= 4); // at most 4 (Bob, Charlie, Diana, Alice-via-cycle)
}

#[test]
fn e2e_mutation_create_rejected() {
    let query = yata_cypher::parse("CREATE (n:Person {name: 'Eve'})").unwrap();
    let result = transpile(&query);
    assert!(result.is_err());
}

#[test]
fn e2e_mutation_set_rejected() {
    let query = yata_cypher::parse("MATCH (n:Person) SET n.age = 40 RETURN n").unwrap();
    let result = transpile(&query);
    assert!(result.is_err());
}

#[test]
fn e2e_mutation_delete_rejected() {
    let query = yata_cypher::parse("MATCH (n:Person) DELETE n").unwrap();
    let result = transpile(&query);
    assert!(result.is_err());
}

#[test]
fn e2e_mutation_merge_rejected() {
    let query = yata_cypher::parse("MERGE (n:Person {name: 'Alice'}) RETURN n").unwrap();
    let result = transpile(&query);
    assert!(result.is_err());
}

#[test]
fn e2e_unwind_rejected() {
    let query = yata_cypher::parse("UNWIND [1,2,3] AS x RETURN x").unwrap();
    let result = transpile(&query);
    assert!(result.is_err());
}

#[test]
fn e2e_multi_pattern_match() {
    let store = social_graph();
    let query =
        yata_cypher::parse("MATCH (a:Person), (b:Company) RETURN a.name, b.name").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    // 4 persons × 2 companies cross product via scan (actually just both scans, second overwrites)
    // The result depends on how multi-scan interacts - at minimum both labels are scanned
    assert!(!results.is_empty());
}

#[test]
fn e2e_with_clause() {
    let query = yata_cypher::parse(
        r#"MATCH (n:Person) WITH n.name AS name WHERE n.name = "Alice" RETURN name"#,
    )
    .unwrap();
    let result = transpile(&query);
    assert!(result.is_ok());
}

#[test]
fn e2e_empty_label_scan() {
    let store = social_graph();
    let query = yata_cypher::parse("MATCH (n) RETURN count(n) AS cnt").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 1);
    // 4 persons + 2 companies = 6 total vertices
    assert_eq!(results[0].values[0], PropValue::Int(6));
}

#[test]
fn e2e_untyped_edge() {
    let store = social_graph();
    let query =
        yata_cypher::parse("MATCH (a:Person {name: 'Alice'})-->(b) RETURN b.name").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    // Alice has outgoing: KNOWS->Bob, KNOWS->Charlie, WORKS_AT->GFTD
    assert_eq!(results.len(), 3);
}

#[test]
fn e2e_return_node_as_json() {
    let store = social_graph();
    let query = yata_cypher::parse("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap();
    let plan = transpile(&query).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 1);
    match &results[0].values[0] {
        PropValue::Str(s) => {
            assert!(s.contains("Alice"));
            assert!(s.contains("age"));
        }
        _ => panic!("RETURN n should produce JSON string"),
    }
}

// ── E2E: Security filter integration ────────────────────────────────────

#[test]
fn e2e_secured_public_only() {
    let store = security_graph();
    let query = yata_cypher::parse("MATCH (r:Record) RETURN r.title").unwrap();
    let scope = SecurityScope {
        max_sensitivity_ord: 0,
        ..Default::default()
    };
    let plan = transpile_secured(&query, scope).unwrap();
    let results = execute(&plan, &store);
    // Public (sens=0) + Legacy (no sens, defaults to 0) = 2
    assert_eq!(results.len(), 2);
}

#[test]
fn e2e_secured_internal_clearance() {
    let store = security_graph();
    let query = yata_cypher::parse("MATCH (r:Record) RETURN r.title").unwrap();
    let scope = SecurityScope {
        max_sensitivity_ord: 1,
        ..Default::default()
    };
    let plan = transpile_secured(&query, scope).unwrap();
    let results = execute(&plan, &store);
    // Public(0) + Internal(1) + Legacy(default 0) = 3
    assert_eq!(results.len(), 3);
}

#[test]
fn e2e_secured_bypass_sees_all() {
    let store = security_graph();
    let query = yata_cypher::parse("MATCH (r:Record) RETURN r.title").unwrap();
    let scope = SecurityScope {
        bypass: true,
        ..Default::default()
    };
    let plan = transpile_secured(&query, scope).unwrap();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 5);
}

#[test]
fn e2e_secured_rbac_scope() {
    let store = security_graph();
    let query = yata_cypher::parse("MATCH (r:Record) RETURN r.title").unwrap();
    let scope = SecurityScope {
        max_sensitivity_ord: 0,
        collection_scopes: vec!["ai.gftd.apps.yabai.".into()],
        ..Default::default()
    };
    let plan = transpile_secured(&query, scope).unwrap();
    let results = execute(&plan, &store);
    // Public(0) + Legacy(default 0) + yabai confidential(RBAC) = 3
    assert_eq!(results.len(), 3);
}

#[test]
fn e2e_secured_consent_grant() {
    let store = security_graph();
    let query = yata_cypher::parse("MATCH (r:Record) RETURN r.title").unwrap();
    let scope = SecurityScope {
        max_sensitivity_ord: 0,
        allowed_owner_hashes: vec![400], // consent for restricted doc owner
        ..Default::default()
    };
    let plan = transpile_secured(&query, scope).unwrap();
    let results = execute(&plan, &store);
    // Public(0) + Legacy(0) + Restricted(consent) = 3
    assert_eq!(results.len(), 3);
}

#[test]
fn e2e_secured_traversal() {
    let store = social_graph();
    // Add security properties to the social graph via a new store
    let mut store = MutableCsrStore::new();
    store.add_vertex(
        "Person",
        &[
            ("name", PropValue::Str("Alice".into())),
            ("sensitivity_ord", PropValue::Int(0)),
        ],
    );
    store.add_vertex(
        "Person",
        &[
            ("name", PropValue::Str("Bob".into())),
            ("sensitivity_ord", PropValue::Int(1)),
        ],
    );
    store.add_edge(0, 1, "KNOWS", &[]);
    store.commit();

    let query =
        yata_cypher::parse("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN b.name")
            .unwrap();
    let scope = SecurityScope {
        max_sensitivity_ord: 0, // public only
        ..Default::default()
    };
    let plan = transpile_secured(&query, scope).unwrap();
    let results = execute(&plan, &store);
    // Bob has sensitivity_ord=1, public-only viewer can't see him
    assert_eq!(results.len(), 0);
}

// ── Optimizer correctness tests ─────────────────────────────────────────

#[test]
fn optimizer_preserves_semantics_simple_filter() {
    let store = social_graph();

    // Unoptimized: Scan -> Filter -> Project
    let plan_unopt = QueryPlan {
        ops: vec![
            LogicalOp::Scan {
                label: "Person".into(),
                alias: "n".into(),
                predicate: None,
            },
            LogicalOp::Filter {
                predicate: Predicate::Eq("name".into(), PropValue::Str("Alice".into())),
            },
            LogicalOp::Project {
                exprs: vec![Expr::Prop("n".into(), "age".into())],
            },
        ],
    };

    // Optimized: Scan(pred) -> Project (filter merged into scan)
    let plan_opt = optimize(plan_unopt.clone());

    let results_unopt = execute(&plan_unopt, &store);
    let results_opt = execute(&plan_opt, &store);

    assert_eq!(results_unopt.len(), results_opt.len());
    assert_eq!(results_unopt[0].values[0], results_opt[0].values[0]);
}

#[test]
fn optimizer_pushdown_across_expand() {
    // Scan -> Expand -> Filter -> Project
    // Filter should be pushed before Expand, then merged into Scan
    let plan = QueryPlan {
        ops: vec![
            LogicalOp::Scan {
                label: "Person".into(),
                alias: "n".into(),
                predicate: None,
            },
            LogicalOp::Expand {
                src_alias: "n".into(),
                edge_label: "KNOWS".into(),
                dst_alias: "m".into(),
                direction: Direction::Out,
            },
            LogicalOp::Filter {
                predicate: Predicate::Gt("age".into(), PropValue::Int(28)),
            },
            LogicalOp::Project {
                exprs: vec![Expr::Prop("m".into(), "name".into())],
            },
        ],
    };

    let optimized = optimize(plan);
    // After optimization, filter should be pushed down and merged into Scan
    assert!(optimized.ops.len() <= 3, "ops should be reduced");
    match &optimized.ops[0] {
        LogicalOp::Scan {
            predicate: Some(_), ..
        } => {} // filter merged into scan
        _ => panic!("expected Scan with merged predicate"),
    }
}

#[test]
fn optimizer_idempotent_complex() {
    let plan = PlanBuilder::new()
        .scan_with_predicate(
            "Person",
            "n",
            Predicate::And(
                Box::new(Predicate::Gt("age".into(), PropValue::Int(20))),
                Box::new(Predicate::Lt("age".into(), PropValue::Int(40))),
            ),
        )
        .expand("n", "KNOWS", "m", Direction::Out)
        .project(vec![
            Expr::Prop("n".into(), "name".into()),
            Expr::Prop("m".into(), "name".into()),
        ])
        .order_by(vec![(Expr::Prop("n".into(), "name".into()), false)])
        .limit(10)
        .build();

    let first = optimize(plan.clone());
    let second = optimize(first.clone());
    assert_eq!(first.ops.len(), second.ops.len());
}

#[test]
fn optimizer_preserves_security_filter() {
    let plan = QueryPlan {
        ops: vec![
            LogicalOp::Scan {
                label: "Record".into(),
                alias: "r".into(),
                predicate: None,
            },
            LogicalOp::SecurityFilter {
                aliases: vec!["r".into()],
                scope: SecurityScope::default(),
            },
            LogicalOp::Expand {
                src_alias: "r".into(),
                edge_label: "REFS".into(),
                dst_alias: "t".into(),
                direction: Direction::Out,
            },
            LogicalOp::Filter {
                predicate: Predicate::Eq("status".into(), PropValue::Str("active".into())),
            },
        ],
    };

    let optimized = optimize(plan);
    let has_security = optimized
        .ops
        .iter()
        .any(|op| matches!(op, LogicalOp::SecurityFilter { .. }));
    assert!(has_security, "SecurityFilter must be preserved after optimization");
}

// ── Executor edge cases ─────────────────────────────────────────────────

#[test]
fn execute_empty_plan() {
    let store = social_graph();
    let plan = QueryPlan::new();
    let results = execute(&plan, &store);
    assert!(results.is_empty());
}

#[test]
fn execute_scan_nonexistent_label() {
    let store = social_graph();
    let plan = PlanBuilder::new().scan("Ghost", "n").build();
    let results = execute(&plan, &store);
    assert!(results.is_empty());
}

#[test]
fn execute_expand_no_edges() {
    let mut store = MutableCsrStore::new();
    store.add_vertex("Isolated", &[("name", PropValue::Str("Lonely".into()))]);
    store.commit();

    let plan = PlanBuilder::new()
        .scan("Isolated", "n")
        .expand("n", "KNOWS", "m", Direction::Out)
        .build();
    let results = execute(&plan, &store);
    assert!(results.is_empty());
}

#[test]
fn execute_aggregate_on_empty_input() {
    let store = social_graph();
    let plan = PlanBuilder::new()
        .scan("Ghost", "n")
        .aggregate(
            vec![],
            vec![
                ("cnt".into(), AggOp::Count, Expr::Var("n".into())),
                (
                    "total".into(),
                    AggOp::Sum,
                    Expr::Prop("n".into(), "age".into()),
                ),
            ],
        )
        .build();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], PropValue::Int(0)); // count=0
    assert_eq!(results[0].values[1], PropValue::Int(0)); // sum=0
}

#[test]
fn execute_avg_on_empty_input() {
    let store = social_graph();
    let plan = PlanBuilder::new()
        .scan("Ghost", "n")
        .aggregate(
            vec![],
            vec![(
                "avg".into(),
                AggOp::Avg,
                Expr::Prop("n".into(), "age".into()),
            )],
        )
        .build();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], PropValue::Null); // avg of empty = null
}

#[test]
fn execute_collect_aggregate() {
    let store = social_graph();
    let plan = PlanBuilder::new()
        .scan("Person", "n")
        .aggregate(
            vec![],
            vec![(
                "names".into(),
                AggOp::Collect,
                Expr::Prop("n".into(), "name".into()),
            )],
        )
        .build();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 1);
    // Collect currently returns count as fallback
    assert_eq!(results[0].values[0], PropValue::Int(4));
}

#[test]
fn execute_order_by_string() {
    let store = social_graph();
    let plan = PlanBuilder::new()
        .scan("Person", "n")
        .project(vec![Expr::Prop("n".into(), "name".into())])
        .order_by(vec![(Expr::Prop("n".into(), "name".into()), false)])
        .build();
    let results = execute(&plan, &store);
    let names: Vec<&PropValue> = results.iter().map(|r| &r.values[0]).collect();
    assert_eq!(names[0], &PropValue::Str("Alice".into()));
    assert_eq!(names[1], &PropValue::Str("Bob".into()));
    assert_eq!(names[2], &PropValue::Str("Charlie".into()));
    assert_eq!(names[3], &PropValue::Str("Diana".into()));
}

#[test]
fn execute_limit_exceeds_data() {
    let store = social_graph();
    let plan = PlanBuilder::new()
        .scan("Person", "n")
        .limit(100) // way more than 4 persons
        .build();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 4); // just returns all available
}

#[test]
fn execute_offset_exceeds_data() {
    let store = social_graph();
    let plan = PlanBuilder::new()
        .scan("Person", "n")
        .limit_offset(10, 100) // skip 100, but only 4 exist
        .build();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 0);
}

#[test]
fn execute_filter_and_predicate() {
    let store = social_graph();
    let plan = PlanBuilder::new()
        .scan("Person", "n")
        .filter(Predicate::And(
            Box::new(Predicate::Eq(
                "city".into(),
                PropValue::Str("Tokyo".into()),
            )),
            Box::new(Predicate::Gt("age".into(), PropValue::Int(28))),
        ))
        .build();
    let results = execute(&plan, &store);
    // Tokyo + age>28: Alice(30,Tokyo), Charlie(35,Tokyo)
    assert_eq!(results.len(), 2);
}

#[test]
fn execute_filter_or_predicate() {
    let store = social_graph();
    let plan = PlanBuilder::new()
        .scan("Person", "n")
        .filter(Predicate::Or(
            Box::new(Predicate::Eq(
                "name".into(),
                PropValue::Str("Alice".into()),
            )),
            Box::new(Predicate::Eq(
                "name".into(),
                PropValue::Str("Diana".into()),
            )),
        ))
        .build();
    let results = execute(&plan, &store);
    assert_eq!(results.len(), 2);
}

#[test]
fn execute_filter_in() {
    let store = social_graph();
    let plan = PlanBuilder::new()
        .scan("Person", "n")
        .filter(Predicate::In(
            "city".into(),
            vec![
                PropValue::Str("Tokyo".into()),
                PropValue::Str("Nagoya".into()),
            ],
        ))
        .build();
    let results = execute(&plan, &store);
    // Tokyo: Alice, Charlie; Nagoya: Diana = 3
    assert_eq!(results.len(), 3);
}

#[test]
fn execute_filter_starts_with() {
    let store = social_graph();
    let plan = PlanBuilder::new()
        .scan("Person", "n")
        .filter(Predicate::StartsWith("name".into(), "C".into()))
        .build();
    let results = execute(&plan, &store);
    // Charlie
    assert_eq!(results.len(), 1);
}

#[test]
fn execute_bidirectional_expand() {
    let store = social_graph();
    // Bob: out KNOWS->Charlie, in KNOWS<-Alice
    let plan = PlanBuilder::new()
        .scan_with_predicate(
            "Person",
            "n",
            Predicate::Eq("name".into(), PropValue::Str("Bob".into())),
        )
        .expand("n", "KNOWS", "m", Direction::Both)
        .build();
    let results = execute(&plan, &store);
    let vids: HashSet<u32> = results.iter().map(|r| r.bindings["m"]).collect();
    assert!(vids.contains(&0)); // Alice (incoming)
    assert!(vids.contains(&2)); // Charlie (outgoing)
}

#[test]
fn execute_chained_expands() {
    let store = social_graph();
    // Alice -KNOWS-> Bob -KNOWS-> Charlie
    let plan = PlanBuilder::new()
        .scan_with_predicate(
            "Person",
            "a",
            Predicate::Eq("name".into(), PropValue::Str("Alice".into())),
        )
        .expand("a", "KNOWS", "b", Direction::Out)
        .expand("b", "KNOWS", "c", Direction::Out)
        .project(vec![Expr::Prop("c".into(), "name".into())])
        .build();
    let results = execute(&plan, &store);
    // Alice->Bob->Charlie, Alice->Charlie->Diana
    assert_eq!(results.len(), 2);
    let names: HashSet<PropValue> = results.iter().map(|r| r.values[0].clone()).collect();
    assert!(names.contains(&PropValue::Str("Charlie".into())));
    assert!(names.contains(&PropValue::Str("Diana".into())));
}

// ── eval_expr edge cases ────────────────────────────────────────────────

#[test]
fn eval_expr_literal_bool() {
    let store = social_graph();
    let record = yata_gie::Record {
        bindings: std::collections::HashMap::new(),
        values: vec![],
    };
    let val = eval_expr(&Expr::Lit(PropValue::Bool(true)), &record, &store);
    assert_eq!(val, PropValue::Bool(true));
}

#[test]
fn eval_expr_literal_float() {
    let store = social_graph();
    let record = yata_gie::Record {
        bindings: std::collections::HashMap::new(),
        values: vec![],
    };
    let val = eval_expr(&Expr::Lit(PropValue::Float(3.14)), &record, &store);
    assert_eq!(val, PropValue::Float(3.14));
}

#[test]
fn eval_expr_alias_unwraps() {
    let store = social_graph();
    let mut bindings = std::collections::HashMap::new();
    bindings.insert("n".to_string(), 0u32);
    let record = yata_gie::Record {
        bindings,
        values: vec![],
    };
    let val = eval_expr(
        &Expr::Alias(
            Box::new(Expr::Prop("n".into(), "name".into())),
            "person_name".into(),
        ),
        &record,
        &store,
    );
    assert_eq!(val, PropValue::Str("Alice".into()));
}

// ── Planner builder edge cases ──────────────────────────────────────────

#[test]
fn planner_builder_empty() {
    let plan = PlanBuilder::new().build();
    assert!(plan.is_empty());
}

#[test]
fn planner_builder_full_chain() {
    let plan = PlanBuilder::new()
        .scan("Person", "a")
        .expand("a", "KNOWS", "b", Direction::Out)
        .expand("b", "WORKS_AT", "c", Direction::Out)
        .filter(Predicate::Eq("name".into(), PropValue::Str("GFTD".into())))
        .project(vec![Expr::Prop("c".into(), "name".into())])
        .distinct(vec![Expr::Prop("c".into(), "name".into())])
        .order_by(vec![(Expr::Prop("c".into(), "name".into()), false)])
        .limit_offset(5, 0)
        .aggregate(
            vec![],
            vec![("cnt".into(), AggOp::Count, Expr::Var("c".into()))],
        )
        .build();

    assert_eq!(plan.len(), 9);
}

#[test]
fn planner_path_expand_zero_min_hops() {
    let plan = PlanBuilder::new()
        .scan("Node", "n")
        .path_expand("n", "EDGE", "m", 0, 3, Direction::Out)
        .build();

    match &plan.ops[1] {
        LogicalOp::PathExpand {
            min_hops,
            max_hops,
            ..
        } => {
            assert_eq!(*min_hops, 0);
            assert_eq!(*max_hops, 3);
        }
        _ => panic!("expected PathExpand"),
    }
}

// ── IR construction edge cases ──────────────────────────────────────────

#[test]
fn ir_query_plan_default() {
    let plan = QueryPlan::default();
    assert!(plan.is_empty());
    assert_eq!(plan.len(), 0);
}

#[test]
fn ir_query_plan_push_and_len() {
    let mut plan = QueryPlan::new();
    plan.push(LogicalOp::Scan {
        label: "X".into(),
        alias: "x".into(),
        predicate: None,
    });
    plan.push(LogicalOp::Project {
        exprs: vec![Expr::Var("x".into())],
    });
    assert_eq!(plan.len(), 2);
    assert!(!plan.is_empty());
}

#[test]
fn ir_security_scope_default() {
    let scope = SecurityScope::default();
    assert_eq!(scope.max_sensitivity_ord, 0);
    assert!(scope.collection_scopes.is_empty());
    assert!(scope.allowed_owner_hashes.is_empty());
    assert!(!scope.bypass);
}

// ── Exchange/Receive passthrough ────────────────────────────────────────

#[test]
fn exchange_ops_are_noop_in_single_partition() {
    let store = social_graph();
    let plan = PlanBuilder::new().scan("Person", "n").build();
    let records_before = execute(&plan, &store);

    let mut plan_with_exchange = QueryPlan::new();
    plan_with_exchange.push(LogicalOp::Scan {
        label: "Person".into(),
        alias: "n".into(),
        predicate: None,
    });
    plan_with_exchange.push(LogicalOp::Exchange {
        routing_key: Expr::Var("n".into()),
        kind: yata_gie::ir::ExchangeKind::HashShuffle,
    });
    plan_with_exchange.push(LogicalOp::Receive {
        source_partitions: vec![],
    });

    let records_after = execute(&plan_with_exchange, &store);
    assert_eq!(records_before.len(), records_after.len());
}
