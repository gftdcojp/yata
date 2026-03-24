//! Integration tests for yata-flight SQL executor.

use arrow::array::{Float64Array, Int64Array, StringArray};
use std::sync::{Arc, Mutex};

use yata_grin::{Mutable, PropValue};
use yata_store::MutableCsrStore;

use crate::executor;
use crate::sql_plan;

fn test_store() -> MutableCsrStore {
    let mut store = MutableCsrStore::new();
    store.register_btree_index("Person", "age");
    store.register_btree_index("Person", "name");

    store.add_vertex("Person", &[
        ("name", PropValue::Str("Alice".into())),
        ("age", PropValue::Int(30)),
    ]);
    store.add_vertex("Person", &[
        ("name", PropValue::Str("Bob".into())),
        ("age", PropValue::Int(25)),
    ]);
    store.add_vertex("Person", &[
        ("name", PropValue::Str("Carol".into())),
        ("age", PropValue::Int(35)),
    ]);
    store.add_vertex("Person", &[
        ("name", PropValue::Str("Dave".into())),
        ("age", PropValue::Int(28)),
    ]);

    store.add_edge(0, 1, "KNOWS", &[("weight", PropValue::Float(0.8))]);
    store.add_edge(1, 2, "KNOWS", &[("weight", PropValue::Float(0.9))]);

    store.commit();
    store
}

#[test]
fn select_star() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT * FROM Person").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert_eq!(batch.num_rows(), 4);
    assert!(batch.num_columns() >= 2); // at least name and age
}

#[test]
fn select_specific_columns() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT name FROM Person").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(batch.num_columns(), 1);

    let names = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(names.value(0), "Alice");
    assert_eq!(names.value(1), "Bob");
}

#[test]
fn select_where_eq() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT name, age FROM Person WHERE age = 30").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let names = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(names.value(0), "Alice");
}

#[test]
fn select_where_range() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT name FROM Person WHERE age >= 28 AND age <= 35").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    // Alice(30), Carol(35), Dave(28)
    assert_eq!(batch.num_rows(), 3);
}

#[test]
fn select_order_by_limit() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT name, age FROM Person ORDER BY age LIMIT 2").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert_eq!(batch.num_rows(), 2);

    let ages = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // Youngest 2: Bob(25), Dave(28)
    assert_eq!(ages.value(0), 25);
    assert_eq!(ages.value(1), 28);
}

#[test]
fn select_order_by_desc() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT name FROM Person ORDER BY age DESC LIMIT 1").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let names = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(names.value(0), "Carol"); // oldest
}

#[test]
fn select_edge_table() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT * FROM KNOWS").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert_eq!(batch.num_rows(), 2);
    // Should include _src, _dst, weight
    assert!(batch.num_columns() >= 3);
}

#[test]
fn select_table_not_found() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT * FROM NonExistent").unwrap();
    let result = executor::execute(&store, &plan);
    assert!(result.is_err());
}

#[test]
fn select_offset() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT name FROM Person LIMIT 2 OFFSET 1").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn select_empty_result_no_rows() {
    let mut store = MutableCsrStore::new();
    store.add_vertex("Item", &[("x", PropValue::Int(1))]);
    store.commit();
    let plan = sql_plan::parse_select("SELECT x FROM Item WHERE x = 999").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn select_edge_with_offset_limit() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT * FROM KNOWS LIMIT 1 OFFSET 1").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn select_edge_specific_columns() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT _src, _dst FROM KNOWS").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 2);
}

#[test]
fn select_where_neq() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT name FROM Person WHERE name != 'Alice'").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert_eq!(batch.num_rows(), 3); // Bob, Carol, Dave
}

#[test]
fn select_where_gt_without_btree() {
    // No btree registered — should fall back to columnar scan
    let mut store = MutableCsrStore::new();
    store.add_vertex("Item", &[
        ("val", PropValue::Int(10)),
    ]);
    store.add_vertex("Item", &[
        ("val", PropValue::Int(20)),
    ]);
    store.add_vertex("Item", &[
        ("val", PropValue::Int(30)),
    ]);
    store.commit();
    let plan = sql_plan::parse_select("SELECT * FROM Item WHERE val > 15").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert!(batch.num_rows() >= 1); // at least 20 and 30
}

#[test]
fn select_empty_result() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT name FROM Person WHERE age = 999").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn select_large_offset_returns_empty() {
    let store = test_store();
    let plan = sql_plan::parse_select("SELECT name FROM Person LIMIT 10 OFFSET 1000").unwrap();
    let batch = executor::execute(&store, &plan).unwrap();
    assert_eq!(batch.num_rows(), 0);
}
