//! GRIN: Graph Retrieval INterface — storage-agnostic graph access traits.
//! Inspired by GraphScope's GRIN C API, implemented as Rust traits for zero-cost abstraction.

use serde::{Deserialize, Serialize};

/// Value type for graph properties.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

/// Direction for edge traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
    Both,
}

/// Predicate for storage-level filtering (pushdown).
#[derive(Debug, Clone)]
pub enum Predicate {
    True,
    Eq(String, PropValue),
    Neq(String, PropValue),
    Lt(String, PropValue),
    Gt(String, PropValue),
    In(String, Vec<PropValue>),
    StartsWith(String, String),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
}

/// Neighbor entry returned by adjacency queries.
#[derive(Debug, Clone)]
pub struct Neighbor {
    pub vid: u32,
    pub edge_id: u32,
    pub edge_label: String,
}

/// Graph topology access (O(1) vertex, O(degree) neighbors).
pub trait Topology {
    fn vertex_count(&self) -> usize;
    fn edge_count(&self) -> usize;
    fn has_vertex(&self, vid: u32) -> bool;
    fn out_degree(&self, vid: u32) -> usize;
    fn in_degree(&self, vid: u32) -> usize;
    fn out_neighbors(&self, vid: u32) -> Vec<Neighbor>;
    fn in_neighbors(&self, vid: u32) -> Vec<Neighbor>;
    fn out_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor>;
    fn in_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor>;
}

/// Vertex/edge property access (columnar).
pub trait Property {
    fn vertex_labels(&self, vid: u32) -> Vec<String>;
    fn vertex_prop(&self, vid: u32, key: &str) -> Option<PropValue>;
    fn edge_prop(&self, edge_id: u32, key: &str) -> Option<PropValue>;
    fn vertex_prop_keys(&self, label: &str) -> Vec<String>;
    fn edge_prop_keys(&self, label: &str) -> Vec<String>;
}

/// Schema information.
pub trait Schema {
    fn vertex_labels(&self) -> Vec<String>;
    fn edge_labels(&self) -> Vec<String>;
    fn vertex_primary_key(&self, label: &str) -> Option<String>;
}

/// Predicate pushdown for scan operations.
pub trait Scannable {
    fn scan_vertices(&self, label: &str, predicate: &Predicate) -> Vec<u32>;
    fn scan_vertices_by_label(&self, label: &str) -> Vec<u32>;
    fn scan_all_vertices(&self) -> Vec<u32>;
}

/// Partition awareness (for distributed execution).
pub trait Partitioned {
    fn partition_id(&self) -> u32;
    fn partition_count(&self) -> u32;
    fn vertex_partition(&self, vid: u32) -> u32;
    fn is_master(&self, vid: u32) -> bool;
}

/// Mutation operations.
pub trait Mutable {
    fn add_vertex(&mut self, label: &str, props: &[(&str, PropValue)]) -> u32;
    fn add_edge(&mut self, src: u32, dst: u32, label: &str, props: &[(&str, PropValue)]) -> u32;
    fn set_vertex_prop(&mut self, vid: u32, key: &str, value: PropValue);
    fn delete_vertex(&mut self, vid: u32);
    fn delete_edge(&mut self, edge_id: u32);
    fn commit(&mut self) -> u64;
}

/// Combined graph store trait.
pub trait GraphStore: Topology + Property + Schema + Scannable + Send + Sync {}
impl<T: Topology + Property + Schema + Scannable + Send + Sync> GraphStore for T {}

/// Mutable graph store.
pub trait MutableGraphStore: GraphStore + Mutable {}
impl<T: GraphStore + Mutable> MutableGraphStore for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predicate_construction() {
        let p = Predicate::And(
            Box::new(Predicate::Eq("name".into(), PropValue::Str("Alice".into()))),
            Box::new(Predicate::Gt("age".into(), PropValue::Int(18))),
        );
        match p {
            Predicate::And(_, _) => {}
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_prop_value_equality() {
        assert_eq!(PropValue::Int(42), PropValue::Int(42));
        assert_ne!(PropValue::Int(42), PropValue::Int(43));
        assert_eq!(PropValue::Str("hello".into()), PropValue::Str("hello".into()));
        assert_eq!(PropValue::Null, PropValue::Null);
        assert_ne!(PropValue::Bool(true), PropValue::Bool(false));
    }

    #[test]
    fn test_direction_copy() {
        let d = Direction::Out;
        let d2 = d;
        assert_eq!(d, d2);
    }

    #[test]
    fn test_neighbor_clone() {
        let n = Neighbor { vid: 1, edge_id: 0, edge_label: "KNOWS".into() };
        let n2 = n.clone();
        assert_eq!(n.vid, n2.vid);
        assert_eq!(n.edge_id, n2.edge_id);
        assert_eq!(n.edge_label, n2.edge_label);
    }
}
