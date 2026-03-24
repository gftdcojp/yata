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
    /// Get all properties of a vertex as a map.
    fn vertex_all_props(&self, vid: u32) -> std::collections::HashMap<String, PropValue> {
        let _ = vid;
        std::collections::HashMap::new()
    }
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

// ── Tiered storage (HOT OLTP → COLD OLAP) ──────────────────────────────

/// Storage temperature tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Tier {
    /// In-memory MutableCSR (ns latency, bounded by RAM).
    Hot,
    /// Disk-backed (ms latency, bounded by disk).
    Warm,
    /// S3/R2 (100ms+ latency, infinite scale).
    Cold,
}

/// Tier-aware store — knows which tier it serves and what labels it holds.
pub trait Tiered {
    fn tier(&self) -> Tier;
    fn can_serve(&self, labels: &[String]) -> bool;
}

// ── Versioning (MVCC snapshots) ──────────────────────────────────────────

/// Version identifier (MVCC commit).
pub type VersionId = u64;

/// Time-travel and snapshot support.
pub trait Versioned {
    fn current_version(&self) -> VersionId;
    fn versions(&self, limit: usize) -> Vec<VersionId>;
    fn has_version(&self, version: VersionId) -> bool;
}

// ── Typed ID neighbor (M1: global/local ID separation) ──────────────────
//
// Design decision (M1): GRIN traits keep bare u32 parameters — these are
// implicitly **local** vertex/edge IDs within a single partition store.
// Cross-partition references use VertexRef { partition_id, vid }.
// Future M2 will introduce TypedTopology<V,E> generic traits; for now
// the u32 surface is stable and the global→local translation lives in
// yata-store::GlobalToLocalMap.

/// Typed neighbor entry using explicit LocalVid/LocalEid semantics.
/// Parallel to `Neighbor` but with newtype IDs for stronger guarantees.
#[derive(Debug, Clone)]
pub struct TypedNeighbor {
    /// Local vertex ID of the neighbor (within the same partition).
    pub local_vid: u32,
    /// Local edge ID connecting to this neighbor.
    pub local_eid: u32,
    /// Edge label (relationship type).
    pub edge_label: String,
}

impl From<Neighbor> for TypedNeighbor {
    fn from(n: Neighbor) -> Self {
        Self {
            local_vid: n.vid,
            local_eid: n.edge_id,
            edge_label: n.edge_label,
        }
    }
}

impl From<TypedNeighbor> for Neighbor {
    fn from(n: TypedNeighbor) -> Self {
        Self {
            vid: n.local_vid,
            edge_id: n.local_eid,
            edge_label: n.edge_label,
        }
    }
}

// ── Distributed partition (GRIN edge-cut / vertex-cut) ──────────────────

/// Cross-partition vertex reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VertexRef {
    pub partition_id: u32,
    pub vid: u32,
}

/// Partition strategy for distributed graph storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionStrategy {
    /// Each vertex lives on exactly one partition; edges may cross.
    EdgeCut,
    /// Each edge lives on exactly one partition; vertices may be replicated.
    VertexCut,
}

/// Extended distributed partition awareness (GRIN-style).
pub trait DistributedPartition: Partitioned {
    fn strategy(&self) -> PartitionStrategy;
    fn is_mirror(&self, vid: u32) -> bool {
        !self.is_master(vid)
    }
    fn master_of(&self, vid: u32) -> VertexRef;
    fn mirrors_of(&self, vid: u32) -> Vec<u32>;
}

// ── Combined tiered traits ──────────────────────────────────────────────

/// Tiered + versioned graph store.
pub trait TieredGraphStore: GraphStore + Tiered + Versioned {}
impl<T: GraphStore + Tiered + Versioned> TieredGraphStore for T {}

/// Tiered + mutable graph store.
pub trait TieredMutableGraphStore: TieredGraphStore + Mutable {}
impl<T: TieredGraphStore + Mutable> TieredMutableGraphStore for T {}

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
        assert_eq!(
            PropValue::Str("hello".into()),
            PropValue::Str("hello".into())
        );
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
        let n = Neighbor {
            vid: 1,
            edge_id: 0,
            edge_label: "KNOWS".into(),
        };
        let n2 = n.clone();
        assert_eq!(n.vid, n2.vid);
        assert_eq!(n.edge_id, n2.edge_id);
        assert_eq!(n.edge_label, n2.edge_label);
    }

    #[test]
    fn test_tier_equality() {
        assert_eq!(Tier::Hot, Tier::Hot);
        assert_ne!(Tier::Hot, Tier::Warm);
        assert_ne!(Tier::Warm, Tier::Cold);
    }

    #[test]
    fn test_vertex_ref() {
        let r = VertexRef {
            partition_id: 2,
            vid: 42,
        };
        let r2 = r.clone();
        assert_eq!(r, r2);
        assert_eq!(r.partition_id, 2);
        assert_eq!(r.vid, 42);
    }

    #[test]
    fn test_partition_strategy() {
        assert_eq!(PartitionStrategy::EdgeCut, PartitionStrategy::EdgeCut);
        assert_ne!(PartitionStrategy::EdgeCut, PartitionStrategy::VertexCut);
    }

    #[test]
    fn test_tier_serde() {
        let json = serde_json::to_string(&Tier::Cold).unwrap();
        let tier: Tier = serde_json::from_str(&json).unwrap();
        assert_eq!(tier, Tier::Cold);
    }

    #[test]
    fn test_vertex_ref_serde() {
        let r = VertexRef {
            partition_id: 1,
            vid: 99,
        };
        let json = serde_json::to_string(&r).unwrap();
        let r2: VertexRef = serde_json::from_str(&json).unwrap();
        assert_eq!(r, r2);
    }
}
