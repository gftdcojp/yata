//! PartitionRouter: routes queries to relevant partitions based on label/property hints.
//!
//! Given a Cypher query's extracted hints (labels, rel_types, property eq filters),
//! the router determines which partitions need to be consulted.
//!
//! This avoids full fan-out when the query targets a known label→partition mapping.

use std::collections::HashSet;

use yata_store::partition::{PartitionAssignment, PartitionStoreSet};

/// Hints extracted from a Cypher query for partition routing.
#[derive(Debug, Clone, Default)]
pub struct PartitionHints {
    /// Node labels referenced in the query.
    pub node_labels: Vec<String>,
    /// Relationship types referenced in the query.
    pub rel_types: Vec<String>,
    /// Exact global vertex IDs referenced (e.g., WHERE n._id = 'xxx').
    pub global_vid_hints: Vec<u64>,
    /// Whether this is a read-only query.
    pub is_read_only: bool,
}

/// Result of partition routing: which partitions to query.
#[derive(Debug, Clone)]
pub enum PartitionScope {
    /// Query all partitions (no pruning possible).
    All,
    /// Query only these specific partitions.
    Subset(Vec<u32>),
    /// Query a single partition.
    Single(u32),
}

impl PartitionScope {
    /// Get the partition IDs to query.
    pub fn partition_ids(&self, total: u32) -> Vec<u32> {
        match self {
            PartitionScope::All => (0..total).collect(),
            PartitionScope::Subset(pids) => pids.clone(),
            PartitionScope::Single(pid) => vec![*pid],
        }
    }

    /// Number of partitions in scope.
    pub fn count(&self, total: u32) -> usize {
        match self {
            PartitionScope::All => total as usize,
            PartitionScope::Subset(pids) => pids.len(),
            PartitionScope::Single(_) => 1,
        }
    }

    /// Whether this scope covers all partitions.
    pub fn is_all(&self) -> bool {
        matches!(self, PartitionScope::All)
    }
}

/// Determine which partitions need to be queried.
pub fn route(pss: &PartitionStoreSet, hints: &PartitionHints) -> PartitionScope {
    let assignment = pss.assignment();
    let total = pss.partition_count();

    // Single partition mode: always single
    if total <= 1 {
        return PartitionScope::Single(0);
    }

    // If we have specific global VID hints, route to their partitions
    if !hints.global_vid_hints.is_empty() {
        let mut pids = HashSet::new();
        for &gvid in &hints.global_vid_hints {
            let gv = yata_core::GlobalVid::new(gvid);
            let pid = assignment.assign_vertex(gv, "");
            pids.insert(pid.0);
        }
        let pids: Vec<u32> = pids.into_iter().collect();
        return if pids.len() == 1 {
            PartitionScope::Single(pids[0])
        } else {
            PartitionScope::Subset(pids)
        };
    }

    // Label-based routing: if assignment is Label-based, we can prune
    if let PartitionAssignment::Label {
        label_map,
        default_partition,
    } = assignment
    {
        if !hints.node_labels.is_empty() {
            let mut pids = HashSet::new();
            for label in &hints.node_labels {
                let pid = label_map.get(label).copied().unwrap_or(*default_partition);
                pids.insert(pid);
            }
            let pids: Vec<u32> = pids.into_iter().collect();
            return if pids.len() == 1 {
                PartitionScope::Single(pids[0])
            } else {
                PartitionScope::Subset(pids)
            };
        }
    }

    // Hash partitioning with label hints: check which partitions actually have the label
    if !hints.node_labels.is_empty() {
        let mut pids = HashSet::new();
        for pid in 0..total {
            if let Some(store) = pss.partition(pid) {
                let store_labels = yata_grin::Schema::vertex_labels(store);
                if hints.node_labels.iter().any(|l| store_labels.contains(l)) {
                    pids.insert(pid);
                }
            }
        }
        if !pids.is_empty() && pids.len() < total as usize {
            let pids: Vec<u32> = pids.into_iter().collect();
            return if pids.len() == 1 {
                PartitionScope::Single(pids[0])
            } else {
                PartitionScope::Subset(pids)
            };
        }
    }

    // No pruning possible
    PartitionScope::All
}

/// Extract partition hints from a Cypher query string.
pub fn extract_partition_hints(cypher: &str) -> PartitionHints {
    let mut hints = PartitionHints::default();

    // Try to parse and extract hints
    if let Ok(query) = yata_cypher::parse(cypher) {
        let qh = yata_graph::hints::QueryHints::extract(&query);
        hints.node_labels = qh.node_labels;
        hints.rel_types = qh.rel_types;
        hints.is_read_only = qh.is_read_only;
    }

    hints
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use yata_core::{GlobalVid, LocalVid, PartitionId};
    use yata_grin::PropValue;

    fn make_hash_pss(count: u32, nodes: u64) -> PartitionStoreSet {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash {
            partition_count: count,
        });
        for i in 0..nodes {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            let label = format!("Label{}", i % 4);
            pss.add_vertex(gv, &label, &[("v", PropValue::Int(i as i64))]);
        }
        pss.commit();
        pss
    }

    fn make_label_pss() -> PartitionStoreSet {
        let mut label_map = HashMap::new();
        label_map.insert("Person".to_string(), 0);
        label_map.insert("Company".to_string(), 1);
        label_map.insert("Product".to_string(), 2);
        let assignment = PartitionAssignment::Label {
            label_map,
            default_partition: 0,
        };
        let mut pss = PartitionStoreSet::new(assignment);
        for i in 0..10 {
            let gv = GlobalVid::from_local(i);
            let label = match i % 3 {
                0 => "Person",
                1 => "Company",
                _ => "Product",
            };
            pss.add_vertex(gv, label, &[]);
        }
        pss.commit();
        pss
    }

    #[test]
    fn test_single_partition_always_single() {
        let pss = PartitionStoreSet::single();
        let hints = PartitionHints::default();
        let scope = route(&pss, &hints);
        assert!(matches!(scope, PartitionScope::Single(0)));
    }

    #[test]
    fn test_no_hints_routes_all() {
        let pss = make_hash_pss(4, 40);
        let hints = PartitionHints::default();
        let scope = route(&pss, &hints);
        assert!(scope.is_all());
        assert_eq!(scope.count(4), 4);
    }

    #[test]
    fn test_label_assignment_prunes() {
        let pss = make_label_pss();
        let hints = PartitionHints {
            node_labels: vec!["Person".to_string()],
            ..Default::default()
        };
        let scope = route(&pss, &hints);
        assert!(matches!(scope, PartitionScope::Single(0)));
    }

    #[test]
    fn test_label_assignment_multi_label() {
        let pss = make_label_pss();
        let hints = PartitionHints {
            node_labels: vec!["Person".to_string(), "Company".to_string()],
            ..Default::default()
        };
        let scope = route(&pss, &hints);
        match scope {
            PartitionScope::Subset(pids) => {
                assert!(pids.contains(&0));
                assert!(pids.contains(&1));
                assert_eq!(pids.len(), 2);
            }
            _ => panic!("expected Subset"),
        }
    }

    #[test]
    fn test_global_vid_hint_routes_to_partition() {
        let pss = make_hash_pss(4, 40);
        let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(5));
        let hints = PartitionHints {
            global_vid_hints: vec![gv.0],
            ..Default::default()
        };
        let scope = route(&pss, &hints);
        assert!(matches!(scope, PartitionScope::Single(_)));
    }

    #[test]
    fn test_hash_partition_label_pruning() {
        let pss = make_hash_pss(4, 40);
        // Label0 should exist in some partitions
        let hints = PartitionHints {
            node_labels: vec!["Label0".to_string()],
            ..Default::default()
        };
        let scope = route(&pss, &hints);
        // With hash distribution over 40 nodes and 4 labels,
        // Label0 likely exists in all 4 partitions
        let pids = scope.partition_ids(4);
        assert!(!pids.is_empty());
    }

    #[test]
    fn test_nonexistent_label_prunes_all() {
        let pss = make_hash_pss(4, 40);
        let hints = PartitionHints {
            node_labels: vec!["NonExistent".to_string()],
            ..Default::default()
        };
        let scope = route(&pss, &hints);
        // No partition has this label → falls through to All
        // (because the empty set check returns All as fallback)
        let pids = scope.partition_ids(4);
        assert!(!pids.is_empty());
    }

    #[test]
    fn test_extract_partition_hints() {
        let hints = extract_partition_hints("MATCH (n:Person) RETURN n.name");
        assert!(hints.node_labels.contains(&"Person".to_string()));
        assert!(hints.is_read_only);
    }

    #[test]
    fn test_extract_partition_hints_mutation() {
        let hints = extract_partition_hints("CREATE (n:Person {name: 'Alice'})");
        assert!(!hints.is_read_only);
    }

    #[test]
    fn test_partition_scope_ids() {
        assert_eq!(PartitionScope::All.partition_ids(4), vec![0, 1, 2, 3]);
        assert_eq!(PartitionScope::Single(2).partition_ids(4), vec![2]);
        assert_eq!(
            PartitionScope::Subset(vec![1, 3]).partition_ids(4),
            vec![1, 3]
        );
    }
}
