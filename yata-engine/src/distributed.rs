//! Distributed query dispatch (M6: multi-node execution).
//!
//! Abstracts partition execution as either local (in-process) or remote (via trait).
//! This enables transparent scale-out: a single-node deployment uses `LocalExecutor`,
//! a multi-node deployment implements `RemoteExecutor` with network transport.

use yata_core::GlobalVid;
use yata_store::partition::PartitionStoreSet;

use crate::frontier::{Frontier, expand_hop};

/// A query request dispatched to a partition.
#[derive(Debug, Clone)]
pub struct PartitionRequest {
    pub partition_id: u32,
    pub cypher: String,
    pub params: Vec<(String, String)>,
}

/// A query response from a partition.
#[derive(Debug, Clone)]
pub struct PartitionResponse {
    pub partition_id: u32,
    pub rows: Vec<Vec<(String, String)>>,
    pub error: Option<String>,
}

/// Trait for executing queries on a (possibly remote) partition.
pub trait PartitionExecutor: Send + Sync {
    /// Execute a Cypher query on a specific partition.
    fn execute(&self, req: &PartitionRequest) -> PartitionResponse;

    /// Expand one hop from a set of global vertex IDs on a specific partition.
    fn expand_frontier(
        &self,
        partition_id: u32,
        gvids: &[GlobalVid],
        edge_label: Option<&str>,
    ) -> Vec<(GlobalVid, GlobalVid, String)>;
}

/// Local executor: dispatches to in-process PartitionStoreSet.
pub struct LocalExecutor<'a> {
    pss: &'a PartitionStoreSet,
}

impl<'a> LocalExecutor<'a> {
    pub fn new(pss: &'a PartitionStoreSet) -> Self {
        Self { pss }
    }
}

impl<'a> PartitionExecutor for LocalExecutor<'a> {
    fn execute(&self, req: &PartitionRequest) -> PartitionResponse {
        let store = match self.pss.partition(req.partition_id) {
            Some(s) => s,
            None => {
                return PartitionResponse {
                    partition_id: req.partition_id,
                    rows: Vec::new(),
                    error: Some(format!("partition {} not found", req.partition_id)),
                };
            }
        };

        let hints = crate::partition_query::extract_hints(&req.cypher);
        match crate::partition_query::execute_on_partition_pub(
            store,
            &req.cypher,
            &req.params,
            &hints,
        ) {
            Ok(rows) => PartitionResponse {
                partition_id: req.partition_id,
                rows,
                error: None,
            },
            Err(e) => PartitionResponse {
                partition_id: req.partition_id,
                rows: Vec::new(),
                error: Some(e),
            },
        }
    }

    fn expand_frontier(
        &self,
        _partition_id: u32,
        gvids: &[GlobalVid],
        edge_label: Option<&str>,
    ) -> Vec<(GlobalVid, GlobalVid, String)> {
        let frontier = Frontier::from_vertices(self.pss, gvids);
        let visited = std::collections::HashSet::new();
        let result = expand_hop(self.pss, &frontier, edge_label, &visited);
        result.edges
    }
}

/// Distributed query coordinator.
/// Dispatches queries to partitions and merges results.
pub struct QueryCoordinator<E: PartitionExecutor> {
    executor: E,
    partition_count: u32,
}

impl<E: PartitionExecutor> QueryCoordinator<E> {
    pub fn new(executor: E, partition_count: u32) -> Self {
        Self {
            executor,
            partition_count,
        }
    }

    /// Execute a query across all partitions.
    pub fn query(
        &self,
        cypher: &str,
        params: &[(String, String)],
        target_pids: Option<&[u32]>,
    ) -> Vec<PartitionResponse> {
        let pids: Vec<u32> = target_pids
            .map(|p| p.to_vec())
            .unwrap_or_else(|| (0..self.partition_count).collect());
        let mut responses = Vec::with_capacity(pids.len());

        for pid in pids {
            let req = PartitionRequest {
                partition_id: pid,
                cypher: cypher.to_string(),
                params: params.to_vec(),
            };
            responses.push(self.executor.execute(&req));
        }

        responses
    }

    /// Merge responses from multiple partitions.
    pub fn merge_responses(responses: &[PartitionResponse]) -> Vec<Vec<(String, String)>> {
        let mut all_rows = Vec::new();
        for resp in responses {
            if resp.error.is_none() {
                all_rows.extend(resp.rows.clone());
            }
        }
        all_rows
    }

    /// Get errors from responses.
    pub fn collect_errors(responses: &[PartitionResponse]) -> Vec<(u32, String)> {
        responses
            .iter()
            .filter_map(|r| r.error.as_ref().map(|e| (r.partition_id, e.clone())))
            .collect()
    }

    /// Distributed N-hop traversal using the executor for frontier expansion.
    pub fn traverse(
        &self,
        start: GlobalVid,
        max_hops: u32,
        edge_label: Option<&str>,
    ) -> DistributedTraversalResult {
        let mut visited = std::collections::HashSet::new();
        visited.insert(start.0);

        let mut current_gvids = vec![start];
        let mut all_edges = Vec::new();
        let mut hop_counts = Vec::new();

        for _hop in 0..max_hops {
            if current_gvids.is_empty() {
                break;
            }

            let edges = self.executor.expand_frontier(0, &current_gvids, edge_label);
            let mut next_gvids = Vec::new();
            for &(_, dst, _) in &edges {
                if visited.insert(dst.0) {
                    next_gvids.push(dst);
                }
            }

            hop_counts.push(edges.len());
            all_edges.extend(edges);
            current_gvids = next_gvids;
        }

        DistributedTraversalResult {
            edges: all_edges,
            vertices_reached: visited.len().saturating_sub(1),
            hops: hop_counts,
        }
    }
}

/// Result of a distributed traversal.
#[derive(Debug)]
pub struct DistributedTraversalResult {
    pub edges: Vec<(GlobalVid, GlobalVid, String)>,
    pub vertices_reached: usize,
    pub hops: Vec<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_core::{LocalVid, PartitionId};
    use yata_grin::PropValue;
    use yata_store::partition::PartitionAssignment;

    fn build_test_pss() -> PartitionStoreSet {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 2 });
        for i in 0..10u64 {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            pss.add_vertex(gv, "Node", &[("idx", PropValue::Int(i as i64))]);
        }
        for i in 0..9u64 {
            let src = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            let dst = GlobalVid::encode(PartitionId::new(0), LocalVid::new((i + 1) as u32));
            pss.add_edge(src, dst, "NEXT", &[]);
        }
        pss.commit();
        pss
    }

    #[test]
    fn test_local_executor_query() {
        let pss = build_test_pss();
        let exec = LocalExecutor::new(&pss);
        let resp = exec.execute(&PartitionRequest {
            partition_id: 0,
            cypher: "MATCH (n:Node) RETURN n.idx".to_string(),
            params: vec![],
        });
        assert!(resp.error.is_none());
        assert!(!resp.rows.is_empty());
    }

    #[test]
    fn test_local_executor_expand() {
        let pss = build_test_pss();
        let exec = LocalExecutor::new(&pss);
        let start = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let edges = exec.expand_frontier(0, &[start], None);
        assert!(!edges.is_empty());
    }

    #[test]
    fn test_coordinator_query() {
        let pss = build_test_pss();
        let exec = LocalExecutor::new(&pss);
        let coord = QueryCoordinator::new(exec, 2);
        let responses = coord.query("MATCH (n:Node) RETURN n.idx", &[], None);
        assert_eq!(responses.len(), 2);
        let errors = QueryCoordinator::<LocalExecutor>::collect_errors(&responses);
        assert!(errors.is_empty());

        let merged = QueryCoordinator::<LocalExecutor>::merge_responses(&responses);
        assert!(!merged.is_empty());
    }

    #[test]
    fn test_coordinator_traverse() {
        let pss = build_test_pss();
        let exec = LocalExecutor::new(&pss);
        let coord = QueryCoordinator::new(exec, 2);
        let start = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));

        let result = coord.traverse(start, 3, None);
        assert_eq!(result.hops.len(), 3);
        assert_eq!(result.vertices_reached, 3);
    }

    #[test]
    fn test_partition_not_found() {
        let pss = build_test_pss();
        let exec = LocalExecutor::new(&pss);
        let resp = exec.execute(&PartitionRequest {
            partition_id: 99,
            cypher: "MATCH (n) RETURN n".to_string(),
            params: vec![],
        });
        assert!(resp.error.is_some());
    }
}
