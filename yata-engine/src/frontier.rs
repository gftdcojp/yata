//! Cross-partition frontier exchange (M5: cross-partition query).
//!
//! When a multi-hop traversal crosses partition boundaries, the frontier
//! (set of vertices to expand next) must be dispatched to the correct
//! partitions. This module implements:
//!
//! - `Frontier`: a set of global vertex IDs grouped by partition
//! - `FrontierStep`: one hop expansion producing a new frontier
//! - `TraversalPlan`: N-hop traversal with partition-aware frontier exchange
//! - `TraversalResult`: collected paths with per-hop metrics

use std::collections::{HashMap, HashSet};

use yata_core::{GlobalVid, LocalVid, PartitionId};
use yata_grin::{Neighbor, Topology};
use yata_store::MutableCsrStore;
use yata_store::mirror::MirrorRegistry;
use yata_store::partition::PartitionStoreSet;

// ── Frontier ────────────────────────────────────────────────────────

/// A frontier: set of global vertex IDs grouped by owning partition.
#[derive(Debug, Clone, Default)]
pub struct Frontier {
    /// partition_id → set of GlobalVid on that partition.
    pub by_partition: HashMap<u32, Vec<GlobalVid>>,
    /// Total vertex count across all partitions.
    pub total: usize,
}

impl Frontier {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a frontier from a single vertex (routes to home partition).
    pub fn from_single(pss: &PartitionStoreSet, gvid: GlobalVid) -> Self {
        let pid = pss.find_vertex_home(gvid).unwrap_or(0);
        let mut f = Self::new();
        f.by_partition.entry(pid).or_default().push(gvid);
        f.total = 1;
        f
    }

    /// Create a frontier from multiple global vertices (routes to home partitions).
    pub fn from_vertices(pss: &PartitionStoreSet, gvids: &[GlobalVid]) -> Self {
        let mut f = Self::new();
        for &gvid in gvids {
            let pid = pss.find_vertex_home(gvid).unwrap_or(0);
            f.by_partition.entry(pid).or_default().push(gvid);
        }
        f.total = gvids.len();
        f
    }

    /// Number of partitions touched by this frontier.
    pub fn partition_count(&self) -> usize {
        self.by_partition.len()
    }

    /// Whether the frontier is empty.
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// All global vertex IDs in the frontier.
    pub fn all_vertices(&self) -> Vec<GlobalVid> {
        self.by_partition
            .values()
            .flat_map(|v| v.iter().copied())
            .collect()
    }
}

// ── Traversal step ──────────────────────────────────────────────────

/// Result of expanding one hop from a frontier.
#[derive(Debug, Clone)]
pub struct HopResult {
    /// Edges discovered in this hop: (src_gvid, dst_gvid, edge_label).
    pub edges: Vec<(GlobalVid, GlobalVid, String)>,
    /// Next frontier (destination vertices).
    pub next_frontier: Frontier,
    /// Metrics for this hop.
    pub metrics: HopMetrics,
}

#[derive(Debug, Clone, Default)]
pub struct HopMetrics {
    /// Partitions consulted for this hop.
    pub partitions_touched: usize,
    /// Local expansions (src and dst on same partition).
    pub local_expansions: usize,
    /// Remote expansions (dst on different partition than src).
    pub remote_expansions: usize,
    /// Vertices in the input frontier.
    pub frontier_size: usize,
    /// Edges discovered.
    pub edges_found: usize,
}

/// Expand one hop from a frontier. For each vertex in the frontier,
/// find its outgoing neighbors on its **home partition** (where real
/// outgoing edges live). Ghost vertices on other partitions are skipped.
/// Returns edges + the next frontier of destination vertices.
pub fn expand_hop(
    pss: &PartitionStoreSet,
    frontier: &Frontier,
    edge_label: Option<&str>,
    visited: &HashSet<u64>,
) -> HopResult {
    let mut edges = Vec::new();
    let mut next_vids: HashSet<u64> = HashSet::new();
    let mut touched_pids: HashSet<u32> = HashSet::new();
    let mut local_exp = 0usize;
    let mut remote_exp = 0usize;

    // Collect all unique global VIDs in the frontier
    let all_gvids: Vec<GlobalVid> = frontier.all_vertices();

    for src_gvid in all_gvids {
        // Find the vertex's HOME partition (where real outgoing edges are)
        let (home_pid, store, local_vid) = match resolve_home(pss, src_gvid) {
            Some(r) => r,
            None => continue,
        };
        touched_pids.insert(home_pid);

        let neighbors: Vec<Neighbor> = match edge_label {
            Some(el) => Topology::out_neighbors_by_label(store, local_vid, el),
            None => Topology::out_neighbors(store, local_vid),
        };

        for n in neighbors {
            let dst_gvid = store
                .global_map()
                .to_global(n.vid)
                .unwrap_or(GlobalVid::encode(
                    PartitionId::new(home_pid),
                    LocalVid::new(n.vid),
                ));

            if visited.contains(&dst_gvid.0) {
                continue;
            }

            // Classify local vs remote based on dst home partition
            let dst_home = pss.find_vertex_home(dst_gvid).unwrap_or(home_pid);
            if dst_home == home_pid {
                local_exp += 1;
            } else {
                remote_exp += 1;
            }

            edges.push((src_gvid, dst_gvid, n.edge_label));
            next_vids.insert(dst_gvid.0);
        }
    }

    let metrics = HopMetrics {
        partitions_touched: touched_pids.len(),
        local_expansions: local_exp,
        remote_expansions: remote_exp,
        frontier_size: frontier.total,
        edges_found: edges.len(),
    };

    let next_gvids: Vec<GlobalVid> = next_vids.iter().map(|&v| GlobalVid::new(v)).collect();
    let next_frontier = Frontier::from_vertices(pss, &next_gvids);

    HopResult {
        edges,
        next_frontier,
        metrics,
    }
}

/// Expand one hop from a frontier, using MirrorRegistry to expand ghost vertices locally.
/// For each vertex in the frontier:
/// - If the vertex has a home partition, expand normally.
/// - If the vertex is a ghost (no home found), check MirrorRegistry for cached outgoing edges.
/// This avoids coordinator round-trips for 2-hop cross-partition traversals.
pub fn expand_hop_with_mirrors(
    pss: &PartitionStoreSet,
    mirrors: &MirrorRegistry,
    frontier: &Frontier,
    edge_label: Option<&str>,
    visited: &HashSet<u64>,
) -> HopResult {
    let mut edges = Vec::new();
    let mut next_vids: HashSet<u64> = HashSet::new();
    let mut touched_pids: HashSet<u32> = HashSet::new();
    let mut local_exp = 0usize;
    let mut remote_exp = 0usize;

    let all_gvids: Vec<GlobalVid> = frontier.all_vertices();

    for src_gvid in all_gvids {
        // Try home partition first (normal path)
        match resolve_home(pss, src_gvid) {
            Some((home_pid, store, local_vid)) => {
                touched_pids.insert(home_pid);

                let neighbors: Vec<Neighbor> = match edge_label {
                    Some(el) => Topology::out_neighbors_by_label(store, local_vid, el),
                    None => Topology::out_neighbors(store, local_vid),
                };

                for n in neighbors {
                    let dst_gvid = store
                        .global_map()
                        .to_global(n.vid)
                        .unwrap_or(GlobalVid::encode(
                            PartitionId::new(home_pid),
                            LocalVid::new(n.vid),
                        ));

                    if visited.contains(&dst_gvid.0) {
                        continue;
                    }

                    let dst_home = pss.find_vertex_home(dst_gvid).unwrap_or(home_pid);
                    if dst_home == home_pid {
                        local_exp += 1;
                    } else {
                        remote_exp += 1;
                    }

                    edges.push((src_gvid, dst_gvid, n.edge_label));
                    next_vids.insert(dst_gvid.0);
                }
            }
            None => {
                // No home found — this vertex is only known as a ghost.
                // Check MirrorRegistry for cached outgoing edges.
                if let Some(mirror_edges) = mirrors.get_outgoing(src_gvid) {
                    for me in mirror_edges {
                        if let Some(el) = edge_label {
                            if me.label != el {
                                continue;
                            }
                        }
                        if visited.contains(&me.dst_global_vid.0) {
                            continue;
                        }
                        // Mirror expansions are local (no coordinator round-trip)
                        local_exp += 1;
                        edges.push((src_gvid, me.dst_global_vid, me.label.clone()));
                        next_vids.insert(me.dst_global_vid.0);
                    }
                }
            }
        }
    }

    let metrics = HopMetrics {
        partitions_touched: touched_pids.len(),
        local_expansions: local_exp,
        remote_expansions: remote_exp,
        frontier_size: frontier.total,
        edges_found: edges.len(),
    };

    let next_gvids: Vec<GlobalVid> = next_vids.iter().map(|&v| GlobalVid::new(v)).collect();
    let next_frontier = Frontier::from_vertices(pss, &next_gvids);

    HopResult {
        edges,
        next_frontier,
        metrics,
    }
}

/// Execute an N-hop traversal with MirrorRegistry support.
/// Before each hop, ghost vertices in the frontier are upgraded to mirrors via ensure_mirrors.
pub fn traverse_with_mirrors(
    pss: &PartitionStoreSet,
    mirrors: &mut MirrorRegistry,
    start: GlobalVid,
    max_hops: u32,
    edge_label: Option<&str>,
    max_frontier: usize,
) -> TraversalResult {
    let mut visited: HashSet<u64> = HashSet::new();
    visited.insert(start.0);

    let mut current = Frontier::from_single(pss, start);
    let mut hops = Vec::new();
    let mut total_edges = 0;

    for _hop in 0..max_hops {
        if current.is_empty() {
            break;
        }

        let effective_frontier = if current.total > max_frontier {
            truncate_frontier(&current, max_frontier)
        } else {
            current.clone()
        };

        // Upgrade ghost vertices in the frontier to mirrors before expanding
        let frontier_vids = effective_frontier.all_vertices();
        yata_store::mirror::ensure_mirrors(pss, mirrors, &frontier_vids);

        let result =
            expand_hop_with_mirrors(pss, mirrors, &effective_frontier, edge_label, &visited);

        for v in result.next_frontier.all_vertices() {
            visited.insert(v.0);
        }

        total_edges += result.edges.len();
        current = result.next_frontier.clone();
        hops.push(result);
    }

    let total_vertices_reached = visited.len().saturating_sub(1);

    TraversalResult {
        hops,
        visited,
        total_edges,
        total_vertices_reached,
    }
}

/// Resolve a global vertex to its home partition (non-ghost).
fn resolve_home(pss: &PartitionStoreSet, gvid: GlobalVid) -> Option<(u32, &MutableCsrStore, u32)> {
    for (pid, store) in pss.partitions() {
        if let Some(local) = store.global_map().to_local(gvid) {
            let labels = yata_grin::Property::vertex_labels(store, local);
            if labels.first().map(|l| l.as_str()) != Some("_ghost") {
                return Some((pid, store, local));
            }
        }
    }
    None
}

// ── Multi-hop traversal ─────────────────────────────────────────────

/// Result of a multi-hop traversal.
#[derive(Debug)]
pub struct TraversalResult {
    /// All edges discovered, grouped by hop (0-indexed).
    pub hops: Vec<HopResult>,
    /// All unique vertices visited (as GlobalVid raw values).
    pub visited: HashSet<u64>,
    /// Total edges across all hops.
    pub total_edges: usize,
    /// Total unique vertices reached (excluding start).
    pub total_vertices_reached: usize,
}

/// Execute an N-hop traversal from a starting vertex.
///
/// - `max_hops`: maximum number of hops (1 = direct neighbors, 2 = friends-of-friends, etc.)
/// - `edge_label`: optional edge type filter
/// - `max_frontier`: maximum frontier size per hop (prevents explosion on hub nodes)
pub fn traverse(
    pss: &PartitionStoreSet,
    start: GlobalVid,
    max_hops: u32,
    edge_label: Option<&str>,
    max_frontier: usize,
) -> TraversalResult {
    let mut visited: HashSet<u64> = HashSet::new();
    visited.insert(start.0);

    let mut current = Frontier::from_single(pss, start);
    let mut hops = Vec::new();
    let mut total_edges = 0;

    for _hop in 0..max_hops {
        if current.is_empty() {
            break;
        }

        // Truncate frontier if too large
        let effective_frontier = if current.total > max_frontier {
            truncate_frontier(&current, max_frontier)
        } else {
            current.clone()
        };

        let result = expand_hop(pss, &effective_frontier, edge_label, &visited);

        // Add discovered vertices to visited set
        for v in result.next_frontier.all_vertices() {
            visited.insert(v.0);
        }

        total_edges += result.edges.len();
        current = result.next_frontier.clone();
        hops.push(result);
    }

    let total_vertices_reached = visited.len().saturating_sub(1); // exclude start

    TraversalResult {
        hops,
        visited,
        total_edges,
        total_vertices_reached,
    }
}

/// Shortest path between two vertices using BFS with frontier exchange.
/// Returns the path as a list of GlobalVids, or None if not reachable within max_hops.
pub fn shortest_path(
    pss: &PartitionStoreSet,
    from: GlobalVid,
    to: GlobalVid,
    max_hops: u32,
    edge_label: Option<&str>,
) -> Option<Vec<GlobalVid>> {
    let mut visited: HashSet<u64> = HashSet::new();
    visited.insert(from.0);

    // parent map: child_gvid → parent_gvid
    let mut parent: HashMap<u64, u64> = HashMap::new();
    let mut current = Frontier::from_single(pss, from);

    for _hop in 0..max_hops {
        if current.is_empty() {
            break;
        }

        let result = expand_hop(pss, &current, edge_label, &visited);

        for &(src, dst, _) in &result.edges {
            if !visited.contains(&dst.0) {
                parent.insert(dst.0, src.0);
                visited.insert(dst.0);
            }

            // Found target
            if dst.0 == to.0 {
                return Some(reconstruct_path(&parent, from.0, to.0));
            }
        }

        current = result.next_frontier;
    }

    None
}

fn reconstruct_path(parent: &HashMap<u64, u64>, from: u64, to: u64) -> Vec<GlobalVid> {
    let mut path = vec![GlobalVid::new(to)];
    let mut current = to;
    while current != from {
        match parent.get(&current) {
            Some(&p) => {
                path.push(GlobalVid::new(p));
                current = p;
            }
            None => break,
        }
    }
    path.reverse();
    path
}

/// Truncate a frontier to at most `max` vertices (round-robin across partitions).
fn truncate_frontier(frontier: &Frontier, max: usize) -> Frontier {
    let mut result = Frontier::new();
    let mut count = 0;

    // Round-robin across partitions
    let pids: Vec<u32> = frontier.by_partition.keys().copied().collect();
    let max_per_partition = (max / pids.len().max(1)) + 1;

    for pid in &pids {
        if count >= max {
            break;
        }
        if let Some(vids) = frontier.by_partition.get(pid) {
            let take = vids.len().min(max_per_partition).min(max - count);
            let subset: Vec<GlobalVid> = vids[..take].to_vec();
            count += subset.len();
            result.by_partition.insert(*pid, subset);
        }
    }
    result.total = count;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_grin::PropValue;
    use yata_store::partition::{PartitionAssignment, PartitionStoreSet};

    /// Build a chain graph: 0→1→2→...→(n-1) across partitions.
    fn build_chain(n: u64, partitions: u32) -> PartitionStoreSet {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash {
            partition_count: partitions,
        });
        for i in 0..n {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            pss.add_vertex(gv, "Node", &[("idx", PropValue::Int(i as i64))]);
        }
        for i in 0..n.saturating_sub(1) {
            let src = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            let dst = GlobalVid::encode(PartitionId::new(0), LocalVid::new((i + 1) as u32));
            pss.add_edge(src, dst, "NEXT", &[]);
        }
        pss.commit();
        pss
    }

    /// Build a star graph: center→spoke0, center→spoke1, ...
    fn build_star(spokes: u64, partitions: u32) -> PartitionStoreSet {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash {
            partition_count: partitions,
        });
        let center = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        pss.add_vertex(center, "Hub", &[]);
        for i in 1..=spokes {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            pss.add_vertex(gv, "Spoke", &[]);
            pss.add_edge(center, gv, "CONNECTS", &[]);
        }
        pss.commit();
        pss
    }

    #[test]
    fn test_frontier_from_single() {
        let pss = build_chain(10, 4);
        let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let f = Frontier::from_single(&pss, gv);
        assert_eq!(f.total, 1);
        assert_eq!(f.partition_count(), 1);
    }

    #[test]
    fn test_frontier_from_vertices() {
        let pss = build_chain(10, 4);
        let gvids: Vec<GlobalVid> = (0..5)
            .map(|i| GlobalVid::encode(PartitionId::new(0), LocalVid::new(i)))
            .collect();
        let f = Frontier::from_vertices(&pss, &gvids);
        assert_eq!(f.total, 5);
        // With hash partitioning, vertices may be on different partitions
        assert!(f.partition_count() >= 1);
    }

    #[test]
    fn test_expand_hop_chain() {
        let pss = build_chain(10, 4);
        let start = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let frontier = Frontier::from_single(&pss, start);
        let visited = HashSet::new();

        let result = expand_hop(&pss, &frontier, None, &visited);
        assert_eq!(result.edges.len(), 1); // 0→1
        assert_eq!(result.next_frontier.total, 1);
        assert_eq!(result.metrics.edges_found, 1);
    }

    #[test]
    fn test_expand_hop_star() {
        let pss = build_star(10, 4);
        let center = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let frontier = Frontier::from_single(&pss, center);
        let visited = HashSet::new();

        let result = expand_hop(&pss, &frontier, None, &visited);
        assert_eq!(result.edges.len(), 10);
        assert_eq!(result.next_frontier.total, 10);
    }

    #[test]
    fn test_expand_hop_with_edge_filter() {
        let pss = build_star(10, 4);
        let center = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let frontier = Frontier::from_single(&pss, center);
        let visited = HashSet::new();

        let result = expand_hop(&pss, &frontier, Some("CONNECTS"), &visited);
        assert_eq!(result.edges.len(), 10);

        let result2 = expand_hop(&pss, &frontier, Some("NONEXISTENT"), &visited);
        assert_eq!(result2.edges.len(), 0);
    }

    #[test]
    fn test_traverse_chain_3_hops() {
        let pss = build_chain(10, 4);
        let start = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));

        let result = traverse(&pss, start, 3, None, 1000);
        assert_eq!(result.hops.len(), 3);
        assert_eq!(result.total_edges, 3); // 0→1, 1→2, 2→3
        assert_eq!(result.total_vertices_reached, 3);
    }

    #[test]
    fn test_traverse_chain_exceeds_length() {
        let pss = build_chain(5, 2);
        let start = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));

        let result = traverse(&pss, start, 10, None, 1000);
        // Chain: 0→1→2→3→4, only 4 hops possible
        assert_eq!(result.total_edges, 4);
        assert_eq!(result.total_vertices_reached, 4);
    }

    #[test]
    fn test_traverse_star_1_hop() {
        let pss = build_star(20, 4);
        let center = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));

        let result = traverse(&pss, center, 1, None, 1000);
        assert_eq!(result.hops.len(), 1);
        assert_eq!(result.total_edges, 20);
        assert_eq!(result.total_vertices_reached, 20);
    }

    #[test]
    fn test_traverse_no_cycles() {
        let pss = build_chain(5, 2);
        let start = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));

        let result = traverse(&pss, start, 100, None, 1000);
        // visited set prevents revisiting → total edges = chain length
        assert_eq!(result.total_edges, 4);
    }

    #[test]
    fn test_traverse_max_frontier_limits() {
        let pss = build_star(100, 4);
        let center = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));

        // Limit frontier to 10
        let result = traverse(&pss, center, 2, None, 10);
        // First hop gets 100 neighbors but frontier truncated to 10 for hop 2
        assert_eq!(result.hops[0].edges.len(), 100);
        // Second hop frontier was truncated
        assert!(result.hops.len() >= 1);
    }

    #[test]
    fn test_hop_metrics_local_vs_remote() {
        let pss = build_chain(10, 4);
        let start = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let frontier = Frontier::from_single(&pss, start);
        let visited = HashSet::new();

        let result = expand_hop(&pss, &frontier, None, &visited);
        // 0→1: src and dst may be on same or different partition
        assert_eq!(
            result.metrics.local_expansions + result.metrics.remote_expansions,
            1
        );
    }

    #[test]
    fn test_shortest_path_chain() {
        let pss = build_chain(10, 4);
        let from = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let to = GlobalVid::encode(PartitionId::new(0), LocalVid::new(3));

        let path = shortest_path(&pss, from, to, 10, None);
        assert!(path.is_some());
        let path = path.unwrap();
        assert_eq!(path.len(), 4); // 0→1→2→3
        assert_eq!(path.first().unwrap().0, from.0);
        assert_eq!(path.last().unwrap().0, to.0);
    }

    #[test]
    fn test_shortest_path_unreachable() {
        let pss = build_chain(5, 2);
        let from = GlobalVid::encode(PartitionId::new(0), LocalVid::new(4));
        let to = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));

        // Chain is 0→1→2→3→4, no reverse edges
        let path = shortest_path(&pss, from, to, 10, None);
        assert!(path.is_none());
    }

    #[test]
    fn test_shortest_path_same_vertex() {
        let pss = build_chain(5, 2);
        let v = GlobalVid::encode(PartitionId::new(0), LocalVid::new(2));

        let path = shortest_path(&pss, v, v, 10, None);
        // Already at destination → None (BFS won't find it since start is in visited)
        assert!(path.is_none());
    }

    #[test]
    fn test_shortest_path_max_hops_exceeded() {
        let pss = build_chain(10, 2);
        let from = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let to = GlobalVid::encode(PartitionId::new(0), LocalVid::new(5));

        // Only allow 2 hops — not enough for distance 5
        let path = shortest_path(&pss, from, to, 2, None);
        assert!(path.is_none());

        // Allow 5 hops — exactly enough
        let path = shortest_path(&pss, from, to, 5, None);
        assert!(path.is_some());
        assert_eq!(path.unwrap().len(), 6); // 0,1,2,3,4,5
    }

    #[test]
    fn test_truncate_frontier() {
        let pss = build_star(100, 4);
        let center = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let frontier = Frontier::from_single(&pss, center);
        let visited = HashSet::new();

        let result = expand_hop(&pss, &frontier, None, &visited);
        assert_eq!(result.next_frontier.total, 100);

        let truncated = truncate_frontier(&result.next_frontier, 10);
        assert!(truncated.total <= 10);
        assert!(truncated.total > 0);
    }

    #[test]
    fn test_frontier_from_empty() {
        let pss = build_chain(10, 2);
        let f = Frontier::from_vertices(&pss, &[]);
        assert!(f.is_empty());
        assert_eq!(f.total, 0);
        assert_eq!(f.partition_count(), 0);
        assert!(f.all_vertices().is_empty());
    }

    #[test]
    fn test_frontier_merge() {
        let pss = build_chain(10, 4);
        let gvids_a: Vec<GlobalVid> = (0..3)
            .map(|i| GlobalVid::encode(PartitionId::new(0), LocalVid::new(i)))
            .collect();
        let gvids_b: Vec<GlobalVid> = (5..8)
            .map(|i| GlobalVid::encode(PartitionId::new(0), LocalVid::new(i)))
            .collect();

        let fa = Frontier::from_vertices(&pss, &gvids_a);
        let fb = Frontier::from_vertices(&pss, &gvids_b);

        // Merge: combine both frontiers into one
        let mut merged = Frontier::new();
        for (pid, vids) in fa.by_partition.iter().chain(fb.by_partition.iter()) {
            merged.by_partition.entry(*pid).or_default().extend(vids);
        }
        merged.total = fa.total + fb.total;

        assert_eq!(merged.total, 6);
        let all = merged.all_vertices();
        assert_eq!(all.len(), 6);
        // All original vertices should be present
        for gv in gvids_a.iter().chain(gvids_b.iter()) {
            assert!(all.contains(gv));
        }
    }

    #[test]
    fn test_frontier_partition_grouping() {
        // Use many partitions so vertices land on different ones
        let pss = build_chain(20, 8);
        let gvids: Vec<GlobalVid> = (0..20)
            .map(|i| GlobalVid::encode(PartitionId::new(0), LocalVid::new(i)))
            .collect();
        let f = Frontier::from_vertices(&pss, &gvids);

        assert_eq!(f.total, 20);
        // by_partition should have at least 1 partition
        assert!(f.partition_count() >= 1);

        // Sum of all partition vertex counts must equal total
        let sum: usize = f.by_partition.values().map(|v| v.len()).sum();
        assert_eq!(sum, 20);

        // Every vertex from by_partition should be in the original set
        for vids in f.by_partition.values() {
            for gv in vids {
                assert!(gvids.contains(gv));
            }
        }
    }

    #[test]
    fn test_expand_hop_with_edge_label() {
        // Build a graph with two edge types
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 2 });
        let v0 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let v1 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(1));
        let v2 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(2));
        pss.add_vertex(v0, "Node", &[]);
        pss.add_vertex(v1, "Node", &[]);
        pss.add_vertex(v2, "Node", &[]);
        pss.add_edge(v0, v1, "KNOWS", &[]);
        pss.add_edge(v0, v2, "LIKES", &[]);
        pss.commit();

        let frontier = Frontier::from_single(&pss, v0);
        let visited = HashSet::new();

        // Filter by KNOWS — should find only v1
        let result = expand_hop(&pss, &frontier, Some("KNOWS"), &visited);
        assert_eq!(result.edges.len(), 1);
        let dst_gvids: Vec<u64> = result.edges.iter().map(|(_, d, _)| d.0).collect();
        assert!(dst_gvids.iter().any(|&d| d == v1.0));

        // Filter by LIKES — should find only v2
        let result2 = expand_hop(&pss, &frontier, Some("LIKES"), &visited);
        assert_eq!(result2.edges.len(), 1);
        let dst_gvids2: Vec<u64> = result2.edges.iter().map(|(_, d, _)| d.0).collect();
        assert!(dst_gvids2.iter().any(|&d| d == v2.0));
    }

    #[test]
    fn test_expand_hop_untyped() {
        // Build a graph with multiple edge types, expand without filter
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 2 });
        let v0 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let v1 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(1));
        let v2 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(2));
        let v3 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(3));
        pss.add_vertex(v0, "Node", &[]);
        pss.add_vertex(v1, "Node", &[]);
        pss.add_vertex(v2, "Node", &[]);
        pss.add_vertex(v3, "Node", &[]);
        pss.add_edge(v0, v1, "KNOWS", &[]);
        pss.add_edge(v0, v2, "LIKES", &[]);
        pss.add_edge(v0, v3, "WORKS_WITH", &[]);
        pss.commit();

        let frontier = Frontier::from_single(&pss, v0);
        let visited = HashSet::new();

        // No edge label filter — should find all 3 neighbors
        let result = expand_hop(&pss, &frontier, None, &visited);
        assert_eq!(result.edges.len(), 3);
        assert_eq!(result.next_frontier.total, 3);

        let dst_set: HashSet<u64> = result.edges.iter().map(|(_, d, _)| d.0).collect();
        assert!(dst_set.contains(&v1.0));
        assert!(dst_set.contains(&v2.0));
        assert!(dst_set.contains(&v3.0));
    }

    #[test]
    fn test_cross_partition_traversal_correctness() {
        // Build same graph in single and multi-partition, compare traversal results
        let single = build_chain(20, 1);
        let multi = build_chain(20, 4);
        let start = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));

        let r1 = traverse(&single, start, 5, None, 1000);
        let r2 = traverse(&multi, start, 5, None, 1000);

        assert_eq!(
            r1.total_edges, r2.total_edges,
            "single={} vs multi={}",
            r1.total_edges, r2.total_edges
        );
        assert_eq!(r1.total_vertices_reached, r2.total_vertices_reached);
    }

    // ── Mirror vertex tests ──────────────────────────────────────────

    #[test]
    fn test_expand_hop_with_mirrors_basic() {
        use yata_store::mirror::MirrorRegistry;

        let pss = build_chain(10, 4);
        let start = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let frontier = Frontier::from_single(&pss, start);
        let visited = HashSet::new();
        let mirrors = MirrorRegistry::new();

        // With empty mirror registry, behaves same as expand_hop
        let result = expand_hop_with_mirrors(&pss, &mirrors, &frontier, None, &visited);
        let baseline = expand_hop(&pss, &frontier, None, &visited);
        assert_eq!(result.edges.len(), baseline.edges.len());
    }

    #[test]
    fn test_mirror_2hop_cross_partition_traversal() {
        use yata_store::mirror::MirrorRegistry;

        // Build: Alice(P0) -[:WORKS_AT]-> GFTD(P1) -[:LOCATED_IN]-> Tokyo(P1)
        let mut label_map = std::collections::HashMap::new();
        label_map.insert("Person".to_string(), 0);
        label_map.insert("Company".to_string(), 1);
        label_map.insert("City".to_string(), 1);
        let assignment = PartitionAssignment::Label {
            label_map,
            default_partition: 0,
        };

        let mut pss = PartitionStoreSet::new(assignment);
        let alice = GlobalVid::from_local(0);
        let gftd = GlobalVid::from_local(1);
        let tokyo = GlobalVid::from_local(2);

        pss.add_vertex(alice, "Person", &[("name", PropValue::Str("Alice".into()))]);
        pss.add_vertex(gftd, "Company", &[("name", PropValue::Str("GFTD".into()))]);
        pss.add_vertex(tokyo, "City", &[("name", PropValue::Str("Tokyo".into()))]);
        pss.add_edge(alice, gftd, "WORKS_AT", &[]);
        pss.add_edge(gftd, tokyo, "LOCATED_IN", &[]);
        pss.commit();

        // traverse_with_mirrors should discover the full 2-hop path
        let mut mirrors = MirrorRegistry::new();
        let result = traverse_with_mirrors(&pss, &mut mirrors, alice, 2, None, 1000);

        // Hop 1: Alice -> GFTD
        assert!(result.hops.len() >= 1);
        assert_eq!(result.hops[0].edges.len(), 1);

        // Hop 2: GFTD -> Tokyo (via mirror)
        assert_eq!(result.hops.len(), 2);
        assert_eq!(result.hops[1].edges.len(), 1);
        let (_, dst, label) = &result.hops[1].edges[0];
        assert_eq!(*dst, tokyo);
        assert_eq!(label, "LOCATED_IN");

        assert_eq!(result.total_edges, 2);
        assert_eq!(result.total_vertices_reached, 2); // GFTD + Tokyo

        // Mirror should have been created for GFTD
        assert!(mirrors.is_mirror(gftd));
    }

    #[test]
    fn test_traverse_with_mirrors_edge_filter() {
        use yata_store::mirror::MirrorRegistry;

        let mut label_map = std::collections::HashMap::new();
        label_map.insert("A".to_string(), 0);
        label_map.insert("B".to_string(), 1);
        let assignment = PartitionAssignment::Label {
            label_map,
            default_partition: 0,
        };

        let mut pss = PartitionStoreSet::new(assignment);
        let v0 = GlobalVid::from_local(0);
        let v1 = GlobalVid::from_local(1);
        let v2 = GlobalVid::from_local(2);

        pss.add_vertex(v0, "A", &[]);
        pss.add_vertex(v1, "B", &[]);
        pss.add_vertex(v2, "B", &[]);
        pss.add_edge(v0, v1, "R1", &[]);
        pss.add_edge(v1, v2, "R2", &[]);
        pss.commit();

        // Filter by R1 — should only find v1, not v2
        let mut mirrors = MirrorRegistry::new();
        let result = traverse_with_mirrors(&pss, &mut mirrors, v0, 2, Some("R1"), 1000);
        assert_eq!(result.total_edges, 1);

        // No filter — should find both hops
        let mut mirrors2 = MirrorRegistry::new();
        let result2 = traverse_with_mirrors(&pss, &mut mirrors2, v0, 2, None, 1000);
        assert_eq!(result2.total_edges, 2);
    }
}
