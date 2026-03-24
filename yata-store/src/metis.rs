//! METIS-style graph partitioning for skewed graphs.
//! Minimizes edge-cut while balancing partition sizes.

use std::collections::VecDeque;

/// Partition assignment result from METIS-style partitioning.
#[derive(Debug, Clone)]
pub struct MetisPartitionResult {
    /// vertex_id -> partition_id assignment.
    pub assignments: Vec<u32>,
    /// Number of edges cut (crossing partition boundaries).
    pub edge_cut: u64,
    /// Per-partition vertex counts.
    pub partition_sizes: Vec<u32>,
    /// Imbalance ratio (max_size / avg_size).
    pub imbalance_ratio: f64,
}

/// Simple greedy partitioner (not full METIS, but a fast BFS-based approximation).
/// Assigns vertices by BFS, trying to keep neighbors together.
pub fn greedy_partition(
    vertex_count: u32,
    edges: &[(u32, u32)],
    partition_count: u32,
    max_imbalance: f64,
) -> MetisPartitionResult {
    let target_size = (vertex_count as f64 / partition_count as f64).ceil() as u32;
    let max_size = (target_size as f64 * (1.0 + max_imbalance)) as u32;

    let mut assignments = vec![u32::MAX; vertex_count as usize];
    let mut partition_sizes = vec![0u32; partition_count as usize];

    // Build adjacency list
    let mut adj: Vec<Vec<u32>> = vec![vec![]; vertex_count as usize];
    for &(src, dst) in edges {
        if (src as usize) < adj.len() && (dst as usize) < adj.len() {
            adj[src as usize].push(dst);
            adj[dst as usize].push(src);
        }
    }

    // BFS from unassigned vertices
    let mut current_partition = 0u32;
    for start in 0..vertex_count {
        if assignments[start as usize] != u32::MAX {
            continue;
        }
        if current_partition >= partition_count {
            current_partition = partition_count - 1;
        }

        let mut queue = VecDeque::new();
        queue.push_back(start);

        while let Some(v) = queue.pop_front() {
            let vi = v as usize;
            if assignments[vi] != u32::MAX {
                continue;
            }
            if partition_sizes[current_partition as usize] >= max_size {
                current_partition = (current_partition + 1).min(partition_count - 1);
            }
            assignments[vi] = current_partition;
            partition_sizes[current_partition as usize] += 1;

            for &neighbor in &adj[vi] {
                if assignments[neighbor as usize] == u32::MAX {
                    queue.push_back(neighbor);
                }
            }
        }
    }

    // Count edge cuts
    let mut edge_cut = 0u64;
    for &(src, dst) in edges {
        if assignments[src as usize] != assignments[dst as usize] {
            edge_cut += 1;
        }
    }

    let avg = vertex_count as f64 / partition_count as f64;
    let max_actual = *partition_sizes.iter().max().unwrap_or(&0) as f64;
    let imbalance_ratio = if avg > 0.0 { max_actual / avg } else { 1.0 };

    MetisPartitionResult {
        assignments,
        edge_cut,
        partition_sizes,
        imbalance_ratio,
    }
}

/// Evaluate partition quality for an existing assignment.
pub fn evaluate_partition(
    edges: &[(u32, u32)],
    assignments: &[u32],
    partition_count: u32,
) -> MetisPartitionResult {
    let mut partition_sizes = vec![0u32; partition_count as usize];
    for &pid in assignments {
        if (pid as usize) < partition_sizes.len() {
            partition_sizes[pid as usize] += 1;
        }
    }
    let mut edge_cut = 0u64;
    for &(src, dst) in edges {
        let si = src as usize;
        let di = dst as usize;
        if si < assignments.len() && di < assignments.len() && assignments[si] != assignments[di] {
            edge_cut += 1;
        }
    }
    let avg = assignments.len() as f64 / partition_count as f64;
    let max_actual = *partition_sizes.iter().max().unwrap_or(&0) as f64;
    let imbalance_ratio = if avg > 0.0 { max_actual / avg } else { 1.0 };
    MetisPartitionResult {
        assignments: assignments.to_vec(),
        edge_cut,
        partition_sizes,
        imbalance_ratio,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_greedy_partition_basic() {
        // 10 vertices, 10 edges, 2 partitions
        let edges: Vec<(u32, u32)> = vec![
            (0, 1),
            (1, 2),
            (2, 3),
            (3, 4),
            (5, 6),
            (6, 7),
            (7, 8),
            (8, 9),
            (0, 4),
            (5, 9),
        ];
        let result = greedy_partition(10, &edges, 2, 0.2);
        assert_eq!(result.assignments.len(), 10);
        assert_eq!(result.partition_sizes.len(), 2);
        // All vertices assigned
        for &a in &result.assignments {
            assert!(a < 2);
        }
        let total: u32 = result.partition_sizes.iter().sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_greedy_partition_chain() {
        // Linear chain: 0-1-2-3-4-5-6-7
        let edges: Vec<(u32, u32)> = (0..7).map(|i| (i, i + 1)).collect();
        let result = greedy_partition(8, &edges, 2, 0.3);
        // BFS from 0 should keep neighbors together
        // Check that vertex 0 and 1 are in the same partition (neighbors)
        assert_eq!(result.assignments[0], result.assignments[1]);
        assert_eq!(result.assignments.len(), 8);
    }

    #[test]
    fn test_greedy_partition_disconnected() {
        // Two disconnected components: {0,1,2,3} and {4,5,6,7}
        let edges: Vec<(u32, u32)> = vec![
            (0, 1),
            (1, 2),
            (2, 3),
            (4, 5),
            (5, 6),
            (6, 7),
        ];
        let result = greedy_partition(8, &edges, 2, 0.1);
        // Component 1 vertices should be in same partition
        let p0 = result.assignments[0];
        assert_eq!(result.assignments[1], p0);
        assert_eq!(result.assignments[2], p0);
        assert_eq!(result.assignments[3], p0);
        // Component 2 vertices should be in same partition
        let p4 = result.assignments[4];
        assert_eq!(result.assignments[5], p4);
        assert_eq!(result.assignments[6], p4);
        assert_eq!(result.assignments[7], p4);
        // Edge cut should be 0 (no edges between components)
        assert_eq!(result.edge_cut, 0);
    }

    #[test]
    fn test_evaluate_partition() {
        let edges: Vec<(u32, u32)> = vec![(0, 1), (1, 2), (2, 3)];
        // Manual assignment: 0,1 in partition 0; 2,3 in partition 1
        let assignments = vec![0, 0, 1, 1];
        let result = evaluate_partition(&edges, &assignments, 2);
        // Edge (1,2) crosses partition boundary
        assert_eq!(result.edge_cut, 1);
        assert_eq!(result.partition_sizes, vec![2, 2]);
    }

    #[test]
    fn test_partition_imbalance() {
        // 4 vertices, all in partition 0
        let assignments = vec![0, 0, 0, 0];
        let edges: Vec<(u32, u32)> = vec![(0, 1)];
        let result = evaluate_partition(&edges, &assignments, 2);
        // avg = 4/2 = 2.0, max = 4, imbalance = 4/2 = 2.0
        assert!((result.imbalance_ratio - 2.0).abs() < f64::EPSILON);
        assert_eq!(result.partition_sizes, vec![4, 0]);
    }
}
