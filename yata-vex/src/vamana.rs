use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::distance;
use crate::storage::FlatVectorStore;
use crate::{DistanceMetric, Result, SearchResult, VectorIndex, VexError};
use rand::Rng;
use rand::seq::SliceRandom;

/// Vamana (DiskANN) index configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VamanaConfig {
    /// Maximum out-degree per vertex.
    pub max_degree: usize,
    /// Search beam width during build & query.
    pub search_list_size: usize,
    /// Alpha parameter for robust prune (> 1.0 for diversity).
    pub alpha: f32,
    pub metric: DistanceMetric,
}

impl Default for VamanaConfig {
    fn default() -> Self {
        Self {
            max_degree: 64,
            search_list_size: 128,
            alpha: 1.2,
            metric: DistanceMetric::L2,
        }
    }
}

/// CSR-based adjacency for Vamana proximity graph.
/// Compact representation: neighbors stored contiguously per vertex.
struct CsrGraph {
    /// offsets[i]..offsets[i+1] = neighbor range for vertex i.
    offsets: Vec<usize>,
    /// Flat neighbor array.
    neighbors: Vec<u32>,
    max_degree: usize,
}

impl CsrGraph {
    fn new(n: usize, max_degree: usize) -> Self {
        let mut offsets = Vec::with_capacity(n + 1);
        for i in 0..=n {
            offsets.push(i * max_degree);
        }
        Self {
            offsets,
            neighbors: vec![u32::MAX; n * max_degree],
            max_degree,
        }
    }

    #[inline]
    #[cfg(test)]
    fn degree(&self, v: usize) -> usize {
        let start = self.offsets[v];
        let end = self.offsets[v + 1];
        let slice = &self.neighbors[start..end];
        slice.iter().take_while(|&&n| n != u32::MAX).count()
    }

    #[inline]
    fn get_neighbors(&self, v: usize) -> &[u32] {
        let start = self.offsets[v];
        let end = self.offsets[v + 1];
        let slice = &self.neighbors[start..end];
        let deg = slice.iter().take_while(|&&n| n != u32::MAX).count();
        &slice[..deg]
    }

    fn set_neighbors(&mut self, v: usize, nbrs: &[u32]) {
        let start = self.offsets[v];
        let count = nbrs.len().min(self.max_degree);
        for i in 0..count {
            self.neighbors[start + i] = nbrs[i];
        }
        for i in count..self.max_degree {
            self.neighbors[start + i] = u32::MAX;
        }
    }
}

/// Vamana (DiskANN) index: navigable proximity graph on CSR.
pub struct VamanaIndex {
    graph: CsrGraph,
    vectors: FlatVectorStore,
    entry_point: u32,
    config: VamanaConfig,
}

impl VamanaIndex {
    /// Build Vamana index.
    pub fn build(store: FlatVectorStore, config: VamanaConfig) -> Result<Self> {
        let n = store.len();
        if n == 0 {
            return Err(VexError::EmptyIndex);
        }
        let metric = config.metric;
        let r = config.max_degree;
        let l = config.search_list_size;
        let alpha = config.alpha;

        // Find medoid (approximate: sample 1000 points, pick most central)
        let entry_point = find_medoid(&store, metric);

        // Initialize graph with random neighbors
        let mut graph = CsrGraph::new(n, r);
        {
            let mut rng = rand::thread_rng();
            for i in 0..n {
                let mut nbrs: Vec<u32> = Vec::with_capacity(r);
                while nbrs.len() < r.min(n - 1) {
                    let j = rng.gen_range(0..n) as u32;
                    if j != i as u32 && !nbrs.contains(&j) {
                        nbrs.push(j);
                    }
                }
                graph.set_neighbors(i, &nbrs);
            }
        }

        // Iterative Vamana build: random permutation, greedy search + robust prune
        let mut perm: Vec<usize> = (0..n).collect();
        perm.shuffle(&mut rand::thread_rng());

        for &i in &perm {
            let query = store.get(i);

            // Greedy search from entry point
            let (candidates, _) =
                greedy_search(&graph, &store, query, entry_point as usize, l, metric);

            // Robust prune: select R diverse neighbors from candidates
            let pruned = robust_prune(&store, i, &candidates, alpha, r, metric);
            graph.set_neighbors(i, &pruned);

            // Reverse edges: for each neighbor p of i, add i as neighbor of p
            for &p in &pruned {
                let p = p as usize;
                let mut p_nbrs: Vec<u32> = graph.get_neighbors(p).to_vec();
                if !p_nbrs.contains(&(i as u32)) {
                    p_nbrs.push(i as u32);
                    if p_nbrs.len() > r {
                        // Prune p's neighbors
                        let pruned_p = robust_prune(&store, p, &p_nbrs, alpha, r, metric);
                        graph.set_neighbors(p, &pruned_p);
                    } else {
                        graph.set_neighbors(p, &p_nbrs);
                    }
                }
            }
        }

        Ok(Self {
            graph,
            vectors: store,
            entry_point: entry_point as u32,
            config,
        })
    }

    /// Raw greedy search returning (neighbor_id, distance) pairs.
    fn search_internal(&self, query: &[f32], k: usize, beam: usize) -> Vec<(u32, f32)> {
        let (candidates, _) = greedy_search(
            &self.graph,
            &self.vectors,
            query,
            self.entry_point as usize,
            beam.max(k),
            self.config.metric,
        );
        let mut results: Vec<(u32, f32)> = candidates
            .into_iter()
            .map(|id| {
                let d =
                    distance::distance(query, self.vectors.get(id as usize), self.config.metric);
                (id, d)
            })
            .collect();
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        results.truncate(k);
        results
    }
}

impl VectorIndex for VamanaIndex {
    fn search(&self, query: &[f32], k: usize, _nprobes: usize) -> Result<Vec<SearchResult>> {
        if query.len() != self.vectors.dim() {
            return Err(VexError::DimensionMismatch {
                expected: self.vectors.dim(),
                got: query.len(),
            });
        }
        let results = self.search_internal(query, k, self.config.search_list_size);
        Ok(results
            .into_iter()
            .map(|(id, d)| SearchResult {
                vid: self.vectors.vid(id as usize),
                distance: d,
            })
            .collect())
    }

    fn search_with_filter(
        &self,
        query: &[f32],
        k: usize,
        _nprobes: usize,
        filter: &dyn Fn(u64) -> bool,
    ) -> Result<Vec<SearchResult>> {
        if query.len() != self.vectors.dim() {
            return Err(VexError::DimensionMismatch {
                expected: self.vectors.dim(),
                got: query.len(),
            });
        }
        // Search with larger beam to compensate for filtering
        let beam = self.config.search_list_size * 2;
        let raw = self.search_internal(query, beam, beam);
        let mut results: Vec<SearchResult> = raw
            .into_iter()
            .filter(|(id, _)| filter(self.vectors.vid(*id as usize)))
            .take(k)
            .map(|(id, d)| SearchResult {
                vid: self.vectors.vid(id as usize),
                distance: d,
            })
            .collect();
        results.truncate(k);
        Ok(results)
    }

    fn dim(&self) -> usize {
        self.vectors.dim()
    }

    fn len(&self) -> usize {
        self.vectors.len()
    }

    fn metric(&self) -> DistanceMetric {
        self.config.metric
    }
}

/// Greedy search on the proximity graph.
/// Returns (visited candidates as sorted by distance, visited set).
fn greedy_search(
    graph: &CsrGraph,
    vectors: &FlatVectorStore,
    query: &[f32],
    entry: usize,
    beam_width: usize,
    metric: DistanceMetric,
) -> (Vec<u32>, Vec<bool>) {
    let n = vectors.len();
    let mut visited = vec![false; n];
    let mut candidates: BinaryHeap<Reverse<(OrdF32, u32)>> = BinaryHeap::new();
    let mut best: BinaryHeap<(OrdF32, u32)> = BinaryHeap::new();

    let entry_dist = distance::distance(query, vectors.get(entry), metric);
    candidates.push(Reverse((OrdF32(entry_dist), entry as u32)));
    best.push((OrdF32(entry_dist), entry as u32));
    visited[entry] = true;

    while let Some(Reverse((OrdF32(c_dist), c_id))) = candidates.pop() {
        // If this candidate is worse than the worst in our best-L, stop
        if best.len() >= beam_width {
            if let Some(&(OrdF32(worst), _)) = best.peek() {
                if c_dist > worst {
                    break;
                }
            }
        }

        // Expand neighbors
        for &nbr in graph.get_neighbors(c_id as usize) {
            let nbr_idx = nbr as usize;
            if nbr_idx >= n || visited[nbr_idx] {
                continue;
            }
            visited[nbr_idx] = true;

            let d = distance::distance(query, vectors.get(nbr_idx), metric);

            let should_add = if best.len() < beam_width {
                true
            } else if let Some(&(OrdF32(worst), _)) = best.peek() {
                d < worst
            } else {
                true
            };

            if should_add {
                candidates.push(Reverse((OrdF32(d), nbr)));
                best.push((OrdF32(d), nbr));
                if best.len() > beam_width {
                    best.pop();
                }
            }
        }
    }

    let result: Vec<u32> = best
        .into_sorted_vec()
        .into_iter()
        .map(|(_, id)| id)
        .collect();
    (result, visited)
}

/// Robust prune: select R diverse neighbors from candidates.
/// Alpha > 1 encourages diversity (long-range edges).
fn robust_prune(
    vectors: &FlatVectorStore,
    vertex: usize,
    candidates: &[u32],
    alpha: f32,
    r: usize,
    metric: DistanceMetric,
) -> Vec<u32> {
    if candidates.is_empty() {
        return Vec::new();
    }

    let v = vectors.get(vertex);

    // Sort candidates by distance to vertex
    let mut sorted: Vec<(u32, f32)> = candidates
        .iter()
        .filter(|&&c| c as usize != vertex)
        .map(|&c| {
            let d = distance::distance(v, vectors.get(c as usize), metric);
            (c, d)
        })
        .collect();
    sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    let mut result: Vec<u32> = Vec::with_capacity(r);

    for &(cand, cand_dist) in &sorted {
        if result.len() >= r {
            break;
        }

        // Check if cand is alpha-dominated by any already-selected neighbor
        let dominated = result.iter().any(|&selected| {
            let d_selected_cand = distance::distance(
                vectors.get(selected as usize),
                vectors.get(cand as usize),
                metric,
            );
            alpha * d_selected_cand <= cand_dist
        });

        if !dominated {
            result.push(cand);
        }
    }

    result
}

/// Find approximate medoid by sampling.
fn find_medoid(store: &FlatVectorStore, metric: DistanceMetric) -> usize {
    let n = store.len();
    if n <= 1000 {
        // Exact medoid for small datasets
        let mut best = 0;
        let mut best_total = f32::MAX;
        for i in 0..n {
            let mut total = 0.0f32;
            for j in 0..n {
                total += distance::distance(store.get(i), store.get(j), metric);
            }
            if total < best_total {
                best_total = total;
                best = i;
            }
        }
        return best;
    }

    // Sample 1000 points
    let mut rng = rand::thread_rng();
    let sample_size = 1000usize.min(n);
    let samples: Vec<usize> = (0..sample_size).map(|_| rng.gen_range(0..n)).collect();

    let mut best = samples[0];
    let mut best_total = f32::MAX;
    for &i in &samples {
        let mut total = 0.0f32;
        for &j in &samples {
            total += distance::distance(store.get(i), store.get(j), metric);
        }
        if total < best_total {
            best_total = total;
            best = i;
        }
    }
    best
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdF32(f32);
impl Eq for OrdF32 {}
impl PartialOrd for OrdF32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store(n: usize, dim: usize) -> FlatVectorStore {
        let mut rng = rand::thread_rng();
        let mut store = FlatVectorStore::new(dim);
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|_| rng.r#gen::<f32>()).collect();
            store.push(i as u64, &v);
        }
        store
    }

    #[test]
    fn test_vamana_build_small() {
        let store = make_store(50, 8);
        let config = VamanaConfig {
            max_degree: 8,
            search_list_size: 16,
            alpha: 1.2,
            metric: DistanceMetric::L2,
        };
        let index = VamanaIndex::build(store, config).unwrap();
        assert_eq!(index.len(), 50);
        assert_eq!(index.dim(), 8);
    }

    #[test]
    fn test_vamana_search_self() {
        let store = make_store(200, 16);
        let query = store.get(0).to_vec();
        let config = VamanaConfig {
            max_degree: 16,
            search_list_size: 32,
            alpha: 1.2,
            metric: DistanceMetric::L2,
        };
        let index = VamanaIndex::build(store, config).unwrap();
        let results = index.search(&query, 5, 0).unwrap();
        assert_eq!(results.len(), 5);
        // Self should be nearest
        assert!(
            results[0].vid == 0,
            "expected vid=0, got vid={}",
            results[0].vid
        );
        assert!(results[0].distance < 1e-6);
    }

    #[test]
    fn test_vamana_recall() {
        // Use uniformly distributed data (not clustered) for deterministic recall
        let store = make_store(300, 8);
        let query = store.get(0).to_vec();

        // Brute-force ground truth
        let mut gt: Vec<(u64, f32)> = (0..store.len())
            .map(|i| (store.vid(i), distance::l2_distance(&query, store.get(i))))
            .collect();
        gt.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let gt_top10: Vec<u64> = gt.iter().take(10).map(|x| x.0).collect();

        let config = VamanaConfig {
            max_degree: 32,
            search_list_size: 64,
            alpha: 1.2,
            metric: DistanceMetric::L2,
        };
        let index = VamanaIndex::build(store, config).unwrap();
        let results = index.search(&query, 10, 0).unwrap();
        let found: Vec<u64> = results.iter().map(|r| r.vid).collect();

        // Self (vid=0) should always be found
        assert!(found.contains(&0), "self not found in results");
        // At least some overlap with ground truth
        let recall = gt_top10.iter().filter(|v| found.contains(v)).count();
        assert!(
            recall >= 1,
            "recall@10 = {recall}/10, gt={gt_top10:?}, found={found:?}"
        );
    }

    #[test]
    fn test_vamana_search_with_filter() {
        let store = make_store(200, 8);
        let query = store.get(0).to_vec();
        let config = VamanaConfig {
            max_degree: 16,
            search_list_size: 32,
            alpha: 1.2,
            metric: DistanceMetric::L2,
        };
        let index = VamanaIndex::build(store, config).unwrap();
        let results = index
            .search_with_filter(&query, 5, 0, &|vid| vid % 2 == 0)
            .unwrap();
        for r in &results {
            assert_eq!(r.vid % 2, 0, "filter violated: vid={}", r.vid);
        }
    }

    #[test]
    fn test_vamana_cosine() {
        let store = make_store(100, 8);
        let query = store.get(0).to_vec();
        let config = VamanaConfig {
            max_degree: 8,
            search_list_size: 16,
            alpha: 1.2,
            metric: DistanceMetric::Cosine,
        };
        let index = VamanaIndex::build(store, config).unwrap();
        assert_eq!(index.metric(), DistanceMetric::Cosine);
        let results = index.search(&query, 3, 0).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_vamana_dimension_mismatch() {
        let store = make_store(50, 8);
        let config = VamanaConfig::default();
        let index = VamanaIndex::build(store, config).unwrap();
        let bad = vec![0.0f32; 4];
        assert!(index.search(&bad, 5, 0).is_err());
    }

    #[test]
    fn test_csr_graph_basic() {
        let mut g = CsrGraph::new(3, 4);
        g.set_neighbors(0, &[1, 2]);
        g.set_neighbors(1, &[0]);
        assert_eq!(g.degree(0), 2);
        assert_eq!(g.degree(1), 1);
        assert_eq!(g.degree(2), 0);
        assert_eq!(g.get_neighbors(0), &[1, 2]);
    }
}
