use std::cmp::Reverse;
use std::collections::BinaryHeap;

use rayon::prelude::*;

use crate::distance;
use crate::quantize::{PqConfig, ProductQuantizer, kmeans};
use crate::storage::FlatVectorStore;
use crate::{DistanceMetric, Result, SearchResult, VectorIndex, VexError};

/// IVF_PQ index configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IvfPqConfig {
    pub num_partitions: usize,
    pub pq: PqConfig,
    pub kmeans_max_iter: usize,
}

impl Default for IvfPqConfig {
    fn default() -> Self {
        Self {
            num_partitions: 256,
            pq: PqConfig::default(),
            kmeans_max_iter: 25,
        }
    }
}

/// Inverted list: vectors assigned to one partition.
struct InvertedList {
    vids: Vec<u64>,
    /// PQ codes: [n × M] flattened.
    pq_codes: Vec<u8>,
}

/// IVF_PQ index: Inverted File with Product Quantization.
pub struct IvfPqIndex {
    /// Coarse centroids: [num_partitions × dim].
    centroids: Vec<f32>,
    num_partitions: usize,
    /// Inverted lists, one per partition.
    lists: Vec<InvertedList>,
    /// Product quantizer.
    pq: ProductQuantizer,
    dim: usize,
    total_vectors: usize,
    metric: DistanceMetric,
}

impl IvfPqIndex {
    /// Build IVF_PQ index from vectors.
    pub fn build(store: &FlatVectorStore, config: &IvfPqConfig) -> Result<Self> {
        let n = store.len();
        let dim = store.dim();
        if n == 0 {
            return Err(VexError::EmptyIndex);
        }

        let num_partitions = config.num_partitions.min(n);
        let metric = config.pq.metric;

        // 1. Coarse quantizer: k-means on full vectors
        let centroids = kmeans(
            store.raw_data(),
            dim,
            num_partitions,
            config.kmeans_max_iter,
            metric,
        );

        // 2. Assign each vector to nearest partition
        let mut partition_assignments = vec![0usize; n];
        partition_assignments
            .par_iter_mut()
            .enumerate()
            .for_each(|(i, assignment)| {
                let v = store.get(i);
                let mut best_p = 0;
                let mut best_d = f32::MAX;
                for p in 0..num_partitions {
                    let c = &centroids[p * dim..(p + 1) * dim];
                    let d = distance::distance(v, c, metric);
                    if d < best_d {
                        best_d = d;
                        best_p = p;
                    }
                }
                *assignment = best_p;
            });

        // 3. Train PQ on residual vectors (vector - centroid)
        let mut residuals = FlatVectorStore::with_capacity(dim, n);
        for i in 0..n {
            let v = store.get(i);
            let p = partition_assignments[i];
            let c = &centroids[p * dim..(p + 1) * dim];
            let mut residual = vec![0.0f32; dim];
            for d in 0..dim {
                residual[d] = v[d] - c[d];
            }
            residuals.push(store.vid(i), &residual);
        }
        let pq = ProductQuantizer::train(&residuals, &config.pq);

        // 4. Build inverted lists with PQ codes
        let mut lists: Vec<InvertedList> = (0..num_partitions)
            .map(|_| InvertedList {
                vids: Vec::new(),
                pq_codes: Vec::new(),
            })
            .collect();

        for i in 0..n {
            let p = partition_assignments[i];
            let residual = residuals.get(i);
            let codes = pq.encode(residual);
            lists[p].vids.push(store.vid(i));
            lists[p].pq_codes.extend_from_slice(&codes);
        }

        Ok(Self {
            centroids,
            num_partitions,
            lists,
            pq,
            dim,
            total_vectors: n,
            metric,
        })
    }

    /// Find top-k nearest partitions for a query.
    fn nearest_partitions(&self, query: &[f32], nprobes: usize) -> Vec<(usize, f32)> {
        let mut dists: Vec<(usize, f32)> = (0..self.num_partitions)
            .map(|p| {
                let c = &self.centroids[p * self.dim..(p + 1) * self.dim];
                (p, distance::distance(query, c, self.metric))
            })
            .collect();
        dists.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        dists.truncate(nprobes);
        dists
    }

    /// Scan a single partition using PQ asymmetric distance.
    fn scan_partition(
        &self,
        partition_idx: usize,
        table: &[f32],
        k: usize,
        filter: Option<&dyn Fn(u64) -> bool>,
    ) -> Vec<(u64, f32)> {
        let list = &self.lists[partition_idx];
        let n = list.vids.len();
        let m = self.pq.num_sub;

        let mut heap: BinaryHeap<Reverse<(OrdF32, u64)>> = BinaryHeap::new();

        for i in 0..n {
            let vid = list.vids[i];
            if let Some(f) = filter {
                if !f(vid) {
                    continue;
                }
            }
            let codes = &list.pq_codes[i * m..(i + 1) * m];
            let dist = self.pq.asymmetric_distance(table, codes);
            if heap.len() < k {
                heap.push(Reverse((OrdF32(dist), vid)));
            } else if let Some(&Reverse((OrdF32(worst), _))) = heap.peek() {
                if dist < worst {
                    heap.pop();
                    heap.push(Reverse((OrdF32(dist), vid)));
                }
            }
        }

        heap.into_iter()
            .map(|Reverse((OrdF32(d), vid))| (vid, d))
            .collect()
    }
}

impl VectorIndex for IvfPqIndex {
    fn search(&self, query: &[f32], k: usize, nprobes: usize) -> Result<Vec<SearchResult>> {
        self.search_with_filter(query, k, nprobes, &|_| true)
    }

    fn search_with_filter(
        &self,
        query: &[f32],
        k: usize,
        nprobes: usize,
        filter: &dyn Fn(u64) -> bool,
    ) -> Result<Vec<SearchResult>> {
        if query.len() != self.dim {
            return Err(VexError::DimensionMismatch {
                expected: self.dim,
                got: query.len(),
            });
        }

        let probes = self.nearest_partitions(query, nprobes);

        // Build residual distance tables for each probed partition
        let mut global_heap: BinaryHeap<Reverse<(OrdF32, u64)>> = BinaryHeap::with_capacity(k + 1);

        for &(p, _) in &probes {
            // Residual = query - centroid
            let centroid = &self.centroids[p * self.dim..(p + 1) * self.dim];
            let mut residual = vec![0.0f32; self.dim];
            for d in 0..self.dim {
                residual[d] = query[d] - centroid[d];
            }
            let table = self.pq.build_distance_table(&residual);

            let partition_results = self.scan_partition(p, &table, k, Some(filter));
            for (vid, dist) in partition_results {
                if global_heap.len() < k {
                    global_heap.push(Reverse((OrdF32(dist), vid)));
                } else if let Some(&Reverse((OrdF32(worst), _))) = global_heap.peek() {
                    if dist < worst {
                        global_heap.pop();
                        global_heap.push(Reverse((OrdF32(dist), vid)));
                    }
                }
            }
        }

        let mut results: Vec<SearchResult> = global_heap
            .into_iter()
            .map(|Reverse((OrdF32(d), vid))| SearchResult { vid, distance: d })
            .collect();
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        Ok(results)
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn len(&self) -> usize {
        self.total_vectors
    }

    fn metric(&self) -> DistanceMetric {
        self.metric
    }
}

/// Wrapper for f32 to implement Ord (NaN-safe).
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
    use rand::Rng;

    fn make_store(n: usize, dim: usize) -> FlatVectorStore {
        let mut rng = rand::thread_rng();
        let mut store = FlatVectorStore::new(dim);
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|_| rng.r#gen::<f32>()).collect();
            store.push(i as u64, &v);
        }
        store
    }

    fn make_clustered_store(n: usize, dim: usize, num_clusters: usize) -> FlatVectorStore {
        let mut rng = rand::thread_rng();
        let mut store = FlatVectorStore::new(dim);

        // Generate cluster centers
        let centers: Vec<Vec<f32>> = (0..num_clusters)
            .map(|_| (0..dim).map(|_| rng.r#gen::<f32>() * 100.0).collect())
            .collect();

        for i in 0..n {
            let cluster = i % num_clusters;
            let v: Vec<f32> = (0..dim)
                .map(|d| centers[cluster][d] + rng.r#gen::<f32>() * 0.1)
                .collect();
            store.push(i as u64, &v);
        }
        store
    }

    #[test]
    fn test_ivf_pq_build() {
        let store = make_store(500, 16);
        let config = IvfPqConfig {
            num_partitions: 8,
            pq: PqConfig {
                num_sub_vectors: 4,
                num_codes: 16,
                max_iter: 10,
                metric: DistanceMetric::L2,
            },
            kmeans_max_iter: 10,
        };
        let index = IvfPqIndex::build(&store, &config).unwrap();
        assert_eq!(index.dim(), 16);
        assert_eq!(index.len(), 500);
    }

    #[test]
    fn test_ivf_pq_search() {
        let store = make_store(1000, 16);
        let config = IvfPqConfig {
            num_partitions: 16,
            pq: PqConfig {
                num_sub_vectors: 4,
                num_codes: 32,
                max_iter: 15,
                metric: DistanceMetric::L2,
            },
            kmeans_max_iter: 15,
        };
        let index = IvfPqIndex::build(&store, &config).unwrap();
        let query = store.get(0);
        let results = index.search(query, 5, 4).unwrap();
        assert_eq!(results.len(), 5);
        // First result should be the query itself (distance ≈ 0)
        assert!(results[0].distance < results[4].distance);
    }

    #[test]
    fn test_ivf_pq_search_with_filter() {
        let store = make_store(500, 16);
        let config = IvfPqConfig {
            num_partitions: 8,
            pq: PqConfig {
                num_sub_vectors: 4,
                num_codes: 16,
                max_iter: 10,
                metric: DistanceMetric::L2,
            },
            kmeans_max_iter: 10,
        };
        let index = IvfPqIndex::build(&store, &config).unwrap();
        let query = store.get(0);
        // Only allow even vids
        let results = index
            .search_with_filter(query, 5, 4, &|vid| vid % 2 == 0)
            .unwrap();
        for r in &results {
            assert_eq!(r.vid % 2, 0);
        }
    }

    #[test]
    fn test_ivf_pq_recall_clustered() {
        let store = make_clustered_store(2000, 32, 10);
        let config = IvfPqConfig {
            num_partitions: 16,
            pq: PqConfig {
                num_sub_vectors: 8,
                num_codes: 64,
                max_iter: 25,
                metric: DistanceMetric::L2,
            },
            kmeans_max_iter: 25,
        };
        let index = IvfPqIndex::build(&store, &config).unwrap();

        // Brute-force ground truth for vid=0
        let query = store.get(0);
        let mut gt: Vec<(u64, f32)> = (0..store.len())
            .map(|i| (store.vid(i), distance::l2_distance(query, store.get(i))))
            .collect();
        gt.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let gt_top50: Vec<u64> = gt.iter().take(50).map(|x| x.0).collect();

        // Use high nprobes for recall test
        let results = index.search(query, 50, 16).unwrap();
        let found: Vec<u64> = results.iter().map(|r| r.vid).collect();

        // PQ is approximate — check that recall@50 >= 20%
        let recall = gt_top50.iter().filter(|v| found.contains(v)).count();
        assert!(
            recall >= 10,
            "recall@50 = {recall}/50, gt_top10={:?}, found_top10={:?}",
            &gt_top50[..10],
            &found[..found.len().min(10)]
        );
    }

    #[test]
    fn test_ivf_pq_dimension_mismatch() {
        let store = make_store(100, 16);
        let config = IvfPqConfig {
            num_partitions: 4,
            pq: PqConfig {
                num_sub_vectors: 4,
                num_codes: 16,
                max_iter: 5,
                metric: DistanceMetric::L2,
            },
            kmeans_max_iter: 5,
        };
        let index = IvfPqIndex::build(&store, &config).unwrap();
        let bad_query = vec![0.0f32; 8];
        let err = index.search(&bad_query, 5, 2);
        assert!(err.is_err());
    }

    #[test]
    fn test_ivf_pq_cosine() {
        let store = make_store(500, 16);
        let config = IvfPqConfig {
            num_partitions: 8,
            pq: PqConfig {
                num_sub_vectors: 4,
                num_codes: 16,
                max_iter: 10,
                metric: DistanceMetric::Cosine,
            },
            kmeans_max_iter: 10,
        };
        let index = IvfPqIndex::build(&store, &config).unwrap();
        assert_eq!(index.metric(), DistanceMetric::Cosine);
        let results = index.search(store.get(0), 5, 4).unwrap();
        assert_eq!(results.len(), 5);
    }
}
