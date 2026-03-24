use rand::Rng;
use rayon::prelude::*;

use crate::DistanceMetric;
use crate::distance;
use crate::storage::FlatVectorStore;

/// k-means clustering on flat f32 vectors.
/// Returns centroids as [k × dim] contiguous f32.
pub fn kmeans(
    vectors: &[f32],
    dim: usize,
    k: usize,
    max_iter: usize,
    metric: DistanceMetric,
) -> Vec<f32> {
    let n = vectors.len() / dim;
    if n == 0 || k == 0 {
        return Vec::new();
    }
    let k = k.min(n);

    // k-means++ initialization
    let mut centroids = kmeans_pp_init(vectors, dim, k, metric);
    let mut assignments = vec![0u32; n];

    for _ in 0..max_iter {
        // Assignment step (parallel)
        let changed = assign_parallel(vectors, dim, &centroids, k, &mut assignments, metric);

        // Update step
        update_centroids(vectors, dim, k, &assignments, &mut centroids);

        if !changed {
            break;
        }
    }

    centroids
}

/// k-means++ initialization: pick k centroids with probability proportional to distance.
fn kmeans_pp_init(vectors: &[f32], dim: usize, k: usize, metric: DistanceMetric) -> Vec<f32> {
    let n = vectors.len() / dim;
    let mut rng = rand::thread_rng();
    let mut centroids = Vec::with_capacity(k * dim);

    // First centroid: random
    let idx = rng.gen_range(0..n);
    centroids.extend_from_slice(&vectors[idx * dim..(idx + 1) * dim]);

    let mut min_dists = vec![f32::MAX; n];

    for c in 1..k {
        // Update min distances to nearest centroid
        let new_centroid = &centroids[(c - 1) * dim..c * dim];
        for i in 0..n {
            let v = &vectors[i * dim..(i + 1) * dim];
            let d = distance::distance(v, new_centroid, metric);
            if d < min_dists[i] {
                min_dists[i] = d;
            }
        }

        // Weighted random selection
        let total: f64 = min_dists.iter().map(|&d| d as f64).sum();
        if total < f64::EPSILON {
            // All points are at centroids; pick random
            let idx = rng.gen_range(0..n);
            centroids.extend_from_slice(&vectors[idx * dim..(idx + 1) * dim]);
            continue;
        }

        let threshold = rng.r#gen::<f64>() * total;
        let mut cumulative = 0.0f64;
        let mut chosen = n - 1;
        for i in 0..n {
            cumulative += min_dists[i] as f64;
            if cumulative >= threshold {
                chosen = i;
                break;
            }
        }
        centroids.extend_from_slice(&vectors[chosen * dim..(chosen + 1) * dim]);
    }

    centroids
}

/// Parallel assignment: returns true if any assignment changed.
fn assign_parallel(
    vectors: &[f32],
    dim: usize,
    centroids: &[f32],
    k: usize,
    assignments: &mut [u32],
    metric: DistanceMetric,
) -> bool {
    use std::sync::atomic::{AtomicBool, Ordering};
    let changed = AtomicBool::new(false);

    assignments
        .par_iter_mut()
        .enumerate()
        .for_each(|(i, assignment)| {
            let v = &vectors[i * dim..(i + 1) * dim];
            let mut best_k = 0u32;
            let mut best_d = f32::MAX;
            for c in 0..k {
                let centroid = &centroids[c * dim..(c + 1) * dim];
                let d = distance::distance(v, centroid, metric);
                if d < best_d {
                    best_d = d;
                    best_k = c as u32;
                }
            }
            if *assignment != best_k {
                *assignment = best_k;
                changed.store(true, Ordering::Relaxed);
            }
        });

    changed.load(Ordering::Relaxed)
}

/// Recompute centroids from assignments.
fn update_centroids(
    vectors: &[f32],
    dim: usize,
    k: usize,
    assignments: &[u32],
    centroids: &mut Vec<f32>,
) {
    let mut sums = vec![0.0f64; k * dim];
    let mut counts = vec![0u64; k];

    for (i, &assignment) in assignments.iter().enumerate() {
        let c = assignment as usize;
        counts[c] += 1;
        for d in 0..dim {
            sums[c * dim + d] += vectors[i * dim + d] as f64;
        }
    }

    for c in 0..k {
        if counts[c] == 0 {
            continue;
        }
        let inv = 1.0 / counts[c] as f64;
        for d in 0..dim {
            centroids[c * dim + d] = (sums[c * dim + d] * inv) as f32;
        }
    }
}

/// Product Quantizer: splits D-dimensional vectors into M sub-vectors,
/// each quantized to 256 codes via k-means.
pub struct ProductQuantizer {
    pub dim: usize,
    pub num_sub: usize,
    pub sub_dim: usize,
    /// Codebooks: [M][256 × sub_dim] flattened.
    pub codebooks: Vec<Vec<f32>>,
    pub metric: DistanceMetric,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PqConfig {
    pub num_sub_vectors: usize,
    pub num_codes: usize, // typically 256
    pub max_iter: usize,
    pub metric: DistanceMetric,
}

impl Default for PqConfig {
    fn default() -> Self {
        Self {
            num_sub_vectors: 8,
            num_codes: 256,
            max_iter: 25,
            metric: DistanceMetric::L2,
        }
    }
}

impl ProductQuantizer {
    /// Train PQ codebooks from training vectors.
    pub fn train(store: &FlatVectorStore, config: &PqConfig) -> Self {
        let dim = store.dim();
        let m = config.num_sub_vectors;
        let sub_dim = dim / m;
        assert_eq!(
            dim % m,
            0,
            "dim ({dim}) must be divisible by num_sub_vectors ({m})"
        );

        let n = store.len();
        let raw = store.raw_data();

        // Train each sub-quantizer independently (parallel across sub-vectors)
        let codebooks: Vec<Vec<f32>> = (0..m)
            .into_par_iter()
            .map(|s| {
                // Extract sub-vectors for sub-quantizer s
                let mut sub_vecs = Vec::with_capacity(n * sub_dim);
                for i in 0..n {
                    let base = i * dim + s * sub_dim;
                    sub_vecs.extend_from_slice(&raw[base..base + sub_dim]);
                }
                kmeans(
                    &sub_vecs,
                    sub_dim,
                    config.num_codes,
                    config.max_iter,
                    config.metric,
                )
            })
            .collect();

        Self {
            dim,
            num_sub: m,
            sub_dim,
            codebooks,
            metric: config.metric,
        }
    }

    /// Encode a single vector into M PQ codes (each 0..255).
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let mut codes = Vec::with_capacity(self.num_sub);
        for s in 0..self.num_sub {
            let sub = &vector[s * self.sub_dim..(s + 1) * self.sub_dim];
            let codebook = &self.codebooks[s];
            let num_codes = codebook.len() / self.sub_dim;
            let mut best_code = 0u8;
            let mut best_dist = f32::MAX;
            for c in 0..num_codes {
                let centroid = &codebook[c * self.sub_dim..(c + 1) * self.sub_dim];
                let d = distance::distance(sub, centroid, self.metric);
                if d < best_dist {
                    best_dist = d;
                    best_code = c as u8;
                }
            }
            codes.push(best_code);
        }
        codes
    }

    /// Encode all vectors in a store. Returns [n × M] codes.
    pub fn encode_batch(&self, store: &FlatVectorStore) -> Vec<u8> {
        let n = store.len();
        let raw = store.raw_data();
        let dim = self.dim;

        (0..n)
            .into_par_iter()
            .flat_map(|i| {
                let vec = &raw[i * dim..(i + 1) * dim];
                self.encode(vec)
            })
            .collect()
    }

    /// Build asymmetric distance table for a query vector.
    /// Returns [M × num_codes] distances.
    pub fn build_distance_table(&self, query: &[f32]) -> Vec<f32> {
        let num_codes = self.codebooks[0].len() / self.sub_dim;
        let mut table = Vec::with_capacity(self.num_sub * num_codes);
        for s in 0..self.num_sub {
            let sub_query = &query[s * self.sub_dim..(s + 1) * self.sub_dim];
            let codebook = &self.codebooks[s];
            for c in 0..num_codes {
                let centroid = &codebook[c * self.sub_dim..(c + 1) * self.sub_dim];
                table.push(distance::distance(sub_query, centroid, self.metric));
            }
        }
        table
    }

    /// Compute asymmetric distance from pre-built table and PQ codes.
    #[inline]
    pub fn asymmetric_distance(&self, table: &[f32], codes: &[u8]) -> f32 {
        let num_codes = self.codebooks[0].len() / self.sub_dim;
        let mut dist = 0.0f32;
        for s in 0..self.num_sub {
            dist += table[s * num_codes + codes[s] as usize];
        }
        dist
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
    fn test_kmeans_basic() {
        let vectors = vec![0.0, 0.0, 0.1, 0.1, 10.0, 10.0, 10.1, 10.1];
        let centroids = kmeans(&vectors, 2, 2, 10, DistanceMetric::L2);
        assert_eq!(centroids.len(), 4); // 2 centroids × 2 dim
        // Centroids should be near (0.05, 0.05) and (10.05, 10.05)
        let c0 = &centroids[0..2];
        let c1 = &centroids[2..4];
        let (low, high) = if c0[0] < c1[0] { (c0, c1) } else { (c1, c0) };
        assert!(low[0] < 1.0);
        assert!(high[0] > 9.0);
    }

    #[test]
    fn test_pq_encode_decode() {
        let store = make_store(200, 16);
        let config = PqConfig {
            num_sub_vectors: 4,
            num_codes: 16,
            max_iter: 10,
            metric: DistanceMetric::L2,
        };
        let pq = ProductQuantizer::train(&store, &config);
        assert_eq!(pq.num_sub, 4);
        assert_eq!(pq.sub_dim, 4);

        let codes = pq.encode(store.get(0));
        assert_eq!(codes.len(), 4);

        let batch_codes = pq.encode_batch(&store);
        assert_eq!(batch_codes.len(), 200 * 4);
    }

    #[test]
    fn test_pq_distance_table() {
        let store = make_store(100, 8);
        let config = PqConfig {
            num_sub_vectors: 2,
            num_codes: 16,
            max_iter: 10,
            metric: DistanceMetric::L2,
        };
        let pq = ProductQuantizer::train(&store, &config);

        let query: Vec<f32> = (0..8).map(|i| i as f32 * 0.1).collect();
        let table = pq.build_distance_table(&query);
        assert_eq!(table.len(), 2 * 16); // M × num_codes

        let codes = pq.encode(store.get(0));
        let dist = pq.asymmetric_distance(&table, &codes);
        assert!(dist >= 0.0);
    }

    #[test]
    fn test_pq_asymmetric_distance_ordering() {
        let dim = 16;
        let mut store = FlatVectorStore::new(dim);
        // Add query-like vector and distant vector
        let query: Vec<f32> = vec![0.0; dim];
        let near: Vec<f32> = vec![0.01; dim];
        let far: Vec<f32> = vec![10.0; dim];
        store.push(0, &near);
        store.push(1, &far);
        // Add training data
        let mut rng = rand::thread_rng();
        for i in 2..200 {
            let v: Vec<f32> = (0..dim).map(|_| rng.r#gen::<f32>() * 20.0 - 10.0).collect();
            store.push(i, &v);
        }

        let config = PqConfig {
            num_sub_vectors: 4,
            num_codes: 32,
            max_iter: 15,
            metric: DistanceMetric::L2,
        };
        let pq = ProductQuantizer::train(&store, &config);
        let table = pq.build_distance_table(&query);

        let codes_near = pq.encode(&near);
        let codes_far = pq.encode(&far);
        let dist_near = pq.asymmetric_distance(&table, &codes_near);
        let dist_far = pq.asymmetric_distance(&table, &codes_far);
        assert!(dist_near < dist_far, "near={dist_near} far={dist_far}");
    }
}
