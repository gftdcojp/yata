use crate::DistanceMetric;

#[inline]
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    l2_distance_auto(a, b)
}

#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let (mut dot, mut norm_a, mut norm_b) = (0.0f32, 0.0f32, 0.0f32);
    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    let denom = (norm_a * norm_b).sqrt();
    if denom < f32::EPSILON {
        return 1.0;
    }
    1.0 - dot / denom
}

#[inline]
pub fn dot_product_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut dot = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
    }
    -dot
}

#[inline]
pub fn distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::L2 => l2_distance(a, b),
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::DotProduct => dot_product_distance(a, b),
    }
}

/// Batch L2 distances: query vs N vectors stored contiguously as [N × dim].
/// Returns Vec of (index, distance).
pub fn batch_l2_distances(query: &[f32], vectors: &[f32], dim: usize) -> Vec<f32> {
    let n = vectors.len() / dim;
    let mut dists = Vec::with_capacity(n);
    for i in 0..n {
        let v = &vectors[i * dim..(i + 1) * dim];
        dists.push(l2_distance_auto(query, v));
    }
    dists
}

/// Batch distances with arbitrary metric.
pub fn batch_distances(
    query: &[f32],
    vectors: &[f32],
    dim: usize,
    metric: DistanceMetric,
) -> Vec<f32> {
    let n = vectors.len() / dim;
    let mut dists = Vec::with_capacity(n);
    for i in 0..n {
        let v = &vectors[i * dim..(i + 1) * dim];
        dists.push(distance(query, v, metric));
    }
    dists
}

// --- SIMD-accelerated L2 ---

#[cfg(target_arch = "x86_64")]
#[inline]
fn l2_distance_auto(a: &[f32], b: &[f32]) -> f32 {
    if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
        unsafe { l2_avx2_fma(a, b) }
    } else {
        l2_scalar(a, b)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn l2_distance_auto(a: &[f32], b: &[f32]) -> f32 {
    unsafe { l2_neon(a, b) }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline]
fn l2_distance_auto(a: &[f32], b: &[f32]) -> f32 {
    l2_scalar(a, b)
}

#[inline]
#[allow(dead_code)]
fn l2_scalar(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn l2_avx2_fma(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let n = a.len();
    let chunks = n / 8;
    let rem = n % 8;

    let mut acc = _mm256_setzero_ps();
    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();

    for i in 0..chunks {
        let off = i * 8;
        let va = _mm256_loadu_ps(a_ptr.add(off));
        let vb = _mm256_loadu_ps(b_ptr.add(off));
        let diff = _mm256_sub_ps(va, vb);
        acc = _mm256_fmadd_ps(diff, diff, acc);
    }

    // horizontal sum
    let hi = _mm256_extractf128_ps(acc, 1);
    let lo = _mm256_castps256_ps128(acc);
    let sum128 = _mm_add_ps(lo, hi);
    let shuf = _mm_movehdup_ps(sum128);
    let sums = _mm_add_ps(sum128, shuf);
    let shuf2 = _mm_movehl_ps(sums, sums);
    let sums2 = _mm_add_ss(sums, shuf2);
    let mut result = _mm_cvtss_f32(sums2);

    // remainder
    let base = chunks * 8;
    for i in 0..rem {
        let d = a[base + i] - b[base + i];
        result += d * d;
    }
    result
}

#[cfg(target_arch = "aarch64")]
unsafe fn l2_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    let n = a.len();
    let chunks = n / 4;
    let rem = n % 4;

    let a_ptr = a.as_ptr();
    let b_ptr = b.as_ptr();

    unsafe {
        let mut acc = vdupq_n_f32(0.0);

        for i in 0..chunks {
            let off = i * 4;
            let va = vld1q_f32(a_ptr.add(off));
            let vb = vld1q_f32(b_ptr.add(off));
            let diff = vsubq_f32(va, vb);
            acc = vfmaq_f32(acc, diff, diff);
        }

        let mut result = vaddvq_f32(acc);
        let base = chunks * 4;
        for i in 0..rem {
            let d = a[base + i] - b[base + i];
            result += d * d;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_distance() {
        let a = [1.0, 2.0, 3.0, 4.0];
        let b = [5.0, 6.0, 7.0, 8.0];
        let d = l2_distance(&a, &b);
        assert!((d - 64.0).abs() < 1e-4);
    }

    #[test]
    fn test_l2_zero() {
        let a = [1.0, 2.0, 3.0];
        assert!(l2_distance(&a, &a) < 1e-6);
    }

    #[test]
    fn test_cosine_identical() {
        let a = [1.0, 0.0, 0.0];
        assert!(cosine_distance(&a, &a) < 1e-6);
    }

    #[test]
    fn test_cosine_orthogonal() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        assert!((cosine_distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_dot_product() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 5.0, 6.0];
        assert!((dot_product_distance(&a, &b) - (-32.0)).abs() < 1e-6);
    }

    #[test]
    fn test_batch_l2() {
        let query = [0.0, 0.0];
        let vectors = [1.0, 0.0, 0.0, 1.0, 1.0, 1.0];
        let dists = batch_l2_distances(&query, &vectors, 2);
        assert_eq!(dists.len(), 3);
        assert!((dists[0] - 1.0).abs() < 1e-6);
        assert!((dists[1] - 1.0).abs() < 1e-6);
        assert!((dists[2] - 2.0).abs() < 1e-6);
    }

    #[test]
    fn test_l2_high_dim() {
        let dim = 768;
        let a: Vec<f32> = (0..dim).map(|i| i as f32 * 0.01).collect();
        let b: Vec<f32> = (0..dim).map(|i| (i as f32 + 1.0) * 0.01).collect();
        let d = l2_distance(&a, &b);
        // each diff = 0.01, sum = 768 * 0.0001 = 0.0768
        assert!((d - 0.0768).abs() < 1e-3);
    }

    #[test]
    fn test_simd_matches_scalar() {
        let dim = 33; // non-aligned
        let a: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.1).collect();
        let b: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.2 + 0.5).collect();
        let simd = l2_distance(&a, &b);
        let scalar = l2_scalar(&a, &b);
        assert!((simd - scalar).abs() < 1e-3, "simd={simd} scalar={scalar}");
    }
}
