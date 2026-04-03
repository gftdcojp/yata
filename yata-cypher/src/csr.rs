//! CSR (Compressed Sparse Row) adjacency index for O(degree) neighbor lookup.
//! Built once from edges when MemoryGraph is constructed or invalidated.

use std::collections::HashMap;

/// CSR adjacency index for directed graph traversal.
/// Stores outgoing and incoming edge indices per vertex in packed arrays.
#[derive(Clone)]
pub struct CsrIndex {
    /// vid → (start_offset, count) in out_edges array
    out_offsets: HashMap<String, (usize, usize)>,
    /// Packed array: [edge_idx, edge_idx, ...] grouped by source vid
    out_edges: Vec<usize>,
    /// vid → (start_offset, count) in in_edges array
    in_offsets: HashMap<String, (usize, usize)>,
    /// Packed array: [edge_idx, edge_idx, ...] grouped by destination vid
    in_edges: Vec<usize>,
    /// Edge keys (rel IDs) in insertion order, matching the indices used above.
    edge_keys: Vec<String>,
}

impl CsrIndex {
    /// Build CSR from edge list.
    /// `edges` is `[(src_vid, dst_vid, rel_id), ...]` with implicit edge index.
    pub fn build(edges: &[(String, String, String)]) -> Self {
        let mut out_map: HashMap<String, Vec<usize>> = HashMap::new();
        let mut in_map: HashMap<String, Vec<usize>> = HashMap::new();
        let mut edge_keys = Vec::with_capacity(edges.len());

        for (idx, (src, dst, rel_id)) in edges.iter().enumerate() {
            out_map.entry(src.clone()).or_default().push(idx);
            in_map.entry(dst.clone()).or_default().push(idx);
            edge_keys.push(rel_id.clone());
        }

        let (out_offsets, out_edges) = Self::pack(out_map);
        let (in_offsets, in_edges) = Self::pack(in_map);

        Self {
            out_offsets,
            out_edges,
            in_offsets,
            in_edges,
            edge_keys,
        }
    }

    fn pack(map: HashMap<String, Vec<usize>>) -> (HashMap<String, (usize, usize)>, Vec<usize>) {
        let mut offsets = HashMap::with_capacity(map.len());
        let total: usize = map.values().map(|v| v.len()).sum();
        let mut packed = Vec::with_capacity(total);
        for (vid, indices) in map {
            let start = packed.len();
            packed.extend_from_slice(&indices);
            offsets.insert(vid, (start, indices.len()));
        }
        (offsets, packed)
    }

    /// Get outgoing edge indices for a vertex. O(1) lookup + O(degree) iteration.
    pub fn out_edge_indices(&self, vid: &str) -> &[usize] {
        match self.out_offsets.get(vid) {
            Some(&(start, count)) => &self.out_edges[start..start + count],
            None => &[],
        }
    }

    /// Get incoming edge indices for a vertex. O(1) lookup + O(degree) iteration.
    pub fn in_edge_indices(&self, vid: &str) -> &[usize] {
        match self.in_offsets.get(vid) {
            Some(&(start, count)) => &self.in_edges[start..start + count],
            None => &[],
        }
    }

    /// Get edge key (rel ID) by index.
    pub fn edge_key(&self, idx: usize) -> Option<&str> {
        self.edge_keys.get(idx).map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_graph() {
        let csr = CsrIndex::build(&[]);
        assert!(csr.out_edge_indices("a").is_empty());
        assert!(csr.in_edge_indices("a").is_empty());
    }

    #[test]
    fn test_single_edge() {
        let edges = vec![("a".into(), "b".into(), "e1".into())];
        let csr = CsrIndex::build(&edges);
        assert_eq!(csr.out_edge_indices("a"), &[0]);
        assert!(csr.out_edge_indices("b").is_empty());
        assert!(csr.in_edge_indices("a").is_empty());
        assert_eq!(csr.in_edge_indices("b"), &[0]);
        assert_eq!(csr.edge_key(0), Some("e1"));
    }

    #[test]
    fn test_star_topology() {
        // a -> b, a -> c, a -> d
        let edges = vec![
            ("a".into(), "b".into(), "e1".into()),
            ("a".into(), "c".into(), "e2".into()),
            ("a".into(), "d".into(), "e3".into()),
        ];
        let csr = CsrIndex::build(&edges);
        let out = csr.out_edge_indices("a");
        assert_eq!(out.len(), 3);
        // All edges should be reachable
        let keys: Vec<&str> = out.iter().map(|&i| csr.edge_key(i).unwrap()).collect();
        assert!(keys.contains(&"e1"));
        assert!(keys.contains(&"e2"));
        assert!(keys.contains(&"e3"));
    }

    #[test]
    fn test_bidirectional() {
        // a -> b, b -> a
        let edges = vec![
            ("a".into(), "b".into(), "e1".into()),
            ("b".into(), "a".into(), "e2".into()),
        ];
        let csr = CsrIndex::build(&edges);
        assert_eq!(csr.out_edge_indices("a").len(), 1);
        assert_eq!(csr.in_edge_indices("a").len(), 1);
        assert_eq!(csr.out_edge_indices("b").len(), 1);
        assert_eq!(csr.in_edge_indices("b").len(), 1);
    }

    #[test]
    fn test_no_neighbors_for_isolated_vertex() {
        let edges = vec![("a".into(), "b".into(), "e1".into())];
        let csr = CsrIndex::build(&edges);
        assert!(csr.out_edge_indices("isolated").is_empty());
        assert!(csr.in_edge_indices("isolated").is_empty());
    }

    #[test]
    fn test_multiple_edges_between_same_pair() {
        let edges = vec![
            ("a".into(), "b".into(), "e1".into()),
            ("a".into(), "b".into(), "e2".into()),
        ];
        let csr = CsrIndex::build(&edges);
        assert_eq!(csr.out_edge_indices("a").len(), 2);
        assert_eq!(csr.in_edge_indices("b").len(), 2);
    }
}
