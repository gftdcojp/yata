//! Graph Query Coordinator — routes queries to label-partitioned shards and merges results.
//!
//! Architecture:
//! - ShardedGraphStore holds N MutableCsrStore instances (one per label group)
//! - Coordinator routes queries based on label hints to relevant shards
//! - Results are merged from all queried shards
//! - Cross-shard edges handled via a "default" shard

use std::collections::HashMap;
use yata_grin::*;
use yata_store::MutableCsrStore;

/// A sharded graph store with label-based partitioning.
pub struct ShardedGraphStore {
    /// Label -> shard_id mapping.
    label_to_shard: HashMap<String, usize>,
    /// Shards (each is a MutableCsrStore for a label group).
    shards: Vec<MutableCsrStore>,
    /// Default shard index (for unlabeled vertices and cross-shard edges).
    default_shard: usize,
}

impl ShardedGraphStore {
    /// Create with a single shard (equivalent to non-partitioned).
    pub fn single() -> Self {
        Self {
            label_to_shard: HashMap::new(),
            shards: vec![MutableCsrStore::new()],
            default_shard: 0,
        }
    }

    /// Create with label-based partitioning.
    /// Each label group gets its own shard. Unlabeled data goes to default.
    pub fn with_label_partitions(label_groups: &[Vec<String>]) -> Self {
        let mut label_to_shard = HashMap::new();
        let mut shards = Vec::new();

        for (i, group) in label_groups.iter().enumerate() {
            shards.push(MutableCsrStore::new_partition(i as u32));
            for label in group {
                label_to_shard.insert(label.clone(), i);
            }
        }
        // Default shard for unlabeled / cross-shard data
        let default_id = shards.len();
        shards.push(MutableCsrStore::new_partition(default_id as u32));

        Self {
            label_to_shard,
            shards,
            default_shard: default_id,
        }
    }

    /// Get the shard for a given label.
    pub fn shard_for_label(&self, label: &str) -> &MutableCsrStore {
        let idx = self
            .label_to_shard
            .get(label)
            .copied()
            .unwrap_or(self.default_shard);
        &self.shards[idx]
    }

    /// Get mutable shard for a given label.
    pub fn shard_for_label_mut(&mut self, label: &str) -> &mut MutableCsrStore {
        let idx = self
            .label_to_shard
            .get(label)
            .copied()
            .unwrap_or(self.default_shard);
        &mut self.shards[idx]
    }

    /// Get shard indices relevant to a set of labels. If empty, return all shards.
    pub fn route(&self, labels: &[String]) -> Vec<usize> {
        if labels.is_empty() {
            return (0..self.shards.len()).collect();
        }
        let mut shard_ids: Vec<usize> = labels
            .iter()
            .filter_map(|l| self.label_to_shard.get(l).copied())
            .collect();
        shard_ids.sort_unstable();
        shard_ids.dedup();
        // Always include default shard for cross-shard edges
        if !shard_ids.contains(&self.default_shard) {
            shard_ids.push(self.default_shard);
        }
        shard_ids
    }

    /// Get all shards.
    pub fn all_shards(&self) -> &[MutableCsrStore] {
        &self.shards
    }

    /// Get shard by index.
    pub fn shard(&self, idx: usize) -> Option<&MutableCsrStore> {
        self.shards.get(idx)
    }

    /// Get mutable shard by index.
    pub fn shard_mut(&mut self, idx: usize) -> Option<&mut MutableCsrStore> {
        self.shards.get_mut(idx)
    }

    /// Number of shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Default shard index.
    pub fn default_shard_idx(&self) -> usize {
        self.default_shard
    }

    /// Total vertex count across all shards.
    pub fn total_vertex_count(&self) -> usize {
        self.shards.iter().map(|s| s.vertex_count()).sum()
    }

    /// Total edge count across all shards.
    pub fn total_edge_count(&self) -> usize {
        self.shards.iter().map(|s| s.edge_count()).sum()
    }

    /// Add a vertex to the appropriate shard based on its label.
    /// Returns (shard_index, vid_within_shard).
    pub fn add_vertex(&mut self, label: &str, props: &[(&str, PropValue)]) -> (usize, u32) {
        let shard_idx = self
            .label_to_shard
            .get(label)
            .copied()
            .unwrap_or(self.default_shard);
        let vid = self.shards[shard_idx].add_vertex(label, props);
        (shard_idx, vid)
    }

    /// Add an edge within a specific shard. src and dst are VIDs local to that shard.
    pub fn add_edge(
        &mut self,
        shard_idx: usize,
        src: u32,
        dst: u32,
        label: &str,
        props: &[(&str, PropValue)],
    ) -> u32 {
        self.shards[shard_idx].add_edge(src, dst, label, props)
    }

    /// Commit all shards (rebuild CSR in each).
    pub fn commit_all(&mut self) {
        for shard in &mut self.shards {
            shard.commit();
        }
    }

    /// Scan vertices by label across relevant shards.
    /// Returns Vec<(shard_index, vid)>.
    pub fn scan_vertices_by_label(&self, label: &str) -> Vec<(usize, u32)> {
        let shard_ids = self.route(&[label.to_string()]);
        let mut results = Vec::new();
        for &sid in &shard_ids {
            for vid in self.shards[sid].scan_vertices_by_label(label) {
                results.push((sid, vid));
            }
        }
        results
    }

    /// Scan vertices with predicate across relevant shards.
    /// Returns Vec<(shard_index, vid)>.
    pub fn scan_vertices_filtered(
        &self,
        label: &str,
        pred: &Predicate,
    ) -> Vec<(usize, u32)> {
        let shard_ids = self.route(&[label.to_string()]);
        let mut results = Vec::new();
        for &sid in &shard_ids {
            for vid in self.shards[sid].scan_vertices(label, pred) {
                results.push((sid, vid));
            }
        }
        results
    }
}

// --- GRIN trait implementations for ShardedGraphStore ---

impl Topology for ShardedGraphStore {
    fn vertex_count(&self) -> usize {
        self.total_vertex_count()
    }

    fn edge_count(&self) -> usize {
        self.total_edge_count()
    }

    fn has_vertex(&self, vid: u32) -> bool {
        self.shards.iter().any(|s| s.has_vertex(vid))
    }

    fn out_degree(&self, vid: u32) -> usize {
        for shard in &self.shards {
            if shard.has_vertex(vid) {
                return shard.out_degree(vid);
            }
        }
        0
    }

    fn in_degree(&self, vid: u32) -> usize {
        for shard in &self.shards {
            if shard.has_vertex(vid) {
                return shard.in_degree(vid);
            }
        }
        0
    }

    fn out_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        for shard in &self.shards {
            if shard.has_vertex(vid) {
                return shard.out_neighbors(vid);
            }
        }
        Vec::new()
    }

    fn in_neighbors(&self, vid: u32) -> Vec<Neighbor> {
        for shard in &self.shards {
            if shard.has_vertex(vid) {
                return shard.in_neighbors(vid);
            }
        }
        Vec::new()
    }

    fn out_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        for shard in &self.shards {
            if shard.has_vertex(vid) {
                return shard.out_neighbors_by_label(vid, edge_label);
            }
        }
        Vec::new()
    }

    fn in_neighbors_by_label(&self, vid: u32, edge_label: &str) -> Vec<Neighbor> {
        for shard in &self.shards {
            if shard.has_vertex(vid) {
                return shard.in_neighbors_by_label(vid, edge_label);
            }
        }
        Vec::new()
    }
}

impl Property for ShardedGraphStore {
    fn vertex_labels(&self, vid: u32) -> Vec<String> {
        for shard in &self.shards {
            if shard.has_vertex(vid) {
                return Property::vertex_labels(shard, vid);
            }
        }
        Vec::new()
    }

    fn vertex_prop(&self, vid: u32, key: &str) -> Option<PropValue> {
        for shard in &self.shards {
            if shard.has_vertex(vid) {
                return shard.vertex_prop(vid, key);
            }
        }
        None
    }

    fn edge_prop(&self, edge_id: u32, key: &str) -> Option<PropValue> {
        for shard in &self.shards {
            let result = shard.edge_prop(edge_id, key);
            if result.is_some() {
                return result;
            }
        }
        None
    }

    fn vertex_prop_keys(&self, label: &str) -> Vec<String> {
        let shard = self.shard_for_label(label);
        shard.vertex_prop_keys(label)
    }

    fn edge_prop_keys(&self, label: &str) -> Vec<String> {
        // Aggregate from all shards
        let mut keys = Vec::new();
        for shard in &self.shards {
            for k in shard.edge_prop_keys(label) {
                if !keys.contains(&k) {
                    keys.push(k);
                }
            }
        }
        keys
    }
}

impl Schema for ShardedGraphStore {
    fn vertex_labels(&self) -> Vec<String> {
        let mut labels = Vec::new();
        for shard in &self.shards {
            for l in Schema::vertex_labels(shard) {
                if !labels.contains(&l) {
                    labels.push(l);
                }
            }
        }
        labels
    }

    fn edge_labels(&self) -> Vec<String> {
        let mut labels = Vec::new();
        for shard in &self.shards {
            for l in shard.edge_labels() {
                if !labels.contains(&l) {
                    labels.push(l);
                }
            }
        }
        labels
    }

    fn vertex_primary_key(&self, label: &str) -> Option<String> {
        let shard = self.shard_for_label(label);
        shard.vertex_primary_key(label)
    }
}

impl Scannable for ShardedGraphStore {
    fn scan_vertices(&self, label: &str, predicate: &Predicate) -> Vec<u32> {
        let shard_ids = self.route(&[label.to_string()]);
        let mut results = Vec::new();
        for &sid in &shard_ids {
            results.extend(self.shards[sid].scan_vertices(label, predicate));
        }
        results
    }

    fn scan_vertices_by_label(&self, label: &str) -> Vec<u32> {
        let shard_ids = self.route(&[label.to_string()]);
        let mut results = Vec::new();
        for &sid in &shard_ids {
            results.extend(self.shards[sid].scan_vertices_by_label(label));
        }
        results
    }

    fn scan_all_vertices(&self) -> Vec<u32> {
        let mut results = Vec::new();
        for shard in &self.shards {
            results.extend(shard.scan_all_vertices());
        }
        results
    }
}

// --- Sharded query execution ---

/// Execute a GIE query plan across partitioned shards.
/// Routes Scan ops to relevant shards based on label hints, merges results.
pub fn execute_sharded(
    plan: &yata_gie::ir::QueryPlan,
    store: &ShardedGraphStore,
    labels: &[String],
) -> Vec<yata_gie::executor::Record> {
    let shard_ids = store.route(labels);

    if shard_ids.len() == 1 {
        // Single shard: direct execution
        let shard = store.shard(shard_ids[0]).unwrap();
        return yata_gie::executor::execute(plan, shard);
    }

    // Multi-shard: execute on each, merge results
    let mut all_results = Vec::new();
    for &sid in &shard_ids {
        let shard = store.shard(sid).unwrap();
        let mut results = yata_gie::executor::execute(plan, shard);
        all_results.append(&mut results);
    }
    all_results
}

/// Execute a GIE plan across shards with proper aggregation result merging.
/// If the plan ends with an Aggregate op, shard-local results are merged
/// (count/sum → sum, min → global min, max → global max).
pub fn execute_sharded_merged(
    plan: &yata_gie::ir::QueryPlan,
    store: &ShardedGraphStore,
    labels: &[String],
) -> Vec<yata_gie::executor::Record> {
    let shard_ids = store.route(labels);

    if shard_ids.len() == 1 {
        let shard = store.shard(shard_ids[0]).unwrap();
        return yata_gie::executor::execute(plan, shard);
    }

    // Check if plan ends with aggregation
    let has_final_agg = plan.ops.last().map_or(false, |op| {
        matches!(op, yata_gie::ir::LogicalOp::Aggregate { .. })
    });

    // Execute on each shard
    let mut shard_results: Vec<Vec<yata_gie::executor::Record>> = Vec::new();
    for &sid in &shard_ids {
        let shard = store.shard(sid).unwrap();
        let results = yata_gie::executor::execute(plan, shard);
        shard_results.push(results);
    }

    if has_final_agg {
        merge_aggregation_results(shard_results, plan)
    } else {
        shard_results.into_iter().flatten().collect()
    }
}

/// Merge aggregation results from multiple shards.
fn merge_aggregation_results(
    shard_results: Vec<Vec<yata_gie::executor::Record>>,
    plan: &yata_gie::ir::QueryPlan,
) -> Vec<yata_gie::executor::Record> {
    use yata_gie::ir::{AggOp, LogicalOp};

    let agg_op = match plan.ops.last() {
        Some(LogicalOp::Aggregate { aggs, .. }) if !aggs.is_empty() => &aggs[0].1,
        _ => return shard_results.into_iter().flatten().collect(),
    };

    // Collect all single-row result values
    let values: Vec<&PropValue> = shard_results
        .iter()
        .flat_map(|results| results.iter())
        .flat_map(|r| r.values.first())
        .collect();

    if values.is_empty() {
        return vec![yata_gie::executor::Record {
            bindings: std::collections::HashMap::new(),
            values: vec![PropValue::Int(0)],
        }];
    }

    let merged = match agg_op {
        AggOp::Count | AggOp::Sum => {
            let total: i64 = values
                .iter()
                .filter_map(|v| match v {
                    PropValue::Int(n) => Some(*n),
                    PropValue::Float(f) => Some(*f as i64),
                    _ => None,
                })
                .sum();
            PropValue::Int(total)
        }
        AggOp::Min => values
            .iter()
            .filter_map(|v| match v {
                PropValue::Int(n) => Some(*n),
                _ => None,
            })
            .min()
            .map(PropValue::Int)
            .unwrap_or(PropValue::Null),
        AggOp::Max => values
            .iter()
            .filter_map(|v| match v {
                PropValue::Int(n) => Some(*n),
                _ => None,
            })
            .max()
            .map(PropValue::Int)
            .unwrap_or(PropValue::Null),
        _ => values.first().map(|v| (*v).clone()).unwrap_or(PropValue::Null),
    };

    vec![yata_gie::executor::Record {
        bindings: std::collections::HashMap::new(),
        values: vec![merged],
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_gie::PlanBuilder;

    // --- Test 1: single shard behaves like MutableCsrStore ---

    #[test]
    fn test_single_shard() {
        let mut store = ShardedGraphStore::single();
        assert_eq!(store.shard_count(), 1);

        let (sid, v0) = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        assert_eq!(sid, 0);
        assert_eq!(v0, 0);

        let (_, v1) = store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_edge(0, v0, v1, "KNOWS", &[]);
        store.commit_all();

        assert_eq!(store.total_vertex_count(), 2);
        assert_eq!(store.total_edge_count(), 1);

        // Topology trait
        assert_eq!(store.vertex_count(), 2);
        assert_eq!(store.edge_count(), 1);
        assert!(store.has_vertex(0));
    }

    // --- Test 2: vertices go to correct shards ---

    #[test]
    fn test_label_partitioning() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
        ]);
        // 3 shards: Person(0), Company(1), default(2)
        assert_eq!(store.shard_count(), 3);

        let (sid_p, _) = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        let (sid_c, _) = store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        let (sid_u, _) = store.add_vertex("Unknown", &[("name", PropValue::Str("X".into()))]);

        assert_eq!(sid_p, 0); // Person shard
        assert_eq!(sid_c, 1); // Company shard
        assert_eq!(sid_u, 2); // default shard
    }

    // --- Test 3: route returns correct shard IDs ---

    #[test]
    fn test_route_by_label() {
        let store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
            vec!["Project".into()],
        ]);
        // 4 shards: Person(0), Company(1), Project(2), default(3)

        // Single label -> that shard + default
        let r = store.route(&["Person".into()]);
        assert!(r.contains(&0));
        assert!(r.contains(&3)); // default
        assert!(!r.contains(&1));

        // Multiple labels
        let r = store.route(&["Person".into(), "Company".into()]);
        assert!(r.contains(&0));
        assert!(r.contains(&1));
        assert!(r.contains(&3)); // default

        // No labels -> all shards
        let r = store.route(&[]);
        assert_eq!(r.len(), 4);

        // Unknown label -> only default
        let r = store.route(&["Unknown".into()]);
        assert_eq!(r, vec![3]);
    }

    // --- Test 4: scan across shards ---

    #[test]
    fn test_scan_across_shards() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
        ]);

        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.commit_all();

        // scan_vertices_by_label returns (shard_id, vid) pairs
        let persons = store.scan_vertices_by_label("Person");
        assert_eq!(persons.len(), 2);
        assert!(persons.iter().all(|&(sid, _)| sid == 0)); // All in Person shard

        let companies = store.scan_vertices_by_label("Company");
        assert_eq!(companies.len(), 1);
        assert_eq!(companies[0].0, 1); // In Company shard
    }

    // --- Test 5: cross-shard query routes correctly ---

    #[test]
    fn test_cross_shard_query() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
        ]);

        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.commit_all();

        // Query touching both labels routes to both shards + default
        let r = store.route(&["Person".into(), "Company".into()]);
        assert!(r.contains(&0)); // Person
        assert!(r.contains(&1)); // Company
        assert!(r.contains(&2)); // default
    }

    // --- Test 6: commit rebuilds CSR in all shards ---

    #[test]
    fn test_commit_all() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
        ]);

        let (s0, v0) = store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        let (s1, v1) = store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        assert_eq!(s0, s1); // Same shard
        store.add_edge(s0, v0, v1, "KNOWS", &[]);

        // Before commit: edges not in CSR
        assert_eq!(store.shard(s0).unwrap().out_degree(v0), 0);

        store.commit_all();

        // After commit: edges in CSR
        assert_eq!(store.shard(s0).unwrap().out_degree(v0), 1);
    }

    // --- Test 7: aggregate counts across shards ---

    #[test]
    fn test_total_counts() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
        ]);

        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.commit_all();

        assert_eq!(store.total_vertex_count(), 3);
        assert_eq!(store.total_edge_count(), 0);

        // Via Topology trait
        assert_eq!(store.vertex_count(), 3);
    }

    // --- Test 8: GIE plan on single partition ---

    #[test]
    fn test_execute_single_shard() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
        ]);

        store.add_vertex("Person", &[
            ("name", PropValue::Str("Alice".into())),
            ("age", PropValue::Int(30)),
        ]);
        store.add_vertex("Person", &[
            ("name", PropValue::Str("Bob".into())),
            ("age", PropValue::Int(25)),
        ]);
        store.commit_all();

        let plan = PlanBuilder::new().scan("Person", "n").build();
        let results = execute_sharded(&plan, &store, &["Person".into()]);

        // Person shard + default shard; default is empty.
        assert_eq!(results.len(), 2);
    }

    // --- Test 9: GIE plan merges from multiple partitions ---

    #[test]
    fn test_execute_multi_shard() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
        ]);

        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.commit_all();

        // Scan Person: routes to shard 0 (Person) + shard 2 (default)
        let plan = PlanBuilder::new().scan("Person", "n").build();
        let results = execute_sharded(&plan, &store, &["Person".into()]);
        assert_eq!(results.len(), 2); // Only shard 0 has Person vertices

        // Scan with no label hints (all shards): finds all vertices of that label
        // But scan op is for "Person" specifically, so only Person vertices match
        let plan = PlanBuilder::new().scan("Person", "n").build();
        let all_results = execute_sharded(&plan, &store, &[]);
        // All 3 shards queried, but only shard 0 has Person vertices
        assert_eq!(all_results.len(), 2);
    }

    // --- Test 10: partition isolation ---

    #[test]
    fn test_partition_isolation() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
        ]);

        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.commit_all();

        // Person shard has no Company vertices
        let person_shard = store.shard(0).unwrap();
        let companies_in_person = person_shard.scan_vertices_by_label("Company");
        assert!(companies_in_person.is_empty());

        // Company shard has no Person vertices
        let company_shard = store.shard(1).unwrap();
        let persons_in_company = company_shard.scan_vertices_by_label("Person");
        assert!(persons_in_company.is_empty());
    }

    // --- Additional: shard_for_label ---

    #[test]
    fn test_shard_for_label() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
        ]);

        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.commit_all();

        let person_shard = store.shard_for_label("Person");
        assert_eq!(person_shard.vertex_count(), 1);

        let company_shard = store.shard_for_label("Company");
        assert_eq!(company_shard.vertex_count(), 0);

        // Unknown label falls back to default shard
        let unknown_shard = store.shard_for_label("Unknown");
        assert_eq!(unknown_shard.partition_id(), store.default_shard_idx() as u32);
    }

    // --- Additional: Scannable trait on ShardedGraphStore ---

    #[test]
    fn test_scannable_trait() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
        ]);

        store.add_vertex("Person", &[
            ("name", PropValue::Str("Alice".into())),
            ("age", PropValue::Int(30)),
        ]);
        store.add_vertex("Person", &[
            ("name", PropValue::Str("Bob".into())),
            ("age", PropValue::Int(25)),
        ]);
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.commit_all();

        // Scannable::scan_vertices_by_label
        let persons: Vec<u32> = Scannable::scan_vertices_by_label(&store, "Person");
        assert_eq!(persons.len(), 2);

        // Scannable::scan_vertices with predicate
        let old = Scannable::scan_vertices(
            &store,
            "Person",
            &Predicate::Gt("age".into(), PropValue::Int(28)),
        );
        assert_eq!(old.len(), 1); // Only Alice (30)

        // Scannable::scan_all_vertices
        let all = Scannable::scan_all_vertices(&store);
        assert_eq!(all.len(), 3);
    }

    // --- Additional: filtered scan ---

    #[test]
    fn test_scan_vertices_filtered() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
        ]);

        store.add_vertex("Person", &[
            ("name", PropValue::Str("Alice".into())),
            ("age", PropValue::Int(30)),
        ]);
        store.add_vertex("Person", &[
            ("name", PropValue::Str("Bob".into())),
            ("age", PropValue::Int(25)),
        ]);
        store.commit_all();

        let results = store.scan_vertices_filtered(
            "Person",
            &Predicate::Gt("age".into(), PropValue::Int(28)),
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 0); // Person shard
    }

    // --- Additional: Schema trait ---

    #[test]
    fn test_schema_trait() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
        ]);

        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Company", &[("name", PropValue::Str("GFTD".into()))]);
        store.commit_all();

        let vlabels = Schema::vertex_labels(&store);
        assert!(vlabels.contains(&"Person".to_string()));
        assert!(vlabels.contains(&"Company".to_string()));
    }

    // --- Additional: execute_sharded with expand ---

    #[test]
    fn test_execute_sharded_with_expand() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
        ]);

        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_edge(0, 0, 1, "KNOWS", &[]);
        store.commit_all();

        let plan = PlanBuilder::new()
            .scan_with_predicate(
                "Person",
                "n",
                Predicate::Eq("name".into(), PropValue::Str("Alice".into())),
            )
            .expand("n", "KNOWS", "m", Direction::Out)
            .build();

        let results = execute_sharded(&plan, &store, &["Person".into()]);
        // Alice -> Bob in Person shard; default shard is empty
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].bindings["m"], 1); // Bob
    }

    // --- Test 9: merge count across shards ---

    #[test]
    fn test_merge_count_across_shards() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
        ]);

        // Add persons to Person shard
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Charlie".into()))]);
        store.commit_all();

        // count(Person) across 2 shards (Person shard + default shard)
        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .aggregate(
                vec![],
                vec![("cnt".into(), yata_gie::AggOp::Count, yata_gie::ir::Expr::Var("n".into()))],
            )
            .build();

        let results = execute_sharded_merged(&plan, &store, &["Person".into()]);
        assert_eq!(results.len(), 1);
        // Person shard has 3, default shard has 0 => merged count = 3
        assert_eq!(results[0].values[0], PropValue::Int(3));
    }

    // --- Test 10: merge sum across shards ---

    #[test]
    fn test_merge_sum_across_shards() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
        ]);

        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into())), ("age", PropValue::Int(30))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into())), ("age", PropValue::Int(25))]);
        store.commit_all();

        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .aggregate(
                vec![],
                vec![("total".into(), yata_gie::AggOp::Sum, yata_gie::ir::Expr::Prop("n".into(), "age".into()))],
            )
            .build();

        let results = execute_sharded_merged(&plan, &store, &["Person".into()]);
        assert_eq!(results.len(), 1);
        // Person shard: sum=55, default shard: sum=0 => merged sum = 55
        assert_eq!(results[0].values[0], PropValue::Int(55));
    }

    // --- Test 11: merge min/max across shards ---

    #[test]
    fn test_merge_min_max_across_shards() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
        ]);

        store.add_vertex("Person", &[("age", PropValue::Int(30))]);
        store.add_vertex("Person", &[("age", PropValue::Int(25))]);
        store.add_vertex("Person", &[("age", PropValue::Int(40))]);
        store.commit_all();

        // Test Min
        let plan_min = PlanBuilder::new()
            .scan("Person", "n")
            .aggregate(
                vec![],
                vec![("min_age".into(), yata_gie::AggOp::Min, yata_gie::ir::Expr::Prop("n".into(), "age".into()))],
            )
            .build();

        let results_min = execute_sharded_merged(&plan_min, &store, &["Person".into()]);
        assert_eq!(results_min.len(), 1);
        assert_eq!(results_min[0].values[0], PropValue::Int(25));

        // Test Max
        let plan_max = PlanBuilder::new()
            .scan("Person", "n")
            .aggregate(
                vec![],
                vec![("max_age".into(), yata_gie::AggOp::Max, yata_gie::ir::Expr::Prop("n".into(), "age".into()))],
            )
            .build();

        let results_max = execute_sharded_merged(&plan_max, &store, &["Person".into()]);
        assert_eq!(results_max.len(), 1);
        assert_eq!(results_max[0].values[0], PropValue::Int(40));
    }

    // --- Test 12: non-aggregate query concatenates results ---

    #[test]
    fn test_execute_sharded_merged_non_agg() {
        let mut store = ShardedGraphStore::with_label_partitions(&[
            vec!["Person".into()],
            vec!["Company".into()],
        ]);

        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.commit_all();

        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .build();

        let results = execute_sharded_merged(&plan, &store, &["Person".into()]);
        // Person shard: 2, default shard: 0 => concatenated = 2
        assert_eq!(results.len(), 2);
    }
}
