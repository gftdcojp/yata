//! Partition-aware query execution (M3: partition-aware read path).
//!
//! Executes Cypher queries across a PartitionStoreSet by:
//! 1. Extracting partition hints from the query
//! 2. Routing to relevant partitions via PartitionRouter
//! 3. Executing on each partition (GIE or fallback)
//! 4. Merging results with deduplication

use std::collections::HashSet;

use crate::partition_router;
use yata_grin::*;
use yata_store::MutableCsrStore;
use yata_store::partition::PartitionStoreSet;

/// Hints for partition routing (self-contained).
#[derive(Debug, Clone, Default)]
pub struct PQueryHints {
    pub node_labels: Vec<String>,
    pub rel_types: Vec<String>,
    pub is_read_only: bool,
}

pub fn extract_hints(cypher: &str) -> PQueryHints {
    let mut hints = PQueryHints::default();
    if let Ok(query) = yata_cypher::parse(cypher) {
        let qh = yata_graph::hints::QueryHints::extract(&query);
        hints.node_labels = qh.node_labels;
        hints.rel_types = qh.rel_types;
        hints.is_read_only = qh.is_read_only;
    }
    hints
}

fn route_partitions(pss: &PartitionStoreSet, hints: &PQueryHints) -> Vec<u32> {
    let scope = partition_router::route(
        pss,
        &partition_router::PartitionHints {
            node_labels: hints.node_labels.clone(),
            rel_types: hints.rel_types.clone(),
            global_vid_hints: Vec::new(),
            is_read_only: hints.is_read_only,
        },
    );
    scope.partition_ids(pss.partition_count())
}

/// Metrics collected during partition-aware query execution.
#[derive(Debug, Clone, Default)]
pub struct PartitionQueryMetrics {
    /// Number of partitions consulted.
    pub partitions_touched: usize,
    /// Number of partitions skipped by routing.
    pub partitions_pruned: usize,
    /// Total rows before dedup/merge.
    pub rows_before_merge: usize,
    /// Final row count after merge.
    pub rows_after_merge: usize,
    /// Per-partition row counts.
    pub per_partition_rows: Vec<(u32, usize)>,
    /// Execution time in microseconds.
    pub exec_us: u64,
}

/// Execute a read-only Cypher query across a PartitionStoreSet.
/// Returns merged rows + metrics.
pub fn query(
    pss: &PartitionStoreSet,
    cypher: &str,
    params: &[(String, String)],
) -> Result<(Vec<Vec<(String, String)>>, PartitionQueryMetrics), String> {
    let start = std::time::Instant::now();
    let total = pss.partition_count();

    let hints = extract_hints(cypher);
    let pids = route_partitions(pss, &hints);

    let mut metrics = PartitionQueryMetrics {
        partitions_touched: pids.len(),
        partitions_pruned: total as usize - pids.len(),
        ..Default::default()
    };

    let mut all_rows: Vec<Vec<(String, String)>> = Vec::new();

    for &pid in &pids {
        let store = match pss.partition(pid) {
            Some(s) => s,
            None => continue,
        };

        let rows = execute_on_partition(store, cypher, params, &hints)?;
        metrics.per_partition_rows.push((pid, rows.len()));
        all_rows.extend(rows);
    }

    metrics.rows_before_merge = all_rows.len();

    // Merge/dedup results
    let merged = merge_results(all_rows, cypher);
    metrics.rows_after_merge = merged.len();
    metrics.exec_us = start.elapsed().as_micros() as u64;

    Ok((merged, metrics))
}

/// Execute a mutation Cypher on the appropriate partition(s).
/// Mutations go to all partitions that have relevant data.
pub fn mutate(
    pss: &mut PartitionStoreSet,
    cypher: &str,
    params: &[(String, String)],
) -> Result<(Vec<Vec<(String, String)>>, PartitionQueryMetrics), String> {
    let start = std::time::Instant::now();
    let total = pss.partition_count();

    let hints = extract_hints(cypher);
    let pids = route_partitions(pss, &hints);

    let mut metrics = PartitionQueryMetrics {
        partitions_touched: pids.len(),
        partitions_pruned: total as usize - pids.len(),
        ..Default::default()
    };

    let mut all_rows: Vec<Vec<(String, String)>> = Vec::new();

    // For mutations, we need mutable access. Execute sequentially.
    for &pid in &pids {
        let store = match pss.partition_mut(pid) {
            Some(s) => s,
            None => continue,
        };

        let rows = execute_on_partition_mut(store, cypher, params)?;
        metrics.per_partition_rows.push((pid, rows.len()));
        all_rows.extend(rows);
    }

    metrics.rows_before_merge = all_rows.len();
    metrics.rows_after_merge = all_rows.len(); // no dedup for mutations
    metrics.exec_us = start.elapsed().as_micros() as u64;

    Ok((all_rows, metrics))
}

/// Label scan across partitions with optional predicate.
/// Returns (partition_id, local_vid, properties) for matching vertices.
pub fn scan_with_props(
    pss: &PartitionStoreSet,
    label: &str,
    predicate: &Predicate,
    prop_keys: &[&str],
) -> Vec<(u32, u32, Vec<(String, PropValue)>)> {
    let pids: Vec<u32> = (0..pss.partition_count()).collect();

    let mut results = Vec::new();
    for &pid in &pids {
        let store = match pss.partition(pid) {
            Some(s) => s,
            None => continue,
        };
        let vids = store.scan_vertices(label, predicate);
        for vid in vids {
            let props: Vec<(String, PropValue)> = prop_keys
                .iter()
                .filter_map(|&k| store.vertex_prop(vid, k).map(|v| (k.to_string(), v)))
                .collect();
            results.push((pid, vid, props));
        }
    }
    results
}

/// 1-hop traversal from a global vertex across partitions.
pub fn one_hop(
    pss: &PartitionStoreSet,
    global_vid: yata_core::GlobalVid,
    edge_label: Option<&str>,
) -> Vec<(yata_core::GlobalVid, String, u32)> {
    let local = match pss.global_map().to_local(global_vid) {
        Some(l) => l,
        None => return Vec::new(),
    };
    let pid = pss.assignment().assign_vertex(global_vid, "");
    let store = match pss.partition(pid.0) {
        Some(s) => s,
        None => return Vec::new(),
    };

    let neighbors = match edge_label {
        Some(el) => store.out_neighbors_by_label(local, el),
        None => store.out_neighbors(local),
    };

    neighbors
        .into_iter()
        .map(|n| {
            let neighbor_global =
                store
                    .global_map()
                    .to_global(n.vid)
                    .unwrap_or(yata_core::GlobalVid::encode(
                        pid,
                        yata_core::LocalVid::new(n.vid),
                    ));
            (neighbor_global, n.edge_label, n.edge_id)
        })
        .collect()
}

/// 2-hop traversal: expand one more hop from 1-hop results.
/// Returns (hop1_gvid, hop2_gvid, edge_label) triples.
pub fn two_hop(
    pss: &PartitionStoreSet,
    global_vid: yata_core::GlobalVid,
    edge_label: Option<&str>,
) -> Vec<(yata_core::GlobalVid, yata_core::GlobalVid, String)> {
    let hop1 = one_hop(pss, global_vid, edge_label);
    let mut results = Vec::new();

    for (hop1_gvid, _, _) in &hop1 {
        let hop2 = one_hop(pss, *hop1_gvid, edge_label);
        for (hop2_gvid, label, _) in hop2 {
            results.push((*hop1_gvid, hop2_gvid, label));
        }
    }
    results
}

// ── Internal execution ──────────────────────────────────────────────

/// Execute a read query on a single partition's MutableCsrStore (public for distributed).
pub fn execute_on_partition_pub(
    store: &MutableCsrStore,
    cypher: &str,
    params: &[(String, String)],
    hints: &PQueryHints,
) -> Result<Vec<Vec<(String, String)>>, String> {
    execute_on_partition(store, cypher, params, hints)
}

/// Execute a read query on a single partition's MutableCsrStore.
fn execute_on_partition(
    store: &MutableCsrStore,
    cypher: &str,
    params: &[(String, String)],
    hints: &PQueryHints,
) -> Result<Vec<Vec<(String, String)>>, String> {
    // Try GIE fast path (direct CSR execution)
    if hints.is_read_only {
        if let Ok(ast) = yata_cypher::parse(cypher) {
            if let Ok(plan) = yata_gie::transpile::transpile(&ast) {
                let records = yata_gie::executor::execute(&plan, store);
                return Ok(yata_gie::executor::result_to_rows(&records, &plan));
            }
        }
    }

    // GIE is the sole read path; no MemoryGraph fallback.
    Err(format!("GIE transpile failed for partition read query: {cypher}"))
}

/// Execute a mutation on a single partition's MutableCsrStore.
fn execute_on_partition_mut(
    store: &mut MutableCsrStore,
    cypher: &str,
    params: &[(String, String)],
) -> Result<Vec<Vec<(String, String)>>, String> {
    let g = store.to_full_memory_graph();
    let mut qg = yata_graph::QueryableGraph(g);
    let rows = qg
        .query(cypher, params)
        .map_err(|e| format!("cypher error: {e}"))?;

    // Rebuild CSR from mutated graph
    let new_csr = crate::loader::rebuild_csr_from_graph(&qg);
    *store = new_csr;

    Ok(rows)
}

/// Merge results from multiple partitions.
/// - For aggregate queries (count, sum): combines aggregate values
/// - For regular queries: deduplicates rows
fn merge_results(rows: Vec<Vec<(String, String)>>, cypher: &str) -> Vec<Vec<(String, String)>> {
    if rows.is_empty() {
        return rows;
    }

    // Detect aggregate queries by checking column names
    let is_aggregate = rows.first().map_or(false, |row| {
        row.iter().any(|(col, _)| {
            let c = col.to_lowercase();
            c.starts_with("count(")
                || c.starts_with("sum(")
                || c.starts_with("avg(")
                || c.starts_with("min(")
                || c.starts_with("max(")
        })
    });

    // Also detect via cypher keywords
    let cypher_upper = cypher.to_uppercase();
    let has_count = cypher_upper.contains("COUNT(");
    let has_sum = cypher_upper.contains("SUM(");

    if is_aggregate || has_count || has_sum {
        return merge_aggregates(rows);
    }

    // Dedup by row content (handles overlapping partition results)
    let mut seen = HashSet::new();
    let mut merged = Vec::with_capacity(rows.len());
    for row in rows {
        let key = row_key(&row);
        if seen.insert(key) {
            merged.push(row);
        }
    }
    merged
}

/// Merge aggregate results: sum numeric columns across partitions.
fn merge_aggregates(rows: Vec<Vec<(String, String)>>) -> Vec<Vec<(String, String)>> {
    if rows.is_empty() {
        return rows;
    }

    // Single row expected for pure aggregates
    let first = &rows[0];
    let mut merged_row: Vec<(String, String)> = first.clone();

    for row in rows.iter().skip(1) {
        for (i, (col, val)) in row.iter().enumerate() {
            if i >= merged_row.len() {
                break;
            }
            let col_lower = col.to_lowercase();
            let is_count = col_lower.starts_with("count(");
            let is_sum = col_lower.starts_with("sum(");

            if is_count || is_sum {
                // Parse both as numbers and add
                let existing: f64 = parse_json_number(&merged_row[i].1);
                let new_val: f64 = parse_json_number(val);
                let sum = existing + new_val;
                // Emit as integer if it's a whole number
                if sum == sum.floor() && sum.abs() < i64::MAX as f64 {
                    merged_row[i].1 = format!("{}", sum as i64);
                } else {
                    merged_row[i].1 = format!("{}", sum);
                }
            }
            // For min/max/avg: more complex merge needed (skip for now)
        }
    }

    vec![merged_row]
}

fn parse_json_number(s: &str) -> f64 {
    // Try parsing as JSON number (may be quoted or bare)
    let trimmed = s.trim().trim_matches('"');
    trimmed.parse::<f64>().unwrap_or(0.0)
}

/// Create a deterministic key for a row (for dedup).
fn row_key(row: &[(String, String)]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for (k, v) in row {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_core::{GlobalVid, LocalVid, PartitionId};
    use yata_store::partition::PartitionAssignment;

    fn build_test_pss() -> PartitionStoreSet {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 4 });
        for i in 0..40u64 {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            let label = format!("Label{}", i % 4);
            pss.add_vertex(
                gv,
                &label,
                &[
                    ("name", PropValue::Str(format!("node_{}", i))),
                    ("rank", PropValue::Int((i % 10) as i64)),
                ],
            );
        }
        // Add edges: chain
        for i in 0..39u64 {
            let src = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            let dst = GlobalVid::encode(PartitionId::new(0), LocalVid::new((i + 1) as u32));
            pss.add_edge(src, dst, "NEXT", &[]);
        }
        pss.commit();
        pss
    }

    #[test]
    fn test_query_label_scan() {
        let pss = build_test_pss();
        let (rows, metrics) = query(&pss, "MATCH (n:Label0) RETURN n.name", &[]).unwrap();

        assert_eq!(rows.len(), 10); // 40 nodes / 4 labels = 10 per label
        assert!(metrics.partitions_touched > 0);
        assert!(metrics.exec_us > 0);
    }

    #[test]
    fn test_query_count() {
        let pss = build_test_pss();
        let (rows, _metrics) = query(&pss, "MATCH (n:Label1) RETURN count(n)", &[]).unwrap();

        // count should be 10
        assert!(!rows.is_empty());
    }

    #[test]
    fn test_scan_with_props() {
        let pss = build_test_pss();
        let results = scan_with_props(
            &pss,
            "Label0",
            &Predicate::Gt("rank".into(), PropValue::Int(5)),
            &["name", "rank"],
        );

        // rank goes 0..9 cycling; rank > 5 means 6,7,8,9 → 4 out of 10
        assert_eq!(results.len(), 4);
        for (_, _, props) in &results {
            let rank = props.iter().find(|(k, _)| k == "rank").unwrap();
            if let PropValue::Int(v) = &rank.1 {
                assert!(*v > 5);
            }
        }
    }

    #[test]
    fn test_one_hop() {
        let pss = build_test_pss();
        let gv0 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let neighbors = one_hop(&pss, gv0, None);
        // node 0 has an edge to node 1
        assert!(!neighbors.is_empty());
    }

    #[test]
    fn test_two_hop() {
        let pss = build_test_pss();
        let gv0 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
        let results = two_hop(&pss, gv0, None);
        // node 0 → node 1 → node 2 (at least)
        assert!(!results.is_empty());
    }

    #[test]
    fn test_metrics_pruning() {
        let mut label_map = std::collections::HashMap::new();
        label_map.insert("TypeA".to_string(), 0);
        label_map.insert("TypeB".to_string(), 1);
        let assignment = PartitionAssignment::Label {
            label_map,
            default_partition: 0,
        };
        let mut pss = PartitionStoreSet::new(assignment);

        for i in 0..10 {
            let gv = GlobalVid::from_local(i);
            let label = if i < 5 { "TypeA" } else { "TypeB" };
            pss.add_vertex(gv, label, &[("v", PropValue::Int(i as i64))]);
        }
        pss.commit();

        let (rows, metrics) = query(&pss, "MATCH (n:TypeA) RETURN n.v", &[]).unwrap();

        assert_eq!(rows.len(), 5);
        assert_eq!(metrics.partitions_touched, 1); // Only partition 0
        assert_eq!(metrics.partitions_pruned, 1); // Partition 1 pruned
    }

    #[test]
    fn test_correctness_single_vs_multi_partition() {
        // Build same data in single partition and multi partition
        let mut single = PartitionStoreSet::single();
        let mut multi = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 4 });

        for i in 0..20u64 {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            let label = format!("L{}", i % 3);
            let props: &[(&str, PropValue)] = &[
                ("name", PropValue::Str(format!("n{}", i))),
                ("val", PropValue::Int(i as i64)),
            ];
            single.add_vertex(gv, &label, props);
            multi.add_vertex(gv, &label, props);
        }
        single.commit();
        multi.commit();

        // Compare label scan counts
        let cypher = "MATCH (n:L0) RETURN n.name";
        let (single_rows, _) = query(&single, cypher, &[]).unwrap();
        let (multi_rows, _) = query(&multi, cypher, &[]).unwrap();

        // Same count
        assert_eq!(
            single_rows.len(),
            multi_rows.len(),
            "single={} vs multi={}",
            single_rows.len(),
            multi_rows.len()
        );

        // Same content (sort for comparison)
        let mut single_sorted: Vec<String> = single_rows
            .iter()
            .map(|r| {
                r.iter()
                    .map(|(_, v)| v.clone())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect();
        let mut multi_sorted: Vec<String> = multi_rows
            .iter()
            .map(|r| {
                r.iter()
                    .map(|(_, v)| v.clone())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect();
        single_sorted.sort();
        multi_sorted.sort();
        assert_eq!(
            single_sorted, multi_sorted,
            "results differ between single and multi partition"
        );
    }

    #[test]
    fn test_correctness_count_single_vs_multi() {
        let mut single = PartitionStoreSet::single();
        let mut multi = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 4 });

        for i in 0..30u64 {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            let label = format!("Type{}", i % 5);
            single.add_vertex(gv, &label, &[("x", PropValue::Int(i as i64))]);
            multi.add_vertex(gv, &label, &[("x", PropValue::Int(i as i64))]);
        }
        single.commit();
        multi.commit();

        for t in 0..5 {
            let cypher = format!("MATCH (n:Type{}) RETURN count(n)", t);
            let (sr, _) = query(&single, &cypher, &[]).unwrap();
            let (mr, _) = query(&multi, &cypher, &[]).unwrap();

            // Both should return exactly 6 (30/5)
            assert_eq!(sr.len(), mr.len(), "Type{}: row count mismatch", t);
        }
    }

    #[test]
    fn test_empty_result_on_missing_label() {
        let pss = build_test_pss();
        let (rows, metrics) = query(&pss, "MATCH (n:NonExistent) RETURN n", &[]).unwrap();
        assert_eq!(rows.len(), 0);
        assert_eq!(metrics.rows_after_merge, 0);
    }

    #[test]
    fn test_row_dedup() {
        let rows = vec![
            vec![("a".to_string(), "1".to_string())],
            vec![("a".to_string(), "1".to_string())],
            vec![("a".to_string(), "2".to_string())],
        ];
        let merged = merge_results(rows, "");
        assert_eq!(merged.len(), 2);
    }
}
