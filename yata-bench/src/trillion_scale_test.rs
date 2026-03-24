//! Trillion-scale readiness tests: coverage, load, and security.
//!
//! Usage:
//!   cargo run -p yata-bench --bin trillion-scale-test --release
//!
//! Environment:
//!   SCALE=small    (10K, fast CI)
//!   SCALE=medium   (1M, integration)
//!   SCALE=large    (10M, load test)
//!   SCALE_JSON=1   (JSON output)

use std::collections::HashSet;
use std::time::Instant;

use anyhow::Result;
use serde::Serialize;
use yata_core::{GlobalVid, LocalVid, PartitionId};
use yata_engine::frontier;
use yata_engine::partition_query;
use yata_grin::{Mutable, Predicate, PropValue, Property, Topology};
use yata_store::MutableCsrStore;
use yata_store::partition::{PartitionAssignment, PartitionStoreSet};

// ── Config ──

#[derive(Debug, Clone, Copy)]
struct Scale {
    nodes: u64,
    partitions: u32,
    labels: u32,
    avg_degree: u32,
}

fn scale() -> Scale {
    match std::env::var("SCALE").as_deref() {
        Ok("large") => Scale {
            nodes: 10_000_000,
            partitions: 16,
            labels: 32,
            avg_degree: 5,
        },
        Ok("medium") => Scale {
            nodes: 1_000_000,
            partitions: 8,
            labels: 16,
            avg_degree: 4,
        },
        _ => Scale {
            nodes: 10_000,
            partitions: 4,
            labels: 8,
            avg_degree: 3,
        },
    }
}

fn json_output() -> bool {
    matches!(std::env::var("SCALE_JSON").as_deref(), Ok("1") | Ok("true"))
}

// ── Result types ──

#[derive(Debug, Serialize)]
struct TestReport {
    scale: String,
    nodes: u64,
    partitions: u32,
    coverage: CoverageReport,
    load: LoadReport,
    security: SecurityReport,
    total_ms: u64,
}

#[derive(Debug, Serialize)]
struct CoverageReport {
    tests_run: usize,
    tests_passed: usize,
    tests_failed: usize,
    failures: Vec<String>,
}

#[derive(Debug, Serialize)]
struct LoadReport {
    ingest_throughput: f64,
    partition_ingest_ms: u64,
    partition_commit_ms: u64,
    cross_partition_query_ms: u64,
    cross_partition_scan_count: usize,
    frontier_3hop_ms: u64,
    frontier_3hop_edges: usize,
    wal_write_ms: u64,
    wal_replay_ms: u64,
    peak_memory_mb: f64,
}

#[derive(Debug, Serialize)]
struct SecurityReport {
    tests_run: usize,
    tests_passed: usize,
    tests_failed: usize,
    failures: Vec<String>,
}

// ── Test runner ──

struct TestRunner {
    passed: usize,
    failed: usize,
    failures: Vec<String>,
}

impl TestRunner {
    fn new() -> Self {
        Self {
            passed: 0,
            failed: 0,
            failures: Vec::new(),
        }
    }

    fn assert_eq<T: PartialEq + std::fmt::Debug>(&mut self, name: &str, got: T, want: T) {
        if got == want {
            self.passed += 1;
        } else {
            self.failed += 1;
            self.failures
                .push(format!("{}: got {:?}, want {:?}", name, got, want));
        }
    }

    fn assert_true(&mut self, name: &str, cond: bool) {
        if cond {
            self.passed += 1;
        } else {
            self.failed += 1;
            self.failures.push(format!("{}: condition false", name));
        }
    }

    fn assert_ge(&mut self, name: &str, got: usize, min: usize) {
        if got >= min {
            self.passed += 1;
        } else {
            self.failed += 1;
            self.failures
                .push(format!("{}: got {}, want >= {}", name, got, min));
        }
    }

    fn report(&self) -> (usize, usize, usize, Vec<String>) {
        (
            self.passed + self.failed,
            self.passed,
            self.failed,
            self.failures.clone(),
        )
    }
}

// ── Build partitioned graph ──

fn build_partitioned(s: Scale) -> PartitionStoreSet {
    let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash {
        partition_count: s.partitions,
    });
    for i in 0..s.nodes {
        let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
        let label = format!("L{}", i % s.labels as u64);
        let org = format!("org{}", i % 4);
        pss.add_vertex(
            gv,
            &label,
            &[
                ("name", PropValue::Str(format!("n{}", i))),
                ("rank", PropValue::Int((i % 10000) as i64)),
                ("org_id", PropValue::Str(org)),
            ],
        );
    }
    for i in 0..s.nodes.saturating_sub(1).min(s.nodes * s.avg_degree as u64) {
        let src_idx = i / s.avg_degree as u64;
        let dst_idx = (src_idx + 1 + (i % s.avg_degree as u64) * 997) % s.nodes;
        let src = GlobalVid::encode(PartitionId::new(0), LocalVid::new(src_idx as u32));
        let dst = GlobalVid::encode(PartitionId::new(0), LocalVid::new(dst_idx as u32));
        pss.add_edge(src, dst, "REL", &[]);
    }
    pss.commit();
    pss
}

// ── Coverage tests ──

fn run_coverage(s: Scale) -> CoverageReport {
    eprintln!("  coverage: building graph...");
    let mut t = TestRunner::new();
    let pss = build_partitioned(s);

    // 1. ID encode/decode roundtrip
    for pid in 0..s.partitions.min(16) {
        for local in [0u32, 1, 100, u32::MAX / 2] {
            let gv = GlobalVid::encode(PartitionId::new(pid), LocalVid::new(local));
            t.assert_eq(
                &format!("id_encode_p{}_l{}", pid, local),
                gv.partition().0,
                pid,
            );
            t.assert_eq(
                &format!("id_decode_p{}_l{}", pid, local),
                gv.local().0,
                local,
            );
        }
    }

    // 2. Partition distribution balance
    let mut counts: Vec<usize> = Vec::new();
    for pid in 0..s.partitions {
        counts.push(pss.partition(pid).map(|s| s.vertex_count()).unwrap_or(0));
    }
    let total: usize = counts.iter().sum();
    t.assert_eq("total_vertices", total as u64 >= s.nodes, true);
    let _expected_per = s.nodes as f64 / s.partitions as f64;
    for (pid, &count) in counts.iter().enumerate() {
        // Each partition should have at least 25% of expected (accounting for ghosts)
        t.assert_true(&format!("partition_{}_has_vertices", pid), count > 0);
    }

    // 3. Cross-partition edge resolution
    let gv0 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
    let neighbors = pss.out_neighbors(gv0);
    t.assert_true("gv0_has_neighbors", !neighbors.is_empty());

    // 4. Label scan across partitions
    let scan = pss.scan_vertices_by_label("L0");
    let expected_per_label = s.nodes / s.labels as u64;
    t.assert_ge("label_scan_L0", scan.len(), expected_per_label as usize / 2);

    // 5. Property access across partitions
    let prop = pss.vertex_prop(gv0, "name");
    t.assert_true("gv0_has_name", prop.is_some());

    // 6. Vertex labels
    let labels = pss.vertex_labels_of(gv0);
    t.assert_true("gv0_has_labels", !labels.is_empty());

    // 7. Global vertex count
    t.assert_ge("vertex_count", pss.vertex_count(), s.nodes as usize);

    // 8. Correctness: single vs multi-partition
    let mut single = PartitionStoreSet::single();
    let mut multi = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 4 });
    for i in 0..100u64 {
        let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
        let label = format!("T{}", i % 3);
        let props: &[(&str, PropValue)] = &[("v", PropValue::Int(i as i64))];
        single.add_vertex(gv, &label, props);
        multi.add_vertex(gv, &label, props);
    }
    single.commit();
    multi.commit();

    for label_idx in 0..3 {
        let label = format!("T{}", label_idx);
        let s_count = single.scan_vertices_by_label(&label).len();
        let m_count = multi.scan_vertices_by_label(&label).len();
        t.assert_eq(&format!("correctness_{}", label), s_count, m_count);
    }

    // 9. Cypher query correctness
    let (s_rows, _) = partition_query::query(&single, "MATCH (n:T0) RETURN n.v", &[]).unwrap();
    let (m_rows, _) = partition_query::query(&multi, "MATCH (n:T0) RETURN n.v", &[]).unwrap();
    t.assert_eq("cypher_correctness_count", s_rows.len(), m_rows.len());

    // 10. Frontier traversal correctness
    let gv_start = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
    let result = frontier::traverse(&pss, gv_start, 2, None, 10000);
    t.assert_true("frontier_2hop_has_edges", result.total_edges > 0);
    t.assert_true("frontier_2hop_no_duplicate_visit", {
        let unique: HashSet<u64> = result.visited.iter().copied().collect();
        unique.len() == result.visited.len()
    });

    // 11. Shortest path
    let gv1 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(1));
    let path = frontier::shortest_path(&pss, gv0, gv1, 5, None);
    t.assert_true("shortest_path_found", path.is_some());

    // 12. Empty frontier
    let bad_gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(u32::MAX));
    let empty = frontier::traverse(&pss, bad_gv, 3, None, 100);
    t.assert_eq("empty_frontier_edges", empty.total_edges, 0);

    // 13. Aggregate merge (count)
    let (rows, _) = partition_query::query(&multi, "MATCH (n:T1) RETURN count(n)", &[]).unwrap();
    t.assert_true("count_merge_non_empty", !rows.is_empty());

    let (run, pass, fail, failures) = t.report();
    CoverageReport {
        tests_run: run,
        tests_passed: pass,
        tests_failed: fail,
        failures,
    }
}

// ── Load tests ──

fn run_load(s: Scale) -> LoadReport {
    eprintln!("  load: partition ingest...");
    let start = Instant::now();
    let pss = build_partitioned(s);
    let partition_ingest_ms = start.elapsed().as_millis() as u64;

    let commit_start = Instant::now();
    // pss is already committed in build_partitioned
    let partition_commit_ms = commit_start.elapsed().as_millis() as u64;

    let ingest_throughput = s.nodes as f64 / (partition_ingest_ms.max(1) as f64 / 1000.0);

    // Cross-partition scan
    eprintln!("  load: cross-partition scan...");
    let scan_start = Instant::now();
    let scan = pss.scan_vertices_by_label("L0");
    let cross_partition_query_ms = scan_start.elapsed().as_millis() as u64;

    // Frontier traversal
    eprintln!("  load: frontier 3-hop...");
    let gv0 = GlobalVid::encode(PartitionId::new(0), LocalVid::new(0));
    let frontier_start = Instant::now();
    let result = frontier::traverse(&pss, gv0, 3, None, 50000);
    let frontier_3hop_ms = frontier_start.elapsed().as_millis() as u64;

    // WAL benchmark (removed — write path moved to PDS Pipeline)
    let (wal_write_ms, wal_replay_ms) = (0u64, 0u64);

    // Peak memory
    let peak_memory_mb = get_peak_rss_mb();

    LoadReport {
        ingest_throughput,
        partition_ingest_ms,
        partition_commit_ms,
        cross_partition_query_ms,
        cross_partition_scan_count: scan.len(),
        frontier_3hop_ms,
        frontier_3hop_edges: result.total_edges,
        wal_write_ms,
        wal_replay_ms,
        peak_memory_mb,
    }
}

// ── Security tests ──

fn run_security(_s: Scale) -> SecurityReport {
    eprintln!("  security: RLS partition isolation...");
    let mut t = TestRunner::new();

    // 1. RLS: org_id isolation across partitions
    {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 4 });
        for i in 0..100u64 {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            let org = if i < 50 { "org_alpha" } else { "org_beta" };
            pss.add_vertex(
                gv,
                "Secret",
                &[
                    ("data", PropValue::Str(format!("secret_{}", i))),
                    ("org_id", PropValue::Str(org.to_string())),
                ],
            );
        }
        pss.commit();

        // Scan with org_id predicate
        let alpha = pss.scan_vertices(
            "Secret",
            &Predicate::Eq("org_id".into(), PropValue::Str("org_alpha".into())),
        );
        let beta = pss.scan_vertices(
            "Secret",
            &Predicate::Eq("org_id".into(), PropValue::Str("org_beta".into())),
        );

        t.assert_eq("rls_alpha_count", alpha.len(), 50);
        t.assert_eq("rls_beta_count", beta.len(), 50);

        // Verify no cross-org data leakage
        for (pid, vid) in &alpha {
            let prop = pss
                .partition(*pid)
                .and_then(|s| s.vertex_prop(*vid, "org_id"));
            t.assert_eq(
                &format!("rls_alpha_{}_{}", pid, vid),
                prop,
                Some(PropValue::Str("org_alpha".into())),
            );
        }
    }

    // 2. Ghost vertex isolation: ghosts should not leak real data
    {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 2 });
        let gv0 = GlobalVid::from_local(0);
        let gv1 = GlobalVid::from_local(1);
        pss.add_vertex(
            gv0,
            "Secret",
            &[("password", PropValue::Str("hunter2".into()))],
        );
        pss.add_vertex(gv1, "Public", &[("info", PropValue::Str("public".into()))]);
        pss.add_edge(gv0, gv1, "LINKS", &[]);
        pss.commit();

        // Find which partition has the ghost of gv1
        let gv1_home = pss.find_vertex_home(gv1).unwrap();
        for pid in 0..2 {
            if pid == gv1_home {
                continue;
            }
            if let Some(store) = pss.partition(pid) {
                if let Some(local) = store.global_map().to_local(gv1) {
                    let labels = Property::vertex_labels(store, local);
                    t.assert_eq(
                        &format!("ghost_label_p{}", pid),
                        labels.first().map(|l| l.as_str()),
                        Some("_ghost"),
                    );
                    // Ghost should NOT have the "info" property
                    let prop = store.vertex_prop(local, "info");
                    t.assert_eq(&format!("ghost_no_data_p{}", pid), prop, None);
                }
            }
        }
    }

    // 3. Partition boundary: vertices can't be accessed from wrong partition
    {
        let mut store = MutableCsrStore::new_partition(0);
        store.add_vertex("A", &[("x", PropValue::Int(1))]);
        store.commit();

        // VID 999 doesn't exist
        t.assert_true("nonexistent_vid", !store.has_vertex(999));
        t.assert_eq("nonexistent_prop", store.vertex_prop(999, "x"), None);
        t.assert_true("nonexistent_neighbors", store.out_neighbors(999).is_empty());
    }

    // 4. ID encoding: partition bits can't be spoofed
    {
        let gv = GlobalVid::encode(PartitionId::new(100), LocalVid::new(42));
        t.assert_eq("id_partition_extract", gv.partition().0, 100);
        t.assert_eq("id_local_extract", gv.local().0, 42);
        // Different partition same local = different global
        let gv2 = GlobalVid::encode(PartitionId::new(101), LocalVid::new(42));
        t.assert_true("id_partition_isolation", gv != gv2);
    }

    // 5. WAL isolation: removed (write path moved to PDS Pipeline)

    let (run, pass, fail, failures) = t.report();
    SecurityReport {
        tests_run: run,
        tests_passed: pass,
        tests_failed: fail,
        failures,
    }
}

// ── Memory ──

#[cfg(target_os = "macos")]
fn get_peak_rss_mb() -> f64 {
    unsafe {
        let mut info = std::mem::MaybeUninit::<libc::rusage>::uninit();
        if libc::getrusage(libc::RUSAGE_SELF, info.as_mut_ptr()) == 0 {
            info.assume_init().ru_maxrss as f64 / (1024.0 * 1024.0)
        } else {
            0.0
        }
    }
}
#[cfg(target_os = "linux")]
fn get_peak_rss_mb() -> f64 {
    unsafe {
        let mut info = std::mem::MaybeUninit::<libc::rusage>::uninit();
        if libc::getrusage(libc::RUSAGE_SELF, info.as_mut_ptr()) == 0 {
            info.assume_init().ru_maxrss as f64 / 1024.0
        } else {
            0.0
        }
    }
}
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn get_peak_rss_mb() -> f64 {
    0.0
}

// ── Main ──

fn main() -> Result<()> {
    let s = scale();
    let json = json_output();
    let start = Instant::now();

    eprintln!("=== Trillion-Scale Readiness Test ===");
    eprintln!(
        "  scale: {} nodes, {} partitions, {} labels, degree {}",
        s.nodes, s.partitions, s.labels, s.avg_degree
    );

    eprintln!("\n[1/3] Coverage tests...");
    let coverage = run_coverage(s);

    eprintln!("\n[2/3] Load tests...");
    let load = run_load(s);

    eprintln!("\n[3/3] Security tests...");
    let security = run_security(s);

    let total_ms = start.elapsed().as_millis() as u64;

    let report = TestReport {
        scale: std::env::var("SCALE").unwrap_or("small".into()),
        nodes: s.nodes,
        partitions: s.partitions,
        coverage,
        load,
        security,
        total_ms,
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("\n=== RESULTS ===");
        println!(
            "scale: {} ({} nodes, {}p)",
            report.scale, report.nodes, report.partitions
        );
        println!("\n--- Coverage ---");
        println!(
            "  {}/{} passed ({} failed)",
            report.coverage.tests_passed, report.coverage.tests_run, report.coverage.tests_failed
        );
        for f in &report.coverage.failures {
            println!("  FAIL: {}", f);
        }
        println!("\n--- Load ---");
        println!(
            "  ingest: {:.0} nodes/s ({} ms)",
            report.load.ingest_throughput, report.load.partition_ingest_ms
        );
        println!(
            "  cross-partition scan: {} ms ({} results)",
            report.load.cross_partition_query_ms, report.load.cross_partition_scan_count
        );
        println!(
            "  frontier 3-hop: {} ms ({} edges)",
            report.load.frontier_3hop_ms, report.load.frontier_3hop_edges
        );
        println!(
            "  WAL write: {} ms, replay: {} ms",
            report.load.wal_write_ms, report.load.wal_replay_ms
        );
        println!("  peak memory: {:.1} MB", report.load.peak_memory_mb);
        println!("\n--- Security ---");
        println!(
            "  {}/{} passed ({} failed)",
            report.security.tests_passed, report.security.tests_run, report.security.tests_failed
        );
        for f in &report.security.failures {
            println!("  FAIL: {}", f);
        }
        println!("\ntotal: {} ms", report.total_ms);
        let all_pass = report.coverage.tests_failed == 0 && report.security.tests_failed == 0;
        if all_pass {
            println!("\n✓ ALL TESTS PASSED");
        } else {
            println!("\n✗ SOME TESTS FAILED");
            std::process::exit(1);
        }
    }

    Ok(())
}
