//! Multi-scale benchmark suite for the yata graph engine.
//!
//! Measures graph construction throughput, query latency, Cypher/GIE execution,
//! and DiskVineyard/MmapVineyard cold-start at configurable scale levels.
//!
//! Binary:
//!   cargo run -p yata-bench --bin scale-benchmark --release
//!
//! Tests (small = default, medium/large/xlarge via env):
//!   cargo test -p yata-bench -- scale_benchmark --ignored
//!
//! Environment:
//!   YATA_BENCH_SCALE=small|medium|large|xlarge  (default: small)

use std::time::Instant;

use rand::{Rng, SeedableRng, rngs::StdRng};
use yata_cypher::{Executor, MemoryGraph, parse};
use yata_engine::snapshot::{
    restore_snapshot_from_vineyard, serialize_snapshot_to_vineyard,
};
use yata_gie::{LogicalOp, QueryPlan, execute};
use yata_grin::{Mutable, Scannable, Topology};
use yata_store::vineyard::{DiskVineyard, MmapVineyard, VineyardStore};
use yata_store::MutableCsrStore;

// ── Scale config ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
struct ScaleLevel {
    name: &'static str,
    vertices: u64,
    edges: u64,
}

const SMALL: ScaleLevel = ScaleLevel {
    name: "small",
    vertices: 10_000,
    edges: 50_000,
};
const MEDIUM: ScaleLevel = ScaleLevel {
    name: "medium",
    vertices: 100_000,
    edges: 500_000,
};
const LARGE: ScaleLevel = ScaleLevel {
    name: "large",
    vertices: 1_000_000,
    edges: 5_000_000,
};
const XLARGE: ScaleLevel = ScaleLevel {
    name: "xlarge",
    vertices: 10_000_000,
    edges: 50_000_000,
};

fn parse_scale() -> ScaleLevel {
    match std::env::var("YATA_BENCH_SCALE")
        .as_deref()
        .unwrap_or("small")
    {
        "small" => SMALL,
        "medium" => MEDIUM,
        "large" => LARGE,
        "xlarge" => XLARGE,
        other => {
            eprintln!("unknown YATA_BENCH_SCALE={other}, falling back to small");
            SMALL
        }
    }
}

// ── Results ─────────────────────────────────────────────────────────────────

struct BenchRow {
    scale: String,
    vertices: u64,
    edges: u64,
    build_ms: u64,
    scan_us: u64,
    neighbors_label_us: u64,
    neighbors_all_us: u64,
    one_hop_us: u64,
    two_hop_us: u64,
    count_us: u64,
    memory_mb: f64,
}

struct VineyardRow {
    kind: String,
    snapshot_ms: u64,
    restore_ms: u64,
    cold_query_us: u64,
}

fn print_main_table(rows: &[BenchRow]) {
    println!();
    println!("| Scale   | Vertices   | Edges      | Build(ms) | Scan(us) | NbrLabel(us) | NbrAll(us) | 1-hop(us) | 2-hop(us) | COUNT(us) | Memory(MB) |");
    println!("|---------|------------|------------|-----------|----------|--------------|------------|-----------|-----------|-----------|------------|");
    for r in rows {
        println!(
            "| {:<7} | {:>10} | {:>10} | {:>9} | {:>8} | {:>12} | {:>10} | {:>9} | {:>9} | {:>9} | {:>10.1} |",
            r.scale, r.vertices, r.edges, r.build_ms, r.scan_us,
            r.neighbors_label_us, r.neighbors_all_us,
            r.one_hop_us, r.two_hop_us, r.count_us, r.memory_mb,
        );
    }
    println!();
}

fn print_vineyard_table(rows: &[VineyardRow]) {
    println!();
    println!("| Vineyard     | Snapshot(ms) | Restore(ms) | ColdQuery(us) |");
    println!("|--------------|-------------|-------------|---------------|");
    for r in rows {
        println!(
            "| {:<12} | {:>11} | {:>11} | {:>13} |",
            r.kind, r.snapshot_ms, r.restore_ms, r.cold_query_us,
        );
    }
    println!();
}

// ── Graph construction ──────────────────────────────────────────────────────

const LABELS: &[&str] = &["Person", "Company", "Product", "Event"];
const EDGE_LABELS: &[&str] = &["KNOWS", "WORKS_AT", "USES", "ATTENDED"];

fn build_csr_store(scale: ScaleLevel) -> (MutableCsrStore, u64) {
    let mut store = MutableCsrStore::new();
    let mut rng = StdRng::seed_from_u64(42);

    // Vertices
    for i in 0..scale.vertices {
        let label = LABELS[(i as usize) % LABELS.len()];
        store.add_vertex(
            label,
            &[
                (
                    "name",
                    yata_grin::PropValue::Str(format!("{}_{}", label, i)),
                ),
                (
                    "rank",
                    yata_grin::PropValue::Int(rng.gen_range(0..10_000)),
                ),
            ],
        );
    }

    // Edges (random src/dst)
    let v_max = scale.vertices as u32;
    for i in 0..scale.edges {
        let src = rng.gen_range(0..v_max);
        let mut dst = rng.gen_range(0..v_max);
        if dst == src {
            dst = (src + 1) % v_max;
        }
        let el = EDGE_LABELS[(i as usize) % EDGE_LABELS.len()];
        store.add_edge(
            src,
            dst,
            el,
            &[("weight", yata_grin::PropValue::Float(rng.gen_range(0.0..1.0)))],
        );
    }

    let t = Instant::now();
    store.commit();
    let commit_ms = t.elapsed().as_millis() as u64;

    (store, commit_ms)
}

fn build_memory_graph(store: &MutableCsrStore) -> MemoryGraph {
    store.to_full_memory_graph()
}

// ── Query benchmarks (CSR-level) ────────────────────────────────────────────

fn bench_scan_vertices_by_label(store: &MutableCsrStore) -> u64 {
    let t = Instant::now();
    let _ = store.scan_vertices_by_label("Person");
    t.elapsed().as_micros() as u64
}

fn bench_out_neighbors_by_label(store: &MutableCsrStore, vid: u32) -> u64 {
    let t = Instant::now();
    let _ = store.out_neighbors_by_label(vid, "KNOWS");
    t.elapsed().as_micros() as u64
}

fn bench_out_neighbors(store: &MutableCsrStore, vid: u32) -> u64 {
    let t = Instant::now();
    let _ = store.out_neighbors(vid);
    t.elapsed().as_micros() as u64
}

// ── Cypher benchmarks ───────────────────────────────────────────────────────

fn bench_cypher(graph: &mut MemoryGraph, cypher: &str) -> u64 {
    let exec = Executor::new();
    let query = parse(cypher).expect("cypher parse failed");
    let t = Instant::now();
    let _ = exec.execute(&query, graph).expect("cypher exec failed");
    t.elapsed().as_micros() as u64
}

// ── GIE benchmarks ──────────────────────────────────────────────────────────

#[allow(dead_code)]
fn bench_gie_count(store: &MutableCsrStore) -> u64 {
    let plan = QueryPlan {
        ops: vec![
            LogicalOp::Scan {
                label: "Person".into(),
                alias: "n".into(),
                predicate: None,
            },
            LogicalOp::Project {
                exprs: vec![yata_gie::Expr::Func(
                    "count".into(),
                    vec![yata_gie::Expr::Var("n".into())],
                )],
            },
        ],
    };
    let t = Instant::now();
    let _ = execute(&plan, store);
    t.elapsed().as_micros() as u64
}

// ── Vineyard benchmark ──────────────────────────────────────────────────────

fn vineyard_bench_disk(store: &MutableCsrStore) -> VineyardRow {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dv = DiskVineyard::new(tmp.path(), 512).expect("DiskVineyard::new");

    let t0 = Instant::now();
    let manifest =
        serialize_snapshot_to_vineyard(store, &dv, 0).expect("serialize to DiskVineyard");
    let snapshot_ms = t0.elapsed().as_millis() as u64;

    // Page-out all blobs to simulate cold start
    for meta in dv.list(None) {
        dv.page_out(meta.id);
    }

    let t1 = Instant::now();
    let restored =
        restore_snapshot_from_vineyard(&dv, &manifest).expect("restore from DiskVineyard");
    let restore_ms = t1.elapsed().as_millis() as u64;

    // Cold query after restore
    let t2 = Instant::now();
    let _ = restored.scan_vertices_by_label("Person");
    let cold_query_us = t2.elapsed().as_micros() as u64;

    VineyardRow {
        kind: "DiskVineyard".into(),
        snapshot_ms,
        restore_ms,
        cold_query_us,
    }
}

fn vineyard_bench_mmap(store: &MutableCsrStore) -> VineyardRow {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mv = MmapVineyard::new(tmp.path()).expect("MmapVineyard::new");

    let t0 = Instant::now();
    let manifest =
        serialize_snapshot_to_vineyard(store, &mv, 0).expect("serialize to MmapVineyard");
    let snapshot_ms = t0.elapsed().as_millis() as u64;

    // Page-out to simulate cold start
    for meta in mv.list(None) {
        mv.page_out(meta.id);
    }

    let t1 = Instant::now();
    let restored =
        restore_snapshot_from_vineyard(&mv, &manifest).expect("restore from MmapVineyard");
    let restore_ms = t1.elapsed().as_millis() as u64;

    let t2 = Instant::now();
    let _ = restored.scan_vertices_by_label("Person");
    let cold_query_us = t2.elapsed().as_micros() as u64;

    VineyardRow {
        kind: "MmapVineyard".into(),
        snapshot_ms,
        restore_ms,
        cold_query_us,
    }
}

// ── Peak RSS ────────────────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
fn peak_rss_mb() -> f64 {
    use std::mem::MaybeUninit;
    unsafe {
        let mut info = MaybeUninit::<libc::rusage>::uninit();
        if libc::getrusage(libc::RUSAGE_SELF, info.as_mut_ptr()) == 0 {
            info.assume_init().ru_maxrss as f64 / (1024.0 * 1024.0)
        } else {
            0.0
        }
    }
}

#[cfg(target_os = "linux")]
fn peak_rss_mb() -> f64 {
    use std::mem::MaybeUninit;
    unsafe {
        let mut info = MaybeUninit::<libc::rusage>::uninit();
        if libc::getrusage(libc::RUSAGE_SELF, info.as_mut_ptr()) == 0 {
            info.assume_init().ru_maxrss as f64 / 1024.0
        } else {
            0.0
        }
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn peak_rss_mb() -> f64 {
    0.0
}

// ── Run one scale level ─────────────────────────────────────────────────────

fn run_scale(scale: ScaleLevel) -> BenchRow {
    eprintln!(
        "[{}] building {} vertices, {} edges ...",
        scale.name, scale.vertices, scale.edges
    );

    let total_start = Instant::now();
    let (store, _commit_only_ms) = build_csr_store(scale);
    let build_ms = total_start.elapsed().as_millis() as u64;

    let v_max = store.vertex_count() as u32;
    let mut rng = StdRng::seed_from_u64(99);
    let probe_vid = rng.gen_range(0..v_max);

    // CSR-level queries (median of 5 runs)
    let scan_us = median_of(5, || bench_scan_vertices_by_label(&store));
    let neighbors_label_us = median_of(5, || bench_out_neighbors_by_label(&store, probe_vid));
    let neighbors_all_us = median_of(5, || bench_out_neighbors(&store, probe_vid));

    // Cypher queries
    eprintln!("[{}] cypher benchmarks ...", scale.name);
    let mut mg = build_memory_graph(&store);

    let one_hop_us = median_of(3, || {
        bench_cypher(
            &mut mg,
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN b.name LIMIT 100",
        )
    });

    let two_hop_us = median_of(3, || {
        bench_cypher(
            &mut mg,
            "MATCH (a:Person)-[:KNOWS]->(b)-[:WORKS_AT]->(c) RETURN c.name LIMIT 100",
        )
    });

    let count_us = median_of(5, || {
        bench_cypher(&mut mg, "MATCH (n:Person) RETURN count(n) AS cnt")
    });

    let memory_mb = peak_rss_mb();

    BenchRow {
        scale: scale.name.to_string(),
        vertices: scale.vertices,
        edges: scale.edges,
        build_ms,
        scan_us,
        neighbors_label_us,
        neighbors_all_us,
        one_hop_us,
        two_hop_us,
        count_us,
        memory_mb,
    }
}

fn median_of(n: usize, mut f: impl FnMut() -> u64) -> u64 {
    let mut samples: Vec<u64> = (0..n).map(|_| f()).collect();
    samples.sort_unstable();
    samples[samples.len() / 2]
}

// ── Binary entry point ──────────────────────────────────────────────────────

fn main() {
    let scale = parse_scale();
    eprintln!("=== yata scale benchmark: {} ===", scale.name);

    let row = run_scale(scale);
    print_main_table(&[row]);

    // Vineyard benchmarks at the same scale
    eprintln!("[vineyard] building graph for vineyard bench ...");
    let (store, _) = build_csr_store(scale);
    let dv_row = vineyard_bench_disk(&store);
    let mv_row = vineyard_bench_mmap(&store);
    print_vineyard_table(&[dv_row, mv_row]);

    eprintln!("done");
}

// ── Test entry points ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn scale_benchmark_small() {
        let row = run_scale(SMALL);
        print_main_table(&[row]);

        let (store, _) = build_csr_store(SMALL);
        let dv = vineyard_bench_disk(&store);
        let mv = vineyard_bench_mmap(&store);
        print_vineyard_table(&[dv, mv]);
    }

    #[test]
    #[ignore]
    fn scale_benchmark_medium() {
        let row = run_scale(MEDIUM);
        print_main_table(&[row]);

        let (store, _) = build_csr_store(MEDIUM);
        let dv = vineyard_bench_disk(&store);
        let mv = vineyard_bench_mmap(&store);
        print_vineyard_table(&[dv, mv]);
    }

    #[test]
    #[ignore]
    fn scale_benchmark_large() {
        let row = run_scale(LARGE);
        print_main_table(&[row]);
    }

    #[test]
    #[ignore]
    fn scale_benchmark_xlarge() {
        let row = run_scale(XLARGE);
        print_main_table(&[row]);
    }

    #[test]
    #[ignore]
    fn scale_benchmark_all() {
        let scale = parse_scale();
        let scales: Vec<ScaleLevel> = match scale.name {
            "xlarge" => vec![SMALL, MEDIUM, LARGE, XLARGE],
            "large" => vec![SMALL, MEDIUM, LARGE],
            "medium" => vec![SMALL, MEDIUM],
            _ => vec![SMALL],
        };
        let rows: Vec<BenchRow> = scales.into_iter().map(run_scale).collect();
        print_main_table(&rows);
    }

    #[test]
    #[ignore]
    fn scale_benchmark_gie() {
        let scale = parse_scale();
        let (store, _) = build_csr_store(scale);

        let gie_count_us = median_of(5, || bench_gie_count(&store));
        println!(
            "[GIE] scale={} COUNT(Person) = {}us",
            scale.name, gie_count_us
        );
    }
}
