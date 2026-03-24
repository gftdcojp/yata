//! yata tiered storage benchmark for graph queries.
//!
//! Storage tiers:
//!   Tier-M  MemoryGraph          — pure in-process (baseline)
//!
//! Graph persistence is via MDAG CAS (yata-mdag).
//!
//! Usage:
//!   cargo run -p yata-bench --bin cypher-storage-bench --release
//!
//!   NODES=1000 EDGES=3000

use anyhow::Result;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::time::Instant;
use yata_cypher::{Executor, Graph, MemoryGraph, NodeRef, RelRef, Value, parse};

// ── dataset ───────────────────────────────────────────────────────────────────

struct Dataset {
    nodes: Vec<(String, String, String, i64)>, // id, label, name, age
    edges: Vec<(String, String, String, String)>, // id, src, dst, rel_type
}

fn generate(n_nodes: usize, n_edges: usize, seed: u64) -> Dataset {
    let mut rng = StdRng::seed_from_u64(seed);
    let labels = ["Person", "Company", "Product"];
    let rel_types = ["KNOWS", "WORKS_AT", "USES", "MANAGES"];
    Dataset {
        nodes: (0..n_nodes).map(|i| {
            let lbl = labels[i % labels.len()];
            (format!("node_{i}"), lbl.to_owned(), format!("{lbl}_{i}"), rng.gen_range(18..80i64))
        }).collect(),
        edges: (0..n_edges).map(|i| {
            let src = rng.gen_range(0..n_nodes);
            let mut dst = rng.gen_range(0..n_nodes);
            while dst == src { dst = rng.gen_range(0..n_nodes); }
            (format!("edge_{i}"), format!("node_{src}"), format!("node_{dst}"),
             rel_types[i % rel_types.len()].to_owned())
        }).collect(),
    }
}

fn build_memory_graph(ds: &Dataset) -> MemoryGraph {
    let mut g = MemoryGraph::new();
    for (id, label, name, age) in &ds.nodes {
        let props = [
            ("name".to_owned(), Value::Str(name.clone())),
            ("age".to_owned(), Value::Int(*age)),
        ].into_iter().collect();
        g.add_node(NodeRef { id: id.clone(), labels: vec![label.clone()], props });
    }
    for (id, src, dst, rel_type) in &ds.edges {
        g.add_rel(RelRef { id: id.clone(), rel_type: rel_type.clone(),
                           src: src.clone(), dst: dst.clone(), props: Default::default() });
    }
    g
}

// ── benchmark runner ──────────────────────────────────────────────────────────

struct Latencies(Vec<u64>);
impl Latencies {
    fn new() -> Self { Self(Vec::new()) }
    fn push(&mut self, us: u64) { self.0.push(us); }
    fn report(&mut self, tier: &str, scenario: &str) -> f64 {
        self.0.sort_unstable();
        let n = self.0.len();
        if n == 0 { return 0.0; }
        let mean = self.0.iter().sum::<u64>() as f64 / n as f64;
        let p50  = self.0[n * 50 / 100];
        let p95  = self.0[n * 95 / 100];
        let p99  = self.0[n * 99 / 100];
        let max  = *self.0.last().unwrap();
        let ops  = 1_000_000.0 / mean;
        println!("  [{tier:<22}] {scenario:<44} mean={mean:>8.1}µs  p50={p50:>7}µs  p95={p95:>7}µs  p99={p99:>7}µs  max={max:>8}µs  {ops:>8.0} ops/s");
        mean
    }
}

fn run_bench(g: &mut dyn Graph, queries: &[&str], iters: usize, tier: &str) -> Vec<f64> {
    let exec = Executor::new();
    let mut means = Vec::new();
    for &q in queries {
        let parsed = parse(q).unwrap();
        for _ in 0..20 { let _ = exec.execute(&parsed, g); }
        let mut lat = Latencies::new();
        for _ in 0..iters {
            let t = Instant::now();
            let _ = exec.execute(&parsed, g);
            lat.push(t.elapsed().as_micros() as u64);
        }
        let mean = lat.report(tier, q);
        means.push(mean);
    }
    means
}

// ── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let n_nodes: usize = std::env::var("NODES").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
    let n_edges: usize = std::env::var("EDGES").ok().and_then(|v| v.parse().ok()).unwrap_or(3000);
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║   yata graph benchmark  (Memory)                                           ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!("  Dataset : {n_nodes} nodes  {n_edges} edges");
    println!("  Tiers   : Tier-M (MemoryGraph)");
    println!();

    let ds = generate(n_nodes, n_edges, 42);

    // ── Build memory graph ───────────────────────────────────────────────────
    let t = Instant::now();
    let mut mem_g = build_memory_graph(&ds);
    println!("  [Tier-M memory]   built    in {:.1}ms", t.elapsed().as_secs_f64()*1000.0);

    // ── Benchmark queries ─────────────────────────────────────────────────────
    let queries: &[&str] = &[
        "MATCH (n:Person) RETURN count(n) AS cnt",
        "MATCH (n:Person) WHERE n.age > 50 RETURN count(n) AS cnt",
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS cnt",
        "MATCH (n:Person) WHERE n.age > 30 RETURN n.name ORDER BY n.name ASC LIMIT 10",
        "MATCH (n) RETURN count(n) AS total",
    ];
    let iters = 200;

    println!("\n─── Query benchmark ──────────────────────────────────────────────────────────");
    println!("  {:<24} {:<44} {:>10}  {:>9}  {:>9}  {:>9}  {:>10}  {:>9}",
        "tier", "query", "mean(µs)", "p50(µs)", "p95(µs)", "p99(µs)", "max(µs)", "ops/s");
    println!("  {}", "─".repeat(145));

    let r_mem = run_bench(&mut mem_g, queries, iters, "Tier-M (memory)");

    // ── Summary ──────────────────────────────────────────────────────────────
    println!("\n═══════════════════════════════════════════════════════════════════════════════");
    println!("  SUMMARY — warm query mean latency µs");
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!("  {:<44}  {:>10}",
        "query", "Tier-M(mem)");
    println!("  {}", "─".repeat(60));

    let n = queries.len() as f64;
    for (i, &q) in queries.iter().enumerate() {
        let lbl = if q.len() > 44 { &q[..44] } else { q };
        let m = r_mem.get(i).copied().unwrap_or(0.0);
        println!("  {lbl:<44}  {m:>8.1}µs");
    }
    println!("  {}", "─".repeat(60));

    let avg_m = r_mem.iter().sum::<f64>() / n;
    println!();
    println!("  Tier summary (avg across {} queries):", queries.len());
    println!("    Tier-M  memory       : {:>8.1}µs  (baseline)", avg_m);

    println!("\ndone");
    Ok(())
}
