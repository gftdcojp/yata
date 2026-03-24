//! Cypher performance benchmark: yata-cypher (in-memory).
//!
//! Usage:
//!   cargo run -p yata-bench --bin cypher-bench --release
//!
//! Environment variables:
//!   NODES      (default: 1000)
//!   EDGES      (default: 3000)

use anyhow::Result;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::time::Instant;
use yata_cypher::{Executor, Graph, MemoryGraph, NodeRef, RelRef, Value, parse};

// ── report helpers ───────────────────────────────────────────────────────────

struct Samples(Vec<u64>);

impl Samples {
    fn new() -> Self {
        Self(Vec::new())
    }
    fn push(&mut self, us: u64) {
        self.0.push(us);
    }
    fn report(&mut self, label: &str, engine: &str) {
        if self.0.is_empty() {
            println!("  [{engine}] {label}: no data");
            return;
        }
        self.0.sort_unstable();
        let n = self.0.len();
        let mean = self.0.iter().sum::<u64>() / n as u64;
        let p50 = self.0[n * 50 / 100];
        let p95 = self.0[n * 95 / 100];
        let p99 = self.0[n * 99 / 100];
        let max = *self.0.last().unwrap();
        let ops = 1_000_000.0 / mean as f64;
        println!(
            "  [{engine:<12}] {label:<42} n={n:>5}  mean={mean:>6}µs  p50={p50:>6}µs  p95={p95:>6}µs  p99={p99:>6}µs  max={max:>7}µs  ({ops:.0} ops/s)",
        );
    }
}

// ── dataset generation ───────────────────────────────────────────────────────

struct Dataset {
    nodes: Vec<NodeData>,
    edges: Vec<EdgeData>,
}

struct NodeData {
    id: String,
    label: String,
    name: String,
    age: i64,
}

struct EdgeData {
    id: String,
    src: String,
    dst: String,
    rel_type: String,
}

fn generate_dataset(n_nodes: usize, n_edges: usize, seed: u64) -> Dataset {
    let mut rng = StdRng::seed_from_u64(seed);
    let labels = ["Person", "Company", "Product"];
    let rel_types = ["KNOWS", "WORKS_AT", "USES", "MANAGES"];

    let nodes: Vec<NodeData> = (0..n_nodes)
        .map(|i| {
            let label = labels[i % labels.len()];
            NodeData {
                id: format!("node_{i}"),
                label: label.to_owned(),
                name: format!("{label}_{i}"),
                age: rng.gen_range(18..80),
            }
        })
        .collect();

    let edges: Vec<EdgeData> = (0..n_edges)
        .map(|i| {
            let src = rng.gen_range(0..n_nodes);
            let mut dst = rng.gen_range(0..n_nodes);
            while dst == src {
                dst = rng.gen_range(0..n_nodes);
            }
            let rel_type = rel_types[i % rel_types.len()];
            EdgeData {
                id: format!("edge_{i}"),
                src: format!("node_{src}"),
                dst: format!("node_{dst}"),
                rel_type: rel_type.to_owned(),
            }
        })
        .collect();

    Dataset { nodes, edges }
}

// ── yata-cypher backend ──────────────────────────────────────────────────────

fn build_yata_graph(ds: &Dataset) -> MemoryGraph {
    let mut g = MemoryGraph::new();
    for n in &ds.nodes {
        let props = [
            ("name".to_owned(), Value::Str(n.name.clone())),
            ("age".to_owned(), Value::Int(n.age)),
        ]
        .into_iter()
        .collect();
        g.add_node(NodeRef {
            id: n.id.clone(),
            labels: vec![n.label.clone()],
            props,
        });
    }
    for e in &ds.edges {
        g.add_rel(RelRef {
            id: e.id.clone(),
            rel_type: e.rel_type.clone(),
            src: e.src.clone(),
            dst: e.dst.clone(),
            props: Default::default(),
        });
    }
    g
}

struct YataBenchmark {
    graph: MemoryGraph,
    exec: Executor,
}

impl YataBenchmark {
    fn new(ds: &Dataset) -> Self {
        Self {
            graph: build_yata_graph(ds),
            exec: Executor::new(),
        }
    }

    fn run_query(&mut self, cypher: &str) -> Result<usize> {
        let query = parse(cypher)?;
        let result = self.exec.execute(&query, &mut self.graph)?;
        Ok(result.rows.len())
    }
}

// ── benchmark scenarios ──────────────────────────────────────────────────────

struct Scenario {
    name: &'static str,
    cypher: &'static str,
    iters: usize,
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "full node scan (count)",
            cypher: "MATCH (n) RETURN count(n) AS cnt",
            iters: 200,
        },
        Scenario {
            name: "label scan Person (count)",
            cypher: "MATCH (n:Person) RETURN count(n) AS cnt",
            iters: 200,
        },
        Scenario {
            name: "label scan Company (count)",
            cypher: "MATCH (n:Company) RETURN count(n) AS cnt",
            iters: 200,
        },
        Scenario {
            name: "property filter age>50 (count)",
            cypher: "MATCH (n:Person) WHERE n.age > 50 RETURN count(n) AS cnt",
            iters: 200,
        },
        Scenario {
            name: "point lookup by name",
            cypher: "MATCH (n:Person) WHERE n.name = 'Person_42' RETURN n.name, n.age",
            iters: 200,
        },
        Scenario {
            name: "rel traversal KNOWS count",
            cypher: "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS cnt",
            iters: 50,
        },
        Scenario {
            name: "rel traversal WORKS_AT count",
            cypher: "MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN count(*) AS cnt",
            iters: 50,
        },
        Scenario {
            name: "filter+sort+limit",
            cypher: "MATCH (n:Person) WHERE n.age > 30 RETURN n.name ORDER BY n.name ASC LIMIT 10",
            iters: 200,
        },
        Scenario {
            name: "collect all Person names",
            cypher: "MATCH (n:Person) RETURN collect(n.name) AS names",
            iters: 100,
        },
        Scenario {
            name: "avg age by label",
            cypher: "MATCH (n:Person) RETURN avg(n.age) AS avg_age",
            iters: 200,
        },
    ]
}

// ── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let n_nodes: usize = std::env::var("NODES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let n_edges: usize = std::env::var("EDGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);

    println!("╔══════════════════════════════════════════════════════════════════════════╗");
    println!("║           yata-cypher  —  Cypher Load Test                             ║");
    println!("╚══════════════════════════════════════════════════════════════════════════╝");
    println!(
        "  Dataset:  {} nodes  {} edges  (seed=42)",
        n_nodes, n_edges
    );

    let ds = generate_dataset(n_nodes, n_edges, 42);

    // ── yata-cypher setup ────────────────────────────────────────────────────
    println!("\n[Setup] building yata-cypher in-memory graph...");
    let t = Instant::now();
    let mut yata = YataBenchmark::new(&ds);
    println!(
        "  yata-cypher graph built in {:.1}ms",
        t.elapsed().as_secs_f64() * 1000.0
    );

    // ── warmup ───────────────────────────────────────────────────────────────
    println!("\n[Warmup]");
    let warmup_q = "MATCH (n:Person) RETURN count(n) AS cnt";
    for _ in 0..20 {
        let _ = yata.run_query(warmup_q);
    }
    println!("  done");

    // ── run scenarios ─────────────────────────────────────────────────────────
    println!("\n[Results]");
    println!(
        "  {:<14} {:<42} {:>5}  {:>8}  {:>8}  {:>8}  {:>8}  {:>9}  {:>10}",
        "engine", "scenario", "n", "mean", "p50", "p95", "p99", "max", "ops/s"
    );
    println!("  {}", "-".repeat(130));

    for sc in scenarios() {
        let mut yata_samples = Samples::new();
        for _ in 0..sc.iters {
            let t = Instant::now();
            let _ = yata.run_query(sc.cypher).ok();
            yata_samples.push(t.elapsed().as_micros() as u64);
        }
        yata_samples.report(sc.name, "yata-cypher");
        println!();
    }

    // ── CREATE write benchmark ───────────────────────────────────────────────
    println!("[Write: CREATE node]");
    {
        let create_iters = 500;
        let mut yata_samples = Samples::new();
        for i in 0..create_iters {
            let q = format!(
                "CREATE (n:Person {{name: 'bench_{i}', age: {}}}) RETURN n",
                i % 80
            );
            let t = Instant::now();
            let _ = yata.run_query(&q).ok();
            yata_samples.push(t.elapsed().as_micros() as u64);
        }
        yata_samples.report("CREATE (n:Person {...})", "yata-cypher");
    }

    // ── MERGE write benchmark ────────────────────────────────────────────────
    println!("\n[Write: MERGE node]");
    {
        let merge_iters = 200;
        let mut yata_samples = Samples::new();
        for i in 0..merge_iters {
            let q = format!("MERGE (n:Person {{name: 'merge_{}'}})", i % 50);
            let t = Instant::now();
            let _ = yata.run_query(&q).ok();
            yata_samples.push(t.elapsed().as_micros() as u64);
        }
        yata_samples.report("MERGE (n:Person {name: ...})", "yata-cypher");
    }

    // ── scale test: varying dataset size ─────────────────────────────────────
    println!("\n[Scale: yata-cypher only — MATCH (n:Person) RETURN count(n)]");
    for &sz in &[100usize, 500, 1_000, 5_000, 10_000, 50_000] {
        let ds2 = generate_dataset(sz, sz * 3, 99);
        let mut yata2 = YataBenchmark::new(&ds2);
        let q = "MATCH (n:Person) RETURN count(n) AS cnt";
        // warmup
        for _ in 0..5 {
            let _ = yata2.run_query(q);
        }
        let mut samples = Samples::new();
        for _ in 0..100 {
            let t = Instant::now();
            let _ = yata2.run_query(q);
            samples.push(t.elapsed().as_micros() as u64);
        }
        samples.0.sort_unstable();
        let n = samples.0.len();
        let mean = samples.0.iter().sum::<u64>() / n as u64;
        let p95 = samples.0[n * 95 / 100];
        let ops = 1_000_000.0 / mean as f64;
        println!(
            "  nodes={:>6}  edges={:>6}  mean={:>6}µs  p95={:>6}µs  {:.0} ops/s",
            sz,
            sz * 3,
            mean,
            p95,
            ops
        );
    }

    println!("\ndone");
    Ok(())
}
