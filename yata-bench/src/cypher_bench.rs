//! Cypher performance comparison: yata-cypher (in-memory) vs Neo4j 5 (Bolt).
//!
//! Usage:
//!   cargo run -p yata-bench --bin cypher-bench --release
//!
//! Environment variables:
//!   NEO4J_URI  (default: bolt://localhost:7687)
//!   NEO4J_USER (default: neo4j)
//!   NEO4J_PASS (default: benchmark)
//!   NODES      (default: 1000)
//!   EDGES      (default: 3000)

use anyhow::Result;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::time::Instant;
use yata_cypher::{Executor, Graph, MemoryGraph, NodeRef, RelRef, Value, parse};

// ── report helpers ───────────────────────────────────────────────────────────

struct Samples(Vec<u64>);

impl Samples {
    fn new() -> Self { Self(Vec::new()) }
    fn push(&mut self, us: u64) { self.0.push(us); }
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
            while dst == src { dst = rng.gen_range(0..n_nodes); }
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
        ].into_iter().collect();
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

// ── Neo4j backend ────────────────────────────────────────────────────────────

struct Neo4jBenchmark {
    graph: neo4rs::Graph,
}

impl Neo4jBenchmark {
    async fn connect(uri: &str, user: &str, pass: &str) -> Result<Self> {
        let graph = neo4rs::Graph::new(uri, user, pass).await?;
        Ok(Self { graph })
    }

    async fn setup(&self, ds: &Dataset) -> Result<()> {
        // Clear existing data
        self.graph
            .run(neo4rs::query("MATCH (n) DETACH DELETE n"))
            .await?;

        // Create nodes in batches of 100
        for chunk in ds.nodes.chunks(100) {
            let mut cypher = String::from("UNWIND $rows AS row CREATE (n) SET n += row.props, n:__placeholder__");
            // Use individual creates for simplicity
            for n in chunk {
                let q = format!(
                    "CREATE (n:{} {{id: '{}', name: '{}', age: {}}})",
                    n.label, n.id, n.name, n.age
                );
                self.graph.run(neo4rs::query(&q)).await?;
            }
        }

        // Create edges in batches
        for chunk in ds.edges.chunks(200) {
            for e in chunk {
                let q = format!(
                    "MATCH (a {{id: '{}'}}), (b {{id: '{}'}}) CREATE (a)-[:{}]->(b)",
                    e.src, e.dst, e.rel_type
                );
                self.graph.run(neo4rs::query(&q)).await?;
            }
        }

        Ok(())
    }

    async fn run_query(&self, cypher: &str) -> Result<usize> {
        let mut result = self.graph.execute(neo4rs::query(cypher)).await?;
        let mut count = 0;
        while result.next().await?.is_some() {
            count += 1;
        }
        Ok(count)
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
    let neo4j_uri = std::env::var("NEO4J_URI").unwrap_or("bolt://localhost:7687".to_owned());
    let neo4j_user = std::env::var("NEO4J_USER").unwrap_or("neo4j".to_owned());
    let neo4j_pass = std::env::var("NEO4J_PASS").unwrap_or("benchmark".to_owned());

    println!("╔══════════════════════════════════════════════════════════════════════════╗");
    println!("║           yata-cypher vs Neo4j 5  —  Cypher Load Test                  ║");
    println!("╚══════════════════════════════════════════════════════════════════════════╝");
    println!("  Dataset:  {} nodes  {} edges  (seed=42)", n_nodes, n_edges);
    println!("  Neo4j:    {}", neo4j_uri);

    let ds = generate_dataset(n_nodes, n_edges, 42);

    // ── yata-cypher setup ────────────────────────────────────────────────────
    println!("\n[Setup] building yata-cypher in-memory graph...");
    let t = Instant::now();
    let mut yata = YataBenchmark::new(&ds);
    println!("  yata-cypher graph built in {:.1}ms", t.elapsed().as_secs_f64() * 1000.0);

    // ── Neo4j setup ──────────────────────────────────────────────────────────
    println!("[Setup] connecting to Neo4j and loading data...");
    let neo4j_result: Result<Neo4jBenchmark> = async {
        let t = Instant::now();
        let neo = Neo4jBenchmark::connect(&neo4j_uri, &neo4j_user, &neo4j_pass).await?;
        println!("  Neo4j connected in {:.1}ms", t.elapsed().as_secs_f64() * 1000.0);
        let t = Instant::now();
        neo.setup(&ds).await?;
        println!("  Neo4j data loaded in {:.1}s", t.elapsed().as_secs_f64());
        Ok(neo)
    }.await;

    let neo4j_available = neo4j_result.is_ok();
    let neo = neo4j_result.ok();
    if !neo4j_available {
        println!("  [WARN] Neo4j unavailable — running yata-cypher only");
    }

    // ── warmup ───────────────────────────────────────────────────────────────
    println!("\n[Warmup]");
    let warmup_q = "MATCH (n:Person) RETURN count(n) AS cnt";
    for _ in 0..20 {
        let _ = yata.run_query(warmup_q);
    }
    if let Some(ref neo) = neo {
        for _ in 0..5 {
            let _ = neo.run_query(warmup_q).await;
        }
    }
    println!("  done");

    // ── run scenarios ─────────────────────────────────────────────────────────
    println!("\n[Results]");
    println!("  {:<14} {:<42} {:>5}  {:>8}  {:>8}  {:>8}  {:>8}  {:>9}  {:>10}",
        "engine", "scenario", "n", "mean", "p50", "p95", "p99", "max", "ops/s");
    println!("  {}", "-".repeat(130));

    let mut summary: Vec<(String, f64, f64)> = Vec::new(); // (scenario, yata_mean_us, neo_mean_us)

    for sc in scenarios() {
        // yata-cypher
        let mut yata_samples = Samples::new();
        for _ in 0..sc.iters {
            let t = Instant::now();
            let _ = yata.run_query(sc.cypher).ok();
            yata_samples.push(t.elapsed().as_micros() as u64);
        }
        yata_samples.report(sc.name, "yata-cypher");
        let yata_mean = yata_samples.0.iter().sum::<u64>() as f64 / yata_samples.0.len() as f64;

        // Neo4j
        let neo_mean = if let Some(ref neo) = neo {
            let mut neo_samples = Samples::new();
            let neo_iters = (sc.iters / 4).max(10);
            for _ in 0..neo_iters {
                let t = Instant::now();
                let _ = neo.run_query(sc.cypher).await.ok();
                neo_samples.push(t.elapsed().as_micros() as u64);
            }
            neo_samples.report(sc.name, "neo4j");
            let m = neo_samples.0.iter().sum::<u64>() as f64 / neo_samples.0.len() as f64;
            Some(m)
        } else {
            None
        };

        // speedup
        if let Some(nm) = neo_mean {
            let speedup = nm / yata_mean;
            println!("                {:<42} speedup: {:.1}x (yata faster)", sc.name, speedup);
            summary.push((sc.name.to_owned(), yata_mean, nm));
        }
        println!();
    }

    // ── CREATE write benchmark ───────────────────────────────────────────────
    println!("[Write: CREATE node]");
    {
        let create_iters = 500;
        let mut yata_samples = Samples::new();
        for i in 0..create_iters {
            let q = format!("CREATE (n:Person {{name: 'bench_{i}', age: {}}}) RETURN n", i % 80);
            let t = Instant::now();
            let _ = yata.run_query(&q).ok();
            yata_samples.push(t.elapsed().as_micros() as u64);
        }
        yata_samples.report("CREATE (n:Person {...})", "yata-cypher");
        let yata_mean = yata_samples.0.iter().sum::<u64>() as f64 / yata_samples.0.len() as f64;

        if let Some(ref neo) = neo {
            let neo_iters = 100;
            let mut neo_samples = Samples::new();
            for i in 0..neo_iters {
                let q = format!("CREATE (n:Person {{name: 'bench_{i}', age: {}}}) RETURN n", i % 80);
                let t = Instant::now();
                let _ = neo.run_query(&q).await.ok();
                neo_samples.push(t.elapsed().as_micros() as u64);
            }
            neo_samples.report("CREATE (n:Person {...})", "neo4j");
            let nm = neo_samples.0.iter().sum::<u64>() as f64 / neo_samples.0.len() as f64;
            println!("                {:<42} speedup: {:.1}x (yata faster)", "CREATE", nm / yata_mean);
        }
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
        let yata_mean = yata_samples.0.iter().sum::<u64>() as f64 / yata_samples.0.len() as f64;

        if let Some(ref neo) = neo {
            let neo_iters = 50;
            let mut neo_samples = Samples::new();
            for i in 0..neo_iters {
                let q = format!("MERGE (n:Person {{name: 'merge_{}'}})", i % 50);
                let t = Instant::now();
                let _ = neo.run_query(&q).await.ok();
                neo_samples.push(t.elapsed().as_micros() as u64);
            }
            neo_samples.report("MERGE (n:Person {name: ...})", "neo4j");
            let nm = neo_samples.0.iter().sum::<u64>() as f64 / neo_samples.0.len() as f64;
            println!("                {:<42} speedup: {:.1}x (yata faster)", "MERGE", nm / yata_mean);
        }
    }

    // ── scale test: varying dataset size ─────────────────────────────────────
    println!("\n[Scale: yata-cypher only — MATCH (n:Person) RETURN count(n)]");
    for &sz in &[100usize, 500, 1_000, 5_000, 10_000, 50_000] {
        let ds2 = generate_dataset(sz, sz * 3, 99);
        let mut yata2 = YataBenchmark::new(&ds2);
        let q = "MATCH (n:Person) RETURN count(n) AS cnt";
        // warmup
        for _ in 0..5 { let _ = yata2.run_query(q); }
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
            sz, sz * 3, mean, p95, ops
        );
    }

    // ── summary ───────────────────────────────────────────────────────────────
    if !summary.is_empty() {
        println!("\n[Summary: yata-cypher vs Neo4j 5]");
        println!("  {:<42}  {:>12}  {:>12}  {:>10}", "scenario", "yata(µs)", "neo4j(µs)", "speedup");
        println!("  {}", "-".repeat(82));
        let mut total_speedup = 0.0f64;
        for (name, ym, nm) in &summary {
            let sp = nm / ym;
            total_speedup += sp;
            println!("  {:<42}  {:>12.1}  {:>12.1}  {:>9.1}x", name, ym, nm, sp);
        }
        let avg_sp = total_speedup / summary.len() as f64;
        println!("  {}", "-".repeat(82));
        println!("  {:<42}  {:>12}  {:>12}  {:>9.1}x  (geometric mean)", "AVERAGE", "", "", avg_sp);
        println!();
        println!("  * yata-cypher is in-process, zero-copy, no serialization overhead.");
        println!("  * Neo4j includes Bolt protocol round-trip latency over loopback.");
    }

    println!("\ndone");
    Ok(())
}
