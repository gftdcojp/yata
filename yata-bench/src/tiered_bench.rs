//! Tiered storage load test: TieredGraphEngine with disk WAL + LocalCAS + MDAG delta commit.
//!
//! Measures the FULL production write/read path:
//!   Write: Cypher mutation → CSR → DurableWal fsync → async MDAG delta commit → LocalCAS
//!   Read:  Cypher query → GIE executor on CSR (with lazy label loading from CAS)
//!   Restart: MDAG HEAD restore → CAS fetch → CSR rebuild → query
//!
//! Usage:
//!   cargo run -p yata-bench --bin tiered-bench --release
//!
//! Environment variables:
//!   NODES     (default: 1000)
//!   EDGES     (default: 3000)
//!   ITERS     (default: 200)

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use rand::{Rng, SeedableRng, rngs::StdRng};
use yata_cas::LocalCasStore;
use yata_engine::TieredGraphEngine;
use yata_engine::config::{PersistenceMode, TieredEngineConfig};

// ── latency tracking ─────────────────────────────────────────────────────────

/// Latency tracker in nanoseconds for sub-microsecond precision.
struct Lat(Vec<u64>);
impl Lat {
    fn new() -> Self {
        Self(Vec::new())
    }
    fn push_ns(&mut self, ns: u64) {
        self.0.push(ns);
    }
    fn report(&mut self, tier: &str, scenario: &str) {
        self.0.sort_unstable();
        let n = self.0.len();
        if n == 0 {
            println!("  [{tier}] {scenario}: no data");
            return;
        }
        let mean_ns = self.0.iter().sum::<u64>() as f64 / n as f64;
        let p50 = self.0[n * 50 / 100];
        let p95 = self.0[n * 95 / 100];
        let p99 = self.0[n.saturating_sub(1).min(n * 99 / 100)];
        let max = *self.0.last().unwrap();
        let mean_us = mean_ns / 1000.0;
        let ops = 1_000_000_000.0 / mean_ns;
        println!(
            "  [{tier:<26}] {scenario:<48} n={n:>5}  mean={mean_us:>10.1}µs  p50={:>8.1}µs  p95={:>8.1}µs  p99={:>8.1}µs  max={:>10.1}µs  {ops:>8.0} ops/s",
            p50 as f64 / 1000.0,
            p95 as f64 / 1000.0,
            p99 as f64 / 1000.0,
            max as f64 / 1000.0,
        );
    }
}

fn engine_config_snapshot() -> TieredEngineConfig {
    let mut c = TieredEngineConfig::default();
    c.persistence_mode = PersistenceMode::Snapshot;
    c.enable_mdag = true;
    c.batch_commit_threshold = 50;
    c.batch_commit_timeout_ms = 5000;
    c
}

fn engine_config() -> TieredEngineConfig {
    engine_config_snapshot()
}

fn make_engine(dir: &std::path::Path) -> TieredGraphEngine {
    TieredGraphEngine::new(engine_config(), dir.to_str().unwrap())
}

fn make_engine_with_cas(
    dir: &std::path::Path,
    cas: Arc<dyn yata_cas::CasStore>,
) -> TieredGraphEngine {
    let config = engine_config();
    TieredGraphEngine::with_cas(config, dir.to_str().unwrap(), cas)
}

fn query(e: &TieredGraphEngine, cypher: &str) -> Vec<Vec<(String, String)>> {
    e.query(cypher, &[], None).unwrap()
}

// ── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let n_nodes: usize = std::env::var("NODES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let n_edges: usize = std::env::var("EDGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);
    let iters: usize = std::env::var("ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);

    println!(
        "╔════════════════════════════════════════════════════════════════════════════════════╗"
    );
    println!(
        "║  yata TieredGraphEngine load test — WAL + Snapshot (incremental CSR)             ║"
    );
    println!(
        "╚════════════════════════════════════════════════════════════════════════════════════╝"
    );
    println!("  Dataset : {n_nodes} nodes  {n_edges} edges");
    println!("  Iters   : {iters}");
    println!();

    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("tiered-bench");

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 1: Bulk write (CREATE mutations through TieredGraphEngine)
    // ═══════════════════════════════════════════════════════════════════════════
    println!("─── Phase 1: Bulk write ──────────────────────────────────────────────────────────");
    let cas = Arc::new(LocalCasStore::new(data_dir.join("cas")).await?);
    let e = make_engine_with_cas(&data_dir, cas.clone());

    let mut rng = StdRng::seed_from_u64(42);
    let labels = ["Person", "Company", "Product"];
    let rel_types = ["KNOWS", "WORKS_AT", "USES", "MANAGES"];

    // -- Writes: CREATE nodes --
    let t_bulk = Instant::now();
    let mut write_lats = Lat::new();
    for i in 0..n_nodes {
        let lbl = labels[i % labels.len()];
        let name = format!("{lbl}_{i}");
        let age = rng.gen_range(18..80i64);
        let cypher = format!("CREATE (n:{lbl} {{_doc_id: 'n{i}', name: '{name}', age: {age}}})");
        let t = Instant::now();
        e.query(&cypher, &[], None).unwrap();
        write_lats.push_ns(t.elapsed().as_nanos() as u64);
    }
    write_lats.report("Disk+CAS", "CREATE node");

    // -- Writes: CREATE edges --
    let mut edge_lats = Lat::new();
    for i in 0..n_edges {
        let src = rng.gen_range(0..n_nodes);
        let mut dst = rng.gen_range(0..n_nodes);
        while dst == src {
            dst = rng.gen_range(0..n_nodes);
        }
        let rt = rel_types[i % rel_types.len()];
        let cypher = format!(
            "MATCH (a {{_doc_id: 'n{src}'}}), (b {{_doc_id: 'n{dst}'}}) CREATE (a)-[:{rt}]->(b)"
        );
        let t = Instant::now();
        let _ = e.query(&cypher, &[], None);
        edge_lats.push_ns(t.elapsed().as_nanos() as u64);
    }
    edge_lats.report("Disk+CAS", "CREATE edge");

    let bulk_elapsed = t_bulk.elapsed();
    println!(
        "  Bulk load total: {:.1}s ({} nodes + {} edges)",
        bulk_elapsed.as_secs_f64(),
        n_nodes,
        n_edges
    );

    // Trigger final MDAG commit via a dummy write (threshold=50 so bulk writes may not have committed yet)
    query(&e, "CREATE (n:_Flush {_doc_id: '_flush'})");
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Check WAL and MDAG state
    let head = e.mdag_head();
    println!(
        "  MDAG HEAD: {}",
        head.map(|h| h.hex()).unwrap_or("none".into())
    );

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 2: Read queries on warm CSR (data in memory, backed by disk+CAS)
    // ═══════════════════════════════════════════════════════════════════════════
    println!("\n─── Phase 2: Read queries (warm CSR) ──────────────────────────────────────────");

    let read_queries: &[(&str, &str)] = &[
        (
            "MATCH (n:Person) RETURN count(n) AS cnt",
            "label scan count(Person)",
        ),
        (
            "MATCH (n:Person) WHERE n.age > 50 RETURN count(n) AS cnt",
            "property filter age>50",
        ),
        (
            "MATCH (n {_doc_id: 'n42'}) RETURN n.name AS name",
            "point lookup by _doc_id",
        ),
        ("MATCH (n) RETURN count(n) AS total", "full scan count(*)"),
        (
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS cnt",
            "1-hop traversal KNOWS",
        ),
    ];

    // Queries with cache bypass: use varying params to avoid query cache hits
    {
        // label scan — same query, cache will hit after first
        let mut lat = Lat::new();
        for i in 0..iters {
            // Vary the filter threshold to bypass cache
            let age_threshold = 20 + (i % 60) as i64;
            let cypher =
                format!("MATCH (n:Person) WHERE n.age > {age_threshold} RETURN count(n) AS cnt");
            let t = Instant::now();
            let _ = e.query(&cypher, &[], None);
            lat.push_ns(t.elapsed().as_nanos() as u64);
        }
        lat.report("Warm CSR (disk-backed)", "property filter age>N (varied)");
    }
    {
        let mut lat = Lat::new();
        for i in 0..iters {
            let nid = i % n_nodes;
            let cypher = format!("MATCH (n {{_doc_id: 'n{nid}'}}) RETURN n.name AS name");
            let t = Instant::now();
            let _ = e.query(&cypher, &[], None);
            lat.push_ns(t.elapsed().as_nanos() as u64);
        }
        lat.report("Warm CSR (disk-backed)", "point lookup by _doc_id (varied)");
    }
    for &(cypher, label) in read_queries {
        // warmup
        for _ in 0..3 {
            let _ = query(&e, cypher);
        }
        let mut lat = Lat::new();
        for _ in 0..iters {
            let t = Instant::now();
            let _ = query(&e, cypher);
            lat.push_ns(t.elapsed().as_nanos() as u64);
        }
        lat.report("Warm CSR (cached)", label);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 3: Mixed read/write workload
    // ═══════════════════════════════════════════════════════════════════════════
    println!("\n─── Phase 3: Mixed workload (80% read / 20% write) ──────────────────────────");

    let mut mixed_read = Lat::new();
    let mut mixed_write = Lat::new();
    let mixed_iters = iters * 2;
    let mut rng2 = StdRng::seed_from_u64(99);

    for i in 0..mixed_iters {
        if rng2.gen_ratio(1, 5) {
            // 20% writes
            let id = n_nodes + i;
            let cypher =
                format!("CREATE (n:Person {{_doc_id: 'mix{id}', name: 'mixed_{id}', age: 30}})");
            let t = Instant::now();
            let _ = e.query(&cypher, &[], None);
            mixed_write.push_ns(t.elapsed().as_nanos() as u64);
        } else {
            // 80% reads
            let nid = rng2.gen_range(0..n_nodes);
            let cypher = format!("MATCH (n {{_doc_id: 'n{nid}'}}) RETURN n.name AS name");
            let t = Instant::now();
            let _ = query(&e, &cypher);
            mixed_read.push_ns(t.elapsed().as_nanos() as u64);
        }
    }
    mixed_read.report("Mixed 80/20", "read (point lookup)");
    mixed_write.report("Mixed 80/20", "write (CREATE node)");

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 4: Cold restart — drop engine, restore from MDAG + CAS
    // ═══════════════════════════════════════════════════════════════════════════
    println!("\n─── Phase 4: Cold restart from MDAG + CAS ──────────────────────────────────");
    drop(e);

    let t_restart = Instant::now();
    let e2 = make_engine_with_cas(&data_dir, cas.clone());
    let restart_us = t_restart.elapsed().as_micros();
    println!("  Engine restart (RootIndex load): {}µs", restart_us);

    // First query triggers lazy label loading from CAS
    let t_cold = Instant::now();
    let rows = query(&e2, "MATCH (n:Person) RETURN count(n) AS cnt");
    let cold_us = t_cold.elapsed().as_micros();
    println!(
        "  Cold query (label fetch + GIE): {}µs  result={:?}",
        cold_us, rows
    );

    // Warm queries after cold start
    for &(cypher, label) in read_queries {
        for _ in 0..5 {
            let _ = query(&e2, cypher);
        }
        let mut lat = Lat::new();
        for _ in 0..iters {
            let t = Instant::now();
            let _ = query(&e2, cypher);
            lat.push_ns(t.elapsed().as_nanos() as u64);
        }
        lat.report("Post-restart warm CSR", label);
    }

    // Verify data integrity after restart — query each label to trigger lazy load
    let person_cnt = query(&e2, "MATCH (n:Person) RETURN count(n) AS cnt");
    let company_cnt = query(&e2, "MATCH (n:Company) RETURN count(n) AS cnt");
    let product_cnt = query(&e2, "MATCH (n:Product) RETURN count(n) AS cnt");
    println!(
        "\n  Data integrity after restart: Person={:?} Company={:?} Product={:?}",
        person_cnt, company_cnt, product_cnt
    );

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 5: WAL fsync latency isolation
    // ═══════════════════════════════════════════════════════════════════════════
    println!("\n─── Phase 5: WAL fsync latency (single writes) ──────────────────────────────");

    let wal_dir = tempfile::tempdir()?;
    let wal_data = wal_dir.path().join("wal-bench");
    let wal_cas = Arc::new(LocalCasStore::new(wal_data.join("cas")).await?);
    let mut wal_config = engine_config();
    wal_config.batch_commit_threshold = 10000; // disable background commit during WAL bench
    let e3 = TieredGraphEngine::with_cas(wal_config, wal_data.to_str().unwrap(), wal_cas);

    let mut wal_lat = Lat::new();
    for i in 0..500 {
        let cypher = format!("CREATE (n:WalTest {{_doc_id: 'w{i}', val: {i}}})");
        let t = Instant::now();
        e3.query(&cypher, &[], None).unwrap();
        wal_lat.push_ns(t.elapsed().as_nanos() as u64);
    }
    wal_lat.report("WAL fsync only", "CREATE single node (no MDAG commit)");

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 6: MDAG delta commit latency
    // ═══════════════════════════════════════════════════════════════════════════
    println!("\n─── Phase 6: MDAG delta commit latency ──────────────────────────────────────");

    let commit_dir = tempfile::tempdir()?;
    let commit_data = commit_dir.path().join("commit-bench");
    let commit_cas = Arc::new(LocalCasStore::new(commit_data.join("cas")).await?);
    let mut commit_config = engine_config();
    commit_config.batch_commit_threshold = 1; // commit every mutation
    commit_config.batch_commit_timeout_ms = 0;
    let e4 = TieredGraphEngine::with_cas(commit_config, commit_data.to_str().unwrap(), commit_cas);

    let mut commit_lat = Lat::new();
    for i in 0..100 {
        let cypher = format!("CREATE (n:CommitTest {{_doc_id: 'c{i}', val: {i}}})");
        let t = Instant::now();
        e4.query(&cypher, &[], None).unwrap();
        commit_lat.push_ns(t.elapsed().as_nanos() as u64);
    }
    commit_lat.report("WAL + MDAG commit", "CREATE node (per-op commit)");

    println!(
        "\n═══════════════════════════════════════════════════════════════════════════════════"
    );
    println!("  done");
    Ok(())
}
