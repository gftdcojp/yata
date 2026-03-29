//! COO Read Container benchmark: measures query performance with R2 page-in simulation.
//!
//! Benchmarks the Sorted COO read path:
//!   1. Arrow IPC segment serialize/deserialize throughput
//!   2. Cold start: per-label segment load (disk mmap vs simulated R2 latency)
//!   3. Query latency (point read, 1-hop, label scan, full scan) after page-in
//!   4. Per-label on-demand enrichment cost
//!   5. R2 page-in cost breakdown (deserialization + wal_apply + commit)
//!
//! Usage:
//!   cargo run -p yata-bench --bin coo-read-bench --release
//!
//! Environment:
//!   NODES     (default: 5000)
//!   EDGES     (default: 15000)
//!   LABELS    (default: 10)
//!   ITERS     (default: 500)
//!   R2_LATENCY_MS  (default: 4) — simulated R2 GET latency in ms

use std::collections::HashSet;
use std::time::Instant;

use anyhow::Result;
use rand::{Rng, SeedableRng, rngs::StdRng};
use yata_engine::TieredGraphEngine;
use yata_engine::config::TieredEngineConfig;

// ── latency tracker ──────────────────────────────────────────────────────────

struct Lat(Vec<u64>);
impl Lat {
    fn new() -> Self { Self(Vec::new()) }
    fn push_ns(&mut self, ns: u64) { self.0.push(ns); }
    fn report(&mut self, tier: &str, scenario: &str) {
        self.0.sort_unstable();
        let n = self.0.len();
        if n == 0 {
            println!("  [{tier:<30}] {scenario:<50} (no data)");
            return;
        }
        let mean_ns = self.0.iter().sum::<u64>() as f64 / n as f64;
        let p50 = self.0[n * 50 / 100];
        let p95 = self.0[n * 95 / 100];
        let p99 = self.0[n.saturating_sub(1).min(n * 99 / 100)];
        let max = *self.0.last().unwrap();
        let mean_us = mean_ns / 1000.0;
        let ops = if mean_ns > 0.0 { 1_000_000_000.0 / mean_ns } else { 0.0 };
        println!(
            "  [{tier:<30}] {scenario:<50} n={n:>5}  mean={mean_us:>10.1}µs  p50={:>8.1}µs  p95={:>8.1}µs  p99={:>8.1}µs  max={:>10.1}µs  {ops:>10.0} ops/s",
            p50 as f64 / 1000.0,
            p95 as f64 / 1000.0,
            p99 as f64 / 1000.0,
            max as f64 / 1000.0,
        );
    }
    fn mean_us(&self) -> f64 {
        if self.0.is_empty() { return 0.0; }
        self.0.iter().sum::<u64>() as f64 / self.0.len() as f64 / 1000.0
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn make_engine(dir: &std::path::Path) -> TieredGraphEngine {
    TieredGraphEngine::new(TieredEngineConfig::default(), dir.to_str().unwrap())
}

// ── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let n_nodes = env_usize("NODES", 5000);
    let n_edges = env_usize("EDGES", 15000);
    let n_labels = env_usize("LABELS", 10);
    let iters = env_usize("ITERS", 500);
    let r2_latency_ms = env_usize("R2_LATENCY_MS", 4);

    println!("╔═══════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║  yata COO Read Container Benchmark — Sorted COO + R2 Page-In Simulation             ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════════════╝");
    println!("  Dataset  : {n_nodes} nodes, {n_edges} edges, {n_labels} labels");
    println!("  Iters    : {iters}");
    println!("  R2 sim   : {r2_latency_ms}ms per segment GET");
    println!();

    let dir = tempfile::tempdir()?;
    let data_dir = dir.path().join("coo-bench");

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 1: Build dataset — populate engine with multi-label graph
    // ═══════════════════════════════════════════════════════════════════════════
    println!("─── Phase 1: Build dataset ({n_nodes} nodes, {n_edges} edges, {n_labels} labels) ────");
    let e = make_engine(&data_dir);
    let mut rng = StdRng::seed_from_u64(42);

    let labels: Vec<String> = (0..n_labels).map(|i| format!("Label{i}")).collect();
    let rel_types = ["FOLLOWS", "OWNS", "LIKES", "MANAGES", "REPORTS_TO"];

    // Create nodes
    let t_build = Instant::now();
    for i in 0..n_nodes {
        let lbl = &labels[i % n_labels];
        let name = format!("{lbl}_{i}");
        let score = rng.gen_range(0..1000i64);
        let region = format!("region_{}", i % 20);
        let cypher = format!(
            "CREATE (n:{lbl} {{rkey: 'n{i}', name: '{name}', score: {score}, region: '{region}'}})"
        );
        e.query(&cypher, &[], None).unwrap();
    }

    // Create edges
    for i in 0..n_edges {
        let src = rng.gen_range(0..n_nodes);
        let mut dst = rng.gen_range(0..n_nodes);
        while dst == src { dst = rng.gen_range(0..n_nodes); }
        let rt = rel_types[i % rel_types.len()];
        let cypher = format!(
            "MATCH (a {{rkey: 'n{src}'}}), (b {{rkey: 'n{dst}'}}) CREATE (a)-[:{rt}]->(b)"
        );
        let _ = e.query(&cypher, &[], None);
    }
    let build_ms = t_build.elapsed().as_millis();
    println!("  Build time: {}ms", build_ms);

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 2: Arrow IPC serialize/deserialize throughput
    // ═══════════════════════════════════════════════════════════════════════════
    println!("\n─── Phase 2: Arrow IPC Segment Serialize/Deserialize ──────────────────────────");

    // Create WAL entries to simulate compacted segments
    let mut wal_entries = Vec::new();
    let mut rng2 = StdRng::seed_from_u64(99);
    for i in 0..n_nodes {
        let lbl = &labels[i % n_labels];
        let name = format!("{lbl}_{i}");
        let score_val = rng2.gen_range(0..1000i64);
        let region = format!("region_{}", i % 20);
        wal_entries.push(yata_engine::wal::WalEntry {
            seq: i as u64,
            op: yata_engine::wal::WalOp::Upsert,
            label: lbl.clone(),
            pk_key: "rkey".to_string(),
            pk_value: format!("n{i}"),
            props: vec![
                ("rkey".to_string(), yata_grin::PropValue::Str(format!("n{i}"))),
                ("name".to_string(), yata_grin::PropValue::Str(name)),
                ("score".to_string(), yata_grin::PropValue::Int(score_val)),
                ("region".to_string(), yata_grin::PropValue::Str(region)),
            ],
            timestamp_ms: 1711700000000 + i as u64,
        });
    }

    // Serialize full segment
    let t_ser = Instant::now();
    let segment_data = yata_engine::arrow_wal::serialize_segment_arrow(&wal_entries)
        .expect("serialize");
    let ser_ms = t_ser.elapsed().as_millis();
    let segment_bytes = segment_data.len();
    println!("  Serialize {n_nodes} entries → {} bytes ({:.1} KB): {}ms",
        segment_bytes, segment_bytes as f64 / 1024.0, ser_ms);

    // Deserialize benchmark
    let mut deser_lat = Lat::new();
    for _ in 0..50 {
        let t = Instant::now();
        let entries = yata_engine::arrow_wal::deserialize_segment_auto("test.arrow", &segment_data);
        deser_lat.push_ns(t.elapsed().as_nanos() as u64);
        assert_eq!(entries.len(), n_nodes);
    }
    deser_lat.report("Arrow IPC deser",
        &format!("{n_nodes} entries from {} KB", segment_bytes / 1024));

    // Per-label segment serialize/deserialize
    let dirty_all: HashSet<String> = labels.iter().cloned().collect();
    let segments_input = vec![("full.arrow", segment_data.as_ref())];
    let t_per_label = Instant::now();
    let per_label_results = yata_engine::compaction::compact_segments_by_label(
        &segments_input, &dirty_all,
    ).expect("compact by label");
    let per_label_ms = t_per_label.elapsed().as_millis();

    let total_label_bytes: usize = per_label_results.iter().map(|r| r.data.len()).sum();
    println!("  Per-label compaction ({} labels): {}ms, total {} KB",
        per_label_results.len(), per_label_ms, total_label_bytes / 1024);
    for r in &per_label_results {
        println!("    {:<12}: {} entries, {} bytes ({:.1} KB)",
            r.label, r.entry_count, r.data.len(), r.data.len() as f64 / 1024.0);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 3: Cold start — simulate R2 page-in with latency
    // ═══════════════════════════════════════════════════════════════════════════
    println!("\n─── Phase 3: Cold Start — R2 Page-In Cost Breakdown ──────────────────────────");

    // 3a: Cold start WITHOUT R2 latency (pure compute cost)
    {
        let cold_dir = dir.path().join("cold-bench");
        let e_cold = make_engine(&cold_dir);

        let t_total = Instant::now();
        let mut deser_total_ns = 0u64;
        let mut apply_total_ns = 0u64;
        let mut total_entries = 0usize;

        for r in &per_label_results {
            let t_d = Instant::now();
            let entries = yata_engine::arrow_wal::deserialize_segment_auto(
                &format!("{}.arrow", r.label), &r.data,
            );
            deser_total_ns += t_d.elapsed().as_nanos() as u64;

            let t_a = Instant::now();
            let _ = e_cold.wal_apply(&entries);
            apply_total_ns += t_a.elapsed().as_nanos() as u64;
            total_entries += entries.len();
        }
        let total_ms = t_total.elapsed().as_millis();

        println!("  Cold start (disk, no R2 latency):");
        println!("    Total         : {}ms ({} entries across {} labels)",
            total_ms, total_entries, per_label_results.len());
        println!("    Deserialize   : {:.1}ms", deser_total_ns as f64 / 1_000_000.0);
        println!("    wal_apply     : {:.1}ms", apply_total_ns as f64 / 1_000_000.0);
        println!("    Per-label avg : {:.1}ms deser + {:.1}ms apply",
            deser_total_ns as f64 / 1_000_000.0 / per_label_results.len() as f64,
            apply_total_ns as f64 / 1_000_000.0 / per_label_results.len() as f64);

        // Query performance on cold-started engine
        println!("\n  Query performance (after cold start, {iters} iters):");

        let read_queries: &[(&str, &str)] = &[
            ("MATCH (n) RETURN count(n) AS cnt", "full scan count(*)"),
            ("MATCH (n:Label0) RETURN count(n) AS cnt", "label scan count(Label0)"),
            ("MATCH (n {rkey: 'n42'}) RETURN n.name AS name LIMIT 1", "point read (rkey=n42)"),
            ("MATCH (n:Label0) WHERE n.score > 500 RETURN count(n) AS cnt", "filter score>500 on Label0"),
            ("MATCH (a:Label0)-[:FOLLOWS]->(b) RETURN count(*) AS cnt", "1-hop FOLLOWS from Label0"),
            ("MATCH (a:Label0)-[:FOLLOWS]->(b)-[:OWNS]->(c) RETURN count(*) AS cnt", "2-hop FOLLOWS→OWNS"),
        ];

        for &(cypher, scenario) in read_queries {
            for _ in 0..5 { let _ = e_cold.query(cypher, &[], None); }
            let mut lat = Lat::new();
            for _ in 0..iters {
                let t = Instant::now();
                let _ = e_cold.query(cypher, &[], None);
                lat.push_ns(t.elapsed().as_nanos() as u64);
            }
            lat.report("COO cold-start (disk)", scenario);
        }
    }

    // 3b: Cold start WITH simulated R2 latency
    {
        let cold_dir2 = dir.path().join("cold-r2-bench");
        let e_cold2 = make_engine(&cold_dir2);

        let t_total = Instant::now();
        for r in &per_label_results {
            std::thread::sleep(std::time::Duration::from_millis(r2_latency_ms as u64));
            let entries = yata_engine::arrow_wal::deserialize_segment_auto(
                &format!("{}.arrow", r.label), &r.data,
            );
            let _ = e_cold2.wal_apply(&entries);
        }
        let total_ms = t_total.elapsed().as_millis();
        let r2_overhead_ms = r2_latency_ms * per_label_results.len();

        println!("\n  Cold start (R2, {}ms latency × {} labels):", r2_latency_ms, per_label_results.len());
        println!("    Total         : {}ms (R2 overhead ~{}ms)", total_ms, r2_overhead_ms);
        println!("    Compute only  : ~{}ms", total_ms as usize - r2_overhead_ms);

        println!("\n  Query performance (after R2 cold start, {iters} iters):");
        let read_queries: &[(&str, &str)] = &[
            ("MATCH (n) RETURN count(n) AS cnt", "full scan count(*)"),
            ("MATCH (n:Label0) RETURN count(n) AS cnt", "label scan count(Label0)"),
            ("MATCH (n {rkey: 'n42'}) RETURN n.name AS name LIMIT 1", "point read (rkey=n42)"),
            ("MATCH (a:Label0)-[:FOLLOWS]->(b) RETURN count(*) AS cnt", "1-hop FOLLOWS from Label0"),
        ];

        for &(cypher, scenario) in read_queries {
            for _ in 0..5 { let _ = e_cold2.query(cypher, &[], None); }
            let mut lat = Lat::new();
            for _ in 0..iters {
                let t = Instant::now();
                let _ = e_cold2.query(cypher, &[], None);
                lat.push_ns(t.elapsed().as_nanos() as u64);
            }
            lat.report("COO R2-paged", scenario);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 4: Incremental label page-in cost (on-demand enrichment)
    // ═══════════════════════════════════════════════════════════════════════════
    println!("\n─── Phase 4: Incremental Label Page-In (On-Demand) ──────────────────────────");

    for batch_size in [1, 3, 5, 10].iter().copied().filter(|&b| b <= n_labels) {
        let inc_dir = dir.path().join(format!("inc-bench-{batch_size}"));
        let e_inc = make_engine(&inc_dir);

        // Load initial 2 labels
        for r in per_label_results.iter().take(2) {
            let entries = yata_engine::arrow_wal::deserialize_segment_auto(
                &format!("{}.arrow", r.label), &r.data,
            );
            let _ = e_inc.wal_apply(&entries);
        }

        let mut lat = Lat::new();
        let range = per_label_results.len().saturating_sub(2 + batch_size).max(1);
        for trial in 0..20 {
            let start_idx = 2 + (trial % range);
            let t = Instant::now();
            for r in per_label_results.iter().skip(start_idx).take(batch_size) {
                std::thread::sleep(std::time::Duration::from_millis(r2_latency_ms as u64));
                let entries = yata_engine::arrow_wal::deserialize_segment_auto(
                    &format!("{}.arrow", r.label), &r.data,
                );
                let _ = e_inc.wal_apply(&entries);
            }
            lat.push_ns(t.elapsed().as_nanos() as u64);
        }
        lat.report("Incremental page-in",
            &format!("{batch_size} labels (R2 {r2_latency_ms}ms each)"));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 5: Scale projection — varying dataset sizes
    // ═══════════════════════════════════════════════════════════════════════════
    println!("\n─── Phase 5: Scale Projection ──────────────────────────────────────────────");
    println!("  {:<12} {:<12} {:<12} {:<12} {:<14} {:<14} {:<14}",
        "Nodes", "Segment KB", "Deser µs", "Apply µs", "Cold(disk)ms", "Cold(R2)ms", "Point QPS");

    for scale in [1000, 5000, 10000, 50000] {
        if scale > n_nodes { continue; }
        let subset: Vec<_> = wal_entries.iter().take(scale).cloned().collect();
        let seg = yata_engine::arrow_wal::serialize_segment_arrow(&subset).unwrap();
        let seg_kb = seg.len() / 1024;

        let t_d = Instant::now();
        let entries = yata_engine::arrow_wal::deserialize_segment_auto("test.arrow", &seg);
        let deser_us = t_d.elapsed().as_micros();

        let scale_dir = dir.path().join(format!("scale-{scale}"));
        let e_scale = make_engine(&scale_dir);

        let t_a = Instant::now();
        let _ = e_scale.wal_apply(&entries);
        let apply_us = t_a.elapsed().as_micros();

        let cold_disk_ms = (deser_us + apply_us) as f64 / 1000.0;
        let cold_r2_ms = cold_disk_ms + r2_latency_ms as f64;

        // Point read QPS
        let mut point_lat = Lat::new();
        for i in 0..200 {
            let nid = i % scale;
            let cypher = format!("MATCH (n {{rkey: 'n{nid}'}}) RETURN n.name LIMIT 1");
            let t = Instant::now();
            let _ = e_scale.query(&cypher, &[], None);
            point_lat.push_ns(t.elapsed().as_nanos() as u64);
        }
        let point_qps = if point_lat.mean_us() > 0.0 {
            1_000_000.0 / point_lat.mean_us()
        } else { 0.0 };

        println!("  {:<12} {:<12} {:<12} {:<12} {:<14.1} {:<14.1} {:<14.0}",
            scale, seg_kb, deser_us, apply_us, cold_disk_ms, cold_r2_ms, point_qps);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Phase 6: Warm query comparison — baseline for COO overhead analysis
    // ═══════════════════════════════════════════════════════════════════════════
    println!("\n─── Phase 6: Warm Query (after full load, GIE on CSR) ─────────────────────────");
    println!("  Query performance AFTER all labels loaded (COO → CSR applied).");
    println!("  COO overhead = cold start cost only; warm query = identical to CSR.\n");

    let warm_queries: &[(&str, &str)] = &[
        ("MATCH (n) RETURN count(n) AS cnt", "full scan count(*)"),
        ("MATCH (n:Label0) RETURN count(n) AS cnt", "label scan count(Label0)"),
        ("MATCH (n {rkey: 'n42'}) RETURN n.name LIMIT 1", "point read (rkey=n42)"),
        ("MATCH (n:Label0) WHERE n.score > 500 RETURN count(n)", "filter score>500"),
        ("MATCH (a:Label0)-[:FOLLOWS]->(b) RETURN count(*)", "1-hop FOLLOWS"),
        ("MATCH (a:Label0)-[:FOLLOWS]->(b)-[:OWNS]->(c) RETURN count(*)", "2-hop FOLLOWS→OWNS"),
    ];

    for &(cypher, scenario) in warm_queries {
        for _ in 0..10 { let _ = e.query(cypher, &[], None); }
        let mut lat = Lat::new();
        for _ in 0..iters {
            let t = Instant::now();
            let _ = e.query(cypher, &[], None);
            lat.push_ns(t.elapsed().as_nanos() as u64);
        }
        lat.report("Warm (all labels loaded)", scenario);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Summary
    // ═══════════════════════════════════════════════════════════════════════════
    println!("\n═══════════════════════════════════════════════════════════════════════════════════════");
    println!("  ANALYSIS: COO Read Container Performance");
    println!("═══════════════════════════════════════════════════════════════════════════════════════");
    println!();
    println!("  Key takeaways:");
    println!("  - Cold start cost = R2 GET ({r2_latency_ms}ms/label) + Arrow deser + wal_apply");
    println!("  - After page-in, query performance = CSR warm (no COO overhead)");
    println!("  - Per-label granularity: first query triggers label load");
    println!("  - R2 latency dominates cold start (~{r2_latency_ms}ms vs ~µs deser+apply)");
    println!("  - Segment size grows linearly with entries");
    println!();
    println!("  done");
    Ok(())
}
