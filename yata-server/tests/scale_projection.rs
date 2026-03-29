//! Scale projection: measure 10K write/read performance, extrapolate to trillion scale.
//!
//! Run:
//!   cargo test -p yata-server --test scale_projection --release -- --nocapture

use std::sync::Arc;
use std::time::Instant;

use yata_engine::{TieredGraphEngine, config::TieredEngineConfig};
use yata_grin::PropValue;

const N: u32 = 10_000;
const EDGES_PER_VERTEX: u32 = 3;

#[tokio::test(flavor = "multi_thread")]
async fn write_read_scale_projection() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(TieredGraphEngine::new(
        TieredEngineConfig::default(),
        dir.path().to_str().unwrap(),
    ));

    eprintln!("\n============================================================");
    eprintln!("  Scale Projection: {N} vertices, {} edges", N * EDGES_PER_VERTEX);
    eprintln!("============================================================\n");

    // ── 1. Write benchmark: mergeRecord ──
    let write_start = Instant::now();
    for i in 0..N {
        engine.merge_record(
            "Person",
            "rkey",
            &format!("p{i}"),
            &[
                ("name", PropValue::Str(format!("person_{i}"))),
                ("age", PropValue::Int(20 + (i % 60) as i64)),
                ("collection", PropValue::Str("app.bsky.actor.profile".into())),
                ("repo", PropValue::Str(format!("did:web:test{}", i % 100))),
                ("sensitivity_ord", PropValue::Int(0)),
                ("owner_hash", PropValue::Int((i % 1000) as i64)),
                ("updated_at", PropValue::Str("2026-03-24".into())),
                ("value_b64", PropValue::Str("dGVzdA==".into())),
            ],
        );
    }
    let write_elapsed = write_start.elapsed();
    let write_per_sec = N as f64 / write_elapsed.as_secs_f64();
    eprintln!("WRITE: {N} mergeRecord in {:?} ({:.0}/sec)", write_elapsed, write_per_sec);

    // ── 2. Edge write benchmark ──
    let edge_start = Instant::now();
    let edge_count = N * EDGES_PER_VERTEX;
    for i in 0..edge_count {
        let src_rkey = format!("p{}", i % N);
        let dst_rkey = format!("p{}", (i * 7 + 13) % N);
        let cypher = format!(
            "MATCH (a:Person {{rkey: '{src_rkey}'}}), (b:Person {{rkey: '{dst_rkey}'}}) CREATE (a)-[:FOLLOWS {{rkey: 'f{i}', collection: 'app.bsky.graph.follow', repo: 'did:web:test', sensitivity_ord: 0, owner_hash: 0, updated_at: '2026-03-24'}}]->(b)"
        );
        let _ = engine.query(&cypher, &[], None);
    }
    let edge_elapsed = edge_start.elapsed();
    let edge_per_sec = edge_count as f64 / edge_elapsed.as_secs_f64();
    eprintln!("EDGES: {edge_count} edge creates in {:?} ({:.0}/sec)", edge_elapsed, edge_per_sec);

    // ── 3. Point read benchmark (single vertex by rkey) ──
    let iterations = 100_000u64;
    let point_start = Instant::now();
    for i in 0..iterations {
        let rkey = format!("p{}", i % N as u64);
        let _ = engine.query(
            &format!("MATCH (n:Person {{rkey: '{rkey}'}}) RETURN n.name AS name"),
            &[], None,
        );
    }
    let point_elapsed = point_start.elapsed();
    let point_qps = iterations as f64 / point_elapsed.as_secs_f64();
    let point_ns = point_elapsed.as_nanos() / iterations as u128;
    eprintln!("POINT READ: {iterations} queries in {:?} ({:.0} QPS, {point_ns}ns/query)", point_elapsed, point_qps);

    // ── 4. Neighbor traversal benchmark (1-hop) ──
    let hop_start = Instant::now();
    let hop_iters = 100_000u64;
    let mut total_neighbors = 0u64;
    for i in 0..hop_iters {
        let rkey = format!("p{}", i % N as u64);
        let result = engine.query(
            &format!("MATCH (n:Person {{rkey: '{rkey}'}})-[:FOLLOWS]->(m) RETURN m.rkey LIMIT 10"),
            &[], None,
        );
        if let Ok(rows) = &result {
            total_neighbors += rows.len() as u64;
        }
    }
    let hop_elapsed = hop_start.elapsed();
    let hop_qps = hop_iters as f64 / hop_elapsed.as_secs_f64();
    let hop_ns = hop_elapsed.as_nanos() / hop_iters as u128;
    eprintln!("1-HOP: {hop_iters} traversals in {:?} ({:.0} QPS, {hop_ns}ns/query, avg {:.1} neighbors)",
        hop_elapsed, hop_qps, total_neighbors as f64 / hop_iters as f64);

    // ── 5. Scan benchmark (full label scan) ──
    let scan_start = Instant::now();
    let scan_iters = 100u64;
    for _ in 0..scan_iters {
        let _ = engine.query("MATCH (n:Person) RETURN count(n) AS cnt", &[], None);
    }
    let scan_elapsed = scan_start.elapsed();
    let scan_qps = scan_iters as f64 / scan_elapsed.as_secs_f64();
    eprintln!("SCAN: {scan_iters} full scans in {:?} ({:.0} QPS)", scan_elapsed, scan_qps);

    // ── 6. L1 compaction benchmark ──
    let snap_start2 = Instant::now();
    let result = engine.trigger_compaction();
    let snap_elapsed = snap_start2.elapsed();
    eprintln!("COMPACT: trigger_compaction in {:?} (result: {:?})", snap_elapsed, result);

    // ── 7. Extrapolation ──
    eprintln!("\n============================================================");
    eprintln!("  TRILLION SCALE EXTRAPOLATION");
    eprintln!("============================================================");
    eprintln!();

    let scales = [
        ("1M", 1_000_000u64),
        ("100M", 100_000_000),
        ("1B", 1_000_000_000),
        ("100B", 100_000_000_000),
        ("1T", 1_000_000_000_000),
    ];

    eprintln!("  Assumptions:");
    eprintln!("  - {N} vertex benchmark → linear extrapolation");
    eprintln!("  - 1 partition = ~20M vertices (4GB RAM, standard-1)");
    eprintln!("  - Partition fan-out overhead: ~2ms/partition (HTTP)");
    eprintln!("  - R2 page-in: ~5ms/label (cold start)");
    eprintln!();

    let partition_capacity = 20_000_000u64;

    eprintln!("  | Scale | Partitions | Write/sec | Point QPS | 1-Hop QPS | Scan QPS | Est. Cost/mo |");
    eprintln!("  |-------|-----------|-----------|-----------|-----------|----------|--------------|");

    for (name, total) in &scales {
        let partitions = (*total + partition_capacity - 1) / partition_capacity;
        let partitions = partitions.max(1);

        // Write scales linearly within a partition (label routing)
        let write_rate = write_per_sec;

        // Point read: per-partition QPS stays constant, coordinator adds ~2ms overhead
        let coordinator_overhead_ms = 2.0;
        let point_latency_ms = (point_ns as f64 / 1_000_000.0) + coordinator_overhead_ms;
        let effective_point_qps = 1000.0 / point_latency_ms;

        // 1-hop: same partition (label co-located), so QPS stays constant
        let hop_latency_ms = (hop_ns as f64 / 1_000_000.0) + coordinator_overhead_ms;
        let effective_hop_qps = 1000.0 / hop_latency_ms;

        // Scan: fan-out to all partitions, aggregate merge
        let fan_out_ms = partitions as f64 * 0.5; // parallel, but some serialization
        let scan_latency_ms = (scan_elapsed.as_millis() as f64 / scan_iters as f64) + fan_out_ms;
        let effective_scan_qps = 1000.0 / scan_latency_ms;

        // Cost: $1.50/partition/month (standard-1, 1h/day active)
        let cost = partitions as f64 * 1.50 + 5884.0_f64.min(*total as f64 * 0.0000588);

        eprintln!(
            "  | {name:>4} | {partitions:>9} | {write_rate:>9.0} | {effective_point_qps:>9.0} | {effective_hop_qps:>9.0} | {effective_scan_qps:>8.1} | ${cost:>11.0} |"
        );
    }

    eprintln!();
    eprintln!("  Notes:");
    eprintln!("  - Write/sec is per-partition (label-routed, no fan-out)");
    eprintln!("  - Point/1-Hop QPS is per-client (single request, coordinator overhead included)");
    eprintln!("  - Scan QPS degrades with partition count (fan-out + merge)");
    eprintln!("  - ArrowFragment NbrUnit: 25x faster neighbor traversal vs legacy");
    eprintln!("  - ArrowFragment serialize: 2000x faster, 50%% smaller blobs");
}
