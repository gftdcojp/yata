//! E2E tests for WAL Projection architecture.
//!
//! Requires: `docker compose -f docker-compose.wal.yml up -d --build`
//!
//! Run: `cargo test -p yata-server --test e2e_wal_projection -- --nocapture --test-threads=1`

use serde_json::{json, Value};
use std::time::Instant;

const WRITE_URL: &str = "http://localhost:8083";
const READ0_URL: &str = "http://localhost:8090";
const READ1_URL: &str = "http://localhost:8091";

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap()
}

fn post(url: &str, body: Value) -> Result<Value, String> {
    let resp = client()
        .post(url)
        .header("Content-Type", "application/json")
        .header("X-Magatama-Verified", "true")
        .json(&body)
        .send()
        .map_err(|e| format!("{e}"))?;
    let text = resp.text().map_err(|e| format!("{e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("parse: {e}: {text}"))
}

fn health_check(url: &str) -> bool {
    client()
        .get(&format!("{url}/health"))
        .send()
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

fn skip_if_not_running() -> bool {
    if !health_check(WRITE_URL) {
        eprintln!("SKIP: yata-write not running at {WRITE_URL}. Run: docker compose -f docker-compose.wal.yml up -d --build");
        return true;
    }
    false
}

// ── Test 1: mergeRecordWal returns WalEntry ──

#[test]
fn test_merge_record_wal_returns_entry() {
    if skip_if_not_running() { return; }

    let result = post(
        &format!("{WRITE_URL}/xrpc/ai.gftd.yata.mergeRecordWal"),
        json!({
            "label": "WalTest",
            "pk_key": "rkey",
            "pk_value": "wal_test_1",
            "props": {"text": "hello WAL", "num": 42}
        }),
    ).unwrap();

    assert_eq!(result["ok"], true, "mergeRecordWal should succeed: {result}");
    let entry = &result["wal_entry"];
    assert!(entry["seq"].as_u64().unwrap() > 0, "seq should be > 0");
    assert_eq!(entry["op"], "upsert");
    assert_eq!(entry["label"], "WalTest");
    assert_eq!(entry["pk_value"], "wal_test_1");
    eprintln!("  mergeRecordWal: seq={}", entry["seq"]);
}

// ── Test 2: walTail returns entries ──

#[test]
fn test_wal_tail() {
    if skip_if_not_running() { return; }

    for i in 0..5 {
        post(
            &format!("{WRITE_URL}/xrpc/ai.gftd.yata.mergeRecordWal"),
            json!({
                "label": "TailTest",
                "pk_key": "rkey",
                "pk_value": format!("tail_{i}"),
                "props": {"idx": i}
            }),
        ).unwrap();
    }

    let result = post(
        &format!("{WRITE_URL}/xrpc/ai.gftd.yata.walTail"),
        json!({"after_seq": 0, "limit": 1000}),
    ).unwrap();

    let entries = result["entries"].as_array().unwrap();
    assert!(entries.len() >= 5, "should have >= 5 entries, got {}", entries.len());
    let head_seq = result["head_seq"].as_u64().unwrap();
    assert!(head_seq >= 5, "head_seq should be >= 5, got {head_seq}");
    eprintln!("  walTail: {} entries, head_seq={head_seq}", entries.len());
}

// ── Test 3: walApply on read replica ──

#[test]
fn test_wal_apply_on_read_replica() {
    if skip_if_not_running() { return; }
    if !health_check(READ0_URL) {
        eprintln!("SKIP: yata-read-0 not running at {READ0_URL}");
        return;
    }

    let write_result = post(
        &format!("{WRITE_URL}/xrpc/ai.gftd.yata.mergeRecordWal"),
        json!({
            "label": "ApplyTest",
            "pk_key": "rkey",
            "pk_value": "apply_1",
            "props": {"text": "from write container", "collection": "test.apply", "repo": "did:web:test"}
        }),
    ).unwrap();
    let entry = &write_result["wal_entry"];

    let apply_result = post(
        &format!("{READ0_URL}/xrpc/ai.gftd.yata.walApply"),
        json!({"entries": [entry]}),
    ).unwrap();
    assert!(apply_result["applied"].as_u64().unwrap() >= 1, "should apply >= 1 entry");

    let query_result = post(
        &format!("{READ0_URL}/xrpc/ai.gftd.yata.cypher"),
        json!({"statement": "MATCH (r:ApplyTest) WHERE r.rkey = 'apply_1' RETURN r.text AS text LIMIT 1"}),
    ).unwrap();
    let rows = query_result["rows"].as_array().unwrap();
    assert!(!rows.is_empty(), "read replica should have the record");
    eprintln!("  walApply: applied, query returned {} rows", rows.len());
}

// ── Test 4: Write→Read propagation latency ──

#[test]
fn test_write_read_propagation_latency() {
    if skip_if_not_running() { return; }
    if !health_check(READ0_URL) {
        eprintln!("SKIP: yata-read-0 not running");
        return;
    }

    let pk = format!("latency_{}", std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis());

    let t0 = Instant::now();
    let write_result = post(
        &format!("{WRITE_URL}/xrpc/ai.gftd.yata.mergeRecordWal"),
        json!({
            "label": "LatencyTest",
            "pk_key": "rkey",
            "pk_value": &pk,
            "props": {"ts": "now"}
        }),
    ).unwrap();
    let write_ms = t0.elapsed().as_millis();

    let entry = &write_result["wal_entry"];
    let t1 = Instant::now();
    post(
        &format!("{READ0_URL}/xrpc/ai.gftd.yata.walApply"),
        json!({"entries": [entry]}),
    ).unwrap();
    let apply_ms = t1.elapsed().as_millis();

    let t2 = Instant::now();
    let query_result = post(
        &format!("{READ0_URL}/xrpc/ai.gftd.yata.cypher"),
        json!({"statement": format!("MATCH (r:LatencyTest) WHERE r.rkey = '{pk}' RETURN r.rkey AS rkey LIMIT 1")}),
    ).unwrap();
    let read_ms = t2.elapsed().as_millis();
    let total_ms = t0.elapsed().as_millis();

    let rows = query_result["rows"].as_array().unwrap();
    assert!(!rows.is_empty(), "read replica should see the record");

    eprintln!("  Write→Read: write={write_ms}ms, apply={apply_ms}ms, read={read_ms}ms, total={total_ms}ms");
}

// ── Test 5: walFlushSegment → R2 ──

#[test]
fn test_wal_flush_segment() {
    if skip_if_not_running() { return; }

    for i in 0..10 {
        post(
            &format!("{WRITE_URL}/xrpc/ai.gftd.yata.mergeRecordWal"),
            json!({
                "label": "FlushTest",
                "pk_key": "rkey",
                "pk_value": format!("flush_{i}"),
                "props": {"idx": i}
            }),
        ).unwrap();
    }

    let result = post(
        &format!("{WRITE_URL}/xrpc/ai.gftd.yata.walFlushSegment"),
        json!({}),
    ).unwrap();

    eprintln!("  walFlushSegment: seq {}→{}, {} bytes",
        result["seq_start"], result["seq_end"], result["bytes"]);
}

// ── Test 6: compact (L1 compaction) ──

#[test]
fn test_compact() {
    if skip_if_not_running() { return; }

    for i in 0..5 {
        post(
            &format!("{WRITE_URL}/xrpc/ai.gftd.yata.mergeRecordWal"),
            json!({
                "label": "CompactTest",
                "pk_key": "rkey",
                "pk_value": format!("cp_{i}"),
                "props": {"idx": i}
            }),
        ).unwrap();
    }

    let result = post(
        &format!("{WRITE_URL}/xrpc/ai.gftd.yata.compact"),
        json!({}),
    ).unwrap();

    let output = result["output_entries"].as_u64().unwrap_or(0);
    eprintln!("  compact: {output} entries");
    assert!(output > 0, "compaction should produce entries");
}

// ── Test 7: walColdStart on read replica ──

#[test]
fn test_wal_cold_start() {
    if skip_if_not_running() { return; }
    if !health_check(READ1_URL) {
        eprintln!("SKIP: yata-read-1 not running at {READ1_URL}");
        return;
    }

    for i in 0..3 {
        post(
            &format!("{WRITE_URL}/xrpc/ai.gftd.yata.mergeRecordWal"),
            json!({
                "label": "ColdStartTest",
                "pk_key": "rkey",
                "pk_value": format!("cold_{i}"),
                "props": {"idx": i, "collection": "test.cold", "repo": "did:web:test"}
            }),
        ).unwrap();
    }
    post(&format!("{WRITE_URL}/xrpc/ai.gftd.yata.walFlushSegment"), json!({})).unwrap();
    post(&format!("{WRITE_URL}/xrpc/ai.gftd.yata.walCheckpoint"), json!({})).unwrap();

    let t0 = Instant::now();
    let result = post(
        &format!("{READ1_URL}/xrpc/ai.gftd.yata.walColdStart"),
        json!({}),
    ).unwrap();
    let cold_start_ms = t0.elapsed().as_millis();

    let checkpoint_seq = result["checkpoint_seq"].as_u64().unwrap_or(0);
    eprintln!("  walColdStart: checkpoint_seq={checkpoint_seq}, {cold_start_ms}ms");

    let query_result = post(
        &format!("{READ1_URL}/xrpc/ai.gftd.yata.cypher"),
        json!({"statement": "MATCH (r:ColdStartTest) RETURN count(r) AS cnt"}),
    ).unwrap();
    eprintln!("  ColdStartTest count: {:?}", query_result["rows"]);
}

// ── Test 8: Load test ──

#[test]
fn test_wal_projection_load() {
    if skip_if_not_running() { return; }
    if !health_check(READ0_URL) {
        eprintln!("SKIP: yata-read-0 not running");
        return;
    }

    let num_writes = 100;
    let mut write_times = Vec::with_capacity(num_writes);
    let mut apply_times = Vec::with_capacity(num_writes);

    let t_start = Instant::now();
    for i in 0..num_writes {
        let t0 = Instant::now();
        let result = post(
            &format!("{WRITE_URL}/xrpc/ai.gftd.yata.mergeRecordWal"),
            json!({
                "label": "LoadTest",
                "pk_key": "rkey",
                "pk_value": format!("load_{i}"),
                "props": {"idx": i, "batch": "load"}
            }),
        ).unwrap();
        write_times.push(t0.elapsed().as_millis());

        if let Some(entry) = result.get("wal_entry") {
            let t1 = Instant::now();
            post(
                &format!("{READ0_URL}/xrpc/ai.gftd.yata.walApply"),
                json!({"entries": [entry]}),
            ).unwrap();
            apply_times.push(t1.elapsed().as_millis());
        }
    }
    let total_write_ms = t_start.elapsed().as_millis();

    let t_read = Instant::now();
    let mut read_times = Vec::with_capacity(20);
    for _ in 0..20 {
        let t0 = Instant::now();
        post(
            &format!("{READ0_URL}/xrpc/ai.gftd.yata.cypher"),
            json!({"statement": "MATCH (r:LoadTest) WHERE r.batch = 'load' RETURN count(r) AS cnt"}),
        ).unwrap();
        read_times.push(t0.elapsed().as_millis());
    }
    let total_read_ms = t_read.elapsed().as_millis();

    write_times.sort();
    apply_times.sort();
    read_times.sort();
    let p50 = |v: &[u128]| if v.is_empty() { 0 } else { v[v.len() / 2] };
    let p95 = |v: &[u128]| if v.is_empty() { 0 } else { v[std::cmp::min((v.len() as f64 * 0.95) as usize, v.len() - 1)] };

    eprintln!("\n  === WAL Projection Load Test ({num_writes} writes) ===");
    eprintln!("  Write: total={total_write_ms}ms, p50={}ms, p95={}ms, throughput={:.0} ops/s",
        p50(&write_times), p95(&write_times),
        num_writes as f64 / (total_write_ms as f64 / 1000.0));
    eprintln!("  Apply: p50={}ms, p95={}ms",
        p50(&apply_times), p95(&apply_times));
    eprintln!("  Read:  total={total_read_ms}ms, p50={}ms, p95={}ms",
        p50(&read_times), p95(&read_times));
    eprintln!("  Write→Read lag: ~{}ms (write p50 + apply p50)",
        p50(&write_times) + p50(&apply_times));
}
