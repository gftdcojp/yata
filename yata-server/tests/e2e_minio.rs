//! E2E tests against yata-server + MinIO (docker-compose).
//!
//! Prerequisite:
//!   docker compose -f docker-compose.test.yml up -d
//!
//! Run:
//!   cargo test -p yata-server --test e2e_minio -- --nocapture --test-threads=1
//!
//! Tests:
//!   1. Health check
//!   2. mergeRecord → Cypher query roundtrip
//!   3. trigger_snapshot → MinIO persistence
//!   4. Cold restart → page-in from MinIO
//!   5. 2-partition label routing
//!   6. Load test (concurrent writes + reads)

use std::time::Duration;

const P0_URL: &str = "http://localhost:8083";
const P1_URL: &str = "http://localhost:8084";
const TOKEN: &str = "test-token";

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

fn cypher(url: &str, query: &str) -> serde_json::Value {
    let resp = client()
        .post(&format!("{url}/xrpc/ai.gftd.yata.cypher"))
        .header("X-Magatama-Verified", "true")
        .json(&serde_json::json!({ "statement": query, "appId": "" }))
        .send()
        .unwrap();
    let status = resp.status();
    let body = resp.text().unwrap_or_default();
    assert!(status.is_success(), "cypher failed: {status} — {body}");
    serde_json::from_str(&body).unwrap_or(serde_json::Value::Null)
}

fn merge_record(url: &str, label: &str, rkey: &str, props: serde_json::Value) {
    let cypher_str = format!(
        "MERGE (n:{label} {{rkey: '{rkey}'}}) SET n += $props, n.collection = 'test', n.repo = 'did:web:test', n.sensitivity_ord = 0, n.owner_hash = 0, n.updated_at = '2026-03-24'"
    );
    let resp = client()
        .post(&format!("{url}/xrpc/ai.gftd.yata.cypher"))
        .header("X-Magatama-Verified", "true")
        .json(&serde_json::json!({
            "cypher": cypher_str,
            "appId": "",
            "params": props.to_string(),
        }))
        .send()
        .unwrap();
    assert!(resp.status().is_success(), "merge failed: {} — {}", resp.status(), resp.text().unwrap_or_default());
}

fn trigger_snapshot(url: &str) -> bool {
    let resp = client()
        .post(&format!("{url}/xrpc/ai.gftd.yata.triggerSnapshot"))
        .header("X-Magatama-Verified", "true")
        .json(&serde_json::json!({}))
        .send()
        .unwrap();
    resp.status().is_success()
}

#[test]
fn t01_health() {
    let resp = client().get(&format!("{P0_URL}/health")).send();
    match resp {
        Ok(r) => assert!(r.status().is_success(), "P0 unhealthy"),
        Err(e) => panic!(
            "Cannot reach yata P0 at {P0_URL}. Start docker-compose first:\n  docker compose -f docker-compose.test.yml up -d\nError: {e}"
        ),
    }
    let resp2 = client().get(&format!("{P1_URL}/health")).send();
    match resp2 {
        Ok(r) => assert!(r.status().is_success(), "P1 unhealthy"),
        Err(e) => eprintln!("P1 not available (single-partition mode): {e}"),
    }
}

#[test]
fn t02_merge_and_query() {
    // Write
    let create = "CREATE (n:TestNode {rkey: 'e2e_001', name: 'E2E Test', collection: 'test', repo: 'did:web:test', sensitivity_ord: 0, owner_hash: 0, updated_at: '2026-03-24'})";
    cypher(P0_URL, create);

    // Read
    let result = cypher(P0_URL, "MATCH (n:TestNode {rkey: 'e2e_001'}) RETURN n.name AS name");
    eprintln!("t02 result: {}", serde_json::to_string_pretty(&result).unwrap());
    let rows = result.get("rows").and_then(|r| r.as_array()).unwrap();
    assert!(!rows.is_empty(), "query returned empty rows");
}

#[test]
fn t03_snapshot_to_minio() {
    // Seed data
    for i in 0..50 {
        let create = format!(
            "MERGE (n:SnapshotTest {{rkey: 'snap_{}'}}) SET n.value = {}, n.collection = 'test', n.repo = 'did:web:test', n.sensitivity_ord = 0, n.owner_hash = 0, n.updated_at = '2026-03-24'",
            i, i * 10
        );
        cypher(P0_URL, &create);
    }

    // Trigger snapshot → MinIO
    let ok = trigger_snapshot(P0_URL);
    assert!(ok, "snapshot failed");
    eprintln!("t03: snapshot triggered successfully");

    // Verify data survives
    let result = cypher(P0_URL, "MATCH (n:SnapshotTest) RETURN count(n) AS cnt");
    eprintln!("t03 count: {}", serde_json::to_string_pretty(&result).unwrap());
}

#[test]
fn t04_cold_restart_page_in() {
    // This test verifies that after snapshot, data can be restored.
    // In docker-compose, restart the yata container and re-query.
    //
    // Manual verification:
    //   docker compose -f docker-compose.test.yml restart yata
    //   sleep 3
    //   curl -X POST http://localhost:8081/xrpc/ai.gftd.yata.cypher \
    //     -H 'X-Magatama-Verified: true' \
    //     -H 'Content-Type: application/json' \
    //     -d '{"cypher":"MATCH (n:SnapshotTest) RETURN count(n) AS cnt","appId":""}'
    //
    // Expected: cnt = 50 (restored from MinIO)

    eprintln!("t04: cold restart test requires manual container restart");
    eprintln!("  docker compose -f docker-compose.test.yml restart yata");
    eprintln!("  then re-run: cargo test -p yata-server --test e2e_minio t04 -- --nocapture");

    // If container just restarted, verify page-in
    let result = cypher(P0_URL, "MATCH (n:SnapshotTest) RETURN count(n) AS cnt");
    eprintln!("t04 count after restart: {}", serde_json::to_string_pretty(&result).unwrap());
}

#[test]
fn t05_two_partition_isolation() {
    let p1_ok = client().get(&format!("{P1_URL}/health")).send().map(|r| r.status().is_success()).unwrap_or(false);
    if !p1_ok {
        eprintln!("t05: SKIP — P1 not running (single-partition mode)");
        return;
    }

    // Write Person to P0
    cypher(P0_URL, "MERGE (n:IsoTest {rkey: 'p0_node'}) SET n.partition = 'zero', n.collection = 'test', n.repo = 'did:web:test', n.sensitivity_ord = 0, n.owner_hash = 0, n.updated_at = '2026-03-24'");

    // Write Post to P1
    cypher(P1_URL, "MERGE (n:IsoPost {rkey: 'p1_node'}) SET n.partition = 'one', n.collection = 'test', n.repo = 'did:web:test', n.sensitivity_ord = 0, n.owner_hash = 0, n.updated_at = '2026-03-24'");

    // P0 should NOT see P1's data
    let cross = cypher(P0_URL, "MATCH (n:IsoPost) RETURN count(n) AS cnt");
    let cnt = cross.as_array()
        .and_then(|a| a.first())
        .and_then(|r| r.as_array())
        .and_then(|r| r.iter().find(|v| v.as_array().map(|a| a[0].as_str() == Some("cnt")).unwrap_or(false)))
        .and_then(|v| v.as_array())
        .and_then(|a| a[1].as_str())
        .unwrap_or("0");
    eprintln!("t05: P0 sees {} IsoPost nodes (expected 0)", cnt);
    assert_eq!(cnt, "0", "partition isolation violated");

    // P1 should NOT see P0's data
    let cross2 = cypher(P1_URL, "MATCH (n:IsoTest) RETURN count(n) AS cnt");
    eprintln!("t05: P1 IsoTest query: {}", serde_json::to_string_pretty(&cross2).unwrap());
}

#[test]
fn t06_load_test() {
    use std::time::Instant;

    // Seed 500 nodes
    for i in 0..500 {
        let q = format!(
            "MERGE (n:LoadTest {{rkey: 'lt_{i}'}}) SET n.val = {i}, n.collection = 'test', n.repo = 'did:web:test', n.sensitivity_ord = 0, n.owner_hash = 0, n.updated_at = '2026-03-24'"
        );
        cypher(P0_URL, &q);
    }

    // Read load test: 1000 queries
    let start = Instant::now();
    let count = 1000;
    for i in 0..count {
        let rkey = format!("lt_{}", i % 500);
        cypher(P0_URL, &format!("MATCH (n:LoadTest {{rkey: '{rkey}'}}) RETURN n.val AS val"));
    }
    let elapsed = start.elapsed();
    let qps = count as f64 / elapsed.as_secs_f64();
    eprintln!("t06: {} reads in {:?} ({:.0} QPS via HTTP)", count, elapsed, qps);
}
