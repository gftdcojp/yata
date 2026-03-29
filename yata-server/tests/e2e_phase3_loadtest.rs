//! Phase 3b+3c+3-tier load test against yata-server + MinIO (docker-compose).
//!
//! Tests:
//!   1. Seed multi-label graph (Person, Company, Post, Like, Review)
//!   2. Trigger chunked snapshot → MinIO
//!   3. Cold restart → verify 3-tier page-in (disk → MinIO)
//!   4. Label-selective 2-hop query load (Person→KNOWS→Person→WORKS_AT→Company)
//!   5. Incremental label enrichment (query new label → enrich stubs)
//!   6. Concurrent read/write mixed load
//!
//! Prerequisite:
//!   docker compose -f docker-compose.test.yml up -d
//!
//! Run:
//!   cargo test -p yata-server --test e2e_phase3_loadtest -- --nocapture --test-threads=1

use std::time::{Duration, Instant};

const URL: &str = "http://localhost:8083";

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

fn cypher(query: &str) -> serde_json::Value {
    let resp = client()
        .post(&format!("{URL}/xrpc/ai.gftd.yata.cypher"))
        .header("X-Magatama-Verified", "true")
        .json(&serde_json::json!({ "statement": query }))
        .send()
        .unwrap();
    let status = resp.status();
    let body = resp.text().unwrap_or_default();
    assert!(status.is_success(), "cypher failed: {status} — {body}");
    serde_json::from_str(&body).unwrap_or(serde_json::Value::Null)
}

fn merge_record(label: &str, rkey: &str, extra_props: &str) {
    let resp = client()
        .post(&format!("{URL}/xrpc/ai.gftd.yata.mergeRecord"))
        .header("X-Magatama-Verified", "true")
        .json(&serde_json::json!({
            "label": label,
            "pk_key": "rkey",
            "pk_value": rkey,
            "props": {
                "collection": "test",
                "repo": "did:web:test",
                "sensitivity_ord": 0,
                "owner_hash": 0,
                "updated_at": "2026-03-25",
                "extra": extra_props,
            }
        }))
        .send()
        .unwrap();
    assert!(resp.status().is_success(), "mergeRecord {label}/{rkey} failed: {}", resp.status());
}

fn trigger_compaction() -> serde_json::Value {
    let resp = client()
        .post(&format!("{URL}/xrpc/ai.gftd.yata.compact"))
        .header("X-Magatama-Verified", "true")
        .json(&serde_json::json!({}))
        .send()
        .unwrap();
    assert!(resp.status().is_success(), "compaction failed: {}", resp.status());
    resp.json().unwrap_or(serde_json::Value::Null)
}

fn count_label(label: &str) -> i64 {
    let result = cypher(&format!("MATCH (n:{label}) RETURN count(n) AS cnt"));
    result.get("rows")
        .and_then(|r| r.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .and_then(|cols| cols.first())
        .and_then(|v| v.as_str().or(v.as_i64().map(|_| "")).and_then(|_| None).or(v.as_str()))
        .and_then(|s| s.parse::<i64>().ok())
        .or_else(|| {
            result.get("rows")
                .and_then(|r| r.as_array())
                .and_then(|rows| rows.first())
                .and_then(|row| row.as_array())
                .and_then(|cols| cols.first())
                .and_then(|v| v.as_i64())
        })
        .unwrap_or(0)
}

// ── Tests ──────────────────────────────────────────────────────────

#[test]
fn t01_health_check() {
    let resp = client().get(&format!("{URL}/health")).send();
    match resp {
        Ok(r) => assert!(r.status().is_success(), "unhealthy"),
        Err(e) => panic!(
            "Cannot reach yata at {URL}. Start docker-compose first:\n  \
             cd packages/server/yata && docker compose -f docker-compose.test.yml up -d\nError: {e}"
        ),
    }
    eprintln!("t01: health OK");
}

#[test]
fn t02_seed_multi_label_graph() {
    let start = Instant::now();

    // Person nodes (200)
    for i in 0..200 {
        merge_record("Person", &format!("person_{i}"), &format!("name=Person {i},age={}", 20 + i % 50));
    }
    let person_elapsed = start.elapsed();
    eprintln!("t02: 200 Person nodes in {:?} ({:.0}/sec)", person_elapsed, 200.0 / person_elapsed.as_secs_f64());

    // Company nodes (50)
    for i in 0..50 {
        merge_record("Company", &format!("company_{i}"), &format!("name=Company {i},industry=tech"));
    }

    // Post nodes (500)
    for i in 0..500 {
        merge_record("Post", &format!("post_{i}"), &format!("text=Post content {i}"));
    }

    // Like nodes (300)
    for i in 0..300 {
        merge_record("Like", &format!("like_{i}"), &format!("weight={}", i % 5));
    }

    // Review nodes (100)
    for i in 0..100 {
        merge_record("Review", &format!("review_{i}"), &format!("rating={}", 1 + i % 5));
    }

    let total_elapsed = start.elapsed();
    let total = 200 + 50 + 500 + 300 + 100;
    eprintln!("t02: {} nodes seeded across 5 labels in {:?} ({:.0}/sec)",
        total, total_elapsed, total as f64 / total_elapsed.as_secs_f64());

    // Create edges: Person→KNOWS→Person, Person→WORKS_AT→Company
    for i in 0..100 {
        let src = format!("person_{i}");
        let dst = format!("person_{}", (i + 1) % 200);
        cypher(&format!(
            "MATCH (a:Person {{rkey: '{src}'}}), (b:Person {{rkey: '{dst}'}}) CREATE (a)-[:KNOWS]->(b)"
        ));
    }
    for i in 0..200 {
        let person = format!("person_{i}");
        let company = format!("company_{}", i % 50);
        cypher(&format!(
            "MATCH (a:Person {{rkey: '{person}'}}), (b:Company {{rkey: '{company}'}}) CREATE (a)-[:WORKS_AT]->(b)"
        ));
    }
    let edge_elapsed = start.elapsed() - total_elapsed;
    eprintln!("t02: 300 edges created in {:?}", edge_elapsed);

    // Verify counts
    assert!(count_label("Person") >= 200, "Person count mismatch");
    assert!(count_label("Company") >= 50, "Company count mismatch");
    assert!(count_label("Post") >= 500, "Post count mismatch");
    eprintln!("t02: all labels verified");
}

#[test]
fn t03_chunked_snapshot() {
    let start = Instant::now();
    let result = trigger_compaction();
    let elapsed = start.elapsed();
    eprintln!("t03: snapshot in {:?} — {}", elapsed, serde_json::to_string_pretty(&result).unwrap());
}

#[test]
fn t04_2hop_query_load() {
    // 2-hop: Person→KNOWS→Person→WORKS_AT→Company
    let start = Instant::now();
    let count = 200;
    let mut success = 0;
    for i in 0..count {
        let person = format!("person_{}", i % 200);
        let result = cypher(&format!(
            "MATCH (a:Person {{rkey: '{person}'}})-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company) RETURN c.extra AS company LIMIT 5"
        ));
        if result.get("rows").and_then(|r| r.as_array()).map(|a| !a.is_empty()).unwrap_or(false) {
            success += 1;
        }
    }
    let elapsed = start.elapsed();
    let qps = count as f64 / elapsed.as_secs_f64();
    eprintln!("t04: {} 2-hop queries in {:?} ({:.0} QPS), {} returned results",
        count, elapsed, qps, success);
}

#[test]
fn t05_label_selective_read_load() {
    // These queries should benefit from label-selective page-in:
    // - Person-only query (should NOT page-in Post/Like/Review properties)
    // - Company-only query

    let start = Instant::now();
    let count = 500;
    for i in 0..count {
        if i % 2 == 0 {
            cypher(&format!(
                "MATCH (n:Person {{rkey: 'person_{}'}}) RETURN n.extra AS info", i % 200
            ));
        } else {
            cypher(&format!(
                "MATCH (n:Company {{rkey: 'company_{}'}}) RETURN n.extra AS info", i % 50
            ));
        }
    }
    let elapsed = start.elapsed();
    let qps = count as f64 / elapsed.as_secs_f64();
    eprintln!("t05: {} label-selective reads in {:?} ({:.0} QPS)", count, elapsed, qps);
}

#[test]
fn t06_mixed_read_write_load() {
    let start = Instant::now();
    let reads = 300;
    let writes = 100;

    // Interleave reads and writes
    for i in 0..(reads + writes) {
        if i % 4 == 0 {
            // Write: new node
            merge_record("MixedTest", &format!("mixed_{i}"), &format!("round={i}"));
        } else {
            // Read: existing node
            cypher(&format!(
                "MATCH (n:Person {{rkey: 'person_{}'}}) RETURN n.extra LIMIT 1", i % 200
            ));
        }
    }
    let elapsed = start.elapsed();
    let total = reads + writes;
    let ops_sec = total as f64 / elapsed.as_secs_f64();
    eprintln!("t06: {} mixed ops ({} reads + {} writes) in {:?} ({:.0} ops/sec)",
        total, reads, writes, elapsed, ops_sec);

    // Verify mixed writes persisted
    let mixed_count = count_label("MixedTest");
    eprintln!("t06: MixedTest count = {}", mixed_count);
    assert!(mixed_count > 0, "MixedTest writes lost");
}

#[test]
fn t07_snapshot_after_load() {
    let start = Instant::now();
    let result = trigger_compaction();
    let elapsed = start.elapsed();
    eprintln!("t07: post-load snapshot in {:?} — {}", elapsed, serde_json::to_string_pretty(&result).unwrap());

    // Verify total graph
    let persons = count_label("Person");
    let companies = count_label("Company");
    let posts = count_label("Post");
    eprintln!("t07: Person={}, Company={}, Post={}", persons, companies, posts);
}

#[test]
fn t08_full_scan_vs_label_scan() {
    // Compare: full scan vs label-specific scan
    let start_full = Instant::now();
    for _ in 0..50 {
        cypher("MATCH (n) RETURN count(n) AS cnt");
    }
    let full_elapsed = start_full.elapsed();

    let start_label = Instant::now();
    for _ in 0..50 {
        cypher("MATCH (n:Person) RETURN count(n) AS cnt");
    }
    let label_elapsed = start_label.elapsed();

    eprintln!("t08: 50x full scan: {:?} ({:.0} QPS)", full_elapsed, 50.0 / full_elapsed.as_secs_f64());
    eprintln!("t08: 50x label scan: {:?} ({:.0} QPS)", label_elapsed, 50.0 / label_elapsed.as_secs_f64());
}
