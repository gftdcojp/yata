//! E2E tests for Multi-DID data flows against yata-server + MinIO (docker-compose).
//!
//! Verifies that STARTS WITH Cypher queries correctly aggregate posts from
//! path-based DIDs (e.g. did:web:news.gftd.ai:writer:gigazine).
//!
//! Regression: yoro profile showed 0 posts because getAuthorFeed used exact
//! r.repo match, excluding path-based writer DIDs.
//!
//! Prerequisite:
//!   docker compose -f docker-compose.test.yml up -d
//!
//! Run:
//!   cargo test -p yata-server --test multi_did_e2e -- --nocapture --test-threads=1

use std::time::Duration;

const P0_URL: &str = "http://localhost:8083";

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

fn cypher(query: &str) -> serde_json::Value {
    let resp = client()
        .post(&format!("{P0_URL}/xrpc/ai.gftd.yata.cypher"))
        .header("X-Magatama-Verified", "true")
        .json(&serde_json::json!({ "statement": query, "appId": "" }))
        .send()
        .unwrap();
    let status = resp.status();
    let body = resp.text().unwrap_or_default();
    assert!(status.is_success(), "cypher failed: {status} — {body}");
    serde_json::from_str(&body).unwrap_or(serde_json::Value::Null)
}

fn cypher_mutate(query: &str) {
    let resp = client()
        .post(&format!("{P0_URL}/xrpc/ai.gftd.yata.cypher"))
        .header("X-Magatama-Verified", "true")
        .json(&serde_json::json!({ "statement": query, "appId": "" }))
        .send()
        .unwrap();
    let status = resp.status();
    let body = resp.text().unwrap_or_default();
    eprintln!("  mutate [{status}]: {}", &body[..body.len().min(200)]);
    assert!(status.is_success(), "mutate failed: {status} — {body}");
}

fn seed_post(repo: &str, rkey: &str, text: &str) {
    let val_b64 = base64_encode(&format!(
        r#"{{"$type":"app.bsky.feed.post","text":"{}","createdAt":"2026-03-25T00:00:00Z"}}"#,
        text
    ));
    cypher_mutate(&format!(
        r#"MERGE (n:Post {{rkey: '{rkey}'}}) SET n.repo = '{repo}', n.collection = 'app.bsky.feed.post', n.value_b64 = '{val_b64}', n.sensitivity_ord = 0, n.owner_hash = 0, n.updated_at = '2026-03-25T00:00:00Z'"#
    ));
}

fn base64_encode(s: &str) -> String {
    // Simple base64 without external crate dependency
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = s.as_bytes();
    let mut out = String::with_capacity((bytes.len() + 2) / 3 * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((triple >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// Extract first column of first row as i64 (for count queries).
/// yata returns {"columns":["cnt"],"rows":[[6]]} — rows is array-of-arrays.
fn cnt(val: &serde_json::Value) -> i64 {
    val["rows"].as_array()
        .and_then(|r| r.first())
        .and_then(|row| row.as_array())
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
}

/// Extract rows keyed by column names.
fn named_rows(val: &serde_json::Value) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let cols: Vec<String> = val["columns"].as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    val["rows"].as_array().unwrap_or(&vec![]).iter()
        .filter_map(|row| {
            let arr = row.as_array()?;
            let mut map = serde_json::Map::new();
            for (i, col) in cols.iter().enumerate() {
                if let Some(v) = arr.get(i) { map.insert(col.clone(), v.clone()); }
            }
            Some(map)
        })
        .collect()
}

fn cleanup() {
    // Delete test nodes to avoid interference between runs
    let _ = cypher_mutate(
        "MATCH (n:Post) WHERE n.rkey STARTS WITH 'mdid_' DELETE n",
    );
}

// ══════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════

#[test]
fn t01_health() {
    let resp = client().get(&format!("{P0_URL}/health")).send();
    match resp {
        Ok(r) => assert!(r.status().is_success(), "P0 unhealthy"),
        Err(e) => panic!(
            "Cannot connect to yata at {P0_URL}. Is docker-compose running?\n\
             docker compose -f docker-compose.test.yml up -d\n\
             Error: {e}"
        ),
    }
}

#[test]
fn t02_seed_multi_did_posts() {
    cleanup();

    let app_did = "did:web:news.gftd.ai";
    let writer1 = "did:web:news.gftd.ai:writer:gigazine";
    let writer2 = "did:web:news.gftd.ai:writer:techcrunch";
    let unrelated = "did:web:handotai.gftd.ai";

    // Seed posts from different DIDs
    seed_post(app_did, "mdid_001", "Primary DID post");
    seed_post(app_did, "mdid_002", "Primary DID post 2");
    seed_post(writer1, "mdid_003", "GIGAZINE article");
    seed_post(writer1, "mdid_004", "GIGAZINE article 2");
    seed_post(writer2, "mdid_005", "TechCrunch article");
    seed_post(unrelated, "mdid_006", "Handotai post (unrelated)");

    // Verify all 6 posts exist
    let result = cypher(
        "MATCH (r:Post) WHERE r.rkey STARTS WITH 'mdid_' RETURN count(r) AS cnt",
    );
    let c = cnt(&result);
    assert_eq!(c, 6, "Expected 6 seeded posts, got {c}");
}

#[test]
fn t03_starts_with_aggregates_path_based_dids() {
    let app_did = "did:web:news.gftd.ai";

    // Multi-DID query: exact OR STARTS WITH (the fix)
    let result = cypher(&format!(
        r#"MATCH (r:Post) WHERE r.rkey STARTS WITH 'mdid_' AND (r.repo = '{app_did}' OR r.repo STARTS WITH '{app_did}:') RETURN count(r) AS cnt"#
    ));
    let c = cnt(&result);
    assert_eq!(c, 5, "Multi-DID should return 5 posts (2 primary + 2 gigazine + 1 techcrunch), got {c}");
}

#[test]
fn t04_exact_match_returns_only_primary_did() {
    let app_did = "did:web:news.gftd.ai";

    // Old buggy query: exact match only
    let result = cypher(&format!(
        r#"MATCH (r:Post) WHERE r.rkey STARTS WITH 'mdid_' AND r.repo = '{app_did}' RETURN count(r) AS cnt"#
    ));
    let c = cnt(&result);
    assert_eq!(c, 2, "Exact match should return only 2 primary DID posts, got {c}");
}

#[test]
fn t05_starts_with_excludes_unrelated_dids() {
    let app_did = "did:web:news.gftd.ai";
    let unrelated = "did:web:handotai.gftd.ai";

    // Multi-DID query should NOT include handotai
    let result = cypher(&format!(
        r#"MATCH (r:Post) WHERE r.rkey STARTS WITH 'mdid_' AND (r.repo = '{app_did}' OR r.repo STARTS WITH '{app_did}:') AND r.repo <> '{unrelated}' RETURN r.repo AS repo ORDER BY r.repo"#
    ));
    for row in named_rows(&result) {
        let repo = row.get("repo").and_then(|v| v.as_str()).unwrap_or("");
        assert_ne!(repo, unrelated, "Unrelated DID should not appear in Multi-DID query");
        assert!(
            repo == app_did || repo.starts_with(&format!("{app_did}:")),
            "Unexpected repo: {repo}"
        );
    }
}

#[test]
fn t06_path_based_did_direct_query() {
    let writer1 = "did:web:news.gftd.ai:writer:gigazine";

    // Query a specific path-based DID directly
    let result = cypher(&format!(
        r#"MATCH (r:Post) WHERE r.rkey STARTS WITH 'mdid_' AND (r.repo = '{writer1}' OR r.repo STARTS WITH '{writer1}:') RETURN count(r) AS cnt"#
    ));
    let c = cnt(&result);
    assert_eq!(c, 2, "Direct path-based DID query should return 2 GIGAZINE posts, got {c}");
}

#[test]
fn t07_colon_separator_prevents_prefix_collision() {
    // Seed a post with a DID that has a similar prefix but different path
    seed_post("did:web:news.gftd.ai-extra", "mdid_collision", "Collision test");

    let app_did = "did:web:news.gftd.ai";
    let result = cypher(&format!(
        r#"MATCH (r:Post) WHERE r.rkey STARTS WITH 'mdid_' AND (r.repo = '{app_did}' OR r.repo STARTS WITH '{app_did}:') RETURN count(r) AS cnt"#
    ));
    let c = cnt(&result);
    // "did:web:news.gftd.ai-extra" does NOT start with "did:web:news.gftd.ai:"
    // so it should NOT be included
    assert_eq!(c, 5, "Prefix collision DID should NOT be included, got {c}");

    // Cleanup collision node
    let _ = cypher_mutate("MATCH (n:Post {rkey: 'mdid_collision'}) DELETE n");
}
