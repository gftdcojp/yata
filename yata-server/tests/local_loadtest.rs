//! Local load test + partition simulation + YataFragment roundtrip.
//!
//! Usage:
//!   cargo test -p yata-server --test local_loadtest -- --nocapture

use std::sync::Arc;
use std::time::Instant;

use yata_engine::{TieredGraphEngine, config::TieredEngineConfig};
use yata_grin::PropValue;

fn make_engine(dir: &tempfile::TempDir) -> Arc<TieredGraphEngine> {
    Arc::new(TieredGraphEngine::new(
        TieredEngineConfig::default(),
        dir.path().to_str().unwrap(),
    ))
}

#[tokio::test(flavor = "multi_thread")]
async fn load_test_cypher_reads() {
    let dir = tempfile::tempdir().unwrap();
    let engine = make_engine(&dir);

    // Seed: 1000 vertices via merge_record
    let seed_count = 1000u32;
    for i in 0..seed_count {
        engine.merge_record(
            "Person",
            "rkey",
            &format!("p{i}"),
            &[
                ("name", PropValue::Str(format!("person_{i}"))),
                ("age", PropValue::Int(20 + (i % 60) as i64)),
                ("collection", PropValue::Str("app.bsky.actor.profile".into())),
                ("repo", PropValue::Str("did:web:test".into())),
                ("sensitivity_ord", PropValue::Int(0)),
                ("owner_hash", PropValue::Int(0)),
                ("updated_at", PropValue::Str("2026-03-24".into())),
            ],
        );
    }

    // Verify seed
    let count_result = engine.query("MATCH (n:Person) RETURN count(n) AS cnt", &[], None);
    assert!(count_result.is_ok(), "seed query failed: {:?}", count_result);
    let rows = count_result.unwrap();
    assert!(!rows.is_empty(), "no rows returned");
    eprintln!("Seeded {} Person vertices, query result: {:?}", seed_count, &rows[0]);

    // Load test: 10000 concurrent reads
    let concurrency = 50usize;
    let queries_per_task = 200usize;
    let start = Instant::now();

    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let eng = engine.clone();
        let sc = seed_count;
        handles.push(tokio::spawn(async move {
            for i in 0..queries_per_task {
                let rkey = format!("p{}", i % sc as usize);
                let _result = eng.query(
                    &format!("MATCH (n:Person {{rkey: '{rkey}'}}) RETURN n.name AS name LIMIT 10"),
                    &[],
                    None,
                );
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let total_queries = concurrency * queries_per_task;
    let qps = total_queries as f64 / elapsed.as_secs_f64();
    eprintln!(
        "Load test: {} queries in {:?} ({:.0} QPS, {} concurrent)",
        total_queries, elapsed, qps, concurrency
    );
    assert!(qps > 500.0, "QPS too low: {:.0}", qps);
}

#[tokio::test(flavor = "multi_thread")]
async fn partition_simulation_label_routing() {
    let dir0 = tempfile::tempdir().unwrap();
    let dir1 = tempfile::tempdir().unwrap();
    let engine0 = make_engine(&dir0);
    let engine1 = make_engine(&dir1);

    // Partition 0: Person
    for i in 0..100u32 {
        engine0.merge_record(
            "Person", "rkey", &format!("p{i}"),
            &[
                ("name", PropValue::Str(format!("person_{i}"))),
                ("collection", PropValue::Str("test".into())),
                ("repo", PropValue::Str("did:web:test".into())),
                ("sensitivity_ord", PropValue::Int(0)),
                ("owner_hash", PropValue::Int(0)),
                ("updated_at", PropValue::Str("2026-03-24".into())),
            ],
        );
    }

    // Partition 1: Post
    for i in 0..200u32 {
        engine1.merge_record(
            "Post", "rkey", &format!("post{i}"),
            &[
                ("text", PropValue::Str(format!("hello {i}"))),
                ("collection", PropValue::Str("app.bsky.feed.post".into())),
                ("repo", PropValue::Str("did:web:test".into())),
                ("sensitivity_ord", PropValue::Int(0)),
                ("owner_hash", PropValue::Int(0)),
                ("updated_at", PropValue::Str("2026-03-24".into())),
            ],
        );
    }

    // Label-based routing
    fn route(label: &str) -> usize {
        let mut h: u32 = 0;
        for b in label.bytes() { h = h.wrapping_mul(31).wrapping_add(b as u32); }
        (h % 2) as usize
    }

    let engines = [&engine0, &engine1];
    let p_result = engines[0].query("MATCH (n:Person) RETURN count(n) AS cnt", &[], None);
    let t_result = engines[1].query("MATCH (n:Post) RETURN count(n) AS cnt", &[], None);
    eprintln!("P0 Person: {:?}", p_result);
    eprintln!("P1 Post: {:?}", t_result);

    assert!(p_result.is_ok());
    assert!(t_result.is_ok());

    // Cross-partition: each engine only sees its own labels
    let cross = engines[0].query("MATCH (n:Post) RETURN count(n) AS cnt", &[], None);
    eprintln!("P0 Post (should be empty): {:?}", cross);
}

#[tokio::test]
async fn arrow_fragment_snapshot_roundtrip() {
    use yata_grin::Mutable;
    use yata_store::MutableCsrStore;
    use yata_format::convert::csr_to_fragment;
    use yata_format::BlobStore;
    use yata_format::MemoryBlobStore;

    let mut store = MutableCsrStore::new();

    store.add_vertex("Person", &[
        ("rkey", PropValue::Str("alice".into())),
        ("name", PropValue::Str("Alice".into())),
        ("age", PropValue::Int(30)),
    ]);
    store.add_vertex("Person", &[
        ("rkey", PropValue::Str("bob".into())),
        ("name", PropValue::Str("Bob".into())),
        ("age", PropValue::Int(25)),
    ]);
    store.add_vertex("Post", &[
        ("rkey", PropValue::Str("post1".into())),
        ("text", PropValue::Str("Hello".into())),
    ]);

    store.add_edge(0, 1, "FOLLOWS", &[]);
    store.add_edge(0, 2, "LIKES", &[]);
    store.commit();

    // Convert to YataFragment
    let frag = csr_to_fragment(&store, 0);
    assert_eq!(frag.vertex_label_num(), 2);
    assert_eq!(frag.edge_label_num(), 2);

    // Serialize → Deserialize roundtrip
    let blob_store = MemoryBlobStore::new();
    let meta = frag.serialize(&blob_store);
    let restored = yata_format::YataFragment::deserialize(&meta, &blob_store).unwrap();

    assert_eq!(restored.vertex_label_num(), 2);
    assert_eq!(restored.edge_label_num(), 2);

    // Verify Person vertex count
    assert_eq!(restored.inner_vertex_num(0), 2);

    eprintln!(
        "YataFragment roundtrip: {} v_labels, {} e_labels, {} edges",
        restored.vertex_label_num(),
        restored.edge_label_num(),
        restored.edge_num(),
    );
}
