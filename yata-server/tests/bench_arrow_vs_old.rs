//! Performance comparison: ArrowFragment (new) vs SnapshotBundle (old).
//!
//! Run:
//!   cargo test -p yata-server --test bench_arrow_vs_old --release -- --nocapture

use std::time::Instant;

use yata_grin::{Mutable, PropValue, Topology, Scannable};
use yata_vineyard::blob::BlobStore;
use yata_store::MutableCsrStore;

fn seed_store(n_vertices: u32, n_edges: u32) -> MutableCsrStore {
    let mut store = MutableCsrStore::new();
    for i in 0..n_vertices {
        store.add_vertex("Person", &[
            ("rkey", PropValue::Str(format!("p{i}"))),
            ("name", PropValue::Str(format!("person_{i}"))),
            ("age", PropValue::Int(20 + (i % 60) as i64)),
            ("collection", PropValue::Str("app.bsky.actor.profile".into())),
            ("repo", PropValue::Str("did:web:test".into())),
            ("sensitivity_ord", PropValue::Int(0)),
            ("owner_hash", PropValue::Int(0)),
            ("updated_at", PropValue::Str("2026-03-24".into())),
        ]);
    }
    for i in 0..n_edges {
        let src = i % n_vertices;
        let dst = (i * 7 + 13) % n_vertices;
        store.add_edge(src, dst, "FOLLOWS", &[
            ("rkey", PropValue::Str(format!("f{i}"))),
            ("collection", PropValue::Str("app.bsky.graph.follow".into())),
            ("repo", PropValue::Str("did:web:test".into())),
            ("sensitivity_ord", PropValue::Int(0)),
            ("owner_hash", PropValue::Int(0)),
            ("updated_at", PropValue::Str("2026-03-24".into())),
        ]);
    }
    store.commit();
    store
}

#[test]
fn bench_serialize() {
    for &(v, e) in &[(1_000, 2_000), (10_000, 20_000)] {
        let store = seed_store(v, e);
        eprintln!("\n=== {v} vertices, {e} edges ===");

        // Old: SnapshotBundle
        let start = Instant::now();
        let bundle = yata_engine::snapshot::serialize_snapshot(&store, 0).unwrap();
        let old_serialize_ms = start.elapsed().as_millis();
        let old_bytes: usize = bundle.vertex_groups.iter().map(|(_, b)| b.len()).sum::<usize>()
            + bundle.edge_groups.iter().map(|(_, b)| b.len()).sum::<usize>()
            + bundle.topology.len();

        // New: ArrowFragment
        let start = Instant::now();
        let frag = yata_vineyard::convert::csr_to_fragment(&store, 0);
        let blob_store = yata_vineyard::blob::MemoryBlobStore::new();
        let _meta = frag.serialize(&blob_store);
        let new_serialize_ms = start.elapsed().as_millis();
        let new_bytes = blob_store.total_bytes() as usize;

        let speedup = if new_serialize_ms > 0 {
            old_serialize_ms as f64 / new_serialize_ms as f64
        } else {
            f64::INFINITY
        };

        eprintln!(
            "  Serialize: old={old_serialize_ms}ms ({old_bytes} bytes) | new={new_serialize_ms}ms ({new_bytes} bytes) | {speedup:.1}x"
        );
    }
}

#[test]
fn bench_deserialize() {
    for &(v, e) in &[(1_000, 2_000), (10_000, 20_000)] {
        let store = seed_store(v, e);
        eprintln!("\n=== {v} vertices, {e} edges ===");

        // Old: SnapshotBundle → CSR
        let bundle = yata_engine::snapshot::serialize_snapshot(&store, 0).unwrap();
        let start = Instant::now();
        let _restored_old = yata_engine::snapshot::restore_snapshot_bundle(&bundle).unwrap();
        let old_ms = start.elapsed().as_millis();

        // New: ArrowFragment → BlobStore (no CSR rebuild — fragment IS the read format)
        let frag = yata_vineyard::convert::csr_to_fragment(&store, 0);
        let blob_store = yata_vineyard::blob::MemoryBlobStore::new();
        let meta = frag.serialize(&blob_store);
        let start = Instant::now();
        let _restored_new = yata_vineyard::ArrowFragment::deserialize(&meta, &blob_store).unwrap();
        let new_ms = start.elapsed().as_millis();

        let speedup = if new_ms > 0 {
            old_ms as f64 / new_ms as f64
        } else {
            f64::INFINITY
        };

        eprintln!(
            "  Deserialize: old={old_ms}ms | new={new_ms}ms | {speedup:.1}x"
        );
    }
}

#[test]
fn bench_neighbor_traversal() {
    let v = 10_000u32;
    let e = 20_000u32;
    let store = seed_store(v, e);
    let iterations = 100_000;

    eprintln!("\n=== Neighbor traversal: {v} vertices, {e} edges, {iterations} iterations ===");

    // Old: MutableCsrStore Topology trait
    let start = Instant::now();
    let mut old_total = 0u64;
    for i in 0..iterations {
        let vid = (i % v) as u32;
        let nbrs = store.out_neighbors_by_label(vid, "FOLLOWS");
        old_total += nbrs.len() as u64;
    }
    let old_ns = start.elapsed().as_nanos();
    let old_per_iter_ns = old_ns / iterations as u128;

    // New: ArrowFragment NbrUnit zero-copy
    let frag = yata_vineyard::convert::csr_to_fragment(&store, 0);
    let start = Instant::now();
    let mut new_total = 0u64;
    for i in 0..iterations {
        let vid = (i % v) as u64;
        if let Some(nbrs) = frag.out_neighbors(0, 0, vid) {
            new_total += nbrs.len() as u64;
        }
    }
    let new_ns = start.elapsed().as_nanos();
    let new_per_iter_ns = new_ns / iterations as u128;

    let speedup = old_per_iter_ns as f64 / new_per_iter_ns as f64;

    eprintln!("  Old CSR Topology: {old_per_iter_ns}ns/iter (total neighbors: {old_total})");
    eprintln!("  New ArrowFragment NbrUnit: {new_per_iter_ns}ns/iter (total neighbors: {new_total})");
    eprintln!("  Speedup: {speedup:.1}x");
}

#[test]
fn bench_scan_vertices() {
    let v = 10_000u32;
    let e = 20_000u32;
    let store = seed_store(v, e);
    let iterations = 100;

    eprintln!("\n=== Vertex scan: {v} vertices, {iterations} iterations ===");

    // Old: Scannable trait
    let start = Instant::now();
    let mut old_total = 0usize;
    for _ in 0..iterations {
        let vids = Scannable::scan_vertices_by_label(&store, "Person");
        old_total += vids.len();
    }
    let old_ms = start.elapsed().as_millis();

    // New: ArrowFragment vertex table (direct RecordBatch access)
    let frag = yata_vineyard::convert::csr_to_fragment(&store, 0);
    let start = Instant::now();
    let mut new_total = 0usize;
    for _ in 0..iterations {
        if let Some(batch) = frag.vertex_table(0) {
            new_total += batch.num_rows();
        }
    }
    let new_ms = start.elapsed().as_millis();

    let speedup = if new_ms > 0 { old_ms as f64 / new_ms as f64 } else { f64::INFINITY };

    eprintln!("  Old Scannable: {old_ms}ms ({old_total} total)");
    eprintln!("  New RecordBatch: {new_ms}ms ({new_total} total)");
    eprintln!("  Speedup: {speedup:.1}x");
}
