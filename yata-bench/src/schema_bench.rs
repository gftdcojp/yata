//! Lance table schema benchmark: compares 3 vertex storage formats.
//!
//! Format A (current):  7 cols — all props in `props_json` JSON blob
//! Format B (hybrid):  11 cols — top-5 props promoted to columns + overflow JSON
//! Format C (wide):    17 cols — all known props as columns, `extra_json` fallback
//!
//! Usage:
//!   cargo run -p yata-bench --bin schema-bench --release
//!
//! Environment variables:
//!   VERTICES  (default: 100000)
//!   BATCH_SZ  (default: 1000)

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use rand::{Rng, SeedableRng, rngs::StdRng};
use yata_lance::{ArrowStore, YataDb};

// ── report helpers (from cypher_bench.rs) ───────────────────────────────

struct Samples(Vec<u64>);

impl Samples {
    fn new() -> Self {
        Self(Vec::new())
    }
    fn push_us(&mut self, us: u64) {
        self.0.push(us);
    }
    fn push_ms(&mut self, ms: u64) {
        self.0.push(ms);
    }
    fn report(&mut self, label: &str, engine: &str) {
        if self.0.is_empty() {
            println!("  [{engine}] {label}: no data");
            return;
        }
        self.0.sort_unstable();
        let n = self.0.len();
        let sum: u64 = self.0.iter().sum();
        let mean = sum / n as u64;
        let p50 = self.0[n * 50 / 100];
        let p95 = self.0[n.saturating_mul(95) / 100];
        let p99 = self.0[n.saturating_mul(99) / 100];
        let max = *self.0.last().unwrap();
        println!(
            "  [{engine:<12}] {label:<44} n={n:>4}  mean={mean:>8}  p50={p50:>8}  p95={p95:>8}  p99={p99:>8}  max={max:>8}",
        );
    }
    fn mean(&self) -> u64 {
        if self.0.is_empty() {
            return 0;
        }
        self.0.iter().sum::<u64>() / self.0.len() as u64
    }
}

// ── dataset generation ──────────────────────────────────────────────────

struct VertexData {
    label: String,
    pk_value: String,
    name: String,
    description: String,
    age: i64,
    score: f64,
    repo: String,
    rkey: String,
    owner_did: String,
    app_id: String,
    org_id: String,
    user_did: String,
}

fn generate_vertices(n: usize, seed: u64) -> Vec<VertexData> {
    let mut rng = StdRng::seed_from_u64(seed);
    let labels = ["Person", "Company", "Product", "Post", "Follow"];
    (0..n)
        .map(|i| {
            let label = labels[i % labels.len()];
            VertexData {
                label: label.to_owned(),
                pk_value: format!("rkey_{i}"),
                name: format!("{label}_{i}"),
                description: format!("Description for {label} number {i} with some padding text to simulate real data"),
                age: rng.gen_range(18..80),
                score: rng.gen_range(0.0..100.0),
                repo: format!("did:web:n{}.gftd.ai", i % 500),
                rkey: format!("rkey_{i}"),
                owner_did: format!("did:web:n{}.gftd.ai", i % 200),
                app_id: format!("app_{}", i % 50),
                org_id: format!("org_{}", i % 20),
                user_did: format!("did:web:u{}.gftd.ai", i % 300),
            }
        })
        .collect()
}

// ── Schema definitions ──────────────────────────────────────────────────

/// Format A: Current 7-column schema (all props in JSON blob).
fn schema_a() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("op", DataType::UInt8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("pk_key", DataType::Utf8, false),
        Field::new("pk_value", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::UInt64, false),
        Field::new("props_json", DataType::Utf8, true),
    ]))
}

/// Format B: Hybrid — 5 promoted columns + overflow JSON. No pk_key.
fn schema_b() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("op", DataType::UInt8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("pk_value", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::UInt64, false),
        Field::new("repo", DataType::Utf8, true),
        Field::new("rkey", DataType::Utf8, true),
        Field::new("owner_did", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("app_id", DataType::Utf8, true),
        Field::new("props_json", DataType::Utf8, true),
    ]))
}

/// Format C: Wide columnar — all known props as columns, extra_json fallback.
fn schema_c() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("op", DataType::UInt8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("pk_value", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::UInt64, false),
        Field::new("repo", DataType::Utf8, true),
        Field::new("rkey", DataType::Utf8, true),
        Field::new("owner_did", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("app_id", DataType::Utf8, true),
        Field::new("org_id", DataType::Utf8, true),
        Field::new("user_did", DataType::Utf8, true),
        Field::new("updated_at", DataType::Utf8, true),
        Field::new("age", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("extra_json", DataType::Utf8, true),
    ]))
}

// ── Batch builders ──────────────────────────────────────────────────────

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Build Format A batch: all props serialized into props_json.
fn build_batch_a(vertices: &[VertexData]) -> RecordBatch {
    let n = vertices.len();
    let ts = now_ms();

    let seqs: Vec<u64> = (0..n as u64).collect();
    let ops: Vec<u8> = vec![0u8; n];
    let labels: Vec<&str> = vertices.iter().map(|v| v.label.as_str()).collect();
    let pk_keys: Vec<&str> = vec!["rkey"; n];
    let pk_values: Vec<&str> = vertices.iter().map(|v| v.pk_value.as_str()).collect();
    let timestamps: Vec<u64> = vec![ts; n];

    let props_jsons: Vec<String> = vertices
        .iter()
        .map(|v| {
            serde_json::json!({
                "name": v.name,
                "description": v.description,
                "age": v.age,
                "score": v.score,
                "repo": v.repo,
                "rkey": v.rkey,
                "owner_did": v.owner_did,
                "app_id": v.app_id,
                "org_id": v.org_id,
                "user_did": v.user_did,
            })
            .to_string()
        })
        .collect();
    let props_refs: Vec<&str> = props_jsons.iter().map(|s| s.as_str()).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(seqs)),
        Arc::new(UInt8Array::from(ops)),
        Arc::new(StringArray::from(labels)),
        Arc::new(StringArray::from(pk_keys)),
        Arc::new(StringArray::from(pk_values)),
        Arc::new(UInt64Array::from(timestamps)),
        Arc::new(StringArray::from(props_refs)),
    ];

    RecordBatch::try_new(schema_a(), columns).unwrap()
}

/// Build Format B batch: promoted columns + overflow JSON for remaining props.
fn build_batch_b(vertices: &[VertexData]) -> RecordBatch {
    let n = vertices.len();
    let ts = now_ms();

    let seqs: Vec<u64> = (0..n as u64).collect();
    let ops: Vec<u8> = vec![0u8; n];
    let labels: Vec<&str> = vertices.iter().map(|v| v.label.as_str()).collect();
    let pk_values: Vec<&str> = vertices.iter().map(|v| v.pk_value.as_str()).collect();
    let timestamps: Vec<u64> = vec![ts; n];
    let repos: Vec<&str> = vertices.iter().map(|v| v.repo.as_str()).collect();
    let rkeys: Vec<&str> = vertices.iter().map(|v| v.rkey.as_str()).collect();
    let owner_dids: Vec<&str> = vertices.iter().map(|v| v.owner_did.as_str()).collect();
    let names: Vec<&str> = vertices.iter().map(|v| v.name.as_str()).collect();
    let app_ids: Vec<&str> = vertices.iter().map(|v| v.app_id.as_str()).collect();

    // Overflow JSON: properties not promoted to columns
    let props_jsons: Vec<String> = vertices
        .iter()
        .map(|v| {
            serde_json::json!({
                "description": v.description,
                "age": v.age,
                "score": v.score,
                "org_id": v.org_id,
                "user_did": v.user_did,
            })
            .to_string()
        })
        .collect();
    let props_refs: Vec<&str> = props_jsons.iter().map(|s| s.as_str()).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(seqs)),
        Arc::new(UInt8Array::from(ops)),
        Arc::new(StringArray::from(labels)),
        Arc::new(StringArray::from(pk_values)),
        Arc::new(UInt64Array::from(timestamps)),
        Arc::new(StringArray::from(repos)),
        Arc::new(StringArray::from(rkeys)),
        Arc::new(StringArray::from(owner_dids)),
        Arc::new(StringArray::from(names)),
        Arc::new(StringArray::from(app_ids)),
        Arc::new(StringArray::from(props_refs)),
    ];

    RecordBatch::try_new(schema_b(), columns).unwrap()
}

/// Build Format C batch: all properties as columns, extra_json for unknowns.
fn build_batch_c(vertices: &[VertexData]) -> RecordBatch {
    let n = vertices.len();
    let ts = now_ms();
    let now_str = format!("{}", now_ms());

    let seqs: Vec<u64> = (0..n as u64).collect();
    let ops: Vec<u8> = vec![0u8; n];
    let labels: Vec<&str> = vertices.iter().map(|v| v.label.as_str()).collect();
    let pk_values: Vec<&str> = vertices.iter().map(|v| v.pk_value.as_str()).collect();
    let timestamps: Vec<u64> = vec![ts; n];
    let repos: Vec<&str> = vertices.iter().map(|v| v.repo.as_str()).collect();
    let rkeys: Vec<&str> = vertices.iter().map(|v| v.rkey.as_str()).collect();
    let owner_dids: Vec<&str> = vertices.iter().map(|v| v.owner_did.as_str()).collect();
    let names: Vec<&str> = vertices.iter().map(|v| v.name.as_str()).collect();
    let descriptions: Vec<&str> = vertices.iter().map(|v| v.description.as_str()).collect();
    let app_ids: Vec<&str> = vertices.iter().map(|v| v.app_id.as_str()).collect();
    let org_ids: Vec<&str> = vertices.iter().map(|v| v.org_id.as_str()).collect();
    let user_dids: Vec<&str> = vertices.iter().map(|v| v.user_did.as_str()).collect();
    let updated_ats: Vec<&str> = vec![now_str.as_str(); n];
    let ages: Vec<i64> = vertices.iter().map(|v| v.age).collect();
    let scores: Vec<f64> = vertices.iter().map(|v| v.score).collect();
    let extras: Vec<&str> = vec!["{}"; n];

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(seqs)),
        Arc::new(UInt8Array::from(ops)),
        Arc::new(StringArray::from(labels)),
        Arc::new(StringArray::from(pk_values)),
        Arc::new(UInt64Array::from(timestamps)),
        Arc::new(StringArray::from(repos)),
        Arc::new(StringArray::from(rkeys)),
        Arc::new(StringArray::from(owner_dids)),
        Arc::new(StringArray::from(names)),
        Arc::new(StringArray::from(descriptions)),
        Arc::new(StringArray::from(app_ids)),
        Arc::new(StringArray::from(org_ids)),
        Arc::new(StringArray::from(user_dids)),
        Arc::new(StringArray::from(updated_ats)),
        Arc::new(Int64Array::from(ages)),
        Arc::new(Float64Array::from(scores)),
        Arc::new(StringArray::from(extras)),
    ];

    RecordBatch::try_new(schema_c(), columns).unwrap()
}

// ── ArrowStore adapters (Format A only uses standard from_batches) ───────

/// Build ArrowStore-compatible batches for Format B.
/// Converts promoted columns back into the 7-col schema that ArrowStore expects.
fn build_arrowstore_batches_b(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    let target_schema = schema_a();
    batches
        .iter()
        .map(|batch| {
            let n = batch.num_rows();
            // Reconstruct props_json from promoted columns + overflow
            let label_col = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
            let pk_val_col = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
            let repo_col = batch.column(5).as_any().downcast_ref::<StringArray>().unwrap();
            let rkey_col = batch.column(6).as_any().downcast_ref::<StringArray>().unwrap();
            let owner_col = batch.column(7).as_any().downcast_ref::<StringArray>().unwrap();
            let name_col = batch.column(8).as_any().downcast_ref::<StringArray>().unwrap();
            let app_col = batch.column(9).as_any().downcast_ref::<StringArray>().unwrap();
            let overflow_col = batch.column(10).as_any().downcast_ref::<StringArray>().unwrap();

            let mut props_jsons = Vec::with_capacity(n);
            for row in 0..n {
                let mut map: serde_json::Map<String, serde_json::Value> = if !overflow_col.is_null(row) {
                    serde_json::from_str(overflow_col.value(row)).unwrap_or_default()
                } else {
                    serde_json::Map::new()
                };
                if !repo_col.is_null(row) { map.insert("repo".into(), repo_col.value(row).into()); }
                if !rkey_col.is_null(row) { map.insert("rkey".into(), rkey_col.value(row).into()); }
                if !owner_col.is_null(row) { map.insert("owner_did".into(), owner_col.value(row).into()); }
                if !name_col.is_null(row) { map.insert("name".into(), name_col.value(row).into()); }
                if !app_col.is_null(row) { map.insert("app_id".into(), app_col.value(row).into()); }
                props_jsons.push(serde_json::to_string(&map).unwrap());
            }
            let props_refs: Vec<&str> = props_jsons.iter().map(|s| s.as_str()).collect();

            let labels: Vec<&str> = (0..n).map(|i| label_col.value(i)).collect();
            let pk_vals: Vec<&str> = (0..n).map(|i| pk_val_col.value(i)).collect();

            let columns: Vec<ArrayRef> = vec![
                batch.column(0).clone(), // seq
                batch.column(1).clone(), // op
                Arc::new(StringArray::from(labels)),
                Arc::new(StringArray::from(vec!["rkey"; n])), // pk_key
                Arc::new(StringArray::from(pk_vals)),
                batch.column(4).clone(), // timestamp_ms
                Arc::new(StringArray::from(props_refs)),
            ];
            RecordBatch::try_new(target_schema.clone(), columns).unwrap()
        })
        .collect()
}

/// Build ArrowStore-compatible batches for Format C.
fn build_arrowstore_batches_c(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    let target_schema = schema_a();
    batches
        .iter()
        .map(|batch| {
            let n = batch.num_rows();
            let label_col = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
            let pk_val_col = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
            let repo_col = batch.column(5).as_any().downcast_ref::<StringArray>().unwrap();
            let rkey_col = batch.column(6).as_any().downcast_ref::<StringArray>().unwrap();
            let owner_col = batch.column(7).as_any().downcast_ref::<StringArray>().unwrap();
            let name_col = batch.column(8).as_any().downcast_ref::<StringArray>().unwrap();
            let desc_col = batch.column(9).as_any().downcast_ref::<StringArray>().unwrap();
            let app_col = batch.column(10).as_any().downcast_ref::<StringArray>().unwrap();
            let org_col = batch.column(11).as_any().downcast_ref::<StringArray>().unwrap();
            let udid_col = batch.column(12).as_any().downcast_ref::<StringArray>().unwrap();
            let upd_col = batch.column(13).as_any().downcast_ref::<StringArray>().unwrap();
            let age_col = batch.column(14).as_any().downcast_ref::<Int64Array>().unwrap();
            let score_col = batch.column(15).as_any().downcast_ref::<Float64Array>().unwrap();

            let mut props_jsons = Vec::with_capacity(n);
            for row in 0..n {
                let map = serde_json::json!({
                    "repo": repo_col.value(row),
                    "rkey": rkey_col.value(row),
                    "owner_did": owner_col.value(row),
                    "name": name_col.value(row),
                    "description": desc_col.value(row),
                    "app_id": app_col.value(row),
                    "org_id": org_col.value(row),
                    "user_did": udid_col.value(row),
                    "updated_at": upd_col.value(row),
                    "age": age_col.value(row),
                    "score": score_col.value(row),
                });
                props_jsons.push(map.to_string());
            }
            let props_refs: Vec<&str> = props_jsons.iter().map(|s| s.as_str()).collect();

            let labels: Vec<&str> = (0..n).map(|i| label_col.value(i)).collect();
            let pk_vals: Vec<&str> = (0..n).map(|i| pk_val_col.value(i)).collect();

            let columns: Vec<ArrayRef> = vec![
                batch.column(0).clone(),
                batch.column(1).clone(),
                Arc::new(StringArray::from(labels)),
                Arc::new(StringArray::from(vec!["rkey"; n])),
                Arc::new(StringArray::from(pk_vals)),
                batch.column(4).clone(),
                Arc::new(StringArray::from(props_refs)),
            ];
            RecordBatch::try_new(target_schema.clone(), columns).unwrap()
        })
        .collect()
}

// ── Benchmark runner ────────────────────────────────────────────────────

struct FormatResult {
    name: &'static str,
    bulk_write_ms: u64,
    single_write_mean_us: u64,
    scan_all_ms: u64,
    label_filter_ms: u64,
    name_filter_ms: u64,
    numeric_filter_ms: u64,
    arrowstore_build_ms: u64,
    compact_ms: u64,
    row_count: usize,
    disk_bytes: u64,
}

async fn bench_format<F>(
    format_name: &'static str,
    schema: Arc<Schema>,
    vertices: &[VertexData],
    batch_sz: usize,
    build_batch: F,
) -> FormatResult
where
    F: Fn(&[VertexData]) -> RecordBatch,
{
    let tmpdir = tempfile::tempdir().unwrap();
    let db = YataDb::connect_local(tmpdir.path().to_str().unwrap())
        .await
        .unwrap();

    // ── Write: Bulk insert ──────────────────────────────────────────────
    let t0 = Instant::now();
    let first_batch = build_batch(&vertices[..batch_sz.min(vertices.len())]);
    let tbl = db.create_table("vertices", first_batch).await.unwrap();

    for chunk in vertices[batch_sz.min(vertices.len())..].chunks(batch_sz) {
        let batch = build_batch(chunk);
        tbl.add(batch).await.unwrap();
    }
    let bulk_write_ms = t0.elapsed().as_millis() as u64;

    let row_count = tbl.count_rows(None).await.unwrap();

    // ── Write: Single-row inserts (measure per-append overhead) ─────────
    let single_tmpdir = tempfile::tempdir().unwrap();
    let single_db = YataDb::connect_local(single_tmpdir.path().to_str().unwrap())
        .await
        .unwrap();
    let first = build_batch(&vertices[..1]);
    let single_tbl = single_db.create_table("vertices", first).await.unwrap();

    let mut single_samples = Samples::new();
    for v in vertices.iter().take(1000).skip(1) {
        let batch = build_batch(std::slice::from_ref(v));
        let t = Instant::now();
        single_tbl.add(batch).await.unwrap();
        single_samples.push_us(t.elapsed().as_micros() as u64);
    }
    single_samples.report("single-row append", format_name);

    // ── Read: Full scan ─────────────────────────────────────────────────
    let mut scan_samples = Samples::new();
    for _ in 0..5 {
        let t = Instant::now();
        let _batches = tbl.scan_all().await.unwrap();
        scan_samples.push_ms(t.elapsed().as_millis() as u64);
    }
    scan_samples.report("scan_all (ms)", format_name);

    // ── Read: Label filter ──────────────────────────────────────────────
    let mut label_samples = Samples::new();
    for _ in 0..10 {
        let t = Instant::now();
        let _batches = tbl.scan_filter("label = 'Person'").await.unwrap();
        label_samples.push_ms(t.elapsed().as_millis() as u64);
    }
    label_samples.report("label filter (ms)", format_name);

    // ── Read: Name filter (columnar for B/C, JSON scan for A) ───────────
    let name_filter = if schema.column_with_name("name").is_some() {
        "name = 'Person_500'"
    } else {
        // Format A: Lance can't filter inside props_json — must scan all
        "label = 'Person'"
    };
    let mut name_samples = Samples::new();
    for _ in 0..10 {
        let t = Instant::now();
        let _batches = tbl.scan_filter(name_filter).await.unwrap();
        name_samples.push_ms(t.elapsed().as_millis() as u64);
    }
    let name_label = if schema.column_with_name("name").is_some() {
        "name='Person_500' (columnar)"
    } else {
        "label='Person' (no name col)"
    };
    name_samples.report(name_label, format_name);

    // ── Read: Numeric filter (only Format C has `age` column) ───────────
    let mut numeric_samples = Samples::new();
    if schema.column_with_name("age").is_some() {
        for _ in 0..10 {
            let t = Instant::now();
            let _batches = tbl.scan_filter("age > 50").await.unwrap();
            numeric_samples.push_ms(t.elapsed().as_millis() as u64);
        }
        numeric_samples.report("age > 50 (columnar, ms)", format_name);
    } else {
        // Simulate equivalent: full scan (Lance can't filter JSON internals)
        for _ in 0..10 {
            let t = Instant::now();
            let _batches = tbl.scan_all().await.unwrap();
            numeric_samples.push_ms(t.elapsed().as_millis() as u64);
        }
        numeric_samples.report("full scan (no age col, ms)", format_name);
    }

    // ── ArrowStore build (Format A native, B/C need conversion) ─────────
    let mut arrow_samples = Samples::new();
    for _ in 0..5 {
        let t = Instant::now();
        let raw_batches = tbl.scan_all().await.unwrap();
        // ArrowStore expects the 7-col schema — Format B/C need conversion
        let _store = ArrowStore::from_batches(raw_batches);
        arrow_samples.push_ms(t.elapsed().as_millis() as u64);
    }
    arrow_samples.report("ArrowStore build (ms)", format_name);

    // ── Compaction ──────────────────────────────────────────────────────
    let mut compact_samples = Samples::new();
    {
        let t = Instant::now();
        let _stats = tbl.compact().await;
        compact_samples.push_ms(t.elapsed().as_millis() as u64);
    }
    compact_samples.report("compact (ms)", format_name);

    // ── Disk size ───────────────────────────────────────────────────────
    let disk_bytes = dir_size(tmpdir.path());

    FormatResult {
        name: format_name,
        bulk_write_ms,
        single_write_mean_us: single_samples.mean(),
        scan_all_ms: scan_samples.mean(),
        label_filter_ms: label_samples.mean(),
        name_filter_ms: name_samples.mean(),
        numeric_filter_ms: numeric_samples.mean(),
        arrowstore_build_ms: arrow_samples.mean(),
        compact_ms: compact_samples.mean(),
        row_count,
        disk_bytes,
    }
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let meta = entry.metadata();
            if let Ok(m) = meta {
                if m.is_dir() {
                    total += dir_size(&entry.path());
                } else {
                    total += m.len();
                }
            }
        }
    }
    total
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GiB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MiB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

// ── main ────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let n_vertices: usize = std::env::var("VERTICES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    let batch_sz: usize = std::env::var("BATCH_SZ")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000);

    println!("╔══════════════════════════════════════════════════════════════════════════╗");
    println!("║           yata-schema-bench  —  Lance Table Format Comparison           ║");
    println!("╚══════════════════════════════════════════════════════════════════════════╝");
    println!("  vertices={n_vertices}  batch_sz={batch_sz}  seed=42\n");

    let vertices = generate_vertices(n_vertices, 42);

    // ── Format A (current) ──────────────────────────────────────────────
    println!("━━━ Format A: Current (7 cols, props_json blob) ━━━");
    let result_a = bench_format("format-a", schema_a(), &vertices, batch_sz, |vs| {
        build_batch_a(vs)
    })
    .await;

    println!();

    // ── Format B (hybrid) ───────────────────────────────────────────────
    println!("━━━ Format B: Hybrid (11 cols, promoted + overflow JSON) ━━━");
    let result_b = bench_format("format-b", schema_b(), &vertices, batch_sz, |vs| {
        build_batch_b(vs)
    })
    .await;

    println!();

    // ── Format C (wide) ─────────────────────────────────────────────────
    println!("━━━ Format C: Wide (17 cols, all columnar) ━━━");
    let result_c = bench_format("format-c", schema_c(), &vertices, batch_sz, |vs| {
        build_batch_c(vs)
    })
    .await;

    // ── Summary ─────────────────────────────────────────────────────────
    println!("\n╔══════════════════════════════════════════════════════════════════════════╗");
    println!("║                              Summary                                   ║");
    println!("╚══════════════════════════════════════════════════════════════════════════╝");
    println!();

    let results = [&result_a, &result_b, &result_c];
    println!(
        "  {:<14} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "format", "rows", "bulk(ms)", "1row(µs)", "scan(ms)", "label(ms)", "name(ms)", "num(ms)", "arrow(ms)", "disk"
    );
    println!("  {}", "─".repeat(108));

    for r in &results {
        let delta_bulk = if r.bulk_write_ms == result_a.bulk_write_ms {
            String::new()
        } else {
            let pct = (r.bulk_write_ms as f64 - result_a.bulk_write_ms as f64)
                / result_a.bulk_write_ms as f64
                * 100.0;
            format!(" ({pct:+.0}%)")
        };
        println!(
            "  {:<14} {:>8} {:>7}{:<3} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
            r.name,
            r.row_count,
            r.bulk_write_ms,
            delta_bulk,
            r.single_write_mean_us,
            r.scan_all_ms,
            r.label_filter_ms,
            r.name_filter_ms,
            r.numeric_filter_ms,
            r.arrowstore_build_ms,
            format_bytes(r.disk_bytes),
        );
    }

    // Delta analysis
    println!();
    if result_a.scan_all_ms > 0 {
        let scan_delta_b =
            (result_b.scan_all_ms as f64 - result_a.scan_all_ms as f64) / result_a.scan_all_ms as f64 * 100.0;
        let scan_delta_c =
            (result_c.scan_all_ms as f64 - result_a.scan_all_ms as f64) / result_a.scan_all_ms as f64 * 100.0;
        println!("  Scan Δ vs A:    B={scan_delta_b:+.1}%   C={scan_delta_c:+.1}%");
    }
    if result_a.label_filter_ms > 0 {
        let lf_delta_b =
            (result_b.label_filter_ms as f64 - result_a.label_filter_ms as f64) / result_a.label_filter_ms as f64 * 100.0;
        let lf_delta_c =
            (result_c.label_filter_ms as f64 - result_a.label_filter_ms as f64) / result_a.label_filter_ms as f64 * 100.0;
        println!("  Label Δ vs A:   B={lf_delta_b:+.1}%   C={lf_delta_c:+.1}%");
    }
    if result_a.disk_bytes > 0 {
        let disk_delta_b =
            (result_b.disk_bytes as f64 - result_a.disk_bytes as f64) / result_a.disk_bytes as f64 * 100.0;
        let disk_delta_c =
            (result_c.disk_bytes as f64 - result_a.disk_bytes as f64) / result_a.disk_bytes as f64 * 100.0;
        println!(
            "  Disk Δ vs A:    B={disk_delta_b:+.1}%   C={disk_delta_c:+.1}%"
        );
    }

    println!("\ndone");
    Ok(())
}
