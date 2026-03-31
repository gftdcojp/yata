//! Lance table schema benchmark v2: 5 JSON-free patterns.
//!
//! All patterns store ZERO JSON text. Compares different Arrow-native strategies.
//!
//! D: Narrow Essential (9 cols)  — Core + top-4 typed, no overflow
//! E: Wide Typed (15 cols)       — All known props as native Arrow types
//! F: Struct Column (6 cols)     — Core + Arrow Struct with typed sub-fields
//! G: Dictionary Encoded (15 cols) — Repetitive string cols as Dictionary<Int32,Utf8>
//! H: Partitioned by Label       — Separate table per label, label-specific columns
//!
//! Also includes Format A (baseline) and Format B (hybrid) from v1 for comparison.
//!
//! Usage:
//!   cargo run -p yata-bench --bin schema-bench-v2 --release
//!
//! Environment variables:
//!   VERTICES  (default: 100000)
//!   BATCH_SZ  (default: 1000)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StringBuilder, StructArray, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
use rand::{Rng, SeedableRng, rngs::StdRng};
use yata_lance::YataDb;

// ── report helpers ──────────────────────────────────────────────────────

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
            "  [{engine:<12}] {label:<46} n={n:>4}  mean={mean:>8}  p50={p50:>8}  p95={p95:>8}  p99={p99:>8}  max={max:>8}",
        );
    }
    fn mean(&self) -> u64 {
        if self.0.is_empty() { 0 } else { self.0.iter().sum::<u64>() / self.0.len() as u64 }
    }
}

// ── dataset ─────────────────────────────────────────────────────────────

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
                description: format!("Description for {label} number {i} with padding text"),
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ── Format A: Baseline (7 cols, JSON blob) ──────────────────────────────

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

fn build_batch_a(vertices: &[VertexData]) -> RecordBatch {
    let n = vertices.len();
    let ts = now_ms();
    let props: Vec<String> = vertices.iter().map(|v| {
        serde_json::json!({
            "name": v.name, "description": v.description, "age": v.age, "score": v.score,
            "repo": v.repo, "rkey": v.rkey, "owner_did": v.owner_did,
            "app_id": v.app_id, "org_id": v.org_id, "user_did": v.user_did,
        }).to_string()
    }).collect();
    let props_refs: Vec<&str> = props.iter().map(|s| s.as_str()).collect();
    RecordBatch::try_new(schema_a(), vec![
        Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())),
        Arc::new(UInt8Array::from(vec![0u8; n])),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.label.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vec!["rkey"; n])),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.pk_value.as_str()).collect::<Vec<_>>())),
        Arc::new(UInt64Array::from(vec![ts; n])),
        Arc::new(StringArray::from(props_refs)),
    ]).unwrap()
}

// ── Format B: Hybrid (11 cols, promoted + overflow JSON) ────────────────

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

fn build_batch_b(vertices: &[VertexData]) -> RecordBatch {
    let n = vertices.len();
    let ts = now_ms();
    let overflow: Vec<String> = vertices.iter().map(|v| {
        serde_json::json!({
            "description": v.description, "age": v.age, "score": v.score,
            "org_id": v.org_id, "user_did": v.user_did,
        }).to_string()
    }).collect();
    let overflow_refs: Vec<&str> = overflow.iter().map(|s| s.as_str()).collect();
    RecordBatch::try_new(schema_b(), vec![
        Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())),
        Arc::new(UInt8Array::from(vec![0u8; n])),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.label.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.pk_value.as_str()).collect::<Vec<_>>())),
        Arc::new(UInt64Array::from(vec![ts; n])),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.repo.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.rkey.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.owner_did.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.name.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.app_id.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(overflow_refs)),
    ]).unwrap()
}

// ── Format D: Narrow Essential (9 cols, typed, no overflow) ─────────────

fn schema_d() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("op", DataType::UInt8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("pk_value", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("repo", DataType::Utf8, true),
        Field::new("owner_did", DataType::Utf8, true),
        Field::new("age", DataType::Int64, true),
    ]))
}

fn build_batch_d(vertices: &[VertexData]) -> RecordBatch {
    let n = vertices.len();
    let ts = now_ms();
    RecordBatch::try_new(schema_d(), vec![
        Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())),
        Arc::new(UInt8Array::from(vec![0u8; n])),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.label.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.pk_value.as_str()).collect::<Vec<_>>())),
        Arc::new(UInt64Array::from(vec![ts; n])),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.name.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.repo.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.owner_did.as_str()).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(vertices.iter().map(|v| v.age).collect::<Vec<_>>())),
    ]).unwrap()
}

// ── Format E: Wide Typed (15 cols, all native types, no JSON) ───────────

fn schema_e() -> Arc<Schema> {
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
        Field::new("age", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
    ]))
}

fn build_batch_e(vertices: &[VertexData]) -> RecordBatch {
    let n = vertices.len();
    let ts = now_ms();
    RecordBatch::try_new(schema_e(), vec![
        Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())),
        Arc::new(UInt8Array::from(vec![0u8; n])),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.label.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.pk_value.as_str()).collect::<Vec<_>>())),
        Arc::new(UInt64Array::from(vec![ts; n])),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.repo.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.rkey.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.owner_did.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.name.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.description.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.app_id.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.org_id.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.user_did.as_str()).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(vertices.iter().map(|v| v.age).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(vertices.iter().map(|v| v.score).collect::<Vec<_>>())),
    ]).unwrap()
}

// ── Format F: Struct Column (6 cols: core + 1 Struct) ───────────────────

fn props_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("repo", DataType::Utf8, true),
        Field::new("rkey", DataType::Utf8, true),
        Field::new("owner_did", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("app_id", DataType::Utf8, true),
        Field::new("org_id", DataType::Utf8, true),
        Field::new("user_did", DataType::Utf8, true),
        Field::new("age", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
    ])
}

fn schema_f() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("op", DataType::UInt8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("pk_value", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::UInt64, false),
        Field::new("props", DataType::Struct(props_struct_fields()), true),
    ]))
}

fn build_batch_f(vertices: &[VertexData]) -> RecordBatch {
    let n = vertices.len();
    let ts = now_ms();

    let struct_array = StructArray::from(vec![
        (Arc::new(Field::new("repo", DataType::Utf8, true)),
         Arc::new(StringArray::from(vertices.iter().map(|v| v.repo.as_str()).collect::<Vec<_>>())) as ArrayRef),
        (Arc::new(Field::new("rkey", DataType::Utf8, true)),
         Arc::new(StringArray::from(vertices.iter().map(|v| v.rkey.as_str()).collect::<Vec<_>>())) as ArrayRef),
        (Arc::new(Field::new("owner_did", DataType::Utf8, true)),
         Arc::new(StringArray::from(vertices.iter().map(|v| v.owner_did.as_str()).collect::<Vec<_>>())) as ArrayRef),
        (Arc::new(Field::new("name", DataType::Utf8, true)),
         Arc::new(StringArray::from(vertices.iter().map(|v| v.name.as_str()).collect::<Vec<_>>())) as ArrayRef),
        (Arc::new(Field::new("description", DataType::Utf8, true)),
         Arc::new(StringArray::from(vertices.iter().map(|v| v.description.as_str()).collect::<Vec<_>>())) as ArrayRef),
        (Arc::new(Field::new("app_id", DataType::Utf8, true)),
         Arc::new(StringArray::from(vertices.iter().map(|v| v.app_id.as_str()).collect::<Vec<_>>())) as ArrayRef),
        (Arc::new(Field::new("org_id", DataType::Utf8, true)),
         Arc::new(StringArray::from(vertices.iter().map(|v| v.org_id.as_str()).collect::<Vec<_>>())) as ArrayRef),
        (Arc::new(Field::new("user_did", DataType::Utf8, true)),
         Arc::new(StringArray::from(vertices.iter().map(|v| v.user_did.as_str()).collect::<Vec<_>>())) as ArrayRef),
        (Arc::new(Field::new("age", DataType::Int64, true)),
         Arc::new(Int64Array::from(vertices.iter().map(|v| v.age).collect::<Vec<_>>())) as ArrayRef),
        (Arc::new(Field::new("score", DataType::Float64, true)),
         Arc::new(Float64Array::from(vertices.iter().map(|v| v.score).collect::<Vec<_>>())) as ArrayRef),
    ]);

    RecordBatch::try_new(schema_f(), vec![
        Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())),
        Arc::new(UInt8Array::from(vec![0u8; n])),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.label.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.pk_value.as_str()).collect::<Vec<_>>())),
        Arc::new(UInt64Array::from(vec![ts; n])),
        Arc::new(struct_array) as ArrayRef,
    ]).unwrap()
}

// ── Format G: Dictionary Encoded (15 cols, repetitive strings compressed) ──

fn schema_g() -> Arc<Schema> {
    // label, name, rkey, description stay plain Utf8 (Lance filter compat).
    // Repetitive-value cols (repo, owner_did, app_id, org_id, user_did) → Dictionary.
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("op", DataType::UInt8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("pk_value", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::UInt64, false),
        Field::new("repo", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), true),
        Field::new("rkey", DataType::Utf8, true),
        Field::new("owner_did", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), true),
        Field::new("name", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("app_id", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), true),
        Field::new("org_id", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), true),
        Field::new("user_did", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), true),
        Field::new("age", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
    ]))
}

/// Build a Dictionary<Int32, Utf8> array from string values.
fn build_dict_array(values: &[&str]) -> ArrayRef {
    // Build dictionary: unique values → index
    let mut dict: HashMap<&str, i32> = HashMap::new();
    let mut dict_values = Vec::new();
    let mut indices = Vec::with_capacity(values.len());

    for &val in values {
        let idx = if let Some(&existing) = dict.get(val) {
            existing
        } else {
            let idx = dict_values.len() as i32;
            dict.insert(val, idx);
            dict_values.push(val);
            idx
        };
        indices.push(idx);
    }

    let keys = Int32Array::from(indices);
    let values_array = StringArray::from(dict_values);
    Arc::new(
        arrow::array::DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values_array)).unwrap(),
    )
}

fn build_batch_g(vertices: &[VertexData]) -> RecordBatch {
    let n = vertices.len();
    let ts = now_ms();

    let repos: Vec<&str> = vertices.iter().map(|v| v.repo.as_str()).collect();
    let owner_dids: Vec<&str> = vertices.iter().map(|v| v.owner_did.as_str()).collect();
    let app_ids: Vec<&str> = vertices.iter().map(|v| v.app_id.as_str()).collect();
    let org_ids: Vec<&str> = vertices.iter().map(|v| v.org_id.as_str()).collect();
    let user_dids: Vec<&str> = vertices.iter().map(|v| v.user_did.as_str()).collect();

    RecordBatch::try_new(schema_g(), vec![
        Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())),
        Arc::new(UInt8Array::from(vec![0u8; n])),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.label.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.pk_value.as_str()).collect::<Vec<_>>())),
        Arc::new(UInt64Array::from(vec![ts; n])),
        build_dict_array(&repos),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.rkey.as_str()).collect::<Vec<_>>())),
        build_dict_array(&owner_dids),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.name.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.description.as_str()).collect::<Vec<_>>())),
        build_dict_array(&app_ids),
        build_dict_array(&org_ids),
        build_dict_array(&user_dids),
        Arc::new(Int64Array::from(vertices.iter().map(|v| v.age).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(vertices.iter().map(|v| v.score).collect::<Vec<_>>())),
    ]).unwrap()
}

// ── Format H: Partitioned by Label (separate table per label) ───────────

/// Schema for each per-label table (no label column needed).
fn schema_h() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("op", DataType::UInt8, false),
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
        Field::new("age", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
    ]))
}

fn build_batch_h(vertices: &[VertexData]) -> RecordBatch {
    let n = vertices.len();
    let ts = now_ms();
    RecordBatch::try_new(schema_h(), vec![
        Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())),
        Arc::new(UInt8Array::from(vec![0u8; n])),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.pk_value.as_str()).collect::<Vec<_>>())),
        Arc::new(UInt64Array::from(vec![ts; n])),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.repo.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.rkey.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.owner_did.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.name.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.description.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.app_id.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.org_id.as_str()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vertices.iter().map(|v| v.user_did.as_str()).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(vertices.iter().map(|v| v.age).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(vertices.iter().map(|v| v.score).collect::<Vec<_>>())),
    ]).unwrap()
}

// ── Benchmark runner ────────────────────────────────────────────────────

#[derive(Default)]
struct FormatResult {
    name: String,
    bulk_write_ms: u64,
    single_write_mean_us: u64,
    scan_all_ms: u64,
    label_filter_ms: u64,
    name_filter_ms: u64,
    age_filter_ms: u64,
    compact_ms: u64,
    row_count: usize,
    disk_bytes: u64,
}

/// Standard benchmark for single-table formats (A, B, D, E, F, G).
async fn bench_single_table<F>(
    format_name: &str,
    schema: Arc<Schema>,
    vertices: &[VertexData],
    batch_sz: usize,
    build_batch: F,
) -> FormatResult
where
    F: Fn(&[VertexData]) -> RecordBatch,
{
    let tmpdir = tempfile::tempdir().unwrap();
    let db = YataDb::connect_local(tmpdir.path().to_str().unwrap()).await.unwrap();

    // ── Bulk write ──────────────────────────────────────────────────────
    let t0 = Instant::now();
    let first = build_batch(&vertices[..batch_sz.min(vertices.len())]);
    let tbl = db.create_table("vertices", first).await.unwrap();
    for chunk in vertices[batch_sz.min(vertices.len())..].chunks(batch_sz) {
        tbl.add(build_batch(chunk)).await.unwrap();
    }
    let bulk_write_ms = t0.elapsed().as_millis() as u64;
    let row_count = tbl.count_rows(None).await.unwrap();

    // ── Single-row write ────────────────────────────────────────────────
    let stmpdir = tempfile::tempdir().unwrap();
    let sdb = YataDb::connect_local(stmpdir.path().to_str().unwrap()).await.unwrap();
    let stbl = sdb.create_table("vertices", build_batch(&vertices[..1])).await.unwrap();
    let mut single_samples = Samples::new();
    for v in vertices.iter().take(500).skip(1) {
        let batch = build_batch(std::slice::from_ref(v));
        let t = Instant::now();
        stbl.add(batch).await.unwrap();
        single_samples.push_us(t.elapsed().as_micros() as u64);
    }
    single_samples.report("single-row append (µs)", format_name);

    // ── Scan all ────────────────────────────────────────────────────────
    let mut scan_samples = Samples::new();
    for _ in 0..5 {
        let t = Instant::now();
        let _b = tbl.scan_all().await.unwrap();
        scan_samples.push_ms(t.elapsed().as_millis() as u64);
    }
    scan_samples.report("scan_all (ms)", format_name);

    // ── Label filter ────────────────────────────────────────────────────
    let has_label_col = schema.column_with_name("label").is_some();
    let mut label_samples = Samples::new();
    if has_label_col {
        for _ in 0..10 {
            let t = Instant::now();
            let _b = tbl.scan_filter("label = 'Person'").await.unwrap();
            label_samples.push_ms(t.elapsed().as_millis() as u64);
        }
        label_samples.report("label='Person' (ms)", format_name);
    }

    // ── Name filter (columnar pushdown) ─────────────────────────────────
    let mut name_samples = Samples::new();
    let name_filter = if schema.column_with_name("name").is_some() {
        "name = 'Person_500'"
    } else if schema.column_with_name("props").is_some() {
        // Struct field access via DataFusion dot notation
        "props.name = 'Person_500'"
    } else {
        "label = 'Person'" // fallback
    };
    for _ in 0..10 {
        let t = Instant::now();
        let result = tbl.scan_filter(name_filter).await;
        match result {
            Ok(_b) => name_samples.push_ms(t.elapsed().as_millis() as u64),
            Err(e) => {
                println!("  [{format_name:<12}] name filter FAILED: {e}");
                break;
            }
        }
    }
    if !name_samples.0.is_empty() {
        let label = format!("filter: {name_filter} (ms)");
        name_samples.report(&label, format_name);
    }

    // ── Age filter (numeric, columnar) ──────────────────────────────────
    let mut age_samples = Samples::new();
    let age_filter = if schema.column_with_name("age").is_some() {
        Some("age > 50")
    } else if schema.column_with_name("props").is_some() {
        Some("props.age > 50")
    } else {
        None
    };
    if let Some(af) = age_filter {
        for _ in 0..10 {
            let t = Instant::now();
            match tbl.scan_filter(af).await {
                Ok(_b) => age_samples.push_ms(t.elapsed().as_millis() as u64),
                Err(e) => {
                    println!("  [{format_name:<12}] age filter FAILED: {e}");
                    break;
                }
            }
        }
        if !age_samples.0.is_empty() {
            let label = format!("filter: {af} (ms)");
            age_samples.report(&label, format_name);
        }
    }

    // ── Compact ─────────────────────────────────────────────────────────
    let mut compact_samples = Samples::new();
    let t = Instant::now();
    let _ = tbl.compact().await;
    compact_samples.push_ms(t.elapsed().as_millis() as u64);
    compact_samples.report("compact (ms)", format_name);

    let disk_bytes = dir_size(tmpdir.path());

    FormatResult {
        name: format_name.to_string(),
        bulk_write_ms,
        single_write_mean_us: single_samples.mean(),
        scan_all_ms: scan_samples.mean(),
        label_filter_ms: label_samples.mean(),
        name_filter_ms: name_samples.mean(),
        age_filter_ms: age_samples.mean(),
        compact_ms: compact_samples.mean(),
        row_count,
        disk_bytes,
    }
}

/// Benchmark for Format H: partitioned by label (one table per label).
async fn bench_partitioned(
    vertices: &[VertexData],
    batch_sz: usize,
) -> FormatResult {
    let format_name = "format-h";
    let tmpdir = tempfile::tempdir().unwrap();
    let db = YataDb::connect_local(tmpdir.path().to_str().unwrap()).await.unwrap();

    // Group vertices by label
    let mut by_label: HashMap<String, Vec<&VertexData>> = HashMap::new();
    for v in vertices {
        by_label.entry(v.label.clone()).or_default().push(v);
    }

    // ── Bulk write (one table per label) ────────────────────────────────
    let t0 = Instant::now();
    let mut tables: HashMap<String, yata_lance::YataTable> = HashMap::new();
    for (label, verts) in &by_label {
        let tbl_name = format!("v_{}", label.to_lowercase());
        for (ci, chunk) in verts.chunks(batch_sz).enumerate() {
            let owned: Vec<VertexData> = chunk.iter().map(|v| VertexData {
                label: v.label.clone(), pk_value: v.pk_value.clone(), name: v.name.clone(),
                description: v.description.clone(), age: v.age, score: v.score,
                repo: v.repo.clone(), rkey: v.rkey.clone(), owner_did: v.owner_did.clone(),
                app_id: v.app_id.clone(), org_id: v.org_id.clone(), user_did: v.user_did.clone(),
            }).collect();
            let batch = build_batch_h(&owned);
            if ci == 0 {
                let tbl = db.create_table(&tbl_name, batch).await.unwrap();
                tables.insert(label.clone(), tbl);
            } else {
                tables.get(label.as_str()).unwrap().add(batch).await.unwrap();
            }
        }
    }
    let bulk_write_ms = t0.elapsed().as_millis() as u64;

    let mut total_rows = 0usize;
    for tbl in tables.values() {
        total_rows += tbl.count_rows(None).await.unwrap();
    }

    // ── Single-row write ────────────────────────────────────────────────
    let mut single_samples = Samples::new();
    // Pick the Person table
    if let Some(person_tbl) = tables.get("Person") {
        let person_verts: Vec<&VertexData> = vertices.iter().filter(|v| v.label == "Person").take(500).collect();
        for v in person_verts.iter().skip(1) {
            let owned = vec![VertexData {
                label: v.label.clone(), pk_value: v.pk_value.clone(), name: v.name.clone(),
                description: v.description.clone(), age: v.age, score: v.score,
                repo: v.repo.clone(), rkey: v.rkey.clone(), owner_did: v.owner_did.clone(),
                app_id: v.app_id.clone(), org_id: v.org_id.clone(), user_did: v.user_did.clone(),
            }];
            let batch = build_batch_h(&owned);
            let t = Instant::now();
            person_tbl.add(batch).await.unwrap();
            single_samples.push_us(t.elapsed().as_micros() as u64);
        }
    }
    single_samples.report("single-row append (µs)", format_name);

    // ── Scan all (must scan all tables) ─────────────────────────────────
    let mut scan_samples = Samples::new();
    for _ in 0..5 {
        let t = Instant::now();
        for tbl in tables.values() {
            let _b = tbl.scan_all().await.unwrap();
        }
        scan_samples.push_ms(t.elapsed().as_millis() as u64);
    }
    scan_samples.report("scan_all (all tables, ms)", format_name);

    // ── Label filter (just open the right table) ────────────────────────
    let mut label_samples = Samples::new();
    if let Some(person_tbl) = tables.get("Person") {
        for _ in 0..10 {
            let t = Instant::now();
            let _b = person_tbl.scan_all().await.unwrap();
            label_samples.push_ms(t.elapsed().as_millis() as u64);
        }
    }
    label_samples.report("label='Person' (table select, ms)", format_name);

    // ── Name filter ─────────────────────────────────────────────────────
    let mut name_samples = Samples::new();
    if let Some(person_tbl) = tables.get("Person") {
        for _ in 0..10 {
            let t = Instant::now();
            let _b = person_tbl.scan_filter("name = 'Person_500'").await.unwrap();
            name_samples.push_ms(t.elapsed().as_millis() as u64);
        }
    }
    name_samples.report("name='Person_500' (in Person tbl, ms)", format_name);

    // ── Age filter ──────────────────────────────────────────────────────
    let mut age_samples = Samples::new();
    if let Some(person_tbl) = tables.get("Person") {
        for _ in 0..10 {
            let t = Instant::now();
            let _b = person_tbl.scan_filter("age > 50").await.unwrap();
            age_samples.push_ms(t.elapsed().as_millis() as u64);
        }
    }
    age_samples.report("age > 50 (in Person tbl, ms)", format_name);

    // ── Compact ─────────────────────────────────────────────────────────
    let mut compact_samples = Samples::new();
    let t = Instant::now();
    for tbl in tables.values() {
        let _ = tbl.compact().await;
    }
    compact_samples.push_ms(t.elapsed().as_millis() as u64);
    compact_samples.report("compact (all tables, ms)", format_name);

    let disk_bytes = dir_size(tmpdir.path());

    FormatResult {
        name: format_name.to_string(),
        bulk_write_ms,
        single_write_mean_us: single_samples.mean(),
        scan_all_ms: scan_samples.mean(),
        label_filter_ms: label_samples.mean(),
        name_filter_ms: name_samples.mean(),
        age_filter_ms: age_samples.mean(),
        compact_ms: compact_samples.mean(),
        row_count: total_rows,
        disk_bytes,
    }
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(m) = entry.metadata() {
                if m.is_dir() { total += dir_size(&entry.path()); } else { total += m.len(); }
            }
        }
    }
    total
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_048_576 { format!("{:.1} MiB", bytes as f64 / 1_048_576.0) }
    else if bytes >= 1024 { format!("{:.1} KiB", bytes as f64 / 1024.0) }
    else { format!("{bytes} B") }
}

fn delta_str(val: u64, base: u64) -> String {
    if base == 0 || val == base { String::new() }
    else { format!(" ({:+.0}%)", (val as f64 - base as f64) / base as f64 * 100.0) }
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
        .ok().and_then(|v| v.parse().ok()).unwrap_or(100_000);
    let batch_sz: usize = std::env::var("BATCH_SZ")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(1_000);

    println!("╔══════════════════════════════════════════════════════════════════════════════════╗");
    println!("║       yata-schema-bench v2  —  JSON-Free Lance Table Format Comparison          ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════════╝");
    println!("  vertices={n_vertices}  batch_sz={batch_sz}  seed=42\n");

    let vertices = generate_vertices(n_vertices, 42);

    // ── Format A (baseline) ─────────────────────────────────────────────
    println!("━━━ A: Current (7 cols, props_json blob) ━━━");
    let ra = bench_single_table("format-a", schema_a(), &vertices, batch_sz, build_batch_a).await;
    println!();

    // ── Format B (hybrid) ───────────────────────────────────────────────
    println!("━━━ B: Hybrid (11 cols, promoted + overflow JSON) ━━━");
    let rb = bench_single_table("format-b", schema_b(), &vertices, batch_sz, build_batch_b).await;
    println!();

    // ── Format D (narrow essential) ─────────────────────────────────────
    println!("━━━ D: Narrow Essential (9 cols, typed, no overflow) ━━━");
    let rd = bench_single_table("format-d", schema_d(), &vertices, batch_sz, build_batch_d).await;
    println!();

    // ── Format E (wide typed) ───────────────────────────────────────────
    println!("━━━ E: Wide Typed (15 cols, all native types, no JSON) ━━━");
    let re = bench_single_table("format-e", schema_e(), &vertices, batch_sz, build_batch_e).await;
    println!();

    // ── Format F (struct column) ────────────────────────────────────────
    println!("━━━ F: Struct Column (6 cols: core + Arrow Struct) ━━━");
    let rf = bench_single_table("format-f", schema_f(), &vertices, batch_sz, build_batch_f).await;
    println!();

    // ── Format G (dictionary encoded) ───────────────────────────────────
    println!("━━━ G: Dictionary Encoded (15 cols, Dict<i32,Utf8> for repetitive) ━━━");
    let rg = bench_single_table("format-g", schema_g(), &vertices, batch_sz, build_batch_g).await;
    println!();

    // ── Format H (partitioned by label) ─────────────────────────────────
    println!("━━━ H: Partitioned by Label (separate table per label) ━━━");
    let rh = bench_partitioned(&vertices, batch_sz).await;
    println!();

    // ── Summary ─────────────────────────────────────────────────────────
    println!("╔══════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                                    Summary                                      ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════════╝\n");

    println!(
        "  {:<12} {:>6} {:>5} {:>10} {:>7} {:>9} {:>9} {:>9} {:>8} {:>10}",
        "format", "cols", "rows", "bulk(ms)", "1r(µs)", "scan(ms)", "label(ms)", "name(ms)", "age(ms)", "disk"
    );
    println!("  {}", "─".repeat(100));

    let all = [&ra, &rb, &rd, &re, &rf, &rg, &rh];
    let col_counts = ["7", "11", "9", "15", "6", "15", "14×5"];

    for (i, r) in all.iter().enumerate() {
        println!(
            "  {:<12} {:>6} {:>5} {:>7}{:<3} {:>7}{:<2} {:>9} {:>9} {:>9} {:>8} {:>10}",
            r.name, col_counts[i], r.row_count,
            r.bulk_write_ms, delta_str(r.bulk_write_ms, ra.bulk_write_ms),
            r.single_write_mean_us, delta_str(r.single_write_mean_us, ra.single_write_mean_us),
            r.scan_all_ms,
            r.label_filter_ms,
            r.name_filter_ms,
            r.age_filter_ms,
            format_bytes(r.disk_bytes),
        );
    }

    // ── Delta table ─────────────────────────────────────────────────────
    println!("\n  Δ vs Format A (baseline):");
    println!("  {:<12} {:>12} {:>12} {:>12} {:>12}", "format", "scan", "label", "name", "disk");
    println!("  {}", "─".repeat(62));
    for r in &all[1..] {
        println!(
            "  {:<12} {:>12} {:>12} {:>12} {:>12}",
            r.name,
            delta_str(r.scan_all_ms, ra.scan_all_ms),
            delta_str(r.label_filter_ms, ra.label_filter_ms),
            delta_str(r.name_filter_ms, ra.name_filter_ms),
            delta_str(r.disk_bytes, ra.disk_bytes),
        );
    }

    println!("\ndone");
    Ok(())
}
