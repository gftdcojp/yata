//! Compare local CSR scans against multiple Lance query modes on the same synthetic dataset.
//!
//! Usage:
//!   cargo run -p yata-bench --bin csr-vs-lance-bench --release
//!
//! Environment:
//!   NODES  (default: 100000)
//!   LABELS (default: 10)
//!   ITERS  (default: 200)
//!   COLD_ITERS (default: 20)

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use arrow::array::{Array, BooleanArray, StringArray, UInt32Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use yata_grin::{Mutable, Predicate, PropValue, Scannable};
use yata_lance::YataDataset;
use yata_store::MutableCsrStore;

struct Lat(Vec<u64>);

impl Lat {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn push_ns(&mut self, ns: u64) {
        self.0.push(ns);
    }

    fn mean_us(&self) -> f64 {
        self.0.iter().sum::<u64>() as f64 / self.0.len() as f64 / 1000.0
    }

    fn p95_us(&mut self) -> f64 {
        self.0.sort_unstable();
        self.0[self.0.len() * 95 / 100] as f64 / 1000.0
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn batch_str_col<'a>(batch: &'a RecordBatch, idx: usize) -> &'a StringArray {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column")
}

fn batch_bool_col<'a>(batch: &'a RecordBatch, idx: usize) -> &'a BooleanArray {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("bool column")
}

fn count_label_in_batches(batches: &[RecordBatch], target_label: &str) -> usize {
    let mut total = 0usize;
    for batch in batches {
        let labels = batch_str_col(batch, 1);
        let alive = batch_bool_col(batch, 5);
        for row in 0..batch.num_rows() {
            if alive.value(row) && labels.value(row) == target_label {
                total += 1;
            }
        }
    }
    total
}

fn count_point_in_batches(batches: &[RecordBatch], target_label: &str, target_rkey: &str) -> usize {
    let mut total = 0usize;
    for batch in batches {
        let labels = batch_str_col(batch, 1);
        let pk_values = batch_str_col(batch, 3);
        let alive = batch_bool_col(batch, 5);
        for row in 0..batch.num_rows() {
            if alive.value(row)
                && labels.value(row) == target_label
                && pk_values.value(row) == target_rkey
            {
                total += 1;
            }
        }
    }
    total
}

fn build_csr(nodes: usize, labels: usize) -> MutableCsrStore {
    let mut store = MutableCsrStore::new();
    for i in 0..nodes {
        let label = format!("Label{}", i % labels);
        let rkey = format!("n{i}");
        store.add_vertex(
            &label,
            &[
                ("rkey", PropValue::Str(rkey)),
                ("score", PropValue::Int((i % 1000) as i64)),
            ],
        );
    }
    store.commit();
    store
}

async fn build_lance(path: &std::path::Path, nodes: usize, labels: usize) -> Result<YataDataset> {
    let partition_ids = UInt32Array::from(vec![0u32; nodes]);
    let labels_arr =
        StringArray::from((0..nodes).map(|i| format!("Label{}", i % labels)).collect::<Vec<_>>());
    let pk_keys = StringArray::from(vec!["rkey"; nodes]);
    let pk_values = StringArray::from((0..nodes).map(|i| format!("n{i}")).collect::<Vec<_>>());
    let vids = StringArray::from((0..nodes).map(|i| format!("v{i}")).collect::<Vec<_>>());
    let alive = BooleanArray::from(vec![true; nodes]);
    let latest_seq = UInt64Array::from((0..nodes).map(|i| i as u64).collect::<Vec<_>>());
    let repos = StringArray::from(vec![Some("bench"); nodes]);
    let rkeys = StringArray::from((0..nodes).map(|i| Some(format!("n{i}"))).collect::<Vec<_>>());
    let owner_dids = StringArray::from(vec![None::<String>; nodes]);
    let updated_at_ms = UInt64Array::from(vec![1_711_900_000_000u64; nodes]);
    let props_json = StringArray::from(
        (0..nodes)
            .map(|i| Some(format!(r#"{{"rkey":"n{i}","score":{}}}"#, i % 1000)))
            .collect::<Vec<_>>(),
    );

    let schema = Arc::new(yata_lance::vertex_live_schema().clone());
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(partition_ids),
            Arc::new(labels_arr),
            Arc::new(pk_keys),
            Arc::new(pk_values),
            Arc::new(vids),
            Arc::new(alive),
            Arc::new(latest_seq),
            Arc::new(repos),
            Arc::new(rkeys),
            Arc::new(owner_dids),
            Arc::new(updated_at_ms),
            Arc::new(props_json),
        ],
    )?;

    Ok(YataDataset::create(path.to_str().unwrap(), vec![batch], schema).await?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let nodes = env_usize("NODES", 100_000);
    let labels = env_usize("LABELS", 10);
    let iters = env_usize("ITERS", 200);
    let cold_iters = env_usize("COLD_ITERS", 20);
    let target_label = "Label0";
    let target_rkey = format!("n{}", nodes / 2);

    println!("CSR vs Lance benchmark");
    println!("  nodes  : {nodes}");
    println!("  labels : {labels}");
    println!("  iters  : {iters}");
    println!("  cold   : {cold_iters}");

    let t0 = Instant::now();
    let csr = build_csr(nodes, labels);
    let csr_build_ms = t0.elapsed().as_millis();

    let tmp = tempfile::tempdir()?;
    let t1 = Instant::now();
    let lance = build_lance(tmp.path(), nodes, labels).await?;
    let lance_build_ms = t1.elapsed().as_millis();
    let lance_uri = tmp.path().to_str().unwrap().to_string();
    let t2 = Instant::now();
    let lance_batches = lance.scan_all().await?;
    let lance_preload_ms = t2.elapsed().as_millis();

    println!("  build csr   : {} ms", csr_build_ms);
    println!("  build lance : {} ms", lance_build_ms);
    println!("  preload rb  : {} ms", lance_preload_ms);

    let mut csr_label = Lat::new();
    for _ in 0..iters {
        let t = Instant::now();
        let _ = csr.scan_vertices_by_label(target_label).len();
        csr_label.push_ns(t.elapsed().as_nanos() as u64);
    }

    let mut lance_warm_label = Lat::new();
    for _ in 0..iters {
        let t = Instant::now();
        let _ = lance
            .count_rows(Some(&format!("label = '{target_label}'")))
            .await?;
        lance_warm_label.push_ns(t.elapsed().as_nanos() as u64);
    }

    let mut csr_point = Lat::new();
    for _ in 0..iters {
        let t = Instant::now();
        let _ = csr.scan_vertices(
            target_label,
            &Predicate::Eq("rkey".to_string(), PropValue::Str(target_rkey.clone())),
        );
        csr_point.push_ns(t.elapsed().as_nanos() as u64);
    }

    let mut lance_warm_point = Lat::new();
    for _ in 0..iters {
        let t = Instant::now();
        let _ = lance
            .count_rows(Some(&format!(
                "label = '{target_label}' AND pk_value = '{target_rkey}'"
            )))
            .await?;
        lance_warm_point.push_ns(t.elapsed().as_nanos() as u64);
    }

    let mut lance_mem_label = Lat::new();
    for _ in 0..iters {
        let t = Instant::now();
        let _ = count_label_in_batches(&lance_batches, target_label);
        lance_mem_label.push_ns(t.elapsed().as_nanos() as u64);
    }

    let mut lance_mem_point = Lat::new();
    for _ in 0..iters {
        let t = Instant::now();
        let _ = count_point_in_batches(&lance_batches, target_label, &target_rkey);
        lance_mem_point.push_ns(t.elapsed().as_nanos() as u64);
    }

    let mut lance_cold_label = Lat::new();
    for _ in 0..cold_iters {
        let t = Instant::now();
        let ds = YataDataset::open(&lance_uri).await?;
        let _ = ds.count_rows(Some(&format!("label = '{target_label}'"))).await?;
        lance_cold_label.push_ns(t.elapsed().as_nanos() as u64);
    }

    let mut lance_cold_point = Lat::new();
    for _ in 0..cold_iters {
        let t = Instant::now();
        let ds = YataDataset::open(&lance_uri).await?;
        let _ = ds
            .count_rows(Some(&format!(
                "label = '{target_label}' AND pk_value = '{target_rkey}'"
            )))
            .await?;
        lance_cold_point.push_ns(t.elapsed().as_nanos() as u64);
    }

    println!();
    println!(
        "label scan  : csr resident mean {:.1}us p95 {:.1}us | lance warm mean {:.1}us p95 {:.1}us | lance batches mean {:.1}us p95 {:.1}us | lance reopen mean {:.1}us p95 {:.1}us",
        csr_label.mean_us(),
        csr_label.p95_us(),
        lance_warm_label.mean_us(),
        lance_warm_label.p95_us(),
        lance_mem_label.mean_us(),
        lance_mem_label.p95_us(),
        lance_cold_label.mean_us(),
        lance_cold_label.p95_us()
    );
    println!(
        "point lookup: csr resident mean {:.1}us p95 {:.1}us | lance warm mean {:.1}us p95 {:.1}us | lance batches mean {:.1}us p95 {:.1}us | lance reopen mean {:.1}us p95 {:.1}us",
        csr_point.mean_us(),
        csr_point.p95_us(),
        lance_warm_point.mean_us(),
        lance_warm_point.p95_us(),
        lance_mem_point.mean_us(),
        lance_mem_point.p95_us(),
        lance_cold_point.mean_us(),
        lance_cold_point.p95_us()
    );

    Ok(())
}
