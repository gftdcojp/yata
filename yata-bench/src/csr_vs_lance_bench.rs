//! Compare local CSR scans against direct Lance scans on the same synthetic dataset.
//!
//! Usage:
//!   cargo run -p yata-bench --bin csr-vs-lance-bench --release
//!
//! Environment:
//!   NODES  (default: 100000)
//!   LABELS (default: 10)
//!   ITERS  (default: 200)

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use arrow::array::{BooleanArray, StringArray, UInt32Array, UInt64Array};
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
    let target_label = "Label0";
    let target_rkey = format!("n{}", nodes / 2);

    println!("CSR vs Lance benchmark");
    println!("  nodes  : {nodes}");
    println!("  labels : {labels}");
    println!("  iters  : {iters}");

    let t0 = Instant::now();
    let csr = build_csr(nodes, labels);
    let csr_build_ms = t0.elapsed().as_millis();

    let tmp = tempfile::tempdir()?;
    let t1 = Instant::now();
    let lance = build_lance(tmp.path(), nodes, labels).await?;
    let lance_build_ms = t1.elapsed().as_millis();

    println!("  build csr   : {} ms", csr_build_ms);
    println!("  build lance : {} ms", lance_build_ms);

    let mut csr_label = Lat::new();
    for _ in 0..iters {
        let t = Instant::now();
        let _ = csr.scan_vertices_by_label(target_label).len();
        csr_label.push_ns(t.elapsed().as_nanos() as u64);
    }

    let mut lance_label = Lat::new();
    for _ in 0..iters {
        let t = Instant::now();
        let _ = lance
            .count_rows(Some(&format!("label = '{target_label}'")))
            .await?;
        lance_label.push_ns(t.elapsed().as_nanos() as u64);
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

    let mut lance_point = Lat::new();
    for _ in 0..iters {
        let t = Instant::now();
        let _ = lance
            .count_rows(Some(&format!(
                "label = '{target_label}' AND pk_value = '{target_rkey}'"
            )))
            .await?;
        lance_point.push_ns(t.elapsed().as_nanos() as u64);
    }

    println!();
    println!(
        "label scan  : csr mean {:.1}us p95 {:.1}us | lance mean {:.1}us p95 {:.1}us",
        csr_label.mean_us(),
        csr_label.p95_us(),
        lance_label.mean_us(),
        lance_label.p95_us()
    );
    println!(
        "point lookup: csr mean {:.1}us p95 {:.1}us | lance mean {:.1}us p95 {:.1}us",
        csr_point.mean_us(),
        csr_point.p95_us(),
        lance_point.mean_us(),
        lance_point.p95_us()
    );

    Ok(())
}
