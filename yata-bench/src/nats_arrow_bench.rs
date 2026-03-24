//! Arrow batch benchmark.
//!
//! Evaluates:
//! 1. Arrow IPC encode throughput (various batch sizes)
//! 2. Arrow IPC decode throughput
//! 3. Batch size overhead analysis
//!
//! Usage: cargo run -p yata-bench --release --bin nats-arrow-bench

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;

fn main() {
    println!("=== Arrow Batch Benchmark ===\n");

    bench_ipc_encode();
    bench_ipc_decode();
    bench_batch_sizes();
}

// ---- helpers ---------------------------------------------------------------

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64) * p / 100.0).ceil() as usize;
    sorted[idx.saturating_sub(1).min(sorted.len() - 1)]
}

fn print_latency(label: &str, samples: &mut Vec<u64>) {
    samples.sort();
    let mean = samples.iter().sum::<u64>() / samples.len().max(1) as u64;
    println!(
        "  {:<40} n={:<6} mean={:<8}µs p50={:<8}µs p95={:<8}µs p99={:<8}µs max={}µs",
        label,
        samples.len(),
        mean,
        percentile(samples, 50.0),
        percentile(samples, 95.0),
        percentile(samples, 99.0),
        percentile(samples, 100.0),
    );
}

fn make_bench_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("ts_ns", DataType::Int64, false),
    ]))
}

fn make_bench_batch(n: usize) -> RecordBatch {
    let schema = make_bench_schema();
    let ids: Vec<String> = (0..n).map(|i| format!("id-{i}")).collect();
    let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
    let labels: Vec<&str> = (0..n).map(|_| "bench").collect();
    let values: Vec<&str> = (0..n).map(|_| "hello world").collect();
    let ts_ns: Vec<i64> = (0..n).map(|_| 1710000000_000_000_000i64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(id_refs)) as Arc<dyn Array>,
            Arc::new(StringArray::from(labels)),
            Arc::new(StringArray::from(values)),
            Arc::new(Int64Array::from(ts_ns)),
        ],
    )
    .unwrap()
}

// ---- benchmarks ------------------------------------------------------------

fn bench_ipc_encode() {
    println!("--- Arrow IPC Encode Throughput ---");
    for &rows in &[1, 10, 100, 1000, 10_000] {
        let batch = make_bench_batch(rows);
        let iters = if rows <= 100 { 10_000 } else { 1_000 };

        for _ in 0..100 {
            let _ = yata_arrow::batch_to_ipc(&batch).unwrap();
        }

        let mut samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let start = Instant::now();
            let ipc = yata_arrow::batch_to_ipc(&batch).unwrap();
            let elapsed = start.elapsed().as_micros() as u64;
            samples.push(elapsed);
            std::hint::black_box(ipc);
        }
        let total_bytes: usize = yata_arrow::batch_to_ipc(&batch).unwrap().len();
        print_latency(&format!("encode {rows} rows ({total_bytes}B)"), &mut samples);
    }
    println!();
}

fn bench_ipc_decode() {
    println!("--- Arrow IPC Decode Throughput ---");
    for &rows in &[1, 10, 100, 1000, 10_000] {
        let batch = make_bench_batch(rows);
        let ipc = yata_arrow::batch_to_ipc(&batch).unwrap();
        let iters = if rows <= 100 { 10_000 } else { 1_000 };

        for _ in 0..100 {
            let _ = yata_arrow::ipc_to_batch(&ipc).unwrap();
        }

        let mut samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let start = Instant::now();
            let b = yata_arrow::ipc_to_batch(&ipc).unwrap();
            let elapsed = start.elapsed().as_micros() as u64;
            samples.push(elapsed);
            std::hint::black_box(b);
        }
        print_latency(&format!("decode {rows} rows ({}B)", ipc.len()), &mut samples);
    }
    println!();
}

fn bench_batch_sizes() {
    println!("--- Batch Size: IPC Overhead Analysis ---");
    println!("  {:<20} {:<12} {:<12} {:<12} {:<12}", "rows", "ipc_bytes", "per_row", "schema_overhead", "efficiency");
    for &rows in &[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024] {
        let batch = make_bench_batch(rows);
        let ipc = yata_arrow::batch_to_ipc(&batch).unwrap();
        let raw_data_est = rows * (4 + 5 + 11 + 8); // id + label + value + ts
        let per_row = ipc.len() / rows;
        let overhead = if ipc.len() > raw_data_est {
            ipc.len() - raw_data_est
        } else {
            0
        };
        let efficiency = (raw_data_est as f64 / ipc.len() as f64 * 100.0) as u32;
        println!(
            "  {:<20} {:<12} {:<12} {:<12} {:<12}",
            rows,
            ipc.len(),
            per_row,
            overhead,
            format!("{efficiency}%"),
        );
    }
    println!();
}

    println!("=== Done ===");
}
