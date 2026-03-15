//! NATS Arrow batch produce/consume benchmark.
//!
//! Evaluates:
//! 1. Arrow IPC encode throughput (various batch sizes)
//! 2. Arrow IPC decode throughput
//! 3. Lance write throughput (single-row vs accumulated batches)
//! 4. KV entry batching efficiency (1-row vs 64-row vs 1024-row)
//! 5. End-to-end: encode → Lance write at various scales
//!
//! Does NOT require a running NATS server — benchmarks the Arrow IPC
//! codec and Lance write path directly.
//!
//! Usage: cargo run -p yata-bench --release --bin nats-arrow-bench

use arrow::array::{Array, Int64Array, StringArray, UInt64Array, LargeBinaryArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;
use yata_core::{BucketId, KvEntry, KvOp, Revision};

fn main() {
    println!("=== NATS Arrow Batch Benchmark ===\n");

    bench_ipc_encode();
    bench_ipc_decode();
    bench_batch_sizes();
    bench_kv_batching();
    bench_lance_write();
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

fn make_kv_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("bucket", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("revision", DataType::UInt64, false),
        Field::new("value_bytes", DataType::LargeBinary, true),
        Field::new("ts_ns", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]))
}

fn make_kv_batch(n: usize) -> RecordBatch {
    let schema = make_kv_schema();
    let buckets: Vec<&str> = (0..n).map(|_| "test").collect();
    let keys: Vec<String> = (0..n).map(|i| format!("key-{i}")).collect();
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    let revisions: Vec<u64> = (0..n).map(|i| i as u64 + 1).collect();
    let values: Vec<Option<&[u8]>> = (0..n).map(|_| Some(b"hello world" as &[u8])).collect();
    let ts_ns: Vec<i64> = (0..n).map(|_| 1710000000_000_000_000i64).collect();
    let ops: Vec<&str> = (0..n).map(|_| "put").collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(buckets)) as Arc<dyn Array>,
            Arc::new(StringArray::from(key_refs)),
            Arc::new(UInt64Array::from(revisions)),
            Arc::new(LargeBinaryArray::from_opt_vec(values)),
            Arc::new(Int64Array::from(ts_ns)),
            Arc::new(StringArray::from(ops)),
        ],
    )
    .unwrap()
}

fn make_kv_entries(n: usize) -> Vec<KvEntry> {
    (0..n)
        .map(|i| KvEntry {
            bucket: BucketId::from("test"),
            key: format!("key-{i}"),
            revision: Revision(i as u64 + 1),
            value: b"hello world".to_vec(),
            ts_ns: 1710000000_000_000_000i64,
            op: KvOp::Put,
        })
        .collect()
}

// ---- benchmarks ------------------------------------------------------------

fn bench_ipc_encode() {
    println!("--- Arrow IPC Encode Throughput ---");
    for &rows in &[1, 10, 100, 1000, 10_000] {
        let batch = make_kv_batch(rows);
        let iters = if rows <= 100 { 10_000 } else { 1_000 };

        // Warmup
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
        let batch = make_kv_batch(rows);
        let ipc = yata_arrow::batch_to_ipc(&batch).unwrap();
        let iters = if rows <= 100 { 10_000 } else { 1_000 };

        // Warmup
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
        let batch = make_kv_batch(rows);
        let ipc = yata_arrow::batch_to_ipc(&batch).unwrap();
        let raw_data_est = rows * (4 + 5 + 8 + 11 + 8 + 3); // bucket + key + rev + value + ts + op
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

fn bench_kv_batching() {
    println!("--- KV Entry → Arrow Batch Conversion ---");
    for &count in &[1, 10, 64, 256, 1024, 4096] {
        let entries = make_kv_entries(count);
        let iters = if count <= 64 { 10_000 } else { 1_000 };

        // Warmup
        for _ in 0..100 {
            let _ = yata_nats::kv::kv_entries_to_batch(&entries).unwrap();
        }

        let mut samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let start = Instant::now();
            let batch = yata_nats::kv::kv_entries_to_batch(&entries).unwrap();
            let elapsed = start.elapsed().as_micros() as u64;
            samples.push(elapsed);
            std::hint::black_box(batch);
        }
        print_latency(&format!("kv_entries_to_batch({count})"), &mut samples);
    }
    println!();
}

fn bench_lance_write() {
    println!("--- Lance Write: Single-Row vs Accumulated ---");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();

        // Test 1: 1000 single-row writes (simulates old 1-msg-1-write pattern)
        {
            let lance_uri = tmp.path().join("single_row").to_string_lossy().to_string();
            let conn = lancedb::connect(&lance_uri).execute().await.unwrap();
            let total_rows = 1000;

            let start = Instant::now();
            for i in 0..total_rows {
                let batch = make_kv_batch(1);
                let schema = batch.schema();
                let reader = arrow::record_batch::RecordBatchIterator::new(
                    std::iter::once(Ok::<_, arrow::error::ArrowError>(batch)),
                    schema,
                );
                if i == 0 {
                    conn.create_table("kv", reader).execute().await.unwrap();
                } else {
                    conn.open_table("kv").execute().await.unwrap()
                        .add(reader).execute().await.unwrap();
                }
            }
            let elapsed = start.elapsed();
            println!(
                "  {:<40} {total_rows} writes in {:.1}ms ({:.0} ops/sec, {} fragments)",
                "single-row (1 msg = 1 write)",
                elapsed.as_secs_f64() * 1000.0,
                total_rows as f64 / elapsed.as_secs_f64(),
                total_rows,
            );
        }

        // Test 2: same 1000 rows accumulated into batches of 64
        {
            let lance_uri = tmp.path().join("accumulated").to_string_lossy().to_string();
            let conn = lancedb::connect(&lance_uri).execute().await.unwrap();
            let total_rows = 1000;
            let batch_size = 64;
            let num_writes = (total_rows + batch_size - 1) / batch_size;

            let start = Instant::now();
            for i in 0..num_writes {
                let rows = std::cmp::min(batch_size, total_rows - i * batch_size);
                let batch = make_kv_batch(rows);
                let schema = batch.schema();
                let reader = arrow::record_batch::RecordBatchIterator::new(
                    std::iter::once(Ok::<_, arrow::error::ArrowError>(batch)),
                    schema,
                );
                if i == 0 {
                    conn.create_table("kv", reader).execute().await.unwrap();
                } else {
                    conn.open_table("kv").execute().await.unwrap()
                        .add(reader).execute().await.unwrap();
                }
            }
            let elapsed = start.elapsed();
            println!(
                "  {:<40} {total_rows} rows in {:.1}ms ({:.0} rows/sec, {} fragments)",
                format!("accumulated (batch_size={})", batch_size),
                elapsed.as_secs_f64() * 1000.0,
                total_rows as f64 / elapsed.as_secs_f64(),
                num_writes,
            );
        }

        // Test 3: single bulk write of 1000 rows
        {
            let lance_uri = tmp.path().join("bulk").to_string_lossy().to_string();
            let conn = lancedb::connect(&lance_uri).execute().await.unwrap();
            let total_rows = 1000;

            let start = Instant::now();
            let batch = make_kv_batch(total_rows);
            let schema = batch.schema();
            let reader = arrow::record_batch::RecordBatchIterator::new(
                std::iter::once(Ok::<_, arrow::error::ArrowError>(batch)),
                schema,
            );
            conn.create_table("kv", reader).execute().await.unwrap();
            let elapsed = start.elapsed();
            println!(
                "  {:<40} {total_rows} rows in {:.1}ms ({:.0} rows/sec, 1 fragment)",
                "bulk (single write)",
                elapsed.as_secs_f64() * 1000.0,
                total_rows as f64 / elapsed.as_secs_f64(),
            );
        }

        // Test 4: 10K rows accumulated vs single-row
        {
            let lance_uri = tmp.path().join("acc_10k").to_string_lossy().to_string();
            let conn = lancedb::connect(&lance_uri).execute().await.unwrap();
            let total_rows = 10_000;
            let batch_size = 4096;
            let num_writes = (total_rows + batch_size - 1) / batch_size;

            let start = Instant::now();
            for i in 0..num_writes {
                let rows = std::cmp::min(batch_size, total_rows - i * batch_size);
                let batch = make_kv_batch(rows);
                let schema = batch.schema();
                let reader = arrow::record_batch::RecordBatchIterator::new(
                    std::iter::once(Ok::<_, arrow::error::ArrowError>(batch)),
                    schema,
                );
                if i == 0 {
                    conn.create_table("kv", reader).execute().await.unwrap();
                } else {
                    conn.open_table("kv").execute().await.unwrap()
                        .add(reader).execute().await.unwrap();
                }
            }
            let elapsed = start.elapsed();
            println!(
                "  {:<40} {total_rows} rows in {:.1}ms ({:.0} rows/sec, {} fragments)",
                format!("accumulated 10K (batch_size={})", batch_size),
                elapsed.as_secs_f64() * 1000.0,
                total_rows as f64 / elapsed.as_secs_f64(),
                num_writes,
            );
        }
    });

    println!();
    println!("=== Done ===");
}
