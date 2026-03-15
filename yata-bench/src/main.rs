//! YATA load test harness.
//!
//! Usage:
//!   cargo run -p yata-bench --release -- [all|log|kv|object|e2e]
//!
//! Output: plain text report with ops/sec, MB/s, latency percentiles.

use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use bytes::Bytes;
use tokio::sync::Semaphore;

// ── helpers ──────────────────────────────────────────────────────────────────

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64) * p / 100.0).ceil() as usize;
    sorted[idx.min(sorted.len()) - 1]
}

fn print_latency(label: &str, samples: &mut Vec<u64>) {
    samples.sort_unstable();
    let n = samples.len();
    let mean = samples.iter().sum::<u64>() / n as u64;
    println!(
        "  {:<28} n={:>6}  mean={:>7}µs  p50={:>7}µs  p95={:>7}µs  p99={:>7}µs  max={:>7}µs",
        label,
        n,
        mean,
        percentile(samples, 50.0),
        percentile(samples, 95.0),
        percentile(samples, 99.0),
        samples.last().copied().unwrap_or(0),
    );
}

fn print_throughput(label: &str, count: u64, elapsed: Duration) {
    let ops_sec = count as f64 / elapsed.as_secs_f64();
    println!(
        "  {:<28} {:.0} ops/sec  ({} ops in {:.2}s)",
        label,
        ops_sec,
        count,
        elapsed.as_secs_f64()
    );
}

fn print_bandwidth(label: &str, bytes: u64, elapsed: Duration) {
    let mbps = bytes as f64 / elapsed.as_secs_f64() / 1_000_000.0;
    println!(
        "  {:<28} {:.1} MB/s  ({} bytes in {:.2}s)",
        label,
        mbps,
        bytes,
        elapsed.as_secs_f64()
    );
}

// ── broker factory ───────────────────────────────────────────────────────────

async fn make_broker(dir: &tempfile::TempDir) -> Arc<yata_server::Broker> {
    let config = yata_server::BrokerConfig {
        data_dir: dir.path().join("data"),
        lance_uri: dir.path().join("lance").to_str().unwrap().to_owned(),
        lance_flush_interval_ms: 999_999,
    };
    Arc::new(yata_server::Broker::new(config).await.unwrap())
}

// ── scenario 1: AppendLog latency ─────────────────────────────────────────

async fn bench_log_latency(dir: &tempfile::TempDir) -> Result<()> {
    use yata_core::{AppendLog, Blake3Hash, Envelope, PayloadRef, PublishRequest, SchemaId, StreamId, Subject};
    use yata_log::LocalLog;

    println!("\n[AppendLog — single producer latency]");
    let log = Arc::new(LocalLog::new(dir.path().join("log_lat")).await?);

    let stream = StreamId::from("bench.latency");
    let subject = Subject::from("bench.subject");
    let schema = SchemaId::from("bench.schema");
    let payload = Bytes::from(vec![42u8; 256]);

    const WARMUP: usize = 100;
    const ITERS: usize = 2_000;

    for _ in 0..WARMUP {
        let hash = Blake3Hash::of(&payload);
        let env = Envelope::new(subject.clone(), schema.clone(), hash);
        let req = PublishRequest {
            stream: stream.clone(),
            subject: subject.clone(),
            envelope: env,
            payload: PayloadRef::InlineBytes(payload.clone()),
            expected_last_seq: None,
        };
        log.append(req).await?;
    }

    let mut latencies = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let hash = Blake3Hash::of(&payload);
        let env = Envelope::new(subject.clone(), schema.clone(), hash);
        let req = PublishRequest {
            stream: stream.clone(),
            subject: subject.clone(),
            envelope: env,
            payload: PayloadRef::InlineBytes(payload.clone()),
            expected_last_seq: None,
        };
        let t = Instant::now();
        log.append(req).await?;
        latencies.push(t.elapsed().as_micros() as u64);
    }

    print_latency("append 256B payload", &mut latencies);
    Ok(())
}

// ── scenario 2: AppendLog concurrent throughput ───────────────────────────

async fn bench_log_throughput(dir: &tempfile::TempDir) -> Result<()> {
    use yata_core::{AppendLog, Blake3Hash, Envelope, PayloadRef, PublishRequest, SchemaId, StreamId, Subject};
    use yata_log::LocalLog;

    println!("\n[AppendLog — concurrent producer throughput]");
    let log = Arc::new(LocalLog::new(dir.path().join("log_tput")).await?);

    for &concurrency in &[1usize, 4, 8, 16] {
        let log = log.clone();
        let msgs_per_producer: usize = 500;
        let total = concurrency * msgs_per_producer;

        let start = Instant::now();
        let mut handles = Vec::new();
        for p in 0..concurrency {
            let log = log.clone();
            let stream = StreamId::from(format!("bench.tput.{}", p).as_str());
            let subject = Subject::from("s");
            let schema = SchemaId::from("bench.schema");
            let payload = Bytes::from(vec![0u8; 512]);

            handles.push(tokio::spawn(async move {
                for _ in 0..msgs_per_producer {
                    let hash = Blake3Hash::of(&payload);
                    let env = Envelope::new(subject.clone(), schema.clone(), hash);
                    let req = PublishRequest {
                        stream: stream.clone(),
                        subject: subject.clone(),
                        envelope: env,
                        payload: PayloadRef::InlineBytes(payload.clone()),
                        expected_last_seq: None,
                    };
                    log.append(req).await.unwrap();
                }
            }));
        }
        for h in handles {
            h.await?;
        }
        let elapsed = start.elapsed();
        print_throughput(&format!("append {}x producers", concurrency), total as u64, elapsed);
    }
    Ok(())
}

// ── scenario 3: KV throughput ─────────────────────────────────────────────

async fn bench_kv(dir: &tempfile::TempDir) -> Result<()> {
    use yata_core::{BucketId, KvPutRequest, KvStore};
    use yata_log::LocalLog;
    use yata_kv::KvBucketStore;

    println!("\n[KV — put/get throughput]");
    let log = Arc::new(LocalLog::new(dir.path().join("kv_log")).await?);
    let kv = Arc::new(KvBucketStore::new(log).await?);
    let bucket = BucketId::from("bench.kv");

    const ITERS: usize = 2_000;
    let value = Bytes::from(vec![99u8; 128]);

    // sequential put
    let start = Instant::now();
    for i in 0..ITERS {
        kv.put(KvPutRequest {
            bucket: bucket.clone(),
            key: format!("key{}", i % 200),
            value: value.clone(),
            expected_revision: None,
            ttl_secs: None,
        })
        .await?;
    }
    print_throughput("kv put (seq, 200 keys)", ITERS as u64, start.elapsed());

    // sequential get (from snapshot)
    let start = Instant::now();
    let mut found = 0usize;
    for i in 0..ITERS {
        if kv.get(&bucket, &format!("key{}", i % 200)).await?.is_some() {
            found += 1;
        }
    }
    print_throughput(&format!("kv get (seq, {} found)", found), ITERS as u64, start.elapsed());

    // concurrent put
    for &concurrency in &[4usize, 16] {
        let kv = kv.clone();
        let iters_per = 250;
        let start = Instant::now();
        let mut handles = Vec::new();
        for t in 0..concurrency {
            let kv = kv.clone();
            let bucket = BucketId::from(format!("bench.kv.c{}", t).as_str());
            let value = value.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..iters_per {
                    kv.put(KvPutRequest {
                        bucket: bucket.clone(),
                        key: format!("key{}", i % 50),
                        value: value.clone(),
                        expected_revision: None,
                        ttl_secs: None,
                    })
                    .await
                    .unwrap();
                }
            }));
        }
        for h in handles {
            h.await?;
        }
        let total = (concurrency * iters_per) as u64;
        print_throughput(&format!("kv put {}x concurrent", concurrency), total, start.elapsed());
    }
    Ok(())
}

// ── scenario 4: Object store throughput ──────────────────────────────────

async fn bench_object(dir: &tempfile::TempDir) -> Result<()> {
    use yata_core::{ObjectMeta, ObjectStorage};
    use yata_object::LocalObjectStore;

    println!("\n[ObjectStore — put/get throughput]");
    let store = Arc::new(LocalObjectStore::new(dir.path().join("objects")).await?);
    let meta = ObjectMeta {
        media_type: "application/octet-stream".into(),
        schema_id: None,
        lineage: vec![],
    };

    for &size in &[1_024usize, 64_000, 1_000_000, 10_000_000] {
        let data = Bytes::from(vec![0u8; size]);
        let iters = if size < 100_000 {
            100usize
        } else if size < 2_000_000 {
            20
        } else {
            5
        };

        // put
        let start = Instant::now();
        let mut last_id = None;
        for _ in 0..iters {
            let m = store.put_object(data.clone(), meta.clone()).await?;
            last_id = Some(m.object_id);
        }
        let elapsed = start.elapsed();
        let total_bytes = (size * iters) as u64;
        print_bandwidth(&format!("put {}B x {}", size, iters), total_bytes, elapsed);

        // get
        if let Some(id) = last_id {
            let start = Instant::now();
            for _ in 0..iters {
                let _ = store.get_object(&id).await?;
            }
            let elapsed = start.elapsed();
            print_bandwidth(&format!("get {}B x {}", size, iters), total_bytes, elapsed);
        }
    }
    Ok(())
}

// ── scenario 5: End-to-end BrokerBackend ─────────────────────────────────

async fn bench_e2e(dir: &tempfile::TempDir) -> Result<()> {
    use arrow_array::{Int64Array, StringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use yata_arrow::ArrowBatchHandle;
    use yata_server::BrokerBackend;

    println!("\n[BrokerBackend (end-to-end) — publish_arrow + kv_put]");
    let broker = make_broker(dir).await;
    let client = BrokerBackend::new(broker).into_client();

    // build a small Arrow batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![1i64, 2, 3])),
        ],
    )?;
    let batch_handle = ArrowBatchHandle::from_batch(batch)?;

    // publish_arrow throughput
    const ITERS: usize = 500;
    let mut latencies = Vec::with_capacity(ITERS);
    let start = Instant::now();
    for _ in 0..ITERS {
        let t = Instant::now();
        client
            .publish_arrow("bench.e2e", "bench.subject", batch_handle.clone(), Default::default())
            .await?;
        latencies.push(t.elapsed().as_micros() as u64);
    }
    let elapsed = start.elapsed();
    print_throughput("publish_arrow (seq)", ITERS as u64, elapsed);
    print_latency("publish_arrow latency", &mut latencies);

    // kv_put throughput
    let mut kv_latencies = Vec::with_capacity(ITERS);
    let start = Instant::now();
    for i in 0..ITERS {
        let t = Instant::now();
        client
            .kv_put("bench", &format!("key{}", i % 100), Bytes::from_static(b"v"), None)
            .await?;
        kv_latencies.push(t.elapsed().as_micros() as u64);
    }
    let elapsed = start.elapsed();
    print_throughput("kv_put (seq)", ITERS as u64, elapsed);
    print_latency("kv_put latency", &mut kv_latencies);

    // concurrent publish
    for &concurrency in &[4usize, 16] {
        let client = client.clone();
        let iters_per = 100;
        let sem = Arc::new(Semaphore::new(concurrency));
        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..(concurrency * iters_per) {
            let client = client.clone();
            let bh = batch_handle.clone();
            let permit = sem.clone().acquire_owned().await?;
            handles.push(tokio::spawn(async move {
                let _p = permit;
                client
                    .publish_arrow("bench.conc", "s", bh, Default::default())
                    .await
                    .unwrap();
            }));
        }
        for h in handles {
            h.await?;
        }
        let total = (concurrency * iters_per) as u64;
        print_throughput(
            &format!("publish_arrow {}x concurrent", concurrency),
            total,
            start.elapsed(),
        );
    }
    Ok(())
}

// ── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .compact()
        .init();

    let args: Vec<String> = std::env::args().collect();
    let scenario = args.get(1).map(|s| s.as_str()).unwrap_or("all");

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║              YATA Load Test                              ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("scenario: {}", scenario);

    let run = |name: &str| -> bool { scenario == "all" || scenario == name };

    if run("log") {
        let d = tempfile::tempdir()?;
        bench_log_latency(&d).await?;
        let d = tempfile::tempdir()?;
        bench_log_throughput(&d).await?;
    }

    if run("kv") {
        let d = tempfile::tempdir()?;
        bench_kv(&d).await?;
    }

    if run("object") {
        let d = tempfile::tempdir()?;
        bench_object(&d).await?;
    }

    if run("e2e") {
        let d = tempfile::tempdir()?;
        bench_e2e(&d).await?;
    }

    println!("\ndone");
    Ok(())
}
