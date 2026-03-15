//! AT Protocol load test.
//!
//! Scenarios:
//!   firehose  — decode_firehose_message throughput (CBOR parse, no network)
//!   car       — CARv1 decode throughput (block parsing + CID parsing)
//!   dagcbor   — dag-cbor → JSON decode throughput
//!   bridge    — full firehose commit → YATA log end-to-end (mock broker)
//!
//! Usage:
//!   cargo run -p yata-bench --bin at-bench --release -- [all|firehose|car|dagcbor|bridge]

use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;

// ── helpers (same as main.rs) ─────────────────────────────────────────────────

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    let idx = ((sorted.len() as f64) * p / 100.0).ceil() as usize;
    sorted[idx.min(sorted.len()) - 1]
}

fn print_latency(label: &str, samples: &mut Vec<u64>) {
    samples.sort_unstable();
    let n = samples.len();
    let mean = samples.iter().sum::<u64>() / n as u64;
    println!(
        "  {:<30} n={:>6}  mean={:>6}µs  p50={:>6}µs  p95={:>6}µs  p99={:>6}µs  max={:>6}µs",
        label, n, mean,
        percentile(samples, 50.0), percentile(samples, 95.0),
        percentile(samples, 99.0), samples.last().copied().unwrap_or(0),
    );
}

fn print_throughput(label: &str, count: u64, elapsed: Duration) {
    let ops_sec = count as f64 / elapsed.as_secs_f64();
    println!("  {:<30} {:.0} ops/sec  ({} ops in {:.2}s)", label, ops_sec, count, elapsed.as_secs_f64());
}

// ── test data builders ────────────────────────────────────────────────────────

/// Build a two-CBOR firehose message (#commit) with `n_ops` ops.
fn make_firehose_cbor(seq: i64, n_ops: usize) -> Vec<u8> {
    use ciborium::value::Value;

    let header = Value::Map(vec![
        (Value::Text("op".into()), Value::Integer(1.into())),
        (Value::Text("t".into()), Value::Text("#commit".into())),
    ]);

    let ops: Vec<Value> = (0..n_ops).map(|i| {
        Value::Map(vec![
            (Value::Text("action".into()), Value::Text("create".into())),
            (Value::Text("path".into()), Value::Text(format!("ai.gftd.cmd/rkey{}", i))),
            // Simulate a real dag-cbor CID (tag 42, identity-prefixed)
            (Value::Text("cid".into()), Value::Tag(42, Box::new(Value::Bytes(make_fake_cid_tag_bytes())))),
        ])
    }).collect();

    let blocks = make_car_v1(n_ops);

    let body = Value::Map(vec![
        (Value::Text("seq".into()),   Value::Integer(seq.into())),
        (Value::Text("repo".into()),  Value::Text("did:plc:benchtest0001".into())),
        (Value::Text("rev".into()),   Value::Text("rev1".into())),
        (Value::Text("ops".into()),   Value::Array(ops)),
        (Value::Text("blocks".into()), Value::Bytes(blocks)),
        (Value::Text("time".into()),  Value::Text("2026-01-01T00:00:00Z".into())),
    ]);

    let mut buf = Vec::new();
    ciborium::into_writer(&header, &mut buf).unwrap();
    ciborium::into_writer(&body, &mut buf).unwrap();
    buf
}

/// Raw CIDv1 bytes: version(1) || codec(0x71 dag-cbor) || multihash(sha2-256 code + len + zeros).
fn make_fake_cid_bytes() -> Vec<u8> {
    let mut b = Vec::with_capacity(36);
    b.push(0x01); // CIDv1
    b.push(0x71); // dag-cbor codec
    b.push(0x12); // sha2-256 multihash code
    b.push(0x20); // 32-byte digest
    b.extend_from_slice(&[0xab; 32]); // fake digest (non-zero for variety)
    b
}

/// Bytes for CBOR tag-42: [0x00 identity prefix] || [raw CID bytes].
fn make_fake_cid_tag_bytes() -> Vec<u8> {
    let mut b = vec![0x00];
    b.extend_from_slice(&make_fake_cid_bytes());
    b
}

/// Encode a dag-cbor record as raw CBOR bytes (a simple map).
fn make_dagcbor_record(idx: usize) -> Vec<u8> {
    use ciborium::value::Value;
    let val = Value::Map(vec![
        (Value::Text("$type".into()), Value::Text("ai.gftd.cmd".into())),
        (Value::Text("text".into()),  Value::Text(format!("message body number {}", idx))),
        (Value::Text("ts".into()),    Value::Text("2026-01-01T00:00:00Z".into())),
        (Value::Text("seq".into()),   Value::Integer((idx as i64).into())),
    ]);
    let mut buf = Vec::new();
    ciborium::into_writer(&val, &mut buf).unwrap();
    buf
}

/// Write an unsigned LEB128 varint to a Vec<u8>.
fn write_uvarint(buf: &mut Vec<u8>, mut n: u64) {
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 { buf.push(byte); break; }
        else { buf.push(byte | 0x80); }
    }
}

/// Build a minimal valid CARv1 with `n_blocks` dag-cbor blocks.
fn make_car_v1(n_blocks: usize) -> Vec<u8> {
    use ciborium::value::Value;

    let cid_bytes = make_fake_cid_bytes();

    // Header: { version: 1, roots: [CID as tag-42] }
    let root_tagged = Value::Tag(42, Box::new(Value::Bytes({
        let mut b = vec![0x00];
        b.extend_from_slice(&cid_bytes);
        b
    })));
    let header_val = Value::Map(vec![
        (Value::Text("version".into()), Value::Integer(1.into())),
        (Value::Text("roots".into()),   Value::Array(vec![root_tagged])),
    ]);
    let mut header_cbor = Vec::new();
    ciborium::into_writer(&header_val, &mut header_cbor).unwrap();

    let mut car = Vec::new();
    write_uvarint(&mut car, header_cbor.len() as u64);
    car.extend_from_slice(&header_cbor);

    for i in 0..n_blocks {
        let data = make_dagcbor_record(i);
        let section_len = cid_bytes.len() + data.len();
        write_uvarint(&mut car, section_len as u64);
        car.extend_from_slice(&cid_bytes);
        car.extend_from_slice(&data);
    }

    car
}

// ── scenario 1: firehose CBOR decode ─────────────────────────────────────────

fn bench_firehose_decode() -> Result<()> {
    println!("\n[AT Protocol — firehose CBOR decode]");

    let msg_1op  = make_firehose_cbor(1, 1);
    let msg_5ops = make_firehose_cbor(2, 5);

    for (label, data) in [("decode 1-op commit", &msg_1op), ("decode 5-op commit", &msg_5ops)] {
        const WARMUP: usize = 500;
        const ITERS: usize  = 10_000;

        for _ in 0..WARMUP {
            let _ = yata_at::firehose::decode_firehose_message(data);
        }

        let mut latencies = Vec::with_capacity(ITERS);
        let start = Instant::now();
        for _ in 0..ITERS {
            let t = Instant::now();
            let _ = yata_at::firehose::decode_firehose_message(data).unwrap();
            latencies.push(t.elapsed().as_nanos() as u64 / 1_000); // ns → µs (keep < 1 for ns precision)
        }
        let elapsed = start.elapsed();
        print_throughput(label, ITERS as u64, elapsed);
        // use ns for short operations
        let ns_mean = latencies.iter().sum::<u64>() * 1000 / ITERS as u64; // back to ns approx
        let _ = ns_mean; // just use µs latency print
        let mut ns_samples: Vec<u64> = {
            let start2 = Instant::now();
            let mut v = Vec::with_capacity(ITERS);
            for _ in 0..ITERS {
                let t = Instant::now();
                let _ = yata_at::firehose::decode_firehose_message(data).unwrap();
                v.push(t.elapsed().as_nanos() as u64);
            }
            let _ = start2;
            v
        };
        ns_samples.sort_unstable();
        let n = ns_samples.len();
        let mean_ns = ns_samples.iter().sum::<u64>() / n as u64;
        println!(
            "  {:<30} mean={:>5}ns  p50={:>5}ns  p95={:>5}ns  p99={:>5}ns",
            format!("  {} latency", label),
            mean_ns,
            percentile(&ns_samples, 50.0),
            percentile(&ns_samples, 95.0),
            percentile(&ns_samples, 99.0),
        );
    }
    Ok(())
}

// ── scenario 2: CARv1 decode ──────────────────────────────────────────────────

fn bench_car_decode() -> Result<()> {
    println!("\n[AT Protocol — CARv1 decode]");

    let car_1   = make_car_v1(1);
    let car_10  = make_car_v1(10);
    let car_100 = make_car_v1(100);

    for (label, data) in [
        ("decode CAR 1 block",   car_1.as_slice()),
        ("decode CAR 10 blocks", car_10.as_slice()),
        ("decode CAR 100 blocks",car_100.as_slice()),
    ] {
        const ITERS: usize = 5_000;
        let start = Instant::now();
        for _ in 0..ITERS {
            let _ = yata_at::decode_car(data).unwrap();
        }
        let elapsed = start.elapsed();
        print_throughput(label, ITERS as u64, elapsed);
    }
    Ok(())
}

// ── scenario 3: dag-cbor → JSON decode ───────────────────────────────────────

fn bench_dagcbor_decode() -> Result<()> {
    println!("\n[AT Protocol — dag-cbor → JSON decode]");

    let record = make_dagcbor_record(0);
    let large: Vec<u8> = {
        use ciborium::value::Value;
        let items: Vec<Value> = (0..50).map(|i| {
            Value::Map(vec![
                (Value::Text("k".into()), Value::Text(format!("val{}", i))),
                (Value::Text("n".into()), Value::Integer((i as i64).into())),
            ])
        }).collect();
        let val = Value::Array(items);
        let mut buf = Vec::new();
        ciborium::into_writer(&val, &mut buf).unwrap();
        buf
    };

    for (label, data) in [("dagcbor_to_json small", record.as_slice()), ("dagcbor_to_json large", large.as_slice())] {
        const ITERS: usize = 20_000;
        let mut samples = Vec::with_capacity(ITERS);
        let start = Instant::now();
        for _ in 0..ITERS {
            let t = Instant::now();
            let _ = yata_at::dagcbor_to_json(data).unwrap();
            samples.push(t.elapsed().as_nanos() as u64);
        }
        let elapsed = start.elapsed();
        print_throughput(label, ITERS as u64, elapsed);
        samples.sort_unstable();
        let n = samples.len();
        let mean = samples.iter().sum::<u64>() / n as u64;
        println!(
            "  {:<30} mean={:>5}ns  p50={:>5}ns  p95={:>5}ns  p99={:>5}ns",
            format!("  {} latency", label),
            mean,
            percentile(&samples, 50.0), percentile(&samples, 95.0), percentile(&samples, 99.0),
        );
    }
    Ok(())
}

// ── scenario 4: bridge end-to-end publish ────────────────────────────────────

async fn bench_bridge_publish() -> Result<()> {
    use yata_server::Broker;
    use yata_at::bridge::AtFirehoseBridge;
    use yata_at::firehose::decode_firehose_message;
    use yata_core::{AppendLog, Blake3Hash, Envelope, PayloadRef, PublishRequest, SchemaId, StreamId, Subject};

    println!("\n[AT Protocol — bridge publish to YATA log]");

    let dir = tempfile::tempdir()?;
    let config = yata_server::BrokerConfig {
        data_dir: dir.path().join("data"),
        lance_uri: dir.path().join("lance").to_str().unwrap().to_owned(),
        lance_flush_interval_ms: 999_999,
    };
    let broker = Arc::new(Broker::new(config).await?);

    // Simulate what AtFirehoseBridge.run() does — direct log publish
    // (we can't connect to a real relay, so we call broker.log.append directly
    //  with the same payload shape as the bridge)
    let commit_cbor = make_firehose_cbor(1, 3);
    let commit = decode_firehose_message(&commit_cbor).unwrap();
    let decoded_blocks = commit.decode_car_blocks();

    const WARMUP: usize = 100;
    const ITERS: usize  = 2_000;

    let log = broker.log.clone();

    // warm up
    for _ in 0..WARMUP {
        for op in &commit.ops {
            let cid_str = op.cid_string();
            let record = cid_str.as_deref().and_then(|c| decoded_blocks.get(c)).cloned();
            let payload = serde_json::to_vec(&serde_json::json!({
                "seq": commit.seq, "repo": commit.repo, "rev": commit.rev,
                "action": op.action, "collection": op.collection(), "rkey": op.rkey(),
                "cid": cid_str, "ts": commit.ts, "record": record,
            })).unwrap();
            let hash = Blake3Hash::of(&payload);
            let env  = Envelope::new(Subject::from("at.bench"), SchemaId::from("at.firehose.commit"), hash);
            let req  = PublishRequest {
                stream: StreamId::from("bench.at.bridge"),
                subject: Subject::from("at.bench"),
                envelope: env,
                payload: PayloadRef::InlineBytes(bytes::Bytes::from(payload)),
                expected_last_seq: None,
            };
            log.append(req).await?;
        }
    }

    // measure
    let mut latencies = Vec::with_capacity(ITERS * commit.ops.len());
    let start = Instant::now();
    for _ in 0..ITERS {
        for op in &commit.ops {
            let cid_str = op.cid_string();
            let record = cid_str.as_deref().and_then(|c| decoded_blocks.get(c)).cloned();
            let payload = serde_json::to_vec(&serde_json::json!({
                "seq": commit.seq, "repo": commit.repo, "rev": commit.rev,
                "action": op.action, "collection": op.collection(), "rkey": op.rkey(),
                "cid": cid_str, "ts": commit.ts, "record": record,
            })).unwrap();
            let hash = Blake3Hash::of(&payload);
            let env  = Envelope::new(Subject::from("at.bench"), SchemaId::from("at.firehose.commit"), hash);
            let req  = PublishRequest {
                stream: StreamId::from("bench.at.bridge"),
                subject: Subject::from("at.bench"),
                envelope: env,
                payload: PayloadRef::InlineBytes(bytes::Bytes::from(payload)),
                expected_last_seq: None,
            };
            let t = Instant::now();
            log.append(req).await?;
            latencies.push(t.elapsed().as_micros() as u64);
        }
    }
    let elapsed = start.elapsed();
    let total_ops = (ITERS * commit.ops.len()) as u64;
    print_throughput("log.append (AT commit op)", total_ops, elapsed);
    print_latency("log.append latency", &mut latencies);

    Ok(())
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .compact()
        .init();

    let args: Vec<String> = std::env::args().collect();
    let scenario = args.get(1).map(|s| s.as_str()).unwrap_or("all");

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║           AT Protocol Load Test                          ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("scenario: {}", scenario);

    let run = |name: &str| -> bool { scenario == "all" || scenario == name };

    if run("firehose") { bench_firehose_decode()?; }
    if run("car")      { bench_car_decode()?; }
    if run("dagcbor")  { bench_dagcbor_decode()?; }
    if run("bridge")   { bench_bridge_publish().await?; }

    println!("\ndone");
    Ok(())
}
