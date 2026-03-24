//! yata-cypher transport benchmark: UCX tier simulation
//!
//! UCX transport hierarchy (approximated without RDMA hardware):
//!
//!   Tier 0: In-process          (baseline, zero overhead)
//!   Tier 1: UCX shmem           ≈ POSIX shm + atomic polling (knem/CMA style)
//!   Tier 2: UCX UDS             ≈ Unix Domain Socket (UCX sm/cma fallback)
//!   Tier 3: UCX TCP             ≈ TCP loopback (UCX tcp_iface)
//!
//! Wire protocol for Tier 1–3:
//!   Request:  [u32 LE: query_len][query_len bytes: UTF-8 Cypher]
//!   Response: [u32 LE: json_len][json_len bytes: UTF-8 JSON result]
//!
//! Usage:
//!   cargo run -p yata-bench --bin cypher-transport-bench --release
//!
//!   NODES=1000 EDGES=3000

use anyhow::Result;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use yata_cypher::{Executor, Graph, MemoryGraph, NodeRef, RelRef, Value, parse};

// ── dataset ───────────────────────────────────────────────────────────────────

struct Dataset {
    nodes: Vec<(String, String, String, i64)>, // id, label, name, age
    edges: Vec<(String, String, String, String)>, // id, src, dst, rel_type
}

fn generate(n_nodes: usize, n_edges: usize, seed: u64) -> Dataset {
    let mut rng = StdRng::seed_from_u64(seed);
    let labels = ["Person", "Company", "Product"];
    let rel_types = ["KNOWS", "WORKS_AT", "USES", "MANAGES"];
    let nodes = (0..n_nodes)
        .map(|i| {
            let lbl = labels[i % labels.len()];
            (
                format!("node_{i}"),
                lbl.to_owned(),
                format!("{lbl}_{i}"),
                rng.gen_range(18..80i64),
            )
        })
        .collect();
    let edges = (0..n_edges)
        .map(|i| {
            let src = rng.gen_range(0..n_nodes);
            let mut dst = rng.gen_range(0..n_nodes);
            while dst == src {
                dst = rng.gen_range(0..n_nodes);
            }
            (
                format!("edge_{i}"),
                format!("node_{src}"),
                format!("node_{dst}"),
                rel_types[i % rel_types.len()].to_owned(),
            )
        })
        .collect();
    Dataset { nodes, edges }
}

fn build_graph(ds: &Dataset) -> MemoryGraph {
    let mut g = MemoryGraph::new();
    for (id, label, name, age) in &ds.nodes {
        let props = [
            ("name".to_owned(), Value::Str(name.clone())),
            ("age".to_owned(), Value::Int(*age)),
        ]
        .into_iter()
        .collect();
        g.add_node(NodeRef {
            id: id.clone(),
            labels: vec![label.clone()],
            props,
        });
    }
    for (id, src, dst, rel_type) in &ds.edges {
        g.add_rel(RelRef {
            id: id.clone(),
            rel_type: rel_type.clone(),
            src: src.clone(),
            dst: dst.clone(),
            props: Default::default(),
        });
    }
    g
}

// ── statistics ────────────────────────────────────────────────────────────────

struct Latencies(Vec<u64>);

impl Latencies {
    fn new() -> Self {
        Self(Vec::new())
    }
    fn push(&mut self, us: u64) {
        self.0.push(us);
    }

    fn report(&mut self, tier: &str, scenario: &str) -> f64 {
        self.0.sort_unstable();
        let n = self.0.len();
        if n == 0 {
            println!("  [{tier:<18}] {scenario:<38} no data");
            return 0.0;
        }
        let mean = self.0.iter().sum::<u64>() as f64 / n as f64;
        let p50 = self.0[n * 50 / 100];
        let p99 = self.0[n * 99 / 100];
        let max = *self.0.last().unwrap();
        let ops = 1_000_000.0 / mean;
        println!(
            "  [{tier:<18}] {scenario:<38} mean={mean:>7.1}µs  p50={p50:>6}µs  p99={p99:>7}µs  max={max:>7}µs  {ops:>8.0} ops/s",
        );
        mean
    }
}

// ── Tier 0: in-process ────────────────────────────────────────────────────────

fn bench_inprocess(graph: &mut MemoryGraph, queries: &[&str], iters: usize) -> Vec<(String, f64)> {
    let exec = Executor::new();
    let mut results = Vec::new();
    for &q in queries {
        // warmup
        for _ in 0..20 {
            let parsed = parse(q).unwrap();
            let _ = exec.execute(&parsed, graph);
        }
        let parsed = parse(q).unwrap();
        let mut lat = Latencies::new();
        for _ in 0..iters {
            let t = Instant::now();
            let _ = exec.execute(&parsed, graph);
            lat.push(t.elapsed().as_micros() as u64);
        }
        let mean = lat.report("in-process", q);
        results.push((q.to_owned(), mean));
    }
    results
}

// ── wire protocol helpers ─────────────────────────────────────────────────────

async fn send_query<W: AsyncWriteExt + Unpin>(w: &mut W, cypher: &str) -> Result<()> {
    let b = cypher.as_bytes();
    w.write_u32_le(b.len() as u32).await?;
    w.write_all(b).await?;
    w.flush().await?;
    Ok(())
}

async fn recv_response<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<String> {
    let len = r.read_u32_le().await? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(String::from_utf8(buf)?)
}

async fn read_query<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<String> {
    let len = r.read_u32_le().await? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(String::from_utf8(buf)?)
}

async fn send_response<W: AsyncWriteExt + Unpin>(w: &mut W, json: &str) -> Result<()> {
    let b = json.as_bytes();
    w.write_u32_le(b.len() as u32).await?;
    w.write_all(b).await?;
    w.flush().await?;
    Ok(())
}

fn exec_query(graph: &mut MemoryGraph, cypher: &str) -> String {
    match parse(cypher) {
        Err(e) => format!(r#"{{"error":"{}"}}"#, e),
        Ok(q) => match Executor::new().execute(&q, graph) {
            Err(e) => format!(r#"{{"error":"{}"}}"#, e),
            Ok(rs) => format!(r#"{{"rows":{}}}"#, rs.rows.len()),
        },
    }
}

// ── Tier 2: UDS server/client ─────────────────────────────────────────────────

async fn start_uds_server(ds: &Dataset, path: &str) -> tokio::task::JoinHandle<()> {
    // Build graph snapshot for server
    let mut graph = build_graph(ds);
    let path = path.to_owned();
    let _ = std::fs::remove_file(&path);
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            loop {
                match read_query(&mut stream).await {
                    Ok(q) if q == "__quit__" => break,
                    Ok(q) => {
                        let r = exec_query(&mut graph, &q);
                        if send_response(&mut stream, &r).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    })
}

async fn bench_uds(ds: &Dataset, uds_path: &str, queries: &[&str], iters: usize) -> Vec<f64> {
    let _srv = start_uds_server(ds, uds_path).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut results = Vec::new();
    for &q in queries {
        let mut stream = tokio::net::UnixStream::connect(uds_path).await.unwrap();
        // warmup
        for _ in 0..20 {
            send_query(&mut stream, q).await.unwrap();
            let _ = recv_response(&mut stream).await.unwrap();
        }
        let mut lat = Latencies::new();
        for _ in 0..iters {
            let t = Instant::now();
            send_query(&mut stream, q).await.unwrap();
            let _ = recv_response(&mut stream).await.unwrap();
            lat.push(t.elapsed().as_micros() as u64);
        }
        let mean = lat.report("UCX-UDS(≈sm/cma)", q);
        results.push(mean);
        send_query(&mut stream, "__quit__").await.ok();
    }
    results
}

// ── Tier 3: TCP loopback ──────────────────────────────────────────────────────

async fn start_tcp_server(ds: &Dataset, port: u16) -> tokio::task::JoinHandle<()> {
    let mut graph = build_graph(ds);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            // Disable Nagle for minimum latency
            stream.set_nodelay(true).unwrap();
            loop {
                match read_query(&mut stream).await {
                    Ok(q) if q == "__quit__" => break,
                    Ok(q) => {
                        let r = exec_query(&mut graph, &q);
                        if send_response(&mut stream, &r).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    })
}

async fn bench_tcp(ds: &Dataset, port: u16, queries: &[&str], iters: usize) -> Vec<f64> {
    let _srv = start_tcp_server(ds, port).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut results = Vec::new();
    for &q in queries {
        let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        stream.set_nodelay(true).unwrap();
        // warmup
        for _ in 0..20 {
            send_query(&mut stream, q).await.unwrap();
            let _ = recv_response(&mut stream).await.unwrap();
        }
        let mut lat = Latencies::new();
        for _ in 0..iters {
            let t = Instant::now();
            send_query(&mut stream, q).await.unwrap();
            let _ = recv_response(&mut stream).await.unwrap();
            lat.push(t.elapsed().as_micros() as u64);
        }
        let mean = lat.report("UCX-TCP(≈tcp_iface)", q);
        results.push(mean);
        send_query(&mut stream, "__quit__").await.ok();
    }
    results
}

// ── Tier 1: Shared memory (POSIX mmap + atomic spin) ─────────────────────────
//
// Layout of the shared region (64KB ring):
//   [0..4]   req_state: u32  (0=idle, 1=query_ready, 2=result_ready)
//   [4..8]   req_len:   u32
//   [8..4104] req_buf:  4096 bytes
//   [4104..4108] res_len: u32
//   [4108..8204] res_buf: 4096 bytes
//
// This simulates UCX Active Message with shmem transport.

const SHM_SIZE: usize = 16 * 1024;
const OFF_STATE: usize = 0; // AtomicU32: 0=idle, 1=req, 2=res
const OFF_REQ_LEN: usize = 4;
const OFF_REQ_BUF: usize = 8;
const OFF_RES_LEN: usize = 4108;
const OFF_RES_BUF: usize = 4112;
const MAX_PAYLOAD: usize = 4096;

fn shm_state(ptr: *mut u8) -> &'static AtomicU32 {
    unsafe { &*(ptr.add(OFF_STATE) as *const AtomicU32) }
}
fn shm_req_len(ptr: *mut u8) -> &'static AtomicU32 {
    unsafe { &*(ptr.add(OFF_REQ_LEN) as *const AtomicU32) }
}
fn shm_res_len(ptr: *mut u8) -> &'static AtomicU32 {
    unsafe { &*(ptr.add(OFF_RES_LEN) as *const AtomicU32) }
}

struct ShmRegion {
    ptr: *mut u8,
    _mmap: memmap2::MmapMut,
}
unsafe impl Send for ShmRegion {}
unsafe impl Sync for ShmRegion {}

impl ShmRegion {
    fn new() -> Self {
        let mut mmap = memmap2::MmapMut::map_anon(SHM_SIZE).unwrap();
        let ptr = mmap.as_mut_ptr();
        Self { ptr, _mmap: mmap }
    }
}

fn start_shm_server(ds: &Dataset, shm: Arc<ShmRegion>) {
    let mut graph = build_graph(ds);
    std::thread::spawn(move || {
        let ptr = shm.ptr;
        loop {
            // spin-wait for state=1 (request ready)
            loop {
                let s = shm_state(ptr).load(Ordering::Acquire);
                if s == 1 {
                    break;
                }
                if s == 99 {
                    return;
                } // shutdown
                std::hint::spin_loop();
            }
            // read query
            let qlen = shm_req_len(ptr).load(Ordering::Acquire) as usize;
            let query = unsafe {
                let slice = std::slice::from_raw_parts(ptr.add(OFF_REQ_BUF), qlen.min(MAX_PAYLOAD));
                std::str::from_utf8(slice).unwrap_or("").to_owned()
            };
            if query == "__quit__" {
                shm_state(ptr).store(99, Ordering::Release);
                return;
            }
            // execute
            let result = exec_query(&mut graph, &query);
            let rbytes = result.as_bytes();
            let rlen = rbytes.len().min(MAX_PAYLOAD);
            unsafe {
                std::ptr::copy_nonoverlapping(rbytes.as_ptr(), ptr.add(OFF_RES_BUF), rlen);
            }
            shm_res_len(ptr).store(rlen as u32, Ordering::Release);
            // set state=2 (result ready)
            shm_state(ptr).store(2, Ordering::Release);
        }
    });
}

async fn bench_shm(ds: &Dataset, queries: &[&str], iters: usize) -> Vec<f64> {
    let shm = Arc::new(ShmRegion::new());
    start_shm_server(ds, Arc::clone(&shm));
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let mut results = Vec::new();
    for &q in queries {
        let ptr = shm.ptr;
        let qb = q.as_bytes();
        let qlen = qb.len().min(MAX_PAYLOAD);

        // warmup
        for _ in 0..20 {
            unsafe {
                std::ptr::copy_nonoverlapping(qb.as_ptr(), ptr.add(OFF_REQ_BUF), qlen);
            }
            shm_req_len(ptr).store(qlen as u32, Ordering::Release);
            shm_state(ptr).store(1, Ordering::Release);
            loop {
                if shm_state(ptr).load(Ordering::Acquire) == 2 {
                    break;
                }
                std::hint::spin_loop();
            }
            shm_state(ptr).store(0, Ordering::Release);
        }

        let mut lat = Latencies::new();
        for _ in 0..iters {
            let t = Instant::now();
            unsafe {
                std::ptr::copy_nonoverlapping(qb.as_ptr(), ptr.add(OFF_REQ_BUF), qlen);
            }
            shm_req_len(ptr).store(qlen as u32, Ordering::Release);
            shm_state(ptr).store(1, Ordering::Release);
            // spin-wait for result (mimics UCX AM shmem progress engine)
            loop {
                if shm_state(ptr).load(Ordering::Acquire) == 2 {
                    break;
                }
                std::hint::spin_loop();
            }
            lat.push(t.elapsed().as_micros() as u64);
            shm_state(ptr).store(0, Ordering::Release);
        }
        let mean = lat.report("UCX-shmem(≈knem)", q);
        results.push(mean);
    }

    // shutdown
    let ptr = shm.ptr;
    let qb = b"__quit__";
    unsafe {
        std::ptr::copy_nonoverlapping(qb.as_ptr(), ptr.add(OFF_REQ_BUF), qb.len());
    }
    shm_req_len(ptr).store(qb.len() as u32, Ordering::Release);
    shm_state(ptr).store(1, Ordering::Release);
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    results
}

// ── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let n_nodes: usize = std::env::var("NODES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let n_edges: usize = std::env::var("EDGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║   yata-cypher — UCX transport tier simulation                              ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!("  Dataset : {} nodes  {} edges", n_nodes, n_edges);
    println!("  UCX tiers: in-process | shmem(≈knem/CMA) | UDS(≈sm fallback) | TCP(≈tcp_iface)");
    println!();

    let ds = generate(n_nodes, n_edges, 42);
    let mut graph = build_graph(&ds);

    // Benchmark queries (same across all tiers)
    let queries: &[&str] = &[
        "MATCH (n:Person) RETURN count(n) AS cnt",
        "MATCH (n:Person) WHERE n.age > 50 RETURN count(n) AS cnt",
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS cnt",
        "MATCH (n:Person) WHERE n.age > 30 RETURN n.name ORDER BY n.name ASC LIMIT 10",
    ];

    let iters = 300;

    // ── Tier 0: in-process
    println!("─── Tier 0: in-process (zero transport overhead) ─────────────────────────────");
    let t0 = bench_inprocess(&mut graph, queries, iters);
    println!();

    // ── Tier 1: UCX shmem simulation
    println!("─── Tier 1: UCX-shmem ≈ mmap+atomic-spin (knem/CMA tier) ────────────────────");
    let t1 = bench_shm(&ds, queries, iters).await;
    println!();

    // ── Tier 2: UDS
    println!("─── Tier 2: UCX-UDS ≈ Unix Domain Socket (sm/cma fallback) ──────────────────");
    let uds_path = "/tmp/yata-cypher-bench.sock";
    let t2 = bench_uds(&ds, uds_path, queries, iters).await;
    let _ = std::fs::remove_file(uds_path);
    println!();

    // ── Tier 3: TCP loopback
    println!("─── Tier 3: UCX-TCP ≈ TCP loopback w/ TCP_NODELAY (tcp_iface) ───────────────");
    let t3 = bench_tcp(&ds, 19876, queries, iters).await;
    println!();

    // ── Summary table ─────────────────────────────────────────────────────────
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!("  SUMMARY — mean latency (µs)  and  overhead vs in-process");
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!(
        "  {:<40}  {:>10}  {:>12}  {:>10}  {:>10}",
        "query", "inprocess", "shmem(UCX1)", "UDS(UCX2)", "TCP(UCX3)"
    );
    println!("  {}", "─".repeat(90));

    for (i, &q) in queries.iter().enumerate() {
        let ip = t0.get(i).map(|(_, v)| *v).unwrap_or(0.0);
        let sh = t1.get(i).copied().unwrap_or(0.0);
        let ud = t2.get(i).copied().unwrap_or(0.0);
        let tc = t3.get(i).copied().unwrap_or(0.0);

        let sh_ov = sh - ip;
        let ud_ov = ud - ip;
        let tc_ov = tc - ip;

        // short label
        let label = if q.len() > 40 { &q[..40] } else { q };
        println!(
            "  {label:<40}  {ip:>8.1}µs  {sh:>8.1}µs(+{sh_ov:.0})  {ud:>6.1}µs(+{ud_ov:.0})  {tc:>6.1}µs(+{tc_ov:.0})"
        );
    }

    println!("  {}", "─".repeat(90));

    // overhead decomposition
    println!();
    println!("  Transport overhead breakdown (avg across queries):");
    let n = queries.len() as f64;
    let avg_ip = t0.iter().map(|(_, v)| v).sum::<f64>() / n;
    let avg_sh = t1.iter().sum::<f64>() / n;
    let avg_ud = t2.iter().sum::<f64>() / n;
    let avg_tc = t3.iter().sum::<f64>() / n;
    println!("    Tier 0  in-process    : {avg_ip:>8.1}µs  (baseline)");
    println!(
        "    Tier 1  UCX-shmem     : {avg_sh:>8.1}µs  (+{:.1}µs  = shm copy + atomic RTT)",
        avg_sh - avg_ip
    );
    println!(
        "    Tier 2  UCX-UDS       : {avg_ud:>8.1}µs  (+{:.1}µs  = syscall + kernel copy)",
        avg_ud - avg_ip
    );
    println!(
        "    Tier 3  UCX-TCP       : {avg_tc:>8.1}µs  (+{:.1}µs  = TCP stack + Nagle off)",
        avg_tc - avg_ip
    );

    // Scale test
    println!();
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!("  SCALE: UCX-shmem tier — 'MATCH (n:Person) RETURN count(n)' vs node count");
    println!("═══════════════════════════════════════════════════════════════════════════════");
    let scale_q = "MATCH (n:Person) RETURN count(n) AS cnt";
    for &sz in &[100usize, 500, 1_000, 5_000, 10_000] {
        let ds2 = generate(sz, sz * 3, 99);
        let shm2 = Arc::new(ShmRegion::new());
        start_shm_server(&ds2, Arc::clone(&shm2));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let ptr = shm2.ptr;
        let qb = scale_q.as_bytes();
        let qlen = qb.len();
        // warmup
        for _ in 0..10 {
            unsafe {
                std::ptr::copy_nonoverlapping(qb.as_ptr(), ptr.add(OFF_REQ_BUF), qlen);
            }
            shm_req_len(ptr).store(qlen as u32, Ordering::Release);
            shm_state(ptr).store(1, Ordering::Release);
            loop {
                if shm_state(ptr).load(Ordering::Acquire) == 2 {
                    break;
                }
                std::hint::spin_loop();
            }
            shm_state(ptr).store(0, Ordering::Release);
        }

        let mut lats = Latencies::new();
        for _ in 0..200 {
            let t = Instant::now();
            unsafe {
                std::ptr::copy_nonoverlapping(qb.as_ptr(), ptr.add(OFF_REQ_BUF), qlen);
            }
            shm_req_len(ptr).store(qlen as u32, Ordering::Release);
            shm_state(ptr).store(1, Ordering::Release);
            loop {
                if shm_state(ptr).load(Ordering::Acquire) == 2 {
                    break;
                }
                std::hint::spin_loop();
            }
            lats.push(t.elapsed().as_micros() as u64);
            shm_state(ptr).store(0, Ordering::Release);
        }
        lats.0.sort_unstable();
        let n = lats.0.len();
        let mean = lats.0.iter().sum::<u64>() as f64 / n as f64;
        let p99 = lats.0[n * 99 / 100];
        let ops = 1_000_000.0 / mean;
        println!(
            "  nodes={sz:>6}  edges={:>6}  mean={mean:>7.1}µs  p99={p99:>7}µs  {ops:>8.0} ops/s",
            sz * 3
        );

        // shutdown
        let qb2 = b"__quit__";
        unsafe {
            std::ptr::copy_nonoverlapping(qb2.as_ptr(), ptr.add(OFF_REQ_BUF), qb2.len());
        }
        shm_req_len(ptr).store(qb2.len() as u32, Ordering::Release);
        shm_state(ptr).store(1, Ordering::Release);
    }

    println!("\ndone");
    Ok(())
}
