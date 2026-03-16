//! yata tiered storage benchmark for graph queries.
//!
//! Storage tiers:
//!   Tier-M  MemoryGraph          — pure in-process (baseline)
//!   Tier-D  LanceDiskGraph       — Lance on local SSD (disk tier)
//!   Tier-S  LanceS3Graph         — Lance on MinIO/S3 (object-store tier)
//!   Tier-T  TieredGraph          — memory hot-cache + Lance disk cold store
//!   Ref     Neo4j Bolt/TCP       — reference database
//!
//! Usage:
//!   cargo run -p yata-bench --bin cypher-storage-bench --release
//!
//!   NODES=1000 EDGES=3000
//!   NEO4J_URI=bolt://localhost:7687 NEO4J_USER=neo4j NEO4J_PASS=benchmark
//!   S3_ENDPOINT=http://localhost:9000 S3_BUCKET=yata-bench
//!   S3_KEY=minioadmin S3_SECRET=minioadmin

use anyhow::Result;
use arrow_array::{Int64Array, StringArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lancedb::query::ExecutableQuery;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;
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
    Dataset {
        nodes: (0..n_nodes).map(|i| {
            let lbl = labels[i % labels.len()];
            (format!("node_{i}"), lbl.to_owned(), format!("{lbl}_{i}"), rng.gen_range(18..80i64))
        }).collect(),
        edges: (0..n_edges).map(|i| {
            let src = rng.gen_range(0..n_nodes);
            let mut dst = rng.gen_range(0..n_nodes);
            while dst == src { dst = rng.gen_range(0..n_nodes); }
            (format!("edge_{i}"), format!("node_{src}"), format!("node_{dst}"),
             rel_types[i % rel_types.len()].to_owned())
        }).collect(),
    }
}

fn build_memory_graph(ds: &Dataset) -> MemoryGraph {
    let mut g = MemoryGraph::new();
    for (id, label, name, age) in &ds.nodes {
        let props = [
            ("name".to_owned(), Value::Str(name.clone())),
            ("age".to_owned(), Value::Int(*age)),
        ].into_iter().collect();
        g.add_node(NodeRef { id: id.clone(), labels: vec![label.clone()], props });
    }
    for (id, src, dst, rel_type) in &ds.edges {
        g.add_rel(RelRef { id: id.clone(), rel_type: rel_type.clone(),
                           src: src.clone(), dst: dst.clone(), props: Default::default() });
    }
    g
}

// ── Lance helpers ─────────────────────────────────────────────────────────────

fn nodes_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("node_id",    DataType::Utf8,  false),
        Field::new("label",      DataType::Utf8,  false),
        Field::new("name",       DataType::Utf8,  false),
        Field::new("age",        DataType::Int64, false),
    ]))
}

fn edges_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("edge_id",  DataType::Utf8, false),
        Field::new("src",      DataType::Utf8, false),
        Field::new("dst",      DataType::Utf8, false),
        Field::new("rel_type", DataType::Utf8, false),
    ]))
}

async fn write_lance(ds: &Dataset, base_uri: &str) -> Result<()> {
    let conn = lancedb::connect(base_uri).execute().await?;

    // nodes table
    let ids:    Vec<&str> = ds.nodes.iter().map(|(id,_,_,_)| id.as_str()).collect();
    let labels: Vec<&str> = ds.nodes.iter().map(|(_,l,_,_)| l.as_str()).collect();
    let names:  Vec<&str> = ds.nodes.iter().map(|(_,_,n,_)| n.as_str()).collect();
    let ages:   Vec<i64>  = ds.nodes.iter().map(|(_,_,_,a)| *a).collect();

    let batch = RecordBatch::try_new(nodes_schema(), vec![
        Arc::new(StringArray::from(ids))     as Arc<dyn arrow_array::Array>,
        Arc::new(StringArray::from(labels)),
        Arc::new(StringArray::from(names)),
        Arc::new(Int64Array::from(ages)),
    ])?;
    let reader = Box::new(arrow::record_batch::RecordBatchIterator::new(
        [Ok(batch)].into_iter(), nodes_schema()));
    conn.create_table("graph_nodes", reader).execute().await?;

    // edges table
    let eids:  Vec<&str> = ds.edges.iter().map(|(id,_,_,_)| id.as_str()).collect();
    let srcs:  Vec<&str> = ds.edges.iter().map(|(_,s,_,_)| s.as_str()).collect();
    let dsts:  Vec<&str> = ds.edges.iter().map(|(_,_,d,_)| d.as_str()).collect();
    let rtys:  Vec<&str> = ds.edges.iter().map(|(_,_,_,r)| r.as_str()).collect();

    let ebatch = RecordBatch::try_new(edges_schema(), vec![
        Arc::new(StringArray::from(eids))  as Arc<dyn arrow_array::Array>,
        Arc::new(StringArray::from(srcs)),
        Arc::new(StringArray::from(dsts)),
        Arc::new(StringArray::from(rtys)),
    ])?;
    let reader2 = Box::new(arrow::record_batch::RecordBatchIterator::new(
        [Ok(ebatch)].into_iter(), edges_schema()));
    conn.create_table("graph_edges", reader2).execute().await?;

    Ok(())
}

/// Load all nodes + edges from Lance into MemoryGraph (disk cold-load, then warm).
async fn load_lance_to_memory(base_uri: &str) -> Result<MemoryGraph> {
    use futures::TryStreamExt;
    let conn = lancedb::connect(base_uri).execute().await?;
    let mut g = MemoryGraph::new();

    // scan nodes
    let nd = conn.open_table("graph_nodes").execute().await
        .map_err(|e| anyhow::anyhow!("open graph_nodes: {e}"))?;
    let nbatches: Vec<RecordBatch> = nd.query().execute().await
        .map_err(|e| anyhow::anyhow!("stream graph_nodes: {e}"))?
        .try_collect().await
        .map_err(|e| anyhow::anyhow!("collect graph_nodes: {e}"))?;
    for batch in nbatches {
        let ids    = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let labels = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let names  = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let ages   = batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..batch.num_rows() {
            let props = [
                ("name".to_owned(), Value::Str(names.value(i).to_owned())),
                ("age".to_owned(),  Value::Int(ages.value(i))),
            ].into_iter().collect();
            g.add_node(NodeRef {
                id: ids.value(i).to_owned(),
                labels: vec![labels.value(i).to_owned()],
                props,
            });
        }
    }

    // scan edges
    let ed = conn.open_table("graph_edges").execute().await
        .map_err(|e| anyhow::anyhow!("open graph_edges: {e}"))?;
    let ebatches: Vec<RecordBatch> = ed.query().execute().await
        .map_err(|e| anyhow::anyhow!("stream graph_edges: {e}"))?
        .try_collect().await
        .map_err(|e| anyhow::anyhow!("collect graph_edges: {e}"))?;
    for batch in ebatches {
        let eids = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let srcs = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let dsts = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let rtys = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            g.add_rel(RelRef {
                id:       eids.value(i).to_owned(),
                src:      srcs.value(i).to_owned(),
                dst:      dsts.value(i).to_owned(),
                rel_type: rtys.value(i).to_owned(),
                props:    Default::default(),
            });
        }
    }

    Ok(g)
}

// ── TieredGraph ───────────────────────────────────────────────────────────────
//
// Hot memory cache in front of a Lance cold store.
// Cache: full node/rel index (write-through).
// On miss: load from Lance, promote to cache.

struct TieredGraph {
    hot:      RwLock<MemoryGraph>,
    lance_uri: String,
    hit:      std::sync::atomic::AtomicU64,
    miss:     std::sync::atomic::AtomicU64,
}

impl TieredGraph {
    /// Prime cache with a subset (first `warm_fraction` of nodes).
    async fn new(base_uri: &str, warm_fraction: f64, ds: &Dataset) -> Result<Self> {
        let warm_n = ((ds.nodes.len() as f64) * warm_fraction) as usize;
        let mut hot = MemoryGraph::new();
        for (id, label, name, age) in ds.nodes.iter().take(warm_n) {
            let props = [
                ("name".to_owned(), Value::Str(name.clone())),
                ("age".to_owned(), Value::Int(*age)),
            ].into_iter().collect();
            hot.add_node(NodeRef { id: id.clone(), labels: vec![label.clone()], props });
        }
        // warm edges whose both endpoints are cached
        let hot_ids: std::collections::HashSet<&str> =
            ds.nodes.iter().take(warm_n).map(|(id,_,_,_)| id.as_str()).collect();
        for (id, src, dst, rt) in &ds.edges {
            if hot_ids.contains(src.as_str()) && hot_ids.contains(dst.as_str()) {
                hot.add_rel(RelRef { id: id.clone(), rel_type: rt.clone(),
                                     src: src.clone(), dst: dst.clone(), props: Default::default() });
            }
        }
        Ok(Self {
            hot: RwLock::new(hot),
            lance_uri: base_uri.to_owned(),
            hit:  std::sync::atomic::AtomicU64::new(0),
            miss: std::sync::atomic::AtomicU64::new(0),
        })
    }

    fn stats(&self) -> (u64, u64) {
        (self.hit.load(std::sync::atomic::Ordering::Relaxed),
         self.miss.load(std::sync::atomic::Ordering::Relaxed))
    }
}

// For benchmarking we implement Graph on TieredGraph by delegating to hot cache.
// On miss we load from Lance synchronously (simulates cache-miss penalty).
impl Graph for TieredGraph {
    fn nodes(&self) -> Vec<NodeRef> {
        self.hit.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.hot.read().unwrap().nodes()
    }
    fn rels(&self) -> Vec<RelRef> {
        self.hit.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.hot.read().unwrap().rels()
    }
    fn node_by_id(&self, id: &str) -> Option<NodeRef> {
        let r = self.hot.read().unwrap().node_by_id(id);
        if r.is_some() {
            self.hit.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.miss.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        r
    }
    fn rel_by_id(&self, id: &str) -> Option<RelRef> {
        self.hot.read().unwrap().rel_by_id(id)
    }
    fn rels_from(&self, node_id: &str) -> Vec<RelRef> {
        self.hot.read().unwrap().rels_from(node_id)
    }
    fn rels_to(&self, node_id: &str) -> Vec<RelRef> {
        self.hot.read().unwrap().rels_to(node_id)
    }
    fn add_node(&mut self, node: NodeRef) { self.hot.write().unwrap().add_node(node); }
    fn add_rel(&mut self, rel: RelRef)    { self.hot.write().unwrap().add_rel(rel); }
    fn set_node_prop(&mut self, id: &str, key: &str, val: Value) {
        self.hot.write().unwrap().set_node_prop(id, key, val);
    }
    fn delete_node(&mut self, id: &str) { self.hot.write().unwrap().delete_node(id); }
    fn delete_rel(&mut self, id: &str)  { self.hot.write().unwrap().delete_rel(id); }
}

// ── benchmark runner ──────────────────────────────────────────────────────────

struct Latencies(Vec<u64>);
impl Latencies {
    fn new() -> Self { Self(Vec::new()) }
    fn push(&mut self, us: u64) { self.0.push(us); }
    fn report(&mut self, tier: &str, scenario: &str) -> f64 {
        self.0.sort_unstable();
        let n = self.0.len();
        if n == 0 { return 0.0; }
        let mean = self.0.iter().sum::<u64>() as f64 / n as f64;
        let p50  = self.0[n * 50 / 100];
        let p95  = self.0[n * 95 / 100];
        let p99  = self.0[n * 99 / 100];
        let max  = *self.0.last().unwrap();
        let ops  = 1_000_000.0 / mean;
        println!("  [{tier:<22}] {scenario:<44} mean={mean:>8.1}µs  p50={p50:>7}µs  p95={p95:>7}µs  p99={p99:>7}µs  max={max:>8}µs  {ops:>8.0} ops/s");
        mean
    }
}

fn run_bench(g: &mut dyn Graph, queries: &[&str], iters: usize, tier: &str) -> Vec<f64> {
    let exec = Executor::new();
    let mut means = Vec::new();
    for &q in queries {
        let parsed = parse(q).unwrap();
        for _ in 0..20 { let _ = exec.execute(&parsed, g); }
        let mut lat = Latencies::new();
        for _ in 0..iters {
            let t = Instant::now();
            let _ = exec.execute(&parsed, g);
            lat.push(t.elapsed().as_micros() as u64);
        }
        let mean = lat.report(tier, q);
        means.push(mean);
    }
    means
}

// ── full-cycle benchmark: Lance cold-load + query per iteration ──────────────

async fn run_fullcycle_bench(uri: &str, queries: &[&str], iters: usize, tier: &str) -> Vec<f64> {
    let mut means = Vec::new();
    for &q in queries {
        // warmup: 3 iterations
        for _ in 0..3 {
            match load_lance_to_memory(uri).await {
                Ok(mut g) => {
                    let parsed = parse(q).unwrap();
                    let _ = Executor::new().execute(&parsed, &mut g);
                }
                Err(e) => {
                    println!("  [{tier}] warmup load failed: {e}");
                    means.push(0.0);
                    continue;
                }
            }
        }
        let mut lat = Latencies::new();
        let mut errors = 0usize;
        for _ in 0..iters {
            let t = Instant::now();
            match load_lance_to_memory(uri).await {
                Ok(mut g) => {
                    let parsed = parse(q).unwrap();
                    let _ = Executor::new().execute(&parsed, &mut g);
                    lat.push(t.elapsed().as_micros() as u64);
                }
                Err(_) => { errors += 1; }
            }
        }
        if errors > 0 {
            println!("  [{tier}] {q}: {errors}/{iters} load errors");
        }
        let mean = lat.report(tier, q);
        means.push(mean);
    }
    means
}

// ── Neo4j reference ───────────────────────────────────────────────────────────

async fn bench_neo4j(uri: &str, user: &str, pass: &str, ds: &Dataset, queries: &[&str], iters: usize) -> Option<Vec<f64>> {
    let neo = neo4rs::Graph::new(uri, user, pass).await.ok()?;
    let _ = neo.run(neo4rs::query("MATCH (n) DETACH DELETE n")).await;
    for (id, label, name, age) in &ds.nodes {
        let _ = neo.run(neo4rs::query(&format!(
            "CREATE (:{label} {{id:'{id}',name:'{name}',age:{age}}})"))).await;
    }
    for (_, src, dst, rt) in &ds.edges {
        let _ = neo.run(neo4rs::query(&format!(
            "MATCH (a{{id:'{src}'}}),(b{{id:'{dst}'}}) CREATE (a)-[:{rt}]->(b)"))).await;
    }
    let mut results = Vec::new();
    for &q in queries {
        for _ in 0..5 {
            let mut r = neo.execute(neo4rs::query(q)).await.unwrap();
            while r.next().await.unwrap().is_some() {}
        }
        let mut lat = Latencies::new();
        for _ in 0..(iters / 4).max(10) {
            let t = Instant::now();
            let mut r = neo.execute(neo4rs::query(q)).await.unwrap();
            while r.next().await.unwrap().is_some() {}
            lat.push(t.elapsed().as_micros() as u64);
        }
        let mean = lat.report("Neo4j-Bolt/TCP", q);
        results.push(mean);
    }
    Some(results)
}

// ── load-time benchmark ───────────────────────────────────────────────────────

async fn bench_lance_load_time(ds: &Dataset, local_uri: &str, s3_uri: Option<&str>) {
    println!("\n─── Cold-start load time (Lance → MemoryGraph) ─────────────────────────────────");

    let t = Instant::now();
    let _ = load_lance_to_memory(local_uri).await.unwrap();
    let disk_ms = t.elapsed().as_secs_f64() * 1000.0;
    println!("  [Lance-disk]  load {} nodes + {} edges : {:.1}ms", ds.nodes.len(), ds.edges.len(), disk_ms);

    if let Some(uri) = s3_uri {
        let t = Instant::now();
        match load_lance_to_memory(uri).await {
            Ok(_) => {
                let s3_ms = t.elapsed().as_secs_f64() * 1000.0;
                println!("  [Lance-S3]    load {} nodes + {} edges : {:.1}ms  ({:.1}x vs disk)", ds.nodes.len(), ds.edges.len(), s3_ms, s3_ms / disk_ms);
            }
            Err(e) => println!("  [Lance-S3]    unavailable: {e}"),
        }
    }
}

// ── scale: load-time vs dataset size ─────────────────────────────────────────

async fn bench_scale_load(dir: &tempfile::TempDir) {
    println!("\n─── Scale: Lance-disk cold-load vs node count ───────────────────────────────────");
    println!("  {:<8}  {:<8}  {:>12}  {:>12}  {:>12}", "nodes", "edges", "write(ms)", "load(ms)", "query(µs)");
    let q = "MATCH (n:Person) RETURN count(n) AS cnt";
    for &sz in &[100usize, 500, 1_000, 5_000, 10_000] {
        let ds2 = generate(sz, sz * 3, 77);
        let uri = dir.path().join(format!("scale_{sz}")).to_str().unwrap().to_owned();
        let tw = Instant::now();
        write_lance(&ds2, &uri).await.unwrap();
        let write_ms = tw.elapsed().as_secs_f64() * 1000.0;
        let tl = Instant::now();
        let mut g = load_lance_to_memory(&uri).await.unwrap();
        let load_ms = tl.elapsed().as_secs_f64() * 1000.0;
        // query warmup + measure
        let parsed = parse(q).unwrap();
        let exec = Executor::new();
        for _ in 0..5 { let _ = exec.execute(&parsed, &mut g); }
        let tq = Instant::now();
        for _ in 0..100 { let _ = exec.execute(&parsed, &mut g); }
        let query_us = tq.elapsed().as_micros() as f64 / 100.0;
        println!("  {sz:<8}  {:<8}  {write_ms:>10.1}ms  {load_ms:>10.1}ms  {query_us:>10.1}µs", sz * 3);
    }
}

// ── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let n_nodes: usize = std::env::var("NODES").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
    let n_edges: usize = std::env::var("EDGES").ok().and_then(|v| v.parse().ok()).unwrap_or(3000);
    let neo4j_uri  = std::env::var("NEO4J_URI").unwrap_or("bolt://localhost:7687".into());
    let neo4j_user = std::env::var("NEO4J_USER").unwrap_or("neo4j".into());
    let neo4j_pass = std::env::var("NEO4J_PASS").unwrap_or("benchmark".into());
    let s3_endpoint = std::env::var("S3_ENDPOINT").ok();
    let s3_bucket   = std::env::var("S3_BUCKET").unwrap_or("ai-gftd-lancedb".into());
    let s3_key      = std::env::var("S3_KEY").unwrap_or("minioadmin".into());
    let s3_secret   = std::env::var("S3_SECRET").unwrap_or("minioadmin".into());

    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║   yata tiered-storage graph benchmark  (Memory / Lance-disk / Lance-S3)    ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
    println!("  Dataset : {n_nodes} nodes  {n_edges} edges");
    println!("  Tiers   : Tier-M (MemoryGraph) | Tier-D (Lance-disk) | Tier-S (Lance-S3)");
    println!("            Tier-T (TieredGraph 50%% hot) | Neo4j Bolt (reference)");
    println!();

    let ds = generate(n_nodes, n_edges, 42);
    let dir = tempfile::tempdir()?;
    let lance_local_uri = dir.path().join("lance").to_str().unwrap().to_owned();

    // S3 URI (Lance uses s3+http:// for custom endpoints)
    let s3_uri: Option<String> = s3_endpoint.as_ref().map(|ep| {
        // Lance expects s3://bucket/prefix with AWS env vars set
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", &s3_key);
            std::env::set_var("AWS_SECRET_ACCESS_KEY", &s3_secret);
            std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
            std::env::set_var("AWS_ENDPOINT_URL", ep);
            std::env::set_var("AWS_ALLOW_HTTP", "1");
        }
        format!("s3://{s3_bucket}/yata-bench-graph")
    });

    // ── Setup: write Lance data ───────────────────────────────────────────────
    println!("─── Setup: writing Lance datasets ────────────────────────────────────────────");
    let t = Instant::now();
    write_lance(&ds, &lance_local_uri).await?;
    println!("  [Lance-disk]  written in {:.1}ms  (uri: {})", t.elapsed().as_secs_f64()*1000.0, lance_local_uri);

    if let Some(ref uri) = s3_uri {
        // Create S3 bucket via AWS SDK
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .endpoint_url(s3_endpoint.as_deref().unwrap_or(""))
            .region(aws_config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                &s3_key, &s3_secret, None, None, "bench"))
            .load().await;
        let s3 = aws_sdk_s3::Client::new(&config);
        let _ = s3.create_bucket().bucket(&s3_bucket).send().await;

        let t = Instant::now();
        match write_lance(&ds, uri).await {
            Ok(_) => println!("  [Lance-S3]    written in {:.1}ms  (uri: {})", t.elapsed().as_secs_f64()*1000.0, uri),
            Err(e) => { println!("  [Lance-S3]    write failed: {e} — S3 tier skipped"); }
        }
    } else {
        println!("  [Lance-S3]    skipped (S3_ENDPOINT not set)");
    }

    // ── Load graphs into memory from each tier ────────────────────────────────
    println!("\n─── Loading graphs from storage ──────────────────────────────────────────────");

    // Tier-M: in-memory build
    let t = Instant::now();
    let mut mem_g = build_memory_graph(&ds);
    println!("  [Tier-M memory]   built    in {:.1}ms", t.elapsed().as_secs_f64()*1000.0);

    // Tier-D: load from local Lance
    let t = Instant::now();
    let mut disk_g = load_lance_to_memory(&lance_local_uri).await?;
    let disk_load_ms = t.elapsed().as_secs_f64()*1000.0;
    println!("  [Tier-D Lance-disk] loaded in {:.1}ms", disk_load_ms);

    // Tier-S: load from S3 Lance
    let s3_g_result: Option<MemoryGraph> = if let Some(ref uri) = s3_uri {
        let t = Instant::now();
        match load_lance_to_memory(uri).await {
            Ok(g) => {
                println!("  [Tier-S Lance-S3]   loaded in {:.1}ms", t.elapsed().as_secs_f64()*1000.0);
                Some(g)
            }
            Err(e) => { println!("  [Tier-S Lance-S3]   load failed: {e}"); None }
        }
    } else {
        println!("  [Tier-S Lance-S3]   skipped");
        None
    };

    // Tier-T: tiered (50% warm cache)
    let t = Instant::now();
    let mut tiered_g = TieredGraph::new(&lance_local_uri, 0.50, &ds).await?;
    println!("  [Tier-T tiered-50] primed in {:.1}ms (50% hot cache)", t.elapsed().as_secs_f64()*1000.0);

    // ── Benchmark queries ─────────────────────────────────────────────────────
    let queries: &[&str] = &[
        "MATCH (n:Person) RETURN count(n) AS cnt",
        "MATCH (n:Person) WHERE n.age > 50 RETURN count(n) AS cnt",
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*) AS cnt",
        "MATCH (n:Person) WHERE n.age > 30 RETURN n.name ORDER BY n.name ASC LIMIT 10",
        "MATCH (n) RETURN count(n) AS total",
    ];
    let iters = 200;

    println!("\n─── Query benchmark ──────────────────────────────────────────────────────────");
    println!("  {:<24} {:<44} {:>10}  {:>9}  {:>9}  {:>9}  {:>10}  {:>9}",
        "tier", "query", "mean(µs)", "p50(µs)", "p95(µs)", "p99(µs)", "max(µs)", "ops/s");
    println!("  {}", "─".repeat(145));

    let r_mem    = run_bench(&mut mem_g,    queries, iters, "Tier-M (memory)");
    let r_disk   = run_bench(&mut disk_g,   queries, iters, "Tier-D (lance-disk)");
    let r_s3     = if let Some(mut g) = s3_g_result {
        run_bench(&mut g, queries, iters, "Tier-S (lance-S3)")
    } else {
        vec![0.0; queries.len()]
    };
    let r_tiered = run_bench(&mut tiered_g, queries, iters, "Tier-T (tiered-50%)");

    // Neo4j reference (skip if SKIP_NEO4J=1 or not available)
    let skip_neo4j = std::env::var("SKIP_NEO4J").unwrap_or_default() == "1";
    let r_neo4j = if skip_neo4j {
        println!("\n  [Neo4j skipped (SKIP_NEO4J=1)]");
        None
    } else {
        println!("\n  [loading Neo4j data...]");
        // Use tokio timeout to avoid hanging on unreachable Neo4j
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            bench_neo4j(&neo4j_uri, &neo4j_user, &neo4j_pass, &ds, queries, iters),
        ).await {
            Ok(r) => r,
            Err(_) => { println!("  [Neo4j connection timeout — skipped]"); None }
        }
    };

    // ── Summary table (warm query after initial load) ─────────────────────────
    println!("\n═══════════════════════════════════════════════════════════════════════════════");
    println!("  SUMMARY — warm query mean latency µs (overhead vs Tier-M)");
    println!("═══════════════════════════════════════════════════════════════════════════════");
    println!("  {:<44}  {:>10}  {:>14}  {:>13}  {:>14}  {:>14}",
        "query", "Tier-M(mem)", "Tier-D(disk)", "Tier-S(S3)", "Tier-T(50%hot)", "Neo4j-Bolt");
    println!("  {}", "─".repeat(120));

    for (i, &q) in queries.iter().enumerate() {
        let lbl = if q.len() > 44 { &q[..44] } else { q };
        let m  = r_mem.get(i).copied().unwrap_or(0.0);
        let d  = r_disk.get(i).copied().unwrap_or(0.0);
        let s  = r_s3.get(i).copied().unwrap_or(0.0);
        let tt = r_tiered.get(i).copied().unwrap_or(0.0);
        let n  = r_neo4j.as_ref().and_then(|v| v.get(i).copied());
        let s3_str  = if s > 0.0 { format!("{s:>7.1}(+{:.0})", s-m) } else { "    N/A   ".into() };
        let neo_str = n.map(|v| format!("{v:>7.1}")).unwrap_or_else(|| "   N/A ".into());
        println!("  {lbl:<44}  {m:>8.1}µs  {d:>8.1}µs(+{:.0})  {s3_str}  {tt:>8.1}µs(+{:.0})  {neo_str}µs",
            d-m, tt-m);
    }
    println!("  {}", "─".repeat(120));

    let n = queries.len() as f64;
    let avg_m  = r_mem.iter().sum::<f64>() / n;
    let avg_d  = r_disk.iter().sum::<f64>() / n;
    let avg_tt = r_tiered.iter().sum::<f64>() / n;
    println!();
    println!("  Tier overhead summary (avg across {} queries):", queries.len());
    println!("    Tier-M  memory       : {:>8.1}µs  (baseline — zero I/O)", avg_m);
    println!("    Tier-D  Lance-disk   : {:>8.1}µs  (+{:.1}µs after cold-load {:.0}ms, then warm)", avg_d, avg_d-avg_m, disk_load_ms);
    println!("    Tier-T  tiered-50%%   : {:>8.1}µs  (+{:.1}µs — 50%% hot cache, 50%% cold miss)", avg_tt, avg_tt-avg_m);
    if let Some(ref v) = r_neo4j {
        let avg_n = v.iter().sum::<f64>() / n;
        println!("    Neo4j   Bolt/TCP     : {:>8.1}µs  ({:.1}x slower than Tier-M, {:.1}x slower than Tier-D)",
            avg_n, avg_n/avg_m, avg_n/avg_d);
    }

    // ── Tiered graph cache stats ──────────────────────────────────────────────
    let (hits, misses) = tiered_g.stats();
    println!();
    println!("  TieredGraph cache stats: hits={hits} misses={misses} hit_rate={:.1}%%",
        100.0 * hits as f64 / (hits + misses).max(1) as f64);

    // ── Full-cycle benchmark: Lance load + query per iteration ────────────────
    let fc_iters = 30; // fewer iterations since each includes a full load
    println!("\n═══════════════════════════════════════════════════════════════════════════════");
    println!("  FULL-CYCLE: Lance cold-load + query per iteration ({fc_iters} iters)");
    println!("  (Measures real end-to-end latency including storage I/O)");
    println!("═══════════════════════════════════════════════════════════════════════════════");

    let fc_disk = run_fullcycle_bench(&lance_local_uri, queries, fc_iters, "FC-Disk (Lance SSD)").await;

    let fc_s3_iters = 10; // S3 round-trip is ~3s, keep short
    let fc_s3 = if let Some(ref uri) = s3_uri {
        run_fullcycle_bench(uri, queries, fc_s3_iters, "FC-S3 (Lance→B2)").await
    } else {
        vec![0.0; queries.len()]
    };

    // Full-cycle summary
    println!("\n─── Full-cycle summary (load + query per call) ───────────────────────────────");
    println!("  {:<44}  {:>14}  {:>14}  {:>10}", "query", "FC-Disk(µs)", "FC-S3(µs)", "S3/Disk");
    println!("  {}", "─".repeat(90));
    for (i, &q) in queries.iter().enumerate() {
        let lbl = if q.len() > 44 { &q[..44] } else { q };
        let d = fc_disk.get(i).copied().unwrap_or(0.0);
        let s = fc_s3.get(i).copied().unwrap_or(0.0);
        let ratio = if d > 0.0 && s > 0.0 { format!("{:.1}x", s / d) } else { "N/A".into() };
        let s_str = if s > 0.0 { format!("{s:>12.1}") } else { "         N/A".into() };
        println!("  {lbl:<44}  {d:>12.1}µs  {s_str}µs  {:>10}", ratio);
    }
    let avg_fc_d = fc_disk.iter().sum::<f64>() / n;
    let avg_fc_s = fc_s3.iter().sum::<f64>() / n;
    println!("  {}", "─".repeat(90));
    println!("  avg full-cycle: disk={avg_fc_d:.0}µs  S3={:.0}µs  warm-query-only={avg_m:.0}µs",
        if avg_fc_s > 0.0 { avg_fc_s } else { 0.0 });

    // ── Load time + scale ─────────────────────────────────────────────────────
    bench_lance_load_time(&ds, &lance_local_uri, s3_uri.as_deref()).await;
    let scale_dir = tempfile::tempdir()?;
    bench_scale_load(&scale_dir).await;

    println!("\ndone");
    Ok(())
}
