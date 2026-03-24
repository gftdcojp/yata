//! Large synthetic graph benchmark for M1 scale baselining.
//!
//! Usage:
//!   cargo run -p yata-bench --bin graph-scale-bench --release
//!
//! Environment:
//!   SCALE_NODES=1000000   (1M default, supports 10M+)
//!   SCALE_DEGREE=3
//!   SCALE_LABELS=8
//!   SCALE_BATCH=100000
//!   SCALE_SKEW=0          (0=uniform, 1=power-law degree distribution)
//!   SCALE_JSON=1
//!   SCALE_GLOBAL_IDS=0    (1=use add_vertex_with_global_id for ID mapping bench)

use std::time::Instant;

use anyhow::Result;
use serde::Serialize;
use yata_engine::snapshot::serialize_snapshot;
use yata_grin::{Mutable, Scannable, Topology};
use yata_store::MutableCsrStore;

#[derive(Debug, Clone, Copy)]
struct Config {
    nodes: u64,
    avg_degree: u32,
    labels: u32,
    batch: u64,
    json: bool,
    skew: bool,
    global_ids: bool,
}

#[derive(Debug, Serialize)]
struct BenchResult {
    dataset: DatasetSummary,
    ingest: IngestResult,
    snapshot: SnapshotResult,
    restore: RestoreResult,
    queries: QuerySummary,
    memory: MemoryResult,
    global_id: Option<GlobalIdResult>,
    partitioned: Option<PartitionedResult>,
}

#[derive(Debug, Serialize)]
struct PartitionedResult {
    partition_count: u32,
    ingest_ms: u64,
    commit_ms: u64,
    vertex_distribution: Vec<usize>,
    selective_checkpoint_ms: u64,
    checkpointed_count: usize,
    skipped_count: usize,
    scan_across_partitions_ms: u64,
    scan_count: usize,
}

#[derive(Debug, Serialize)]
struct DatasetSummary {
    vertices: u64,
    edges: u64,
    labels: u32,
    batch: u64,
    skew: bool,
}

#[derive(Debug, Serialize)]
struct IngestResult {
    elapsed_ms: u64,
    vertex_ingest_ms: u64,
    edge_ingest_ms: u64,
    throughput_per_sec: f64,
    vertex_throughput: f64,
    edge_throughput: f64,
}

#[derive(Debug, Serialize)]
struct SnapshotResult {
    serialize_ms: u64,
    vertex_groups: usize,
    edge_groups: usize,
    topology_bytes: usize,
    total_arrow_bytes: usize,
}

#[derive(Debug, Serialize)]
struct RestoreResult {
    deserialize_ms: u64,
    vertex_count_restored: u64,
    edge_count_restored: u64,
}

#[derive(Debug, Serialize)]
struct QuerySummary {
    label_scan_ms: u64,
    label_scan_count: usize,
    out_degree_probe_us: u64,
    one_hop_ms: u64,
    one_hop_neighbors: usize,
    two_hop_ms: u64,
    two_hop_frontier: usize,
    property_filter_ms: u64,
    property_filter_count: usize,
}

#[derive(Debug, Serialize)]
struct MemoryResult {
    peak_rss_mb: f64,
    vertex_props_estimate_mb: f64,
    csr_estimate_mb: f64,
}

#[derive(Debug, Serialize)]
struct GlobalIdResult {
    ingest_with_global_ms: u64,
    lookup_p50_ns: u64,
    lookup_p99_ns: u64,
    map_size: usize,
}

fn config() -> Config {
    Config {
        nodes: std::env::var("SCALE_NODES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000_000),
        avg_degree: std::env::var("SCALE_DEGREE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3),
        labels: std::env::var("SCALE_LABELS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8),
        batch: std::env::var("SCALE_BATCH")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100_000),
        json: matches!(
            std::env::var("SCALE_JSON").as_deref(),
            Ok("1") | Ok("true") | Ok("yes")
        ),
        skew: matches!(
            std::env::var("SCALE_SKEW").as_deref(),
            Ok("1") | Ok("true") | Ok("yes")
        ),
        global_ids: matches!(
            std::env::var("SCALE_GLOBAL_IDS").as_deref(),
            Ok("1") | Ok("true") | Ok("yes")
        ),
    }
}

/// Power-law degree: ~80% of nodes have degree 1, ~20% have higher degree.
fn skewed_degree(node_idx: u64, avg_degree: u32) -> u32 {
    let hash = (node_idx.wrapping_mul(2654435761) >> 16) % 100;
    if hash < 80 {
        1
    } else if hash < 95 {
        avg_degree * 2
    } else if hash < 99 {
        avg_degree * 10
    } else {
        avg_degree * 50
    }
}

fn build_store(cfg: Config) -> (MutableCsrStore, IngestResult, u64) {
    let mut store = MutableCsrStore::new();
    let mut total_edges: u64 = 0;

    // Vertex ingest
    let v_start = Instant::now();
    for i in 0..cfg.nodes {
        let label_idx = if cfg.labels == 0 {
            0
        } else {
            (i % cfg.labels as u64) as u32
        };
        let label = format!("Label{}", label_idx);
        let ext_id = format!("n{:016x}", i);
        store.add_vertex(
            &label,
            &[
                ("ext_id", yata_grin::PropValue::Str(ext_id)),
                ("rank", yata_grin::PropValue::Int((i % 10_000) as i64)),
            ],
        );
        if (i + 1) % cfg.batch == 0 {
            store.commit();
        }
    }
    store.commit();
    let vertex_ms = v_start.elapsed().as_millis() as u64;

    // Edge ingest
    let e_start = Instant::now();
    for src in 0..cfg.nodes {
        let src_u32 = src as u32;
        let degree = if cfg.skew {
            skewed_degree(src, cfg.avg_degree)
        } else {
            cfg.avg_degree
        };
        for offset in 0..degree {
            let dst = ((src + 1 + offset as u64 * 997) % cfg.nodes) as u32;
            store.add_edge(
                src_u32,
                dst,
                "REL",
                &[("weight", yata_grin::PropValue::Float((offset + 1) as f64))],
            );
            total_edges += 1;
        }
        if (src + 1) % cfg.batch == 0 {
            store.commit();
        }
    }
    store.commit();
    let edge_ms = e_start.elapsed().as_millis() as u64;

    let total_ms = vertex_ms + edge_ms;
    let ingest = IngestResult {
        elapsed_ms: total_ms,
        vertex_ingest_ms: vertex_ms,
        edge_ingest_ms: edge_ms,
        throughput_per_sec: (cfg.nodes + total_edges) as f64 / (total_ms as f64 / 1000.0),
        vertex_throughput: cfg.nodes as f64 / (vertex_ms as f64 / 1000.0),
        edge_throughput: total_edges as f64 / (edge_ms.max(1) as f64 / 1000.0),
    };
    (store, ingest, total_edges)
}

fn snapshot_bench(
    store: &MutableCsrStore,
) -> (
    SnapshotResult,
    bytes::Bytes,
    Vec<(String, bytes::Bytes)>,
    Vec<(String, bytes::Bytes)>,
) {
    let start = Instant::now();
    let bundle = serialize_snapshot(store, 0).expect("serialize_snapshot failed");
    let serialize_ms = start.elapsed().as_millis() as u64;

    let total_arrow: usize = bundle
        .vertex_groups
        .iter()
        .map(|(_, b)| b.len())
        .sum::<usize>()
        + bundle
            .edge_groups
            .iter()
            .map(|(_, b)| b.len())
            .sum::<usize>();

    let result = SnapshotResult {
        serialize_ms,
        vertex_groups: bundle.vertex_groups.len(),
        edge_groups: bundle.edge_groups.len(),
        topology_bytes: bundle.topology.len(),
        total_arrow_bytes: total_arrow,
    };
    (
        result,
        bundle.topology,
        bundle.vertex_groups,
        bundle.edge_groups,
    )
}

fn restore_bench(
    vertex_groups: &[(String, bytes::Bytes)],
    edge_groups: &[(String, bytes::Bytes)],
) -> RestoreResult {
    use yata_store::arrow_codec;

    let start = Instant::now();
    let mut store = MutableCsrStore::new();

    for (_label, data) in vertex_groups {
        if let Ok(group) = arrow_codec::decode_vertex_group(data) {
            for vb in &group.vertices {
                let props: Vec<(&str, yata_grin::PropValue)> = vb
                    .props
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect();
                let primary_label = vb
                    .labels
                    .first()
                    .map(|s| s.as_str())
                    .unwrap_or(&group.label);
                store.add_vertex(primary_label, &props);
            }
        }
    }

    for (_label, data) in edge_groups {
        if let Ok(group) = arrow_codec::decode_edge_group(data) {
            for eb in &group.edges {
                let props: Vec<(&str, yata_grin::PropValue)> = eb
                    .props
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect();
                store.add_edge(eb.src, eb.dst, &eb.label, &props);
            }
        }
    }
    store.commit();
    let deserialize_ms = start.elapsed().as_millis() as u64;

    RestoreResult {
        deserialize_ms,
        vertex_count_restored: store.vertex_count() as u64,
        edge_count_restored: store.edge_count() as u64,
    }
}

fn query_bench(store: &MutableCsrStore, cfg: Config) -> QuerySummary {
    let probe_vid = (cfg.nodes.saturating_sub(1).min(u32::MAX as u64)) as u32;

    // Label scan
    let start = Instant::now();
    let scan_count = store.scan_vertices_by_label("Label0").len();
    let label_scan_ms = start.elapsed().as_millis() as u64;

    // Out-degree probe (µs precision)
    let start = Instant::now();
    let _ = store.out_degree(probe_vid);
    let out_degree_probe_us = start.elapsed().as_micros() as u64;

    // 1-hop traversal
    let start = Instant::now();
    let neighbors = store.out_neighbors(probe_vid);
    let one_hop_ms = start.elapsed().as_millis() as u64;

    // 2-hop traversal (1-hop + neighbor expansion)
    let start = Instant::now();
    let mut frontier_count: usize = 0;
    for n in &neighbors {
        frontier_count += store.out_neighbors(n.vid).len();
    }
    let two_hop_ms = start.elapsed().as_millis() as u64;

    // Property filter: rank > 5000
    let start = Instant::now();
    let filter_count = store
        .scan_vertices(
            "Label0",
            &yata_grin::Predicate::Gt("rank".into(), yata_grin::PropValue::Int(5000)),
        )
        .len();
    let property_filter_ms = start.elapsed().as_millis() as u64;

    QuerySummary {
        label_scan_ms,
        label_scan_count: scan_count,
        out_degree_probe_us,
        one_hop_ms,
        one_hop_neighbors: neighbors.len(),
        two_hop_ms,
        two_hop_frontier: frontier_count,
        property_filter_ms,
        property_filter_count: filter_count,
    }
}

fn memory_estimate(_store: &MutableCsrStore, cfg: Config) -> MemoryResult {
    // Estimate vertex property memory: ~200 bytes/vertex (2 props + HashMap overhead)
    let vertex_props_mb = (cfg.nodes as f64 * 200.0) / (1024.0 * 1024.0);
    // Estimate CSR memory: offsets = (V+2)*4 per label, edge_ids = E*4
    let total_edges = cfg.nodes * cfg.avg_degree as u64;
    let csr_mb =
        ((cfg.nodes + 2) as f64 * 4.0 * 2.0 + total_edges as f64 * 4.0 * 2.0) / (1024.0 * 1024.0);

    // Peak RSS from OS (macOS/Linux)
    let peak_rss_mb = get_peak_rss_mb();

    MemoryResult {
        peak_rss_mb,
        vertex_props_estimate_mb: vertex_props_mb,
        csr_estimate_mb: csr_mb,
    }
}

#[cfg(target_os = "macos")]
fn get_peak_rss_mb() -> f64 {
    use std::mem::MaybeUninit;
    unsafe {
        let mut info = MaybeUninit::<libc::rusage>::uninit();
        if libc::getrusage(libc::RUSAGE_SELF, info.as_mut_ptr()) == 0 {
            let info = info.assume_init();
            // macOS reports in bytes
            info.ru_maxrss as f64 / (1024.0 * 1024.0)
        } else {
            0.0
        }
    }
}

#[cfg(target_os = "linux")]
fn get_peak_rss_mb() -> f64 {
    use std::mem::MaybeUninit;
    unsafe {
        let mut info = MaybeUninit::<libc::rusage>::uninit();
        if libc::getrusage(libc::RUSAGE_SELF, info.as_mut_ptr()) == 0 {
            let info = info.assume_init();
            // Linux reports in kilobytes
            info.ru_maxrss as f64 / 1024.0
        } else {
            0.0
        }
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn get_peak_rss_mb() -> f64 {
    0.0
}

fn global_id_bench(cfg: Config) -> GlobalIdResult {
    use yata_core::{GlobalVid, PartitionId};

    let mut store = MutableCsrStore::new();
    let partition = PartitionId::new(0);

    let start = Instant::now();
    for i in 0..cfg.nodes.min(1_000_000) {
        let gv = GlobalVid::encode(partition, yata_core::LocalVid::new(i as u32));
        let label_idx = if cfg.labels == 0 {
            0
        } else {
            (i % cfg.labels as u64) as u32
        };
        let label = format!("Label{}", label_idx);
        store.add_vertex_with_global_id(
            gv,
            &label,
            &[("rank", yata_grin::PropValue::Int(i as i64))],
        );
    }
    store.commit();
    let ingest_ms = start.elapsed().as_millis() as u64;

    // Lookup latency (sample 1000 random lookups)
    let count = cfg.nodes.min(1_000_000) as u32;
    let mut latencies = Vec::with_capacity(1000);
    for i in 0..1000u32 {
        let idx = (i.wrapping_mul(2654435761) % count) as u32;
        let gv = GlobalVid::encode(partition, yata_core::LocalVid::new(idx));
        let t = Instant::now();
        let _ = store.lookup_local_vid(gv);
        latencies.push(t.elapsed().as_nanos() as u64);
    }
    latencies.sort_unstable();
    let p50 = latencies.get(500).copied().unwrap_or(0);
    let p99 = latencies.get(990).copied().unwrap_or(0);

    GlobalIdResult {
        ingest_with_global_ms: ingest_ms,
        lookup_p50_ns: p50,
        lookup_p99_ns: p99,
        map_size: store.global_map().len(),
    }
}

fn partitioned_bench(cfg: Config) -> PartitionedResult {
    use yata_core::{GlobalVid, LocalVid, PartitionId};
    use yata_engine::partition_snapshot;
    use yata_store::partition::{PartitionAssignment, PartitionStoreSet};

    let pcount = 4u32;
    let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash {
        partition_count: pcount,
    });
    let node_count = cfg.nodes.min(100_000); // cap for bench speed

    // Ingest
    let start = Instant::now();
    for i in 0..node_count {
        let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
        let label_idx = if cfg.labels == 0 {
            0
        } else {
            (i % cfg.labels as u64) as u32
        };
        let label = format!("Label{}", label_idx);
        pss.add_vertex(
            gv,
            &label,
            &[("rank", yata_grin::PropValue::Int((i % 1000) as i64))],
        );
    }
    let ingest_ms = start.elapsed().as_millis() as u64;

    // Add edges
    for i in 0..node_count.saturating_sub(1) {
        let src = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
        let dst = GlobalVid::encode(
            PartitionId::new(0),
            LocalVid::new(((i + 1) % node_count) as u32),
        );
        pss.add_edge(src, dst, "REL", &[]);
    }

    let commit_start = Instant::now();
    pss.commit();
    let commit_ms = commit_start.elapsed().as_millis() as u64;

    // Distribution
    let mut distribution = Vec::new();
    for pid in 0..pcount {
        distribution.push(pss.partition(pid).map(|s| s.vertex_count()).unwrap_or(0));
    }

    // Selective checkpoint
    let checkpoint_start = Instant::now();
    let result = partition_snapshot::selective_checkpoint(&mut pss);
    let selective_checkpoint_ms = checkpoint_start.elapsed().as_millis() as u64;

    // Cross-partition scan
    let scan_start = Instant::now();
    let scan_results = pss.scan_vertices_by_label("Label0");
    let scan_ms = scan_start.elapsed().as_millis() as u64;

    PartitionedResult {
        partition_count: pcount,
        ingest_ms,
        commit_ms,
        vertex_distribution: distribution,
        selective_checkpoint_ms,
        checkpointed_count: result.checkpointed_pids.len(),
        skipped_count: result.skipped_pids.len(),
        scan_across_partitions_ms: scan_ms,
        scan_count: scan_results.len(),
    }
}

fn main() -> Result<()> {
    let cfg = config();

    eprintln!(
        "building store: {} nodes, degree={}, labels={}, skew={}",
        cfg.nodes, cfg.avg_degree, cfg.labels, cfg.skew
    );
    let (store, ingest, total_edges) = build_store(cfg);

    eprintln!("snapshot serialize...");
    let (snapshot, _topo, vgroups, egroups) = snapshot_bench(&store);

    eprintln!("snapshot restore...");
    let restore = restore_bench(&vgroups, &egroups);

    eprintln!("query benchmark...");
    let queries = query_bench(&store, cfg);
    let memory = memory_estimate(&store, cfg);

    let global_id = if cfg.global_ids {
        eprintln!("global ID benchmark...");
        Some(global_id_bench(cfg))
    } else {
        None
    };

    eprintln!("partitioned benchmark...");
    let partitioned = Some(partitioned_bench(cfg));

    let result = BenchResult {
        dataset: DatasetSummary {
            vertices: store.vertex_count() as u64,
            edges: total_edges,
            labels: cfg.labels,
            batch: cfg.batch,
            skew: cfg.skew,
        },
        ingest,
        snapshot,
        restore,
        queries,
        memory,
        global_id,
        partitioned,
    };

    if cfg.json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("=== dataset ===");
        println!(
            "  vertices={} edges={} labels={} batch={} skew={}",
            result.dataset.vertices,
            result.dataset.edges,
            result.dataset.labels,
            result.dataset.batch,
            result.dataset.skew
        );
        println!("=== ingest ===");
        println!(
            "  total_ms={} vertex_ms={} edge_ms={}",
            result.ingest.elapsed_ms, result.ingest.vertex_ingest_ms, result.ingest.edge_ingest_ms
        );
        println!(
            "  throughput={:.0}/s vertex={:.0}/s edge={:.0}/s",
            result.ingest.throughput_per_sec,
            result.ingest.vertex_throughput,
            result.ingest.edge_throughput
        );
        println!("=== snapshot ===");
        println!(
            "  serialize_ms={} vertex_groups={} edge_groups={} topo_bytes={} arrow_bytes={}",
            result.snapshot.serialize_ms,
            result.snapshot.vertex_groups,
            result.snapshot.edge_groups,
            result.snapshot.topology_bytes,
            result.snapshot.total_arrow_bytes
        );
        println!("=== restore ===");
        println!(
            "  deserialize_ms={} vertices={} edges={}",
            result.restore.deserialize_ms,
            result.restore.vertex_count_restored,
            result.restore.edge_count_restored
        );
        println!("=== queries ===");
        println!(
            "  label_scan_ms={} count={}",
            result.queries.label_scan_ms, result.queries.label_scan_count
        );
        println!(
            "  out_degree_probe_us={}",
            result.queries.out_degree_probe_us
        );
        println!(
            "  1hop_ms={} neighbors={}",
            result.queries.one_hop_ms, result.queries.one_hop_neighbors
        );
        println!(
            "  2hop_ms={} frontier={}",
            result.queries.two_hop_ms, result.queries.two_hop_frontier
        );
        println!(
            "  prop_filter_ms={} count={}",
            result.queries.property_filter_ms, result.queries.property_filter_count
        );
        println!("=== memory ===");
        println!(
            "  peak_rss_mb={:.1} vertex_props_est_mb={:.1} csr_est_mb={:.1}",
            result.memory.peak_rss_mb,
            result.memory.vertex_props_estimate_mb,
            result.memory.csr_estimate_mb
        );
        if let Some(ref gid) = result.global_id {
            println!("=== global_id ===");
            println!(
                "  ingest_ms={} lookup_p50_ns={} lookup_p99_ns={} map_size={}",
                gid.ingest_with_global_ms, gid.lookup_p50_ns, gid.lookup_p99_ns, gid.map_size
            );
        }
        if let Some(ref p) = result.partitioned {
            println!("=== partitioned ({}p) ===", p.partition_count);
            println!("  ingest_ms={} commit_ms={}", p.ingest_ms, p.commit_ms);
            println!("  distribution={:?}", p.vertex_distribution);
            println!(
                "  selective_checkpoint_ms={} checkpointed={} skipped={}",
                p.selective_checkpoint_ms, p.checkpointed_count, p.skipped_count
            );
            println!(
                "  cross_partition_scan_ms={} count={}",
                p.scan_across_partitions_ms, p.scan_count
            );
        }
    }

    Ok(())
}
