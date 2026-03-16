// Distributed graph benchmark — simulates multi-shard execution on single node.
// Measures: throughput, latency, partition scalability, cross-shard overhead.

use std::time::Instant;
use yata_grin::*;
use yata_store::MutableCsrStore;
use yata_coordinator::*;
use yata_gie::ir::*;
use yata_gie::executor;

fn main() {
    println!("=== yata-graph Distributed Benchmark ===\n");

    // Scale factors
    for &(n_vertices, n_edges, n_shards) in &[
        (10_000, 50_000, 1),
        (10_000, 50_000, 2),
        (10_000, 50_000, 4),
        (100_000, 500_000, 1),
        (100_000, 500_000, 4),
        (100_000, 500_000, 8),
        (1_000_000, 5_000_000, 1),
        (1_000_000, 5_000_000, 4),
        (1_000_000, 5_000_000, 8),
    ] {
        bench_scale(n_vertices, n_edges, n_shards);
    }

    println!("\n=== LDBC-style Interactive Workload ===\n");
    bench_ldbc_interactive();
}

fn bench_scale(n_vertices: usize, n_edges: usize, n_shards: usize) {
    let labels: Vec<Vec<String>> = (0..n_shards)
        .map(|i| vec![format!("Label{}", i)])
        .collect();

    let mut store = if n_shards <= 1 {
        ShardedGraphStore::single()
    } else {
        ShardedGraphStore::with_label_partitions(&labels)
    };

    // Populate
    let pop_start = Instant::now();
    for i in 0..n_vertices {
        let label = if n_shards <= 1 {
            "Person"
        } else {
            // Distribute across shards
            match i % n_shards {
                0 => "Label0", 1 => "Label1", 2 => "Label2", 3 => "Label3",
                4 => "Label4", 5 => "Label5", 6 => "Label6", _ => "Label7",
            }
        };
        store.add_vertex(label, &[
            ("name", PropValue::Str(format!("v{}", i))),
            ("age", PropValue::Int((20 + i % 50) as i64)),
        ]);
    }

    // Add edges within shards
    for i in 0..n_edges {
        let shard_idx = if n_shards <= 1 { 0 } else { i % n_shards };
        let verts_per_shard = n_vertices / n_shards.max(1);
        let src = (i % verts_per_shard) as u32;
        let dst = ((i * 7 + 3) % verts_per_shard) as u32;
        if src != dst {
            store.add_edge(shard_idx, src, dst, "KNOWS", &[]);
        }
    }

    store.commit_all();
    let pop_elapsed = pop_start.elapsed();

    // Benchmark: label scan
    let label = if n_shards <= 1 { "Person" } else { "Label0" };
    let scan_iters = 100;
    let scan_start = Instant::now();
    let mut scan_total = 0;
    for _ in 0..scan_iters {
        scan_total += store.scan_vertices_by_label(label).len();
    }
    let scan_elapsed = scan_start.elapsed();

    // Benchmark: GIE plan execution (Scan + Expand)
    let mut plan = QueryPlan::new();
    plan.push(LogicalOp::Scan {
        label: label.to_string(),
        alias: "n".into(),
        predicate: None,
    });
    plan.push(LogicalOp::Expand {
        src_alias: "n".into(),
        edge_label: "KNOWS".into(),
        dst_alias: "m".into(),
        direction: Direction::Out,
    });
    plan.push(LogicalOp::Limit { count: 100, offset: 0 });

    let gie_iters = 50;
    let gie_start = Instant::now();
    let mut gie_total = 0;
    for _ in 0..gie_iters {
        let results = execute_sharded_merged(&plan, &store, &[label.to_string()]);
        gie_total += results.len();
    }
    let gie_elapsed = gie_start.elapsed();

    // Benchmark: count aggregation across shards
    let mut count_plan = QueryPlan::new();
    count_plan.push(LogicalOp::Scan {
        label: label.to_string(),
        alias: "n".into(),
        predicate: None,
    });
    count_plan.push(LogicalOp::Aggregate {
        group_by: vec![],
        aggs: vec![("cnt".into(), AggOp::Count, Expr::Var("n".into()))],
    });

    let agg_iters = 100;
    let agg_start = Instant::now();
    for _ in 0..agg_iters {
        execute_sharded_merged(&count_plan, &store, &[label.to_string()]);
    }
    let agg_elapsed = agg_start.elapsed();

    println!(
        "V={:>8} E={:>9} shards={} | populate: {:>8.1}ms | scan: {:>7.1}μs/iter ({} found) | GIE traverse: {:>7.1}μs/iter ({} results) | count agg: {:>7.1}μs/iter",
        n_vertices, n_edges, n_shards,
        pop_elapsed.as_secs_f64() * 1000.0,
        scan_elapsed.as_secs_f64() * 1_000_000.0 / scan_iters as f64,
        scan_total / scan_iters,
        gie_elapsed.as_secs_f64() * 1_000_000.0 / gie_iters as f64,
        gie_total / gie_iters,
        agg_elapsed.as_secs_f64() * 1_000_000.0 / agg_iters as f64,
    );
}

fn bench_ldbc_interactive() {
    // Simulate LDBC Interactive workload: mix of reads + writes
    let mut store = ShardedGraphStore::with_label_partitions(&[
        vec!["Person".into()],
        vec!["Post".into()],
        vec!["Comment".into()],
    ]);

    // Pre-populate: 100K persons, 500K posts, 1M comments
    let pop_start = Instant::now();
    for i in 0..100_000 {
        store.add_vertex("Person", &[
            ("name", PropValue::Str(format!("person{}", i))),
            ("age", PropValue::Int((20 + i % 50) as i64)),
        ]);
    }
    for i in 0..500_000 {
        store.add_vertex("Post", &[
            ("content", PropValue::Str(format!("post content {}", i))),
        ]);
    }
    for i in 0..1_000_000 {
        store.add_vertex("Comment", &[
            ("text", PropValue::Str(format!("comment {}", i))),
        ]);
    }
    // Edges within shards
    for i in 0..200_000 {
        store.add_edge(0, (i % 100_000) as u32, ((i * 3 + 7) % 100_000) as u32, "KNOWS", &[]);
    }
    store.commit_all();
    let pop_elapsed = pop_start.elapsed();
    println!("LDBC populate (1.6M vertices, 200K edges): {:.1}ms", pop_elapsed.as_secs_f64() * 1000.0);

    // Interactive workload: 70% reads, 30% writes
    let total_ops = 10_000;
    let read_ops = (total_ops as f64 * 0.7) as usize;
    let write_ops = total_ops - read_ops;

    let work_start = Instant::now();

    // Reads: scan Person by predicate
    for i in 0..read_ops {
        let pred = Predicate::Eq("age".into(), PropValue::Int((20 + i as i64 % 50)));
        store.scan_vertices_filtered("Person", &pred);
    }

    // Writes: add new vertices
    for i in 0..write_ops {
        store.add_vertex("Person", &[
            ("name", PropValue::Str(format!("new_person{}", i))),
            ("age", PropValue::Int(25)),
        ]);
    }

    let work_elapsed = work_start.elapsed();
    let ops_per_sec = total_ops as f64 / work_elapsed.as_secs_f64();

    println!(
        "LDBC Interactive: {} ops ({} reads + {} writes) in {:.1}ms → {:.0} ops/sec",
        total_ops, read_ops, write_ops,
        work_elapsed.as_secs_f64() * 1000.0,
        ops_per_sec,
    );

    // GIE pipeline: complex read (Scan + Expand + Project + Limit)
    let mut plan = QueryPlan::new();
    plan.push(LogicalOp::Scan {
        label: "Person".into(),
        alias: "n".into(),
        predicate: Some(Predicate::Eq("age".into(), PropValue::Int(25))),
    });
    plan.push(LogicalOp::Expand {
        src_alias: "n".into(),
        edge_label: "KNOWS".into(),
        dst_alias: "m".into(),
        direction: Direction::Out,
    });
    plan.push(LogicalOp::Limit { count: 10, offset: 0 });

    let complex_iters = 1000;
    let complex_start = Instant::now();
    for _ in 0..complex_iters {
        execute_sharded_merged(&plan, &store, &["Person".into()]);
    }
    let complex_elapsed = complex_start.elapsed();
    println!(
        "LDBC Complex Read (Scan+Expand+Limit): {:.1}μs/query → {:.0} QPS",
        complex_elapsed.as_secs_f64() * 1_000_000.0 / complex_iters as f64,
        complex_iters as f64 / complex_elapsed.as_secs_f64(),
    );
}
