//! YATA load test harness.
//!
//! Usage:
//!   cargo run -p yata-bench --release -- [graph-scale|trillion]
//!
//! For specific benchmarks, use the dedicated binaries:
//!   cargo run -p yata-bench --bin graph-scale-bench --release
//!   cargo run -p yata-bench --bin trillion-scale-test --release

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let scenario = args.get(1).map(|s| s.as_str()).unwrap_or("help");

    println!("yata-bench: Available benchmarks:");
    println!("  graph-scale-bench     — Arrow IPC encode/decode + CSR scale");
    println!("  trillion-scale-test   — Trillion-scale partition test");
    println!("  cypher-bench          — Cypher parser benchmark");
    println!("  cypher-transport-bench — Cypher transport benchmark");
    println!();
    println!("Run: cargo run -p yata-bench --bin <name> --release");
    if scenario != "help" {
        println!(
            "(scenario '{}' not available — use dedicated binaries)",
            scenario
        );
    }
}
