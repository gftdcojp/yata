//! YATA load test harness.
//!
//! For specific benchmarks, use the dedicated binaries.

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let scenario = args.get(1).map(|s| s.as_str()).unwrap_or("help");

    println!("yata-bench: Available benchmarks:");
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
