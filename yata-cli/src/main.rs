use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use yata_core::{BucketId, KvPutRequest, KvStore};
use yata_lance::LanceSink;
use yata_ocel::OcelProjector;
use yata_server::{Broker, BrokerConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    match args[1].as_str() {
        "status" => cmd_status().await?,
        "kv" => {
            if args.len() < 3 {
                eprintln!("usage: yata kv <get|put> ...");
                std::process::exit(1);
            }
            match args[2].as_str() {
                "get" => {
                    if args.len() < 5 {
                        eprintln!("usage: yata kv get <bucket> <key>");
                        std::process::exit(1);
                    }
                    cmd_kv_get(&args[3], &args[4]).await?;
                }
                "put" => {
                    if args.len() < 6 {
                        eprintln!("usage: yata kv put <bucket> <key> <value>");
                        std::process::exit(1);
                    }
                    cmd_kv_put(&args[3], &args[4], &args[5]).await?;
                }
                other => {
                    eprintln!("unknown kv subcommand: {}", other);
                    std::process::exit(1);
                }
            }
        }
        "ocel" => {
            if args.len() < 3 {
                eprintln!("usage: yata ocel <export> ...");
                std::process::exit(1);
            }
            match args[2].as_str() {
                "export" => {
                    if args.len() < 4 {
                        eprintln!("usage: yata ocel export <output_path>");
                        std::process::exit(1);
                    }
                    cmd_ocel_export(&args[3]).await?;
                }
                other => {
                    eprintln!("unknown ocel subcommand: {}", other);
                    std::process::exit(1);
                }
            }
        }
        other => {
            eprintln!("unknown command: {}", other);
            print_usage();
            std::process::exit(1);
        }
    }

    Ok(())
}

fn print_usage() {
    println!("yata - YATA broker CLI");
    println!();
    println!("USAGE:");
    println!("  yata status                         Show broker status");
    println!("  yata kv get <bucket> <key>          Get a KV entry");
    println!("  yata kv put <bucket> <key> <value>  Put a KV entry");
    println!("  yata ocel export <output_path>      Export OCEL log as JSON");
}

async fn open_broker() -> anyhow::Result<Arc<Broker>> {
    let config = BrokerConfig::default();
    let broker = Broker::new(config).await?;
    Ok(Arc::new(broker))
}

async fn cmd_status() -> anyhow::Result<()> {
    let broker = open_broker().await?;

    println!("YATA Broker Status");
    println!("==================");
    println!("data_dir:  {}", broker.config.data_dir.display());
    println!("lance_uri: {}", broker.config.lance_uri);

    // Show lance sync stats
    let stats = broker.lance.sync_stats().await?;
    println!();
    println!("Lance Sync Stats:");
    println!("  messages_written: {}", stats.messages_written);
    println!("  events_written:   {}", stats.events_written);
    println!("  objects_written:  {}", stats.objects_written);
    println!("  edges_written:    {}", stats.edges_written);
    println!("  bytes_written:    {}", stats.bytes_written);

    Ok(())
}

async fn cmd_kv_get(bucket: &str, key: &str) -> anyhow::Result<()> {
    let broker = open_broker().await?;
    let bucket_id = BucketId::from(bucket);

    // Load snapshot first (local KV only)
    if let Some(ref local_kv) = broker.local_kv {
        local_kv.load_snapshot(&bucket_id).await?;
    }

    match broker.kv.get(&bucket_id, key).await? {
        Some(entry) => {
            let value_str = String::from_utf8_lossy(&entry.value);
            println!("bucket:   {}", entry.bucket);
            println!("key:      {}", entry.key);
            println!("revision: {}", entry.revision);
            println!("ts_ns:    {}", entry.ts_ns);
            println!("value:    {}", value_str);
        }
        None => {
            println!("(not found)");
        }
    }
    Ok(())
}

async fn cmd_kv_put(bucket: &str, key: &str, value: &str) -> anyhow::Result<()> {
    let broker = open_broker().await?;
    let bucket_id = BucketId::from(bucket);

    let req = KvPutRequest {
        bucket: bucket_id.clone(),
        key: key.to_owned(),
        value: bytes::Bytes::from(value.to_owned()),
        expected_revision: None,
        ttl_secs: None,
    };
    let ack = broker.kv.put(req).await?;
    println!("OK revision={} ts_ns={}", ack.revision, ack.ts_ns);
    Ok(())
}

async fn cmd_ocel_export(output_path: &str) -> anyhow::Result<()> {
    let broker = open_broker().await?;
    let snapshot = broker.ocel.snapshot().await?;
    let json = yata_ocel::export_json(&snapshot)?;
    tokio::fs::write(output_path, json.as_bytes()).await?;
    println!("OCEL log exported to {}", output_path);
    Ok(())
}
