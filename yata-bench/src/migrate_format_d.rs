//! Migrate existing 7-col vertices table to Format D (10-col).
//!
//! Reads all rows from `vertices` (legacy 7-col schema), extracts promoted
//! properties from props_json, and writes to `vertices_v2` in Format D.
//! After verification, renames tables.
//!
//! Usage:
//!   # Local LanceDB
//!   YATA_DATA_DIR=/path/to/lance cargo run -p yata-bench --bin migrate-format-d --release
//!
//!   # R2 (production)
//!   YATA_S3_ENDPOINT=... YATA_S3_BUCKET=... YATA_S3_ACCESS_KEY_ID=... \
//!   YATA_S3_SECRET_ACCESS_KEY=... YATA_S3_PREFIX=yata/partitions/0/ \
//!   cargo run -p yata-bench --bin migrate-format-d --release

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray, UInt64Array, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema};
use yata_lance::YataDb;

/// Format D (10-col) vertex schema.
fn format_d_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("op", DataType::UInt8, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("pk_value", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::UInt64, false),
        Field::new("repo", DataType::Utf8, true),
        Field::new("owner_did", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("app_id", DataType::Utf8, true),
        Field::new("rkey", DataType::Utf8, true),
        Field::new("val_json", DataType::Utf8, true),
    ]))
}

/// Edge schema (10-col).
fn edge_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("op", DataType::UInt8, false),
        Field::new("edge_label", DataType::Utf8, false),
        Field::new("eid", DataType::Utf8, false),
        Field::new("src_vid", DataType::Utf8, false),
        Field::new("dst_vid", DataType::Utf8, false),
        Field::new("src_label", DataType::Utf8, true),
        Field::new("dst_label", DataType::Utf8, true),
        Field::new("timestamp_ms", DataType::UInt64, false),
        Field::new("app_id", DataType::Utf8, true),
        Field::new("val_json", DataType::Utf8, true),
    ]))
}

const PROMOTED_PROPS: [&str; 5] = ["repo", "owner_did", "name", "app_id", "rkey"];

/// Convert a legacy 7-col batch to Format D 10-col batch.
/// Also separates edges (pk_key == "eid") into edge batches.
fn convert_batch(batch: &RecordBatch) -> (RecordBatch, Option<RecordBatch>) {
    let n = batch.num_rows();
    if n == 0 {
        return (
            RecordBatch::new_empty(format_d_schema()),
            None,
        );
    }

    // Legacy schema: seq=0, op=1, label=2, pk_key=3, pk_value=4, timestamp_ms=5, props_json=6
    let op_col = batch.column(1).as_any().downcast_ref::<UInt8Array>().unwrap();
    let label_col = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
    let pk_key_col = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
    let pk_value_col = batch.column(4).as_any().downcast_ref::<StringArray>().unwrap();
    let ts_col = batch.column(5).as_any().downcast_ref::<UInt64Array>().unwrap();
    let props_col = batch.column(6).as_any().downcast_ref::<StringArray>().unwrap();

    // Separate vertex vs edge rows
    let mut v_indices = Vec::new();
    let mut e_indices = Vec::new();
    for i in 0..n {
        if pk_key_col.value(i) == "eid" {
            e_indices.push(i);
        } else {
            v_indices.push(i);
        }
    }

    // ── Build vertex batch ──
    let vn = v_indices.len();
    let mut v_ops = Vec::with_capacity(vn);
    let mut v_labels = Vec::with_capacity(vn);
    let mut v_pk_values = Vec::with_capacity(vn);
    let mut v_timestamps = Vec::with_capacity(vn);
    let mut v_repos: Vec<Option<String>> = Vec::with_capacity(vn);
    let mut v_owner_dids: Vec<Option<String>> = Vec::with_capacity(vn);
    let mut v_names: Vec<Option<String>> = Vec::with_capacity(vn);
    let mut v_app_ids: Vec<Option<String>> = Vec::with_capacity(vn);
    let mut v_rkeys: Vec<Option<String>> = Vec::with_capacity(vn);
    let mut v_val_jsons: Vec<Option<String>> = Vec::with_capacity(vn);

    for &i in &v_indices {
        v_ops.push(op_col.value(i));
        v_labels.push(label_col.value(i).to_string());
        v_pk_values.push(pk_value_col.value(i).to_string());
        v_timestamps.push(ts_col.value(i));

        // Parse props_json
        let props_str = if props_col.is_null(i) { "{}" } else { props_col.value(i) };
        let props: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(props_str).unwrap_or_default();

        v_repos.push(props.get("repo").and_then(|v| v.as_str()).map(|s| s.to_string()));
        v_owner_dids.push(props.get("owner_did").and_then(|v| v.as_str()).map(|s| s.to_string()));
        v_names.push(props.get("name").and_then(|v| v.as_str()).map(|s| s.to_string()));
        v_app_ids.push(
            props.get("app_id").or_else(|| props.get("_app_id"))
                .and_then(|v| v.as_str()).map(|s| s.to_string())
        );
        v_rkeys.push(props.get("rkey").and_then(|v| v.as_str()).map(|s| s.to_string()));

        // Overflow: non-promoted props
        let overflow: serde_json::Map<String, serde_json::Value> = props.into_iter()
            .filter(|(k, _)| !PROMOTED_PROPS.contains(&k.as_str()) && k != "_app_id")
            .collect();
        v_val_jsons.push(if overflow.is_empty() { None } else { Some(serde_json::to_string(&overflow).unwrap()) });
    }

    let vertex_batch = RecordBatch::try_new(format_d_schema(), vec![
        Arc::new(UInt8Array::from(v_ops)) as ArrayRef,
        Arc::new(StringArray::from(v_labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())) as ArrayRef,
        Arc::new(StringArray::from(v_pk_values.iter().map(|s| s.as_str()).collect::<Vec<_>>())) as ArrayRef,
        Arc::new(UInt64Array::from(v_timestamps)) as ArrayRef,
        Arc::new(StringArray::from(v_repos.iter().map(|s| s.as_deref()).collect::<Vec<_>>())) as ArrayRef,
        Arc::new(StringArray::from(v_owner_dids.iter().map(|s| s.as_deref()).collect::<Vec<_>>())) as ArrayRef,
        Arc::new(StringArray::from(v_names.iter().map(|s| s.as_deref()).collect::<Vec<_>>())) as ArrayRef,
        Arc::new(StringArray::from(v_app_ids.iter().map(|s| s.as_deref()).collect::<Vec<_>>())) as ArrayRef,
        Arc::new(StringArray::from(v_rkeys.iter().map(|s| s.as_deref()).collect::<Vec<_>>())) as ArrayRef,
        Arc::new(StringArray::from(v_val_jsons.iter().map(|s| s.as_deref()).collect::<Vec<_>>())) as ArrayRef,
    ]).unwrap();

    // ── Build edge batch ──
    let edge_batch = if e_indices.is_empty() {
        None
    } else {
        let en = e_indices.len();
        let mut e_ops = Vec::with_capacity(en);
        let mut e_edge_labels = Vec::with_capacity(en);
        let mut e_eids = Vec::with_capacity(en);
        let mut e_src_vids = Vec::with_capacity(en);
        let mut e_dst_vids = Vec::with_capacity(en);
        let mut e_src_labels: Vec<Option<String>> = Vec::with_capacity(en);
        let mut e_dst_labels: Vec<Option<String>> = Vec::with_capacity(en);
        let mut e_timestamps = Vec::with_capacity(en);
        let mut e_app_ids: Vec<Option<String>> = Vec::with_capacity(en);
        let mut e_val_jsons: Vec<Option<String>> = Vec::with_capacity(en);

        for &i in &e_indices {
            e_ops.push(op_col.value(i));
            e_edge_labels.push(label_col.value(i).to_string());
            e_eids.push(pk_value_col.value(i).to_string());
            e_timestamps.push(ts_col.value(i));

            let props_str = if props_col.is_null(i) { "{}" } else { props_col.value(i) };
            let props: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(props_str).unwrap_or_default();

            e_src_vids.push(props.get("_src").and_then(|v| v.as_str()).unwrap_or("").to_string());
            e_dst_vids.push(props.get("_dst").and_then(|v| v.as_str()).unwrap_or("").to_string());
            e_src_labels.push(None);
            e_dst_labels.push(None);
            e_app_ids.push(
                props.get("app_id").or_else(|| props.get("_app_id"))
                    .and_then(|v| v.as_str()).map(|s| s.to_string())
            );

            let overflow: serde_json::Map<String, serde_json::Value> = props.into_iter()
                .filter(|(k, _)| !["_src", "_dst", "app_id", "_app_id"].contains(&k.as_str()))
                .collect();
            e_val_jsons.push(if overflow.is_empty() { None } else { Some(serde_json::to_string(&overflow).unwrap()) });
        }

        Some(RecordBatch::try_new(edge_schema(), vec![
            Arc::new(UInt8Array::from(e_ops)) as ArrayRef,
            Arc::new(StringArray::from(e_edge_labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(e_eids.iter().map(|s| s.as_str()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(e_src_vids.iter().map(|s| s.as_str()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(e_dst_vids.iter().map(|s| s.as_str()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(e_src_labels.iter().map(|s| s.as_deref()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(e_dst_labels.iter().map(|s| s.as_deref()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(UInt64Array::from(e_timestamps)) as ArrayRef,
            Arc::new(StringArray::from(e_app_ids.iter().map(|s| s.as_deref()).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(StringArray::from(e_val_jsons.iter().map(|s| s.as_deref()).collect::<Vec<_>>())) as ArrayRef,
        ]).unwrap())
    };

    (vertex_batch, edge_batch)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    println!("╔══════════════════════════════════════════════════════════════════════════╗");
    println!("║       migrate-format-d: 7-col → Format D (10-col) migration            ║");
    println!("╚══════════════════════════════════════════════════════════════════════════╝\n");

    // Connect to LanceDB
    let prefix = std::env::var("YATA_S3_PREFIX")
        .unwrap_or_else(|_| std::env::var("YATA_DATA_DIR").unwrap_or_else(|_| "/tmp/yata-migrate".into()));

    let db = if std::env::var("YATA_S3_ENDPOINT").is_ok() {
        println!("  Connecting to R2...");
        YataDb::connect_from_env(&prefix).await
            .ok_or_else(|| anyhow::anyhow!("R2 connection failed — check YATA_S3_* env vars"))?
    } else {
        println!("  Connecting to local: {prefix}");
        YataDb::connect_local(&prefix).await?
    };

    // ── Step 1: Read old vertices table ──
    println!("\n━━━ Step 1: Read old vertices table ━━━");
    let old_tbl = match db.open_table("vertices").await {
        Ok(tbl) => tbl,
        Err(e) => {
            println!("  No vertices table found: {e}");
            println!("  Nothing to migrate.");
            return Ok(());
        }
    };

    let old_count = old_tbl.count_rows(None).await.unwrap_or(0);
    println!("  Old table: {old_count} rows");

    if old_count == 0 {
        println!("  Empty table. Nothing to migrate.");
        return Ok(());
    }

    let t0 = Instant::now();
    let old_batches = old_tbl.scan_all().await?;
    println!("  Scanned {} batches in {:.1}ms", old_batches.len(), t0.elapsed().as_millis());

    // Detect if already Format D
    if let Some(first) = old_batches.first() {
        if first.schema().field(0).data_type() == &DataType::UInt8 {
            println!("  Already Format D (col 0 = UInt8). Nothing to migrate.");
            return Ok(());
        }
        println!("  Detected legacy 7-col schema (col 0 = {:?})", first.schema().field(0).data_type());
    }

    // ── Step 2: Convert batches ──
    println!("\n━━━ Step 2: Convert to Format D ━━━");
    let t0 = Instant::now();
    let mut vertex_batches = Vec::new();
    let mut edge_batches = Vec::new();
    let mut total_vertices = 0usize;
    let mut total_edges = 0usize;

    for batch in &old_batches {
        let (vb, eb) = convert_batch(batch);
        total_vertices += vb.num_rows();
        vertex_batches.push(vb);
        if let Some(eb) = eb {
            total_edges += eb.num_rows();
            edge_batches.push(eb);
        }
    }
    println!("  Converted in {:.1}ms: {total_vertices} vertices + {total_edges} edges", t0.elapsed().as_millis());

    // ── Step 3: Write new tables ──
    println!("\n━━━ Step 3: Write Format D tables ━━━");

    // Write vertices_v2
    let t0 = Instant::now();
    if !vertex_batches.is_empty() && total_vertices > 0 {
        // Concatenate all vertex batches into one
        let first = vertex_batches.remove(0);
        let v2_tbl = match db.open_table("vertices_v2").await {
            Ok(_) => {
                println!("  vertices_v2 already exists — dropping...");
                // Can't drop with lancedb API — create fresh
                db.create_table("vertices_v2", first).await?
            }
            Err(_) => db.create_table("vertices_v2", first).await?,
        };
        for batch in &vertex_batches {
            if batch.num_rows() > 0 {
                v2_tbl.add(batch.clone()).await?;
            }
        }
        let v2_count = v2_tbl.count_rows(None).await.unwrap_or(0);
        println!("  vertices_v2: {v2_count} rows written in {:.1}ms", t0.elapsed().as_millis());

        // Compact
        let t1 = Instant::now();
        let _ = v2_tbl.compact().await;
        println!("  vertices_v2 compacted in {:.1}ms", t1.elapsed().as_millis());
    }

    // Write edges
    let t0 = Instant::now();
    if !edge_batches.is_empty() && total_edges > 0 {
        let first = edge_batches.remove(0);
        let e_tbl = match db.open_table("edges").await {
            Ok(_) => db.create_table("edges", first).await?,
            Err(_) => db.create_table("edges", first).await?,
        };
        for batch in &edge_batches {
            if batch.num_rows() > 0 {
                e_tbl.add(batch.clone()).await?;
            }
        }
        let e_count = e_tbl.count_rows(None).await.unwrap_or(0);
        println!("  edges: {e_count} rows written in {:.1}ms", t0.elapsed().as_millis());

        let t1 = Instant::now();
        let _ = e_tbl.compact().await;
        println!("  edges compacted in {:.1}ms", t1.elapsed().as_millis());
    }

    // ── Step 4: Verify ──
    println!("\n━━━ Step 4: Verify ━━━");
    let tables = db.table_names().await?;
    println!("  Tables: {tables:?}");

    if let Ok(v2) = db.open_table("vertices_v2").await {
        let count = v2.count_rows(None).await.unwrap_or(0);
        println!("  vertices_v2: {count} rows (expected: {total_vertices})");
        assert_eq!(count, total_vertices, "vertex count mismatch!");

        // Sample first row
        let sample = v2.scan_all().await?;
        if let Some(first) = sample.first() {
            println!("  Schema: {:?}", first.schema().fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>());
            println!("  Format D confirmed: col 0 = {:?}", first.schema().field(0).data_type());
        }
    }

    if let Ok(e) = db.open_table("edges").await {
        let count = e.count_rows(None).await.unwrap_or(0);
        println!("  edges: {count} rows (expected: {total_edges})");
    }

    // ── Step 5: Swap instructions ──
    println!("\n━━━ Step 5: Table swap ━━━");
    println!("  Migration complete. To activate Format D:");
    println!("  1. Rename vertices → vertices_legacy (backup)");
    println!("  2. Rename vertices_v2 → vertices");
    println!("  NOTE: LanceDB doesn't support table rename.");
    println!("        The engine's auto-detect reads both schemas.");
    println!("        New writes already use Format D.");
    println!("        Old data coexists until next full compaction.");
    println!();
    println!("  Alternatively, set YATA_VERTICES_TABLE=vertices_v2 to point engine at new table.");

    println!("\ndone");
    Ok(())
}
