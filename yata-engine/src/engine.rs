use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use yata_grin::{Predicate, PropValue, Property, Scannable, Topology};

use crate::config::TieredEngineConfig;
use crate::router;

/// Compaction result (LanceDB-native).
#[derive(Debug, Clone, serde::Serialize)]
pub struct CompactionResult {
    pub max_seq: u64,
    pub input_entries: usize,
    pub output_entries: usize,
    pub labels: Vec<String>,
}

/// CPM metrics: CP5 mutation frequency + read/write ratio + compaction monitoring.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CpmStats {
    pub cypher_read_count: u64,
    pub cypher_mutation_count: u64,
    pub cypher_mutation_avg_us: u64,
    pub cypher_mutation_us_total: u64,
    pub merge_record_count: u64,
    pub mutation_ratio: f64,
    pub vertex_count: u64,
    pub edge_count: u64,
    pub last_compaction_ms: u64,
}

/// Shared tokio runtime for all engine instances (avoids nested runtime issues).
pub(crate) static ENGINE_RT: std::sync::LazyLock<tokio::runtime::Runtime> = std::sync::LazyLock::new(|| {
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .thread_name("yata-engine")
        .build()
        .expect("yata-engine tokio runtime")
});

/// FNV-1a 32-bit hash (fast, non-cryptographic — for owner_hash property matching).
fn fnv1a_32(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

static ID_COUNTER: AtomicU64 = AtomicU64::new(0);

fn generate_node_id() -> String {
    let seq = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("n_{}_{}", now_ms(), seq)
}

fn generate_edge_id() -> String {
    let seq = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("e_{}_{}", now_ms(), seq)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn inject_metadata(props: &mut Vec<(String, yata_grin::PropValue)>, ctx: Option<&MutationContext>, now: &str) {
    if let Some(ctx) = ctx {
        if !ctx.app_id.is_empty() { props.push(("_app_id".into(), yata_grin::PropValue::Str(ctx.app_id.clone()))); }
        if !ctx.org_id.is_empty() { props.push(("_org_id".into(), yata_grin::PropValue::Str(ctx.org_id.clone()))); }
        if !ctx.user_did.is_empty() { props.push(("_user_did".into(), yata_grin::PropValue::Str(ctx.user_did.clone()))); }
        props.push(("_updated_at".into(), yata_grin::PropValue::Str(now.to_string())));
    }
}

/// Canonical Lance table schema for vertices (Format D: 10-col typed).
///
/// Core 6 properties promoted to columns for Lance pushdown.
/// `val_json` holds overflow domain properties only (not used for filter).
fn lance_schema() -> Arc<arrow::datatypes::Schema> {
    use arrow::datatypes::{DataType, Field, Schema};
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

/// Lance table schema for edges (separate table).
///
/// `src_vid`/`dst_vid` as first-class columns enable Lance pushdown for traversal.
fn edge_lance_schema() -> Arc<arrow::datatypes::Schema> {
    use arrow::datatypes::{DataType, Field, Schema};
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

async fn ensure_lance_scalar_indices(
    vertices: Option<&yata_lance::YataTable>,
    edges: Option<&yata_lance::YataTable>,
) {
    use yata_lance::dataset::ScalarIndexKind;

    if let Some(vertices) = vertices {
        for (column, kind, name) in [
            ("pk_value", ScalarIndexKind::BTree, "vertices_pk_value_btree"),
            ("label", ScalarIndexKind::Bitmap, "vertices_label_bitmap"),
            ("repo", ScalarIndexKind::Bitmap, "vertices_repo_bitmap"),
            ("owner_did", ScalarIndexKind::Bitmap, "vertices_owner_did_bitmap"),
            ("app_id", ScalarIndexKind::Bitmap, "vertices_app_id_bitmap"),
            ("rkey", ScalarIndexKind::BTree, "vertices_rkey_btree"),
        ] {
            if let Err(error) = vertices.ensure_scalar_index(column, kind, name).await {
                tracing::warn!(table = "vertices", column, index_name = name, error = %error, "failed to ensure Lance scalar index");
            }
        }
    }
    if let Some(edges) = edges {
        for (column, kind, name) in [
            ("eid", ScalarIndexKind::BTree, "edges_eid_btree"),
            ("src_vid", ScalarIndexKind::BTree, "edges_src_vid_btree"),
            ("dst_vid", ScalarIndexKind::BTree, "edges_dst_vid_btree"),
            ("edge_label", ScalarIndexKind::Bitmap, "edges_edge_label_bitmap"),
            ("src_label", ScalarIndexKind::Bitmap, "edges_src_label_bitmap"),
            ("dst_label", ScalarIndexKind::Bitmap, "edges_dst_label_bitmap"),
            ("app_id", ScalarIndexKind::Bitmap, "edges_app_id_bitmap"),
        ] {
            if let Err(error) = edges.ensure_scalar_index(column, kind, name).await {
                tracing::warn!(table = "edges", column, index_name = name, error = %error, "failed to ensure Lance scalar index");
            }
        }
    }
}

/// Persistent stats catalog schema for planner cold-start hydration.
fn stats_catalog_schema() -> Arc<arrow::datatypes::Schema> {
    use arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("kind", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("raw_rows", DataType::UInt64, false),
        Field::new("live_rows", DataType::UInt64, false),
        Field::new("dead_rows", DataType::UInt64, false),
        Field::new("dead_ratio", DataType::Float64, false),
        Field::new("timestamp_ms", DataType::UInt64, false),
    ]))
}

/// Promoted vertex property names (columns 4-8 in lance_schema).
const PROMOTED_PROPS: [&str; 5] = ["repo", "owner_did", "name", "app_id", "rkey"];


/// Extract a promoted property value as string from props slice.
fn extract_prop_str<'a>(props: &'a [(&str, yata_grin::PropValue)], key: &str) -> Option<&'a str> {
    props.iter().find_map(|(k, v)| {
        if *k == key {
            match v {
                yata_grin::PropValue::Str(s) => Some(s.as_str()),
                _ => None,
            }
        } else {
            None
        }
    })
}

/// Serialize a PropValue to serde_json::Value.
fn prop_to_json(v: &yata_grin::PropValue) -> serde_json::Value {
    match v {
        yata_grin::PropValue::Str(s) => serde_json::Value::String(s.clone()),
        yata_grin::PropValue::Int(i) => serde_json::json!(*i),
        yata_grin::PropValue::Float(f) => serde_json::json!(*f),
        yata_grin::PropValue::Bool(b) => serde_json::Value::Bool(*b),
        yata_grin::PropValue::Binary(b) => {
            use base64::Engine;
            serde_json::Value::String(format!("b64:{}", base64::engine::general_purpose::STANDARD.encode(b)))
        }
        yata_grin::PropValue::Null => serde_json::Value::Null,
    }
}

/// Build a single-row vertex Arrow RecordBatch (Format D: 10-col).
///
/// Promoted properties (repo, owner_did, name, app_id, rkey) go to dedicated columns.
/// Remaining properties go to `val_json` overflow.
fn build_lance_batch(
    op: u8,
    label: &str,
    _pk_key: &str,
    pk_value: &str,
    props: &[(&str, yata_grin::PropValue)],
) -> Result<arrow::array::RecordBatch, String> {
    use arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array, UInt8Array};

    let repo = extract_prop_str(props, "repo");
    let owner_did = extract_prop_str(props, "owner_did");
    let name = extract_prop_str(props, "name");
    let app_id = extract_prop_str(props, "app_id").or_else(|| extract_prop_str(props, "_app_id"));
    let rkey = extract_prop_str(props, "rkey");

    // Overflow: non-promoted properties → val_json
    let overflow: serde_json::Map<String, serde_json::Value> = props
        .iter()
        .filter(|(k, _)| !PROMOTED_PROPS.contains(k) && *k != "_app_id")
        .map(|(k, v)| (k.to_string(), prop_to_json(v)))
        .collect();
    let val_json = if overflow.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&overflow).unwrap_or_default())
    };

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt8Array::from(vec![op])),
        Arc::new(StringArray::from(vec![label])),
        Arc::new(StringArray::from(vec![pk_value])),
        Arc::new(UInt64Array::from(vec![now_ms()])),
        Arc::new(StringArray::from(vec![repo])) as ArrayRef,
        Arc::new(StringArray::from(vec![owner_did])) as ArrayRef,
        Arc::new(StringArray::from(vec![name])) as ArrayRef,
        Arc::new(StringArray::from(vec![app_id])) as ArrayRef,
        Arc::new(StringArray::from(vec![rkey])) as ArrayRef,
        Arc::new(StringArray::from(vec![val_json.as_deref()])) as ArrayRef,
    ];

    RecordBatch::try_new(lance_schema(), columns)
        .map_err(|e| format!("Arrow RecordBatch build failed: {e}"))
}

/// Build a single-row edge Arrow RecordBatch (10-col).
fn build_edge_lance_batch(
    op: u8,
    edge_label: &str,
    eid: &str,
    src_vid: &str,
    dst_vid: &str,
    src_label: Option<&str>,
    dst_label: Option<&str>,
    props: &[(&str, yata_grin::PropValue)],
) -> Result<arrow::array::RecordBatch, String> {
    use arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array, UInt8Array};

    let app_id = extract_prop_str(props, "app_id").or_else(|| extract_prop_str(props, "_app_id"));

    // Edge overflow: all non-structural props
    let overflow: serde_json::Map<String, serde_json::Value> = props
        .iter()
        .filter(|(k, _)| !["app_id", "_app_id", "_src", "_dst", "_src_label", "_dst_label"].contains(k))
        .map(|(k, v)| (k.to_string(), prop_to_json(v)))
        .collect();
    let val_json = if overflow.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&overflow).unwrap_or_default())
    };

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt8Array::from(vec![op])),
        Arc::new(StringArray::from(vec![edge_label])),
        Arc::new(StringArray::from(vec![eid])),
        Arc::new(StringArray::from(vec![src_vid])),
        Arc::new(StringArray::from(vec![dst_vid])),
        Arc::new(StringArray::from(vec![src_label])) as ArrayRef,
        Arc::new(StringArray::from(vec![dst_label])) as ArrayRef,
        Arc::new(UInt64Array::from(vec![now_ms()])),
        Arc::new(StringArray::from(vec![app_id])) as ArrayRef,
        Arc::new(StringArray::from(vec![val_json.as_deref()])) as ArrayRef,
    ];

    RecordBatch::try_new(edge_lance_schema(), columns)
        .map_err(|e| format!("Arrow edge RecordBatch build failed: {e}"))
}

fn build_stats_catalog_batch(
    kind: &str,
    key: &str,
    stat: &CachedPressureStat,
) -> Result<arrow::array::RecordBatch, String> {
    use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray, UInt64Array};

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![kind])),
        Arc::new(StringArray::from(vec![key])),
        Arc::new(UInt64Array::from(vec![stat.raw_rows as u64])),
        Arc::new(UInt64Array::from(vec![stat.live_rows as u64])),
        Arc::new(UInt64Array::from(vec![stat.dead_rows as u64])),
        Arc::new(Float64Array::from(vec![stat.dead_ratio])),
        Arc::new(UInt64Array::from(vec![now_ms()])),
    ];

    RecordBatch::try_new(stats_catalog_schema(), columns)
        .map_err(|e| format!("Arrow stats catalog RecordBatch build failed: {e}"))
}



/// Mutation context: metadata auto-injected into every mutated node.
#[derive(Debug, Clone, Default)]
pub struct MutationContext {
    pub app_id: String,
    pub org_id: String,
    pub user_id: String,
    pub actor_id: String,
    /// DID-based user identity (federation-ready).
    pub user_did: String,
    /// DID-based actor identity (federation-ready).
    pub actor_did: String,
}

const AUTO_COMPACT_FRAGMENT_THRESHOLD: u64 = 8;
const EDGE_SRC_VID_PUSHDOWN_LIMIT: usize = 256;
const EDGE_DST_VID_PUSHDOWN_LIMIT: usize = 256;
const EDGE_SRC_SCAN_LIMIT: usize = 256;
const EDGE_DST_SCAN_LIMIT: usize = 256;
const PATH_EXPAND_FRONTIER_LIMIT: usize = 256;
const COUNT_STATS_CACHE_MAX: usize = 512;
const COUNT_STATS_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(30);
const PRESSURE_STATS_CACHE_MAX: usize = 64;
const PRESSURE_STATS_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(30);
const COMPACTION_DEAD_ROWS_THRESHOLD: usize = 64;
const COMPACTION_DEAD_RATIO_THRESHOLD: f64 = 0.20;

#[derive(Debug, Clone)]
struct StagedPathRecord {
    bindings: HashMap<String, String>,
    values: Vec<PropValue>,
}

#[derive(Debug, Clone)]
struct CachedCountStat {
    count: usize,
    at: std::time::Instant,
}

#[derive(Debug, Clone)]
struct CachedPressureStat {
    raw_rows: usize,
    live_rows: usize,
    dead_rows: usize,
    dead_ratio: f64,
    at: std::time::Instant,
}

#[derive(Debug, Clone)]
struct PlannerLabelStatsView {
    label: Option<String>,
    live_rows: usize,
    dead_ratio: f64,
    last_compacted_at_ms: u64,
}

#[derive(Debug, Clone)]
struct PlannerTraversalStatsView {
    source: PlannerLabelStatsView,
    edge: PlannerLabelStatsView,
    avg_out_degree: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableKind {
    Vertices,
    Edges,
}

fn runtime_allows_staged_traversal(current_rows: usize, limit: Option<usize>) -> bool {
    if current_rows == 0 || current_rows > PATH_EXPAND_FRONTIER_LIMIT {
        return false;
    }
    let limit_ok = limit.is_none_or(|l| current_rows <= l.saturating_mul(4).max(32));
    limit_ok
}

fn should_stage_traversal(
    op: &yata_gie::ir::LogicalOp,
    current_rows: usize,
    limit: Option<usize>,
) -> bool {
    use yata_gie::ir::TraversalStrategy;

    let strategy = match op {
        yata_gie::ir::LogicalOp::Expand {
            direction,
            strategy,
            ..
        }
        | yata_gie::ir::LogicalOp::PathExpand {
            direction,
            strategy,
            ..
        } => {
            if matches!(direction, yata_grin::Direction::Both) {
                return false;
            }
            *strategy
        }
        _ => return false,
    };

    match strategy {
        TraversalStrategy::PreferGie => false,
        TraversalStrategy::PreferStaged | TraversalStrategy::Auto => {
            runtime_allows_staged_traversal(current_rows, limit)
        }
    }
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn strip_explain_prefix(cypher: &str) -> Option<&str> {
    let trimmed = cypher.trim_start();
    let upper = trimmed.to_uppercase();
    if upper.starts_with("EXPLAIN ") {
        Some(trimmed[7..].trim_start())
    } else {
        None
    }
}

fn expr_to_sql_literal(expr: &yata_cypher::ast::Expr, params: &HashMap<String, String>) -> Option<String> {
    match expr {
        yata_cypher::ast::Expr::Lit(yata_cypher::ast::Literal::Str(s)) => Some(sql_quote(s)),
        yata_cypher::ast::Expr::Lit(yata_cypher::ast::Literal::Int(i)) => Some(i.to_string()),
        yata_cypher::ast::Expr::Lit(yata_cypher::ast::Literal::Float(f)) => Some(f.to_string()),
        yata_cypher::ast::Expr::Lit(yata_cypher::ast::Literal::Bool(b)) => Some(b.to_string()),
        yata_cypher::ast::Expr::Param(name) => params.get(name).map(|v| sql_quote(v)),
        _ => None,
    }
}

fn build_vertex_pushdown_filter(
    node: &yata_cypher::ast::NodePattern,
    params: &HashMap<String, String>,
) -> Option<String> {
    let promoted = ["repo", "owner_did", "name", "app_id", "rkey"];
    let mut parts = Vec::new();
    if let Some(label) = node.labels.first() {
        parts.push(format!("label = {}", sql_quote(label)));
    }
    for (key, expr) in &node.props {
        if !promoted.contains(&key.as_str()) {
            continue;
        }
        let literal = expr_to_sql_literal(expr, params)?;
        parts.push(format!("{key} = {literal}"));
    }
    if parts.len() <= 1 {
        return None;
    }
    Some(parts.join(" AND "))
}

fn edge_endpoint_labels(ast: &yata_cypher::ast::Query) -> (Vec<String>, Vec<String>) {
    let mut src_labels = Vec::new();
    let mut dst_labels = Vec::new();

    let mut collect_from_patterns = |patterns: &[yata_cypher::ast::Pattern]| {
        for pattern in patterns {
            for window in pattern.elements.windows(3) {
                let [yata_cypher::ast::PatternElement::Node(src), yata_cypher::ast::PatternElement::Rel(_), yata_cypher::ast::PatternElement::Node(dst)] = window else {
                    continue;
                };
                if let Some(label) = src.labels.first() {
                    src_labels.push(label.clone());
                }
                if let Some(label) = dst.labels.first() {
                    dst_labels.push(label.clone());
                }
            }
        }
    };

    for clause in &ast.clauses {
        match clause {
            yata_cypher::ast::Clause::Match { patterns, .. }
            | yata_cypher::ast::Clause::OptionalMatch { patterns, .. }
            | yata_cypher::ast::Clause::Create { patterns } => collect_from_patterns(patterns),
            yata_cypher::ast::Clause::Merge { pattern, .. } => collect_from_patterns(std::slice::from_ref(pattern)),
            _ => {}
        }
    }

    src_labels.sort();
    src_labels.dedup();
    dst_labels.sort();
    dst_labels.dedup();
    (src_labels, dst_labels)
}

fn edge_source_patterns<'a>(ast: &'a yata_cypher::ast::Query) -> Vec<&'a yata_cypher::ast::NodePattern> {
    let mut nodes = Vec::new();
    let mut collect_from_patterns = |patterns: &'a [yata_cypher::ast::Pattern]| {
        for pattern in patterns {
            for window in pattern.elements.windows(3) {
                let [yata_cypher::ast::PatternElement::Node(src), yata_cypher::ast::PatternElement::Rel(_), yata_cypher::ast::PatternElement::Node(_)] = window else {
                    continue;
                };
                nodes.push(src);
            }
        }
    };

    for clause in &ast.clauses {
        match clause {
            yata_cypher::ast::Clause::Match { patterns, .. }
            | yata_cypher::ast::Clause::OptionalMatch { patterns, .. }
            | yata_cypher::ast::Clause::Create { patterns } => collect_from_patterns(patterns),
            yata_cypher::ast::Clause::Merge { pattern, .. } => collect_from_patterns(std::slice::from_ref(pattern)),
            _ => {}
        }
    }
    nodes
}

fn edge_destination_patterns<'a>(ast: &'a yata_cypher::ast::Query) -> Vec<&'a yata_cypher::ast::NodePattern> {
    let mut nodes = Vec::new();
    let mut collect_from_patterns = |patterns: &'a [yata_cypher::ast::Pattern]| {
        for pattern in patterns {
            for window in pattern.elements.windows(3) {
                let [yata_cypher::ast::PatternElement::Node(_), yata_cypher::ast::PatternElement::Rel(_), yata_cypher::ast::PatternElement::Node(dst)] = window else {
                    continue;
                };
                nodes.push(dst);
            }
        }
    };

    for clause in &ast.clauses {
        match clause {
            yata_cypher::ast::Clause::Match { patterns, .. }
            | yata_cypher::ast::Clause::OptionalMatch { patterns, .. }
            | yata_cypher::ast::Clause::Create { patterns } => collect_from_patterns(patterns),
            yata_cypher::ast::Clause::Merge { pattern, .. } => collect_from_patterns(std::slice::from_ref(pattern)),
            _ => {}
        }
    }
    nodes
}

fn reverse_sql_op(op: &yata_cypher::ast::BinOp) -> Option<&'static str> {
    match op {
        yata_cypher::ast::BinOp::Eq => Some("="),
        yata_cypher::ast::BinOp::Neq => Some("!="),
        yata_cypher::ast::BinOp::Lt => Some(">"),
        yata_cypher::ast::BinOp::Lte => Some(">="),
        yata_cypher::ast::BinOp::Gt => Some("<"),
        yata_cypher::ast::BinOp::Gte => Some("<="),
        _ => None,
    }
}

fn sql_op(op: &yata_cypher::ast::BinOp) -> Option<&'static str> {
    match op {
        yata_cypher::ast::BinOp::Eq => Some("="),
        yata_cypher::ast::BinOp::Neq => Some("!="),
        yata_cypher::ast::BinOp::Lt => Some("<"),
        yata_cypher::ast::BinOp::Lte => Some("<="),
        yata_cypher::ast::BinOp::Gt => Some(">"),
        yata_cypher::ast::BinOp::Gte => Some(">="),
        _ => None,
    }
}

fn collect_var_where_filters(
    expr: &yata_cypher::ast::Expr,
    vars: &std::collections::HashSet<String>,
    params: &HashMap<String, String>,
    out: &mut HashMap<String, Vec<String>>,
) {
    use yata_cypher::ast::{BinOp, Expr};
    match expr {
        Expr::BinOp(BinOp::And, lhs, rhs) => {
            collect_var_where_filters(lhs, vars, params, out);
            collect_var_where_filters(rhs, vars, params, out);
        }
        Expr::BinOp(op, lhs, rhs) => {
            let promoted = ["repo", "owner_did", "name", "app_id", "rkey"];
            if let Expr::Prop(base, key) = lhs.as_ref() {
                if let Expr::Var(var) = base.as_ref() {
                    if vars.contains(var) && promoted.contains(&key.as_str()) {
                        if let (Some(op_str), Some(lit)) = (sql_op(op), expr_to_sql_literal(rhs, params)) {
                            out.entry(var.clone()).or_default().push(format!("{key} {op_str} {lit}"));
                        }
                    }
                }
            }
            if let Expr::Prop(base, key) = rhs.as_ref() {
                if let Expr::Var(var) = base.as_ref() {
                    if vars.contains(var) && promoted.contains(&key.as_str()) {
                        if let (Some(op_str), Some(lit)) = (reverse_sql_op(op), expr_to_sql_literal(lhs, params)) {
                            out.entry(var.clone()).or_default().push(format!("{key} {op_str} {lit}"));
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

#[derive(Debug, Clone)]
struct NodePushdownContext {
    filter: String,
    label: Option<String>,
}

fn collect_node_pushdown_filters(
    ast: &yata_cypher::ast::Query,
    params: &HashMap<String, String>,
) -> HashMap<String, NodePushdownContext> {
    let mut filters = HashMap::new();
    let mut nodes_by_var: HashMap<String, yata_cypher::ast::NodePattern> = HashMap::new();
    let mut collect_from_patterns = |patterns: &[yata_cypher::ast::Pattern]| {
        for pattern in patterns {
            for element in &pattern.elements {
                let yata_cypher::ast::PatternElement::Node(node) = element else {
                    continue;
                };
                let Some(var) = node.var.clone() else {
                    continue;
                };
                nodes_by_var.entry(var).or_insert_with(|| node.clone());
            }
        }
    };

    for clause in &ast.clauses {
        match clause {
            yata_cypher::ast::Clause::Match { patterns, .. }
            | yata_cypher::ast::Clause::OptionalMatch { patterns, .. }
            | yata_cypher::ast::Clause::Create { patterns } => collect_from_patterns(patterns),
            yata_cypher::ast::Clause::Merge { pattern, .. } => {
                collect_from_patterns(std::slice::from_ref(pattern))
            }
            _ => {}
        }
    }

    let node_vars: std::collections::HashSet<String> = nodes_by_var.keys().cloned().collect();
    let mut where_filters_by_var: HashMap<String, Vec<String>> = HashMap::new();
    for clause in &ast.clauses {
        match clause {
            yata_cypher::ast::Clause::Match { where_, .. }
            | yata_cypher::ast::Clause::OptionalMatch { where_, .. } => {
                if let Some(expr) = where_ {
                    collect_var_where_filters(expr, &node_vars, params, &mut where_filters_by_var);
                }
            }
            _ => {}
        }
    }

    for (var, node) in nodes_by_var {
        let mut parts = Vec::new();
        if let Some(base) = build_vertex_pushdown_filter(&node, params) {
            parts.push(base);
        } else if let Some(label) = node.labels.first() {
            parts.push(format!("label = {}", sql_quote(label)));
        }
        if let Some(extra) = where_filters_by_var.get(&var) {
            parts.extend(extra.iter().cloned());
        }
        if !parts.is_empty() {
            filters.insert(var, NodePushdownContext {
                filter: parts.join(" AND "),
                label: node.labels.first().cloned(),
            });
        }
    }

    filters
}

/// Graph engine: LanceDB-backed. No persistent CSR. Query = LanceDB scan → ephemeral CSR → GIE.
pub struct TieredGraphEngine {
    config: TieredEngineConfig,
    lance_db: Arc<tokio::sync::Mutex<Option<yata_lance::YataDb>>>,
    lance_table: Arc<tokio::sync::Mutex<Option<yata_lance::YataTable>>>,
    /// Separate edge table (P1).
    lance_edge_table: Arc<tokio::sync::Mutex<Option<yata_lance::YataTable>>>,
    /// Persistent planner stats catalog.
    lance_stats_table: Arc<tokio::sync::Mutex<Option<yata_lance::YataTable>>>,
    s3_prefix: String,
    /// SecurityScope cache: DID → (SecurityScope, compiled_at). Design E.
    security_scope_cache: Arc<Mutex<HashMap<String, (yata_gie::ir::SecurityScope, std::time::Instant)>>>,
    /// Cheap traversal planning stats: filter key → (count, cached_at).
    count_stats_cache: Arc<Mutex<HashMap<String, CachedCountStat>>>,
    /// Tombstone-aware table pressure stats: table key → raw/live/dead counts.
    pressure_stats_cache: Arc<Mutex<HashMap<String, CachedPressureStat>>>,
    cypher_read_count: Arc<AtomicU64>,
    cypher_mutation_count: Arc<AtomicU64>,
    cypher_mutation_us_total: Arc<AtomicU64>,
    merge_record_count: Arc<AtomicU64>,
    last_compaction_ms: Arc<AtomicU64>,
}

impl TieredGraphEngine {
    fn cached_pressure_stat(&self, key: &str) -> Option<CachedPressureStat> {
        let cache = self.pressure_stats_cache.lock().ok()?;
        let entry = cache.get(key)?;
        if entry.at.elapsed() < PRESSURE_STATS_CACHE_TTL {
            Some(entry.clone())
        } else {
            None
        }
    }

    fn store_pressure_stat(&self, key: String, stat: CachedPressureStat) {
        if let Ok(mut cache) = self.pressure_stats_cache.lock() {
            if cache.len() >= PRESSURE_STATS_CACHE_MAX {
                cache.retain(|_, entry| entry.at.elapsed() < PRESSURE_STATS_CACHE_TTL);
                if cache.len() >= PRESSURE_STATS_CACHE_MAX {
                    cache.clear();
                }
            }
            cache.insert(key, stat);
        }
    }

    fn persist_pressure_stat_catalog(&self, key: &str, stat: &CachedPressureStat) {
        self.ensure_lance();
        let lance_stats_tbl = self.lance_stats_table.clone();
        let kind = if key.contains(":edges:") { "edges" } else { "vertices" }.to_string();
        let key_owned = key.to_string();
        let stat_owned = stat.clone();
        let _ = self.block_on(async move {
            let tbl_guard = lance_stats_tbl.lock().await;
            let tbl = match tbl_guard.as_ref() {
                Some(tbl) => tbl,
                None => return Ok::<(), String>(()),
            };
            let batch = build_stats_catalog_batch(&kind, &key_owned, &stat_owned)?;
            tbl.add(batch)
                .await
                .map_err(|e| format!("stats catalog add: {e}"))?;
            Ok(())
        });
    }

    fn hydrate_pressure_stats_cache_from_catalog(&self) {
        let lance_stats_tbl = self.lance_stats_table.clone();
        let hydrated = self.block_on(async move {
            let tbl_guard = lance_stats_tbl.lock().await;
            let tbl = tbl_guard.as_ref()?;
            let batches = tbl.scan_all().await.ok()?;
            let mut latest: HashMap<String, (u64, CachedPressureStat)> = HashMap::new();
            for batch in batches {
                let key_col = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>()?;
                let raw_col = batch.column(2).as_any().downcast_ref::<arrow::array::UInt64Array>()?;
                let live_col = batch.column(3).as_any().downcast_ref::<arrow::array::UInt64Array>()?;
                let dead_col = batch.column(4).as_any().downcast_ref::<arrow::array::UInt64Array>()?;
                let ratio_col = batch.column(5).as_any().downcast_ref::<arrow::array::Float64Array>()?;
                let ts_col = batch.column(6).as_any().downcast_ref::<arrow::array::UInt64Array>()?;
                for row in 0..batch.num_rows() {
                    let key = key_col.value(row).to_string();
                    let ts = ts_col.value(row);
                    let stat = CachedPressureStat {
                        raw_rows: raw_col.value(row) as usize,
                        live_rows: live_col.value(row) as usize,
                        dead_rows: dead_col.value(row) as usize,
                        dead_ratio: ratio_col.value(row),
                        at: std::time::Instant::now(),
                    };
                    match latest.get(&key) {
                        Some((prev_ts, _)) if *prev_ts >= ts => {}
                        _ => {
                            latest.insert(key, (ts, stat));
                        }
                    }
                }
            }
            Some(latest.into_iter().map(|(k, (_, v))| (k, v)).collect::<HashMap<_, _>>())
        });

        if let Some(entries) = hydrated
            && let Ok(mut cache) = self.pressure_stats_cache.lock()
        {
            for (key, stat) in entries {
                cache.insert(key, stat);
            }
        }
    }

    fn cached_count_stat(&self, key: &str) -> Option<usize> {
        let cache = self.count_stats_cache.lock().ok()?;
        let entry = cache.get(key)?;
        if entry.at.elapsed() < COUNT_STATS_CACHE_TTL {
            Some(entry.count)
        } else {
            None
        }
    }

    fn store_count_stat(&self, key: String, count: usize) {
        if let Ok(mut cache) = self.count_stats_cache.lock() {
            if cache.len() >= COUNT_STATS_CACHE_MAX {
                cache.retain(|_, entry| entry.at.elapsed() < COUNT_STATS_CACHE_TTL);
                if cache.len() >= COUNT_STATS_CACHE_MAX {
                    cache.clear();
                }
            }
            cache.insert(key, CachedCountStat {
                count,
                at: std::time::Instant::now(),
            });
        }
    }

    fn invalidate_count_stats_cache(&self) {
        if let Ok(mut cache) = self.count_stats_cache.lock() {
            cache.clear();
        }
        if let Ok(mut cache) = self.pressure_stats_cache.lock() {
            cache.clear();
        }
    }

    fn pressure_cache_key(kind: TableKind, label: Option<&str>) -> String {
        match (kind, label) {
            (TableKind::Vertices, Some(label)) => format!("pressure:vertices:{label}"),
            (TableKind::Edges, Some(label)) => format!("pressure:edges:{label}"),
            (TableKind::Vertices, None) => "pressure:vertices:*".to_string(),
            (TableKind::Edges, None) => "pressure:edges:*".to_string(),
        }
    }

    fn estimate_table_pressure(&self, kind: TableKind, label: Option<&str>) -> Option<CachedPressureStat> {
        let cache_key = Self::pressure_cache_key(kind, label);
        if let Some(stat) = self.cached_pressure_stat(&cache_key) {
            return Some(stat);
        }

        let raw_rows = match kind {
            TableKind::Vertices => {
                let filter = label.map(|label| format!("label = {}", sql_quote(label)));
                self.ensure_lance();
                let lance_tbl = self.lance_table.clone();
                self.block_on(async {
                    let tbl_guard = lance_tbl.lock().await;
                    let tbl = tbl_guard.as_ref()?;
                    tbl.count_rows(filter.as_deref()).await.ok()
                })?
            }
            TableKind::Edges => {
                let filter = label.map(|label| format!("edge_label = {}", sql_quote(label)));
                self.ensure_lance();
                let lance_tbl = self.lance_edge_table.clone();
                self.block_on(async {
                    let tbl_guard = lance_tbl.lock().await;
                    let tbl = tbl_guard.as_ref()?;
                    tbl.count_rows(filter.as_deref()).await.ok()
                })?
            }
        };

        let live_rows = match kind {
            TableKind::Vertices => {
                if let Some(label) = label {
                    self.build_read_store(&[label]).ok()?.vertex_count()
                } else {
                    self.build_read_store(&[]).ok()?.vertex_count()
                }
            }
            TableKind::Edges => {
                let edge_labels: Vec<&str> = label.into_iter().collect();
                self.build_read_store_pushdown(&[], &edge_labels, &[], &[], &[], &[], &[], &[], None)
                    .ok()?
                    .edge_count()
            }
        };

        let dead_rows = raw_rows.saturating_sub(live_rows);
        let dead_ratio = if raw_rows == 0 {
            0.0
        } else {
            dead_rows as f64 / raw_rows as f64
        };
        let stat = CachedPressureStat {
            raw_rows,
            live_rows,
            dead_rows,
            dead_ratio,
            at: std::time::Instant::now(),
        };
        self.store_pressure_stat(cache_key, stat.clone());
        self.persist_pressure_stat_catalog(&Self::pressure_cache_key(kind, label), &stat);
        Some(stat)
    }

    fn estimate_vertex_filter_cardinality(&self, filter: &str) -> Option<usize> {
        if filter.is_empty() {
            return None;
        }
        let cache_key = format!("vertex:{filter}");
        if let Some(count) = self.cached_count_stat(&cache_key) {
            return Some(count);
        }
        self.ensure_lance();
        let lance_tbl = self.lance_table.clone();
        let count = self.block_on(async {
            let tbl_guard = lance_tbl.lock().await;
            let tbl = tbl_guard.as_ref()?;
            tbl.count_rows(Some(filter)).await.ok()
        });
        if let Some(count) = count {
            self.store_count_stat(cache_key, count);
        }
        count
    }

    fn estimate_edge_filter_cardinality(&self, filter: &str) -> Option<usize> {
        if filter.is_empty() {
            return None;
        }
        let cache_key = format!("edge:{filter}");
        if let Some(count) = self.cached_count_stat(&cache_key) {
            return Some(count);
        }
        self.ensure_lance();
        let lance_tbl = self.lance_edge_table.clone();
        let count = self.block_on(async {
            let tbl_guard = lance_tbl.lock().await;
            let tbl = tbl_guard.as_ref()?;
            tbl.count_rows(Some(filter)).await.ok()
        });
        if let Some(count) = count {
            self.store_count_stat(cache_key, count);
        }
        count
    }

    fn build_edge_cardinality_filter(&self, edge_label: &str, src_label: Option<&str>) -> String {
        let mut parts = vec![format!("edge_label = {}", sql_quote(edge_label))];
        if let Some(label) = src_label {
            parts.push(format!("src_label = {}", sql_quote(label)));
        }
        parts.join(" AND ")
    }

    fn planner_label_stats_view(
        &self,
        kind: TableKind,
        label: Option<&str>,
    ) -> Option<PlannerLabelStatsView> {
        let stat = self.estimate_table_pressure(kind, label)?;
        Some(PlannerLabelStatsView {
            label: label.map(str::to_string),
            live_rows: stat.live_rows,
            dead_ratio: stat.dead_ratio,
            last_compacted_at_ms: self.last_compaction_ms.load(Ordering::Relaxed),
        })
    }

    fn planner_traversal_stats_view(
        &self,
        source_label: Option<&str>,
        edge_label: &str,
    ) -> Option<PlannerTraversalStatsView> {
        let source = self.planner_label_stats_view(TableKind::Vertices, source_label)?;
        let edge = self.planner_label_stats_view(TableKind::Edges, Some(edge_label))?;
        let avg_out_degree = if source.live_rows == 0 {
            0.0
        } else {
            edge.live_rows as f64 / source.live_rows as f64
        };
        Some(PlannerTraversalStatsView {
            source,
            edge,
            avg_out_degree,
        })
    }

    fn explain_query_plan(
        &self,
        cypher: &str,
        params: &[(String, String)],
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        let ast = yata_cypher::parse(cypher).map_err(|e| format!("parse: {e}"))?;
        let mut plan = yata_gie::transpile::transpile(&ast).map_err(|e| format!("transpile: {e}"))?;
        let param_map: HashMap<String, String> = params.iter().cloned().collect();
        let hints = crate::hints::QueryHints::extract(cypher);
        self.refine_traversal_strategy_with_stats(&mut plan, &ast, &param_map, hints.limit);

        let node_filters = collect_node_pushdown_filters(&ast, &param_map);
        let mut rows = Vec::new();
        for (idx, op) in plan.ops.iter().enumerate() {
            match op {
                yata_gie::ir::LogicalOp::Scan {
                    label,
                    alias,
                    predicate,
                } => {
                    let mut row = vec![
                        ("op_index".to_string(), idx.to_string()),
                        ("op".to_string(), "Scan".to_string()),
                        ("alias".to_string(), alias.clone()),
                        ("label".to_string(), label.clone()),
                    ];
                    if let Some(predicate) = predicate {
                        row.push(("predicate".to_string(), format!("{predicate:?}")));
                    }
                    if let Some(view) = self.planner_label_stats_view(TableKind::Vertices, Some(label)) {
                        row.push(("live_rows".to_string(), view.live_rows.to_string()));
                        row.push(("dead_ratio".to_string(), format!("{:.4}", view.dead_ratio)));
                        row.push(("last_compacted_at_ms".to_string(), view.last_compacted_at_ms.to_string()));
                    }
                    rows.push(row);
                }
                yata_gie::ir::LogicalOp::Expand {
                    src_alias,
                    edge_label,
                    dst_alias,
                    direction,
                    strategy,
                    ..
                }
                | yata_gie::ir::LogicalOp::PathExpand {
                    src_alias,
                    edge_label,
                    dst_alias,
                    direction,
                    strategy,
                    ..
                } => {
                    let label = node_filters
                        .get(src_alias)
                        .and_then(|ctx| ctx.label.as_deref());
                    let planner_view = self.planner_traversal_stats_view(label, edge_label);
                    let mut row = vec![
                        ("op_index".to_string(), idx.to_string()),
                        ("op".to_string(), match op {
                            yata_gie::ir::LogicalOp::Expand { .. } => "Expand".to_string(),
                            _ => "PathExpand".to_string(),
                        }),
                        ("src_alias".to_string(), src_alias.clone()),
                        ("dst_alias".to_string(), dst_alias.clone()),
                        ("edge_label".to_string(), edge_label.clone()),
                        ("direction".to_string(), format!("{direction:?}")),
                        ("strategy".to_string(), format!("{strategy:?}")),
                    ];
                    if let yata_gie::ir::LogicalOp::PathExpand {
                        min_hops,
                        max_hops,
                        ..
                    } = op
                    {
                        row.push(("min_hops".to_string(), min_hops.to_string()));
                        row.push(("max_hops".to_string(), max_hops.to_string()));
                    }
                    if let Some(view) = planner_view {
                        row.push(("source_label".to_string(), view.source.label.unwrap_or_default()));
                        row.push(("source_live_rows".to_string(), view.source.live_rows.to_string()));
                        row.push(("source_dead_ratio".to_string(), format!("{:.4}", view.source.dead_ratio)));
                        row.push(("edge_live_rows".to_string(), view.edge.live_rows.to_string()));
                        row.push(("edge_dead_ratio".to_string(), format!("{:.4}", view.edge.dead_ratio)));
                        row.push(("avg_out_degree".to_string(), format!("{:.4}", view.avg_out_degree)));
                        row.push(("last_compacted_at_ms".to_string(), view.edge.last_compacted_at_ms.to_string()));
                    }
                    rows.push(row);
                }
                yata_gie::ir::LogicalOp::Filter { alias, predicate } => {
                    let mut row = vec![
                        ("op_index".to_string(), idx.to_string()),
                        ("op".to_string(), "Filter".to_string()),
                        ("predicate".to_string(), format!("{predicate:?}")),
                    ];
                    if let Some(alias) = alias {
                        row.push(("alias".to_string(), alias.clone()));
                    }
                    rows.push(row);
                }
                yata_gie::ir::LogicalOp::Project { exprs } => {
                    rows.push(vec![
                        ("op_index".to_string(), idx.to_string()),
                        ("op".to_string(), "Project".to_string()),
                        ("exprs".to_string(), format!("{exprs:?}")),
                    ]);
                }
                yata_gie::ir::LogicalOp::Aggregate { group_by, aggs } => {
                    rows.push(vec![
                        ("op_index".to_string(), idx.to_string()),
                        ("op".to_string(), "Aggregate".to_string()),
                        ("group_by".to_string(), format!("{group_by:?}")),
                        ("aggs".to_string(), format!("{aggs:?}")),
                    ]);
                }
                yata_gie::ir::LogicalOp::OrderBy { keys } => {
                    rows.push(vec![
                        ("op_index".to_string(), idx.to_string()),
                        ("op".to_string(), "OrderBy".to_string()),
                        ("keys".to_string(), format!("{keys:?}")),
                    ]);
                }
                yata_gie::ir::LogicalOp::Limit { count, offset } => {
                    rows.push(vec![
                        ("op_index".to_string(), idx.to_string()),
                        ("op".to_string(), "Limit".to_string()),
                        ("count".to_string(), count.to_string()),
                        ("offset".to_string(), offset.to_string()),
                    ]);
                }
                yata_gie::ir::LogicalOp::Distinct { keys } => {
                    rows.push(vec![
                        ("op_index".to_string(), idx.to_string()),
                        ("op".to_string(), "Distinct".to_string()),
                        ("keys".to_string(), format!("{keys:?}")),
                    ]);
                }
                yata_gie::ir::LogicalOp::SecurityFilter { aliases, .. } => {
                    rows.push(vec![
                        ("op_index".to_string(), idx.to_string()),
                        ("op".to_string(), "SecurityFilter".to_string()),
                        ("aliases".to_string(), format!("{aliases:?}")),
                    ]);
                }
                yata_gie::ir::LogicalOp::Exchange { routing_key, kind } => {
                    rows.push(vec![
                        ("op_index".to_string(), idx.to_string()),
                        ("op".to_string(), "Exchange".to_string()),
                        ("routing_key".to_string(), format!("{routing_key:?}")),
                        ("kind".to_string(), format!("{kind:?}")),
                    ]);
                }
                yata_gie::ir::LogicalOp::Receive { source_partitions } => {
                    rows.push(vec![
                        ("op_index".to_string(), idx.to_string()),
                        ("op".to_string(), "Receive".to_string()),
                        ("source_partitions".to_string(), format!("{source_partitions:?}")),
                    ]);
                }
            }
        }
        Ok(rows)
    }

    fn apply_pressure_penalty(
        &self,
        base_cost: usize,
        vertex_label: Option<&str>,
        edge_label: &str,
    ) -> usize {
        let view = self.planner_traversal_stats_view(vertex_label, edge_label);
        let vertex_penalty = view
            .as_ref()
            .map(|view| 1.0 + (view.source.dead_ratio * 4.0))
            .unwrap_or(1.0);
        let edge_penalty = view
            .as_ref()
            .map(|view| 1.0 + (view.edge.dead_ratio * 4.0))
            .unwrap_or(1.0);
        ((base_cost as f64) * vertex_penalty * edge_penalty).ceil() as usize
    }

    fn refine_traversal_strategy_with_stats(
        &self,
        plan: &mut yata_gie::ir::QueryPlan,
        ast: &yata_cypher::ast::Query,
        params: &HashMap<String, String>,
        limit: Option<usize>,
    ) {
        use yata_gie::ir::TraversalStrategy;

        let node_filters = collect_node_pushdown_filters(ast, params);
        let soft_limit = limit.map(|l| l.saturating_mul(4).max(32));

        for op in &mut plan.ops {
            match op {
                yata_gie::ir::LogicalOp::Expand {
                    src_alias,
                    edge_label,
                    direction,
                    strategy,
                    ..
                } => {
                    if matches!(direction, yata_grin::Direction::Both) || edge_label.is_empty() {
                        *strategy = TraversalStrategy::PreferGie;
                        continue;
                    }
                    let Some(ctx) = node_filters.get(src_alias) else {
                        continue;
                    };
                    let Some(source_cardinality) = self.estimate_vertex_filter_cardinality(&ctx.filter) else {
                        continue;
                    };
                    let planner_view = self.planner_traversal_stats_view(ctx.label.as_deref(), edge_label);
                    if planner_view
                        .as_ref()
                        .is_some_and(|view| view.edge.dead_ratio > 0.5)
                    {
                        *strategy = TraversalStrategy::PreferGie;
                        continue;
                    }
                    let edge_filter = self.build_edge_cardinality_filter(edge_label, ctx.label.as_deref());
                    let edge_cardinality = self.estimate_edge_filter_cardinality(&edge_filter).unwrap_or(0);
                    let estimated_cost = self.apply_pressure_penalty(
                        edge_cardinality.max(source_cardinality),
                        ctx.label.as_deref(),
                        edge_label,
                    );

                    if source_cardinality == 0 || edge_cardinality == 0 {
                        *strategy = TraversalStrategy::PreferGie;
                        continue;
                    }
                    if estimated_cost <= PATH_EXPAND_FRONTIER_LIMIT {
                        *strategy = TraversalStrategy::PreferStaged;
                        continue;
                    }
                    if estimated_cost > PATH_EXPAND_FRONTIER_LIMIT.saturating_mul(4) {
                        *strategy = TraversalStrategy::PreferGie;
                        continue;
                    }
                    if let Some(cap) = soft_limit {
                        *strategy = if estimated_cost <= cap {
                            TraversalStrategy::PreferStaged
                        } else {
                            TraversalStrategy::PreferGie
                        };
                    }
                }
                yata_gie::ir::LogicalOp::PathExpand {
                    src_alias,
                    edge_label,
                    max_hops,
                    direction,
                    strategy,
                    ..
                } => {
                    if matches!(direction, yata_grin::Direction::Both) || edge_label.is_empty() {
                        *strategy = TraversalStrategy::PreferGie;
                        continue;
                    }
                    let Some(ctx) = node_filters.get(src_alias) else {
                        continue;
                    };
                    let Some(source_cardinality) = self.estimate_vertex_filter_cardinality(&ctx.filter) else {
                        continue;
                    };
                    let planner_view = self.planner_traversal_stats_view(ctx.label.as_deref(), edge_label);
                    if planner_view
                        .as_ref()
                        .is_some_and(|view| view.edge.dead_ratio > 0.5)
                    {
                        *strategy = TraversalStrategy::PreferGie;
                        continue;
                    }
                    let edge_filter = self.build_edge_cardinality_filter(edge_label, ctx.label.as_deref());
                    let edge_cardinality = self.estimate_edge_filter_cardinality(&edge_filter).unwrap_or(0);
                    let estimated_cost = self.apply_pressure_penalty(
                        edge_cardinality
                            .saturating_mul((*max_hops as usize).max(1))
                            .max(source_cardinality),
                        ctx.label.as_deref(),
                        edge_label,
                    );

                    if source_cardinality == 0 || edge_cardinality == 0 {
                        *strategy = TraversalStrategy::PreferGie;
                        continue;
                    }
                    if estimated_cost <= PATH_EXPAND_FRONTIER_LIMIT {
                        *strategy = TraversalStrategy::PreferStaged;
                        continue;
                    }
                    if estimated_cost > PATH_EXPAND_FRONTIER_LIMIT.saturating_mul(4) {
                        *strategy = TraversalStrategy::PreferGie;
                        continue;
                    }
                    if let Some(cap) = soft_limit {
                        *strategy = if estimated_cost <= cap {
                            TraversalStrategy::PreferStaged
                        } else {
                            TraversalStrategy::PreferGie
                        };
                    }
                }
                _ => {}
            }
        }
    }

    fn maybe_schedule_compaction(&self) {
        let merges = self.merge_record_count.load(Ordering::Relaxed);
        let vertex_pressure = self.estimate_table_pressure(TableKind::Vertices, None);
        let edge_pressure = self.estimate_table_pressure(TableKind::Edges, None);
        let vertices_need_compaction = vertex_pressure.as_ref().is_some_and(|stat| {
            stat.dead_rows >= COMPACTION_DEAD_ROWS_THRESHOLD
                || stat.dead_ratio >= COMPACTION_DEAD_RATIO_THRESHOLD
        });
        let edges_need_compaction = edge_pressure.as_ref().is_some_and(|stat| {
            stat.dead_rows >= COMPACTION_DEAD_ROWS_THRESHOLD
                || stat.dead_ratio >= COMPACTION_DEAD_RATIO_THRESHOLD
        });
        let periodic_trigger = merges != 0 && merges % AUTO_COMPACT_FRAGMENT_THRESHOLD == 0;

        if !periodic_trigger && !vertices_need_compaction && !edges_need_compaction {
            return;
        }
        let lance_ds = self.lance_table.clone();
        let lance_edge_ds = self.lance_edge_table.clone();
        let last_compact_ms = self.last_compaction_ms.clone();
        std::thread::spawn(move || {
            ENGINE_RT.block_on(async {
                for (name, handle, should_compact) in [
                    ("vertices", lance_ds, periodic_trigger || vertices_need_compaction),
                    ("edges", lance_edge_ds, periodic_trigger || edges_need_compaction),
                ] {
                    if !should_compact {
                        continue;
                    }
                    let ds_guard = handle.lock().await;
                    if let Some(ref ds) = *ds_guard {
                        match ds.optimize_all().await {
                            Ok(stats) => {
                                let removed = stats.compaction.as_ref().map(|c| c.fragments_removed).unwrap_or(0);
                                let added = stats.compaction.as_ref().map(|c| c.fragments_added).unwrap_or(0);
                                tracing::info!(table = name, removed, added, "background compaction complete");
                            }
                            Err(e) => {
                                tracing::warn!(table = name, error = %e, "background compaction failed");
                            }
                        }
                    }
                }
                last_compact_ms.store(now_ms(), Ordering::SeqCst);
            });
        });
    }

    fn collect_vid_candidates_for_nodes(
        &self,
        nodes: Vec<&yata_cypher::ast::NodePattern>,
        scan_limit: usize,
        pushdown_limit: usize,
        ast: &yata_cypher::ast::Query,
        params: &HashMap<String, String>,
    ) -> Result<Vec<String>, String> {
        self.ensure_lance();
        let node_vars: std::collections::HashSet<String> = nodes.iter()
            .filter_map(|node| node.var.clone())
            .collect();
        let mut where_filters_by_var: HashMap<String, Vec<String>> = HashMap::new();
        for clause in &ast.clauses {
            match clause {
                yata_cypher::ast::Clause::Match { where_, .. }
                | yata_cypher::ast::Clause::OptionalMatch { where_, .. } => {
                    if let Some(expr) = where_ {
                        collect_var_where_filters(expr, &node_vars, params, &mut where_filters_by_var);
                    }
                }
                _ => {}
            }
        }

        let filters: Vec<String> = nodes.into_iter()
            .filter_map(|node| {
                let mut parts = Vec::new();
                if let Some(base) = build_vertex_pushdown_filter(node, params) {
                    parts.push(base);
                } else if let Some(label) = node.labels.first() {
                    parts.push(format!("label = {}", sql_quote(label)));
                }
                if let Some(var) = &node.var {
                    if let Some(extra) = where_filters_by_var.get(var) {
                        parts.extend(extra.iter().cloned());
                    }
                }
                if parts.len() <= 1 {
                    None
                } else {
                    Some(parts.join(" AND "))
                }
            })
            .collect();
        if filters.is_empty() {
            return Ok(Vec::new());
        }

        let lance_tbl = self.lance_table.clone();
        self.block_on(async {
            let tbl_guard = lance_tbl.lock().await;
            let tbl = match tbl_guard.as_ref() {
                Some(t) => t,
                None => return Ok(Vec::new()),
            };
            let mut vids = Vec::new();
            for filter in filters {
                let batches = tbl.scan_filter_limit(&filter, Some(scan_limit), None)
                    .await
                    .map_err(|e| format!("LanceDB vid candidate scan: {e}"))?;
                for batch in batches {
                    let pk_val_col = batch.column(2).as_any().downcast_ref::<arrow::array::StringArray>()
                        .ok_or("column 2 (pk_value) is not Utf8")?;
                    for row in 0..batch.num_rows() {
                        vids.push(pk_val_col.value(row).to_string());
                        if vids.len() >= pushdown_limit {
                            vids.sort();
                            vids.dedup();
                            return Ok(vids);
                        }
                    }
                }
            }
            vids.sort();
            vids.dedup();
            Ok(vids)
        })
    }

    fn collect_src_vid_candidates(
        &self,
        ast: &yata_cypher::ast::Query,
        params: &HashMap<String, String>,
    ) -> Result<Vec<String>, String> {
        let source_nodes = edge_source_patterns(ast);
        self.collect_vid_candidates_for_nodes(
            source_nodes,
            EDGE_SRC_SCAN_LIMIT,
            EDGE_SRC_VID_PUSHDOWN_LIMIT,
            ast,
            params,
        )
    }

    fn collect_dst_vid_candidates(
        &self,
        ast: &yata_cypher::ast::Query,
        params: &HashMap<String, String>,
    ) -> Result<Vec<String>, String> {
        let destination_nodes = edge_destination_patterns(ast);
        self.collect_vid_candidates_for_nodes(
            destination_nodes,
            EDGE_DST_SCAN_LIMIT,
            EDGE_DST_VID_PUSHDOWN_LIMIT,
            ast,
            params,
        )
    }

    fn collect_path_expand_frontier_vids(
        &self,
        plan: &yata_gie::ir::QueryPlan,
        initial_src_vids: &[String],
    ) -> Result<(Vec<String>, Vec<String>, Vec<String>), String> {
        let Some(yata_gie::ir::LogicalOp::PathExpand {
            edge_label,
            max_hops,
            direction,
            ..
        }) = plan.ops.iter().find(|op| matches!(op, yata_gie::ir::LogicalOp::PathExpand { .. })) else {
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        };
        if initial_src_vids.is_empty() || initial_src_vids.len() > PATH_EXPAND_FRONTIER_LIMIT || *max_hops <= 1 {
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        }

        let lance_edge_tbl = self.lance_edge_table.clone();
        self.block_on(async {
            let tbl_guard = lance_edge_tbl.lock().await;
            let Some(tbl) = tbl_guard.as_ref() else {
                return Ok((Vec::new(), Vec::new(), Vec::new()));
            };

            let mut frontier: Vec<String> = initial_src_vids.to_vec();
            let mut scanned_src_vids = Vec::new();
            let mut scanned_dst_vids = Vec::new();
            let mut touched_vids = initial_src_vids.to_vec();

            for _hop in 0..*max_hops {
                if frontier.is_empty() || frontier.len() > PATH_EXPAND_FRONTIER_LIMIT {
                    break;
                }
                let frontier_exprs: Vec<String> = frontier.iter().map(|v| sql_quote(v)).collect();
                let edge_label_filter = if edge_label.is_empty() {
                    String::new()
                } else {
                    format!("edge_label = {}", sql_quote(edge_label))
                };

                let filter = match direction {
                    yata_grin::Direction::Out => {
                        if frontier_exprs.len() == 1 {
                            format!("src_vid = {}", frontier_exprs[0])
                        } else {
                            format!("src_vid IN ({})", frontier_exprs.join(", "))
                        }
                    }
                    yata_grin::Direction::In => {
                        if frontier_exprs.len() == 1 {
                            format!("dst_vid = {}", frontier_exprs[0])
                        } else {
                            format!("dst_vid IN ({})", frontier_exprs.join(", "))
                        }
                    }
                    yata_grin::Direction::Both => break,
                };
                let combined_filter = if edge_label_filter.is_empty() {
                    filter
                } else {
                    format!("{edge_label_filter} AND {filter}")
                };

                let batches = tbl
                    .scan_filter_limit(&combined_filter, None, None)
                    .await
                    .map_err(|e| format!("LanceDB path frontier scan: {e}"))?;

                let mut next_frontier = Vec::new();
                for batch in batches {
                    let src_vid_col = batch.column(3).as_any().downcast_ref::<arrow::array::StringArray>()
                        .ok_or("edge column 3 (src_vid) is not Utf8")?;
                    let dst_vid_col = batch.column(4).as_any().downcast_ref::<arrow::array::StringArray>()
                        .ok_or("edge column 4 (dst_vid) is not Utf8")?;
                    for row in 0..batch.num_rows() {
                        match direction {
                            yata_grin::Direction::Out => {
                                scanned_src_vids.push(src_vid_col.value(row).to_string());
                                let dst = dst_vid_col.value(row).to_string();
                                touched_vids.push(dst.clone());
                                next_frontier.push(dst);
                            }
                            yata_grin::Direction::In => {
                                scanned_dst_vids.push(dst_vid_col.value(row).to_string());
                                let src = src_vid_col.value(row).to_string();
                                touched_vids.push(src.clone());
                                next_frontier.push(src);
                            }
                            yata_grin::Direction::Both => {}
                        }
                    }
                }
                next_frontier.sort();
                next_frontier.dedup();
                frontier = next_frontier;
            }

            scanned_src_vids.sort();
            scanned_src_vids.dedup();
            scanned_dst_vids.sort();
            scanned_dst_vids.dedup();
            touched_vids.sort();
            touched_vids.dedup();
            Ok((scanned_src_vids, scanned_dst_vids, touched_vids))
        })
    }

    fn scan_path_edges_for_frontier(
        &self,
        edge_label: &str,
        direction: yata_grin::Direction,
        frontier: &[String],
    ) -> Result<Vec<(String, String)>, String> {
        if frontier.is_empty() || frontier.len() > PATH_EXPAND_FRONTIER_LIMIT {
            return Ok(Vec::new());
        }
        let lance_edge_tbl = self.lance_edge_table.clone();
        let edge_label = edge_label.to_string();
        let frontier_owned = frontier.to_vec();
        self.block_on(async {
            let tbl_guard = lance_edge_tbl.lock().await;
            let Some(tbl) = tbl_guard.as_ref() else {
                return Ok(Vec::new());
            };
            let frontier_exprs: Vec<String> = frontier_owned.iter().map(|v| sql_quote(v)).collect();
            let endpoint_filter = match direction {
                yata_grin::Direction::Out => {
                    if frontier_exprs.len() == 1 {
                        format!("src_vid = {}", frontier_exprs[0])
                    } else {
                        format!("src_vid IN ({})", frontier_exprs.join(", "))
                    }
                }
                yata_grin::Direction::In => {
                    if frontier_exprs.len() == 1 {
                        format!("dst_vid = {}", frontier_exprs[0])
                    } else {
                        format!("dst_vid IN ({})", frontier_exprs.join(", "))
                    }
                }
                yata_grin::Direction::Both => return Ok(Vec::new()),
            };
            let combined_filter = if edge_label.is_empty() {
                endpoint_filter
            } else {
                format!("edge_label = {} AND {}", sql_quote(&edge_label), endpoint_filter)
            };
            let batches = tbl
                .scan_filter_limit(&combined_filter, None, None)
                .await
                .map_err(|e| format!("LanceDB staged path scan: {e}"))?;
            let mut edges = Vec::new();
            for batch in batches {
                let src_vid_col = batch.column(3).as_any().downcast_ref::<arrow::array::StringArray>()
                    .ok_or("edge column 3 (src_vid) is not Utf8")?;
                let dst_vid_col = batch.column(4).as_any().downcast_ref::<arrow::array::StringArray>()
                    .ok_or("edge column 4 (dst_vid) is not Utf8")?;
                for row in 0..batch.num_rows() {
                    edges.push((
                        src_vid_col.value(row).to_string(),
                        dst_vid_col.value(row).to_string(),
                    ));
                }
            }
            Ok(edges)
        })
    }

    fn execute_staged_path_expand(
        &self,
        initial_store: &yata_lance::ArrowStore,
        initial_records: Vec<yata_gie::executor::Record>,
        path_op: &yata_gie::ir::LogicalOp,
        rel_refs: &[&str],
        limit: Option<usize>,
    ) -> Result<(yata_lance::ArrowStore, Vec<yata_gie::executor::Record>), String> {
        let yata_gie::ir::LogicalOp::PathExpand {
            src_alias,
            edge_label,
            dst_alias,
            min_hops,
            max_hops,
            direction,
            ..
        } = path_op else {
            return Err("expected PathExpand op".into());
        };

        let mut staged_records = Vec::new();
        let mut touched_vids = Vec::new();
        let mut scanned_src_vids = Vec::new();
        let mut scanned_dst_vids = Vec::new();

        for record in initial_records {
            let Some(&start_vid) = record.bindings.get(src_alias.as_str()) else {
                continue;
            };
            for &vid in record.bindings.values() {
                if let Some(PropValue::Str(bound_pk)) = initial_store.vertex_prop(vid, "pk_value") {
                    touched_vids.push(bound_pk);
                }
            }
            let Some(PropValue::Str(start_pk)) = initial_store.vertex_prop(start_vid, "pk_value") else {
                continue;
            };

            let mut frontier: Vec<(String, u32)> = vec![(start_pk.clone(), 0)];
            let mut visited = std::collections::HashSet::new();
            visited.insert(start_pk.clone());
            touched_vids.push(start_pk.clone());

            while let Some((current_pk, depth)) = frontier.pop() {
                if depth >= *min_hops && depth <= *max_hops {
                    let mut bindings = HashMap::new();
                    for (alias, &vid) in &record.bindings {
                        if let Some(PropValue::Str(pk)) = initial_store.vertex_prop(vid, "pk_value") {
                            bindings.insert(alias.clone(), pk);
                        }
                    }
                    bindings.insert(dst_alias.clone(), current_pk.clone());
                    staged_records.push(StagedPathRecord {
                        bindings,
                        values: record.values.clone(),
                    });
                }

                if depth >= *max_hops {
                    continue;
                }
                let edges = self.scan_path_edges_for_frontier(edge_label, *direction, &[current_pk.clone()])?;
                for (src_pk, dst_pk) in edges {
                    match direction {
                        yata_grin::Direction::Out => scanned_src_vids.push(src_pk.clone()),
                        yata_grin::Direction::In => scanned_dst_vids.push(dst_pk.clone()),
                        yata_grin::Direction::Both => {}
                    }
                    let next_pk = match direction {
                        yata_grin::Direction::Out => dst_pk,
                        yata_grin::Direction::In => src_pk,
                        yata_grin::Direction::Both => continue,
                    };
                    if visited.insert(next_pk.clone()) {
                        touched_vids.push(next_pk.clone());
                        frontier.push((next_pk, depth + 1));
                    }
                }
            }
        }

        touched_vids.sort();
        touched_vids.dedup();
        scanned_src_vids.sort();
        scanned_src_vids.dedup();
        scanned_dst_vids.sort();
        scanned_dst_vids.dedup();

        let final_store = self.build_read_store_pushdown(
            &[],
            rel_refs,
            &[],
            &[],
            &touched_vids,
            &scanned_src_vids,
            &scanned_dst_vids,
            &[],
            limit,
        )?;

        let mut pk_to_vid = HashMap::new();
        for vid in final_store.scan_all_vertices() {
            if let Some(PropValue::Str(pk)) = final_store.vertex_prop(vid, "pk_value") {
                pk_to_vid.insert(pk, vid);
            }
        }

        let data: Vec<yata_gie::executor::Record> = staged_records
            .into_iter()
            .filter_map(|record| {
                let mut bindings = HashMap::new();
                for (alias, pk) in record.bindings {
                    let vid = pk_to_vid.get(&pk).copied()?;
                    bindings.insert(alias, vid);
                }
                Some(yata_gie::executor::Record { bindings, values: record.values })
            })
            .collect();
        Ok((final_store, data))
    }

    fn execute_staged_expand(
        &self,
        initial_store: &yata_lance::ArrowStore,
        initial_records: Vec<yata_gie::executor::Record>,
        expand_op: &yata_gie::ir::LogicalOp,
        rel_refs: &[&str],
        limit: Option<usize>,
    ) -> Result<(yata_lance::ArrowStore, Vec<yata_gie::executor::Record>), String> {
        let yata_gie::ir::LogicalOp::Expand {
            src_alias,
            edge_label,
            dst_alias,
            direction,
            ..
        } = expand_op else {
            return Err("expected Expand op".into());
        };

        let mut staged_records = Vec::new();
        let mut touched_vids = Vec::new();
        let mut scanned_src_vids = Vec::new();
        let mut scanned_dst_vids = Vec::new();

        for record in initial_records {
            let Some(&start_vid) = record.bindings.get(src_alias.as_str()) else {
                continue;
            };
            for &vid in record.bindings.values() {
                if let Some(PropValue::Str(bound_pk)) = initial_store.vertex_prop(vid, "pk_value") {
                    touched_vids.push(bound_pk);
                }
            }
            let Some(PropValue::Str(start_pk)) = initial_store.vertex_prop(start_vid, "pk_value") else {
                continue;
            };
            touched_vids.push(start_pk.clone());

            let edges = self.scan_path_edges_for_frontier(edge_label, *direction, std::slice::from_ref(&start_pk))?;
            for (src_pk, dst_pk) in edges {
                let mut bindings = HashMap::new();
                for (alias, &vid) in &record.bindings {
                    if let Some(PropValue::Str(pk)) = initial_store.vertex_prop(vid, "pk_value") {
                        bindings.insert(alias.clone(), pk);
                    }
                }
                match direction {
                    yata_grin::Direction::Out => {
                        scanned_src_vids.push(src_pk.clone());
                        touched_vids.push(dst_pk.clone());
                        bindings.insert(dst_alias.clone(), dst_pk);
                    }
                    yata_grin::Direction::In => {
                        scanned_dst_vids.push(dst_pk.clone());
                        touched_vids.push(src_pk.clone());
                        bindings.insert(dst_alias.clone(), src_pk);
                    }
                    yata_grin::Direction::Both => continue,
                }
                staged_records.push(StagedPathRecord {
                    bindings,
                    values: record.values.clone(),
                });
            }
        }

        touched_vids.sort();
        touched_vids.dedup();
        scanned_src_vids.sort();
        scanned_src_vids.dedup();
        scanned_dst_vids.sort();
        scanned_dst_vids.dedup();

        let final_store = self.build_read_store_pushdown(
            &[],
            rel_refs,
            &[],
            &[],
            &touched_vids,
            &scanned_src_vids,
            &scanned_dst_vids,
            &[],
            limit,
        )?;

        let mut pk_to_vid = HashMap::new();
        for vid in final_store.scan_all_vertices() {
            if let Some(PropValue::Str(pk)) = final_store.vertex_prop(vid, "pk_value") {
                pk_to_vid.insert(pk, vid);
            }
        }

        let data: Vec<yata_gie::executor::Record> = staged_records
            .into_iter()
            .filter_map(|record| {
                let mut bindings = HashMap::new();
                for (alias, pk) in record.bindings {
                    let vid = pk_to_vid.get(&pk).copied()?;
                    bindings.insert(alias, vid);
                }
                Some(yata_gie::executor::Record { bindings, values: record.values })
            })
            .collect();
        Ok((final_store, data))
    }

    /// Create a new engine with Lance-backed persistence.
    pub fn new(config: TieredEngineConfig, data_dir: &str) -> Self {
        Self::build(config, data_dir)
    }

    fn build(config: TieredEngineConfig, data_dir: &str) -> Self {
        let partition_count = config.partition_count;
        if partition_count > 1 {
            tracing::info!(partition_count, "partitioned graph store enabled");
        }
        // s3_prefix is used for LanceDB connect path.
        // In production: YATA_S3_PREFIX env (e.g. "yata/") → connect_from_env builds s3:// URI
        // In tests: data_dir (tempdir) → connect_local uses this path
        let s3_prefix = std::env::var("YATA_S3_PREFIX").unwrap_or_else(|_| data_dir.to_string());

        tracing::info!("using LanceDB (no persistent CSR)");
        Self {
            config,
            lance_db: Arc::new(tokio::sync::Mutex::new(None)),
            lance_table: Arc::new(tokio::sync::Mutex::new(None)),
            lance_edge_table: Arc::new(tokio::sync::Mutex::new(None)),
            lance_stats_table: Arc::new(tokio::sync::Mutex::new(None)),
            s3_prefix,
            security_scope_cache: Arc::new(Mutex::new(HashMap::new())),
            count_stats_cache: Arc::new(Mutex::new(HashMap::new())),
            pressure_stats_cache: Arc::new(Mutex::new(HashMap::new())),
            cypher_read_count: Arc::new(AtomicU64::new(0)),
            cypher_mutation_count: Arc::new(AtomicU64::new(0)),
            cypher_mutation_us_total: Arc::new(AtomicU64::new(0)),
            merge_record_count: Arc::new(AtomicU64::new(0)),
            last_compaction_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Build an ArrowStore from LanceDB (zero-copy, lazy props).
    /// Label pushdown: only scans fragments matching requested labels.
    fn build_read_store(&self, labels: &[&str]) -> Result<yata_lance::ArrowStore, String> {
        self.build_read_store_pushdown(labels, &[], &[], &[], &[], &[], &[], &[], None)
    }

    /// Build ArrowStore with full pushdown: label filter + WHERE conditions + LIMIT.
    ///
    /// `lance_filters`: additional SQL conditions on promoted columns (e.g., `timestamp_ms > 1000`).
    /// `limit`: max rows to scan from Lance (reduces I/O for large tables).
    fn build_read_store_pushdown(
        &self,
        labels: &[&str],
        edge_labels: &[&str],
        edge_src_labels: &[&str],
        edge_dst_labels: &[&str],
        vertex_pk_values: &[String],
        edge_src_vids: &[String],
        edge_dst_vids: &[String],
        lance_filters: &[String],
        limit: Option<usize>,
    ) -> Result<yata_lance::ArrowStore, String> {
        let lance_tbl = self.lance_table.clone();
        let lance_edge_tbl = self.lance_edge_table.clone();
        let filters_owned: Vec<String> = lance_filters.to_vec();
        let edge_labels_owned: Vec<String> = edge_labels.iter().map(|s| (*s).to_string()).collect();
        self.block_on(async {
            let vertex_batches = {
                let tbl_guard = lance_tbl.lock().await;
                let tbl = match tbl_guard.as_ref() {
                    Some(t) => t,
                    None => return yata_lance::ArrowStore::from_batches(Vec::new()),
                };

                // Build combined filter: label IN (...) AND extra conditions
                let mut filter_parts = Vec::new();
                if !labels.is_empty() {
                    let label_exprs: Vec<String> = labels.iter()
                        .map(|l| format!("'{}'", l.replace('\'', "''")))
                        .collect();
                    if label_exprs.len() == 1 {
                        filter_parts.push(format!("label = {}", label_exprs[0]));
                    } else {
                        filter_parts.push(format!("label IN ({})", label_exprs.join(", ")));
                    }
                }
                if !vertex_pk_values.is_empty() {
                    let pk_exprs: Vec<String> = vertex_pk_values.iter().map(|v| sql_quote(v)).collect();
                    if pk_exprs.len() == 1 {
                        filter_parts.push(format!("pk_value = {}", pk_exprs[0]));
                    } else {
                        filter_parts.push(format!("pk_value IN ({})", pk_exprs.join(", ")));
                    }
                }
                for f in &filters_owned {
                    filter_parts.push(f.clone());
                }

                let combined_filter = filter_parts.join(" AND ");
                if combined_filter.is_empty() && limit.is_none() {
                    tbl.scan_all().await.map_err(|e| format!("LanceDB scan: {e}"))?
                } else {
                    tbl.scan_filter_limit(&combined_filter, limit, None)
                        .await.map_err(|e| format!("LanceDB scan: {e}"))?
                }
            };

            let edge_batches = {
                let tbl_guard = lance_edge_tbl.lock().await;
                match tbl_guard.as_ref() {
                    Some(tbl) => {
                        let mut edge_filter_parts = Vec::new();
                        if !edge_labels_owned.is_empty() {
                            let label_exprs: Vec<String> = edge_labels_owned.iter()
                                .map(|l| format!("'{}'", l.replace('\'', "''")))
                                .collect();
                            if label_exprs.len() == 1 {
                                edge_filter_parts.push(format!("edge_label = {}", label_exprs[0]));
                            } else {
                                edge_filter_parts.push(format!("edge_label IN ({})", label_exprs.join(", ")));
                            }
                        }
                        if !edge_src_labels.is_empty() {
                            let label_exprs: Vec<String> = edge_src_labels.iter()
                                .map(|l| format!("'{}'", l.replace('\'', "''")))
                                .collect();
                            if label_exprs.len() == 1 {
                                edge_filter_parts.push(format!("src_label = {}", label_exprs[0]));
                            } else {
                                edge_filter_parts.push(format!("src_label IN ({})", label_exprs.join(", ")));
                            }
                        }
                        if !edge_dst_labels.is_empty() {
                            let label_exprs: Vec<String> = edge_dst_labels.iter()
                                .map(|l| format!("'{}'", l.replace('\'', "''")))
                                .collect();
                            if label_exprs.len() == 1 {
                                edge_filter_parts.push(format!("dst_label = {}", label_exprs[0]));
                            } else {
                                edge_filter_parts.push(format!("dst_label IN ({})", label_exprs.join(", ")));
                            }
                        }
                        if !edge_src_vids.is_empty() && edge_src_vids.len() <= EDGE_SRC_VID_PUSHDOWN_LIMIT {
                            let vid_exprs: Vec<String> = edge_src_vids.iter()
                                .map(|v| sql_quote(v))
                                .collect();
                            if vid_exprs.len() == 1 {
                                edge_filter_parts.push(format!("src_vid = {}", vid_exprs[0]));
                            } else {
                                edge_filter_parts.push(format!("src_vid IN ({})", vid_exprs.join(", ")));
                            }
                        }
                        if !edge_dst_vids.is_empty() && edge_dst_vids.len() <= EDGE_DST_VID_PUSHDOWN_LIMIT {
                            let vid_exprs: Vec<String> = edge_dst_vids.iter()
                                .map(|v| sql_quote(v))
                                .collect();
                            if vid_exprs.len() == 1 {
                                edge_filter_parts.push(format!("dst_vid = {}", vid_exprs[0]));
                            } else {
                                edge_filter_parts.push(format!("dst_vid IN ({})", vid_exprs.join(", ")));
                            }
                        }
                        let combined_filter = edge_filter_parts.join(" AND ");
                        if combined_filter.is_empty() {
                            tbl.scan_all().await.map_err(|e| format!("LanceDB edge scan: {e}"))?
                        } else {
                            tbl.scan_filter_limit(&combined_filter, None, None)
                                .await.map_err(|e| format!("LanceDB edge scan: {e}"))?
                        }
                    }
                    None => Vec::new(),
                }
            };
            yata_lance::ArrowStore::from_vertex_edge_batches_with_spill(
                vertex_batches,
                edge_batches,
                self.config.arrowstore_budget_mb * 1024 * 1024,
                &self.config.vineyard_dir,
            )
        })
    }

    /// Ensure LanceDB connection + vertex/edge tables are initialized.
    ///
    /// If the existing vertices table has the old 7-col schema (seq at col 0),
    /// migrate it: read all data, drop table, create with Format D schema, re-insert.
    fn ensure_lance(&self) {
        let lance_db = self.lance_db.clone();
        let lance_tbl = self.lance_table.clone();
        let lance_edge_tbl = self.lance_edge_table.clone();
        let lance_stats_tbl = self.lance_stats_table.clone();
        let prefix = self.s3_prefix.clone();
        let _ = self.block_on(async {
            let mut db_guard = lance_db.lock().await;
            if db_guard.is_none() {
                let new_db = match yata_lance::YataDb::connect_from_env(&prefix).await {
                    Some(db) => db,
                    None => match yata_lance::YataDb::connect_local(&prefix).await {
                        Ok(db) => db,
                        Err(_) => return,
                    },
                };
                *db_guard = Some(new_db);
            }
            // Vertex table
            let mut tbl_guard = lance_tbl.lock().await;
            if tbl_guard.is_none() {
                if let Some(ref db) = *db_guard {
                    match db.open_table("vertices").await {
                        Ok(tbl) => {
                            // Check if old schema (col 0 = UInt64 "seq") — needs migration
                            let is_legacy = {
                                match tbl.scan_all().await {
                                    Ok(batches) => batches.first()
                                        .map(|b| b.schema().field(0).data_type() == &arrow::datatypes::DataType::UInt64)
                                        .unwrap_or(false),
                                    Err(_) => false,
                                }
                            };
                            if is_legacy {
                                tracing::info!("detected legacy 7-col vertices table — migrating to Format D in-place");
                                // Read all data via ArrowStore (handles both schemas)
                                let batches = tbl.scan_all().await.unwrap_or_default();
                                let store = yata_lance::ArrowStore::from_batches(batches).unwrap_or_else(|_| {
                                    yata_lance::ArrowStore::from_batches(Vec::new()).unwrap()
                                });
                                let vertex_count = store.vertex_count();
                                tracing::info!(vertex_count, "legacy data read, dropping old table");
                                // Drop old table and create Format D
                                let _ = db.inner().drop_table("vertices", &[]).await;
                                let new_tbl = db.create_empty_table("vertices", lance_schema()).await;
                                match new_tbl {
                                    Ok(new_t) => {
                                        tracing::info!("Format D vertices table created");
                                        *tbl_guard = Some(new_t);
                                    }
                                    Err(e) => {
                                        tracing::warn!(error = %e, "failed to create Format D table");
                                    }
                                }
                            } else {
                                *tbl_guard = Some(tbl);
                            }
                        }
                        Err(_) => {
                            let schema = lance_schema();
                            if let Ok(tbl) = db.create_empty_table("vertices", schema).await {
                                *tbl_guard = Some(tbl);
                            }
                        }
                    }
                }
            }
            let vertices_ready = tbl_guard.is_some();
            drop(tbl_guard);
            // Edge table (P1)
            let mut edge_guard = lance_edge_tbl.lock().await;
            if edge_guard.is_none() {
                if let Some(ref db) = *db_guard {
                    match db.open_table("edges").await {
                        Ok(tbl) => { *edge_guard = Some(tbl); }
                        Err(_) => {
                            let schema = edge_lance_schema();
                            if let Ok(tbl) = db.create_empty_table("edges", schema).await {
                                *edge_guard = Some(tbl);
                            }
                        }
                    }
                }
            }
            let edges_ready = edge_guard.is_some();
            drop(edge_guard);
            let mut stats_guard = lance_stats_tbl.lock().await;
            if stats_guard.is_none() {
                if let Some(ref db) = *db_guard {
                    match db.open_table("stats_catalog").await {
                        Ok(tbl) => { *stats_guard = Some(tbl); }
                        Err(_) => {
                            let schema = stats_catalog_schema();
                            if let Ok(tbl) = db.create_empty_table("stats_catalog", schema).await {
                                *stats_guard = Some(tbl);
                            }
                        }
                    }
                }
            }
            drop(stats_guard);
            if vertices_ready || edges_ready {
                let vertices_guard = lance_tbl.lock().await;
                let edges_guard = lance_edge_tbl.lock().await;
                ensure_lance_scalar_indices(vertices_guard.as_ref(), edges_guard.as_ref()).await;
            }
        });
        let should_hydrate = self
            .pressure_stats_cache
            .lock()
            .map(|cache| cache.is_empty())
            .unwrap_or(false);
        if should_hydrate {
            self.hydrate_pressure_stats_cache_from_catalog();
        }
    }

    /// Run an async future, handling both inside-runtime and outside-runtime contexts.
    fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(f))
        } else {
            ENGINE_RT.block_on(f)
        }
    }

    /// Mark a single label as dirty.


    /// Mutation context for provenance tracking.
    /// Injected as node properties on every mutation.
    pub fn query_with_context(
        &self,
        cypher: &str,
        params: &[(String, String)],
        rls_org_id: Option<&str>,
        ctx: &MutationContext,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        self.query_inner(cypher, params, rls_org_id, Some(ctx))
    }

    /// Execute multiple Cypher mutations in a single batch.
    /// Each statement is executed directly via Lance operations.
    pub fn batch_query_with_context(
        &self,
        statements: &[(&str, &[(String, String)])],
        _rls_org_id: Option<&str>,
        ctx: &MutationContext,
    ) -> Result<Vec<Vec<Vec<(String, String)>>>, String> {
        if statements.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_lance();
        let mut all_results = Vec::with_capacity(statements.len());
        for (cypher, params) in statements {
            let rows = self.execute_mutation_direct(cypher, params, Some(ctx))?;
            all_results.push(rows);
        }
        Ok(all_results)
    }

    /// Query with scoped mutation context (org_id + user_did + actor_did).
    pub fn query_with_scoped_context(
        &self,
        cypher: &str,
        params: &[(String, String)],
        ctx: &MutationContext,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        let rls_org = if ctx.org_id.is_empty() {
            None
        } else {
            Some(ctx.org_id.as_str())
        };
        self.query_inner(cypher, params, rls_org, Some(ctx))
    }

    /// Main query entry point (sync, for WIT host compatibility).
    /// Auto-constructs MutationContext from env vars (PERFORMER_ID, rls_org_id).
    pub fn query(
        &self,
        cypher: &str,
        params: &[(String, String)],
        rls_org_id: Option<&str>,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        let app_id = &self.config.app_id;
        let config_org = &self.config.org_id;
        let resolved_org = rls_org_id.unwrap_or_default();
        let org_id = if resolved_org.is_empty() {
            config_org.as_str()
        } else {
            resolved_org
        };
        let ctx = if !app_id.is_empty() || !org_id.is_empty() {
            Some(MutationContext {
                app_id: app_id.clone(),
                org_id: org_id.to_string(),
                user_id: String::new(),
                actor_id: String::new(),
                user_did: String::new(),
                actor_did: String::new(),
            })
        } else {
            None
        };
        self.query_inner(cypher, params, rls_org_id, ctx.as_ref())
    }

    fn query_inner(
        &self,
        cypher: &str,
        params: &[(String, String)],
        _rls_org_id: Option<&str>,
        mutation_ctx: Option<&MutationContext>,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        if let Some(explain_cypher) = strip_explain_prefix(cypher) {
            return self.explain_query_plan(explain_cypher, params);
        }
        self.ensure_lance();
        let is_mutation = router::is_cypher_mutation(cypher);
        if is_mutation {
            self.cypher_mutation_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.cypher_read_count.fetch_add(1, Ordering::Relaxed);
        }
        let query_start = std::time::Instant::now();

        if !is_mutation {

            // Extract full pushdown hints: labels, WHERE conditions, LIMIT
            let hints = crate::hints::QueryHints::extract(cypher);
            let vl_refs: Vec<&str> = hints.node_labels.iter().map(|s| s.as_str()).collect();
            let rel_refs: Vec<&str> = hints.rel_types.iter().map(|s| s.as_str()).collect();
            let param_map: HashMap<String, String> = params.iter().cloned().collect();

            // GIE path: Cypher → IR Plan → execute directly on the read store.
            // Design E: SecurityScope is compiled from policy vertices via query_with_did().
            // This path (query_inner) is Internal-only (no SecurityFilter needed).
            if let Ok(ast) = yata_cypher::parse(cypher) {
                let plan_result = yata_gie::transpile::transpile(&ast);
                let (edge_src_labels, edge_dst_labels) = edge_endpoint_labels(&ast);
                let mut src_vid_candidates = self.collect_src_vid_candidates(&ast, &param_map).unwrap_or_default();
                let mut dst_vid_candidates = self.collect_dst_vid_candidates(&ast, &param_map).unwrap_or_default();
                let vertex_lance_filters: Vec<String> = if rel_refs.is_empty() {
                    hints.lance_filters.clone()
                } else {
                    Vec::new()
                };

                if let Ok(mut plan) = plan_result {
                    self.refine_traversal_strategy_with_stats(&mut plan, &ast, &param_map, hints.limit);
                    if plan.ops.iter().any(|op| matches!(
                        op,
                        yata_gie::ir::LogicalOp::Expand { .. } | yata_gie::ir::LogicalOp::PathExpand { .. }
                    )) {
                        let mut current_store = self.build_read_store_pushdown(
                            &vl_refs,
                            &[],
                            &[],
                            &[],
                            &[],
                            &src_vid_candidates,
                            &[],
                            &vertex_lance_filters,
                            hints.limit,
                        )?;
                        let has_orderby = plan.ops.iter().any(|op| matches!(op, yata_gie::ir::LogicalOp::OrderBy { .. }));
                        let has_aggregate = plan.ops.iter().any(|op| matches!(op, yata_gie::ir::LogicalOp::Aggregate { .. }));
                        let intermediate_cap = hints.limit.map(|l| if has_orderby { l.saturating_mul(4) } else { l });
                        let mut data = Vec::new();
                        for op in &plan.ops {
                            let stage_this_op = should_stage_traversal(op, data.len(), hints.limit);
                            if stage_this_op {
                                match op {
                                    yata_gie::ir::LogicalOp::Expand { .. } => {
                                        let (new_store, new_data) = self.execute_staged_expand(
                                            &current_store,
                                            data,
                                            op,
                                            &rel_refs,
                                            hints.limit,
                                        )?;
                                        current_store = new_store;
                                        data = new_data;
                                    }
                                    yata_gie::ir::LogicalOp::PathExpand { .. } => {
                                        let (new_store, new_data) = self.execute_staged_path_expand(
                                            &current_store,
                                            data,
                                            op,
                                            &rel_refs,
                                            hints.limit,
                                        )?;
                                        current_store = new_store;
                                        data = new_data;
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
                                data = yata_gie::executor::execute_op(op, data, &current_store);
                            }
                            if !has_aggregate {
                                if let Some(cap) = intermediate_cap {
                                    match op {
                                        yata_gie::ir::LogicalOp::Scan { .. }
                                        | yata_gie::ir::LogicalOp::Expand { .. }
                                        | yata_gie::ir::LogicalOp::PathExpand { .. }
                                        | yata_gie::ir::LogicalOp::Filter { .. }
                                        | yata_gie::ir::LogicalOp::SecurityFilter { .. } => {
                                            if data.len() > cap {
                                                data.truncate(cap);
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        return Ok(yata_gie::executor::result_to_rows(&data, &plan));
                    }
                    let path_expand_meta = plan.ops.iter().find_map(|op| {
                        if let yata_gie::ir::LogicalOp::PathExpand { max_hops, .. } = op {
                            Some(*max_hops)
                        } else {
                            None
                        }
                    });
                    let mut effective_edge_src_labels = edge_src_labels.clone();
                    let mut effective_edge_dst_labels = edge_dst_labels.clone();
                    let mut vertex_pk_candidates: Vec<String> = Vec::new();
                    if let Some(max_hops) = path_expand_meta.filter(|h| *h > 1) {
                        let (path_src_vids, path_dst_vids, path_vertex_vids) = self
                            .collect_path_expand_frontier_vids(&plan, &src_vid_candidates)
                            .unwrap_or_default();
                        if !path_src_vids.is_empty() {
                            src_vid_candidates = path_src_vids;
                        }
                        if !path_dst_vids.is_empty() {
                            dst_vid_candidates = path_dst_vids;
                        }
                        // Final endpoint constraints are not safe to apply to every hop.
                        effective_edge_src_labels.clear();
                        effective_edge_dst_labels.clear();
                        dst_vid_candidates.clear();
                        vertex_pk_candidates = path_vertex_vids;
                        let _ = max_hops;
                    }
                    // Use enhanced pushdown: label + WHERE + LIMIT pushed to Lance
                    let read_store = self.build_read_store_pushdown(
                        &vl_refs,
                        &rel_refs,
                        &effective_edge_src_labels.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                        &effective_edge_dst_labels.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                        &vertex_pk_candidates,
                        &src_vid_candidates,
                        &dst_vid_candidates,
                        &vertex_lance_filters,
                        hints.limit,
                    )?;
                    // Use limit-aware executor for early termination
                    let records = yata_gie::executor::execute_with_limit(
                        &plan,
                        &read_store,
                        hints.limit,
                    );
                    let rows = yata_gie::executor::result_to_rows(&records, &plan);
                    return Ok(rows);
                }
                return Err(format!("GIE transpile failed for query: {cypher}"));
            }
            return Err(format!("Cypher parse failed for query: {cypher}"));
        }

        // Direct Lance mutation: translate Cypher AST → merge_record/delete_record
        let result = self.execute_mutation_direct(cypher, params, mutation_ctx);
        if is_mutation {
            let elapsed_us = query_start.elapsed().as_micros() as u64;
            self.cypher_mutation_us_total.fetch_add(elapsed_us, Ordering::Relaxed);
        }
        result
    }

    /// Execute Cypher mutation by translating AST directly to Lance operations.
    /// No MemoryGraph — O(1) for CREATE, O(matched) for DELETE.
    fn execute_mutation_direct(
        &self,
        cypher: &str,
        params: &[(String, String)],
        mutation_ctx: Option<&MutationContext>,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        let ast = yata_cypher::parse(cypher).map_err(|e| format!("parse: {e}"))?;
        let param_map: HashMap<String, String> = params.iter().cloned().collect();
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

        let mut var_bindings: HashMap<String, Vec<(String, String)>> = HashMap::new();

        for clause in &ast.clauses {
            match clause {
                yata_cypher::Clause::Create { patterns } => {
                    for pattern in patterns {
                        let mut prev_node_id: Option<String> = None;
                        let mut prev_node_label: Option<String> = None;
                        let mut pending_rel: Option<(String, String, String, Vec<(String, yata_grin::PropValue)>)> = None; // (src_id, src_label, rel_type, props)

                        for elem in &pattern.elements {
                            match elem {
                                yata_cypher::PatternElement::Node(np) => {
                                    let label = np.labels.first().map(|s| s.as_str()).unwrap_or("_default");
                                    let node_id = generate_node_id();
                                    let mut props = self.resolve_pattern_props(&np.props, &param_map)?;
                                    inject_metadata(&mut props, mutation_ctx, &now);
                                    let props_ref: Vec<(&str, yata_grin::PropValue)> = props.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                                    self.merge_record(label, "rkey", &node_id, &props_ref)?;

                                    if let Some(ref var) = np.var {
                                        var_bindings.entry(var.clone()).or_default().push((label.to_string(), node_id.clone()));
                                    }

                                    // Complete pending relationship
                                    if let Some((src, src_label, rel_type, eprops)) = pending_rel.take() {
                                        let mut full_props = eprops;
                                        full_props.push(("_src".into(), yata_grin::PropValue::Str(src)));
                                        full_props.push(("_dst".into(), yata_grin::PropValue::Str(node_id.clone())));
                                        full_props.push(("_src_label".into(), yata_grin::PropValue::Str(src_label)));
                                        full_props.push(("_dst_label".into(), yata_grin::PropValue::Str(label.to_string())));
                                        let edge_id = generate_edge_id();
                                        let props_ref: Vec<(&str, yata_grin::PropValue)> = full_props.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                                        self.merge_record(&rel_type, "eid", &edge_id, &props_ref)?;
                                    }

                                    prev_node_id = Some(node_id);
                                    prev_node_label = Some(label.to_string());
                                }
                                yata_cypher::PatternElement::Rel(rp) => {
                                    let rel_type = rp.types.first().cloned().unwrap_or_else(|| "RELATED".into());
                                    let eprops = self.resolve_pattern_props(&rp.props, &param_map)?;
                                    let src_label = prev_node_label.clone().unwrap_or_else(|| "_default".to_string());
                                    pending_rel = Some((prev_node_id.clone().unwrap_or_default(), src_label, rel_type, eprops));
                                }
                            }
                        }
                    }
                }
                yata_cypher::Clause::Match { patterns, where_: where_clause } => {
                    // Resolve matched nodes → bind to variables
                    for pattern in patterns {
                        for elem in &pattern.elements {
                            if let yata_cypher::PatternElement::Node(np) = elem {
                                let Some(ref var) = np.var else { continue };
                                let label = np.labels.first().map(|s| s.as_str()).unwrap_or("");
                                if label.is_empty() { continue; }
                                let store = self.build_read_store(&[label])?;
                                let matched = self.find_matching_vertices(&store, np, where_clause, &param_map);
                                var_bindings.insert(var.clone(), matched);
                            }
                        }
                    }
                }
                yata_cypher::Clause::Delete { exprs, .. } => {
                    for expr in exprs {
                        if let yata_cypher::Expr::Var(var) = expr {
                            if let Some(nodes) = var_bindings.get(var) {
                                for (label, node_id) in nodes.clone() {
                                    self.delete_record(&label, "rkey", &node_id)?;
                                }
                            }
                        }
                    }
                }
                // MERGE treated as CREATE (Lance last-writer-wins dedup provides MERGE semantics)
                yata_cypher::Clause::Merge { pattern, on_create, on_match } => {
                    for elem in &pattern.elements {
                        if let yata_cypher::PatternElement::Node(np) = elem {
                            let label = np.labels.first().map(|s| s.as_str()).unwrap_or("_default");
                            let node_id = np.props.iter().find_map(|(k, v)| {
                                if k == "rkey" { if let yata_cypher::Expr::Lit(yata_cypher::Literal::Str(s)) = v { Some(s.clone()) } else { None } } else { None }
                            }).unwrap_or_else(generate_node_id);
                            let mut props = self.resolve_pattern_props(&np.props, &param_map)?;
                            for set_item in on_create.iter().chain(on_match.iter()) {
                                if let yata_cypher::SetItem::PropSet(_lhs, rhs) = set_item {
                                    if let yata_cypher::Expr::Map(map_entries) = rhs {
                                        for (k, v) in map_entries {
                                            if let yata_cypher::Expr::Lit(lit) = v {
                                                let val = match lit {
                                                    yata_cypher::Literal::Str(s) => yata_grin::PropValue::Str(s.clone()),
                                                    yata_cypher::Literal::Int(i) => yata_grin::PropValue::Int(*i),
                                                    yata_cypher::Literal::Float(f) => yata_grin::PropValue::Float(*f),
                                                    yata_cypher::Literal::Bool(b) => yata_grin::PropValue::Bool(*b),
                                                    yata_cypher::Literal::Null => yata_grin::PropValue::Null,
                                                };
                                                props.push((k.clone(), val));
                                            }
                                        }
                                    }
                                }
                            }
                            inject_metadata(&mut props, mutation_ctx, &now);
                            let props_ref: Vec<(&str, yata_grin::PropValue)> = props.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                            self.merge_record(label, "rkey", &node_id, &props_ref)?;
                        }
                    }
                }
                yata_cypher::Clause::Set { .. } => {}
                yata_cypher::Clause::Return { .. } => {} // reads handled by GIE path
                _ => {
                    return Err(format!("unsupported mutation clause: {cypher}"));
                }
            }
        }

        Ok(Vec::new())
    }

    /// Resolve pattern props, substituting $params with values from param_map.
    fn resolve_pattern_props(
        &self,
        props: &[(String, yata_cypher::Expr)],
        param_map: &HashMap<String, String>,
    ) -> Result<Vec<(String, yata_grin::PropValue)>, String> {
        let mut result = Vec::new();
        for (k, expr) in props {
            let v = match expr {
                yata_cypher::Expr::Lit(lit) => match lit {
                    yata_cypher::Literal::Int(i) => yata_grin::PropValue::Int(*i),
                    yata_cypher::Literal::Float(f) => yata_grin::PropValue::Float(*f),
                    yata_cypher::Literal::Str(s) => yata_grin::PropValue::Str(s.clone()),
                    yata_cypher::Literal::Bool(b) => yata_grin::PropValue::Bool(*b),
                    yata_cypher::Literal::Null => yata_grin::PropValue::Null,
                },
                yata_cypher::Expr::Param(name) => {
                    let raw = param_map.get(name).ok_or_else(|| format!("missing param ${name}"))?;
                    // JSON-decode param value (params are JSON-encoded strings)
                    match serde_json::from_str::<serde_json::Value>(raw) {
                        Ok(serde_json::Value::String(s)) => yata_grin::PropValue::Str(s),
                        Ok(serde_json::Value::Number(n)) => {
                            if let Some(i) = n.as_i64() { yata_grin::PropValue::Int(i) }
                            else { yata_grin::PropValue::Float(n.as_f64().unwrap_or(0.0)) }
                        }
                        Ok(serde_json::Value::Bool(b)) => yata_grin::PropValue::Bool(b),
                        Ok(serde_json::Value::Null) => yata_grin::PropValue::Null,
                        _ => yata_grin::PropValue::Str(raw.clone()),
                    }
                }
                _ => return Err(format!("unsupported expression in CREATE props for key '{k}'")),
            };
            result.push((k.clone(), v));
        }
        Ok(result)
    }

    /// Find vertices matching a MATCH pattern (label + inline props + WHERE clause).
    fn find_matching_vertices(
        &self,
        store: &yata_lance::ArrowStore,
        np: &yata_cypher::NodePattern,
        _where_clause: &Option<yata_cypher::Expr>,
        param_map: &HashMap<String, String>,
    ) -> Vec<(String, String)> {
        let label = np.labels.first().map(|s| s.as_str()).unwrap_or("");
        if label.is_empty() { return Vec::new(); }
        let vids = store.scan_vertices_by_label(label);
        let mut matched = Vec::new();
        for vid in vids {
            // Check inline props (e.g., {tid: 'remove-me'})
            let mut all_match = true;
            for (k, expr) in &np.props {
                let expected = match expr {
                    yata_cypher::Expr::Lit(yata_cypher::Literal::Str(s)) => yata_grin::PropValue::Str(s.clone()),
                    yata_cypher::Expr::Lit(yata_cypher::Literal::Int(i)) => yata_grin::PropValue::Int(*i),
                    yata_cypher::Expr::Param(name) => {
                        if let Some(raw) = param_map.get(name) {
                            match serde_json::from_str::<serde_json::Value>(raw) {
                                Ok(serde_json::Value::String(s)) => yata_grin::PropValue::Str(s),
                                Ok(serde_json::Value::Number(n)) => {
                                    if let Some(i) = n.as_i64() { yata_grin::PropValue::Int(i) }
                                    else { yata_grin::PropValue::Float(n.as_f64().unwrap_or(0.0)) }
                                }
                                _ => yata_grin::PropValue::Str(raw.clone()),
                            }
                        } else { continue; }
                    }
                    _ => continue,
                };
                if store.vertex_prop(vid, k) != Some(expected) {
                    all_match = false;
                    break;
                }
            }
            if all_match {
                let node_id = store.vertex_prop(vid, "rkey")
                    .or_else(|| store.vertex_prop(vid, "pk_value"))
                    .map(|v| match v { PropValue::Str(s) => s, _ => format!("{vid}") })
                    .unwrap_or_else(|| format!("{vid}"));
                matched.push((label.to_string(), node_id));
            }
        }
        matched
    }

    // ── Vector search ───────────────────────────────────────────────────

    /// Write vertices with embeddings to the LanceDB embeddings table.
    pub fn write_embeddings(
        &self,
        nodes: &[yata_cypher::NodeRef],
        embedding_key: &str,
        dim: usize,
    ) -> Result<usize, String> {
        if nodes.is_empty() { return Ok(0); }
        let count = nodes.len();

        let lance_db = self.lance_db.clone();
        let prefix_owned = self.s3_prefix.clone();
        let emb_key = embedding_key.to_string();

        self.block_on(async {
            let mut db_guard = lance_db.lock().await;
            if db_guard.is_none() {
                let new_db = match yata_lance::YataDb::connect_from_env(&prefix_owned).await {
                    Some(db) => db,
                    None => yata_lance::YataDb::connect_local(&prefix_owned).await
                        .map_err(|e| format!("LanceDB connect failed: {e}"))?,
                };
                *db_guard = Some(new_db);
            }
            let db = db_guard.as_ref().ok_or("LanceDB not connected")?;
            let tbl = db.open_or_create_embeddings_table(dim).await
                .map_err(|e| format!("embeddings table: {e}"))?;

            // Build batch from nodes — extract vector from props[emb_key]
            let embedding_key = &emb_key;
            let vids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
            let labels: Vec<&str> = nodes.iter().map(|n| {
                n.labels.first().map(|s| s.as_str()).unwrap_or("unknown")
            }).collect();
            let vid_arr = arrow::array::StringArray::from(vids);
            let label_arr = arrow::array::StringArray::from(labels);
            let vectors: Vec<Option<Vec<Option<f32>>>> = nodes.iter().map(|n| {
                if let Some(yata_cypher::types::Value::List(list)) = n.props.get(embedding_key) {
                    let v: Vec<Option<f32>> = list.iter().map(|val| match val {
                        yata_cypher::types::Value::Float(f) => Some(*f as f32),
                        yata_cypher::types::Value::Int(i) => Some(*i as f32),
                        _ => Some(0.0),
                    }).collect();
                    Some(v)
                } else {
                    Some(vec![Some(0.0f32); dim])
                }
            }).collect();
            let vec_arr = arrow::array::FixedSizeListArray::from_iter_primitive::<arrow::datatypes::Float32Type, _, _>(
                vectors, dim as i32,
            );
            let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("vid", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("label", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("vector",
                    arrow::datatypes::DataType::FixedSizeList(
                        std::sync::Arc::new(arrow::datatypes::Field::new("item", arrow::datatypes::DataType::Float32, true)),
                        dim as i32),
                    true),
            ]));
            let batch = arrow::record_batch::RecordBatch::try_new(schema, vec![
                std::sync::Arc::new(vid_arr),
                std::sync::Arc::new(label_arr),
                std::sync::Arc::new(vec_arr),
            ]).map_err(|e| format!("batch build: {e}"))?;
            tbl.add(batch).await.map_err(|e| format!("embeddings add: {e}"))?;
            Ok::<(), String>(())
        })?;
        Ok(count)
    }

    /// Vector search: find nearest neighbors via LanceDB.
    pub fn vector_search(
        &self,
        query_vector: Vec<f32>,
        limit: usize,
        label_filter: Option<&str>,
        _prop_filter: Option<&str>,
    ) -> Result<Vec<(yata_cypher::NodeRef, f32)>, String> {
        let lance_db = self.lance_db.clone();

        self.block_on(async {
            let db_guard = lance_db.lock().await;
            let db = match db_guard.as_ref() {
                Some(db) => db,
                None => return Ok(Vec::new()),
            };
            let tbl = match db.open_table("embeddings").await {
                Ok(tbl) => tbl,
                Err(_) => return Ok(Vec::new()),
            };
            let filter = label_filter.map(|l| format!("label = '{}'", l.replace('\'', "''")));
            let batches = tbl.vector_search(&query_vector, limit, filter.as_deref()).await
                .map_err(|e| format!("vector search: {e}"))?;

            let mut results = Vec::new();
            for batch in &batches {
                let vid_col = batch.column_by_name("vid")
                    .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>());
                let dist_col = batch.column_by_name("_distance")
                    .and_then(|c| c.as_any().downcast_ref::<arrow::array::Float32Array>());
                if let (Some(vids), Some(dists)) = (vid_col, dist_col) {
                    for i in 0..batch.num_rows() {
                        let vid = vids.value(i).to_string();
                        let dist = dists.value(i);
                        results.push((yata_cypher::NodeRef { id: vid, labels: Vec::new(), props: indexmap::IndexMap::new() }, dist));
                    }
                }
            }
            Ok(results)
        })
    }

    /// Create vector index on the embeddings table.
    pub fn create_embedding_index(&self) -> Result<(), String> {
        let lance_db = self.lance_db.clone();

        self.block_on(async {
            let db_guard = lance_db.lock().await;
            let db = match db_guard.as_ref() {
                Some(db) => db,
                None => return Ok(()),
            };
            let tbl = match db.open_table("embeddings").await {
                Ok(tbl) => tbl,
                Err(_) => return Ok(()),
            };
            tbl.create_vector_index().await.map_err(|e| format!("create index: {e}"))
        })
    }

    /// Write a record to LanceDB (append-only, Format D).
    ///
    /// Edges (pk_key == "eid") go to the separate edges table (P1).
    /// Vertices go to the vertices table with Format D schema (P2).
    pub fn merge_record(
        &self,
        label: &str,
        pk_key: &str,
        pk_value: &str,
        props: &[(&str, yata_grin::PropValue)],
    ) -> Result<u32, String> {
        let started_at = std::time::Instant::now();
        self.merge_record_count.fetch_add(1, Ordering::Relaxed);
        self.invalidate_security_cache_if_policy(label, props);
        self.ensure_lance();

        // Edge detection: pk_key == "eid" → edge table (P1)
        if pk_key == "eid" {
            let src = props.iter().find_map(|(k, v)| if *k == "_src" { if let PropValue::Str(s) = v { Some(s.as_str()) } else { None } } else { None }).unwrap_or("");
            let dst = props.iter().find_map(|(k, v)| if *k == "_dst" { if let PropValue::Str(s) = v { Some(s.as_str()) } else { None } } else { None }).unwrap_or("");
            let src_label = props.iter().find_map(|(k, v)| if *k == "_src_label" { if let PropValue::Str(s) = v { Some(s.as_str()) } else { None } } else { None });
            let dst_label = props.iter().find_map(|(k, v)| if *k == "_dst_label" { if let PropValue::Str(s) = v { Some(s.as_str()) } else { None } } else { None });
            let batch = build_edge_lance_batch(0, label, pk_value, src, dst, src_label, dst_label, props)?;
            let lance_edge_tbl = self.lance_edge_table.clone();
            self.block_on(async {
                let tbl_guard = lance_edge_tbl.lock().await;
                let tbl = tbl_guard.as_ref().ok_or("LanceDB edge table not initialized")?;
                tbl.add(batch).await.map_err(|e| format!("LanceDB edge add: {e}"))?;
                Ok::<(), String>(())
            })?;
            self.invalidate_count_stats_cache();
            self.maybe_schedule_compaction();
            let elapsed_ms = started_at.elapsed().as_millis() as u64;
            tracing::info!(
                op = "merge_record",
                kind = "edge",
                label,
                pk_key,
                pk_value,
                props = props.len(),
                elapsed_ms,
                "Lance mutation complete"
            );
            return Ok(0);
        }

        // Vertex write → direct Lance append (Format D)
        let batch = build_lance_batch(0, label, pk_key, pk_value, props)?;
        let lance_tbl = self.lance_table.clone();
        self.block_on(async {
            let tbl_guard = lance_tbl.lock().await;
            let tbl = tbl_guard.as_ref().ok_or("LanceDB vertex table not initialized")?;
            tbl.add(batch).await.map_err(|e| format!("LanceDB vertex add: {e}"))?;
            Ok::<(), String>(())
        })?;

        self.invalidate_count_stats_cache();
        self.maybe_schedule_compaction();
        let elapsed_ms = started_at.elapsed().as_millis() as u64;
        tracing::info!(
            op = "merge_record",
            kind = "vertex",
            label,
            pk_key,
            pk_value,
            props = props.len(),
            elapsed_ms,
            "Lance mutation complete"
        );
        Ok(0)
    }

    /// Delete a record (tombstone write to LanceDB). Returns false if record doesn't exist.
    pub fn delete_record(&self, label: &str, pk_key: &str, pk_value: &str) -> Result<bool, String> {
        let started_at = std::time::Instant::now();
        self.ensure_lance();
        if pk_key == "eid" {
            let batch = build_edge_lance_batch(1, label, pk_value, "", "", None, None, &[])?;
            let lance_edge_tbl = self.lance_edge_table.clone();
            self.block_on(async {
                let tbl_guard = lance_edge_tbl.lock().await;
                let tbl = tbl_guard.as_ref().ok_or("LanceDB edge table not initialized")?;
                tbl.add(batch).await.map_err(|e| format!("LanceDB edge tombstone add: {e}"))?;
                Ok::<(), String>(())
            })?;
            self.invalidate_count_stats_cache();
            self.maybe_schedule_compaction();
            let elapsed_ms = started_at.elapsed().as_millis() as u64;
            tracing::info!(
                op = "delete_record",
                kind = "edge",
                label,
                pk_key,
                pk_value,
                elapsed_ms,
                "Lance tombstone write complete"
            );
            return Ok(true);
        }
        let store = self.build_read_store(&[label])?;
        let exists = store.find_vertex_by_pk(label, pk_key, &yata_grin::PropValue::Str(pk_value.to_string())).is_some();
        if !exists {
            let elapsed_ms = started_at.elapsed().as_millis() as u64;
            tracing::info!(
                op = "delete_record",
                kind = "vertex",
                label,
                pk_key,
                pk_value,
                elapsed_ms,
                "Lance tombstone skipped: record not found"
            );
            return Ok(false);
        }
        let batch = build_lance_batch(1, label, pk_key, pk_value, &[])?;
        let lance_tbl = self.lance_table.clone();
        self.block_on(async {
            let tbl_guard = lance_tbl.lock().await;
            let tbl = tbl_guard.as_ref().ok_or("LanceDB table not initialized")?;
            tbl.add(batch).await.map_err(|e| format!("LanceDB add: {e}"))?;
            Ok::<(), String>(())
        })?;
        self.invalidate_count_stats_cache();
        self.maybe_schedule_compaction();
        let elapsed_ms = started_at.elapsed().as_millis() as u64;
        tracing::info!(
            op = "delete_record",
            kind = "vertex",
            label,
            pk_key,
            pk_value,
            elapsed_ms,
            "Lance tombstone write complete"
        );
        Ok(true)
    }

    /// CPM metrics.
    pub fn cpm_stats(&self) -> CpmStats {
        let reads = self.cypher_read_count.load(Ordering::Relaxed);
        let mutations = self.cypher_mutation_count.load(Ordering::Relaxed);
        let mutation_us = self.cypher_mutation_us_total.load(Ordering::Relaxed);
        let merges = self.merge_record_count.load(Ordering::Relaxed);
        let (v_count, e_count) = (0u64, 0u64);
        CpmStats {
            cypher_read_count: reads,
            cypher_mutation_count: mutations,
            cypher_mutation_avg_us: if mutations > 0 { mutation_us / mutations } else { 0 },
            cypher_mutation_us_total: mutation_us,
            merge_record_count: merges,
            mutation_ratio: if reads + mutations > 0 {
                mutations as f64 / (reads + mutations) as f64
            } else {
                0.0
            },
            vertex_count: v_count,
            edge_count: e_count,
            last_compaction_ms: self.last_compaction_ms.load(Ordering::Relaxed),
        }
    }

    /// LanceDB compaction.
    pub fn trigger_compaction(&self) -> Result<CompactionResult, String> {
        let started_at = std::time::Instant::now();
        self.ensure_lance();
        let lance_ds = self.lance_table.clone();
        let lance_edge_ds = self.lance_edge_table.clone();

        let result = self.block_on(async {
            let mut max_seq = 0u64;
            let mut input_entries = 0usize;
            let mut output_entries = 0usize;
            for (name, handle) in [("vertices", lance_ds), ("edges", lance_edge_ds)] {
                let ds_guard = handle.lock().await;
                if let Some(ref ds) = *ds_guard {
                    let version_before = ds.version().await.unwrap_or(0);
                    match ds.optimize_all().await {
                        Ok(stats) => {
                            let version_after = ds.version().await.unwrap_or(version_before);
                            let removed = stats.compaction.as_ref().map(|c| c.fragments_removed).unwrap_or(0);
                            let added = stats.compaction.as_ref().map(|c| c.fragments_added).unwrap_or(0);
                            tracing::info!(table = name, version_before, version_after, files_removed = removed, files_added = added, "Lance compaction complete");
                            max_seq = max_seq.max(version_after);
                            input_entries += removed;
                            output_entries += added;
                        }
                        Err(e) => {
                            tracing::warn!(table = name, error = %e, "Lance compaction failed, skipping");
                            max_seq = max_seq.max(version_before);
                        }
                    }
                } else if name == "vertices" {
                    return Err("compaction: LanceDB vertices table not initialized".to_string());
                }
            }
            Ok(CompactionResult {
                max_seq,
                input_entries,
                output_entries,
                labels: Vec::new(),
            })
        });

        self.last_compaction_ms.store(now_ms(), Ordering::SeqCst);
        match &result {
            Ok(stats) => tracing::info!(
                op = "trigger_compaction",
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                input_entries = stats.input_entries,
                output_entries = stats.output_entries,
                max_seq = stats.max_seq,
                "Lance compaction finished"
            ),
            Err(error) => tracing::warn!(
                op = "trigger_compaction",
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                error = %error,
                "Lance compaction failed"
            ),
        }
        result
    }

    /// LanceDB cold start. Opens vertex + edge tables (manifest + fragments from R2).
    fn cold_start_lance(&self) -> Result<u64, String> {
        let lance_tbl = self.lance_table.clone();
        let lance_edge_tbl = self.lance_edge_table.clone();
        let lance_stats_tbl = self.lance_stats_table.clone();
        let lance_db = self.lance_db.clone();
        let prefix_owned = self.s3_prefix.clone();

        let checkpoint_seq = self.block_on(async {
            let mut db_guard = lance_db.lock().await;
            if db_guard.is_none() {
                let new_db = match yata_lance::YataDb::connect_from_env(&prefix_owned).await {
                    Some(db) => db,
                    None => {
                        tracing::warn!("R2 connection unavailable — falling back to local filesystem (data will NOT persist across restarts)");
                        match yata_lance::YataDb::connect_local(&prefix_owned).await {
                            Ok(db) => db,
                            Err(e) => {
                                tracing::error!(error = %e, "LanceDB local connect also failed");
                                return 0u64;
                            }
                        }
                    }
                };
                *db_guard = Some(new_db);
            }
            let mut version = 0u64;
            if let Some(ref db) = *db_guard {
                // Vertex table
                match db.open_table("vertices").await {
                    Ok(tbl) => {
                        version = tbl.version().await.unwrap_or(0);
                        let rows = tbl.count_rows(None).await.unwrap_or(0);
                        tracing::info!(version, rows, "LanceDB cold start: vertices opened");
                        *lance_tbl.lock().await = Some(tbl);
                    }
                    Err(e) => {
                        tracing::info!(error = %e, "no LanceDB vertices table (new database)");
                    }
                }
                // Edge table (P1)
                match db.open_table("edges").await {
                    Ok(tbl) => {
                        let rows = tbl.count_rows(None).await.unwrap_or(0);
                        tracing::info!(rows, "LanceDB cold start: edges opened");
                        *lance_edge_tbl.lock().await = Some(tbl);
                    }
                    Err(e) => {
                        tracing::info!(error = %e, "no LanceDB edges table (new database)");
                    }
                }
                match db.open_table("stats_catalog").await {
                    Ok(tbl) => {
                        *lance_stats_tbl.lock().await = Some(tbl);
                    }
                    Err(_) => {
                        if let Ok(tbl) = db.create_empty_table("stats_catalog", stats_catalog_schema()).await {
                            *lance_stats_tbl.lock().await = Some(tbl);
                        }
                    }
                }
            }
            version
        });

        self.hydrate_pressure_stats_cache_from_catalog();

        Ok(checkpoint_seq)
    }

    /// Cold start entrypoint used by the REST API.
    pub fn cold_start(&self) -> Result<u64, String> {
        self.cold_start_lance()
    }

    /// Repair corrupted Lance table by rolling back to a valid version.
    /// Finds the newest version with intact data fragments and restores it.
    pub fn repair_lance(&self) -> Result<(u64, usize), String> {
        self.ensure_lance();
        let lance_tbl = self.lance_table.clone();

        self.block_on(async {
            let guard = lance_tbl.lock().await;
            if let Some(ref tbl) = *guard {
                tbl.repair().await.map_err(|e| format!("repair failed: {e}"))
            } else {
                Err("repair: LanceDB table not initialized".to_string())
            }
        })
    }

    // ── Design E: SecurityScope compilation from graph policy vertices ──

    const POLICY_LABELS: [&'static str; 5] = [
        "ClearanceAssignment", "RBACAssignment", "ConsentGrant",
        "RACIAssignment", "PreKeyBundle",
    ];
    const SCOPE_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(300);
    const SCOPE_CACHE_MAX: usize = 10_000;

    /// Invalidate SecurityScope cache when a policy vertex is written.
    fn invalidate_security_cache_if_policy(&self, label: &str, props: &[(&str, yata_grin::PropValue)]) {
        if !Self::POLICY_LABELS.contains(&label) {
            return;
        }
        let did = props.iter()
            .find(|(k, _)| *k == "did" || *k == "grantee_did")
            .and_then(|(_, v)| match v {
                yata_grin::PropValue::Str(s) => Some(s.as_str()),
                _ => None,
            });
        if let Some(did) = did {
            if let Ok(mut cache) = self.security_scope_cache.lock() {
                cache.remove(did);
            }
        }
    }

    /// Compile SecurityScope from policy vertices in the read store for a given DID.
    /// Results are cached with TTL.
    pub fn compile_security_scope(&self, did: &str) -> yata_gie::ir::SecurityScope {
        // Empty DID = public access
        if did.is_empty() {
            return yata_gie::ir::SecurityScope {
                max_sensitivity_ord: 0,
                collection_scopes: Vec::new(),
                allowed_owner_hashes: Vec::new(),
                bypass: false,
            };
        }

        // Check cache
        if let Ok(cache) = self.security_scope_cache.lock() {
            if let Some((scope, at)) = cache.get(did) {
                if at.elapsed() < Self::SCOPE_CACHE_TTL {
                    return scope.clone();
                }
            }
        }

        // Compile from read-store policy vertices
        let mut max_sensitivity_ord: u8 = 0;
        let mut collection_scopes: Vec<String> = Vec::new();
        let mut allowed_owner_hashes: Vec<u32> = Vec::new();

        { let store = self.build_read_store(&[]).unwrap_or_else(|_| yata_lance::ArrowStore::from_batches(Vec::new()).unwrap()); {
                let did_val = PropValue::Str(did.to_string());

                // ClearanceAssignment: scan by did → extract level
                for vid in store.scan_vertices("ClearanceAssignment", &Predicate::Eq("did".to_string(), did_val.clone())) {
                    if let Some(PropValue::Str(level)) = store.vertex_prop(vid, "level") {
                        max_sensitivity_ord = match level.as_str() {
                            "restricted" => 3,
                            "confidential" => 2,
                            "internal" => 1,
                            _ => 0,
                        };
                    }
                }

                // RBACAssignment: scan by did → extract scope (collection prefix)
                for vid in store.scan_vertices("RBACAssignment", &Predicate::Eq("did".to_string(), did_val.clone())) {
                    if let Some(PropValue::Str(scope)) = store.vertex_prop(vid, "scope") {
                        if scope != "*" {
                            collection_scopes.push(scope.clone());
                        }
                    }
                }

                // ConsentGrant: scan by grantee_did → extract grantor_did → hash
                for vid in store.scan_vertices("ConsentGrant", &Predicate::Eq("grantee_did".to_string(), did_val.clone())) {
                    if let Some(PropValue::Str(grantor)) = store.vertex_prop(vid, "grantor_did") {
                        allowed_owner_hashes.push(fnv1a_32(grantor.as_bytes()));
                    }
                }
            }
        }

        let scope = yata_gie::ir::SecurityScope {
            max_sensitivity_ord,
            collection_scopes,
            allowed_owner_hashes,
            bypass: false,
        };

        // Cache result
        if let Ok(mut cache) = self.security_scope_cache.lock() {
            if cache.len() >= Self::SCOPE_CACHE_MAX {
                cache.retain(|_, (_, at)| at.elapsed() < Self::SCOPE_CACHE_TTL);
            }
            cache.insert(did.to_string(), (scope.clone(), std::time::Instant::now()));
        }

        scope
    }

    /// Query with DID-based SecurityScope (Design E).
    /// Compiles SecurityScope from policy vertices, then runs GIE with SecurityFilter.
    pub fn query_with_did(
        &self,
        cypher: &str,
        params: &[(String, String)],
        did: &str,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        let scope = self.compile_security_scope(did);
        self.query_with_security_scope(cypher, params, scope)
    }

    /// Query with an explicit SecurityScope. GIE transpile_secured path only.
    fn query_with_security_scope(
        &self,
        cypher: &str,
        params: &[(String, String)],
        scope: yata_gie::ir::SecurityScope,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        if !router::is_cypher_mutation(cypher) {
            if let Ok(ast) = yata_cypher::parse(cypher) {
                let plan = yata_gie::transpile::transpile_secured(&ast, scope)
                    .map_err(|e| format!("GIE transpile: {}", e))?;
                let read_store = self.build_read_store(&[])?;
                let records = yata_gie::executor::execute(&plan, &read_store);
                let rows = yata_gie::executor::result_to_rows(&records, &plan);
                return Ok(rows);
            }
        }

        // Mutation path: delegate to existing query()
        self.query(cypher, params, None)
    }

    /// Resolve DID's P-256 public key multibase string from DIDDocument vertex.
    /// Returns the raw multibase string (z-prefix base58btc) or None.
    pub fn resolve_did_pubkey_multibase(&self, did: &str) -> Option<String> {
        { let store = self.build_read_store(&[]).unwrap_or_else(|_| yata_lance::ArrowStore::from_batches(Vec::new()).unwrap()); {
                let did_val = PropValue::Str(did.to_string());
                let vids = store.scan_vertices("DIDDocument", &Predicate::Eq("did".to_string(), did_val));
                let vid = *vids.first()?;
                match store.vertex_prop(vid, "public_key_multibase") {
                    Some(PropValue::Str(s)) => return Some(s.clone()),
                    _ => {}
                }
            }
        }
        None
    }

    /// Phase 5: Execute a distributed plan fragment step on the local read store.
    pub fn execute_fragment_step(
        &self,
        cypher: &str,
        partition_id: u32,
        partition_count: u32,
        target_round: u32,
        inbound: &std::collections::HashMap<u32, Vec<yata_gie::MaterializedRecord>>,
    ) -> Result<yata_gie::ExchangePayload, String> {
        let store = self.build_read_store(&[]).map_err(|e| format!("build: {e}"))?;
        yata_gie::execute_step(cypher, &store, partition_id, partition_count, target_round, inbound)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use yata_grin::{Property, Topology};

    fn make_engine(dir: &tempfile::TempDir) -> TieredGraphEngine {
        TieredGraphEngine::new(TieredEngineConfig::default(), dir.path().to_str().unwrap())
    }

    fn engine_at(data_dir: &std::path::Path) -> TieredGraphEngine {
        TieredGraphEngine::new(TieredEngineConfig::default(), data_dir.to_str().unwrap())
    }


    fn run_query(
        engine: &TieredGraphEngine,
        cypher: &str,
        params: &[(String, String)],
        rls: Option<&str>,
    ) -> Result<Vec<Vec<(String, String)>>, String> {
        engine.query(cypher, params, rls)
    }

    fn get_col(rows: &[Vec<(String, String)>], col: &str) -> Vec<String> {
        rows.iter().filter_map(|row| {
            row.iter().find(|(k, _)| k == col).map(|(_, v)| v.clone())
        }).collect()
    }

    #[test]
    fn test_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let engine = make_engine(&dir);
        run_query(
            &engine,
            "CREATE (a:Person {name: 'Alice', age: 30})",
            &[],
            None,
        )
        .unwrap();
        let rows = run_query(&engine, "MATCH (n:Person) RETURN n.name AS name", &[], None).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_write_read_consistency() {
        let dir = tempfile::tempdir().unwrap();
        let engine = make_engine(&dir);
        run_query(
            &engine,
            "CREATE (e:Entity {eid: 'e1', name: 'Test'})",
            &[],
            None,
        )
        .unwrap();
        run_query(
            &engine,
            "CREATE (ev:Evidence {evid: 'ev1', eid: 'e1', cat: 'Fraud'})",
            &[],
            None,
        )
        .unwrap();
        let rows = run_query(
            &engine,
            "MATCH (ev:Evidence {eid: 'e1'}) RETURN ev.evid AS id, ev.cat AS cat",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_rls() {
        // Design E: query_inner is Internal-only (no SecurityFilter).
        // Parameter-based RLS removed — all nodes visible on internal path.
        // Security filtering is via query_with_did() → policy vertex lookup on read_store.
        let dir = tempfile::tempdir().unwrap();
        let engine = make_engine(&dir);
        run_query(
            &engine,
            "CREATE (n:Item {name: 'A', org_id: 'org_1'})",
            &[],
            None,
        )
        .unwrap();
        run_query(
            &engine,
            "CREATE (n:Item {name: 'B', org_id: 'org_2'})",
            &[],
            None,
        )
        .unwrap();
        run_query(&engine, "CREATE (n:Schema {name: 'System'})", &[], None).unwrap();
        let rows = run_query(
            &engine,
            "MATCH (n) RETURN n.name AS name",
            &[],
            Some("org_1"),
        )
        .unwrap();
        let names: Vec<&str> = rows
            .iter()
            .flat_map(|r| r.iter().find(|(c, _)| c == "name").map(|(_, v)| v.as_str()))
            .collect();
        // Internal path: all nodes visible (no RLS filtering)
        assert!(names.contains(&"\"A\""));
        assert!(names.contains(&"\"System\""));
        assert!(names.contains(&"\"B\""));
    }

    #[test]
    fn test_rls_mutation_create_with_org_id() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        // CREATE with RLS org_id — should inject org_id into new vertex
        run_query(&e, "CREATE (n:Item {name: 'X'})", &[], Some("org_1")).unwrap();
        // Read without RLS — should see org_id property
        let rows = run_query(
            &e,
            "MATCH (n:Item) RETURN n.name AS name, n.org_id AS oid",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_rls_read_filters_by_org() {
        // Design E: query_inner is Internal-only — all nodes visible.
        // Parameter-based RLS removed. Security via query_with_did().
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(
            &e,
            "CREATE (n:Item {name: 'A', org_id: 'org_1'})",
            &[],
            None,
        )
        .unwrap();
        run_query(
            &e,
            "CREATE (n:Item {name: 'B', org_id: 'org_2'})",
            &[],
            None,
        )
        .unwrap();
        run_query(&e, "CREATE (n:Item {name: 'C'})", &[], None).unwrap(); // no org_id
        let rows = run_query(
            &e,
            "MATCH (n:Item) RETURN n.name AS name",
            &[],
            Some("org_1"),
        )
        .unwrap();
        assert_eq!(
            rows.len(),
            3,
            "Internal path: all Item nodes visible (no RLS filtering)"
        );
        let has_a = rows.iter().any(|r| r.iter().any(|(_, v)| v.contains("A")));
        let has_b = rows.iter().any(|r| r.iter().any(|(_, v)| v.contains("B")));
        let has_c = rows.iter().any(|r| r.iter().any(|(_, v)| v.contains("C")));
        assert!(has_a, "Should see node A");
        assert!(has_b, "Should see node B");
        assert!(has_c, "Should see node C");
    }

    // ── RETURN n via GIE path ───────────────────────────────────────

    #[test]
    fn test_return_n_via_gie_produces_json() {
        // End-to-end: engine.query() with RETURN n should produce JSON, not just vid
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Gie {key: 'k1', val: 'hello'})", &[], None).unwrap();
        let rows = run_query(&e, "MATCH (n:Gie {key: 'k1'}) RETURN n", &[], None).unwrap();
        assert_eq!(rows.len(), 1);
        let n_val = &rows[0][0].1;
        assert!(
            n_val.contains("key"),
            "RETURN n should contain 'key' property, got: {n_val}"
        );
        assert!(
            n_val.contains("hello"),
            "RETURN n should contain 'hello' value, got: {n_val}"
        );
    }

    #[test]
    fn test_return_n_multiple_nodes_json() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Multi {id: '1', x: 'a'})", &[], None).unwrap();
        run_query(&e, "CREATE (n:Multi {id: '2', x: 'b'})", &[], None).unwrap();
        let rows = run_query(&e, "MATCH (n:Multi) RETURN n", &[], None).unwrap();
        assert_eq!(rows.len(), 2);
        for row in &rows {
            let json = &row[0].1;
            assert!(
                json.contains("id"),
                "Each node JSON must contain 'id', got: {json}"
            );
        }
    }

    // ── UNWIND / UNION (cypher via engine) ──────────────────────────

    // ── GIE read path ───────────────────────────────────────────────

    #[test]
    fn test_where_eq_filter() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Filt {key: 'k1', val: 'hello'})", &[], None).unwrap();
        run_query(&e, "CREATE (n:Filt {key: 'k2', val: 'world'})", &[], None).unwrap();

        let rows = run_query(
            &e,
            "MATCH (n:Filt {key: 'k1'}) RETURN n.val AS v",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(get_col(&rows, "v").iter().any(|s| s.contains("hello")));
    }

    #[test]
    fn test_where_no_match() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:NM {key: 'exists'})", &[], None).unwrap();

        let rows = run_query(&e, "MATCH (n:NM {key: 'nonexistent'}) RETURN n", &[], None).unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_where_multiple_props() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:MP {a: 'x', b: 'y'})", &[], None).unwrap();
        run_query(&e, "CREATE (n:MP {a: 'x', b: 'z'})", &[], None).unwrap();

        let rows = run_query(
            &e,
            "MATCH (n:MP) WHERE n.a = 'x' AND n.b = 'y' RETURN n.b AS b",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(get_col(&rows, "b").iter().any(|s| s.contains("y")));
    }

    // ── Stress: many nodes + query ──────────────────────────────────

    #[test]
    fn test_stress_100_nodes_list() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        for i in 0..100 {
            run_query(&e, &format!("CREATE (n:Stress {{idx: {i}}})"), &[], None).unwrap();
        }
        let rows = run_query(&e, "MATCH (n:Stress) RETURN n.idx AS idx", &[], None).unwrap();
        assert_eq!(rows.len(), 100);
    }

    #[test]
    fn test_stress_rapid_upsert_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Rapid {rid: 'r1', counter: 0})", &[], None).unwrap();

        for i in 1..=20 {
            // DELETE+CREATE upsert
            run_query(&e, "MATCH (n:Rapid {rid: 'r1'}) DELETE n", &[], None).unwrap();
            run_query(
                &e,
                &format!("CREATE (n:Rapid {{rid: 'r1', counter: {i}}})"),
                &[],
                None,
            )
            .unwrap();

            // Immediate read must return latest value
            let rows = run_query(
                &e,
                "MATCH (n:Rapid {rid: 'r1'}) RETURN n.counter AS c",
                &[],
                None,
            )
            .unwrap();
            assert_eq!(rows.len(), 1, "iter {i}: node must exist");
            assert!(
                get_col(&rows, "c").iter().any(|s| s == &i.to_string()),
                "iter {i}: counter must be {i}"
            );
        }
    }

    // ── Complex query patterns ──────────────────────────────────────

    #[test]
    fn test_where_gt_lt_combined() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        for i in [10, 20, 30, 40, 50] {
            run_query(&e, &format!("CREATE (n:Range {{v: {i}}})"), &[], None).unwrap();
        }
        let rows = run_query(
            &e,
            "MATCH (n:Range) WHERE n.v > 15 AND n.v < 45 RETURN n.v AS v",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 3); // 20, 30, 40
    }

    #[test]
    fn test_count_aggregation() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        for i in 0..5 {
            run_query(&e, &format!("CREATE (n:Cnt {{v: {i}}})"), &[], None).unwrap();
        }
        let rows = run_query(&e, "MATCH (n:Cnt) RETURN count(n) AS c", &[], None).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(get_col(&rows, "c").iter().any(|s| s == "5"));
    }

    #[test]
    fn test_order_by_limit() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        for i in [30, 10, 20, 50, 40] {
            run_query(&e, &format!("CREATE (n:OL {{v: {i}}})"), &[], None).unwrap();
        }
        let rows = run_query(
            &e,
            "MATCH (n:OL) RETURN n.v AS v ORDER BY n.v ASC LIMIT 3",
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 3);
        assert!(get_col(&rows, "v").iter().any(|s| s == "10"));
    }

    #[test]
    fn test_vector_search() {
        // TieredGraphEngine::vector_search → YataVectorStore on Lance vector index
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);

        // Write embeddings via vector store path
        let nodes: Vec<yata_cypher::NodeRef> = (0..100)
            .map(|i| {
                let mut props = indexmap::IndexMap::new();
                props.insert(
                    "name".into(),
                    yata_cypher::types::Value::Str(format!("doc_{i}")),
                );
                // Embedding: unit vector rotated by i degrees
                let angle = (i as f64) * std::f64::consts::PI / 180.0;
                props.insert(
                    "emb".into(),
                    yata_cypher::types::Value::List(vec![
                        yata_cypher::types::Value::Float(angle.cos()),
                        yata_cypher::types::Value::Float(angle.sin()),
                        yata_cypher::types::Value::Float(0.0),
                        yata_cypher::types::Value::Float(0.0),
                    ]),
                );
                yata_cypher::NodeRef {
                    id: format!("doc_{i}"),
                    labels: vec!["Doc".into()],
                    props,
                }
            })
            .collect();

        let count = e.write_embeddings(&nodes, "emb", 4).unwrap();
        assert_eq!(count, 100);

        // Search for vector closest to [1, 0, 0, 0] (= doc_0)
        let results = e
            .vector_search(vec![1.0, 0.0, 0.0, 0.0], 5, None, None)
            .unwrap();
        assert!(!results.is_empty(), "vector search should return results");
        assert!(results.len() <= 5, "should return at most 5 results");

        // doc_0 should be nearest (distance ≈ 0)
        let nearest_vid = &results[0].0.id;
        let nearest_dist = results[0].1;
        assert!(
            nearest_dist < 0.01,
            "nearest distance should be ~0, got {nearest_dist}"
        );
        assert_eq!(
            nearest_vid, "doc_0",
            "nearest should be doc_0, got {nearest_vid}"
        );
    }

    // ── UTF-8 / CJK tests (TieredGraphEngine end-to-end) ────────────

    #[test]
    fn test_utf8_create_and_query_japanese() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(
            &e,
            r#"CREATE (n:Article {id: "ja1", title: "半導体市場が急拡大"})"#,
            &[],
            None,
        )
        .unwrap();
        let rows = run_query(
            &e,
            r#"MATCH (n:Article {id: "ja1"}) RETURN n.title AS title"#,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let title: String = serde_json::from_str(&rows[0][0].1).unwrap();
        assert_eq!(title, "半導体市場が急拡大");
    }

    #[test]
    fn test_utf8_create_with_params() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let params = vec![
            ("id".to_string(), "\"ja2\"".to_string()),
            ("title".to_string(), "\"こんにちは世界\"".to_string()),
        ];
        run_query(
            &e,
            "CREATE (n:Article {id: $id, title: $title})",
            &params,
            None,
        )
        .unwrap();
        let rows = run_query(
            &e,
            r#"MATCH (n:Article {id: "ja2"}) RETURN n.title AS title"#,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let title: String = serde_json::from_str(&rows[0][0].1).unwrap();
        assert_eq!(title, "こんにちは世界");
    }

    #[test]
    fn test_utf8_emoji_and_mixed() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let params = vec![
            ("id".to_string(), "\"e1\"".to_string()),
            (
                "content".to_string(),
                "\"🎮 ゲーム攻略 — Level 42 完了!\"".to_string(),
            ),
        ];
        run_query(
            &e,
            "CREATE (n:Post {id: $id, content: $content})",
            &params,
            None,
        )
        .unwrap();
        let rows = run_query(
            &e,
            r#"MATCH (n:Post {id: "e1"}) RETURN n.content AS content"#,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let content: String = serde_json::from_str(&rows[0][0].1).unwrap();
        assert_eq!(content, "🎮 ゲーム攻略 — Level 42 完了!");
    }

    #[test]
    fn test_utf8_large_text_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let large = "人工知能向け半導体の世界市場が成長を続けている。".repeat(200);
        let params = vec![
            ("id".to_string(), "\"large1\"".to_string()),
            (
                "content".to_string(),
                serde_json::to_string(&large).unwrap(),
            ),
        ];
        run_query(
            &e,
            "CREATE (n:Article {id: $id, content: $content})",
            &params,
            None,
        )
        .unwrap();
        let rows = run_query(
            &e,
            r#"MATCH (n:Article {id: "large1"}) RETURN n.content AS content"#,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let content: String = serde_json::from_str(&rows[0][0].1).unwrap();
        assert_eq!(content, large);
    }

    #[test]
    fn test_utf8_cjk_all_scripts() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let params = vec![
            ("id".to_string(), "\"cjk1\"".to_string()),
            ("ja".to_string(), "\"日本語テスト\"".to_string()),
            ("zh".to_string(), "\"人工智能芯片市场\"".to_string()),
            ("ko".to_string(), "\"반도체 시장 성장\"".to_string()),
        ];
        run_query(
            &e,
            "CREATE (n:I18n {id: $id, ja: $ja, zh: $zh, ko: $ko})",
            &params,
            None,
        )
        .unwrap();
        let rows = run_query(
            &e,
            r#"MATCH (n:I18n {id: "cjk1"}) RETURN n.ja AS ja, n.zh AS zh, n.ko AS ko"#,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        let m: std::collections::HashMap<_, _> = rows[0]
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(
            serde_json::from_str::<String>(m["ja"]).unwrap(),
            "日本語テスト"
        );
        assert_eq!(
            serde_json::from_str::<String>(m["zh"]).unwrap(),
            "人工智能芯片市场"
        );
        assert_eq!(
            serde_json::from_str::<String>(m["ko"]).unwrap(),
            "반도체 시장 성장"
        );
    }

    // Snapshot persistence tests removed (write path moved to PDS Pipeline).

    // ── merge_record / delete_record (read-store write path) ──────────

    #[test]
    fn test_delete_record_removes_vertex() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record(
            "Item",
            "rkey",
            "del-1",
            &[("rkey", PropValue::Str("del-1".into()))],
        )
        .unwrap();
        let deleted = e.delete_record("Item", "rkey", "del-1").unwrap();
        assert!(deleted, "delete_record should return true when vertex found");
    }

    #[test]
    fn test_delete_record_nonexistent_returns_false() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let deleted = e.delete_record("Item", "rkey", "no-such-key").unwrap();
        assert!(!deleted, "delete_record should return false for nonexistent PK");
    }

    #[test]
    fn test_merge_then_delete_then_merge() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record(
            "X",
            "rkey",
            "x1",
            &[
                ("rkey", PropValue::Str("x1".into())),
                ("val", PropValue::Int(1)),
            ],
        )
        .unwrap();
        e.delete_record("X", "rkey", "x1").unwrap();
        // Re-create after delete
        e.merge_record(
            "X",
            "rkey",
            "x1",
            &[
                ("rkey", PropValue::Str("x1".into())),
                ("val", PropValue::Int(2)),
            ],
        )
        .unwrap();
        // Verify re-created record is visible
        let store = e.build_read_store(&["X"]).unwrap();
        assert!(store.find_vertex_by_pk("X", "rkey", &PropValue::Str("x1".into())).is_some(),
            "re-created record must be visible after delete+merge");
    }

    // ══════════════════════════════════════════════════════════════
    // Coverage: LanceDB persistence roundtrip tests
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn test_persist_merge_record_read_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record("Post", "rkey", "p1", &[
            ("rkey", PropValue::Str("p1".into())),
            ("text", PropValue::Str("hello world".into())),
            ("likes", PropValue::Int(42)),
        ]).unwrap();
        e.merge_record("Post", "rkey", "p2", &[
            ("rkey", PropValue::Str("p2".into())),
            ("text", PropValue::Str("second post".into())),
        ]).unwrap();

        let store = e.build_read_store(&[]).unwrap();
        assert_eq!(store.vertex_count(), 2, "must see 2 posts");
        assert!(store.find_vertex_by_pk("Post", "rkey", &PropValue::Str("p1".into())).is_some());
        assert!(store.find_vertex_by_pk("Post", "rkey", &PropValue::Str("p2".into())).is_some());
    }

    #[test]
    fn test_persist_cold_restart_reads_lance() {
        let dir = tempfile::tempdir().unwrap();
        {
            let e = engine_at(dir.path());
            e.merge_record("Doc", "rkey", "d1", &[
                ("rkey", PropValue::Str("d1".into())),
                ("title", PropValue::Str("Persistent".into())),
            ]).unwrap();
            // Engine dropped here — no explicit flush
        }
        // New engine on same directory — cold restart
        {
            let e2 = engine_at(dir.path());
            e2.ensure_lance();
            let store = e2.build_read_store(&[]).unwrap();
            assert_eq!(store.vertex_count(), 1, "cold restart must recover persisted data");
            assert!(store.find_vertex_by_pk("Doc", "rkey", &PropValue::Str("d1".into())).is_some(),
                "cold restart must find the persisted vertex by PK");
        }
    }

    #[test]
    fn test_persist_pk_dedup_upsert() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        // Write same PK three times with different values
        e.merge_record("Item", "rkey", "i1", &[
            ("rkey", PropValue::Str("i1".into())),
            ("version", PropValue::Int(1)),
        ]).unwrap();
        e.merge_record("Item", "rkey", "i1", &[
            ("rkey", PropValue::Str("i1".into())),
            ("version", PropValue::Int(2)),
        ]).unwrap();
        e.merge_record("Item", "rkey", "i1", &[
            ("rkey", PropValue::Str("i1".into())),
            ("version", PropValue::Int(3)),
        ]).unwrap();

        let store = e.build_read_store(&[]).unwrap();
        // PK dedup: only 1 vertex with latest version
        let vid = store.find_vertex_by_pk("Item", "rkey", &PropValue::Str("i1".into()));
        assert!(vid.is_some(), "deduped vertex must exist");
        let version = store.vertex_prop(vid.unwrap(), "version");
        assert_eq!(version, Some(PropValue::Int(3)), "last-writer-wins: version must be 3");
    }

    #[test]
    fn test_persist_delete_tombstone_hides_record() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record("Task", "rkey", "t1", &[
            ("rkey", PropValue::Str("t1".into())),
            ("status", PropValue::Str("active".into())),
        ]).unwrap();

        // Verify visible
        let store1 = e.build_read_store(&["Task"]).unwrap();
        assert!(store1.find_vertex_by_pk("Task", "rkey", &PropValue::Str("t1".into())).is_some());

        // Delete
        let deleted = e.delete_record("Task", "rkey", "t1").unwrap();
        assert!(deleted, "delete existing record must return true");

        // Verify hidden after tombstone
        let store2 = e.build_read_store(&["Task"]).unwrap();
        assert!(store2.find_vertex_by_pk("Task", "rkey", &PropValue::Str("t1".into())).is_none(),
            "tombstoned record must not be visible");
    }

    #[test]
    fn test_persist_delete_edge_tombstone_hides_record() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record("User", "rkey", "alice", &[("rkey", PropValue::Str("alice".into()))]).unwrap();
        e.merge_record("User", "rkey", "bob", &[("rkey", PropValue::Str("bob".into()))]).unwrap();
        e.merge_record("FOLLOWS", "eid", "edge-1", &[
            ("_src", PropValue::Str("alice".into())),
            ("_dst", PropValue::Str("bob".into())),
            ("_src_label", PropValue::Str("User".into())),
            ("_dst_label", PropValue::Str("User".into())),
        ]).unwrap();

        let store1 = e.build_read_store(&[]).unwrap();
        assert_eq!(store1.edge_count(), 1, "edge must be visible before tombstone");

        let deleted = e.delete_record("FOLLOWS", "eid", "edge-1").unwrap();
        assert!(deleted, "delete existing edge must return true");

        let store2 = e.build_read_store(&[]).unwrap();
        assert_eq!(store2.edge_count(), 0, "tombstoned edge must not be visible");
    }

    #[test]
    fn test_persist_multi_label_same_pk_coexist() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        // Same pk_value "alice" under different labels
        e.merge_record("Person", "rkey", "alice", &[
            ("rkey", PropValue::Str("alice".into())),
            ("role", PropValue::Str("human".into())),
        ]).unwrap();
        e.merge_record("Agent", "rkey", "alice", &[
            ("rkey", PropValue::Str("alice".into())),
            ("role", PropValue::Str("bot".into())),
        ]).unwrap();

        let store = e.build_read_store(&[]).unwrap();
        assert_eq!(store.vertex_count(), 2, "same pk_value + different labels = 2 vertices");
        assert!(store.find_vertex_by_pk("Person", "rkey", &PropValue::Str("alice".into())).is_some());
        assert!(store.find_vertex_by_pk("Agent", "rkey", &PropValue::Str("alice".into())).is_some());
    }

    #[test]
    fn test_persist_cypher_edge_write_back() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        // Create two nodes + edge via Cypher
        run_query(&e, "CREATE (a:User {name: 'Alice'})-[r:FOLLOWS]->(b:User {name: 'Bob'})", &[], None).unwrap();

        // Verify nodes persisted
        let store = e.build_read_store(&[]).unwrap();
        assert!(store.vertex_count() >= 2, "must have at least 2 User nodes");
        assert_eq!(store.edge_count(), 1, "must persist 1 FOLLOWS edge");

        // Verify nodes queryable via Cypher
        let rows = run_query(&e, "MATCH (u:User) RETURN u.name AS name LIMIT 10", &[], None).unwrap();
        assert_eq!(rows.len(), 2, "must find 2 User nodes via Cypher");
        let names = get_col(&rows, "name");
        assert!(names.iter().any(|n| n.contains("Alice")), "Alice must be in results");
        assert!(names.iter().any(|n| n.contains("Bob")), "Bob must be in results");

        let traverse_rows = run_query(
            &e,
            "MATCH (a:User {name: 'Alice'})-[:FOLLOWS]->(b:User) RETURN b.name AS name LIMIT 10",
            &[],
            None,
        ).unwrap();
        let followed = get_col(&traverse_rows, "name");
        assert!(followed.iter().any(|n| n.contains("Bob")), "Bob must be reachable over FOLLOWS");
    }

    #[test]
    fn test_collect_src_vid_candidates_from_source_where() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (a:User {name: 'Alice'})-[r:FOLLOWS]->(b:User {name: 'Bob'})", &[], None).unwrap();
        run_query(&e, "CREATE (a:User {name: 'Carol'})-[r:FOLLOWS]->(b:User {name: 'Dave'})", &[], None).unwrap();

        let ast = yata_cypher::parse(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN b.name AS name LIMIT 10"
        ).unwrap();
        let candidates = e.collect_src_vid_candidates(&ast, &HashMap::new()).unwrap();
        assert_eq!(candidates.len(), 1, "source WHERE should narrow src_vid candidates to one vertex");
    }

    #[test]
    fn test_collect_dst_vid_candidates_from_destination_where() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (a:User {name: 'Alice'})-[r:FOLLOWS]->(b:User {name: 'Bob'})", &[], None).unwrap();
        run_query(&e, "CREATE (a:User {name: 'Carol'})-[r:FOLLOWS]->(b:User {name: 'Dave'})", &[], None).unwrap();

        let ast = yata_cypher::parse(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE b.name = 'Bob' RETURN a.name AS name LIMIT 10"
        ).unwrap();
        let candidates = e.collect_dst_vid_candidates(&ast, &HashMap::new()).unwrap();
        assert_eq!(candidates.len(), 1, "destination WHERE should narrow dst_vid candidates to one vertex");
    }

    #[test]
    fn test_persist_cypher_edge_read_with_destination_where() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (a:User {name: 'Alice'})-[r:FOLLOWS]->(b:User {name: 'Bob'})", &[], None).unwrap();
        run_query(&e, "CREATE (a:User {name: 'Carol'})-[r:FOLLOWS]->(b:User {name: 'Dave'})", &[], None).unwrap();

        let rows = run_query(
            &e,
            "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE b.name = 'Bob' RETURN a.name AS name LIMIT 10",
            &[],
            None,
        ).unwrap();
        let names = get_col(&rows, "name");
        assert_eq!(rows.len(), 1);
        assert!(names.iter().any(|n| n.contains("Alice")), "Alice must match destination-side filter");
    }

    #[test]
    fn test_persist_cypher_variable_hop_with_destination_where() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (a:User {name: 'Alice'})-[:FOLLOWS]->(b:User {name: 'Bob'})-[:FOLLOWS]->(c:User {name: 'Carol'})", &[], None).unwrap();
        run_query(&e, "CREATE (a:User {name: 'Dave'})-[r:FOLLOWS]->(b:User {name: 'Eve'})", &[], None).unwrap();

        let rows = run_query(
            &e,
            "MATCH (a:User {name: 'Alice'})-[:FOLLOWS*1..2]->(b:User) WHERE b.name = 'Carol' RETURN b.name AS name LIMIT 10",
            &[],
            None,
        ).unwrap();
        let names = get_col(&rows, "name");
        assert_eq!(rows.len(), 1);
        assert!(names.iter().any(|n| n.contains("Carol")), "Carol must be reachable via variable-hop destination filter");
    }

    #[test]
    fn test_persist_cypher_multi_expand_chain_with_destination_where() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (a:User {name: 'Alice'})-[:FOLLOWS]->(b:User {name: 'Bob'})-[:FOLLOWS]->(c:User {name: 'Carol'})", &[], None).unwrap();
        run_query(&e, "CREATE (a:User {name: 'Dave'})-[:FOLLOWS]->(b:User {name: 'Eve'})-[:FOLLOWS]->(c:User {name: 'Mallory'})", &[], None).unwrap();

        let rows = run_query(
            &e,
            "MATCH (a:User {name: 'Alice'})-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) WHERE c.name = 'Carol' RETURN c.name AS name LIMIT 10",
            &[],
            None,
        ).unwrap();
        let names = get_col(&rows, "name");
        assert_eq!(rows.len(), 1);
        assert!(names.iter().any(|n| n.contains("Carol")), "Carol must match chained expand destination filter");
    }

    #[test]
    fn test_refine_traversal_strategy_with_stats_prefers_staged_for_small_source() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (a:User {name: 'Alice'})-[:FOLLOWS]->(b:User {name: 'Bob'})", &[], None).unwrap();
        run_query(&e, "CREATE (a:User {name: 'Carol'})-[:FOLLOWS]->(b:User {name: 'Dave'})", &[], None).unwrap();

        let ast = yata_cypher::parse(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN b.name AS name LIMIT 10"
        ).unwrap();
        let mut plan = yata_gie::transpile::transpile(&ast).unwrap();
        e.refine_traversal_strategy_with_stats(&mut plan, &ast, &HashMap::new(), Some(10));

        assert!(matches!(
            plan.ops.iter().find(|op| matches!(op, yata_gie::ir::LogicalOp::Expand { .. })),
            Some(yata_gie::ir::LogicalOp::Expand {
                strategy: yata_gie::ir::TraversalStrategy::PreferStaged,
                ..
            })
        ));
    }

    #[test]
    fn test_refine_traversal_strategy_with_stats_prefers_gie_for_broad_source() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        for i in 0..300 {
            run_query(
                &e,
                &format!("CREATE (a:User {{name: 'User{i}'}})-[:FOLLOWS]->(b:User {{name: 'Dst{i}'}})"),
                &[],
                None,
            ).unwrap();
        }

        let ast = yata_cypher::parse(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.name AS name LIMIT 10"
        ).unwrap();
        let mut plan = yata_gie::transpile::transpile(&ast).unwrap();
        e.refine_traversal_strategy_with_stats(&mut plan, &ast, &HashMap::new(), Some(10));

        assert!(matches!(
            plan.ops.iter().find(|op| matches!(op, yata_gie::ir::LogicalOp::Expand { .. })),
            Some(yata_gie::ir::LogicalOp::Expand {
                strategy: yata_gie::ir::TraversalStrategy::PreferGie,
                ..
            })
        ));
    }

    #[test]
    fn test_refine_traversal_strategy_with_stats_prefers_gie_for_broad_edge_label() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (a:User {name: 'Alice'})-[:FOLLOWS]->(b:User {name: 'Bob'})", &[], None).unwrap();
        for i in 0..300 {
            run_query(
                &e,
                &format!("CREATE (a:User {{name: 'User{i}'}})-[:FOLLOWS]->(b:User {{name: 'Dst{i}'}})"),
                &[],
                None,
            ).unwrap();
        }

        let ast = yata_cypher::parse(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN b.name AS name LIMIT 10"
        ).unwrap();
        let mut plan = yata_gie::transpile::transpile(&ast).unwrap();
        e.refine_traversal_strategy_with_stats(&mut plan, &ast, &HashMap::new(), Some(10));

        assert!(matches!(
            plan.ops.iter().find(|op| matches!(op, yata_gie::ir::LogicalOp::Expand { .. })),
            Some(yata_gie::ir::LogicalOp::Expand {
                strategy: yata_gie::ir::TraversalStrategy::PreferGie,
                ..
            })
        ));
    }

    #[test]
    fn test_refine_traversal_strategy_with_stats_prefers_gie_for_tombstone_heavy_edge_label() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record("User", "rkey", "alice", &[("rkey", PropValue::Str("alice".into())), ("name", PropValue::Str("Alice".into()))]).unwrap();
        e.merge_record("User", "rkey", "bob", &[("rkey", PropValue::Str("bob".into())), ("name", PropValue::Str("Bob".into()))]).unwrap();
        e.merge_record("FOLLOWS", "eid", "live-1", &[
            ("_src", PropValue::Str("alice".into())),
            ("_dst", PropValue::Str("bob".into())),
            ("_src_label", PropValue::Str("User".into())),
            ("_dst_label", PropValue::Str("User".into())),
        ]).unwrap();
        for i in 0..40 {
            let eid = format!("dead-{i}");
            e.merge_record("FOLLOWS", "eid", &eid, &[
                ("_src", PropValue::Str("alice".into())),
                ("_dst", PropValue::Str("bob".into())),
                ("_src_label", PropValue::Str("User".into())),
                ("_dst_label", PropValue::Str("User".into())),
            ]).unwrap();
            e.delete_record("FOLLOWS", "eid", &eid).unwrap();
        }

        let ast = yata_cypher::parse(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN b.name AS name LIMIT 10"
        ).unwrap();
        let mut plan = yata_gie::transpile::transpile(&ast).unwrap();
        e.refine_traversal_strategy_with_stats(&mut plan, &ast, &HashMap::new(), Some(10));

        assert!(matches!(
            plan.ops.iter().find(|op| matches!(op, yata_gie::ir::LogicalOp::Expand { .. })),
            Some(yata_gie::ir::LogicalOp::Expand {
                strategy: yata_gie::ir::TraversalStrategy::PreferGie,
                ..
            })
        ));
    }

    #[test]
    fn test_count_stats_cache_populates_and_invalidates_on_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (a:User {name: 'Alice'})-[:FOLLOWS]->(b:User {name: 'Bob'})", &[], None).unwrap();

        let vertex_filter = "label = 'User' AND name = 'Alice'";
        let edge_filter = "edge_label = 'FOLLOWS' AND src_label = 'User'";

        assert_eq!(e.estimate_vertex_filter_cardinality(vertex_filter), Some(1));
        assert_eq!(e.estimate_edge_filter_cardinality(edge_filter), Some(1));
        assert!(e.cached_count_stat(&format!("vertex:{vertex_filter}")).is_some());
        assert!(e.cached_count_stat(&format!("edge:{edge_filter}")).is_some());

        run_query(&e, "CREATE (a:User {name: 'Carol'})-[:FOLLOWS]->(b:User {name: 'Dave'})", &[], None).unwrap();

        assert!(e.cached_count_stat(&format!("vertex:{vertex_filter}")).is_none());
        assert!(e.cached_count_stat(&format!("edge:{edge_filter}")).is_none());
    }

    #[test]
    fn test_vertex_pressure_stats_are_tombstone_aware() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record("Task", "rkey", "t1", &[("rkey", PropValue::Str("t1".into()))]).unwrap();
        e.merge_record("Task", "rkey", "t2", &[("rkey", PropValue::Str("t2".into()))]).unwrap();
        e.delete_record("Task", "rkey", "t1").unwrap();

        let stat = e.estimate_table_pressure(TableKind::Vertices, Some("Task")).unwrap();
        assert_eq!(stat.raw_rows, 3);
        assert_eq!(stat.live_rows, 1);
        assert_eq!(stat.dead_rows, 2);
        assert!(stat.dead_ratio > 0.6);
    }

    #[test]
    fn test_edge_pressure_stats_are_tombstone_aware() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record("User", "rkey", "alice", &[("rkey", PropValue::Str("alice".into()))]).unwrap();
        e.merge_record("User", "rkey", "bob", &[("rkey", PropValue::Str("bob".into()))]).unwrap();
        e.merge_record("FOLLOWS", "eid", "e1", &[
            ("_src", PropValue::Str("alice".into())),
            ("_dst", PropValue::Str("bob".into())),
            ("_src_label", PropValue::Str("User".into())),
            ("_dst_label", PropValue::Str("User".into())),
        ]).unwrap();
        e.delete_record("FOLLOWS", "eid", "e1").unwrap();

        let stat = e.estimate_table_pressure(TableKind::Edges, Some("FOLLOWS")).unwrap();
        assert_eq!(stat.raw_rows, 2);
        assert_eq!(stat.live_rows, 0);
        assert_eq!(stat.dead_rows, 2);
        assert!(stat.dead_ratio > 0.9);
    }

    #[test]
    fn test_pressure_stats_catalog_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record("Task", "rkey", "t1", &[("rkey", PropValue::Str("t1".into()))]).unwrap();
        e.delete_record("Task", "rkey", "t1").unwrap();

        let key = TieredGraphEngine::pressure_cache_key(TableKind::Vertices, Some("Task"));
        let stat = e.estimate_table_pressure(TableKind::Vertices, Some("Task")).unwrap();
        assert!(stat.dead_rows >= 1);

        let e2 = engine_at(dir.path());
        e2.cold_start().unwrap();
        let restored = e2.cached_pressure_stat(&key).unwrap();
        assert!(restored.dead_rows >= 1);
        assert!(restored.dead_ratio > 0.0);
    }

    #[test]
    fn test_planner_traversal_stats_view_exposes_avg_out_degree_and_dead_ratio() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (a:User {name: 'Alice'})-[:FOLLOWS]->(b:User {name: 'Bob'})", &[], None).unwrap();
        run_query(&e, "CREATE (a:User {name: 'Alice'})-[:FOLLOWS]->(b:User {name: 'Carol'})", &[], None).unwrap();
        e.delete_record("FOLLOWS", "eid", "missing-edge").ok(); // no-op path, keeps API covered

        let view = e.planner_traversal_stats_view(Some("User"), "FOLLOWS").unwrap();
        assert_eq!(view.source.label.as_deref(), Some("User"));
        assert_eq!(view.edge.label.as_deref(), Some("FOLLOWS"));
        assert!(view.source.live_rows >= 2);
        assert!(view.edge.live_rows >= 2);
        assert!(view.avg_out_degree >= 0.5);
        assert_eq!(view.edge.last_compacted_at_ms, e.last_compaction_ms.load(Ordering::Relaxed));
    }

    #[test]
    fn test_explain_query_plan_returns_traversal_strategy_rows() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (a:User {name: 'Alice'})-[:FOLLOWS]->(b:User {name: 'Bob'})", &[], None).unwrap();

        let rows = run_query(
            &e,
            "EXPLAIN MATCH (a:User)-[:FOLLOWS]->(b:User) WHERE a.name = 'Alice' RETURN b.name AS name LIMIT 10",
            &[],
            None,
        ).unwrap();

        assert!(rows.len() >= 4);
        let ops: Vec<String> = rows
            .iter()
            .filter_map(|row| row.iter().find(|(k, _)| k == "op").map(|(_, v)| v.clone()))
            .collect();
        assert!(ops.iter().any(|op| op == "Scan"));
        assert!(ops.iter().any(|op| op == "Expand"));
        assert!(ops.iter().any(|op| op == "Project"));
        assert!(ops.iter().any(|op| op == "Limit"));

        let expand_row = rows.iter().find(|row| row.iter().any(|(k, v)| k == "op" && v == "Expand")).unwrap();
        let strategy = expand_row.iter().find(|(k, _)| k == "strategy").map(|(_, v)| v.clone()).unwrap_or_default();
        let edge_label = expand_row.iter().find(|(k, _)| k == "edge_label").map(|(_, v)| v.clone()).unwrap_or_default();
        let avg_out_degree = expand_row.iter().find(|(k, _)| k == "avg_out_degree").map(|(_, v)| v.clone()).unwrap_or_default();
        assert_eq!(edge_label, "FOLLOWS");
        assert!(!strategy.is_empty());
        assert!(!avg_out_degree.is_empty());
    }

    #[test]
    fn test_persist_cypher_delete_removes_from_lance() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        run_query(&e, "CREATE (n:Temp {tid: 'remove-me'})", &[], None).unwrap();

        // Verify created
        let rows1 = run_query(&e, "MATCH (n:Temp) RETURN n.tid AS tid LIMIT 10", &[], None).unwrap();
        assert_eq!(rows1.len(), 1);

        // Delete via Cypher
        run_query(&e, "MATCH (n:Temp {tid: 'remove-me'}) DELETE n", &[], None).unwrap();

        // Verify deleted
        let rows2 = run_query(&e, "MATCH (n:Temp) RETURN n.tid AS tid LIMIT 10", &[], None).unwrap();
        assert_eq!(rows2.len(), 0, "deleted node must not appear in query");
    }

    #[test]
    fn test_persist_batch_mutation_write_back() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let ctx = MutationContext {
            app_id: "test".into(), org_id: "org1".into(),
            user_id: "".into(), actor_id: "".into(),
            user_did: "".into(), actor_did: "".into(),
        };
        let stmts: Vec<(&str, &[(String, String)])> = vec![
            ("CREATE (a:BatchNode {bid: 'b1'})", &[]),
            ("CREATE (b:BatchNode {bid: 'b2'})", &[]),
            ("CREATE (c:BatchNode {bid: 'b3'})", &[]),
        ];
        let results = e.batch_query_with_context(&stmts, None, &ctx).unwrap();
        assert_eq!(results.len(), 3);

        // Verify all 3 nodes persisted
        let rows = run_query(&e, "MATCH (n:BatchNode) RETURN n.bid AS bid LIMIT 10", &[], None).unwrap();
        assert_eq!(rows.len(), 3, "batch mutation must persist all 3 nodes");
    }

    #[test]
    fn test_persist_cold_restart_after_cypher_mutation() {
        let dir = tempfile::tempdir().unwrap();
        {
            let e = engine_at(dir.path());
            run_query(&e, "CREATE (n:Durable {key: 'survives-restart', val: 99})", &[], None).unwrap();
        }
        // Cold restart
        {
            let e2 = engine_at(dir.path());
            let rows = run_query(&e2, "MATCH (n:Durable) RETURN n.key AS key, n.val AS val LIMIT 10", &[], None).unwrap();
            assert_eq!(rows.len(), 1, "Cypher-created node must survive cold restart");
            assert!(get_col(&rows, "key").iter().any(|k| k.contains("survives-restart")));
        }
    }

    #[test]
    fn test_persist_delete_nonexistent_returns_false() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        let deleted = e.delete_record("Item", "rkey", "no-such-key").unwrap();
        assert!(!deleted, "delete_record should return false for nonexistent PK");
    }

    #[test]
    fn test_persist_merge_record_props_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let e = make_engine(&dir);
        e.merge_record("Rich", "rkey", "r1", &[
            ("rkey", PropValue::Str("r1".into())),
            ("name", PropValue::Str("日本語テスト".into())),
            ("count", PropValue::Int(12345)),
            ("ratio", PropValue::Float(3.14)),
            ("active", PropValue::Bool(true)),
        ]).unwrap();

        let store = e.build_read_store(&[]).unwrap();
        let vid = store.find_vertex_by_pk("Rich", "rkey", &PropValue::Str("r1".into())).unwrap();
        assert_eq!(store.vertex_prop(vid, "name"), Some(PropValue::Str("日本語テスト".into())));
        assert_eq!(store.vertex_prop(vid, "count"), Some(PropValue::Int(12345)));
        // Float roundtrip via JSON may lose precision — check approximate
        if let Some(PropValue::Float(f)) = store.vertex_prop(vid, "ratio") {
            assert!((f - 3.14).abs() < 0.001, "float roundtrip: {f}");
        }
    }
}
