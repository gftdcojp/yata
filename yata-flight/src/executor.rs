//! SQL execution engine: SqlPlan → Arrow RecordBatch.
//!
//! Maps SQL operations to MutableCsrStore operations:
//! - Table name → vertex/edge label
//! - Column projection → Arrow column selection
//! - WHERE → B-tree index (range) or hash index (equality) or columnar scan
//! - ORDER BY → B-tree order_by
//! - LIMIT/OFFSET → slice result

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

use yata_grin::{Predicate, PropValue, Property, Scannable, Schema as GrinSchema};
use yata_store::MutableCsrStore;

use crate::sql_plan::{CompareOp, SqlPlan, WherePred};

#[derive(thiserror::Error, Debug)]
pub enum ExecError {
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

/// Execute a SQL plan against a MutableCsrStore, returning an Arrow RecordBatch.
pub fn execute(store: &MutableCsrStore, plan: &SqlPlan) -> Result<RecordBatch, ExecError> {
    // Check if table is a vertex or edge label
    let vertex_labels = <MutableCsrStore as GrinSchema>::vertex_labels(store);
    let edge_labels = <MutableCsrStore as GrinSchema>::edge_labels(store);

    if vertex_labels.contains(&plan.table) {
        execute_vertex_query(store, plan)
    } else if edge_labels.contains(&plan.table) {
        execute_edge_query(store, plan)
    } else {
        Err(ExecError::TableNotFound(plan.table.clone()))
    }
}

fn execute_vertex_query(
    store: &MutableCsrStore,
    plan: &SqlPlan,
) -> Result<RecordBatch, ExecError> {
    let label = &plan.table;

    // Get candidate VIDs: apply B-tree range index if available, otherwise full label scan
    let (mut vids, btree_handled_columns) = get_candidate_vids(store, plan);

    // Apply WHERE predicates (filter) — skip columns already handled by B-tree
    for pred in &plan.predicates {
        if btree_handled_columns.contains(&pred.column) {
            continue;
        }
        let grin_pred = where_to_predicate(pred);
        let filtered = store.scan_vertices(label, &grin_pred);
        let filtered_set: std::collections::HashSet<u32> = filtered.into_iter().collect();
        vids.retain(|v| filtered_set.contains(v));
    }

    // Apply ORDER BY via B-tree index
    if !plan.order_by.is_empty() {
        let order = &plan.order_by[0]; // primary sort only
        let ordered = store.btree_order_by(
            label,
            &order.column,
            order.ascending,
            vids.len(), // get all, then apply offset+limit
        );
        if !ordered.is_empty() {
            // Intersect ordered with remaining candidates
            let candidate_set: std::collections::HashSet<u32> = vids.iter().copied().collect();
            vids = ordered
                .into_iter()
                .filter(|v| candidate_set.contains(v))
                .collect();
        }
    }

    // Apply OFFSET
    if let Some(offset) = plan.offset {
        if offset < vids.len() {
            vids = vids[offset..].to_vec();
        } else {
            vids.clear();
        }
    }

    // Apply LIMIT
    if let Some(limit) = plan.limit {
        vids.truncate(limit);
    }

    // Determine columns to output
    let all_prop_keys = store.vertex_prop_keys(label);
    let output_cols = if plan.columns.is_empty() {
        all_prop_keys
    } else {
        plan.columns.clone()
    };

    // Build Arrow RecordBatch
    build_vertex_batch(store, &vids, &output_cols)
}

fn execute_edge_query(
    store: &MutableCsrStore,
    plan: &SqlPlan,
) -> Result<RecordBatch, ExecError> {
    // For edge queries, iterate all edges with matching label
    let label = &plan.table;
    let edge_count = store.edge_count_raw();

    let mut eids: Vec<u32> = (0..edge_count as u32)
        .filter(|&eid| {
            store.edge_label(eid).map(|l| l == label).unwrap_or(false)
                && store.edge_alive(eid)
        })
        .collect();

    // Apply LIMIT/OFFSET
    if let Some(offset) = plan.offset {
        if offset < eids.len() {
            eids = eids[offset..].to_vec();
        } else {
            eids.clear();
        }
    }
    if let Some(limit) = plan.limit {
        eids.truncate(limit);
    }

    let all_prop_keys = store.edge_prop_keys(label);
    let output_cols = if plan.columns.is_empty() {
        // Include _src, _dst for edge tables
        let mut cols = vec!["_src".to_string(), "_dst".to_string()];
        cols.extend(all_prop_keys);
        cols
    } else {
        plan.columns.clone()
    };

    build_edge_batch(store, &eids, &output_cols)
}

/// Returns (candidate VIDs, set of column names handled by B-tree index).
fn get_candidate_vids(
    store: &MutableCsrStore,
    plan: &SqlPlan,
) -> (Vec<u32>, std::collections::HashSet<String>) {
    let mut handled = std::collections::HashSet::new();

    // Collect range bounds per column from predicates
    let mut lower_bounds: std::collections::HashMap<String, PropValue> =
        std::collections::HashMap::new();
    let mut upper_bounds: std::collections::HashMap<String, PropValue> =
        std::collections::HashMap::new();

    for pred in &plan.predicates {
        if !store.btree_index().is_registered(&plan.table, &pred.column) {
            continue;
        }
        match pred.op {
            CompareOp::Gte | CompareOp::Gt => {
                lower_bounds.insert(pred.column.clone(), pred.value.clone());
            }
            CompareOp::Lte | CompareOp::Lt => {
                upper_bounds.insert(pred.column.clone(), pred.value.clone());
            }
            CompareOp::Eq => {
                lower_bounds.insert(pred.column.clone(), pred.value.clone());
                upper_bounds.insert(pred.column.clone(), pred.value.clone());
            }
            _ => {}
        }
    }

    // If we have range predicates on a B-tree-indexed column, use it
    let all_columns: std::collections::HashSet<String> = lower_bounds
        .keys()
        .chain(upper_bounds.keys())
        .cloned()
        .collect();

    let mut result: Option<Vec<u32>> = None;

    for col in &all_columns {
        let lo = lower_bounds.get(col);
        let hi = upper_bounds.get(col);
        let vids = store.btree_range(&plan.table, col, lo, hi);
        handled.insert(col.clone());

        result = Some(match result {
            Some(prev) => {
                let set: std::collections::HashSet<u32> = vids.into_iter().collect();
                prev.into_iter().filter(|v| set.contains(v)).collect()
            }
            None => vids,
        });
    }

    match result {
        Some(vids) if !vids.is_empty() => (vids, handled),
        _ => {
            // Fallback: full label scan
            (store.scan_vertices(&plan.table, &Predicate::True), std::collections::HashSet::new())
        }
    }
}

fn where_to_predicate(pred: &WherePred) -> Predicate {
    match pred.op {
        CompareOp::Eq => Predicate::Eq(pred.column.clone(), pred.value.clone()),
        CompareOp::Neq => Predicate::Neq(pred.column.clone(), pred.value.clone()),
        CompareOp::Lt | CompareOp::Lte => Predicate::Lt(pred.column.clone(), pred.value.clone()),
        CompareOp::Gt | CompareOp::Gte => Predicate::Gt(pred.column.clone(), pred.value.clone()),
    }
}

fn build_vertex_batch(
    store: &MutableCsrStore,
    vids: &[u32],
    columns: &[String],
) -> Result<RecordBatch, ExecError> {
    if columns.is_empty() || vids.is_empty() {
        let schema = Arc::new(Schema::empty());
        return Ok(RecordBatch::new_empty(schema));
    }

    // Infer types from first non-null value per column
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for col_name in columns {
        let (field, array) = build_column(store, vids, col_name, true)?;
        fields.push(field);
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn build_edge_batch(
    store: &MutableCsrStore,
    eids: &[u32],
    columns: &[String],
) -> Result<RecordBatch, ExecError> {
    if columns.is_empty() || eids.is_empty() {
        let schema = Arc::new(Schema::empty());
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for col_name in columns {
        if col_name == "_src" || col_name == "_dst" {
            let mut builder = Int64Builder::with_capacity(eids.len());
            for &eid in eids {
                let val = if col_name == "_src" {
                    store.edge_src(eid).unwrap_or(0) as i64
                } else {
                    store.edge_dst(eid).unwrap_or(0) as i64
                };
                builder.append_value(val);
            }
            fields.push(Field::new(col_name.as_str(), DataType::Int64, false));
            arrays.push(Arc::new(builder.finish()));
        } else {
            let (field, array) = build_edge_column(store, eids, col_name)?;
            fields.push(field);
            arrays.push(array);
        }
    }

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn build_column(
    store: &MutableCsrStore,
    vids: &[u32],
    col_name: &str,
    _is_vertex: bool,
) -> Result<(Field, ArrayRef), ExecError> {
    // Collect values
    let values: Vec<Option<PropValue>> = vids
        .iter()
        .map(|&vid| store.vertex_prop(vid, col_name))
        .collect();

    // Infer type from first non-null
    let inferred_type = values
        .iter()
        .flatten()
        .next()
        .map(prop_to_arrow_type)
        .unwrap_or(DataType::Utf8);

    let field = Field::new(col_name, inferred_type.clone(), true);
    let array = build_array(&inferred_type, &values);
    Ok((field, array))
}

fn build_edge_column(
    store: &MutableCsrStore,
    eids: &[u32],
    col_name: &str,
) -> Result<(Field, ArrayRef), ExecError> {
    let values: Vec<Option<PropValue>> = eids
        .iter()
        .map(|&eid| store.edge_prop(eid, col_name))
        .collect();

    let inferred_type = values
        .iter()
        .flatten()
        .next()
        .map(prop_to_arrow_type)
        .unwrap_or(DataType::Utf8);

    let field = Field::new(col_name, inferred_type.clone(), true);
    let array = build_array(&inferred_type, &values);
    Ok((field, array))
}

fn prop_to_arrow_type(val: &PropValue) -> DataType {
    match val {
        PropValue::Int(_) => DataType::Int64,
        PropValue::Float(_) => DataType::Float64,
        PropValue::Bool(_) => DataType::Boolean,
        PropValue::Str(_) => DataType::Utf8,
        PropValue::Null => DataType::Utf8,
    }
}

fn build_array(dt: &DataType, values: &[Option<PropValue>]) -> ArrayRef {
    match dt {
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Some(PropValue::Int(n)) => b.append_value(*n),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Some(PropValue::Float(n)) => b.append_value(*n),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    Some(PropValue::Bool(n)) => b.append_value(*n),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        _ => {
            // Default: Utf8
            let mut b = StringBuilder::with_capacity(values.len(), values.len() * 16);
            for v in values {
                match v {
                    Some(PropValue::Str(s)) => b.append_value(s),
                    Some(other) => b.append_value(format!("{:?}", other)),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    }
}
