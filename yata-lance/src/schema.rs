//! Lance table schemas for COO sorted graph data.

use arrow_schema::{DataType, Field, Schema};
use once_cell::sync::Lazy;
use std::sync::Arc;

/// Vertices table schema — one row per graph vertex.
pub static VERTICES_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("label", DataType::Utf8, false),
        Field::new("rkey", DataType::Utf8, false),
        Field::new("collection", DataType::Utf8, true),
        Field::new("value_b64", DataType::Utf8, true),
        Field::new("repo", DataType::Utf8, true),
        Field::new("updated_at", DataType::Int64, true),
        Field::new("sensitivity_ord", DataType::UInt8, true),
        Field::new("owner_hash", DataType::UInt32, true),
    ]))
});

/// Edges table schema — one row per graph edge.
pub static EDGES_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("src_label", DataType::Utf8, false),
        Field::new("src_rkey", DataType::Utf8, false),
        Field::new("edge_label", DataType::Utf8, false),
        Field::new("dst_label", DataType::Utf8, false),
        Field::new("dst_rkey", DataType::Utf8, false),
    ]))
});
