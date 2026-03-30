use std::sync::LazyLock;

use arrow_schema::{DataType, Field, Schema};

pub const GRAPH_FORMAT: &str = "yata-lance-graph-v1";

pub const VERTEX_LOG_TABLE: &str = "yata_vertex_log";
pub const EDGE_LOG_TABLE: &str = "yata_edge_log";
pub const VERTEX_LIVE_TABLE: &str = "yata_vertex_live";
pub const EDGE_LIVE_OUT_TABLE: &str = "yata_edge_live_out";
pub const EDGE_LIVE_IN_TABLE: &str = "yata_edge_live_in";

fn utf8(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable)
}

fn u32_field(name: &str) -> Field {
    Field::new(name, DataType::UInt32, false)
}

fn u64_field(name: &str) -> Field {
    Field::new(name, DataType::UInt64, false)
}

fn bool_field(name: &str) -> Field {
    Field::new(name, DataType::Boolean, false)
}

pub static VERTEX_LOG_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        u32_field("partition_id"),
        u64_field("seq"),
        utf8("tx_id", false),
        utf8("op", false),
        utf8("label", false),
        utf8("pk_key", false),
        utf8("pk_value", false),
        utf8("vid", false),
        utf8("repo", true),
        utf8("rkey", true),
        utf8("owner_did", true),
        u64_field("created_at_ms"),
        u64_field("updated_at_ms"),
        bool_field("tombstone"),
        utf8("props_json", true),
        utf8("props_hash", true),
    ])
});

pub static EDGE_LOG_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        u32_field("partition_id"),
        u64_field("seq"),
        utf8("tx_id", false),
        utf8("op", false),
        utf8("edge_label", false),
        utf8("pk_key", false),
        utf8("pk_value", false),
        utf8("eid", false),
        utf8("src_vid", false),
        utf8("dst_vid", false),
        utf8("src_label", true),
        utf8("dst_label", true),
        u64_field("created_at_ms"),
        u64_field("updated_at_ms"),
        bool_field("tombstone"),
        utf8("props_json", true),
        utf8("props_hash", true),
    ])
});

pub static VERTEX_LIVE_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        u32_field("partition_id"),
        utf8("label", false),
        utf8("pk_key", false),
        utf8("pk_value", false),
        utf8("vid", false),
        bool_field("alive"),
        u64_field("latest_seq"),
        utf8("repo", true),
        utf8("rkey", true),
        utf8("owner_did", true),
        u64_field("updated_at_ms"),
        utf8("props_json", true),
    ])
});

pub static EDGE_LIVE_OUT_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        u32_field("partition_id"),
        utf8("edge_label", false),
        utf8("pk_key", false),
        utf8("pk_value", false),
        utf8("eid", false),
        utf8("src_vid", false),
        utf8("dst_vid", false),
        utf8("src_label", true),
        utf8("dst_label", true),
        bool_field("alive"),
        u64_field("latest_seq"),
        u64_field("updated_at_ms"),
        utf8("props_json", true),
    ])
});

pub static EDGE_LIVE_IN_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        u32_field("partition_id"),
        utf8("edge_label", false),
        utf8("pk_key", false),
        utf8("pk_value", false),
        utf8("eid", false),
        utf8("dst_vid", false),
        utf8("src_vid", false),
        utf8("src_label", true),
        utf8("dst_label", true),
        bool_field("alive"),
        u64_field("latest_seq"),
        u64_field("updated_at_ms"),
        utf8("props_json", true),
    ])
});

pub fn vertex_log_schema() -> &'static Schema {
    &VERTEX_LOG_SCHEMA
}

pub fn edge_log_schema() -> &'static Schema {
    &EDGE_LOG_SCHEMA
}

pub fn vertex_live_schema() -> &'static Schema {
    &VERTEX_LIVE_SCHEMA
}

pub fn edge_live_out_schema() -> &'static Schema {
    &EDGE_LIVE_OUT_SCHEMA
}

pub fn edge_live_in_schema() -> &'static Schema {
    &EDGE_LIVE_IN_SCHEMA
}
