//! yata-lance — Lance Dataset persistence for yata graph.
//!
//! Thin wrapper around `lance::Dataset` for COO sorted graph data.
//! All manifest, fragment, compaction, and MVCC management is delegated to Lance.

pub mod catalog;
pub mod dataset;
pub mod manifest;
pub mod schema;
pub mod vector;
pub mod writer;

pub use catalog::GraphCatalog;
pub use dataset::YataDataset;
pub use dataset::{
    create_edge_live_in_dataset,
    create_edge_live_out_dataset,
    create_edge_log_dataset,
    create_vertex_live_dataset,
    create_vertex_log_dataset,
};
pub use manifest::{
    GraphManifest,
    GraphManifestKeyLayout,
    GraphManifestLatestPointer,
    GraphManifestSeqRange,
    GraphManifestTableRef,
    GraphManifestTables,
    ManifestStore,
    OpenedGraphDatasets,
    S3ManifestStore,
    load_manifest,
    load_manifest_via_latest,
    load_latest_pointer,
    parse_latest_pointer_json,
    parse_manifest_json,
    publish_manifest,
    render_latest_pointer_json,
    save_manifest,
    render_manifest_json,
};
pub use schema::{
    EDGE_LIVE_IN_TABLE,
    EDGE_LIVE_OUT_TABLE,
    EDGE_LOG_TABLE,
    GRAPH_FORMAT,
    VERTEX_LIVE_TABLE,
    VERTEX_LOG_TABLE,
    edge_live_in_schema,
    edge_live_out_schema,
    edge_log_schema,
    vertex_live_schema,
    vertex_log_schema,
};
pub use vector::YataVectorStore;
pub use writer::{EdgeMutation, VertexMutation, apply_edge_mutation, apply_vertex_mutation};
