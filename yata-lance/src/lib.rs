//! yata-lance — Lance Dataset persistence for yata graph.
//!
//! Thin wrapper around `lance::Dataset` for COO sorted graph data.
//! All manifest, fragment, compaction, and MVCC management is delegated to Lance.

pub mod dataset;

pub use dataset::YataDataset;
