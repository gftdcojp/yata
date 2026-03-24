//! GIE-inspired lazy evaluation pipeline for Cypher queries.
//! Converts Cypher AST -> operator DAG -> lazy execution.
//! Only loads data that the query actually needs.

use crate::hints::QueryHints;

/// A pipeline operator that lazily produces rows.
#[derive(Debug, Clone)]
pub enum PipelineOp {
    /// Scan vertices with label filter.
    ScanVertices {
        labels: Vec<String>,
        props_filter: Vec<(String, String)>,
    },
    /// Expand edges from a vertex set (adjacency lookup).
    ExpandEdges {
        rel_types: Vec<String>,
        direction: Direction,
    },
    /// Filter rows by predicate expression (Cypher WHERE).
    Filter { predicate: String },
    /// Project specific columns (Cypher RETURN).
    Project { columns: Vec<String> },
    /// Limit output rows.
    Limit { count: usize },
    /// Order by column.
    OrderBy { column: String, desc: bool },
}

/// Edge traversal direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
    Both,
}

/// A query execution pipeline — ordered sequence of operators.
#[derive(Debug, Clone, Default)]
pub struct Pipeline {
    pub ops: Vec<PipelineOp>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn push(&mut self, op: PipelineOp) {
        self.ops.push(op);
    }

    /// True if the pipeline contains any scan or expand operators
    /// (i.e., it actually does something).
    pub fn has_data_ops(&self) -> bool {
        self.ops.iter().any(|op| {
            matches!(
                op,
                PipelineOp::ScanVertices { .. } | PipelineOp::ExpandEdges { .. }
            )
        })
    }
}

/// Build a pipeline from QueryHints extracted from the Cypher AST.
pub fn plan_from_hints(hints: &QueryHints) -> Pipeline {
    let mut pipeline = Pipeline::new();

    // Step 1: Scan vertices with label + property filters
    if !hints.node_labels.is_empty() || !hints.prop_eq_filters.is_empty() {
        pipeline.push(PipelineOp::ScanVertices {
            labels: hints.node_labels.clone(),
            props_filter: hints.prop_eq_filters.clone(),
        });
    }

    // Step 2: Expand edges if relationship types specified
    if !hints.rel_types.is_empty() {
        pipeline.push(PipelineOp::ExpandEdges {
            rel_types: hints.rel_types.clone(),
            direction: Direction::Out,
        });
    }

    pipeline
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_hints_empty_pipeline() {
        let hints = QueryHints {
            node_labels: vec![],
            rel_types: vec![],
            prop_eq_filters: vec![],
            is_read_only: true,
        };
        let pipeline = plan_from_hints(&hints);
        assert!(!pipeline.has_data_ops());
    }

    #[test]
    fn test_label_filter_produces_scan() {
        let hints = QueryHints {
            node_labels: vec!["Person".into()],
            rel_types: vec![],
            prop_eq_filters: vec![],
            is_read_only: true,
        };
        let pipeline = plan_from_hints(&hints);
        assert!(pipeline.has_data_ops());
        assert_eq!(pipeline.ops.len(), 1);
        match &pipeline.ops[0] {
            PipelineOp::ScanVertices { labels, .. } => {
                assert_eq!(labels, &["Person"]);
            }
            _ => panic!("expected ScanVertices"),
        }
    }

    #[test]
    fn test_rel_type_produces_expand() {
        let hints = QueryHints {
            node_labels: vec!["Person".into()],
            rel_types: vec!["KNOWS".into()],
            prop_eq_filters: vec![],
            is_read_only: true,
        };
        let pipeline = plan_from_hints(&hints);
        assert_eq!(pipeline.ops.len(), 2);
        match &pipeline.ops[1] {
            PipelineOp::ExpandEdges {
                rel_types,
                direction,
            } => {
                assert_eq!(rel_types, &["KNOWS"]);
                assert_eq!(*direction, Direction::Out);
            }
            _ => panic!("expected ExpandEdges"),
        }
    }

    #[test]
    fn test_prop_filter_in_scan() {
        let hints = QueryHints {
            node_labels: vec![],
            rel_types: vec![],
            prop_eq_filters: vec![("name".into(), "\"Alice\"".into())],
            is_read_only: true,
        };
        let pipeline = plan_from_hints(&hints);
        assert_eq!(pipeline.ops.len(), 1);
        match &pipeline.ops[0] {
            PipelineOp::ScanVertices { props_filter, .. } => {
                assert_eq!(props_filter.len(), 1);
                assert_eq!(props_filter[0].0, "name");
            }
            _ => panic!("expected ScanVertices"),
        }
    }
}
