//! Intermediate Representation for graph query plans.
//! Decouples query language (Cypher/Gremlin) from execution engine.

use yata_grin::{Direction, Predicate, PropValue};

/// A column expression in the query plan.
#[derive(Debug, Clone)]
pub enum Expr {
    /// Variable reference: "n" -> vertex/edge bound to variable.
    Var(String),
    /// Property access: "n.name" -> (variable, property_key).
    Prop(String, String),
    /// Literal value.
    Lit(PropValue),
    /// Function call: count(*), sum(n.age), etc.
    Func(String, Vec<Expr>),
    /// Alias: expr AS name.
    Alias(Box<Expr>, String),
}

/// Logical query plan operator.
#[derive(Debug, Clone)]
pub enum LogicalOp {
    /// Scan vertices by label. Binds to alias.
    Scan {
        label: String,
        alias: String,
        predicate: Option<Predicate>,
    },
    /// Expand edges from bound vertex. Produces (src, edge, dst) tuples.
    Expand {
        src_alias: String,
        edge_label: String,
        dst_alias: String,
        direction: Direction,
    },
    /// Variable-length path expansion.
    PathExpand {
        src_alias: String,
        edge_label: String,
        dst_alias: String,
        min_hops: u32,
        max_hops: u32,
        direction: Direction,
    },
    /// Filter rows by predicate.
    Filter { predicate: Predicate },
    /// Project (select) columns.
    Project { exprs: Vec<Expr> },
    /// Aggregate with optional group-by.
    Aggregate {
        group_by: Vec<Expr>,
        aggs: Vec<(String, AggOp, Expr)>,
    },
    /// Order by columns. Each entry: (expr, descending).
    OrderBy { keys: Vec<(Expr, bool)> },
    /// Limit output rows.
    Limit { count: usize, offset: usize },
    /// Distinct (dedup) by key expressions.
    Distinct { keys: Vec<Expr> },
}

/// Aggregation operation.
#[derive(Debug, Clone)]
pub enum AggOp {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
}

/// A query plan: sequence of logical operators forming a pipeline.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub ops: Vec<LogicalOp>,
}

impl QueryPlan {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn push(&mut self, op: LogicalOp) {
        self.ops.push(op);
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

impl Default for QueryPlan {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_plan_construction() {
        let mut plan = QueryPlan::new();
        assert!(plan.is_empty());
        assert_eq!(plan.len(), 0);

        plan.push(LogicalOp::Scan {
            label: "Person".into(),
            alias: "n".into(),
            predicate: None,
        });
        plan.push(LogicalOp::Project {
            exprs: vec![Expr::Prop("n".into(), "name".into())],
        });

        assert_eq!(plan.len(), 2);
        assert!(!plan.is_empty());
    }

    #[test]
    fn test_expr_construction() {
        let var = Expr::Var("n".into());
        let prop = Expr::Prop("n".into(), "name".into());
        let lit = Expr::Lit(PropValue::Int(42));
        let func = Expr::Func("count".into(), vec![Expr::Var("*".into())]);
        let alias = Expr::Alias(Box::new(Expr::Var("n".into())), "node".into());

        // Verify Debug works (not panicking is the test)
        let _ = format!("{:?}", var);
        let _ = format!("{:?}", prop);
        let _ = format!("{:?}", lit);
        let _ = format!("{:?}", func);
        let _ = format!("{:?}", alias);
    }

    #[test]
    fn test_agg_op_variants() {
        let ops = vec![
            AggOp::Count,
            AggOp::Sum,
            AggOp::Avg,
            AggOp::Min,
            AggOp::Max,
            AggOp::Collect,
        ];
        assert_eq!(ops.len(), 6);
    }

    #[test]
    fn test_scan_with_predicate() {
        let plan_op = LogicalOp::Scan {
            label: "Person".into(),
            alias: "n".into(),
            predicate: Some(Predicate::Eq("name".into(), PropValue::Str("Alice".into()))),
        };
        match plan_op {
            LogicalOp::Scan { predicate: Some(Predicate::Eq(k, _)), .. } => {
                assert_eq!(k, "name");
            }
            _ => panic!("expected Scan with Eq predicate"),
        }
    }

    #[test]
    fn test_expand_direction() {
        let expand = LogicalOp::Expand {
            src_alias: "n".into(),
            edge_label: "KNOWS".into(),
            dst_alias: "m".into(),
            direction: Direction::Out,
        };
        match expand {
            LogicalOp::Expand { direction: Direction::Out, .. } => {}
            _ => panic!("expected Out direction"),
        }
    }

    #[test]
    fn test_full_plan() {
        let mut plan = QueryPlan::new();
        plan.push(LogicalOp::Scan {
            label: "Person".into(),
            alias: "n".into(),
            predicate: None,
        });
        plan.push(LogicalOp::Expand {
            src_alias: "n".into(),
            edge_label: "KNOWS".into(),
            dst_alias: "m".into(),
            direction: Direction::Out,
        });
        plan.push(LogicalOp::Filter {
            predicate: Predicate::Gt("age".into(), PropValue::Int(18)),
        });
        plan.push(LogicalOp::Project {
            exprs: vec![
                Expr::Prop("n".into(), "name".into()),
                Expr::Prop("m".into(), "name".into()),
            ],
        });
        plan.push(LogicalOp::OrderBy {
            keys: vec![(Expr::Prop("n".into(), "name".into()), false)],
        });
        plan.push(LogicalOp::Limit { count: 10, offset: 0 });

        assert_eq!(plan.len(), 6);
    }

    #[test]
    fn test_default() {
        let plan = QueryPlan::default();
        assert!(plan.is_empty());
    }
}
