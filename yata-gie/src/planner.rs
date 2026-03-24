//! Planner: converts high-level query descriptions into IR QueryPlans.
//!
//! Two entry points:
//! - `plan_from_description`: build a plan from explicit parameters (label, rel, predicates)
//! - `PlanBuilder`: fluent builder API for constructing plans programmatically

use crate::ir::*;
use yata_grin::{Direction, Predicate};

/// Build a simple scan-only plan from a label and optional predicate.
pub fn plan_scan(label: &str, alias: &str, predicate: Option<Predicate>) -> QueryPlan {
    let mut plan = QueryPlan::new();
    plan.push(LogicalOp::Scan {
        label: label.to_string(),
        alias: alias.to_string(),
        predicate,
    });
    plan
}

/// Build a traversal plan: scan source label, expand by edge, project destination.
pub fn plan_traversal(
    src_label: &str,
    src_alias: &str,
    edge_label: &str,
    dst_alias: &str,
    direction: Direction,
) -> QueryPlan {
    let mut plan = QueryPlan::new();
    plan.push(LogicalOp::Scan {
        label: src_label.to_string(),
        alias: src_alias.to_string(),
        predicate: None,
    });
    plan.push(LogicalOp::Expand {
        src_alias: src_alias.to_string(),
        edge_label: edge_label.to_string(),
        dst_alias: dst_alias.to_string(),
        direction,
    });
    plan
}

/// Fluent builder for constructing QueryPlans.
pub struct PlanBuilder {
    plan: QueryPlan,
}

impl PlanBuilder {
    pub fn new() -> Self {
        Self {
            plan: QueryPlan::new(),
        }
    }

    pub fn scan(mut self, label: &str, alias: &str) -> Self {
        self.plan.push(LogicalOp::Scan {
            label: label.to_string(),
            alias: alias.to_string(),
            predicate: None,
        });
        self
    }

    pub fn scan_with_predicate(mut self, label: &str, alias: &str, predicate: Predicate) -> Self {
        self.plan.push(LogicalOp::Scan {
            label: label.to_string(),
            alias: alias.to_string(),
            predicate: Some(predicate),
        });
        self
    }

    pub fn expand(
        mut self,
        src_alias: &str,
        edge_label: &str,
        dst_alias: &str,
        direction: Direction,
    ) -> Self {
        self.plan.push(LogicalOp::Expand {
            src_alias: src_alias.to_string(),
            edge_label: edge_label.to_string(),
            dst_alias: dst_alias.to_string(),
            direction,
        });
        self
    }

    pub fn path_expand(
        mut self,
        src_alias: &str,
        edge_label: &str,
        dst_alias: &str,
        min_hops: u32,
        max_hops: u32,
        direction: Direction,
    ) -> Self {
        self.plan.push(LogicalOp::PathExpand {
            src_alias: src_alias.to_string(),
            edge_label: edge_label.to_string(),
            dst_alias: dst_alias.to_string(),
            min_hops,
            max_hops,
            direction,
        });
        self
    }

    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.plan.push(LogicalOp::Filter { predicate });
        self
    }

    pub fn project(mut self, exprs: Vec<Expr>) -> Self {
        self.plan.push(LogicalOp::Project { exprs });
        self
    }

    pub fn aggregate(mut self, group_by: Vec<Expr>, aggs: Vec<(String, AggOp, Expr)>) -> Self {
        self.plan.push(LogicalOp::Aggregate { group_by, aggs });
        self
    }

    pub fn order_by(mut self, keys: Vec<(Expr, bool)>) -> Self {
        self.plan.push(LogicalOp::OrderBy { keys });
        self
    }

    pub fn limit(mut self, count: usize) -> Self {
        self.plan.push(LogicalOp::Limit { count, offset: 0 });
        self
    }

    pub fn limit_offset(mut self, count: usize, offset: usize) -> Self {
        self.plan.push(LogicalOp::Limit { count, offset });
        self
    }

    pub fn distinct(mut self, keys: Vec<Expr>) -> Self {
        self.plan.push(LogicalOp::Distinct { keys });
        self
    }

    pub fn build(self) -> QueryPlan {
        self.plan
    }
}

impl Default for PlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_grin::PropValue;

    #[test]
    fn test_plan_simple_scan() {
        let plan = plan_scan("Person", "n", None);
        assert_eq!(plan.len(), 1);
        match &plan.ops[0] {
            LogicalOp::Scan {
                label,
                alias,
                predicate,
            } => {
                assert_eq!(label, "Person");
                assert_eq!(alias, "n");
                assert!(predicate.is_none());
            }
            _ => panic!("expected Scan"),
        }
    }

    #[test]
    fn test_plan_scan_with_predicate() {
        let pred = Predicate::Eq("name".into(), PropValue::Str("Alice".into()));
        let plan = plan_scan("Person", "n", Some(pred));
        assert_eq!(plan.len(), 1);
        match &plan.ops[0] {
            LogicalOp::Scan {
                predicate: Some(Predicate::Eq(k, _)),
                ..
            } => {
                assert_eq!(k, "name");
            }
            _ => panic!("expected Scan with predicate"),
        }
    }

    #[test]
    fn test_plan_traversal() {
        let plan = plan_traversal("Person", "n", "KNOWS", "m", Direction::Out);
        assert_eq!(plan.len(), 2);
        match &plan.ops[0] {
            LogicalOp::Scan { label, .. } => assert_eq!(label, "Person"),
            _ => panic!("expected Scan"),
        }
        match &plan.ops[1] {
            LogicalOp::Expand {
                edge_label,
                direction,
                ..
            } => {
                assert_eq!(edge_label, "KNOWS");
                assert_eq!(*direction, Direction::Out);
            }
            _ => panic!("expected Expand"),
        }
    }

    #[test]
    fn test_builder_full_pipeline() {
        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .expand("n", "KNOWS", "m", Direction::Out)
            .filter(Predicate::Gt("age".into(), PropValue::Int(18)))
            .project(vec![
                Expr::Prop("n".into(), "name".into()),
                Expr::Prop("m".into(), "name".into()),
            ])
            .order_by(vec![(Expr::Prop("n".into(), "name".into()), false)])
            .limit(10)
            .build();

        assert_eq!(plan.len(), 6);
    }

    #[test]
    fn test_builder_scan_with_predicate() {
        let plan = PlanBuilder::new()
            .scan_with_predicate(
                "Person",
                "n",
                Predicate::Eq("name".into(), PropValue::Str("Alice".into())),
            )
            .project(vec![Expr::Var("n".into())])
            .build();

        assert_eq!(plan.len(), 2);
    }

    #[test]
    fn test_builder_aggregate() {
        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .aggregate(
                vec![],
                vec![("cnt".into(), AggOp::Count, Expr::Var("n".into()))],
            )
            .build();

        assert_eq!(plan.len(), 2);
    }

    #[test]
    fn test_builder_distinct() {
        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .distinct(vec![Expr::Prop("n".into(), "name".into())])
            .build();

        assert_eq!(plan.len(), 2);
    }

    #[test]
    fn test_builder_path_expand() {
        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .path_expand("n", "KNOWS", "m", 1, 3, Direction::Out)
            .build();

        assert_eq!(plan.len(), 2);
        match &plan.ops[1] {
            LogicalOp::PathExpand {
                min_hops, max_hops, ..
            } => {
                assert_eq!(*min_hops, 1);
                assert_eq!(*max_hops, 3);
            }
            _ => panic!("expected PathExpand"),
        }
    }

    #[test]
    fn test_builder_limit_offset() {
        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .limit_offset(10, 5)
            .build();

        match &plan.ops[1] {
            LogicalOp::Limit { count, offset } => {
                assert_eq!(*count, 10);
                assert_eq!(*offset, 5);
            }
            _ => panic!("expected Limit"),
        }
    }
}
