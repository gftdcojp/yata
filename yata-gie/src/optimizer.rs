//! Query optimizer: transforms a QueryPlan for better execution performance.
//!
//! Optimizations:
//! - Predicate pushdown: move Filter ops into Scan predicates when possible
//! - Filter merge: merge adjacent Scan + Filter into Scan with predicate

use crate::ir::*;
use yata_grin::Predicate;

/// Apply all optimization passes to a query plan.
pub fn optimize(plan: QueryPlan) -> QueryPlan {
    let plan = push_down_filters(plan);
    let plan = merge_adjacent_scans(plan);
    plan
}

/// Move Filter ops before Expand when the filter only references the scan alias.
///
/// Pattern: Scan(alias=n) -> Expand -> Filter(pred on n) -> ...
/// becomes: Scan(alias=n) -> Filter(pred on n) -> Expand -> ...
///
/// This enables the subsequent merge pass to fold the filter into the scan.
fn push_down_filters(plan: QueryPlan) -> QueryPlan {
    let mut ops = plan.ops;
    let mut changed = true;

    while changed {
        changed = false;
        let mut i = 0;
        while i + 1 < ops.len() {
            // If ops[i] is Expand and ops[i+1] is Filter referencing only
            // the scan alias (before the expand), swap them.
            let should_swap =
                if let (LogicalOp::Expand { src_alias: _, .. }, LogicalOp::Filter { predicate }) =
                    (&ops[i], &ops[i + 1])
                {
                    // Check if the filter predicate only references properties
                    // (not variables from the expand destination).
                    // Simple heuristic: if we can find a Scan before this Expand
                    // whose alias matches, and the predicate doesn't reference
                    // the dst_alias, we can push down.
                    let expand_dst = if let LogicalOp::Expand { dst_alias, .. } = &ops[i] {
                        dst_alias.clone()
                    } else {
                        String::new()
                    };
                    // Predicates in GRIN format don't reference aliases directly,
                    // they reference property keys. We push down all pure property
                    // predicates that appear after an Expand.
                    !predicate_references_alias(&expand_dst, predicate) && !expand_dst.is_empty()
                } else {
                    false
                };

            if should_swap {
                ops.swap(i, i + 1);
                changed = true;
            }
            i += 1;
        }
    }

    QueryPlan { ops }
}

/// Merge consecutive Scan + Filter into a single Scan with predicate.
///
/// Pattern: Scan(label, alias, None) -> Filter(pred)
/// becomes: Scan(label, alias, Some(pred))
///
/// Pattern: Scan(label, alias, Some(existing)) -> Filter(pred)
/// becomes: Scan(label, alias, Some(And(existing, pred)))
fn merge_adjacent_scans(plan: QueryPlan) -> QueryPlan {
    let ops = plan.ops;
    let mut result: Vec<LogicalOp> = Vec::with_capacity(ops.len());

    let mut i = 0;
    while i < ops.len() {
        if i + 1 < ops.len() {
            if let (LogicalOp::Scan { .. }, LogicalOp::Filter { .. }) = (&ops[i], &ops[i + 1]) {
                // Take ownership of both ops
                let scan = ops[i].clone();
                let filter = ops[i + 1].clone();

                if let (
                    LogicalOp::Scan {
                        label,
                        alias,
                        predicate: existing,
                    },
                    LogicalOp::Filter {
                        predicate: new_pred,
                    },
                ) = (scan, filter)
                {
                    let merged_pred = match existing {
                        None => new_pred,
                        Some(existing_pred) => {
                            Predicate::And(Box::new(existing_pred), Box::new(new_pred))
                        }
                    };
                    result.push(LogicalOp::Scan {
                        label,
                        alias,
                        predicate: Some(merged_pred),
                    });
                    i += 2;
                    continue;
                }
            }
        }
        result.push(ops[i].clone());
        i += 1;
    }

    QueryPlan { ops: result }
}

/// Check if a predicate conceptually references a given alias.
/// Since GRIN predicates are property-key based (not alias-based),
/// this is a heuristic: we return false for all pure property predicates,
/// meaning they can be pushed down past expands.
fn predicate_references_alias(_alias: &str, _predicate: &Predicate) -> bool {
    // GRIN predicates (Eq, Lt, Gt, etc.) reference property keys, not aliases.
    // They are always safe to push down since they filter on vertex properties
    // of whatever the current context vertex is.
    // In a more sophisticated system, we'd track which alias each predicate
    // applies to. For now, all predicates are considered pushdown-safe.
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_grin::PropValue;

    #[test]
    fn test_filter_pushdown() {
        // Scan -> Expand -> Filter  =>  Scan -> Filter -> Expand
        let plan = QueryPlan {
            ops: vec![
                LogicalOp::Scan {
                    label: "Person".into(),
                    alias: "n".into(),
                    predicate: None,
                },
                LogicalOp::Expand {
                    src_alias: "n".into(),
                    edge_label: "KNOWS".into(),
                    dst_alias: "m".into(),
                    direction: yata_grin::Direction::Out,
                },
                LogicalOp::Filter {
                    predicate: Predicate::Eq("age".into(), PropValue::Int(30)),
                },
            ],
        };

        let optimized = push_down_filters(plan);
        assert_eq!(optimized.ops.len(), 3);

        // Filter should now be before Expand
        assert!(matches!(&optimized.ops[0], LogicalOp::Scan { .. }));
        assert!(matches!(&optimized.ops[1], LogicalOp::Filter { .. }));
        assert!(matches!(&optimized.ops[2], LogicalOp::Expand { .. }));
    }

    #[test]
    fn test_merge_scans() {
        // Scan(no pred) -> Filter(pred)  =>  Scan(pred)
        let plan = QueryPlan {
            ops: vec![
                LogicalOp::Scan {
                    label: "Person".into(),
                    alias: "n".into(),
                    predicate: None,
                },
                LogicalOp::Filter {
                    predicate: Predicate::Eq("name".into(), PropValue::Str("Alice".into())),
                },
            ],
        };

        let optimized = merge_adjacent_scans(plan);
        assert_eq!(optimized.ops.len(), 1);

        match &optimized.ops[0] {
            LogicalOp::Scan {
                predicate: Some(Predicate::Eq(k, _)),
                ..
            } => {
                assert_eq!(k, "name");
            }
            _ => panic!("expected Scan with merged predicate"),
        }
    }

    #[test]
    fn test_merge_scans_with_existing_predicate() {
        // Scan(pred1) -> Filter(pred2)  =>  Scan(And(pred1, pred2))
        let plan = QueryPlan {
            ops: vec![
                LogicalOp::Scan {
                    label: "Person".into(),
                    alias: "n".into(),
                    predicate: Some(Predicate::Eq("name".into(), PropValue::Str("Alice".into()))),
                },
                LogicalOp::Filter {
                    predicate: Predicate::Gt("age".into(), PropValue::Int(18)),
                },
            ],
        };

        let optimized = merge_adjacent_scans(plan);
        assert_eq!(optimized.ops.len(), 1);

        match &optimized.ops[0] {
            LogicalOp::Scan {
                predicate: Some(Predicate::And(_, _)),
                ..
            } => {}
            _ => panic!("expected Scan with And predicate"),
        }
    }

    #[test]
    fn test_full_optimize_pipeline() {
        // Scan -> Expand -> Filter -> Project
        // Should become: Scan(pred) -> Expand -> Project
        let plan = QueryPlan {
            ops: vec![
                LogicalOp::Scan {
                    label: "Person".into(),
                    alias: "n".into(),
                    predicate: None,
                },
                LogicalOp::Expand {
                    src_alias: "n".into(),
                    edge_label: "KNOWS".into(),
                    dst_alias: "m".into(),
                    direction: yata_grin::Direction::Out,
                },
                LogicalOp::Filter {
                    predicate: Predicate::Eq("age".into(), PropValue::Int(30)),
                },
                LogicalOp::Project {
                    exprs: vec![Expr::Prop("n".into(), "name".into())],
                },
            ],
        };

        let optimized = optimize(plan);
        // After pushdown: Scan -> Filter -> Expand -> Project
        // After merge: Scan(pred) -> Expand -> Project
        assert_eq!(optimized.ops.len(), 3);
        assert!(matches!(
            &optimized.ops[0],
            LogicalOp::Scan {
                predicate: Some(_),
                ..
            }
        ));
        assert!(matches!(&optimized.ops[1], LogicalOp::Expand { .. }));
        assert!(matches!(&optimized.ops[2], LogicalOp::Project { .. }));
    }

    #[test]
    fn test_no_optimization_needed() {
        // Already optimal: Scan(pred) -> Expand -> Project
        let plan = QueryPlan {
            ops: vec![
                LogicalOp::Scan {
                    label: "Person".into(),
                    alias: "n".into(),
                    predicate: Some(Predicate::Eq("age".into(), PropValue::Int(30))),
                },
                LogicalOp::Expand {
                    src_alias: "n".into(),
                    edge_label: "KNOWS".into(),
                    dst_alias: "m".into(),
                    direction: yata_grin::Direction::Out,
                },
                LogicalOp::Project {
                    exprs: vec![Expr::Var("m".into())],
                },
            ],
        };

        let optimized = optimize(plan);
        assert_eq!(optimized.ops.len(), 3);
    }

    #[test]
    fn test_empty_plan() {
        let plan = QueryPlan::new();
        let optimized = optimize(plan);
        assert!(optimized.is_empty());
    }

    #[test]
    fn test_single_scan_no_merge() {
        let plan = QueryPlan {
            ops: vec![LogicalOp::Scan {
                label: "Person".into(),
                alias: "n".into(),
                predicate: None,
            }],
        };
        let optimized = optimize(plan);
        assert_eq!(optimized.ops.len(), 1);
    }
}
