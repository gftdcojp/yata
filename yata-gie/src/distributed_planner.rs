//! Distributed query planner — splits a QueryPlan into per-partition fragments.
//!
//! Inserts Exchange/Receive operators at partition boundaries so that
//! multi-hop traversals can execute across partitions.
//!
//! NOTE: Code exists but hash partition routing is NOT WIRED into mutation flow.
//! Exchange/Receive are no-op passthrough in the current single-partition executor.

use crate::ir::*;
use yata_grin::Direction;

/// Split a QueryPlan into a DistributedPlan for N partitions.
///
/// Rules:
/// - Scan: runs on all partitions (each scans its local vertices)
/// - Expand: if the destination might be on another partition, insert Exchange(HashShuffle)
///   after the expand to route records to the correct partition
/// - PathExpand: generates iterative exchange rounds (one per hop)
/// - Aggregate: insert Exchange(Gather) before final aggregation
/// - Filter/Project/OrderBy/Limit/Distinct: run locally (no exchange needed)
pub fn plan_distributed(plan: &QueryPlan, partition_count: u32) -> DistributedPlan {
    if partition_count <= 1 {
        // Single partition: no distribution needed.
        return DistributedPlan {
            fragments: vec![PartitionPlanFragment {
                partition_id: 0,
                plan: plan.clone(),
                outbound_exchanges: vec![],
                inbound_receives: vec![],
            }],
            exchange_rounds: 0,
            original_query: String::new(),
        };
    }

    let mut exchange_round = 0u32;
    let mut fragment_ops: Vec<LogicalOp> = Vec::new();
    let mut outbound_exchanges: Vec<ExchangeSpec> = Vec::new();
    let mut inbound_receives: Vec<ReceiveSpec> = Vec::new();

    for op in &plan.ops {
        match op {
            // Expand may cross partition boundaries — insert exchange after.
            LogicalOp::Expand {
                dst_alias,
                edge_label,
                ..
            } => {
                fragment_ops.push(op.clone());

                // After expanding, destination vertices may be on other partitions.
                // Insert HashShuffle exchange to route records to the correct partition.
                let exchange = LogicalOp::Exchange {
                    routing_key: Expr::Var(dst_alias.clone()),
                    kind: ExchangeKind::HashShuffle,
                };
                fragment_ops.push(exchange);

                outbound_exchanges.push(ExchangeSpec {
                    round: exchange_round,
                    kind: ExchangeKind::HashShuffle,
                    routing_key: Expr::Var(dst_alias.clone()),
                });

                let receive = LogicalOp::Receive {
                    source_partitions: vec![], // all partitions
                };
                fragment_ops.push(receive);

                inbound_receives.push(ReceiveSpec {
                    round: exchange_round,
                    source_partitions: vec![],
                });

                exchange_round += 1;
            }

            // PathExpand: one exchange round per hop.
            LogicalOp::PathExpand {
                dst_alias,
                min_hops,
                max_hops,
                ..
            } => {
                fragment_ops.push(op.clone());

                // Each hop requires an exchange round for frontier propagation.
                for _hop in 0..*max_hops {
                    outbound_exchanges.push(ExchangeSpec {
                        round: exchange_round,
                        kind: ExchangeKind::HashShuffle,
                        routing_key: Expr::Var(dst_alias.clone()),
                    });
                    inbound_receives.push(ReceiveSpec {
                        round: exchange_round,
                        source_partitions: vec![],
                    });
                    exchange_round += 1;
                }
            }

            // Aggregate: insert Gather exchange to collect all records at coordinator.
            LogicalOp::Aggregate { .. } => {
                let gather = LogicalOp::Exchange {
                    routing_key: Expr::Lit(yata_grin::PropValue::Int(0)),
                    kind: ExchangeKind::Gather,
                };
                fragment_ops.push(gather);

                outbound_exchanges.push(ExchangeSpec {
                    round: exchange_round,
                    kind: ExchangeKind::Gather,
                    routing_key: Expr::Lit(yata_grin::PropValue::Int(0)),
                });

                exchange_round += 1;

                // Aggregate runs on coordinator after gather.
                fragment_ops.push(op.clone());
            }

            // All other ops run locally — no exchange needed.
            _ => {
                fragment_ops.push(op.clone());
            }
        }
    }

    // Create identical plan fragment for each partition.
    // (In Phase 5, the coordinator will specialize fragments per partition.)
    let fragments: Vec<PartitionPlanFragment> = (0..partition_count)
        .map(|pid| PartitionPlanFragment {
            partition_id: pid,
            plan: QueryPlan {
                ops: fragment_ops.clone(),
            },
            outbound_exchanges: outbound_exchanges.clone(),
            inbound_receives: inbound_receives.clone(),
        })
        .collect();

    DistributedPlan {
        fragments,
        exchange_rounds: exchange_round,
        original_query: String::new(),
    }
}

/// Analyze whether a query plan requires distributed execution.
/// Returns true if any Expand or PathExpand operator exists.
pub fn requires_distribution(plan: &QueryPlan) -> bool {
    plan.ops.iter().any(|op| {
        matches!(
            op,
            LogicalOp::Expand { .. } | LogicalOp::PathExpand { .. }
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::PlanBuilder;

    #[test]
    fn test_single_partition_no_exchange() {
        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .project(vec![Expr::Prop("n".into(), "name".into())])
            .build();

        let dist = plan_distributed(&plan, 1);
        assert_eq!(dist.fragments.len(), 1);
        assert_eq!(dist.exchange_rounds, 0);
        assert!(dist.fragments[0].outbound_exchanges.is_empty());
    }

    #[test]
    fn test_scan_only_no_exchange() {
        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .project(vec![Expr::Prop("n".into(), "name".into())])
            .build();

        let dist = plan_distributed(&plan, 4);
        assert_eq!(dist.fragments.len(), 4);
        // Scan + Project only — no exchange needed.
        assert_eq!(dist.exchange_rounds, 0);
    }

    #[test]
    fn test_expand_inserts_exchange() {
        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .expand("n", "KNOWS", "m", Direction::Out)
            .project(vec![Expr::Prop("m".into(), "name".into())])
            .build();

        let dist = plan_distributed(&plan, 4);
        assert_eq!(dist.fragments.len(), 4);
        assert_eq!(dist.exchange_rounds, 1);

        let frag = &dist.fragments[0];
        assert_eq!(frag.outbound_exchanges.len(), 1);
        assert!(matches!(
            frag.outbound_exchanges[0].kind,
            ExchangeKind::HashShuffle
        ));
        assert_eq!(frag.inbound_receives.len(), 1);
    }

    #[test]
    fn test_two_hop_two_exchanges() {
        let plan = PlanBuilder::new()
            .scan("Person", "a")
            .expand("a", "KNOWS", "b", Direction::Out)
            .expand("b", "WORKS_AT", "c", Direction::Out)
            .project(vec![Expr::Prop("c".into(), "name".into())])
            .build();

        let dist = plan_distributed(&plan, 4);
        assert_eq!(dist.exchange_rounds, 2); // One per expand
        assert_eq!(dist.fragments[0].outbound_exchanges.len(), 2);
    }

    #[test]
    fn test_aggregate_inserts_gather() {
        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .aggregate(
                vec![],
                vec![("cnt".into(), AggOp::Count, Expr::Var("n".into()))],
            )
            .build();

        let dist = plan_distributed(&plan, 4);
        assert_eq!(dist.exchange_rounds, 1);
        assert!(matches!(
            dist.fragments[0].outbound_exchanges[0].kind,
            ExchangeKind::Gather
        ));
    }

    #[test]
    fn test_path_expand_multi_round() {
        let plan = PlanBuilder::new()
            .scan("Person", "a")
            .path_expand("a", "KNOWS", "b", 1, 3, Direction::Out)
            .project(vec![Expr::Prop("b".into(), "name".into())])
            .build();

        let dist = plan_distributed(&plan, 4);
        // PathExpand with max_hops=3 → 3 exchange rounds
        assert_eq!(dist.exchange_rounds, 3);
        assert_eq!(dist.fragments[0].outbound_exchanges.len(), 3);
    }

    #[test]
    fn test_requires_distribution() {
        let scan_only = PlanBuilder::new()
            .scan("Person", "n")
            .project(vec![Expr::Prop("n".into(), "name".into())])
            .build();
        assert!(!requires_distribution(&scan_only));

        let with_expand = PlanBuilder::new()
            .scan("Person", "n")
            .expand("n", "KNOWS", "m", Direction::Out)
            .build();
        assert!(requires_distribution(&with_expand));
    }

    #[test]
    fn test_fragment_partition_ids() {
        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .expand("n", "KNOWS", "m", Direction::Out)
            .build();

        let dist = plan_distributed(&plan, 8);
        assert_eq!(dist.fragments.len(), 8);
        for (i, frag) in dist.fragments.iter().enumerate() {
            assert_eq!(frag.partition_id, i as u32);
        }
    }
}
