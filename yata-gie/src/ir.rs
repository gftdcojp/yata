//! Intermediate Representation for graph query plans.
//! Decouples query language (Cypher/Gremlin) from execution engine.

use serde::{Deserialize, Serialize};
use yata_grin::{Direction, Predicate, PropValue};

/// A column expression in the query plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Security filter: prune vertices that fail governance checks.
    /// Injected by transpile_secured() after each Scan op.
    /// Evaluated inline during GIE CSR traversal — no MemoryGraph copy.
    SecurityFilter {
        /// All bound aliases to check (from preceding Scan/Expand).
        aliases: Vec<String>,
        /// Security scope from PDS AuthContext.
        scope: SecurityScope,
    },

    // ── Distributed operators (Phase 2 design: IR defined, NOT WIRED into mutation flow) ──

    /// Exchange: send records to other partitions based on routing key.
    /// Inserted by the distributed planner at partition boundaries.
    Exchange {
        /// Key expression determining target partition (hash of value % N).
        routing_key: Expr,
        /// How records are exchanged between partitions.
        kind: ExchangeKind,
    },

    /// Receive: collect records from remote partitions. Paired with Exchange.
    Receive {
        /// Source partition IDs to receive from (empty = all).
        source_partitions: Vec<u32>,
    },
}

/// How records are exchanged between partitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExchangeKind {
    /// Hash-partition by routing key (for joins, expands across partitions).
    HashShuffle,
    /// Broadcast all records to all partitions (for broadcast joins).
    Broadcast,
    /// Gather all records to coordinator (for final aggregation).
    Gather,
}

/// Aggregation operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggOp {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
}

/// Security scope for GIE SecurityFilter — derived from PDS AuthContext.
/// Enables GIE fast path (<1µs) WITH security enforcement (no MemoryGraph fallback).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecurityScope {
    /// Maximum sensitivity ordinal the viewer can access (0=public, 1=internal, 2=confidential, 3=restricted).
    pub max_sensitivity_ord: u8,
    /// Collection prefixes the viewer's RBAC roles grant access to (e.g., "ai.gftd.apps.yabai.").
    /// Empty = no RBAC scope restriction (public-only access enforced by sensitivity).
    pub collection_scopes: Vec<String>,
    /// FNV-1a hashes of repo DIDs the viewer has consent grants for.
    /// Empty = no cross-owner access (only public data).
    pub allowed_owner_hashes: Vec<u32>,
    /// If true, skip all filtering (internal/system scope with restricted clearance).
    pub bypass: bool,
}

/// A query plan: sequence of logical operators forming a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// A plan fragment assigned to a specific partition for distributed execution.
/// The distributed planner splits a QueryPlan into per-partition fragments
/// with explicit Exchange/Receive boundaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionPlanFragment {
    /// Target partition ID.
    pub partition_id: u32,
    /// The plan operators to execute on this partition.
    pub plan: QueryPlan,
    /// Exchange rounds: (round_index, exchange_kind, routing_key_expr)
    pub outbound_exchanges: Vec<ExchangeSpec>,
    /// Receive rounds: (round_index, source_partition_ids)
    pub inbound_receives: Vec<ReceiveSpec>,
}

/// Specification for outbound data exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeSpec {
    /// Round index (for multi-round iterative queries like variable-hop).
    pub round: u32,
    /// Exchange kind.
    pub kind: ExchangeKind,
    /// Key expression for hash-based routing.
    pub routing_key: Expr,
}

/// Specification for inbound data receive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiveSpec {
    /// Round index.
    pub round: u32,
    /// Source partitions to receive from (empty = all).
    pub source_partitions: Vec<u32>,
}

/// A distributed query plan: collection of partition fragments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedPlan {
    /// Per-partition plan fragments.
    pub fragments: Vec<PartitionPlanFragment>,
    /// Total number of exchange rounds.
    pub exchange_rounds: u32,
    /// Original query (for debugging).
    pub original_query: String,
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
            LogicalOp::Scan {
                predicate: Some(Predicate::Eq(k, _)),
                ..
            } => {
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
            LogicalOp::Expand {
                direction: Direction::Out,
                ..
            } => {}
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
        plan.push(LogicalOp::Limit {
            count: 10,
            offset: 0,
        });

        assert_eq!(plan.len(), 6);
    }

    #[test]
    fn test_default() {
        let plan = QueryPlan::default();
        assert!(plan.is_empty());
    }

    #[test]
    fn test_exchange_kind_variants() {
        let kinds = vec![
            ExchangeKind::HashShuffle,
            ExchangeKind::Broadcast,
            ExchangeKind::Gather,
        ];
        assert_eq!(kinds.len(), 3);
        let _ = format!("{:?}", kinds);
    }

    #[test]
    fn test_exchange_operator() {
        let op = LogicalOp::Exchange {
            routing_key: Expr::Var("n".into()),
            kind: ExchangeKind::HashShuffle,
        };
        match op {
            LogicalOp::Exchange { kind: ExchangeKind::HashShuffle, .. } => {}
            _ => panic!("expected HashShuffle Exchange"),
        }
    }

    #[test]
    fn test_receive_operator() {
        let op = LogicalOp::Receive {
            source_partitions: vec![0, 1, 2],
        };
        match op {
            LogicalOp::Receive { source_partitions } => {
                assert_eq!(source_partitions.len(), 3);
            }
            _ => panic!("expected Receive"),
        }
    }

    #[test]
    fn test_partition_plan_fragment() {
        let fragment = PartitionPlanFragment {
            partition_id: 0,
            plan: QueryPlan::new(),
            outbound_exchanges: vec![ExchangeSpec {
                round: 0,
                kind: ExchangeKind::HashShuffle,
                routing_key: Expr::Var("n".into()),
            }],
            inbound_receives: vec![ReceiveSpec {
                round: 0,
                source_partitions: vec![1, 2],
            }],
        };
        assert_eq!(fragment.partition_id, 0);
        assert_eq!(fragment.outbound_exchanges.len(), 1);
        assert_eq!(fragment.inbound_receives.len(), 1);
    }

    #[test]
    fn test_distributed_plan() {
        let plan = DistributedPlan {
            fragments: vec![
                PartitionPlanFragment {
                    partition_id: 0,
                    plan: QueryPlan::new(),
                    outbound_exchanges: vec![],
                    inbound_receives: vec![],
                },
                PartitionPlanFragment {
                    partition_id: 1,
                    plan: QueryPlan::new(),
                    outbound_exchanges: vec![],
                    inbound_receives: vec![],
                },
            ],
            exchange_rounds: 0,
            original_query: "MATCH (n) RETURN n".into(),
        };
        assert_eq!(plan.fragments.len(), 2);
        assert_eq!(plan.exchange_rounds, 0);
    }

    #[test]
    fn test_plan_with_exchange_ops() {
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
        plan.push(LogicalOp::Exchange {
            routing_key: Expr::Var("m".into()),
            kind: ExchangeKind::HashShuffle,
        });
        plan.push(LogicalOp::Receive {
            source_partitions: vec![],
        });
        plan.push(LogicalOp::Project {
            exprs: vec![Expr::Prop("m".into(), "name".into())],
        });
        plan.push(LogicalOp::Exchange {
            routing_key: Expr::Var("m".into()),
            kind: ExchangeKind::Gather,
        });
        assert_eq!(plan.len(), 6);
    }
}
