//! yata-gie: Graph Interactive Engine
//!
//! Push-based streaming query executor for graph data stored in MutableCsrStore.
//! Inspired by GraphScope Flex's GIE architecture.
//!
//! Pipeline: IR LogicalOps -> Optimizer -> Executor
//!
//! Depends only on yata-grin (traits) and yata-store (storage).

pub mod distributed_executor;
pub mod distributed_planner;
pub mod executor;
pub mod ir;
pub mod optimizer;
pub mod planner;
pub mod transpile;

pub use distributed_executor::{ExchangeTransport, MemoryExchangeTransport, execute_fragment};
pub use distributed_planner::{plan_distributed, requires_distribution};
pub use executor::{Record, execute, execute_op, eval_expr};
pub use ir::{AggOp, DistributedPlan, ExchangeKind, Expr, LogicalOp, PartitionPlanFragment, QueryPlan, SecurityScope};
pub use optimizer::optimize;
pub use planner::{PlanBuilder, plan_scan, plan_traversal};
pub use transpile::{transpile, transpile_secured};
