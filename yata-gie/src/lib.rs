//! yata-gie: Graph Interactive Engine
//!
//! Push-based streaming query executor for graph data stored in MutableCsrStore.
//! Inspired by GraphScope Flex's GIE architecture.
//!
//! Pipeline: IR LogicalOps -> Optimizer -> Executor
//!
//! Depends only on yata-grin (traits) and yata-store (storage).

pub mod ir;
pub mod planner;
pub mod optimizer;
pub mod executor;

pub use ir::{AggOp, Expr, LogicalOp, QueryPlan};
pub use planner::{plan_scan, plan_traversal, PlanBuilder};
pub use optimizer::optimize;
pub use executor::{execute, Record};
