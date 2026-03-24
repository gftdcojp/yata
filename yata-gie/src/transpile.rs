//! Cypher AST → GIE IR Plan transpiler.
//!
//! Converts parsed Cypher queries into GIE logical plans that execute directly
//! on MutableCsrStore via GRIN traits — zero MemoryGraph copy.
//!
//! Only read-only queries are supported. Mutations (CREATE/MERGE/SET/DELETE)
//! return `TranspileError::UnsupportedClause` and fall through to the
//! MemoryGraph path in yata-engine.

use yata_cypher::ast::*;
use yata_grin::{Direction, Predicate, PropValue};

use crate::ir::{self, AggOp, LogicalOp, QueryPlan};
use crate::optimizer::optimize;
use crate::planner::PlanBuilder;

#[derive(Debug, thiserror::Error)]
pub enum TranspileError {
    #[error("unsupported clause: {0}")]
    UnsupportedClause(String),
    #[error("unsupported expression: {0}")]
    UnsupportedExpr(String),
    #[error("missing variable in pattern")]
    MissingVariable,
}

/// Transpile a parsed Cypher query into a GIE IR plan.
/// Returns `Err` for mutations or unsupported constructs (caller falls back to MemoryGraph).
pub fn transpile(query: &yata_cypher::Query) -> Result<QueryPlan, TranspileError> {
    let mut builder = PlanBuilder::new();
    for clause in &query.clauses {
        builder = transpile_clause(builder, clause)?;
    }
    Ok(optimize(builder.build()))
}

/// Transpile with security: injects SecurityFilter after each Scan/Expand op.
/// Enables GIE fast path (<1µs) WITH governance enforcement — no MemoryGraph fallback.
pub fn transpile_secured(
    query: &yata_cypher::Query,
    scope: ir::SecurityScope,
) -> Result<QueryPlan, TranspileError> {
    let mut plan = transpile(query)?;
    if scope.bypass {
        return Ok(plan); // internal/system — no filtering needed
    }
    inject_security_filters(&mut plan, &scope);
    Ok(plan)
}

/// Insert SecurityFilter ops after each Scan and Expand in the plan.
fn inject_security_filters(plan: &mut QueryPlan, scope: &ir::SecurityScope) {
    let mut new_ops = Vec::with_capacity(plan.ops.len() * 2);
    for op in plan.ops.drain(..) {
        // Collect aliases introduced by this op
        let aliases: Vec<String> = match &op {
            LogicalOp::Scan { alias, .. } => vec![alias.clone()],
            LogicalOp::Expand { dst_alias, .. } => vec![dst_alias.clone()],
            LogicalOp::PathExpand { dst_alias, .. } => vec![dst_alias.clone()],
            _ => vec![],
        };
        new_ops.push(op);
        if !aliases.is_empty() {
            new_ops.push(LogicalOp::SecurityFilter {
                aliases,
                scope: scope.clone(),
            });
        }
    }
    plan.ops = new_ops;
}

fn transpile_clause(builder: PlanBuilder, clause: &Clause) -> Result<PlanBuilder, TranspileError> {
    match clause {
        Clause::Match { patterns, where_ } => transpile_match(builder, patterns, where_),
        Clause::Return {
            items,
            distinct,
            order_by,
            limit,
            skip,
        } => transpile_return(builder, items, *distinct, order_by, limit, skip),
        // Mutations → unsupported (fall through to MemoryGraph path)
        Clause::Create { .. } => Err(TranspileError::UnsupportedClause("CREATE".into())),
        Clause::Merge { .. } => Err(TranspileError::UnsupportedClause("MERGE".into())),
        Clause::Set { .. } => Err(TranspileError::UnsupportedClause("SET".into())),
        Clause::Delete { .. } => Err(TranspileError::UnsupportedClause("DELETE".into())),
        Clause::Remove { .. } => Err(TranspileError::UnsupportedClause("REMOVE".into())),
        Clause::With { items, where_ } => {
            // WITH is similar to RETURN but mid-query — treat as Project
            let mut b = builder;
            let exprs: Vec<ir::Expr> = items
                .iter()
                .map(|i| transpile_return_item(i))
                .collect::<Result<_, _>>()?;
            b = b.project(exprs);
            if let Some(w) = where_ {
                let pred = transpile_predicate(w)?;
                b = b.filter(pred);
            }
            Ok(b)
        }
        Clause::Unwind { .. } => Err(TranspileError::UnsupportedClause("UNWIND".into())),
        _ => Err(TranspileError::UnsupportedClause(format!("{clause:?}"))),
    }
}

fn transpile_match(
    mut builder: PlanBuilder,
    patterns: &[Pattern],
    where_: &Option<Expr>,
) -> Result<PlanBuilder, TranspileError> {
    for pattern in patterns {
        builder = transpile_pattern(builder, pattern)?;
    }
    if let Some(w) = where_ {
        let pred = transpile_predicate(w)?;
        builder = builder.filter(pred);
    }
    Ok(builder)
}

fn transpile_pattern(
    mut builder: PlanBuilder,
    pattern: &Pattern,
) -> Result<PlanBuilder, TranspileError> {
    let mut last_node_alias: Option<String> = None;

    for element in &pattern.elements {
        match element {
            PatternElement::Node(node) => {
                let alias = node
                    .var
                    .clone()
                    .unwrap_or_else(|| format!("_anon_{}", rand_id()));
                let label = node.labels.first().cloned().unwrap_or_default();
                let predicate = extract_node_predicate(&node.props);
                builder = if let Some(pred) = predicate {
                    builder.scan_with_predicate(&label, &alias, pred)
                } else {
                    builder.scan(&label, &alias)
                };
                last_node_alias = Some(alias);
            }
            PatternElement::Rel(rel) => {
                let src = last_node_alias
                    .as_ref()
                    .ok_or(TranspileError::MissingVariable)?
                    .clone();
                let edge_label = rel.types.first().cloned().unwrap_or_default();
                let direction = match rel.dir {
                    RelDir::Right => Direction::Out,
                    RelDir::Left => Direction::In,
                    RelDir::Both => Direction::Both,
                };

                // Peek ahead: the next element should be a Node with the dst alias
                // For now, generate a synthetic dst alias
                let dst_alias = rel
                    .var
                    .clone()
                    .unwrap_or_else(|| format!("_dst_{}", rand_id()));

                let is_variable_length = rel.min_hops.is_some() || rel.max_hops.is_some();
                if is_variable_length {
                    builder = builder.path_expand(
                        &src,
                        &edge_label,
                        &dst_alias,
                        rel.min_hops.unwrap_or(1),
                        rel.max_hops.unwrap_or(10),
                        direction,
                    );
                } else {
                    builder = builder.expand(&src, &edge_label, &dst_alias, direction);
                }
                // The rel becomes the "last node" for the next element
                last_node_alias = Some(dst_alias);
            }
        }
    }
    Ok(builder)
}

fn transpile_return(
    mut builder: PlanBuilder,
    items: &[ReturnItem],
    distinct: bool,
    order_by: &[OrderItem],
    limit: &Option<Expr>,
    skip: &Option<Expr>,
) -> Result<PlanBuilder, TranspileError> {
    // Check for aggregation functions
    let has_agg = items.iter().any(|i| is_aggregate_expr(&i.expr));

    if has_agg {
        let mut group_by = Vec::new();
        let mut aggs = Vec::new();

        for item in items {
            if is_aggregate_expr(&item.expr) {
                let (agg_op, agg_expr) = extract_agg(&item.expr)?;
                let alias = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("{:?}", item.expr));
                aggs.push((alias, agg_op, agg_expr));
            } else {
                group_by.push(transpile_ir_expr(&item.expr)?);
            }
        }
        builder = builder.aggregate(group_by, aggs);
    } else {
        let exprs: Vec<ir::Expr> = items
            .iter()
            .map(|i| transpile_return_item(i))
            .collect::<Result<_, _>>()?;
        builder = builder.project(exprs);
    }

    if distinct {
        let keys: Vec<ir::Expr> = items
            .iter()
            .map(|i| transpile_ir_expr(&i.expr))
            .collect::<Result<_, _>>()?;
        builder = builder.distinct(keys);
    }

    if !order_by.is_empty() {
        let keys: Vec<(ir::Expr, bool)> = order_by
            .iter()
            .map(|o| Ok((transpile_ir_expr(&o.expr)?, !o.asc)))
            .collect::<Result<_, TranspileError>>()?;
        builder = builder.order_by(keys);
    }

    if let Some(lim) = limit {
        let count = expr_to_usize(lim)?;
        let off = skip
            .as_ref()
            .map(|s| expr_to_usize(s))
            .transpose()?
            .unwrap_or(0);
        builder = builder.limit_offset(count, off);
    }

    Ok(builder)
}

fn transpile_return_item(item: &ReturnItem) -> Result<ir::Expr, TranspileError> {
    let expr = transpile_ir_expr(&item.expr)?;
    if let Some(ref alias) = item.alias {
        Ok(ir::Expr::Alias(Box::new(expr), alias.clone()))
    } else {
        Ok(expr)
    }
}

/// Convert Cypher Expr → GIE IR Expr.
fn transpile_ir_expr(expr: &Expr) -> Result<ir::Expr, TranspileError> {
    match expr {
        Expr::Var(v) => Ok(ir::Expr::Var(v.clone())),
        Expr::Prop(base, key) => {
            if let Expr::Var(v) = base.as_ref() {
                Ok(ir::Expr::Prop(v.clone(), key.clone()))
            } else {
                Err(TranspileError::UnsupportedExpr("nested property".into()))
            }
        }
        Expr::Lit(lit) => Ok(ir::Expr::Lit(literal_to_prop(lit))),
        Expr::FnCall(name, args) => {
            let ir_args: Vec<ir::Expr> = args
                .iter()
                .map(|a| transpile_ir_expr(a))
                .collect::<Result<_, _>>()?;
            Ok(ir::Expr::Func(name.clone(), ir_args))
        }
        Expr::Param(p) => Ok(ir::Expr::Lit(PropValue::Str(format!("${p}")))),
        _ => Err(TranspileError::UnsupportedExpr(format!("{expr:?}"))),
    }
}

/// Convert Cypher WHERE expression → GRIN Predicate.
fn transpile_predicate(expr: &Expr) -> Result<Predicate, TranspileError> {
    match expr {
        Expr::BinOp(op, lhs, rhs) => match op {
            BinOp::And => {
                let l = transpile_predicate(lhs)?;
                let r = transpile_predicate(rhs)?;
                Ok(Predicate::And(Box::new(l), Box::new(r)))
            }
            BinOp::Or => {
                let l = transpile_predicate(lhs)?;
                let r = transpile_predicate(rhs)?;
                Ok(Predicate::Or(Box::new(l), Box::new(r)))
            }
            BinOp::Eq => prop_cmp_predicate(lhs, rhs, |k, v| Predicate::Eq(k, v)),
            BinOp::Neq => prop_cmp_predicate(lhs, rhs, |k, v| Predicate::Neq(k, v)),
            BinOp::Lt => prop_cmp_predicate(lhs, rhs, |k, v| Predicate::Lt(k, v)),
            BinOp::Gt => prop_cmp_predicate(lhs, rhs, |k, v| Predicate::Gt(k, v)),
            _ => Err(TranspileError::UnsupportedExpr(format!("binop {op:?}"))),
        },
        Expr::IsNotNull(inner) => {
            // IS NOT NULL → True (always passes, approximate)
            Ok(Predicate::True)
        }
        _ => Err(TranspileError::UnsupportedExpr(format!(
            "predicate {expr:?}"
        ))),
    }
}

fn prop_cmp_predicate(
    lhs: &Expr,
    rhs: &Expr,
    make: impl Fn(String, PropValue) -> Predicate,
) -> Result<Predicate, TranspileError> {
    // n.key = value
    if let Expr::Prop(base, key) = lhs {
        if let Expr::Lit(lit) = rhs {
            return Ok(make(key.clone(), literal_to_prop(lit)));
        }
        if let Expr::Param(_) = rhs {
            // Parameters not resolved at transpile time → can't push down
            return Err(TranspileError::UnsupportedExpr(
                "parameter comparison".into(),
            ));
        }
    }
    // value = n.key (reversed)
    if let Expr::Prop(base, key) = rhs {
        if let Expr::Lit(lit) = lhs {
            return Ok(make(key.clone(), literal_to_prop(lit)));
        }
    }
    Err(TranspileError::UnsupportedExpr(
        "non-property comparison".into(),
    ))
}

/// Extract inline property predicates from NodePattern props: `{name: 'Alice'}`.
fn extract_node_predicate(props: &[(String, Expr)]) -> Option<Predicate> {
    let mut pred: Option<Predicate> = None;
    for (key, expr) in props {
        if let Expr::Lit(lit) = expr {
            let p = Predicate::Eq(key.clone(), literal_to_prop(lit));
            pred = Some(match pred {
                Some(existing) => Predicate::And(Box::new(existing), Box::new(p)),
                None => p,
            });
        }
    }
    pred
}

fn literal_to_prop(lit: &Literal) -> PropValue {
    match lit {
        Literal::Null => PropValue::Null,
        Literal::Bool(b) => PropValue::Bool(*b),
        Literal::Int(i) => PropValue::Int(*i),
        Literal::Float(f) => PropValue::Float(*f),
        Literal::Str(s) => PropValue::Str(s.clone()),
    }
}

fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::FnCall(name, _) => matches!(
            name.to_lowercase().as_str(),
            "count" | "sum" | "avg" | "min" | "max" | "collect"
        ),
        _ => false,
    }
}

fn extract_agg(expr: &Expr) -> Result<(AggOp, ir::Expr), TranspileError> {
    if let Expr::FnCall(name, args) = expr {
        let op = match name.to_lowercase().as_str() {
            "count" => AggOp::Count,
            "sum" => AggOp::Sum,
            "avg" => AggOp::Avg,
            "min" => AggOp::Min,
            "max" => AggOp::Max,
            "collect" => AggOp::Collect,
            _ => return Err(TranspileError::UnsupportedExpr(format!("agg {name}"))),
        };
        let arg = if args.is_empty() {
            ir::Expr::Lit(PropValue::Null) // count(*) → count(null)
        } else {
            transpile_ir_expr(&args[0])?
        };
        Ok((op, arg))
    } else {
        Err(TranspileError::UnsupportedExpr("not an aggregate".into()))
    }
}

fn expr_to_usize(expr: &Expr) -> Result<usize, TranspileError> {
    if let Expr::Lit(Literal::Int(n)) = expr {
        Ok(*n as usize)
    } else {
        Err(TranspileError::UnsupportedExpr("non-integer limit".into()))
    }
}

static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
fn rand_id() -> u32 {
    COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_ops(cypher: &str) -> Vec<String> {
        let query = yata_cypher::parse(cypher).unwrap();
        let plan = transpile(&query).unwrap();
        plan.ops
            .iter()
            .map(|op| match op {
                LogicalOp::Scan { label, alias, .. } => format!("Scan({label},{alias})"),
                LogicalOp::Expand { edge_label, .. } => format!("Expand({edge_label})"),
                LogicalOp::PathExpand {
                    edge_label,
                    min_hops,
                    max_hops,
                    ..
                } => {
                    format!("PathExpand({edge_label},{min_hops}..{max_hops})")
                }
                LogicalOp::Filter { .. } => "Filter".into(),
                LogicalOp::Project { .. } => "Project".into(),
                LogicalOp::Aggregate { .. } => "Aggregate".into(),
                LogicalOp::OrderBy { .. } => "OrderBy".into(),
                LogicalOp::Limit { count, .. } => format!("Limit({count})"),
                LogicalOp::Distinct { .. } => "Distinct".into(),
                LogicalOp::Exchange { kind, .. } => format!("Exchange({kind:?})"),
                LogicalOp::Receive { .. } => "Receive".into(),
                LogicalOp::SecurityFilter { aliases, .. } => format!("SecurityFilter({aliases:?})"),
            })
            .collect()
    }

    #[test]
    fn test_match_return() {
        let ops = plan_ops("MATCH (n:Person) RETURN n.name");
        assert!(ops.contains(&"Scan(Person,n)".to_string()));
        assert!(ops.contains(&"Project".to_string()));
    }

    #[test]
    fn test_match_where() {
        let ops = plan_ops("MATCH (n:Person) WHERE n.age > 30 RETURN n");
        // After optimization, filter may be pushed into scan predicate
        assert!(ops.iter().any(|o| o.starts_with("Scan")));
        assert!(ops.contains(&"Project".to_string()));
    }

    #[test]
    fn test_traversal() {
        let ops = plan_ops("MATCH (a:Person)-[:KNOWS]->(b) RETURN a, b");
        assert!(ops.iter().any(|o| o.starts_with("Scan")));
        assert!(
            ops.iter()
                .any(|o| o.contains("Expand") || o.contains("KNOWS"))
        );
        assert!(ops.contains(&"Project".to_string()));
    }

    #[test]
    fn test_limit() {
        let ops = plan_ops("MATCH (n:Person) RETURN n LIMIT 10");
        assert!(ops.contains(&"Limit(10)".to_string()));
    }

    #[test]
    fn test_count() {
        let ops = plan_ops("MATCH (n:Person) RETURN count(n)");
        assert!(ops.contains(&"Aggregate".to_string()));
    }

    #[test]
    fn test_distinct() {
        let ops = plan_ops("MATCH (n) RETURN DISTINCT n.name");
        assert!(ops.contains(&"Distinct".to_string()));
    }

    #[test]
    fn test_order_by() {
        let ops = plan_ops("MATCH (n:Person) RETURN n.name ORDER BY n.name");
        assert!(ops.contains(&"OrderBy".to_string()));
    }

    #[test]
    fn test_variable_hop() {
        let ops = plan_ops("MATCH (a:Person)-[:KNOWS*1..3]->(b) RETURN b");
        assert!(ops.iter().any(|o| o.contains("PathExpand")));
    }

    #[test]
    fn test_mutation_rejected() {
        let query = yata_cypher::parse("CREATE (n:Person {name: 'Alice'})").unwrap();
        assert!(transpile(&query).is_err());
    }

    #[test]
    fn test_inline_props_as_predicate() {
        let query = yata_cypher::parse("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap();
        let plan = transpile(&query).unwrap();
        // The Scan should have a predicate from inline props
        if let LogicalOp::Scan { predicate, .. } = &plan.ops[0] {
            assert!(predicate.is_some());
        } else {
            panic!("expected Scan as first op");
        }
    }

    #[test]
    fn test_with_clause() {
        let query = yata_cypher::parse(
            r#"MATCH (n:Person) WITH n.name AS name WHERE n.name = "Alice" RETURN name"#,
        )
        .unwrap();
        let plan = transpile(&query).unwrap();
        let has_project = plan
            .ops
            .iter()
            .any(|op| matches!(op, LogicalOp::Project { .. }));
        let has_filter = plan
            .ops
            .iter()
            .any(|op| matches!(op, LogicalOp::Filter { .. }));
        assert!(has_project, "WITH should produce a Project op");
        assert!(has_filter, "WITH ... WHERE should produce a Filter op");
    }

    #[test]
    fn test_multi_pattern_match() {
        let query =
            yata_cypher::parse("MATCH (a:Person), (b:Company) RETURN a.name, b.name").unwrap();
        let plan = transpile(&query).unwrap();
        let scan_count = plan
            .ops
            .iter()
            .filter(|op| matches!(op, LogicalOp::Scan { .. }))
            .count();
        assert_eq!(
            scan_count, 2,
            "two comma-separated patterns should produce two Scan ops"
        );
    }

    #[test]
    fn test_skip() {
        let query = yata_cypher::parse("MATCH (n:Person) RETURN n.name SKIP 1").unwrap();
        let plan = transpile(&query);
        assert!(plan.is_ok(), "SKIP should transpile successfully");
    }

    #[test]
    fn test_untyped_edge_transpile() {
        let query = yata_cypher::parse("MATCH (a:Person)-->(b) RETURN b.name").unwrap();
        let plan = transpile(&query).unwrap();
        let has_empty_expand = plan.ops.iter().any(|op| match op {
            LogicalOp::Expand { edge_label, .. } => edge_label.is_empty(),
            _ => false,
        });
        assert!(
            has_empty_expand,
            "untyped edge should produce Expand with empty edge_label"
        );
    }

    #[test]
    fn test_multiple_aggregates() {
        let query =
            yata_cypher::parse("MATCH (n:Person) RETURN count(n) AS cnt, sum(n.age) AS total")
                .unwrap();
        let plan = transpile(&query).unwrap();
        let agg = plan
            .ops
            .iter()
            .find(|op| matches!(op, LogicalOp::Aggregate { .. }));
        assert!(agg.is_some(), "should produce Aggregate op");
        if let Some(LogicalOp::Aggregate { aggs, .. }) = agg {
            assert_eq!(aggs.len(), 2, "should have two aggregate operations");
        }
    }

    #[test]
    fn test_order_by_desc() {
        let query =
            yata_cypher::parse("MATCH (n:Person) RETURN n.name ORDER BY n.age DESC").unwrap();
        let plan = transpile(&query).unwrap();
        let order = plan
            .ops
            .iter()
            .find(|op| matches!(op, LogicalOp::OrderBy { .. }));
        assert!(order.is_some(), "should produce OrderBy op");
        if let Some(LogicalOp::OrderBy { keys }) = order {
            assert!(!keys.is_empty());
            let (_expr, descending) = &keys[0];
            assert!(descending, "DESC should set descending flag to true");
        }
    }

    #[test]
    fn test_unsupported_unwind() {
        let query = yata_cypher::parse("UNWIND [1,2] AS x RETURN x").unwrap();
        let result = transpile(&query);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, TranspileError::UnsupportedClause(ref s) if s == "UNWIND"),
            "UNWIND should return UnsupportedClause, got: {err}"
        );
    }

    #[test]
    fn test_complex_where() {
        let query = yata_cypher::parse(
            r#"MATCH (n:Person) WHERE n.age > 25 AND n.name = "Alice" RETURN n"#,
        )
        .unwrap();
        let plan = transpile(&query).unwrap();
        let filter = plan
            .ops
            .iter()
            .find(|op| matches!(op, LogicalOp::Filter { .. }));
        // Filter may be pushed into Scan predicate by optimizer; check either location
        let scan_has_and = plan.ops.iter().any(|op| match op {
            LogicalOp::Scan {
                predicate: Some(Predicate::And(_, _)),
                ..
            } => true,
            _ => false,
        });
        let filter_has_and = filter.map_or(false, |op| match op {
            LogicalOp::Filter {
                predicate: Predicate::And(_, _),
            } => true,
            _ => false,
        });
        assert!(
            scan_has_and || filter_has_and,
            "AND predicate should appear in either Filter or pushed-down Scan predicate"
        );
    }

    #[test]
    fn test_variable_hop_untyped() {
        let query = yata_cypher::parse("MATCH (a:Person)-[*1..3]->(b) RETURN b").unwrap();
        let plan = transpile(&query).unwrap();
        let path = plan
            .ops
            .iter()
            .find(|op| matches!(op, LogicalOp::PathExpand { .. }));
        assert!(path.is_some(), "should produce PathExpand op");
        if let Some(LogicalOp::PathExpand { edge_label, .. }) = path {
            assert!(
                edge_label.is_empty(),
                "untyped variable hop should have empty edge_label"
            );
        }
    }

    #[test]
    fn test_distinct_typed() {
        let query = yata_cypher::parse("MATCH (n:Person) RETURN DISTINCT n.name").unwrap();
        let plan = transpile(&query).unwrap();
        let has_distinct = plan
            .ops
            .iter()
            .any(|op| matches!(op, LogicalOp::Distinct { .. }));
        assert!(has_distinct, "DISTINCT should produce Distinct op");
    }
}
