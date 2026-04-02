//! Cypher AST → GIE IR Plan transpiler.
//!
//! Converts parsed Cypher queries into GIE logical plans that execute directly
//! on a GRIN-compatible graph store — zero MemoryGraph copy.
//!
//! Only read-only queries are supported. Mutations (CREATE/MERGE/SET/DELETE)
//! return `TranspileError::UnsupportedClause`; the engine routes mutations
//! through the dedicated mutation path (CSR copy → mutate → rebuild).

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
/// Returns `Err` for mutations or unsupported constructs.
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
        // Mutations → unsupported (engine routes to dedicated mutation path)
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
                let (alias, pred) = transpile_predicate(w)?;
                b = match alias {
                    Some(alias) => b.filter_on(&alias, pred),
                    None => b.filter(pred),
                };
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
        let (alias, pred) = transpile_predicate(w)?;
        builder = match alias {
            Some(alias) => builder.filter_on(&alias, pred),
            None => builder.filter(pred),
        };
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

/// Convert Cypher WHERE expression → GRIN Predicate plus the bound alias when unambiguous.
fn transpile_predicate(expr: &Expr) -> Result<(Option<String>, Predicate), TranspileError> {
    match expr {
        Expr::BinOp(op, lhs, rhs) => match op {
            BinOp::And => {
                let (l_alias, l_pred) = transpile_predicate(lhs)?;
                let (r_alias, r_pred) = transpile_predicate(rhs)?;
                let alias = if l_alias == r_alias { l_alias } else { None };
                Ok((alias, Predicate::And(Box::new(l_pred), Box::new(r_pred))))
            }
            BinOp::Or => {
                let (l_alias, l_pred) = transpile_predicate(lhs)?;
                let (r_alias, r_pred) = transpile_predicate(rhs)?;
                let alias = if l_alias == r_alias { l_alias } else { None };
                Ok((alias, Predicate::Or(Box::new(l_pred), Box::new(r_pred))))
            }
            BinOp::Eq => prop_cmp_predicate(lhs, rhs, |k, v| Predicate::Eq(k, v)),
            BinOp::Neq => prop_cmp_predicate(lhs, rhs, |k, v| Predicate::Neq(k, v)),
            BinOp::Lt => prop_cmp_predicate(lhs, rhs, |k, v| Predicate::Lt(k, v)),
            BinOp::Gt => prop_cmp_predicate(lhs, rhs, |k, v| Predicate::Gt(k, v)),
            _ => Err(TranspileError::UnsupportedExpr(format!("binop {op:?}"))),
        },
        Expr::IsNotNull(_inner) => {
            // IS NOT NULL → True (always passes, approximate)
            Ok((None, Predicate::True))
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
) -> Result<(Option<String>, Predicate), TranspileError> {
    // n.key = value
    if let Expr::Prop(base, key) = lhs {
        if let Expr::Lit(lit) = rhs {
            return Ok((prop_base_alias(base), make(key.clone(), literal_to_prop(lit))));
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
            return Ok((prop_base_alias(base), make(key.clone(), literal_to_prop(lit))));
        }
    }
    Err(TranspileError::UnsupportedExpr(
        "non-property comparison".into(),
    ))
}

fn prop_base_alias(base: &Expr) -> Option<String> {
    match base {
        Expr::Var(var) => Some(var.clone()),
        _ => None,
    }
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
                ..
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

    // ── Mutation rejection tests ─────────────────────────────────────

    #[test]
    fn test_set_rejected() {
        let query = yata_cypher::parse("MATCH (n:Person) SET n.age = 40 RETURN n").unwrap();
        let result = transpile(&query);
        assert!(result.is_err());
        match result.unwrap_err() {
            TranspileError::UnsupportedClause(s) => assert_eq!(s, "SET"),
            e => panic!("expected UnsupportedClause(SET), got: {e}"),
        }
    }

    #[test]
    fn test_delete_rejected() {
        let query = yata_cypher::parse("MATCH (n:Person) DELETE n").unwrap();
        let result = transpile(&query);
        assert!(result.is_err());
        match result.unwrap_err() {
            TranspileError::UnsupportedClause(s) => assert_eq!(s, "DELETE"),
            e => panic!("expected UnsupportedClause(DELETE), got: {e}"),
        }
    }

    #[test]
    fn test_merge_rejected() {
        let query = yata_cypher::parse("MERGE (n:Person {name: 'Alice'}) RETURN n").unwrap();
        let result = transpile(&query);
        assert!(result.is_err());
        match result.unwrap_err() {
            TranspileError::UnsupportedClause(s) => assert_eq!(s, "MERGE"),
            e => panic!("expected UnsupportedClause(MERGE), got: {e}"),
        }
    }

    #[test]
    fn test_create_node_rejected() {
        let query = yata_cypher::parse("CREATE (n:Person {name: 'Bob'})").unwrap();
        let result = transpile(&query);
        assert!(result.is_err());
        match result.unwrap_err() {
            TranspileError::UnsupportedClause(s) => assert_eq!(s, "CREATE"),
            e => panic!("expected UnsupportedClause(CREATE), got: {e}"),
        }
    }

    // ── Predicate transpilation tests ────────────────────────────────

    #[test]
    fn test_or_predicate() {
        let query = yata_cypher::parse(
            r#"MATCH (n:Person) WHERE n.age = 25 OR n.age = 35 RETURN n"#,
        )
        .unwrap();
        let plan = transpile(&query).unwrap();
        // Should have either a Filter with Or or a Scan with pushed-down Or
        let has_or = plan.ops.iter().any(|op| match op {
            LogicalOp::Filter {
                predicate: Predicate::Or(_, _),
                ..
            } => true,
            LogicalOp::Scan {
                predicate: Some(Predicate::Or(_, _)),
                ..
            } => true,
            _ => false,
        });
        assert!(has_or, "OR predicate should appear in Filter or Scan");
    }

    #[test]
    fn test_neq_predicate() {
        let query = yata_cypher::parse(
            r#"MATCH (n:Person) WHERE n.name <> "Alice" RETURN n"#,
        )
        .unwrap();
        let plan = transpile(&query).unwrap();
        let has_neq = plan.ops.iter().any(|op| match op {
            LogicalOp::Filter {
                predicate: Predicate::Neq(_, _),
                ..
            } => true,
            LogicalOp::Scan {
                predicate: Some(Predicate::Neq(_, _)),
                ..
            } => true,
            _ => false,
        });
        assert!(has_neq, "NEQ predicate should appear in plan");
    }

    #[test]
    fn test_lt_predicate() {
        let query = yata_cypher::parse(
            r#"MATCH (n:Person) WHERE n.age < 30 RETURN n"#,
        )
        .unwrap();
        let plan = transpile(&query).unwrap();
        let has_lt = plan.ops.iter().any(|op| match op {
            LogicalOp::Filter {
                predicate: Predicate::Lt(_, _),
                ..
            } => true,
            LogicalOp::Scan {
                predicate: Some(Predicate::Lt(_, _)),
                ..
            } => true,
            _ => false,
        });
        assert!(has_lt, "LT predicate should appear in plan");
    }

    // ── Aggregate function tests ─────────────────────────────────────

    #[test]
    fn test_sum_aggregate() {
        let query = yata_cypher::parse("MATCH (n:Person) RETURN sum(n.age) AS total").unwrap();
        let plan = transpile(&query).unwrap();
        let agg = plan.ops.iter().find(|op| matches!(op, LogicalOp::Aggregate { .. }));
        assert!(agg.is_some(), "should produce Aggregate op for sum()");
        if let Some(LogicalOp::Aggregate { aggs, .. }) = agg {
            assert_eq!(aggs.len(), 1);
            assert!(matches!(aggs[0].1, AggOp::Sum));
        }
    }

    #[test]
    fn test_avg_aggregate() {
        let query = yata_cypher::parse("MATCH (n:Person) RETURN avg(n.age) AS average").unwrap();
        let plan = transpile(&query).unwrap();
        let agg = plan.ops.iter().find(|op| matches!(op, LogicalOp::Aggregate { .. }));
        assert!(agg.is_some(), "should produce Aggregate op for avg()");
        if let Some(LogicalOp::Aggregate { aggs, .. }) = agg {
            assert!(matches!(aggs[0].1, AggOp::Avg));
        }
    }

    #[test]
    fn test_min_aggregate() {
        let query = yata_cypher::parse("MATCH (n:Person) RETURN min(n.age) AS youngest").unwrap();
        let plan = transpile(&query).unwrap();
        let agg = plan.ops.iter().find(|op| matches!(op, LogicalOp::Aggregate { .. }));
        assert!(agg.is_some());
        if let Some(LogicalOp::Aggregate { aggs, .. }) = agg {
            assert!(matches!(aggs[0].1, AggOp::Min));
        }
    }

    #[test]
    fn test_max_aggregate() {
        let query = yata_cypher::parse("MATCH (n:Person) RETURN max(n.age) AS oldest").unwrap();
        let plan = transpile(&query).unwrap();
        let agg = plan.ops.iter().find(|op| matches!(op, LogicalOp::Aggregate { .. }));
        assert!(agg.is_some());
        if let Some(LogicalOp::Aggregate { aggs, .. }) = agg {
            assert!(matches!(aggs[0].1, AggOp::Max));
        }
    }

    #[test]
    fn test_collect_aggregate() {
        let query =
            yata_cypher::parse("MATCH (n:Person) RETURN collect(n.name) AS names").unwrap();
        let plan = transpile(&query).unwrap();
        let agg = plan.ops.iter().find(|op| matches!(op, LogicalOp::Aggregate { .. }));
        assert!(agg.is_some());
        if let Some(LogicalOp::Aggregate { aggs, .. }) = agg {
            assert!(matches!(aggs[0].1, AggOp::Collect));
        }
    }

    #[test]
    fn test_count_star() {
        let query = yata_cypher::parse("MATCH (n:Person) RETURN count(n)").unwrap();
        let plan = transpile(&query).unwrap();
        let agg = plan.ops.iter().find(|op| matches!(op, LogicalOp::Aggregate { .. }));
        assert!(agg.is_some());
        if let Some(LogicalOp::Aggregate { aggs, .. }) = agg {
            assert!(matches!(aggs[0].1, AggOp::Count));
        }
    }

    // ── Aggregate with GROUP BY ──────────────────────────────────────

    #[test]
    fn test_group_by_with_count() {
        let query = yata_cypher::parse(
            "MATCH (n:Person) RETURN n.dept, count(n) AS cnt",
        )
        .unwrap();
        let plan = transpile(&query).unwrap();
        let agg = plan.ops.iter().find(|op| matches!(op, LogicalOp::Aggregate { .. }));
        assert!(agg.is_some());
        if let Some(LogicalOp::Aggregate { group_by, aggs }) = agg {
            assert_eq!(group_by.len(), 1, "should have 1 group-by key");
            assert_eq!(aggs.len(), 1, "should have 1 aggregate");
        }
    }

    // ── Skip/Limit combination ───────────────────────────────────────

    #[test]
    fn test_skip_and_limit_combined() {
        let query =
            yata_cypher::parse("MATCH (n:Person) RETURN n.name SKIP 1 LIMIT 2").unwrap();
        let plan = transpile(&query).unwrap();
        let limit = plan.ops.iter().find(|op| matches!(op, LogicalOp::Limit { .. }));
        assert!(limit.is_some(), "should produce Limit op for SKIP+LIMIT");
        if let Some(LogicalOp::Limit { count, offset }) = limit {
            assert_eq!(*count, 2);
            assert_eq!(*offset, 1);
        }
    }

    // ── Expression transpilation ─────────────────────────────────────

    #[test]
    fn test_return_alias() {
        let query =
            yata_cypher::parse("MATCH (n:Person) RETURN n.name AS person_name").unwrap();
        let plan = transpile(&query).unwrap();
        let project = plan.ops.iter().find(|op| matches!(op, LogicalOp::Project { .. }));
        assert!(project.is_some());
        if let Some(LogicalOp::Project { exprs }) = project {
            assert_eq!(exprs.len(), 1);
            assert!(
                matches!(&exprs[0], ir::Expr::Alias(_, alias) if alias == "person_name"),
                "should have alias 'person_name'"
            );
        }
    }

    #[test]
    fn test_return_literal() {
        let query = yata_cypher::parse("MATCH (n:Person) RETURN 42, n.name").unwrap();
        let plan = transpile(&query).unwrap();
        let project = plan.ops.iter().find(|op| matches!(op, LogicalOp::Project { .. }));
        assert!(project.is_some());
        if let Some(LogicalOp::Project { exprs }) = project {
            assert!(
                exprs.iter().any(|e| matches!(e, ir::Expr::Lit(PropValue::Int(42)))),
                "should have literal 42 in projections"
            );
        }
    }

    // ── Transpile_secured tests ──────────────────────────────────────

    #[test]
    fn test_transpile_secured_injects_after_scan() {
        let query = yata_cypher::parse("MATCH (n:Person) RETURN n.name").unwrap();
        let scope = ir::SecurityScope {
            max_sensitivity_ord: 1,
            ..Default::default()
        };
        let plan = transpile_secured(&query, scope).unwrap();
        // SecurityFilter should appear right after Scan
        let scan_idx = plan.ops.iter().position(|op| matches!(op, LogicalOp::Scan { .. }));
        assert!(scan_idx.is_some());
        let next = &plan.ops[scan_idx.unwrap() + 1];
        assert!(
            matches!(next, LogicalOp::SecurityFilter { .. }),
            "SecurityFilter should be injected after Scan, got: {next:?}"
        );
    }

    #[test]
    fn test_transpile_secured_injects_after_expand() {
        let query =
            yata_cypher::parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN b.name").unwrap();
        let scope = ir::SecurityScope {
            max_sensitivity_ord: 1,
            ..Default::default()
        };
        let plan = transpile_secured(&query, scope).unwrap();
        // Should have SecurityFilter after both Scan and Expand
        let security_count = plan
            .ops
            .iter()
            .filter(|op| matches!(op, LogicalOp::SecurityFilter { .. }))
            .count();
        assert!(
            security_count >= 2,
            "should inject SecurityFilter after both Scan and Expand, got {security_count}"
        );
    }

    #[test]
    fn test_transpile_secured_bypass_no_filter() {
        let query = yata_cypher::parse("MATCH (n:Person) RETURN n.name").unwrap();
        let scope = ir::SecurityScope {
            bypass: true,
            ..Default::default()
        };
        let plan = transpile_secured(&query, scope).unwrap();
        let has_security = plan
            .ops
            .iter()
            .any(|op| matches!(op, LogicalOp::SecurityFilter { .. }));
        assert!(
            !has_security,
            "bypass scope should not inject SecurityFilter"
        );
    }

    // ── Edge direction tests ─────────────────────────────────────────

    #[test]
    fn test_left_direction_edge() {
        let query =
            yata_cypher::parse("MATCH (a:Person)<-[:KNOWS]-(b) RETURN a, b").unwrap();
        let plan = transpile(&query).unwrap();
        let expand = plan.ops.iter().find(|op| matches!(op, LogicalOp::Expand { .. }));
        assert!(expand.is_some(), "should have Expand op");
    }

    // ── Multiple return items without aggregation ────────────────────

    #[test]
    fn test_multiple_return_items() {
        let query =
            yata_cypher::parse("MATCH (n:Person) RETURN n.name, n.age").unwrap();
        let plan = transpile(&query).unwrap();
        let project = plan.ops.iter().find(|op| matches!(op, LogicalOp::Project { .. }));
        assert!(project.is_some());
        if let Some(LogicalOp::Project { exprs }) = project {
            assert_eq!(exprs.len(), 2, "should have 2 projected expressions");
        }
    }

    // ── Inline property predicate with multiple props ────────────────

    #[test]
    fn test_inline_multi_props() {
        let query = yata_cypher::parse(
            "MATCH (n:Person {name: 'Alice', age: 30}) RETURN n",
        )
        .unwrap();
        let plan = transpile(&query).unwrap();
        let scan = plan.ops.iter().find(|op| matches!(op, LogicalOp::Scan { .. }));
        if let Some(LogicalOp::Scan { predicate: Some(Predicate::And(_, _)), .. }) = scan {
            // Multiple inline props should produce And predicate
        } else if let Some(LogicalOp::Scan { predicate: Some(_), .. }) = scan {
            // At least some predicate from inline props
        } else {
            panic!("expected Scan with predicate from inline props");
        }
    }

    // ── Path expand with typed edge ──────────────────────────────────

    #[test]
    fn test_path_expand_typed() {
        let ops = plan_ops("MATCH (a:Person)-[:KNOWS*2..5]->(b) RETURN b");
        let path = ops.iter().find(|o| o.contains("PathExpand"));
        assert!(path.is_some(), "should produce PathExpand op");
        assert!(
            path.unwrap().contains("KNOWS"),
            "PathExpand should have KNOWS edge label"
        );
        assert!(
            path.unwrap().contains("2..5"),
            "PathExpand should have correct hop range"
        );
    }
}
