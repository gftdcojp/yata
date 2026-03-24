//! SQL → execution plan translator.
//!
//! Parses SQL SELECT/INSERT/UPDATE/DELETE and maps to yata CSR operations.
//! Table names map to vertex/edge labels in the PropertyGraphSchema.

use sqlparser::ast::{
    BinaryOperator, Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use yata_grin::PropValue;

#[derive(Debug, Clone)]
pub struct SqlPlan {
    pub table: String,
    pub columns: Vec<String>,        // empty = all (SELECT *)
    pub predicates: Vec<WherePred>,
    pub order_by: Vec<OrderSpec>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct WherePred {
    pub column: String,
    pub op: CompareOp,
    pub value: PropValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompareOp {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
}

#[derive(Debug, Clone)]
pub struct OrderSpec {
    pub column: String,
    pub ascending: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum PlanError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("no table specified")]
    NoTable,
}

/// Parse SQL string into an execution plan.
pub fn parse_select(sql: &str) -> Result<SqlPlan, PlanError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| PlanError::Parse(e.to_string()))?;

    if statements.is_empty() {
        return Err(PlanError::Parse("empty SQL".to_string()));
    }

    match &statements[0] {
        Statement::Query(query) => parse_query(query),
        _ => Err(PlanError::Unsupported(
            "only SELECT queries supported".to_string(),
        )),
    }
}

fn parse_query(query: &Query) -> Result<SqlPlan, PlanError> {
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select.as_ref(),
        _ => {
            return Err(PlanError::Unsupported(
                "only simple SELECT supported".to_string(),
            ))
        }
    };

    let table = extract_table(select)?;
    let columns = extract_columns(select);
    let predicates = extract_predicates(select)?;
    let order_by = extract_order_by(query);
    let limit = extract_limit(query);
    let offset = extract_offset(query);

    Ok(SqlPlan {
        table,
        columns,
        predicates,
        order_by,
        limit,
        offset,
    })
}

fn extract_table(select: &Select) -> Result<String, PlanError> {
    if select.from.is_empty() {
        return Err(PlanError::NoTable);
    }
    match &select.from[0].relation {
        TableFactor::Table { name, .. } => Ok(name.to_string()),
        _ => Err(PlanError::Unsupported("complex FROM not supported".to_string())),
    }
}

fn extract_columns(select: &Select) -> Vec<String> {
    let mut cols = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                cols.push(ident.value.clone());
            }
            SelectItem::Wildcard(_) => {
                return Vec::new(); // empty = all columns
            }
            _ => {}
        }
    }
    cols
}

fn extract_predicates(select: &Select) -> Result<Vec<WherePred>, PlanError> {
    let mut preds = Vec::new();
    if let Some(selection) = &select.selection {
        extract_expr_predicates(selection, &mut preds)?;
    }
    Ok(preds)
}

fn extract_expr_predicates(expr: &Expr, preds: &mut Vec<WherePred>) -> Result<(), PlanError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                extract_expr_predicates(left, preds)?;
                extract_expr_predicates(right, preds)?;
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::Gt
            | BinaryOperator::LtEq
            | BinaryOperator::GtEq => {
                if let (Expr::Identifier(ident), Some(val)) = (left.as_ref(), expr_to_value(right))
                {
                    let cmp = match op {
                        BinaryOperator::Eq => CompareOp::Eq,
                        BinaryOperator::NotEq => CompareOp::Neq,
                        BinaryOperator::Lt => CompareOp::Lt,
                        BinaryOperator::Gt => CompareOp::Gt,
                        BinaryOperator::LtEq => CompareOp::Lte,
                        BinaryOperator::GtEq => CompareOp::Gte,
                        _ => unreachable!(),
                    };
                    preds.push(WherePred {
                        column: ident.value.clone(),
                        op: cmp,
                        value: val,
                    });
                }
            }
            _ => {}
        },
        _ => {}
    }
    Ok(())
}

fn expr_to_value(expr: &Expr) -> Option<PropValue> {
    match expr {
        Expr::Value(val) => match val {
            Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Some(PropValue::Int(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Some(PropValue::Float(f))
                } else {
                    None
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Some(PropValue::Str(s.clone()))
            }
            Value::Boolean(b) => Some(PropValue::Bool(*b)),
            Value::Null => Some(PropValue::Null),
            _ => None,
        },
        _ => None,
    }
}

fn extract_order_by(query: &Query) -> Vec<OrderSpec> {
    let mut specs = Vec::new();
    if let Some(ref order_by) = query.order_by {
        for expr in &order_by.exprs {
            if let Expr::Identifier(ident) = &expr.expr {
                specs.push(OrderSpec {
                    column: ident.value.clone(),
                    ascending: expr.asc.unwrap_or(true),
                });
            }
        }
    }
    specs
}

fn extract_limit(query: &Query) -> Option<usize> {
    match &query.limit {
        Some(Expr::Value(Value::Number(n, _))) => n.parse().ok(),
        _ => None,
    }
}

fn extract_offset(query: &Query) -> Option<usize> {
    query.offset.as_ref().and_then(|o| match &o.value {
        Expr::Value(Value::Number(n, _)) => n.parse().ok(),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_select() {
        let plan = parse_select("SELECT name, age FROM Person").unwrap();
        assert_eq!(plan.table, "Person");
        assert_eq!(plan.columns, vec!["name", "age"]);
        assert!(plan.predicates.is_empty());
        assert!(plan.order_by.is_empty());
        assert!(plan.limit.is_none());
    }

    #[test]
    fn parse_select_star() {
        let plan = parse_select("SELECT * FROM Person").unwrap();
        assert_eq!(plan.table, "Person");
        assert!(plan.columns.is_empty()); // empty = all
    }

    #[test]
    fn parse_where_eq() {
        let plan = parse_select("SELECT * FROM Person WHERE age = 30").unwrap();
        assert_eq!(plan.predicates.len(), 1);
        assert_eq!(plan.predicates[0].column, "age");
        assert_eq!(plan.predicates[0].op, CompareOp::Eq);
        assert_eq!(plan.predicates[0].value, PropValue::Int(30));
    }

    #[test]
    fn parse_where_string() {
        let plan = parse_select("SELECT * FROM Person WHERE name = 'Alice'").unwrap();
        assert_eq!(plan.predicates[0].value, PropValue::Str("Alice".into()));
    }

    #[test]
    fn parse_where_and() {
        let plan = parse_select("SELECT * FROM Person WHERE age >= 20 AND age <= 30").unwrap();
        assert_eq!(plan.predicates.len(), 2);
        assert_eq!(plan.predicates[0].op, CompareOp::Gte);
        assert_eq!(plan.predicates[1].op, CompareOp::Lte);
    }

    #[test]
    fn parse_order_by_limit() {
        let plan =
            parse_select("SELECT * FROM Person ORDER BY age DESC LIMIT 10").unwrap();
        assert_eq!(plan.order_by.len(), 1);
        assert_eq!(plan.order_by[0].column, "age");
        assert!(!plan.order_by[0].ascending);
        assert_eq!(plan.limit, Some(10));
    }

    #[test]
    fn parse_offset() {
        let plan =
            parse_select("SELECT * FROM Person LIMIT 10 OFFSET 20").unwrap();
        assert_eq!(plan.limit, Some(10));
        assert_eq!(plan.offset, Some(20));
    }
}
