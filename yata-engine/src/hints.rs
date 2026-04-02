//! Query hints extracted from Cypher AST for partition routing and Lance pushdown.

/// Promoted columns in Format D schema that support Lance SQL filter pushdown.
const PROMOTED_COLUMNS: &[&str] = &["repo", "owner_did", "name", "app_id", "rkey", "timestamp_ms", "label"];

pub struct QueryHints {
    pub node_labels: Vec<String>,
    pub rel_types: Vec<String>,
    pub is_read_only: bool,
    /// Lance SQL WHERE fragments extracted from Cypher WHERE clause.
    /// Only includes conditions on promoted columns (pushdown-safe).
    pub lance_filters: Vec<String>,
    /// LIMIT value if present in the Cypher query.
    pub limit: Option<usize>,
}

impl QueryHints {
    pub fn extract(cypher: &str) -> Self {
        let upper = cypher.to_uppercase();
        let is_read_only = !upper.contains("CREATE") && !upper.contains("MERGE")
            && !upper.contains("DELETE") && !upper.contains("SET ");

        let mut node_labels = Vec::new();
        let mut rel_types = Vec::new();

        // Extract (:Label) patterns
        let mut chars = cypher.chars().peekable();
        while let Some(c) = chars.next() {
            if c == ':' {
                let mut label = String::new();
                while let Some(&next) = chars.peek() {
                    if next.is_alphanumeric() || next == '_' {
                        label.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                if !label.is_empty() {
                    // Check if inside [] (relationship) or () (node)
                    let before: String = cypher[..cypher.find(&format!(":{label}")).unwrap_or(0)].chars().rev().take(20).collect();
                    if before.contains('[') {
                        rel_types.push(label);
                    } else {
                        node_labels.push(label);
                    }
                }
            }
        }
        node_labels.sort();
        node_labels.dedup();
        rel_types.sort();
        rel_types.dedup();

        let lance_filters = Self::extract_lance_filters(cypher);
        let limit = Self::extract_limit(&upper);

        Self { node_labels, rel_types, is_read_only, lance_filters, limit }
    }

    pub fn has_filters(&self) -> bool {
        !self.node_labels.is_empty() || !self.rel_types.is_empty()
    }

    /// Build a combined Lance SQL filter string from label pushdown + WHERE pushdown.
    pub fn to_lance_filter(&self) -> String {
        let mut parts = Vec::new();
        if !self.node_labels.is_empty() {
            let labels: Vec<String> = self.node_labels.iter()
                .map(|l| format!("'{}'", l.replace('\'', "''")))
                .collect();
            if labels.len() == 1 {
                parts.push(format!("label = {}", labels[0]));
            } else {
                parts.push(format!("label IN ({})", labels.join(", ")));
            }
        }
        for f in &self.lance_filters {
            parts.push(f.clone());
        }
        parts.join(" AND ")
    }

    /// Extract WHERE conditions that reference promoted columns (safe for Lance pushdown).
    ///
    /// Parses simple patterns: `n.col > value`, `n.col = 'str'`, `n.col < value`.
    /// Only emits filters for promoted columns to avoid pushing down val_json conditions.
    fn extract_lance_filters(cypher: &str) -> Vec<String> {
        let mut filters = Vec::new();
        let upper = cypher.to_uppercase();
        let where_pos = match upper.find("WHERE") {
            Some(p) => p,
            None => return filters,
        };
        // Extract the WHERE clause text (up to RETURN/ORDER/LIMIT/WITH or end)
        let after_where = &cypher[where_pos + 5..];
        let clause_end = ["RETURN", "ORDER", "LIMIT", "WITH", "CREATE", "MERGE", "DELETE", "SET"]
            .iter()
            .filter_map(|kw| after_where.to_uppercase().find(kw))
            .min()
            .unwrap_or(after_where.len());
        let where_text = after_where[..clause_end].trim();

        // Split by AND and check each condition
        for condition in where_text.split(" AND ").chain(where_text.split(" and ")) {
            let cond = condition.trim();
            if cond.is_empty() { continue; }
            // Match patterns like `n.column_name op value`
            if let Some(lance_cond) = parse_simple_condition(cond) {
                filters.push(lance_cond);
            }
        }
        filters.sort();
        filters.dedup();
        filters
    }

    /// Extract LIMIT value from Cypher query.
    fn extract_limit(upper: &str) -> Option<usize> {
        let limit_pos = upper.rfind("LIMIT")?;
        let after = upper[limit_pos + 5..].trim();
        let num_end = after.find(|c: char| !c.is_ascii_digit()).unwrap_or(after.len());
        after[..num_end].parse().ok()
    }
}

/// Parse a simple WHERE condition and return Lance SQL if it references a promoted column.
///
/// Handles: `var.col op value`, `var.col op 'string'`, `var.col op number`.
fn parse_simple_condition(cond: &str) -> Option<String> {
    // Match `identifier.column op value` pattern
    let ops = &[">=", "<=", "!=", "<>", "=", ">", "<"];
    for op in ops {
        if let Some(op_pos) = cond.find(op) {
            let lhs = cond[..op_pos].trim();
            let rhs = cond[op_pos + op.len()..].trim();
            // lhs should be `var.column` — extract column name
            let col_name = if let Some(dot) = lhs.rfind('.') {
                &lhs[dot + 1..]
            } else {
                continue;
            };
            let col_name = col_name.trim();
            // Check if it's a promoted column
            if PROMOTED_COLUMNS.iter().any(|&c| c.eq_ignore_ascii_case(col_name)) {
                let lance_op = if *op == "<>" { "!=" } else { op };
                return Some(format!("{col_name} {lance_op} {rhs}"));
            }
        }
    }
    None
}
