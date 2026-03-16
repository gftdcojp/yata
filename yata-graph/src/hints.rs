//! Query hint extraction from parsed Cypher AST.
//!
//! Walks MATCH clause patterns to collect node labels, relationship types,
//! and inline property equality filters. Used by `LanceGraphStore` to build
//! Lance SQL pushdown filters for subgraph loading.

use yata_cypher::ast::*;

/// Hints extracted from a parsed Cypher query for Lance SQL pushdown.
pub struct QueryHints {
    pub node_labels: Vec<String>,
    pub rel_types: Vec<String>,
    /// Equality filters from inline property maps, e.g. `{name: 'Alice'}`.
    /// Each entry is `(property_key, json_encoded_value)`.
    pub prop_eq_filters: Vec<(String, String)>,
    pub is_read_only: bool,
}

impl QueryHints {
    /// Extract hints by walking the AST of a parsed Cypher query.
    pub fn extract(query: &Query) -> Self {
        let mut node_labels = Vec::new();
        let mut rel_types = Vec::new();
        let mut prop_eq_filters = Vec::new();
        let mut is_read_only = true;

        for clause in &query.clauses {
            Self::visit_clause(
                clause,
                &mut node_labels,
                &mut rel_types,
                &mut prop_eq_filters,
                &mut is_read_only,
            );
        }

        // Dedup while preserving order.
        node_labels.dedup();
        rel_types.dedup();

        Self { node_labels, rel_types, prop_eq_filters, is_read_only }
    }

    /// Returns true if there are any label or property filters to push down.
    pub fn has_filters(&self) -> bool {
        !self.node_labels.is_empty() || !self.prop_eq_filters.is_empty()
    }

    fn visit_clause(
        clause: &Clause,
        node_labels: &mut Vec<String>,
        rel_types: &mut Vec<String>,
        prop_eq_filters: &mut Vec<(String, String)>,
        is_read_only: &mut bool,
    ) {
        match clause {
            Clause::Match { patterns, .. } | Clause::OptionalMatch { patterns, .. } => {
                for pattern in patterns {
                    Self::visit_pattern(pattern, node_labels, rel_types, prop_eq_filters);
                }
            }
            Clause::Create { .. }
            | Clause::Merge { .. }
            | Clause::Delete { .. }
            | Clause::Set { .. }
            | Clause::Remove { .. } => {
                *is_read_only = false;
            }
            Clause::Foreach { body, .. } => {
                for sub in body {
                    Self::visit_clause(sub, node_labels, rel_types, prop_eq_filters, is_read_only);
                }
            }
            Clause::Call { subquery } => {
                for sub in subquery {
                    Self::visit_clause(sub, node_labels, rel_types, prop_eq_filters, is_read_only);
                }
            }
            _ => {}
        }
    }

    fn visit_pattern(
        pattern: &Pattern,
        node_labels: &mut Vec<String>,
        rel_types: &mut Vec<String>,
        prop_eq_filters: &mut Vec<(String, String)>,
    ) {
        for elem in &pattern.elements {
            match elem {
                PatternElement::Node(np) => {
                    for label in &np.labels {
                        if !node_labels.contains(label) {
                            node_labels.push(label.clone());
                        }
                    }
                    Self::extract_prop_eq(&np.props, prop_eq_filters);
                }
                PatternElement::Rel(rp) => {
                    for rt in &rp.types {
                        if !rel_types.contains(rt) {
                            rel_types.push(rt.clone());
                        }
                    }
                    Self::extract_prop_eq(&rp.props, prop_eq_filters);
                }
            }
        }
    }

    /// Extract equality filters from inline property maps like `{name: 'Alice'}`.
    /// Only literal values (string, int, float, bool) are captured.
    fn extract_prop_eq(
        props: &[(String, Expr)],
        out: &mut Vec<(String, String)>,
    ) {
        for (key, expr) in props {
            if let Expr::Lit(lit) = expr {
                let json_val = match lit {
                    Literal::Str(s) => serde_json::Value::String(s.clone()).to_string(),
                    Literal::Int(i) => i.to_string(),
                    Literal::Float(f) => f.to_string(),
                    Literal::Bool(b) => b.to_string(),
                    Literal::Null => continue,
                };
                out.push((key.clone(), json_val));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_labels_and_types() {
        let q = yata_cypher::parse("MATCH (n:Person)-[:KNOWS]->(m:Company) RETURN n, m").unwrap();
        let hints = QueryHints::extract(&q);
        assert!(hints.is_read_only);
        assert_eq!(hints.node_labels, vec!["Person", "Company"]);
        assert_eq!(hints.rel_types, vec!["KNOWS"]);
        assert!(hints.has_filters());
    }

    #[test]
    fn test_extract_prop_eq() {
        let q = yata_cypher::parse("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap();
        let hints = QueryHints::extract(&q);
        assert_eq!(hints.prop_eq_filters.len(), 1);
        assert_eq!(hints.prop_eq_filters[0].0, "name");
        assert_eq!(hints.prop_eq_filters[0].1, "\"Alice\"");
    }

    #[test]
    fn test_mutation_detected() {
        let q = yata_cypher::parse("CREATE (n:Person {name: 'Bob'})").unwrap();
        let hints = QueryHints::extract(&q);
        assert!(!hints.is_read_only);
    }

    #[test]
    fn test_no_filters() {
        let q = yata_cypher::parse("MATCH (n) RETURN n").unwrap();
        let hints = QueryHints::extract(&q);
        assert!(!hints.has_filters());
        assert!(hints.node_labels.is_empty());
    }
}
