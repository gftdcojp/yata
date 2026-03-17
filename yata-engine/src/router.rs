/// Detect whether a Cypher statement is a mutation (CREATE/MERGE/DELETE/SET/REMOVE).
pub fn is_cypher_mutation(cypher: &str) -> bool {
    let upper = cypher.to_uppercase();
    for kw in &["CREATE", "MERGE", "DELETE", "DETACH", "SET", "REMOVE"] {
        if let Some(pos) = upper.find(kw) {
            let before_ok = pos == 0
                || upper.as_bytes()[pos - 1].is_ascii_whitespace()
                || upper.as_bytes()[pos - 1] == b'(';
            let after_pos = pos + kw.len();
            let after_ok = after_pos >= upper.len()
                || upper.as_bytes()[after_pos].is_ascii_whitespace()
                || upper.as_bytes()[after_pos] == b'('
                || upper.as_bytes()[after_pos] == b' ';
            if before_ok && after_ok {
                return true;
            }
        }
    }
    false
}

/// Extract label + rel_type hints from Cypher AST for Lance pushdown.
/// Returns (node_labels, rel_types, is_read_only) or None if no pushdown possible.
pub fn extract_pushdown_hints(cypher: &str) -> Option<(Vec<String>, Vec<String>)> {
    let query = yata_cypher::parse(cypher).ok()?;
    let hints = yata_graph::hints::QueryHints::extract(&query);
    if !hints.is_read_only {
        return None;
    }
    if !hints.has_filters() {
        return None;
    }
    Some((hints.node_labels, hints.rel_types))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_mutation() {
        assert!(is_cypher_mutation("CREATE (n:Person {name: 'Alice'})"));
        assert!(is_cypher_mutation("MERGE (n:Person {id: $id})"));
        assert!(is_cypher_mutation("MATCH (n) DELETE n"));
        assert!(is_cypher_mutation("MATCH (n) SET n.name = 'Bob'"));
        assert!(!is_cypher_mutation("MATCH (n:Person) RETURN n.name"));
        assert!(!is_cypher_mutation("MATCH (n) WHERE n.createdAt > 0 RETURN n"));
    }
}
