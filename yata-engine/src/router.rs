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

/// Infer OCEL v2 activity name from a Cypher mutation statement.
pub fn infer_activity(cypher: &str) -> String {
    let upper = cypher.to_uppercase();
    let verb = if upper.contains("DELETE") || upper.contains("DETACH") {
        "Delete"
    } else if upper.contains("MERGE") {
        "Merge"
    } else if upper.contains("CREATE") {
        "Create"
    } else if upper.contains("SET") || upper.contains("REMOVE") {
        "Update"
    } else {
        "Mutate"
    };
    // Extract first label from pattern (:Label)
    let label = cypher
        .find(':')
        .and_then(|i| {
            let rest = &cypher[i + 1..];
            let end = rest
                .find(|c: char| !c.is_alphanumeric() && c != '_')
                .unwrap_or(rest.len());
            let l = &rest[..end];
            if l.is_empty() { None } else { Some(l) }
        })
        .unwrap_or("Node");
    format!("{}{}", verb, label)
}

/// Extract label + rel_type hints from Cypher AST for query pushdown.
/// Returns (node_labels, rel_types, is_read_only) or None if no pushdown possible.
pub fn extract_pushdown_hints(cypher: &str) -> Option<(Vec<String>, Vec<String>)> {
    let query = yata_cypher::parse(cypher).ok()?;
    let hints = crate::hints::QueryHints::extract(&query);
    if !hints.is_read_only {
        return None;
    }
    if !hints.has_filters() {
        return None;
    }
    Some((hints.node_labels, hints.rel_types))
}

/// Extract label hints from a mutation Cypher (CREATE/MERGE/MATCH patterns).
/// Used to load only the referenced labels instead of the entire graph.
pub fn extract_mutation_hints(cypher: &str) -> Option<(Vec<String>, Vec<String>)> {
    let query = yata_cypher::parse(cypher).ok()?;
    let hints = crate::hints::QueryHints::extract(&query);
    if hints.node_labels.is_empty() && hints.rel_types.is_empty() {
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
        assert!(!is_cypher_mutation(
            "MATCH (n) WHERE n.createdAt > 0 RETURN n"
        ));
    }

    #[test]
    fn test_is_mutation_detach_delete() {
        assert!(is_cypher_mutation("MATCH (n) DETACH DELETE n"));
    }

    #[test]
    fn test_is_mutation_remove() {
        assert!(is_cypher_mutation("MATCH (n) REMOVE n.prop"));
    }

    #[test]
    fn test_is_mutation_keyword_in_string_not_detected() {
        // "CREATE" preceded by ' is not detected (boundary check rejects non-whitespace/paren).
        assert!(!is_cypher_mutation(
            "MATCH (n) WHERE n.name = 'CREATE' RETURN n"
        ));
    }

    #[test]
    fn test_is_mutation_keyword_with_paren() {
        // SET after whitespace + CREATE after (
        assert!(is_cypher_mutation("MATCH (n) WHERE(CREATE x) RETURN n"));
    }

    #[test]
    fn test_is_mutation_case_insensitive() {
        assert!(is_cypher_mutation("create (n:X)"));
        assert!(is_cypher_mutation("MATCH (n) set n.x = 1"));
        assert!(is_cypher_mutation("match (n) merge (m:Y)"));
    }

    #[test]
    fn test_pushdown_hints_read_query() {
        let hints = extract_pushdown_hints("MATCH (n:Person) RETURN n.name");
        assert!(hints.is_some());
        let (labels, _) = hints.unwrap();
        assert!(labels.contains(&"Person".to_string()));
    }

    #[test]
    fn test_pushdown_hints_mutation_returns_none() {
        assert!(extract_pushdown_hints("CREATE (n:Person)").is_none());
    }

    #[test]
    fn test_pushdown_hints_no_label() {
        // MATCH (n) without label — no pushdown
        let hints = extract_pushdown_hints("MATCH (n) RETURN n");
        assert!(hints.is_none());
    }

    #[test]
    fn test_pushdown_hints_with_rel() {
        let hints = extract_pushdown_hints("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name");
        if let Some((labels, rels)) = hints {
            assert!(labels.contains(&"Person".to_string()));
            assert!(rels.contains(&"KNOWS".to_string()));
        }
    }

    #[test]
    fn test_infer_activity_create() {
        assert_eq!(
            infer_activity("CREATE (v:Vehicle {name: 'x'})"),
            "CreateVehicle"
        );
    }

    #[test]
    fn test_infer_activity_merge() {
        assert_eq!(
            infer_activity("MERGE (n:Person {id: $id}) SET n.name = $name"),
            "MergePerson"
        );
    }

    #[test]
    fn test_infer_activity_delete() {
        assert_eq!(infer_activity("MATCH (n:Review) DELETE n"), "DeleteReview");
    }

    #[test]
    fn test_infer_activity_update() {
        assert_eq!(
            infer_activity("MATCH (v:Vehicle {id: $id}) SET v.name = $name"),
            "UpdateVehicle"
        );
    }

    #[test]
    fn test_mutation_hints_create() {
        let hints = extract_mutation_hints("CREATE (n:Person {name: 'Alice'})");
        assert!(hints.is_some());
        let (labels, _) = hints.unwrap();
        assert!(labels.contains(&"Person".to_string()));
    }

    #[test]
    fn test_mutation_hints_merge() {
        let hints = extract_mutation_hints("MERGE (n:Person {id: $id}) SET n.name = $name");
        assert!(hints.is_some());
        let (labels, _) = hints.unwrap();
        assert!(labels.contains(&"Person".to_string()));
    }

    #[test]
    fn test_mutation_hints_match_delete() {
        let hints = extract_mutation_hints("MATCH (n:Review) DELETE n");
        assert!(hints.is_some());
        let (labels, _) = hints.unwrap();
        assert!(labels.contains(&"Review".to_string()));
    }

    #[test]
    fn test_mutation_hints_no_label() {
        let hints = extract_mutation_hints("MATCH (n) SET n.x = 1");
        assert!(hints.is_none());
    }
}
