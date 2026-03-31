//! Query hints extracted from Cypher AST for partition routing.

pub struct QueryHints {
    pub node_labels: Vec<String>,
    pub rel_types: Vec<String>,
    pub is_read_only: bool,
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

        Self { node_labels, rel_types, is_read_only }
    }

    pub fn has_filters(&self) -> bool {
        !self.node_labels.is_empty() || !self.rel_types.is_empty()
    }
}
