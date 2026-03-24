use yata_cypher::Graph;
use yata_cypher::types::Value;

/// Data sensitivity level (mirrors magatama:consent/governance WIT).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DataSensitivity {
    Public = 0,
    Internal = 1,
    Confidential = 2,
    Restricted = 3,
}

impl DataSensitivity {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "internal" => Self::Internal,
            "confidential" => Self::Confidential,
            "restricted" => Self::Restricted,
            _ => Self::Public,
        }
    }
}

/// A consent grant covering specific resources (simplified for RLS evaluation).
#[derive(Debug, Clone)]
pub struct ConsentGrant {
    pub grantor_did: String,
    pub grantee_did: String,
    pub resource_ids: Vec<String>,
    pub max_sensitivity: DataSensitivity,
    pub delegatable: bool,
}

impl ConsentGrant {
    /// Check if this grant covers a resource ID (exact match or wildcard "*").
    pub fn covers(&self, resource_id: &str) -> bool {
        self.resource_ids.iter().any(|r| r == "*" || r == resource_id)
    }
}

/// Extended RLS scope: org_id + optional user_did + optional actor_did + clearance + consent.
#[derive(Debug, Clone, Default)]
pub struct RlsScope {
    pub org_id: String,
    pub user_did: Option<String>,
    pub actor_did: Option<String>,
    /// Requester's clearance level. If set, nodes with `data_sensitivity` > clearance are filtered.
    pub clearance: Option<DataSensitivity>,
    /// Active consent grants for cross-org or fine-grained access.
    pub consent_grants: Vec<ConsentGrant>,
    /// Skip org_id filtering (system scope: PDS discovery queries).
    /// When true, only clearance + consent + user/actor filters apply.
    pub skip_org_filter: bool,
}

impl Default for DataSensitivity {
    fn default() -> Self {
        Self::Public
    }
}

/// Filter graph nodes by org_id for Row-Level Security.
/// Removes nodes that have an `org_id` property different from the given org_id.
/// Nodes without `org_id` are kept (schema/system nodes).
pub fn apply_rls_filter(g: &mut yata_cypher::MemoryGraph, org_id: &str) {
    apply_rls_filter_scoped(
        g,
        &RlsScope {
            org_id: org_id.to_string(),
            ..Default::default()
        },
    );
}

/// Filter graph nodes by RlsScope (org_id + user_did + actor_did + clearance + consent).
/// Nodes without the scoped property are kept (schema/system nodes).
pub fn apply_rls_filter_scoped(g: &mut yata_cypher::MemoryGraph, scope: &RlsScope) {
    let org_val = Value::Str(scope.org_id.to_string());
    let remove_ids: std::collections::HashSet<String> = g
        .nodes()
        .iter()
        .filter(|n| {
            // 1. org_id filter (skipped for system scope / public discovery)
            if !scope.skip_org_filter {
            if let Some(v) = n.props.get("org_id") {
                if *v != org_val {
                    // Cross-org access: check consent grants
                    if let Value::Str(node_org) = v {
                        let has_consent = scope.consent_grants.iter().any(|cg| {
                            // Grantor must match the node's org
                            let grantor_matches = cg.grantor_did.contains(node_org.as_str());
                            if !grantor_matches {
                                return false;
                            }
                            // Resource must match: wildcard, node ID, or label
                            cg.covers(&n.id)
                                || cg.resource_ids.iter().any(|r| {
                                    r == "*" || n.labels.iter().any(|l| r == l)
                                })
                        });
                        if !has_consent {
                            return true; // no consent → remove
                        }
                    } else {
                        return true;
                    }
                }
            }
            } // end skip_org_filter
            // 2. user_did filter (optional)
            if let Some(ref user_did) = scope.user_did {
                let user_val = Value::Str(user_did.clone());
                if matches!(n.props.get("_user_did"), Some(v) if *v != user_val) {
                    return true;
                }
            }
            // 3. actor_did filter (optional)
            if let Some(ref actor_did) = scope.actor_did {
                let actor_val = Value::Str(actor_did.clone());
                if matches!(n.props.get("_actor_did"), Some(v) if *v != actor_val) {
                    return true;
                }
            }
            // 4. Clearance ≥ data_sensitivity filter
            if let Some(clearance) = scope.clearance {
                if let Some(Value::Str(sensitivity_str)) = n.props.get("data_sensitivity") {
                    let sensitivity = DataSensitivity::from_str(sensitivity_str);
                    if sensitivity > clearance {
                        return true; // insufficient clearance → remove
                    }
                }
            }
            false
        })
        .map(|n| n.id.clone())
        .collect();
    if remove_ids.is_empty() {
        return;
    }
    g.retain_nodes(|n| !remove_ids.contains(&n.id));
    g.retain_rels(|r| !remove_ids.contains(&r.src) && !remove_ids.contains(&r.dst));
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use yata_cypher::{MemoryGraph, NodeRef, types::Value};

    fn node(id: &str, org: Option<&str>) -> NodeRef {
        let mut props = IndexMap::new();
        if let Some(o) = org {
            props.insert("org_id".into(), Value::Str(o.into()));
        }
        props.insert("name".into(), Value::Str(id.into()));
        NodeRef {
            id: id.into(),
            labels: vec!["Test".into()],
            props,
        }
    }

    fn build_graph(nodes: Vec<NodeRef>) -> MemoryGraph {
        let mut g = MemoryGraph::new();
        for n in nodes {
            g.add_node(n);
        }
        g.build_csr();
        g
    }

    #[test]
    fn test_rls_filter_keeps_matching_org() {
        let mut g = build_graph(vec![node("a", Some("org1")), node("b", Some("org1"))]);
        apply_rls_filter(&mut g, "org1");
        assert_eq!(g.nodes().len(), 2);
    }

    #[test]
    fn test_rls_filter_removes_other_org() {
        let mut g = build_graph(vec![node("a", Some("org1")), node("b", Some("org2"))]);
        apply_rls_filter(&mut g, "org1");
        assert_eq!(g.nodes().len(), 1);
        assert_eq!(g.nodes()[0].id, "a");
    }

    #[test]
    fn test_rls_filter_keeps_nodes_without_org_id() {
        let mut g = build_graph(vec![node("a", Some("org1")), node("sys", None)]);
        apply_rls_filter(&mut g, "org1");
        assert_eq!(
            g.nodes().len(),
            2,
            "system nodes without org_id must be kept"
        );
    }

    #[test]
    fn test_rls_filter_removes_all_when_no_match() {
        let mut g = build_graph(vec![node("a", Some("org2")), node("b", Some("org3"))]);
        apply_rls_filter(&mut g, "org1");
        // Only nodes WITH org_id different from org1 are removed; nodes without org_id are kept.
        assert_eq!(g.nodes().len(), 0);
    }

    #[test]
    fn test_rls_filter_empty_graph() {
        let mut g = MemoryGraph::new();
        g.build_csr();
        apply_rls_filter(&mut g, "org1");
        assert_eq!(g.nodes().len(), 0);
    }

    #[test]
    fn test_inject_rls_adds_org_id() {
        let mut nodes = vec![node("a", None)];
        inject_rls_on_new_vertices(&mut nodes, "org1");
        assert_eq!(
            nodes[0].props.get("org_id"),
            Some(&Value::Str("org1".into()))
        );
    }

    #[test]
    fn test_inject_rls_does_not_overwrite_existing() {
        let mut nodes = vec![node("a", Some("org_existing"))];
        inject_rls_on_new_vertices(&mut nodes, "org_new");
        assert_eq!(
            nodes[0].props.get("org_id"),
            Some(&Value::Str("org_existing".into()))
        );
    }

    #[test]
    fn test_inject_rls_empty_array() {
        let mut nodes: Vec<NodeRef> = vec![];
        inject_rls_on_new_vertices(&mut nodes, "org1");
        assert!(nodes.is_empty());
    }

    fn node_scoped(
        id: &str,
        org: Option<&str>,
        user: Option<&str>,
        actor: Option<&str>,
    ) -> NodeRef {
        let mut props = IndexMap::new();
        if let Some(o) = org {
            props.insert("org_id".into(), Value::Str(o.into()));
        }
        if let Some(u) = user {
            props.insert("_user_did".into(), Value::Str(u.into()));
        }
        if let Some(a) = actor {
            props.insert("_actor_did".into(), Value::Str(a.into()));
        }
        props.insert("name".into(), Value::Str(id.into()));
        NodeRef {
            id: id.into(),
            labels: vec!["Test".into()],
            props,
        }
    }

    #[test]
    fn test_scoped_filter_user_did() {
        let mut g = build_graph(vec![
            node_scoped("a", Some("org1"), Some("did:key:alice"), None),
            node_scoped("b", Some("org1"), Some("did:key:bob"), None),
        ]);
        apply_rls_filter_scoped(
            &mut g,
            &RlsScope {
                org_id: "org1".into(),
                user_did: Some("did:key:alice".into()),
                actor_did: None,
            ..Default::default()
            },
        );
        assert_eq!(g.nodes().len(), 1);
        assert_eq!(g.nodes()[0].id, "a");
    }

    #[test]
    fn test_scoped_filter_actor_did() {
        let mut g = build_graph(vec![
            node_scoped("a", Some("org1"), None, Some("did:key:bot1")),
            node_scoped("b", Some("org1"), None, Some("did:key:bot2")),
            node_scoped("c", Some("org1"), None, None), // no actor_did → kept
        ]);
        apply_rls_filter_scoped(
            &mut g,
            &RlsScope {
                org_id: "org1".into(),
                user_did: None,
                actor_did: Some("did:key:bot1".into()),
                ..Default::default()
            },
        );
        assert_eq!(g.nodes().len(), 2); // a + c (no actor_did)
    }

    #[test]
    fn test_scoped_filter_org_only_backward_compat() {
        let mut g = build_graph(vec![node("a", Some("org1")), node("b", Some("org2"))]);
        apply_rls_filter_scoped(
            &mut g,
            &RlsScope {
                org_id: "org1".into(),
                user_did: None,
                actor_did: None,
            ..Default::default()
            },
        );
        assert_eq!(g.nodes().len(), 1);
        assert_eq!(g.nodes()[0].id, "a");
    }

    #[test]
    fn test_inject_scoped_user_and_actor() {
        let mut nodes = vec![node("a", None)];
        inject_rls_on_new_vertices_scoped(
            &mut nodes,
            &RlsScope {
                org_id: "org1".into(),
                user_did: Some("did:key:alice".into()),
                actor_did: Some("did:key:bot1".into()),
                ..Default::default()
            },
        );
        assert_eq!(
            nodes[0].props.get("org_id"),
            Some(&Value::Str("org1".into()))
        );
        assert_eq!(
            nodes[0].props.get("_user_did"),
            Some(&Value::Str("did:key:alice".into()))
        );
        assert_eq!(
            nodes[0].props.get("_actor_did"),
            Some(&Value::Str("did:key:bot1".into()))
        );
    }

    fn node_with_sensitivity(id: &str, org: &str, sensitivity: &str) -> NodeRef {
        let mut props = IndexMap::new();
        props.insert("org_id".into(), Value::Str(org.into()));
        props.insert("data_sensitivity".into(), Value::Str(sensitivity.into()));
        props.insert("name".into(), Value::Str(id.into()));
        NodeRef {
            id: id.into(),
            labels: vec!["Secret".into()],
            props,
        }
    }

    #[test]
    fn test_clearance_filter_blocks_restricted() {
        let mut g = build_graph(vec![
            node_with_sensitivity("public_doc", "org1", "public"),
            node_with_sensitivity("internal_doc", "org1", "internal"),
            node_with_sensitivity("confidential_doc", "org1", "confidential"),
            node_with_sensitivity("restricted_doc", "org1", "restricted"),
        ]);
        apply_rls_filter_scoped(
            &mut g,
            &RlsScope {
                org_id: "org1".into(),
                clearance: Some(DataSensitivity::Internal),
                ..Default::default()
            },
        );
        // Internal clearance: can see public + internal, not confidential/restricted
        assert_eq!(g.nodes().len(), 2);
        let nodes = g.nodes();
        let ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
        assert!(ids.contains(&"public_doc"));
        assert!(ids.contains(&"internal_doc"));
    }

    #[test]
    fn test_clearance_filter_allows_all_with_restricted() {
        let mut g = build_graph(vec![
            node_with_sensitivity("public_doc", "org1", "public"),
            node_with_sensitivity("restricted_doc", "org1", "restricted"),
        ]);
        apply_rls_filter_scoped(
            &mut g,
            &RlsScope {
                org_id: "org1".into(),
                clearance: Some(DataSensitivity::Restricted),
                ..Default::default()
            },
        );
        assert_eq!(g.nodes().len(), 2); // restricted clearance sees everything
    }

    #[test]
    fn test_clearance_not_applied_when_none() {
        let mut g = build_graph(vec![
            node_with_sensitivity("restricted_doc", "org1", "restricted"),
        ]);
        apply_rls_filter_scoped(
            &mut g,
            &RlsScope {
                org_id: "org1".into(),
                clearance: None, // no clearance check
                ..Default::default()
            },
        );
        assert_eq!(g.nodes().len(), 1); // no clearance = no filtering by sensitivity
    }

    #[test]
    fn test_consent_allows_cross_org_access() {
        let mut g = build_graph(vec![
            node("own", Some("org1")),
            node("other", Some("org2")),
        ]);
        apply_rls_filter_scoped(
            &mut g,
            &RlsScope {
                org_id: "org1".into(),
                consent_grants: vec![ConsentGrant {
                    grantor_did: "did:web:org2".into(),
                    grantee_did: "did:web:org1-user".into(),
                    resource_ids: vec!["*".into()],
                    max_sensitivity: DataSensitivity::Public,
                    delegatable: false,
                }],
                ..Default::default()
            },
        );
        assert_eq!(g.nodes().len(), 2); // consent grant allows cross-org access
    }

    #[test]
    fn test_consent_denied_without_grant() {
        let mut g = build_graph(vec![
            node("own", Some("org1")),
            node("other", Some("org2")),
        ]);
        apply_rls_filter_scoped(
            &mut g,
            &RlsScope {
                org_id: "org1".into(),
                consent_grants: vec![], // no grants
                ..Default::default()
            },
        );
        assert_eq!(g.nodes().len(), 1); // only own org node
        assert_eq!(g.nodes()[0].id, "own");
    }

    // ── Clearance matrix tests ────────────────────────────────────

    #[test]
    fn test_clearance_public_sees_only_public() {
        let mut g = build_graph(vec![
            node_with_sensitivity("pub", "org1", "public"),
            node_with_sensitivity("int", "org1", "internal"),
            node_with_sensitivity("conf", "org1", "confidential"),
            node_with_sensitivity("rest", "org1", "restricted"),
        ]);
        apply_rls_filter_scoped(&mut g, &RlsScope {
            org_id: "org1".into(),
            clearance: Some(DataSensitivity::Public),
            ..Default::default()
        });
        assert_eq!(g.nodes().len(), 1);
        assert_eq!(g.nodes()[0].id, "pub");
    }

    #[test]
    fn test_clearance_confidential_sees_three() {
        let mut g = build_graph(vec![
            node_with_sensitivity("pub", "org1", "public"),
            node_with_sensitivity("int", "org1", "internal"),
            node_with_sensitivity("conf", "org1", "confidential"),
            node_with_sensitivity("rest", "org1", "restricted"),
        ]);
        apply_rls_filter_scoped(&mut g, &RlsScope {
            org_id: "org1".into(),
            clearance: Some(DataSensitivity::Confidential),
            ..Default::default()
        });
        assert_eq!(g.nodes().len(), 3); // public + internal + confidential
    }

    #[test]
    fn test_clearance_ignores_nodes_without_sensitivity() {
        let mut g = build_graph(vec![
            node("normal", Some("org1")),  // no data_sensitivity
            node_with_sensitivity("rest", "org1", "restricted"),
        ]);
        apply_rls_filter_scoped(&mut g, &RlsScope {
            org_id: "org1".into(),
            clearance: Some(DataSensitivity::Public),
            ..Default::default()
        });
        // "normal" has no data_sensitivity → kept. "rest" → blocked.
        assert_eq!(g.nodes().len(), 1);
        assert_eq!(g.nodes()[0].id, "normal");
    }

    // ── Consent grant detail tests ──────────────────────────────

    #[test]
    fn test_consent_wildcard_grant_allows_all() {
        let mut g = build_graph(vec![
            node("own", Some("org1")),
            node("other1", Some("org2")),
            node("other2", Some("org2")),
        ]);
        apply_rls_filter_scoped(&mut g, &RlsScope {
            org_id: "org1".into(),
            consent_grants: vec![ConsentGrant {
                grantor_did: "did:web:org2".into(),
                grantee_did: "did:web:me".into(),
                resource_ids: vec!["*".into()],
                max_sensitivity: DataSensitivity::Restricted,
                delegatable: true,
            }],
            ..Default::default()
        });
        assert_eq!(g.nodes().len(), 3); // all accessible
    }

    #[test]
    fn test_consent_specific_resource_grant() {
        let n1 = {
            let mut props = IndexMap::new();
            props.insert("org_id".into(), Value::Str("org2".into()));
            props.insert("name".into(), Value::Str("shared_doc".into()));
            NodeRef { id: "shared_doc".into(), labels: vec!["Document".into()], props }
        };
        let n2 = {
            let mut props = IndexMap::new();
            props.insert("org_id".into(), Value::Str("org2".into()));
            props.insert("name".into(), Value::Str("private_doc".into()));
            NodeRef { id: "private_doc".into(), labels: vec!["Secret".into()], props }
        };
        let mut g = build_graph(vec![node("own", Some("org1")), n1, n2]);
        apply_rls_filter_scoped(&mut g, &RlsScope {
            org_id: "org1".into(),
            consent_grants: vec![ConsentGrant {
                grantor_did: "did:web:org2".into(),
                grantee_did: "did:web:me".into(),
                resource_ids: vec!["Document".into()], // only Document label
                max_sensitivity: DataSensitivity::Public,
                delegatable: false,
            }],
            ..Default::default()
        });
        // own + shared_doc (Document label matches), NOT private_doc (Secret label)
        assert_eq!(g.nodes().len(), 2);
    }

    #[test]
    fn test_consent_no_grant_for_org_blocks() {
        let mut g = build_graph(vec![
            node("own", Some("org1")),
            node("org2_data", Some("org2")),
            node("org3_data", Some("org3")),
        ]);
        // Grant only covers org2, not org3
        apply_rls_filter_scoped(&mut g, &RlsScope {
            org_id: "org1".into(),
            consent_grants: vec![ConsentGrant {
                grantor_did: "did:web:org2".into(),
                grantee_did: "did:web:me".into(),
                resource_ids: vec!["*".into()],
                max_sensitivity: DataSensitivity::Public,
                delegatable: false,
            }],
            ..Default::default()
        });
        // own + org2_data, NOT org3_data
        assert_eq!(g.nodes().len(), 2);
    }

    // ── Combined clearance + consent ────────────────────────────

    #[test]
    fn test_clearance_and_consent_combined() {
        let n1 = {
            let mut props = IndexMap::new();
            props.insert("org_id".into(), Value::Str("org2".into()));
            props.insert("data_sensitivity".into(), Value::Str("restricted".into()));
            NodeRef { id: "secret".into(), labels: vec!["Doc".into()], props }
        };
        let mut g = build_graph(vec![node("own", Some("org1")), n1]);
        // Has consent for org2 but only Public clearance
        apply_rls_filter_scoped(&mut g, &RlsScope {
            org_id: "org1".into(),
            clearance: Some(DataSensitivity::Public),
            consent_grants: vec![ConsentGrant {
                grantor_did: "did:web:org2".into(),
                grantee_did: "did:web:me".into(),
                resource_ids: vec!["*".into()],
                max_sensitivity: DataSensitivity::Restricted,
                delegatable: false,
            }],
            ..Default::default()
        });
        // Consent allows cross-org, but clearance blocks restricted → only own
        assert_eq!(g.nodes().len(), 1);
        assert_eq!(g.nodes()[0].id, "own");
    }

    // ── DataSensitivity ordering ────────────────────────────────

    #[test]
    fn test_data_sensitivity_ordering() {
        assert!(DataSensitivity::Public < DataSensitivity::Internal);
        assert!(DataSensitivity::Internal < DataSensitivity::Confidential);
        assert!(DataSensitivity::Confidential < DataSensitivity::Restricted);
    }

    #[test]
    fn test_data_sensitivity_from_str() {
        assert_eq!(DataSensitivity::from_str("public"), DataSensitivity::Public);
        assert_eq!(DataSensitivity::from_str("internal"), DataSensitivity::Internal);
        assert_eq!(DataSensitivity::from_str("confidential"), DataSensitivity::Confidential);
        assert_eq!(DataSensitivity::from_str("restricted"), DataSensitivity::Restricted);
        assert_eq!(DataSensitivity::from_str("INTERNAL"), DataSensitivity::Internal);
        assert_eq!(DataSensitivity::from_str("unknown"), DataSensitivity::Public);
    }

    #[test]
    fn test_consent_grant_covers() {
        let grant = ConsentGrant {
            grantor_did: "did:web:org2".into(),
            grantee_did: "did:web:me".into(),
            resource_ids: vec!["doc123".into(), "Document".into()],
            max_sensitivity: DataSensitivity::Public,
            delegatable: false,
        };
        assert!(grant.covers("doc123"));
        assert!(grant.covers("Document"));
        assert!(!grant.covers("other"));

        let wildcard = ConsentGrant {
            resource_ids: vec!["*".into()],
            ..grant.clone()
        };
        assert!(wildcard.covers("anything"));
    }

    #[test]
    fn test_inject_scoped_does_not_overwrite() {
        let mut nodes = vec![node_scoped(
            "a",
            Some("org_old"),
            Some("did:old"),
            Some("did:old_bot"),
        )];
        inject_rls_on_new_vertices_scoped(
            &mut nodes,
            &RlsScope {
                org_id: "org_new".into(),
                user_did: Some("did:new".into()),
                actor_did: Some("did:new_bot".into()),
                ..Default::default()
            },
        );
        assert_eq!(
            nodes[0].props.get("org_id"),
            Some(&Value::Str("org_old".into()))
        );
        assert_eq!(
            nodes[0].props.get("_user_did"),
            Some(&Value::Str("did:old".into()))
        );
        assert_eq!(
            nodes[0].props.get("_actor_did"),
            Some(&Value::Str("did:old_bot".into()))
        );
    }
}

/// Inject org_id into newly created vertices that don't have it.
pub fn inject_rls_on_new_vertices(vertices: &mut [yata_cypher::NodeRef], org_id: &str) {
    inject_rls_on_new_vertices_scoped(
        vertices,
        &RlsScope {
            org_id: org_id.to_string(),
            ..Default::default()
        },
    );
}

/// Inject RLS scope properties (org_id, user_did, actor_did) into new vertices.
pub fn inject_rls_on_new_vertices_scoped(vertices: &mut [yata_cypher::NodeRef], scope: &RlsScope) {
    for node in vertices.iter_mut() {
        if !node.props.contains_key("org_id") {
            node.props
                .insert("org_id".into(), Value::Str(scope.org_id.to_string()));
        }
        if let Some(ref user_did) = scope.user_did {
            if !node.props.contains_key("_user_did") {
                node.props
                    .insert("_user_did".into(), Value::Str(user_did.clone()));
            }
        }
        if let Some(ref actor_did) = scope.actor_did {
            if !node.props.contains_key("_actor_did") {
                node.props
                    .insert("_actor_did".into(), Value::Str(actor_did.clone()));
            }
        }
    }
}
