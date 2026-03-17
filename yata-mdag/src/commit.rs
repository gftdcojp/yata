//! CommitLog: head tracking, history walk, checkout, diff.

use std::sync::Arc;
use yata_cas::CasStore;
use yata_core::Blake3Hash;
use yata_store::MutableCsrStore;

use crate::blocks::GraphRootBlock;
use crate::deserialize::load_graph;
use crate::diff::{merkle_diff, MdagDiff};
use crate::error::{MdagError, Result};
use crate::serialize::commit_graph;

/// Manages the MDAG commit chain. HEAD points to the latest root CID.
pub struct CommitLog {
    cas: Arc<dyn CasStore>,
    head: Option<Blake3Hash>,
}

impl CommitLog {
    pub fn new(cas: Arc<dyn CasStore>) -> Self {
        Self { cas, head: None }
    }

    pub fn with_head(cas: Arc<dyn CasStore>, head: Blake3Hash) -> Self {
        Self { cas, head: Some(head) }
    }

    /// Current HEAD root CID.
    pub fn head(&self) -> Option<&Blake3Hash> {
        self.head.as_ref()
    }

    /// Commit the current graph state, chaining to the previous HEAD.
    pub async fn commit(
        &mut self,
        store: &MutableCsrStore,
        message: &str,
    ) -> Result<Blake3Hash> {
        let root_cid = commit_graph(store, self.cas.as_ref(), self.head.clone(), message).await?;
        self.head = Some(root_cid.clone());
        Ok(root_cid)
    }

    /// Walk the parent chain to list recent commits (most recent first).
    pub async fn history(&self, limit: usize) -> Result<Vec<(Blake3Hash, GraphRootBlock)>> {
        let mut results = Vec::new();
        let mut current = self.head.clone();

        while let Some(cid) = current {
            if results.len() >= limit {
                break;
            }
            let data = self.cas.get(&cid).await?
                .ok_or_else(|| MdagError::NotFound(cid.hex()))?;
            let root: GraphRootBlock = yata_cbor::decode(&data)
                .map_err(|e| MdagError::Deserialize(e.to_string()))?;
            current = root.parent.clone();
            results.push((cid, root));
        }
        Ok(results)
    }

    /// Load graph state at a specific commit.
    pub async fn checkout(&self, root_cid: &Blake3Hash) -> Result<MutableCsrStore> {
        load_graph(root_cid, self.cas.as_ref()).await
    }

    /// Compute diff between two commits.
    pub async fn diff(&self, old: &Blake3Hash, new: &Blake3Hash) -> Result<MdagDiff> {
        merkle_diff(old, new, self.cas.as_ref()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_cas::LocalCasStore;
    use yata_grin::{Mutable, PropValue, Topology, Scannable};

    #[tokio::test]
    async fn test_commit_chain() {
        let dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());
        let mut log = CommitLog::new(cas);

        let mut store = MutableCsrStore::new();
        store.add_vertex("A", &[("v", PropValue::Int(1))]);
        store.commit();
        let c1 = log.commit(&store, "first").await.unwrap();

        store.add_vertex("A", &[("v", PropValue::Int(2))]);
        store.commit();
        let c2 = log.commit(&store, "second").await.unwrap();

        store.add_vertex("A", &[("v", PropValue::Int(3))]);
        store.commit();
        let c3 = log.commit(&store, "third").await.unwrap();

        assert_eq!(log.head(), Some(&c3));

        let history = log.history(10).await.unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].0, c3);
        assert_eq!(history[1].0, c2);
        assert_eq!(history[2].0, c1);
        assert_eq!(history[0].1.message, "third");
        assert!(history[2].1.parent.is_none()); // first commit has no parent
    }

    #[tokio::test]
    async fn test_checkout_old_commit() {
        let dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());
        let mut log = CommitLog::new(cas);

        let mut store = MutableCsrStore::new();
        store.add_vertex("Person", &[("name", PropValue::Str("Alice".into()))]);
        store.commit();
        let c1 = log.commit(&store, "v1").await.unwrap();

        store.add_vertex("Person", &[("name", PropValue::Str("Bob".into()))]);
        store.commit();
        let _c2 = log.commit(&store, "v2").await.unwrap();

        // Checkout v1 — should only have Alice
        let old = log.checkout(&c1).await.unwrap();
        assert_eq!(old.vertex_count(), 1);
        let vids = old.scan_all_vertices();
        assert_eq!(vids.len(), 1);
    }

    #[tokio::test]
    async fn test_commit_diff() {
        let dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());
        let mut log = CommitLog::new(cas);

        let mut store = MutableCsrStore::new();
        store.add_vertex("X", &[("v", PropValue::Int(1))]);
        store.commit();
        let c1 = log.commit(&store, "v1").await.unwrap();

        store.add_vertex("Y", &[("v", PropValue::Int(2))]);
        store.commit();
        let c2 = log.commit(&store, "v2").await.unwrap();

        let diff = log.diff(&c1, &c2).await.unwrap();
        assert!(!diff.is_empty());
        assert!(diff.added_labels.contains(&"Y".to_string()));
    }
}
