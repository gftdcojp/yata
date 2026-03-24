//! RepoRegistry: manages multiple MDAG commit chains within a single CAS.
//!
//! Each RepoScope (App/Org/User/Actor/Channel) has an independent HEAD,
//! enabling per-entity graph partitioning with shared CAS storage.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use yata_cas::CasStore;
use yata_core::Blake3Hash;

use crate::blocks::GraphRootBlock;
use crate::error::Result;
use crate::verify::sign_root;

/// Scope of a commit chain (repo).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum RepoScope {
    /// Per-app graph (existing per-app CommitLog).
    App(String),
    /// Per-organization graph partition.
    Org(String),
    /// Per-user graph partition (user DID).
    User(String),
    /// Per-actor (LLM agent) graph partition (actor DID).
    Actor(String),
    /// Per-channel W Protocol commit chain.
    Channel(String),
}

impl RepoScope {
    /// Human-readable scope identifier for logging/display.
    pub fn scope_id(&self) -> String {
        match self {
            Self::App(id) => format!("app:{id}"),
            Self::Org(id) => format!("org:{id}"),
            Self::User(id) => format!("user:{id}"),
            Self::Actor(id) => format!("actor:{id}"),
            Self::Channel(id) => format!("channel:{id}"),
        }
    }
}

const REGISTRY_FILENAME: &str = "REPO_REGISTRY";

/// Manages multiple MDAG commit chain HEADs within a single CAS.
pub struct RepoRegistry {
    heads: HashMap<RepoScope, Blake3Hash>,
    cas: Arc<dyn CasStore>,
    persist_path: Option<PathBuf>,
}

/// Serialized registry state (CBOR).
#[derive(Serialize, Deserialize)]
struct RegistryState {
    heads: Vec<(RepoScope, String)>, // (scope, hex CID)
}

impl RepoRegistry {
    pub fn new(cas: Arc<dyn CasStore>) -> Self {
        Self {
            heads: HashMap::new(),
            cas,
            persist_path: None,
        }
    }

    /// Create with persistence directory. Restores HEAD map from `REPO_REGISTRY`.
    pub fn with_persistence(cas: Arc<dyn CasStore>, dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        let path = dir.join(REGISTRY_FILENAME);
        let heads = Self::read_registry_file(&path);
        if !heads.is_empty() {
            tracing::info!("repo-registry: restored {} repo heads", heads.len());
        }
        Self {
            heads,
            cas,
            persist_path: Some(dir),
        }
    }

    /// Get HEAD for a scope.
    pub fn head(&self, scope: &RepoScope) -> Option<&Blake3Hash> {
        self.heads.get(scope)
    }

    /// List all scopes with their HEADs.
    pub fn list(&self) -> impl Iterator<Item = (&RepoScope, &Blake3Hash)> {
        self.heads.iter()
    }

    /// Update HEAD for a scope. Persists registry to disk.
    pub fn set_head(&mut self, scope: RepoScope, head: Blake3Hash) {
        self.heads.insert(scope, head);
        self.persist();
    }

    /// Commit a signed root block for a scope. Updates HEAD and persists.
    pub async fn commit_signed(
        &mut self,
        scope: RepoScope,
        mut root: GraphRootBlock,
        signing_key: &SigningKey,
        signer_did: &str,
    ) -> Result<Blake3Hash> {
        // Chain to previous HEAD for this scope
        root.parent = self.heads.get(&scope).cloned();

        // Sign the root block
        sign_root(&mut root, signing_key, signer_did);

        // Store in CAS
        let cbor = yata_cbor::encode(&root)?;
        let cid = self.cas.put(bytes::Bytes::from(cbor)).await?;

        // Update HEAD
        self.heads.insert(scope, cid.clone());
        self.persist();

        Ok(cid)
    }

    /// Migrate existing MDAG_HEAD to RepoScope::App.
    /// For backward compatibility with single-app CommitLog.
    pub fn import_app_head(&mut self, app_id: &str, head: Blake3Hash) {
        let scope = RepoScope::App(app_id.to_string());
        if !self.heads.contains_key(&scope) {
            self.heads.insert(scope, head);
            self.persist();
        }
    }

    fn persist(&self) {
        let Some(ref dir) = self.persist_path else {
            return;
        };
        let state = RegistryState {
            heads: self
                .heads
                .iter()
                .map(|(s, h)| (s.clone(), h.hex()))
                .collect(),
        };
        let cbor = match yata_cbor::encode(&state) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("repo-registry: CBOR encode failed: {e}");
                return;
            }
        };
        let path = dir.join(REGISTRY_FILENAME);
        let tmp = dir.join(format!("{REGISTRY_FILENAME}.tmp"));
        match std::fs::write(&tmp, &cbor) {
            Ok(()) => {
                if let Err(e) = std::fs::rename(&tmp, &path) {
                    tracing::error!("repo-registry: atomic rename failed: {e}");
                    let _ = std::fs::write(&path, &cbor);
                }
            }
            Err(e) => {
                tracing::warn!("repo-registry: persist failed: {e}");
            }
        }
    }

    fn read_registry_file(path: &std::path::Path) -> HashMap<RepoScope, Blake3Hash> {
        let data = match std::fs::read(path) {
            Ok(d) => d,
            Err(_) => return HashMap::new(),
        };
        let state: RegistryState = match yata_cbor::decode(&data) {
            Ok(s) => s,
            Err(_) => return HashMap::new(),
        };
        let mut heads = HashMap::new();
        for (scope, hex) in state.heads {
            if let Ok(hash) = Blake3Hash::from_hex(&hex) {
                heads.insert(scope, hash);
            }
        }
        heads
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_cas::LocalCasStore;

    #[tokio::test]
    async fn test_set_and_get_head() {
        let dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());
        let mut reg = RepoRegistry::new(cas);

        let scope = RepoScope::App("app1".into());
        let head = Blake3Hash::of(b"root1");
        reg.set_head(scope.clone(), head.clone());

        assert_eq!(reg.head(&scope), Some(&head));
        assert_eq!(reg.head(&RepoScope::User("u1".into())), None);
    }

    #[tokio::test]
    async fn test_multiple_scopes() {
        let dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());
        let mut reg = RepoRegistry::new(cas);

        let app = RepoScope::App("app1".into());
        let user = RepoScope::User("did:key:z6MkAlice".into());
        let actor = RepoScope::Actor("did:key:z6MkBot".into());

        reg.set_head(app.clone(), Blake3Hash::of(b"a"));
        reg.set_head(user.clone(), Blake3Hash::of(b"u"));
        reg.set_head(actor.clone(), Blake3Hash::of(b"r"));

        assert_eq!(reg.list().count(), 3);
    }

    #[tokio::test]
    async fn test_persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());

        let scope = RepoScope::Org("org_42".into());
        let head = Blake3Hash::of(b"head");

        {
            let mut reg = RepoRegistry::with_persistence(cas.clone(), dir.path());
            reg.set_head(scope.clone(), head.clone());
        }

        // Reopen — should restore
        let reg2 = RepoRegistry::with_persistence(cas, dir.path());
        assert_eq!(reg2.head(&scope), Some(&head));
    }

    #[tokio::test]
    async fn test_commit_signed() {
        use rand::rngs::OsRng;

        let dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());
        let mut reg = RepoRegistry::new(cas.clone());

        let sk = SigningKey::generate(&mut OsRng);
        let scope = RepoScope::User("did:key:z6MkAlice".into());

        let root = GraphRootBlock {
            version: 1,
            partition_id: yata_core::PartitionId::from(0),
            parent: None,
            schema_cid: Blake3Hash::of(b"s"),
            vertex_groups: vec![],
            edge_groups: vec![],
            vertex_count: 0,
            edge_count: 0,
            timestamp_ns: 100,
            message: "first".into(),
            index_cids: vec![],
            vex_cid: None,
            vineyard_ids: None,
            signer_did: String::new(),
            signature: Vec::new(),
        };

        let cid1 = reg
            .commit_signed(scope.clone(), root.clone(), &sk, "did:key:z6MkAlice")
            .await
            .unwrap();
        assert_eq!(reg.head(&scope), Some(&cid1));

        // Verify stored block is signed
        let data = cas.get(&cid1).await.unwrap().unwrap();
        let stored: GraphRootBlock = yata_cbor::decode(&data).unwrap();
        assert_eq!(stored.signature.len(), 64);
        assert_eq!(stored.signer_did, "did:key:z6MkAlice");
        assert!(stored.parent.is_none()); // first commit

        // Second commit chains to first
        let mut root2 = root;
        root2.message = "second".into();
        let cid2 = reg
            .commit_signed(scope.clone(), root2, &sk, "did:key:z6MkAlice")
            .await
            .unwrap();
        let data2 = cas.get(&cid2).await.unwrap().unwrap();
        let stored2: GraphRootBlock = yata_cbor::decode(&data2).unwrap();
        assert_eq!(stored2.parent, Some(cid1));
    }

    #[tokio::test]
    async fn test_import_app_head() {
        let dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());
        let mut reg = RepoRegistry::new(cas);

        let head = Blake3Hash::of(b"legacy_head");
        reg.import_app_head("myapp", head.clone());
        assert_eq!(reg.head(&RepoScope::App("myapp".into())), Some(&head));

        // Second import should not overwrite
        let head2 = Blake3Hash::of(b"new_head");
        reg.import_app_head("myapp", head2);
        assert_eq!(reg.head(&RepoScope::App("myapp".into())), Some(&head));
    }

    #[test]
    fn test_scope_id_display() {
        assert_eq!(RepoScope::App("a1".into()).scope_id(), "app:a1");
        assert_eq!(RepoScope::Org("org1".into()).scope_id(), "org:org1");
        assert_eq!(
            RepoScope::User("did:key:z6Mk".into()).scope_id(),
            "user:did:key:z6Mk"
        );
        assert_eq!(
            RepoScope::Actor("did:key:z6Mk".into()).scope_id(),
            "actor:did:key:z6Mk"
        );
        assert_eq!(RepoScope::Channel("ch1".into()).scope_id(), "channel:ch1");
    }
}
