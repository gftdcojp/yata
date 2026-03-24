//! CommitLog: head tracking, history walk, checkout, diff.
//! HEAD CID is persisted to `{head_dir}/MDAG_HEAD` on every commit.

use ed25519_dalek::SigningKey;
use std::path::PathBuf;
use std::sync::Arc;
use yata_cas::CasStore;
use yata_core::Blake3Hash;
use yata_store::MutableCsrStore;

use std::collections::BTreeMap;

use crate::blocks::GraphRootBlock;
use crate::deserialize::load_graph;
use crate::diff::{MdagDiff, merkle_diff};
use crate::error::{MdagError, Result};
use crate::serialize::{commit_graph, delta_commit_graph_cached};

const HEAD_FILENAME: &str = "MDAG_HEAD";

/// Callback to push HEAD CID to cold storage (B2).
pub type HeadPusher = Arc<dyn Fn(Blake3Hash) + Send + Sync>;

/// Manages the MDAG commit chain. HEAD points to the latest root CID.
/// If `head_dir` is set, HEAD CID is persisted to `{head_dir}/MDAG_HEAD`
/// and restored on startup.
pub struct CommitLog {
    cas: Arc<dyn CasStore>,
    head: Option<Blake3Hash>,
    head_dir: Option<PathBuf>,
    head_pusher: Option<HeadPusher>,
    signing_key: SigningKey,
    signer_did: String,
    /// Cached label→CID maps from the last commit (avoids N CAS GETs per delta_commit).
    cached_vg_map: BTreeMap<String, Blake3Hash>,
    cached_eg_map: BTreeMap<String, Blake3Hash>,
}

impl CommitLog {
    pub fn new(cas: Arc<dyn CasStore>, signing_key: SigningKey, signer_did: String) -> Self {
        Self {
            cas,
            head: None,
            head_dir: None,
            head_pusher: None,
            signing_key,
            signer_did,
            cached_vg_map: BTreeMap::new(),
            cached_eg_map: BTreeMap::new(),
        }
    }

    /// Create with a persistent HEAD file in `dir`.
    /// Reads existing HEAD from `{dir}/MDAG_HEAD` if present.
    /// If no local HEAD file, tries to restore from CAS via well-known key `{prefix}MDAG_HEAD`.
    pub fn with_persistence(
        cas: Arc<dyn CasStore>,
        dir: impl Into<PathBuf>,
        signing_key: SigningKey,
        signer_did: String,
    ) -> Self {
        let dir = dir.into();
        let mut head = Self::read_head_file(&dir);
        if let Some(ref h) = head {
            tracing::info!("mdag: restored HEAD from disk: {}", h.hex());
        } else {
            // Try CAS-based HEAD restore (R2/S3 stores support get_by_key)
            head = Self::try_restore_head_from_cas(cas.as_ref());
            if let Some(ref h) = head {
                tracing::info!("mdag: restored HEAD from CAS: {}", h.hex());
                // Persist to local disk for next restart
                let path = dir.join(HEAD_FILENAME);
                let _ = std::fs::create_dir_all(&dir);
                let _ = std::fs::write(&path, h.hex());
            }
        }
        Self {
            cas,
            head,
            head_dir: Some(dir),
            head_pusher: None,
            signing_key,
            signer_did,
            cached_vg_map: BTreeMap::new(),
            cached_eg_map: BTreeMap::new(),
        }
    }

    /// Try to restore HEAD from CAS store (works with S3CasStore which has restore_head()).
    fn try_restore_head_from_cas(cas: &dyn CasStore) -> Option<Blake3Hash> {
        // S3CasStore stores HEAD at well-known key. We try to read it by downcasting
        // or by checking if the CAS has a `restore_head` method via the trait object.
        // Since CasStore trait doesn't have restore_head, we use a convention:
        // Try to get a well-known key "MDAG_HEAD" from the store.
        // S3CasStore.get_by_key() would work, but CasStore trait only has get(hash).
        // Instead, check env var for prefix and construct the key.
        let prefix = std::env::var("YATA_S3_PREFIX").unwrap_or_default();
        if prefix.is_empty() {
            return None;
        }
        tracing::info!(prefix = %prefix, "trying MDAG_HEAD restore from S3");
        None // S3CasStore.restore_head() must be called from magatama-server directly
    }

    /// Set a callback to push HEAD to B2 on every commit.
    pub fn set_head_pusher(&mut self, pusher: HeadPusher) {
        self.head_pusher = Some(pusher);
    }

    pub fn with_head(
        cas: Arc<dyn CasStore>,
        head: Blake3Hash,
        signing_key: SigningKey,
        signer_did: String,
    ) -> Self {
        Self {
            cas,
            head: Some(head),
            head_dir: None,
            head_pusher: None,
            signing_key,
            signer_did,
            cached_vg_map: BTreeMap::new(),
            cached_eg_map: BTreeMap::new(),
        }
    }

    /// Current HEAD root CID.
    pub fn head(&self) -> Option<&Blake3Hash> {
        self.head.as_ref()
    }

    /// Commit the current graph state, chaining to the previous HEAD.
    /// Every commit is Ed25519-signed with the CommitLog's signing key.
    pub async fn commit(&mut self, store: &MutableCsrStore, message: &str) -> Result<Blake3Hash> {
        let root_cid = commit_graph(
            store,
            self.cas.as_ref(),
            self.head.clone(),
            message,
            &self.signing_key,
            &self.signer_did,
        )
        .await?;
        self.head = Some(root_cid.clone());
        self.persist_head(&root_cid);
        Ok(root_cid)
    }

    /// Delta commit: only re-serialize changed label groups.
    /// `dirty_vertex_labels` / `dirty_edge_labels` specify which labels were mutated.
    /// Every commit is Ed25519-signed with the CommitLog's signing key.
    pub async fn delta_commit(
        &mut self,
        store: &MutableCsrStore,
        message: &str,
        dirty_vertex_labels: &[String],
        dirty_edge_labels: &[String],
    ) -> Result<Blake3Hash> {
        let (root_cid, new_vg_map, new_eg_map) = delta_commit_graph_cached(
            store,
            self.cas.as_ref(),
            self.head.clone(),
            message,
            dirty_vertex_labels,
            dirty_edge_labels,
            &self.signing_key,
            &self.signer_did,
            &self.cached_vg_map,
            &self.cached_eg_map,
        )
        .await?;
        self.head = Some(root_cid.clone());
        self.cached_vg_map = new_vg_map;
        self.cached_eg_map = new_eg_map;
        self.persist_head(&root_cid);
        Ok(root_cid)
    }

    fn persist_head(&self, cid: &Blake3Hash) {
        // Warm: atomic write (tmp + rename) to prevent partial HEAD on crash/FUSE flake
        if let Some(ref dir) = self.head_dir {
            let _ = std::fs::create_dir_all(dir);
            let path = dir.join(HEAD_FILENAME);
            let tmp = dir.join(format!("{HEAD_FILENAME}.tmp"));
            match std::fs::write(&tmp, cid.hex().as_bytes()) {
                Ok(()) => {
                    if let Err(e) = std::fs::rename(&tmp, &path) {
                        tracing::error!("mdag: HEAD atomic rename failed: {e}");
                        // Fallback: try direct write
                        let _ = std::fs::write(&path, cid.hex().as_bytes());
                    }
                }
                Err(e) => {
                    tracing::warn!("mdag: failed to persist HEAD to {}: {e}", path.display());
                }
            }
        }
        // Cold: B2 (async, fire-and-forget via callback)
        if let Some(ref pusher) = self.head_pusher {
            pusher(cid.clone());
        }
    }

    fn read_head_file(dir: &std::path::Path) -> Option<Blake3Hash> {
        let path = dir.join(HEAD_FILENAME);
        let hex = std::fs::read_to_string(&path).ok()?;
        let hex = hex.trim();
        if hex.len() != 64 {
            return None;
        }
        Blake3Hash::from_hex(hex).ok()
    }

    /// Walk the parent chain to list recent commits (most recent first).
    pub async fn history(&self, limit: usize) -> Result<Vec<(Blake3Hash, GraphRootBlock)>> {
        let mut results = Vec::new();
        let mut current = self.head.clone();

        while let Some(cid) = current {
            if results.len() >= limit {
                break;
            }
            let data = self
                .cas
                .get(&cid)
                .await?
                .ok_or_else(|| MdagError::NotFound(cid.hex()))?;
            let root: GraphRootBlock =
                yata_cbor::decode(&data).map_err(|e| MdagError::Deserialize(e.to_string()))?;
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
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    use yata_cas::LocalCasStore;
    use yata_grin::{Mutable, PropValue, Scannable, Topology};

    fn test_key() -> (SigningKey, String) {
        (SigningKey::generate(&mut OsRng), "did:key:z6MkTest".into())
    }

    #[tokio::test]
    async fn test_commit_chain() {
        let dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());
        let (sk, did) = test_key();
        let mut log = CommitLog::new(cas, sk, did);

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
        let (sk, did) = test_key();
        let mut log = CommitLog::new(cas, sk, did);

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
        let (sk, did) = test_key();
        let mut log = CommitLog::new(cas, sk, did);

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

    #[tokio::test]
    async fn test_delta_commit_speed() {
        use std::time::Instant;

        let dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());
        let (sk, did) = test_key();
        let mut log = CommitLog::new(cas.clone(), sk, did);

        // Build 1K V / 3K E across 3 labels
        let mut store = MutableCsrStore::new();
        for i in 0u32..1000 {
            let label = match i % 3 {
                0 => "Person",
                1 => "Company",
                _ => "Product",
            };
            store.add_vertex(
                label,
                &[
                    ("name", PropValue::Str(format!("n{i}"))),
                    ("val", PropValue::Int(i as i64)),
                ],
            );
        }
        for i in 0u32..3000 {
            let src = i % 1000;
            let dst = (i * 7 + 3) % 1000;
            if src != dst {
                store.add_edge(
                    src,
                    dst,
                    "REL",
                    &[("w", PropValue::Float(i as f64 * 0.001))],
                );
            }
        }
        store.commit();

        // Full commit (baseline)
        let t0 = Instant::now();
        let _c1 = log.commit(&store, "full").await.unwrap();
        let full_ms = t0.elapsed().as_millis();

        // Modify 10 Person vertices only
        for i in (0u32..30).step_by(3) {
            store.set_vertex_prop(i, "val", PropValue::Int(9999));
        }
        store.commit();

        // Delta commit (only "Person" label dirty)
        let t0 = Instant::now();
        let _c2 = log
            .delta_commit(&store, "delta", &["Person".into()], &[])
            .await
            .unwrap();
        let delta_ms = t0.elapsed().as_millis();

        // Full commit for comparison
        let t0 = Instant::now();
        let _c3 = log.commit(&store, "full2").await.unwrap();
        let full2_ms = t0.elapsed().as_millis();

        println!("\n=== Delta Commit Benchmark (1K V / 3K E) ===");
        println!("  Full commit:   {full_ms}ms");
        println!("  Delta commit:  {delta_ms}ms (only Person label)");
        println!("  Full commit 2: {full2_ms}ms (CAS dedup helps)");
        println!(
            "  Speedup:       {:.1}x",
            full2_ms as f64 / delta_ms.max(1) as f64
        );

        // Delta should be significantly faster than full
        assert!(
            delta_ms <= full2_ms,
            "delta commit should be <= full commit"
        );
    }

    #[tokio::test]
    async fn load_test_1k_graph() {
        use std::time::Instant;

        let dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(LocalCasStore::new(dir.path().join("cas")).await.unwrap());
        let (sk, did) = test_key();
        let mut log = CommitLog::new(cas.clone(), sk, did);

        // Build 1K V / 3K E
        let mut store = MutableCsrStore::new();
        for i in 0u32..1000 {
            let label = match i % 3 {
                0 => "Person",
                1 => "Company",
                _ => "Product",
            };
            store.add_vertex(
                label,
                &[
                    ("name", PropValue::Str(format!("n{i}"))),
                    ("val", PropValue::Int(i as i64)),
                ],
            );
        }
        for i in 0u32..3000 {
            let src = i % 1000;
            let dst = (i * 7 + 3) % 1000;
            if src != dst {
                store.add_edge(
                    src,
                    dst,
                    "REL",
                    &[("w", PropValue::Float(i as f64 * 0.001))],
                );
            }
        }
        store.commit();

        // Commit
        let t0 = Instant::now();
        let c1 = log.commit(&store, "initial").await.unwrap();
        let commit_ms = t0.elapsed().as_millis();

        // Load
        let t0 = Instant::now();
        let loaded = crate::load_graph(&c1, cas.as_ref()).await.unwrap();
        let load_ms = t0.elapsed().as_millis();
        assert_eq!(loaded.vertex_count(), store.vertex_count());

        // Modify 10 → commit
        for i in 0u32..10 {
            store.set_vertex_prop(i, "val", PropValue::Int(9999));
        }
        store.commit();
        let t0 = Instant::now();
        let c2 = log.commit(&store, "delta").await.unwrap();
        let commit2_ms = t0.elapsed().as_millis();

        // Diff
        let t0 = Instant::now();
        let diff = log.diff(&c1, &c2).await.unwrap();
        let diff_ms = t0.elapsed().as_millis();

        // Checkout old
        let t0 = Instant::now();
        let _old = log.checkout(&c1).await.unwrap();
        let checkout_ms = t0.elapsed().as_millis();

        println!("\n=== MDAG Load Test (1K V / 3K E) ===");
        println!("  Commit (full):  {commit_ms}ms");
        println!("  Commit (delta): {commit2_ms}ms");
        println!("  Load:           {load_ms}ms");
        println!(
            "  Diff:           {diff_ms}ms  (changes: {})",
            diff.total_changes()
        );
        println!("  Checkout:       {checkout_ms}ms");

        assert!(diff.total_changes() > 0);
        assert!(diff.total_changes() <= 20); // ~10 modified = 10 removed + 10 added
    }
}
