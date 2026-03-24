#![allow(dead_code)]

//! yata-git — Git repository persistence via yata broker.
//!
//! Stores git object database as yata CAS objects with B2 tiered backup.
//! Each evolution repo is a logical namespace under `data_dir/git/{app_nanoid}/`.
//!
//! Architecture:
//!   - Git objects (blobs, trees, commits) are stored in the local filesystem
//!     under `data_dir/git/{app_nanoid}/` as a bare-like repo.
//!   - A snapshot of refs (HEAD, branches) is persisted to yata KV
//!     under bucket `_git.{app_nanoid}` for Raft replication.
//!   - Packfiles are synced to B2 via `sync_dir()` in the broker's
//!     background sync loop, providing disaster recovery.
//!   - On restore (new pod / PVC loss), refs are replayed from KV
//!     and packfiles are fetched from B2.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// Metadata for a single git ref snapshot (persisted to yata KV).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RefSnapshot {
    /// Map of ref name → commit SHA hex (e.g. "HEAD" → "abc123...", "refs/heads/main" → "abc123...")
    pub refs: HashMap<String, String>,
    /// Timestamp of this snapshot (UTC nanos).
    pub ts_ns: i64,
    /// Total commit count at snapshot time.
    pub commit_count: u64,
}

/// Configuration for a git-backed evolution store.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GitStoreConfig {
    /// Base directory for all git repos: `{base_dir}/{app_nanoid}/`.
    pub base_dir: PathBuf,
}

impl Default for GitStoreConfig {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("/data/git"),
        }
    }
}

/// Git repository store — manages evolution repos with yata-backed persistence.
///
/// Lifecycle:
///   1. `open_or_init(app_nanoid)` — creates/opens local repo
///   2. Normal git operations (commit, rollback, etc.) via `EvolutionRepo`
///   3. `snapshot_refs(app_nanoid)` → `RefSnapshot` (caller persists to yata KV)
///   4. Broker B2 sync loop syncs `data_dir/git/` directory to B2
///   5. On recovery: `restore_refs(app_nanoid, snapshot)` replays ref state
pub struct GitStore {
    config: GitStoreConfig,
}

impl GitStore {
    pub fn new(config: GitStoreConfig) -> Self {
        Self { config }
    }

    /// Returns the filesystem path for an app's evolution repo.
    pub fn repo_path(&self, app_nanoid: &str) -> PathBuf {
        self.config.base_dir.join(app_nanoid)
    }

    /// Returns the base directory (for B2 sync registration).
    pub fn base_dir(&self) -> &Path {
        &self.config.base_dir
    }

    /// Open or initialize an evolution repo for the given app.
    pub fn open_or_init(&self, app_nanoid: &str) -> anyhow::Result<EvolutionRepo> {
        let path = self.repo_path(app_nanoid);
        EvolutionRepo::open_or_init(&path)
    }

    /// Snapshot the current ref state of a repo for KV persistence.
    pub fn snapshot_refs(&self, app_nanoid: &str) -> anyhow::Result<RefSnapshot> {
        let path = self.repo_path(app_nanoid);
        let repo = git2_open(&path)?;

        let mut refs = HashMap::new();

        // HEAD
        if let Ok(head) = repo.head() {
            if let Some(target) = head.target() {
                refs.insert("HEAD".to_string(), target.to_string());
            }
            if let Some(name) = head.name() {
                if name != "HEAD" {
                    refs.insert("HEAD_REF".to_string(), name.to_string());
                }
            }
        }

        // All refs
        if let Ok(ref_iter) = repo.references() {
            for reference in ref_iter.flatten() {
                if let (Some(name), Some(target)) = (reference.name(), reference.target()) {
                    refs.insert(name.to_string(), target.to_string());
                }
            }
        }

        let commit_count = count_commits(&repo);

        Ok(RefSnapshot {
            refs,
            ts_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            commit_count,
        })
    }

    /// List all app nanoids that have local repos.
    pub fn list_repos(&self) -> anyhow::Result<Vec<String>> {
        let mut repos = Vec::new();
        if !self.config.base_dir.exists() {
            return Ok(repos);
        }
        for entry in std::fs::read_dir(&self.config.base_dir)? {
            let entry = entry?;
            if entry.path().is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    if !name.starts_with('.') {
                        repos.push(name.to_string());
                    }
                }
            }
        }
        Ok(repos)
    }
}

/// KV bucket name for git ref snapshots.
pub fn kv_bucket(app_nanoid: &str) -> String {
    format!("_git.{}", app_nanoid)
}

/// KV key for the latest ref snapshot.
pub const KV_REFS_KEY: &str = "refs";

/// Serialize a RefSnapshot to bytes for yata KV storage.
pub fn serialize_snapshot(snapshot: &RefSnapshot) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    ciborium::into_writer(snapshot, &mut buf)
        .map_err(|e| anyhow::anyhow!("cbor serialize: {}", e))?;
    Ok(buf)
}

/// Deserialize a RefSnapshot from yata KV bytes.
pub fn deserialize_snapshot(data: &[u8]) -> anyhow::Result<RefSnapshot> {
    ciborium::from_reader(std::io::Cursor::new(data))
        .map_err(|e| anyhow::anyhow!("cbor deserialize: {}", e))
}

// ── EvolutionRepo ────────────────────────────────────────────────────────────

/// A git repository for a single MagatamaApp's evolution history.
pub struct EvolutionRepo {
    repo: git2::Repository,
    path: PathBuf,
}

impl EvolutionRepo {
    /// Open existing or initialize a new evolution repo.
    pub fn open_or_init(path: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(path)?;

        let repo = match git2::Repository::open(path) {
            Ok(r) => r,
            Err(_) => {
                info!(path = %path.display(), "initializing new evolution repo");
                let r = git2::Repository::init(path)?;
                {
                    let sig =
                        git2::Signature::now("yata-git", "yata-git@gftd.ai")?;
                    let tree_id = r.index()?.write_tree()?;
                    let tree = r.find_tree(tree_id)?;
                    r.commit(
                        Some("HEAD"),
                        &sig,
                        &sig,
                        "initial: evolution repo",
                        &tree,
                        &[],
                    )?;
                }
                r
            }
        };

        Ok(Self {
            repo,
            path: path.to_path_buf(),
        })
    }

    /// Repo filesystem path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Generate a summary of source files for agent context.
    pub fn source_summary(&self) -> anyhow::Result<String> {
        let mut summary = String::new();
        let src_dir = self.path.join("src");
        if src_dir.is_dir() {
            self.walk_dir_summary(&src_dir, &self.path, &mut summary)?;
        } else {
            self.walk_dir_summary(&self.path, &self.path, &mut summary)?;
        }
        Ok(summary)
    }

    fn walk_dir_summary(&self, dir: &Path, base: &Path, out: &mut String) -> anyhow::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            if name.starts_with('.') || name == "target" || name == "component.wasm" {
                continue;
            }
            if path.is_dir() {
                self.walk_dir_summary(&path, base, out)?;
            } else {
                let rel = path.strip_prefix(base).unwrap_or(&path);
                let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                if size < 512_000 {
                    if let Ok(content) = std::fs::read_to_string(&path) {
                        out.push_str(&format!(
                            "--- {} ({} bytes) ---\n{}\n\n",
                            rel.display(),
                            size,
                            content
                        ));
                    } else {
                        out.push_str(&format!(
                            "--- {} ({} bytes, binary) ---\n\n",
                            rel.display(),
                            size
                        ));
                    }
                } else {
                    out.push_str(&format!(
                        "--- {} ({} bytes, large) ---\n\n",
                        rel.display(),
                        size
                    ));
                }
            }
        }
        Ok(())
    }

    /// Read a specific file.
    pub fn read_file(&self, rel_path: &str) -> anyhow::Result<String> {
        let full = self.path.join(rel_path);
        std::fs::read_to_string(&full)
            .map_err(|e| anyhow::anyhow!("read {}: {}", full.display(), e))
    }

    /// Apply a diff (unified or FILE: block format).
    pub fn apply_diff(&self, diff: &str) -> anyhow::Result<()> {
        if diff.contains("FILE: ") {
            return self.apply_file_blocks(diff);
        }

        let diff_path = self.path.join(".evolution-patch.diff");
        std::fs::write(&diff_path, diff)?;

        let output = std::process::Command::new("git")
            .args(["apply", "--allow-empty", ".evolution-patch.diff"])
            .current_dir(&self.path)
            .output()?;

        let _ = std::fs::remove_file(&diff_path);

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(stderr = %stderr, "git apply failed");
            return Err(anyhow::anyhow!("git apply failed: {}", stderr));
        }
        Ok(())
    }

    fn apply_file_blocks(&self, diff: &str) -> anyhow::Result<()> {
        let mut current_file: Option<String> = None;
        let mut content = String::new();
        let mut in_code_block = false;

        for line in diff.lines() {
            if line.starts_with("FILE: ") {
                if let Some(ref path) = current_file {
                    self.write_file(path, &content)?;
                }
                current_file = Some(line.strip_prefix("FILE: ").unwrap().trim().to_string());
                content.clear();
                in_code_block = false;
            } else if line.starts_with("```") {
                if in_code_block {
                    if let Some(ref path) = current_file {
                        self.write_file(path, &content)?;
                    }
                    current_file = None;
                    content.clear();
                }
                in_code_block = !in_code_block;
            } else if in_code_block {
                content.push_str(line);
                content.push('\n');
            }
        }

        if let Some(ref path) = current_file {
            if !content.is_empty() {
                self.write_file(path, &content)?;
            }
        }
        Ok(())
    }

    /// Write a file to the repo working directory.
    pub fn write_file(&self, rel_path: &str, content: &str) -> anyhow::Result<()> {
        let full = self.path.join(rel_path);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&full, content)?;
        info!(path = rel_path, "wrote evolved file");
        Ok(())
    }

    /// Stage all changes and commit. Returns the commit OID.
    pub fn commit(&self, message: &str) -> anyhow::Result<git2::Oid> {
        let mut index = self.repo.index()?;
        index.add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)?;
        index.write()?;
        let tree_id = index.write_tree()?;
        let tree = self.repo.find_tree(tree_id)?;

        let sig = git2::Signature::now("yata-git", "yata-git@gftd.ai")?;

        let parent = self.repo.head().ok().and_then(|h| h.peel_to_commit().ok());

        let oid = match parent {
            Some(ref p) => self
                .repo
                .commit(Some("HEAD"), &sig, &sig, message, &tree, &[p])?,
            None => self
                .repo
                .commit(Some("HEAD"), &sig, &sig, message, &tree, &[])?,
        };

        info!(oid = %oid, "committed evolution");
        Ok(oid)
    }

    /// Rollback to HEAD~1.
    pub fn rollback(&self) -> anyhow::Result<()> {
        let head = self.repo.head()?.peel_to_commit()?;
        if head.parent_count() == 0 {
            return Err(anyhow::anyhow!("cannot rollback: no parent commit"));
        }
        let parent = head.parent(0)?;
        self.repo
            .reset(parent.as_object(), git2::ResetType::Hard, None)?;
        info!(from = %head.id(), to = %parent.id(), "rolled back evolution");
        Ok(())
    }

    /// Get last N commit messages.
    pub fn recent_history(&self, n: usize) -> anyhow::Result<Vec<String>> {
        let mut revwalk = self.repo.revwalk()?;
        revwalk.push_head()?;
        revwalk.set_sorting(git2::Sort::TIME)?;

        let mut messages = Vec::new();
        for oid in revwalk.take(n) {
            let oid = oid?;
            if let Ok(commit) = self.repo.find_commit(oid) {
                if let Some(msg) = commit.message() {
                    messages.push(msg.to_string());
                }
            }
        }
        Ok(messages)
    }
}

// ── Upstream sync (evolution repo ↔ main repo) ───────────────────────────────

const META_FILE: &str = ".evolution-meta.toml";

/// Metadata linking an evolution repo to its upstream path in the main repo.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EvolutionMeta {
    /// Relative path in main repo, e.g. "projects/ai-gftd-project-shinshi/wasm/ai-gftd-wasm-shinshi-sh1n5h1x"
    pub upstream_path: String,
    /// App nanoid.
    pub app_nanoid: String,
    /// Timestamp of last seed/sync from upstream (UTC nanos).
    pub seeded_at_ns: i64,
    /// Commit SHA in evolution repo that was last synced to upstream.
    #[serde(default)]
    pub last_synced_oid: Option<String>,
}

impl GitStore {
    /// Seed an evolution repo from the main repo's subdirectory.
    /// Copies all files from `upstream_dir` into the evolution repo and commits.
    pub fn seed_from_upstream(
        &self,
        app_nanoid: &str,
        upstream_dir: &Path,
        upstream_rel_path: &str,
    ) -> anyhow::Result<EvolutionRepo> {
        let repo = self.open_or_init(app_nanoid)?;

        // Copy files from upstream (skip .git, target, component.wasm).
        copy_dir_recursive(
            upstream_dir,
            repo.path(),
            &[".", "target", "component.wasm"],
        )?;

        // Write evolution metadata.
        let meta = EvolutionMeta {
            upstream_path: upstream_rel_path.to_string(),
            app_nanoid: app_nanoid.to_string(),
            seeded_at_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            last_synced_oid: None,
        };
        let meta_toml =
            toml::to_string_pretty(&meta).map_err(|e| anyhow::anyhow!("serialize meta: {}", e))?;
        repo.write_file(META_FILE, &meta_toml)?;

        repo.commit(&format!("seed from upstream: {}", upstream_rel_path))?;
        info!(
            app = app_nanoid,
            upstream = upstream_rel_path,
            "seeded evolution repo"
        );
        Ok(repo)
    }

    /// Sync evolved files back to the main repo's subdirectory.
    /// Returns the list of files that were updated.
    pub fn sync_to_upstream(
        &self,
        app_nanoid: &str,
        main_repo_root: &Path,
    ) -> anyhow::Result<Vec<String>> {
        let repo_path = self.repo_path(app_nanoid);
        let meta = read_meta(&repo_path)?;
        let upstream_dir = main_repo_root.join(&meta.upstream_path);

        if !upstream_dir.exists() {
            return Err(anyhow::anyhow!(
                "upstream dir not found: {}",
                upstream_dir.display()
            ));
        }

        let mut synced = Vec::new();
        sync_files_recursive(&repo_path, &upstream_dir, &repo_path, &mut synced)?;

        // Update last_synced_oid in meta.
        if !synced.is_empty() {
            let repo = git2_open(&repo_path)?;
            if let Ok(head) = repo.head() {
                if let Some(oid) = head.target() {
                    let mut meta = meta.clone();
                    meta.last_synced_oid = Some(oid.to_string());
                    let meta_toml = toml::to_string_pretty(&meta)
                        .map_err(|e| anyhow::anyhow!("serialize meta: {}", e))?;
                    std::fs::write(repo_path.join(META_FILE), meta_toml)?;
                }
            }
        }

        info!(
            app = app_nanoid,
            upstream = %meta.upstream_path,
            files = synced.len(),
            "synced to upstream"
        );
        Ok(synced)
    }

    /// Read evolution metadata for a repo.
    pub fn read_meta(&self, app_nanoid: &str) -> anyhow::Result<EvolutionMeta> {
        read_meta(&self.repo_path(app_nanoid))
    }
}

fn read_meta(repo_path: &Path) -> anyhow::Result<EvolutionMeta> {
    let meta_path = repo_path.join(META_FILE);
    let content = std::fs::read_to_string(&meta_path)
        .map_err(|e| anyhow::anyhow!("read {}: {}", meta_path.display(), e))?;
    toml::from_str(&content).map_err(|e| anyhow::anyhow!("parse meta: {}", e))
}

fn copy_dir_recursive(src: &Path, dst: &Path, skip_prefixes: &[&str]) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if skip_prefixes.iter().any(|p| name_str.starts_with(p)) {
            continue;
        }

        let src_path = entry.path();
        let dst_path = dst.join(&name);

        if src_path.is_dir() {
            std::fs::create_dir_all(&dst_path)?;
            copy_dir_recursive(&src_path, &dst_path, skip_prefixes)?;
        } else {
            if let Some(parent) = dst_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

fn sync_files_recursive(
    src_root: &Path,
    dst_root: &Path,
    current_src: &Path,
    synced: &mut Vec<String>,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(current_src)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Skip git internals, meta, build artifacts.
        if name_str.starts_with('.') || name_str == "target" || name_str == "component.wasm" {
            continue;
        }

        let src_path = entry.path();
        let rel = src_path.strip_prefix(src_root).unwrap_or(&src_path);
        let dst_path = dst_root.join(rel);

        if src_path.is_dir() {
            std::fs::create_dir_all(&dst_path)?;
            sync_files_recursive(src_root, dst_root, &src_path, synced)?;
        } else {
            // Only sync if content differs.
            let src_content = std::fs::read(&src_path)?;
            let needs_sync = if dst_path.exists() {
                let dst_content = std::fs::read(&dst_path)?;
                src_content != dst_content
            } else {
                true
            };

            if needs_sync {
                if let Some(parent) = dst_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&dst_path, &src_content)?;
                synced.push(rel.to_string_lossy().to_string());
            }
        }
    }
    Ok(())
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn git2_open(path: &Path) -> anyhow::Result<git2::Repository> {
    git2::Repository::open(path).map_err(|e| anyhow::anyhow!("git open {}: {}", path.display(), e))
}

fn count_commits(repo: &git2::Repository) -> u64 {
    let mut revwalk = match repo.revwalk() {
        Ok(rw) => rw,
        Err(_) => return 0,
    };
    if revwalk.push_head().is_err() {
        return 0;
    }
    revwalk.count() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_or_init_and_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = EvolutionRepo::open_or_init(tmp.path()).unwrap();

        // Write a file and commit.
        repo.write_file("main.go", "package main\n").unwrap();
        let oid = repo.commit("add main.go").unwrap();
        assert!(!oid.is_zero());

        // Recent history should include our commit.
        let history = repo.recent_history(5).unwrap();
        assert!(history.iter().any(|m| m.contains("add main.go")));
    }

    #[test]
    fn test_snapshot_and_serialize() {
        let tmp = tempfile::tempdir().unwrap();
        let store = GitStore::new(GitStoreConfig {
            base_dir: tmp.path().to_path_buf(),
        });

        let repo = store.open_or_init("test123").unwrap();
        repo.write_file("hello.txt", "world").unwrap();
        repo.commit("test commit").unwrap();

        let snapshot = store.snapshot_refs("test123").unwrap();
        assert!(snapshot.refs.contains_key("HEAD"));
        assert_eq!(snapshot.commit_count, 2); // initial + test commit

        // Round-trip serialization.
        let data = serialize_snapshot(&snapshot).unwrap();
        let restored = deserialize_snapshot(&data).unwrap();
        assert_eq!(restored.refs, snapshot.refs);
        assert_eq!(restored.commit_count, snapshot.commit_count);
    }

    #[test]
    fn test_rollback() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = EvolutionRepo::open_or_init(tmp.path()).unwrap();

        repo.write_file("a.txt", "v1").unwrap();
        repo.commit("v1").unwrap();

        repo.write_file("a.txt", "v2").unwrap();
        repo.commit("v2").unwrap();

        repo.rollback().unwrap();

        let content = repo.read_file("a.txt").unwrap();
        assert_eq!(content, "v1");
    }

    #[test]
    fn test_seed_and_sync_upstream() {
        let tmp = tempfile::tempdir().unwrap();
        let store = GitStore::new(GitStoreConfig {
            base_dir: tmp.path().join("git"),
        });

        // Simulate upstream directory (main repo subdirectory).
        let upstream = tmp.path().join("upstream");
        std::fs::create_dir_all(&upstream).unwrap();
        std::fs::write(upstream.join("main.go"), "package main\n// v1\n").unwrap();
        std::fs::write(upstream.join("go.mod"), "module example\n").unwrap();

        // Seed evolution repo from upstream.
        let repo = store
            .seed_from_upstream("app1", &upstream, "projects/test/wasm/app1")
            .unwrap();

        // Verify files were copied.
        let content = repo.read_file("main.go").unwrap();
        assert!(content.contains("v1"));

        // Verify meta was written.
        let meta = store.read_meta("app1").unwrap();
        assert_eq!(meta.upstream_path, "projects/test/wasm/app1");
        assert_eq!(meta.app_nanoid, "app1");

        // Evolve the code.
        repo.write_file("main.go", "package main\n// v2 evolved\n")
            .unwrap();
        repo.commit("evolve to v2").unwrap();

        // Sync back to a "main repo" directory.
        let main_repo = tmp.path().join("main-repo");
        let main_upstream = main_repo.join("projects/test/wasm/app1");
        std::fs::create_dir_all(&main_upstream).unwrap();
        std::fs::write(main_upstream.join("main.go"), "package main\n// v1\n").unwrap();

        let synced = store.sync_to_upstream("app1", &main_repo).unwrap();
        assert!(synced.contains(&"main.go".to_string()));

        // Verify upstream was updated.
        let synced_content = std::fs::read_to_string(main_upstream.join("main.go")).unwrap();
        assert!(synced_content.contains("v2 evolved"));

        // Verify last_synced_oid was set.
        let meta = store.read_meta("app1").unwrap();
        assert!(meta.last_synced_oid.is_some());
    }

    #[test]
    fn test_list_repos() {
        let tmp = tempfile::tempdir().unwrap();
        let store = GitStore::new(GitStoreConfig {
            base_dir: tmp.path().to_path_buf(),
        });

        store.open_or_init("app1").unwrap();
        store.open_or_init("app2").unwrap();

        let mut repos = store.list_repos().unwrap();
        repos.sort();
        assert_eq!(repos, vec!["app1", "app2"]);
    }
}
