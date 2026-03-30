use indexmap::IndexMap;
use tokio::sync::RwLock;
use yata_cypher::{NodeRef, Value};

#[derive(thiserror::Error, Debug)]
pub enum VectorStoreError {
    #[error("storage error: {0}")]
    Storage(String),
}

pub type VectorStoreResult<T> = std::result::Result<T, VectorStoreError>;

pub struct YataVectorStore {
    vector_index: yata_vex::lazy::LazyVectorIndex,
    vid_map: RwLock<IndexMap<u64, NodeRef>>,
}

impl YataVectorStore {
    pub async fn new(base_uri: impl Into<String>) -> Self {
        let base = base_uri.into();
        let vex_dir = if base.is_empty() || base.contains(':') || base == "." {
            None
        } else {
            let p = std::path::PathBuf::from(&base).join("vex");
            let _ = std::fs::create_dir_all(&p);
            Some(p)
        };
        let vector_index = match vex_dir {
            Some(dir) => {
                yata_vex::lazy::LazyVectorIndex::with_data_dir(0, yata_vex::DistanceMetric::L2, dir)
            }
            None => yata_vex::lazy::LazyVectorIndex::new(0, yata_vex::DistanceMetric::L2),
        };
        Self {
            vector_index,
            vid_map: RwLock::new(IndexMap::new()),
        }
    }

    pub async fn write_vertices_with_embeddings(
        &self,
        nodes: &[NodeRef],
        embedding_key: &str,
        dim: usize,
    ) -> VectorStoreResult<()> {
        if nodes.is_empty() {
            return Ok(());
        }

        let mut vid_map = self.vid_map.write().await;
        for node in nodes {
            if let Some(val) = node.props.get(embedding_key) {
                if let Some(vec) = extract_f32_vec(val) {
                    if vec.len() == dim {
                        let vid_hash = fxhash_vid(&node.id);
                        self.vector_index
                            .append(vid_hash, &vec)
                            .map_err(|e| VectorStoreError::Storage(e.to_string()))?;
                        vid_map.insert(vid_hash, node.clone());
                    }
                }
            }
        }
        drop(vid_map);

        if let Err(e) = self.vector_index.persist_to_disk() {
            tracing::warn!("vector index disk persist failed: {e}");
        }
        Ok(())
    }

    pub async fn vector_search_vertices(
        &self,
        query_vector: Vec<f32>,
        limit: usize,
        label_filter: Option<&str>,
        _prop_filter: Option<&str>,
    ) -> VectorStoreResult<Vec<(NodeRef, f32)>> {
        use yata_vex::VectorIndex;

        let nprobes = 16.min(limit * 2).max(1);
        let results = self
            .vector_index
            .search(&query_vector, limit.saturating_mul(4).max(limit), nprobes)
            .map_err(|e| VectorStoreError::Storage(e.to_string()))?;

        let vid_map = self.vid_map.read().await;
        let mut out = Vec::new();
        for r in results {
            if let Some(node) = vid_map.get(&r.vid) {
                if let Some(label) = label_filter {
                    if !node.labels.iter().any(|l| l == label) {
                        continue;
                    }
                }
                out.push((node.clone(), r.distance));
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    pub async fn create_embedding_index(&self) -> VectorStoreResult<()> {
        self.vector_index
            .ensure_index()
            .map_err(|e| VectorStoreError::Storage(e.to_string()))
    }

    pub async fn vector_count(&self) -> usize {
        self.vid_map.read().await.len()
    }
}

fn fxhash_vid(vid: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in vid.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn extract_f32_vec(val: &Value) -> Option<Vec<f32>> {
    if let Value::List(items) = val {
        items
            .iter()
            .map(|v| match v {
                Value::Float(f) => Some(*f as f32),
                Value::Int(i) => Some(*i as f32),
                _ => None,
            })
            .collect()
    } else {
        None
    }
}
