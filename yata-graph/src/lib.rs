#![allow(dead_code)]
//! Graph store with Cypher query engine and vector search.
//!
//! In-memory MemoryGraph + yata-vex vector index. MDAG CAS is sole persistence.

pub mod cache;
pub mod coordinator;
pub mod hints;
pub mod per_label;
pub mod pipeline;

use arrow_array::{RecordBatch, StringArray};
use indexmap::IndexMap;

// ---- Schema registry types -----------------------------------------------

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub enum SchemaMode {
    #[default]
    Schemaless,
    Default,
    Strict,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum PropertyDataType {
    Str,
    Int64,
    Float64,
    Bool,
    TimestampNs,
    Bytes,
    Vector(usize),
    ListStr,
    ListInt64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Cardinality {
    Single,
    List,
    Set,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Multiplicity {
    Simple,
    Many2Many,
    One2Many,
    Many2One,
    One2One,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PropertyKeyDef {
    pub name: String,
    pub data_type: PropertyDataType,
    pub cardinality: Cardinality,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct VertexLabelDef {
    pub name: String,
    pub properties: Vec<String>,
    pub partition_key: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EdgeLabelDef {
    pub name: String,
    pub multiplicity: Multiplicity,
    pub properties: Vec<String>,
    pub directed: bool,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GraphSchema {
    pub version: u64,
    pub mode: SchemaMode,
    pub property_keys: IndexMap<String, PropertyKeyDef>,
    pub vertex_labels: IndexMap<String, VertexLabelDef>,
    pub edge_labels: IndexMap<String, EdgeLabelDef>,
}

// ---- Errors -------------------------------------------------------------

#[derive(thiserror::Error, Debug)]
pub enum GraphError {
    #[error("storage error: {0}")]
    Storage(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("schema error: {0}")]
    Schema(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("query error: {0}")]
    Query(String),
}

pub type GraphResult<T> = std::result::Result<T, GraphError>;

/// Statistics from a graph delta write operation.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DeltaStats {
    pub nodes_created: usize,
    pub nodes_modified: usize,
    pub nodes_deleted: usize,
    pub edges_created: usize,
    pub edges_modified: usize,
    pub edges_deleted: usize,
}

// ---- Arrow schemas for graph tables ------------------------------------

pub mod graph_arrow {
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    pub fn vertices_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("vid", DataType::Utf8, false),
            Field::new("labels_json", DataType::Utf8, false),
            Field::new("props_json", DataType::Utf8, false),
            Field::new("created_ns", DataType::Int64, false),
        ]))
    }

    pub fn vertices_with_embedding_schema(dim: usize) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("vid", DataType::Utf8, false),
            Field::new("labels_json", DataType::Utf8, false),
            Field::new("props_json", DataType::Utf8, false),
            Field::new("created_ns", DataType::Int64, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    dim as i32,
                ),
                true,
            ),
        ]))
    }

    pub fn edges_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("eid", DataType::Utf8, false),
            Field::new("src", DataType::Utf8, false),
            Field::new("dst", DataType::Utf8, false),
            Field::new("rel_type", DataType::Utf8, false),
            Field::new("props_json", DataType::Utf8, false),
            Field::new("created_ns", DataType::Int64, false),
        ]))
    }

    pub fn adj_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("vid", DataType::Utf8, false),
            Field::new("direction", DataType::Utf8, false),
            Field::new("edge_label", DataType::Utf8, false),
            Field::new("neighbor_vid", DataType::Utf8, false),
            Field::new("eid", DataType::Utf8, false),
            Field::new("created_ns", DataType::Int64, false),
        ]))
    }
}

// ---- GraphStore ---------------------------------------------------------

/// In-memory graph store with Cypher query engine and vector search.
///
/// In-memory graph store. MDAG CAS is the sole persistence path.
/// Vector search uses `yata_vex`.
pub struct GraphStore {
    pub schema: GraphSchema,
    /// In-memory cache: CSR graph + query result LRU.
    pub cache: tokio::sync::RwLock<cache::GraphCache>,
    /// Lazy vector index: auto-tiered brute-force → IVF_PQ.
    vector_index: yata_vex::lazy::LazyVectorIndex,
    /// Mapping from fxhash vid (u64) to original string vid.
    vid_map: tokio::sync::RwLock<IndexMap<u64, String>>,
}

impl GraphStore {
    pub async fn new(_base_uri: impl Into<String>) -> GraphResult<Self> {
        Self::with_cache_config(_base_uri, cache::CacheConfig::default()).await
    }

    pub async fn with_cache_config(
        _base_uri: impl Into<String>,
        cache_config: cache::CacheConfig,
    ) -> GraphResult<Self> {
        let base = _base_uri.into();
        // Use base_uri as data_dir for vector index disk persistence
        // Only use disk-backed vector index for real filesystem paths
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
        Ok(Self {
            schema: GraphSchema::default(),
            cache: tokio::sync::RwLock::new(cache::GraphCache::new(cache_config)),
            vector_index,
            vid_map: tokio::sync::RwLock::new(IndexMap::new()),
        })
    }

    /// Write vertices to the in-memory graph cache.
    pub async fn write_vertices(&self, nodes: &[yata_cypher::NodeRef]) -> GraphResult<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        use yata_cypher::Graph;
        let mut cache = self.cache.write().await;
        let mut mg = cache
            .get_csr()
            .cloned()
            .unwrap_or_else(yata_cypher::MemoryGraph::new);
        for node in nodes {
            mg.add_node(node.clone());
        }
        mg.build_csr();
        cache.set_csr(mg);
        Ok(())
    }

    /// Write edges to the in-memory graph cache.
    pub async fn write_edges(&self, rels: &[yata_cypher::RelRef]) -> GraphResult<()> {
        if rels.is_empty() {
            return Ok(());
        }
        use yata_cypher::Graph;
        let mut cache = self.cache.write().await;
        let mut mg = cache
            .get_csr()
            .cloned()
            .unwrap_or_else(yata_cypher::MemoryGraph::new);
        for rel in rels {
            mg.add_rel(rel.clone());
        }
        mg.build_csr();
        cache.set_csr(mg);
        Ok(())
    }

    /// Load all vertices from cache.
    pub async fn load_vertices(&self) -> GraphResult<Vec<yata_cypher::NodeRef>> {
        let cache = self.cache.read().await;
        if let Some(mg) = cache.get_csr() {
            use yata_cypher::Graph;
            Ok(mg.nodes())
        } else {
            Ok(Vec::new())
        }
    }

    /// Load all edges from cache.
    pub async fn load_edges(&self) -> GraphResult<Vec<yata_cypher::RelRef>> {
        let cache = self.cache.read().await;
        if let Some(mg) = cache.get_csr() {
            use yata_cypher::Graph;
            Ok(mg.rels())
        } else {
            Ok(Vec::new())
        }
    }

    /// Load all vertices + edges into a QueryableGraph for Cypher queries.
    pub async fn to_memory_graph(&self) -> GraphResult<QueryableGraph> {
        self.to_memory_graph_bounded(0, 0).await
    }

    /// Return a clone of the cached CSR MemoryGraph.
    /// On cold start, returns an empty graph (MDAG CAS restore happens externally).
    pub async fn to_memory_graph_cached(&self) -> GraphResult<QueryableGraph> {
        let cache = self.cache.read().await;
        if let Some(csr) = cache.get_csr() {
            return Ok(QueryableGraph(csr.clone()));
        }
        // Cold: return empty graph and store it
        drop(cache);
        let mg = yata_cypher::MemoryGraph::new();
        let qg = QueryableGraph(mg.clone());
        let mut cache = self.cache.write().await;
        if cache.get_csr().is_none() {
            cache.set_csr(mg);
        }
        Ok(qg)
    }

    /// Execute a Cypher query with full caching:
    ///   1. Query result cache hit -> us
    ///   2. CSR cache hit -> ns (graph traversal) + query execution
    ///   3. Cold -> empty graph
    ///
    /// Mutations (CREATE/MERGE/DELETE/SET) bypass query cache and
    /// write-back delta to the in-memory graph, invalidating the CSR.
    pub async fn cached_query(
        &self,
        cypher: &str,
        params: &[(String, String)],
    ) -> GraphResult<CachedQueryResult> {
        let is_mut = is_mutation(cypher);

        // Read-only: check query result cache first
        if !is_mut {
            let key = cache::GraphCache::cache_key(cypher, params);
            let cache = self.cache.read().await;
            if let Some(rows) = cache.get_query(&key) {
                return Ok(CachedQueryResult {
                    rows: rows.clone(),
                    delta: None,
                    cache_hit: true,
                });
            }
        }

        // Load or use cached CSR
        let mut qg = self.to_memory_graph_cached().await?;

        // Snapshot before state for mutations
        let (before_nodes, before_edges) = if is_mut {
            use yata_cypher::Graph;
            let bn: IndexMap<String, yata_cypher::NodeRef> =
                qg.0.nodes()
                    .into_iter()
                    .map(|n| (n.id.clone(), n))
                    .collect();
            let be: IndexMap<String, yata_cypher::RelRef> =
                qg.0.rels().into_iter().map(|e| (e.id.clone(), e)).collect();
            (Some(bn), Some(be))
        } else {
            (None, None)
        };

        // Execute Cypher
        let rows = qg
            .query(cypher, params)
            .map_err(|e| GraphError::Query(e.to_string()))?;

        // Mutation write-back
        let delta = if is_mut {
            let stats = self
                .write_delta(
                    before_nodes.as_ref().unwrap(),
                    before_edges.as_ref().unwrap(),
                    &qg.0,
                )
                .await?;
            // Invalidate query cache and update CSR with new state
            let mut cache = self.cache.write().await;
            cache.invalidate();
            cache.set_csr(qg.0.clone());
            tracing::info!(?stats, "cached_query: delta written, cache invalidated");
            Some(stats)
        } else {
            // Cache the result
            let key = cache::GraphCache::cache_key(cypher, params);
            self.cache.write().await.put_query(key, rows.clone());
            None
        };

        Ok(CachedQueryResult {
            rows,
            delta,
            cache_hit: false,
        })
    }

    /// Cache stats for observability.
    pub async fn cache_stats(&self) -> cache::CacheStats {
        self.cache.read().await.stats()
    }

    /// Load vertices + edges with optional size guards.
    /// `max_nodes=0` / `max_edges=0` means unlimited.
    pub async fn to_memory_graph_bounded(
        &self,
        max_nodes: usize,
        max_edges: usize,
    ) -> GraphResult<QueryableGraph> {
        use yata_cypher::Graph;
        let cache = self.cache.read().await;
        if let Some(csr) = cache.get_csr() {
            let nodes = csr.nodes();
            let rels = csr.rels();
            if max_nodes > 0 && nodes.len() > max_nodes {
                return Err(GraphError::Storage(format!(
                    "graph too large: {} vertices exceeds limit {}",
                    nodes.len(),
                    max_nodes,
                )));
            }
            if max_edges > 0 && rels.len() > max_edges {
                return Err(GraphError::Storage(format!(
                    "graph too large: {} edges exceeds limit {}",
                    rels.len(),
                    max_edges,
                )));
            }
            return Ok(QueryableGraph(csr.clone()));
        }
        // Cold: return empty graph
        Ok(QueryableGraph(yata_cypher::MemoryGraph::new()))
    }

    /// Write pre-built Arrow RecordBatch directly to graph_vertices.
    /// Parses Arrow columns and adds to in-memory graph.
    pub async fn write_vertices_batch(&self, batch: RecordBatch) -> GraphResult<()> {
        let nodes = deserialize_vertices_from_batch(&batch);
        self.write_vertices(&nodes).await
    }

    /// Write pre-built Arrow RecordBatch directly to graph_edges.
    pub async fn write_edges_batch(&self, batch: RecordBatch) -> GraphResult<()> {
        let rels = deserialize_edges_from_batch(&batch);
        self.write_edges(&rels).await
    }

    /// Compute graph delta between before/after state and write to graph.
    pub async fn write_delta(
        &self,
        before_nodes: &indexmap::IndexMap<String, yata_cypher::NodeRef>,
        before_edges: &indexmap::IndexMap<String, yata_cypher::RelRef>,
        after_graph: &yata_cypher::MemoryGraph,
    ) -> GraphResult<DeltaStats> {
        use yata_cypher::Graph;

        let after_nodes = after_graph.nodes();
        let after_edges = after_graph.rels();

        let mut upsert_nodes: Vec<yata_cypher::NodeRef> = Vec::new();
        let mut deleted_node_count: usize = 0;

        for node in &after_nodes {
            match before_nodes.get(&node.id) {
                None => upsert_nodes.push(node.clone()),
                Some(old) if old != node => upsert_nodes.push(node.clone()),
                _ => {}
            }
        }

        for (id, _) in before_nodes {
            if after_graph.node_by_id(id).is_none() {
                deleted_node_count += 1;
            }
        }

        let mut upsert_edges: Vec<yata_cypher::RelRef> = Vec::new();
        let mut deleted_edge_count: usize = 0;

        for edge in &after_edges {
            match before_edges.get(&edge.id) {
                None => upsert_edges.push(edge.clone()),
                Some(old) if old != edge => upsert_edges.push(edge.clone()),
                _ => {}
            }
        }

        for (id, _) in before_edges {
            if after_graph.rel_by_id(id).is_none() {
                deleted_edge_count += 1;
            }
        }

        let nodes_created = upsert_nodes
            .iter()
            .filter(|n| !before_nodes.contains_key(&n.id))
            .count();
        let nodes_modified = upsert_nodes.len() - nodes_created;
        let edges_created = upsert_edges
            .iter()
            .filter(|e| !before_edges.contains_key(&e.id))
            .count();
        let edges_modified = upsert_edges.len() - edges_created;

        // Write upserts to in-memory graph
        if !upsert_nodes.is_empty() {
            self.write_vertices(&upsert_nodes).await?;
        }
        if !upsert_edges.is_empty() {
            self.write_edges(&upsert_edges).await?;
        }

        Ok(DeltaStats {
            nodes_created,
            nodes_modified,
            nodes_deleted: deleted_node_count,
            edges_created,
            edges_modified,
            edges_deleted: deleted_edge_count,
        })
    }

    /// Write vertices with embedding vectors.
    ///
    /// Stores nodes in the in-memory graph and builds a yata-vex vector index
    /// for the embedding property.
    pub async fn write_vertices_with_embeddings(
        &self,
        nodes: &[yata_cypher::NodeRef],
        embedding_key: &str,
        dim: usize,
    ) -> GraphResult<()> {
        if nodes.is_empty() {
            return Ok(());
        }

        let mut vid_map = self.vid_map.write().await;

        for node in nodes {
            if let Some(val) = node.props.get(embedding_key) {
                if let Some(vec) = extract_f32_vec(val) {
                    if vec.len() == dim {
                        let vid_hash = fxhash_vid(&node.id);
                        // Lazy append: no index rebuild, just store
                        self.vector_index
                            .append(vid_hash, &vec)
                            .map_err(|e| GraphError::Storage(e.to_string()))?;
                        vid_map.insert(vid_hash, node.id.clone());
                    }
                }
            }
        }
        drop(vid_map);

        // Write nodes to graph cache (strip embedding from props)
        let stripped: Vec<yata_cypher::NodeRef> = nodes
            .iter()
            .map(|n| {
                let mut node = n.clone();
                node.props.shift_remove(embedding_key);
                node
            })
            .collect();
        self.write_vertices(&stripped).await?;

        // Persist vector store to disk (lazy, skips if clean)
        if let Err(e) = self.vector_index.persist_to_disk() {
            tracing::warn!("vector index disk persist failed: {e}");
        }
        Ok(())
    }

    /// Vector search over vertices using the lazy yata-vex index.
    /// Index is built on first search (lazy), not on write.
    /// < 1000 vectors: brute-force scan. >= 1000: IVF_PQ.
    pub async fn vector_search_vertices(
        &self,
        query_vector: Vec<f32>,
        limit: usize,
        _label_filter: Option<&str>,
        _prop_filter: Option<&str>,
    ) -> GraphResult<Vec<(yata_cypher::NodeRef, f32)>> {
        use yata_vex::VectorIndex;

        let nprobes = 16.min(limit * 2).max(1);
        let results = self
            .vector_index
            .search(&query_vector, limit, nprobes)
            .map_err(|e| GraphError::Storage(e.to_string()))?;

        // Map vid back to NodeRef via vid_map + graph cache
        let vid_map = self.vid_map.read().await;
        let cache = self.cache.read().await;
        let mg = cache.get_csr();

        let mut out = Vec::new();
        for r in results {
            if let Some(str_vid) = vid_map.get(&r.vid) {
                let node = mg
                    .and_then(|g| {
                        use yata_cypher::Graph;
                        g.node_by_id(str_vid)
                    })
                    .unwrap_or_else(|| yata_cypher::NodeRef {
                        id: str_vid.clone(),
                        labels: vec![],
                        props: IndexMap::new(),
                    });
                out.push((node, r.distance));
            } else {
                out.push((
                    yata_cypher::NodeRef {
                        id: format!("v{}", r.vid),
                        labels: vec![],
                        props: IndexMap::new(),
                    },
                    r.distance,
                ));
            }
        }
        Ok(out)
    }

    /// Create vector index (triggers lazy build if dirty).
    pub async fn create_embedding_index(&self) -> GraphResult<()> {
        self.vector_index
            .ensure_index()
            .map_err(|e| GraphError::Storage(e.to_string()))
    }

    /// Restore vector index from disk (cold start).
    /// Returns number of vectors loaded.
    pub fn restore_vector_index(&self) -> GraphResult<usize> {
        let n = self
            .vector_index
            .restore_from_disk()
            .map_err(|e| GraphError::Storage(e.to_string()))?;
        if n > 0 {
            tracing::info!(vectors = n, "GraphStore: restored vector index from disk");
        }
        Ok(n)
    }

    /// Access the lazy vector index (for CAS persist/restore from engine layer).
    pub fn lazy_vector_index(&self) -> &yata_vex::lazy::LazyVectorIndex {
        &self.vector_index
    }

    /// Optimize (no-op, in-memory store has nothing to compact).
    pub async fn optimize(&self) -> GraphResult<()> {
        Ok(())
    }
}

// ---- Helper: vid hashing ------------------------------------------------

fn fxhash_vid(vid: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in vid.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

// ---- Helper: column accessor -------------------------------------------

fn col_str<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

// ---- Helper: deserialize from Arrow batches ----------------------------

fn deserialize_vertices_from_batch(batch: &RecordBatch) -> Vec<yata_cypher::NodeRef> {
    let mut nodes = Vec::new();
    let vids = col_str(batch, "vid");
    let labels_jsons = col_str(batch, "labels_json");
    let props_jsons = col_str(batch, "props_json");
    let (Some(vids), Some(labels_jsons), Some(props_jsons)) = (vids, labels_jsons, props_jsons)
    else {
        return nodes;
    };
    for i in 0..batch.num_rows() {
        let vid = vids.value(i).to_owned();
        let labels: Vec<String> = serde_json::from_str(labels_jsons.value(i)).unwrap_or_default();
        let json_props: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(props_jsons.value(i)).unwrap_or_default();
        let props: IndexMap<String, yata_cypher::Value> = json_props
            .into_iter()
            .map(|(k, v)| (k, json_to_cypher(&v)))
            .collect();
        nodes.push(yata_cypher::NodeRef {
            id: vid,
            labels,
            props,
        });
    }
    nodes
}

fn deserialize_edges_from_batch(batch: &RecordBatch) -> Vec<yata_cypher::RelRef> {
    let mut rels = Vec::new();
    let eids = col_str(batch, "eid");
    let srcs = col_str(batch, "src");
    let dsts = col_str(batch, "dst");
    let rel_types = col_str(batch, "rel_type");
    let props_jsons = col_str(batch, "props_json");
    let (Some(eids), Some(srcs), Some(dsts), Some(rel_types), Some(props_jsons)) =
        (eids, srcs, dsts, rel_types, props_jsons)
    else {
        return rels;
    };
    for i in 0..batch.num_rows() {
        let eid = eids.value(i).to_owned();
        let json_props: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(props_jsons.value(i)).unwrap_or_default();
        let props: IndexMap<String, yata_cypher::Value> = json_props
            .into_iter()
            .map(|(k, v)| (k, json_to_cypher(&v)))
            .collect();
        rels.push(yata_cypher::RelRef {
            id: eid,
            src: srcs.value(i).to_owned(),
            dst: dsts.value(i).to_owned(),
            rel_type: rel_types.value(i).to_owned(),
            props,
        });
    }
    rels
}

// ---- Value conversion --------------------------------------------------

pub fn cypher_to_json(v: &yata_cypher::Value) -> serde_json::Value {
    match v {
        yata_cypher::Value::Null => serde_json::Value::Null,
        yata_cypher::Value::Bool(b) => serde_json::Value::Bool(*b),
        yata_cypher::Value::Int(i) => serde_json::Value::Number((*i).into()),
        yata_cypher::Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        yata_cypher::Value::Str(s) => serde_json::Value::String(s.clone()),
        yata_cypher::Value::List(l) => {
            serde_json::Value::Array(l.iter().map(cypher_to_json).collect())
        }
        yata_cypher::Value::Map(m) => serde_json::Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), cypher_to_json(v)))
                .collect(),
        ),
        yata_cypher::Value::Node(n) => {
            let mut obj: serde_json::Map<String, serde_json::Value> = n
                .props
                .iter()
                .map(|(k, v)| (k.clone(), cypher_to_json(v)))
                .collect();
            obj.insert("__vid".into(), serde_json::Value::String(n.id.clone()));
            obj.insert(
                "__labels".into(),
                serde_json::Value::Array(
                    n.labels
                        .iter()
                        .map(|l| serde_json::Value::String(l.clone()))
                        .collect(),
                ),
            );
            serde_json::Value::Object(obj)
        }
        yata_cypher::Value::Rel(r) => {
            let mut obj: serde_json::Map<String, serde_json::Value> = r
                .props
                .iter()
                .map(|(k, v)| (k.clone(), cypher_to_json(v)))
                .collect();
            obj.insert("__eid".into(), serde_json::Value::String(r.id.clone()));
            obj.insert(
                "__type".into(),
                serde_json::Value::String(r.rel_type.clone()),
            );
            serde_json::Value::Object(obj)
        }
    }
}

pub fn json_to_cypher(v: &serde_json::Value) -> yata_cypher::Value {
    match v {
        serde_json::Value::Null => yata_cypher::Value::Null,
        serde_json::Value::Bool(b) => yata_cypher::Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                yata_cypher::Value::Int(i)
            } else {
                yata_cypher::Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => yata_cypher::Value::Str(s.clone()),
        serde_json::Value::Array(a) => {
            yata_cypher::Value::List(a.iter().map(json_to_cypher).collect())
        }
        serde_json::Value::Object(m) => {
            let mut map = IndexMap::new();
            for (k, v) in m {
                map.insert(k.clone(), json_to_cypher(v));
            }
            yata_cypher::Value::Map(map)
        }
    }
}

/// Extract f32 vector from a Cypher Value.
pub fn extract_f32_vec(val: &yata_cypher::Value) -> Option<Vec<f32>> {
    if let yata_cypher::Value::List(items) = val {
        items
            .iter()
            .map(|v| match v {
                yata_cypher::Value::Float(f) => Some(*f as f32),
                yata_cypher::Value::Int(i) => Some(*i as f32),
                _ => None,
            })
            .collect()
    } else {
        None
    }
}

// ---- CachedQueryResult --------------------------------------------------

/// Result from `GraphStore::cached_query()`.
#[derive(Debug, Clone)]
pub struct CachedQueryResult {
    pub rows: Vec<Vec<(String, String)>>,
    pub delta: Option<DeltaStats>,
    pub cache_hit: bool,
}

fn is_mutation(cypher: &str) -> bool {
    let upper = cypher.to_uppercase();
    upper.contains("CREATE")
        || upper.contains("MERGE")
        || upper.contains("DELETE")
        || upper.contains("SET ")
        || upper.contains("REMOVE ")
}

// ---- QueryableGraph -----------------------------------------------------

/// Wraps a loaded MemoryGraph and exposes a `.query()` convenience method
/// that parses and executes a Cypher string, returning rows as
/// `Vec<Vec<(col_name, json_encoded_value)>>`.
pub struct QueryableGraph(pub yata_cypher::MemoryGraph);

impl QueryableGraph {
    pub fn query(
        &mut self,
        cypher: &str,
        params: &[(String, String)],
    ) -> Result<Vec<Vec<(String, String)>>, yata_cypher::CypherError> {
        let query = yata_cypher::parse(cypher)?;
        let mut param_map = IndexMap::new();
        for (k, v) in params {
            let val: serde_json::Value =
                serde_json::from_str(v).unwrap_or(serde_json::Value::String(v.clone()));
            param_map.insert(k.clone(), json_to_cypher(&val));
        }
        let result = yata_cypher::Executor::with_params(param_map).execute(&query, &mut self.0)?;
        let rows = result
            .rows
            .into_iter()
            .map(|row| {
                row.0
                    .into_iter()
                    .map(|(col, val)| {
                        let json = serde_json::to_string(&cypher_to_json(&val)).unwrap_or_default();
                        (col, json)
                    })
                    .collect()
            })
            .collect();
        Ok(rows)
    }
}

#[cfg(test)]
mod tests;
