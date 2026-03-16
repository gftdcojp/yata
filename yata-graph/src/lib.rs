#![allow(dead_code)]

//! JanusGraph-equivalent graph store on Arrow + LanceDB.
//!
//! Phase 1 (verification): single-table schemaless storage.
//!   graph_vertices — all vertex data (vid, labels as JSON, props as JSON)
//!   graph_edges    — all edge data (eid, src, dst, rel_type, props as JSON)
//!   graph_adj      — adjacency index (vid, direction OUT/IN, edge_label, neighbor_vid, eid)
//!
//! Phase 2 (planned): per-label typed Arrow columns, schema registry in KV.

use std::sync::Arc;
use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray, Float32Array, FixedSizeListArray};
use arrow_schema::{Schema, DataType, Field};
use futures::TryStreamExt;
use indexmap::IndexMap;
use lancedb::query::{ExecutableQuery, QueryBase};

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
            Field::new("embedding", DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                dim as i32,
            ), true),
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

// ---- LanceGraphStore ----------------------------------------------------

pub struct LanceGraphStore {
    conn: lancedb::Connection,
    pub schema: GraphSchema,
}

impl LanceGraphStore {
    pub async fn new(base_uri: impl Into<String>) -> GraphResult<Self> {
        let conn = lancedb::connect(&base_uri.into())
            .execute()
            .await
            .map_err(|e| GraphError::Storage(e.to_string()))?;
        Ok(Self {
            conn,
            schema: GraphSchema::default(),
        })
    }

    /// Write vertices to graph_vertices table (append).
    pub async fn write_vertices(&self, nodes: &[yata_cypher::NodeRef]) -> GraphResult<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        let vids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
        let labels_jsons: Vec<String> = nodes
            .iter()
            .map(|n| serde_json::to_string(&n.labels).unwrap_or_else(|_| "[]".into()))
            .collect();
        let props_jsons: Vec<String> = nodes
            .iter()
            .map(|n| {
                let m: serde_json::Map<String, serde_json::Value> = n
                    .props
                    .iter()
                    .map(|(k, v)| (k.clone(), cypher_to_json(v)))
                    .collect();
                serde_json::to_string(&m).unwrap_or_else(|_| "{}".into())
            })
            .collect();
        let created_nses = vec![now_ns; nodes.len()];

        let schema = graph_arrow::vertices_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vids)) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(
                    labels_jsons.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(
                    props_jsons.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(created_nses)) as Arc<dyn arrow_array::Array>,
            ],
        )
        .map_err(|e| GraphError::Storage(e.to_string()))?;

        self.append_batch("graph_vertices", schema, batch).await
    }

    /// Write edges to graph_edges + adjacency rows to graph_adj (append).
    pub async fn write_edges(&self, rels: &[yata_cypher::RelRef]) -> GraphResult<()> {
        if rels.is_empty() {
            return Ok(());
        }
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // --- graph_edges ---
        let eids: Vec<&str> = rels.iter().map(|r| r.id.as_str()).collect();
        let srcs: Vec<&str> = rels.iter().map(|r| r.src.as_str()).collect();
        let dsts: Vec<&str> = rels.iter().map(|r| r.dst.as_str()).collect();
        let rel_types: Vec<&str> = rels.iter().map(|r| r.rel_type.as_str()).collect();
        let props_jsons: Vec<String> = rels
            .iter()
            .map(|r| {
                let m: serde_json::Map<String, serde_json::Value> = r
                    .props
                    .iter()
                    .map(|(k, v)| (k.clone(), cypher_to_json(v)))
                    .collect();
                serde_json::to_string(&m).unwrap_or_else(|_| "{}".into())
            })
            .collect();
        let created_nses = vec![now_ns; rels.len()];

        let edge_schema = graph_arrow::edges_schema();
        let edge_batch = RecordBatch::try_new(
            edge_schema.clone(),
            vec![
                Arc::new(StringArray::from(eids)) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(srcs)) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(dsts)) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(rel_types)) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(
                    props_jsons.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(created_nses)) as Arc<dyn arrow_array::Array>,
            ],
        )
        .map_err(|e| GraphError::Storage(e.to_string()))?;
        self.append_batch("graph_edges", edge_schema, edge_batch).await?;

        // --- graph_adj (2 rows per edge: OUT and IN) ---
        let mut adj_vids: Vec<String> = Vec::new();
        let mut adj_dirs: Vec<&str> = Vec::new();
        let mut adj_labels: Vec<String> = Vec::new();
        let mut adj_neighbors: Vec<String> = Vec::new();
        let mut adj_eids: Vec<String> = Vec::new();
        let mut adj_nses: Vec<i64> = Vec::new();

        for rel in rels {
            adj_vids.push(rel.src.clone());
            adj_dirs.push("OUT");
            adj_labels.push(rel.rel_type.clone());
            adj_neighbors.push(rel.dst.clone());
            adj_eids.push(rel.id.clone());
            adj_nses.push(now_ns);

            adj_vids.push(rel.dst.clone());
            adj_dirs.push("IN");
            adj_labels.push(rel.rel_type.clone());
            adj_neighbors.push(rel.src.clone());
            adj_eids.push(rel.id.clone());
            adj_nses.push(now_ns);
        }

        let adj_schema = graph_arrow::adj_schema();
        let adj_batch = RecordBatch::try_new(
            adj_schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    adj_vids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(adj_dirs)) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(
                    adj_labels.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(
                    adj_neighbors.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(
                    adj_eids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(adj_nses)) as Arc<dyn arrow_array::Array>,
            ],
        )
        .map_err(|e| GraphError::Storage(e.to_string()))?;
        self.append_batch("graph_adj", adj_schema, adj_batch).await
    }

    /// Load all vertices from LanceDB.
    pub async fn load_vertices(&self) -> GraphResult<Vec<yata_cypher::NodeRef>> {
        let table = match self.conn.open_table("graph_vertices").execute().await {
            Ok(t) => {
                // Ensure we read the latest committed version (not a stale cache).
                let _ = t.checkout_latest().await;
                t
            }
            Err(_) => return Ok(Vec::new()),
        };
        let stream = table
            .query()
            .execute()
            .await
            .map_err(|e| GraphError::Storage(e.to_string()))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| GraphError::Storage(e.to_string()))?;

        // last-write-wins dedup: append-only table may have multiple rows per vid.
        // Rows are in insertion order (created_ns ascending), so later entries override.
        let mut seen: IndexMap<String, yata_cypher::NodeRef> = IndexMap::new();
        for batch in &batches {
            let vids = col_str(batch, "vid");
            let labels_jsons = col_str(batch, "labels_json");
            let props_jsons = col_str(batch, "props_json");
            let (Some(vids), Some(labels_jsons), Some(props_jsons)) =
                (vids, labels_jsons, props_jsons)
            else {
                continue;
            };
            for i in 0..batch.num_rows() {
                let vid = vids.value(i).to_owned();
                let labels: Vec<String> =
                    serde_json::from_str(labels_jsons.value(i)).unwrap_or_default();
                let json_props: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(props_jsons.value(i)).unwrap_or_default();
                let props: IndexMap<String, yata_cypher::Value> = json_props
                    .into_iter()
                    .map(|(k, v)| (k, json_to_cypher(&v)))
                    .collect();
                seen.insert(vid.clone(), yata_cypher::NodeRef { id: vid, labels, props });
            }
        }
        Ok(seen.into_values().collect())
    }

    /// Load all edges from LanceDB.
    pub async fn load_edges(&self) -> GraphResult<Vec<yata_cypher::RelRef>> {
        let table = match self.conn.open_table("graph_edges").execute().await {
            Ok(t) => {
                let _ = t.checkout_latest().await;
                t
            }
            Err(_) => return Ok(Vec::new()),
        };
        let stream = table
            .query()
            .execute()
            .await
            .map_err(|e| GraphError::Storage(e.to_string()))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| GraphError::Storage(e.to_string()))?;

        // last-write-wins dedup on eid.
        let mut seen: IndexMap<String, yata_cypher::RelRef> = IndexMap::new();
        for batch in &batches {
            let eids = col_str(batch, "eid");
            let srcs = col_str(batch, "src");
            let dsts = col_str(batch, "dst");
            let rel_types = col_str(batch, "rel_type");
            let props_jsons = col_str(batch, "props_json");
            let (Some(eids), Some(srcs), Some(dsts), Some(rel_types), Some(props_jsons)) =
                (eids, srcs, dsts, rel_types, props_jsons)
            else {
                continue;
            };
            for i in 0..batch.num_rows() {
                let eid = eids.value(i).to_owned();
                let json_props: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(props_jsons.value(i)).unwrap_or_default();
                let props: IndexMap<String, yata_cypher::Value> = json_props
                    .into_iter()
                    .map(|(k, v)| (k, json_to_cypher(&v)))
                    .collect();
                seen.insert(eid.clone(), yata_cypher::RelRef {
                    id: eid,
                    src: srcs.value(i).to_owned(),
                    dst: dsts.value(i).to_owned(),
                    rel_type: rel_types.value(i).to_owned(),
                    props,
                });
            }
        }
        Ok(seen.into_values().collect())
    }

    /// Load all vertices + edges into a QueryableGraph for Cypher queries.
    pub async fn to_memory_graph(&self) -> GraphResult<QueryableGraph> {
        use yata_cypher::Graph;
        let nodes = self.load_vertices().await?;
        let rels = self.load_edges().await?;
        let mut g = yata_cypher::MemoryGraph::new();
        for node in nodes {
            g.add_node(node);
        }
        for rel in rels {
            g.add_rel(rel);
        }
        Ok(QueryableGraph(g))
    }

    /// Write pre-built Arrow RecordBatch directly to `graph_vertices`.
    ///
    /// Schema must match `graph_arrow::vertices_schema()` (vid, labels_json, props_json, created_ns).
    /// Skips NodeRef→Arrow conversion — zero-copy path for Arrow Flight `do_put`.
    pub async fn write_vertices_batch(&self, batch: RecordBatch) -> GraphResult<()> {
        let schema = graph_arrow::vertices_schema();
        self.append_batch("graph_vertices", schema, batch).await
    }

    /// Write pre-built Arrow RecordBatch directly to `graph_edges` + auto-generate `graph_adj`.
    ///
    /// Schema must match `graph_arrow::edges_schema()` (eid, src, dst, rel_type, props_json, created_ns).
    /// Adjacency index rows are derived from the edge batch columns.
    pub async fn write_edges_batch(&self, batch: RecordBatch) -> GraphResult<()> {
        let adj_batch = self.derive_adj_from_edge_batch(&batch)?;

        let edge_schema = graph_arrow::edges_schema();
        self.append_batch("graph_edges", edge_schema, batch).await?;

        let adj_schema = graph_arrow::adj_schema();
        self.append_batch("graph_adj", adj_schema, adj_batch).await
    }

    /// Derive `graph_adj` rows from an edge RecordBatch.
    /// For each edge: 2 adjacency rows (OUT from src, IN to dst).
    fn derive_adj_from_edge_batch(&self, batch: &RecordBatch) -> GraphResult<RecordBatch> {
        let eids = col_str(batch, "eid")
            .ok_or_else(|| GraphError::Schema("missing eid column".into()))?;
        let srcs = col_str(batch, "src")
            .ok_or_else(|| GraphError::Schema("missing src column".into()))?;
        let dsts = col_str(batch, "dst")
            .ok_or_else(|| GraphError::Schema("missing dst column".into()))?;
        let rel_types = col_str(batch, "rel_type")
            .ok_or_else(|| GraphError::Schema("missing rel_type column".into()))?;
        let created_ns_col = batch
            .column_by_name("created_ns")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| GraphError::Schema("missing created_ns column".into()))?;

        let n = batch.num_rows();
        let mut adj_vids = Vec::with_capacity(n * 2);
        let mut adj_dirs = Vec::with_capacity(n * 2);
        let mut adj_labels = Vec::with_capacity(n * 2);
        let mut adj_neighbors = Vec::with_capacity(n * 2);
        let mut adj_eids = Vec::with_capacity(n * 2);
        let mut adj_nses = Vec::with_capacity(n * 2);

        for i in 0..n {
            let eid = eids.value(i);
            let src = srcs.value(i);
            let dst = dsts.value(i);
            let rt = rel_types.value(i);
            let ns = created_ns_col.value(i);

            // OUT direction
            adj_vids.push(src);
            adj_dirs.push("OUT");
            adj_labels.push(rt);
            adj_neighbors.push(dst);
            adj_eids.push(eid);
            adj_nses.push(ns);

            // IN direction
            adj_vids.push(dst);
            adj_dirs.push("IN");
            adj_labels.push(rt);
            adj_neighbors.push(src);
            adj_eids.push(eid);
            adj_nses.push(ns);
        }

        let adj_schema = graph_arrow::adj_schema();
        RecordBatch::try_new(
            adj_schema,
            vec![
                Arc::new(StringArray::from(adj_vids)) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(adj_dirs)) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(adj_labels)) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(adj_neighbors)) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(adj_eids)) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(adj_nses)) as Arc<dyn arrow_array::Array>,
            ],
        )
        .map_err(|e| GraphError::Storage(e.to_string()))
    }

    /// Compute graph delta between before/after state and write to Lance.
    ///
    /// Efficient batch write: collects all new/modified nodes and edges into
    /// single RecordBatches, then appends once per table.
    /// Deleted entities are written as tombstones (props_json = `{"_deleted":true}`).
    pub async fn write_delta(
        &self,
        before_nodes: &indexmap::IndexMap<String, yata_cypher::NodeRef>,
        before_edges: &indexmap::IndexMap<String, yata_cypher::RelRef>,
        after_graph: &yata_cypher::MemoryGraph,
    ) -> GraphResult<DeltaStats> {
        use yata_cypher::Graph;

        let after_nodes = after_graph.nodes();
        let after_edges = after_graph.rels();

        // Classify nodes: new, modified, deleted
        let mut upsert_nodes: Vec<yata_cypher::NodeRef> = Vec::new();
        let mut deleted_node_count: usize = 0;

        for node in &after_nodes {
            match before_nodes.get(&node.id) {
                None => upsert_nodes.push(node.clone()),
                Some(old) if old != node => upsert_nodes.push(node.clone()),
                _ => {}
            }
        }

        // Deleted nodes: in before but not in after
        let mut tombstone_nodes: Vec<yata_cypher::NodeRef> = Vec::new();
        for (id, _) in before_nodes {
            if after_graph.node_by_id(id).is_none() {
                deleted_node_count += 1;
                tombstone_nodes.push(yata_cypher::NodeRef {
                    id: id.clone(),
                    labels: vec![],
                    props: {
                        let mut m = indexmap::IndexMap::new();
                        m.insert("_deleted".into(), yata_cypher::Value::Bool(true));
                        m
                    },
                });
            }
        }

        // Classify edges
        let mut upsert_edges: Vec<yata_cypher::RelRef> = Vec::new();
        let mut deleted_edge_count: usize = 0;

        for edge in &after_edges {
            match before_edges.get(&edge.id) {
                None => upsert_edges.push(edge.clone()),
                Some(old) if old != edge => upsert_edges.push(edge.clone()),
                _ => {}
            }
        }

        let mut tombstone_edges: Vec<yata_cypher::RelRef> = Vec::new();
        for (id, old) in before_edges {
            if after_graph.rel_by_id(id).is_none() {
                deleted_edge_count += 1;
                tombstone_edges.push(yata_cypher::RelRef {
                    id: id.clone(),
                    src: old.src.clone(),
                    dst: old.dst.clone(),
                    rel_type: old.rel_type.clone(),
                    props: {
                        let mut m = indexmap::IndexMap::new();
                        m.insert("_deleted".into(), yata_cypher::Value::Bool(true));
                        m
                    },
                });
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

        // Merge upserts + tombstones and batch write
        upsert_nodes.extend(tombstone_nodes);
        upsert_edges.extend(tombstone_edges);

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

    /// Write vertices with embedding vectors to graph_vertices table.
    ///
    /// Extracts the embedding property from each node's props, builds a FixedSizeList column,
    /// and writes with the extended schema including the `embedding` column.
    pub async fn write_vertices_with_embeddings(
        &self,
        nodes: &[yata_cypher::NodeRef],
        embedding_key: &str,
        dim: usize,
    ) -> GraphResult<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        let vids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
        let labels_jsons: Vec<String> = nodes
            .iter()
            .map(|n| serde_json::to_string(&n.labels).unwrap_or_else(|_| "[]".into()))
            .collect();
        // Build props_json without the embedding key (stored in dedicated column)
        let props_jsons: Vec<String> = nodes
            .iter()
            .map(|n| {
                let m: serde_json::Map<String, serde_json::Value> = n
                    .props
                    .iter()
                    .filter(|(k, _)| k.as_str() != embedding_key)
                    .map(|(k, v)| (k.clone(), cypher_to_json(v)))
                    .collect();
                serde_json::to_string(&m).unwrap_or_else(|_| "{}".into())
            })
            .collect();
        let created_nses = vec![now_ns; nodes.len()];

        // Build embedding FixedSizeList column
        let mut all_values: Vec<f32> = Vec::with_capacity(nodes.len() * dim);
        let mut valid = vec![true; nodes.len()];
        for (i, node) in nodes.iter().enumerate() {
            if let Some(val) = node.props.get(embedding_key) {
                if let Some(vec) = yata_cypher::graph::extract_f32_vec(val) {
                    if vec.len() == dim {
                        all_values.extend_from_slice(&vec);
                        continue;
                    }
                }
            }
            // Null embedding — fill zeros and mark null
            all_values.extend(std::iter::repeat(0.0f32).take(dim));
            valid[i] = false;
        }

        let values_array = Float32Array::from(all_values);
        let embedding_array = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            dim as i32,
            Arc::new(values_array),
            Some(valid.into()),
        ).map_err(|e| GraphError::Storage(e.to_string()))?;

        let schema = graph_arrow::vertices_with_embedding_schema(dim);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vids)) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(
                    labels_jsons.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(
                    props_jsons.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(created_nses)) as Arc<dyn arrow_array::Array>,
                Arc::new(embedding_array) as Arc<dyn arrow_array::Array>,
            ],
        )
        .map_err(|e| GraphError::Storage(e.to_string()))?;

        self.append_batch("graph_vertices", schema, batch).await
    }

    /// Vector search over graph_vertices using the `embedding` column.
    ///
    /// Returns nodes sorted by distance (ascending), along with distance scores.
    pub async fn vector_search_vertices(
        &self,
        query_vector: Vec<f32>,
        limit: usize,
        label_filter: Option<&str>,
        prop_filter: Option<&str>,
    ) -> GraphResult<Vec<(yata_cypher::NodeRef, f32)>> {
        let table = match self.conn.open_table("graph_vertices").execute().await {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };

        let mut search = table.vector_search(query_vector)
            .map_err(|e| GraphError::Storage(format!("vector_search setup: {e}")))?;
        search = search.column("embedding").limit(limit);

        if let Some(filter) = prop_filter {
            search = search.only_if(filter);
        }

        let stream = search
            .execute()
            .await
            .map_err(|e| GraphError::Storage(format!("vector_search exec: {e}")))?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| GraphError::Storage(format!("vector_search collect: {e}")))?;

        let mut results = Vec::new();
        for batch in &batches {
            let vids = col_str(batch, "vid");
            let labels_jsons = col_str(batch, "labels_json");
            let props_jsons = col_str(batch, "props_json");
            let distances = batch
                .column_by_name("_distance")
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>());
            let (Some(vids), Some(labels_jsons), Some(props_jsons)) =
                (vids, labels_jsons, props_jsons)
            else {
                continue;
            };
            for i in 0..batch.num_rows() {
                let vid = vids.value(i).to_owned();
                let labels: Vec<String> =
                    serde_json::from_str(labels_jsons.value(i)).unwrap_or_default();

                // Apply label filter
                if let Some(lf) = label_filter {
                    if !labels.contains(&lf.to_owned()) {
                        continue;
                    }
                }

                let json_props: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(props_jsons.value(i)).unwrap_or_default();
                let props: IndexMap<String, yata_cypher::Value> = json_props
                    .into_iter()
                    .map(|(k, v)| (k, json_to_cypher(&v)))
                    .collect();
                let distance = distances.map(|d| d.value(i)).unwrap_or(0.0);
                results.push((
                    yata_cypher::NodeRef { id: vid, labels, props },
                    distance,
                ));
            }
        }
        Ok(results)
    }

    /// Create an IVF-PQ vector index on the `embedding` column of graph_vertices.
    pub async fn create_embedding_index(&self) -> GraphResult<()> {
        let table = self.conn.open_table("graph_vertices")
            .execute()
            .await
            .map_err(|e| GraphError::Storage(e.to_string()))?;
        table.create_index(&["embedding"], lancedb::index::Index::Auto)
            .execute()
            .await
            .map_err(|e| GraphError::Storage(format!("create_embedding_index: {e}")))?;
        Ok(())
    }

    /// Append-only write (used for `graph_adj`).
    async fn append_batch(
        &self,
        table_name: &str,
        schema: Arc<Schema>,
        batch: RecordBatch,
    ) -> GraphResult<()> {
        let reader = RecordBatchIterator::new(
            std::iter::once(Ok::<_, arrow::error::ArrowError>(batch)),
            schema,
        );
        match self.conn.open_table(table_name).execute().await {
            Ok(table) => {
                table
                    .add(reader)
                    .execute()
                    .await
                    .map_err(|e| GraphError::Storage(e.to_string()))?;
            }
            Err(_) => {
                self.conn
                    .create_table(table_name, reader)
                    .execute()
                    .await
                    .map_err(|e| GraphError::Storage(e.to_string()))?;
            }
        }
        Ok(())
    }

}

// ---- Helper: column accessor -------------------------------------------

fn col_str<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
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
            m.iter().map(|(k, v)| (k.clone(), cypher_to_json(v))).collect(),
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
                    n.labels.iter().map(|l| serde_json::Value::String(l.clone())).collect(),
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
            obj.insert("__type".into(), serde_json::Value::String(r.rel_type.clone()));
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
                        let json = serde_json::to_string(&cypher_to_json(&val))
                            .unwrap_or_default();
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
