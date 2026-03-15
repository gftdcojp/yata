use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Ticket encoding for Arrow Flight do_get requests.
///
/// Serialized as JSON and stored in `Ticket.ticket`.
/// Clients build this struct and call `to_bytes()` to get the ticket bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanTicket {
    /// Lance table name (e.g. "yata_messages").
    pub table: String,
    /// Optional SQL-style filter expression (e.g. `"stream_id = 'foo'"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    /// Columns to project. None = all columns.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection: Option<Vec<String>>,
    /// Maximum number of rows to return.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
    /// Row offset (for pagination).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
}

impl ScanTicket {
    pub fn table(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            filter: None,
            projection: None,
            limit: None,
            offset: None,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_vec(self).unwrap_or_default())
    }

    pub fn from_bytes(b: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(b)
    }
}

/// Ticket for Cypher queries routed through Arrow Flight do_get.
///
/// The `kind = "cypher"` field distinguishes this from `ScanTicket` in `AnyTicket::from_bytes`.
/// Result is an Arrow IPC stream where every column is `Utf8` (JSON-encoded Cypher value).
/// Column names come from the Cypher RETURN clause — sent once in the schema, not per row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CypherTicket {
    /// Discriminant — must be "cypher".
    pub kind: String,
    /// Cypher query string.
    pub cypher: String,
    /// Query parameters as (name, JSON-encoded value) pairs.
    #[serde(default)]
    pub params: Vec<(String, String)>,
    /// Graph store base URI override. Falls back to server-configured graph_base_uri.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph_uri: Option<String>,
}

impl CypherTicket {
    pub fn new(cypher: impl Into<String>, params: Vec<(String, String)>) -> Self {
        Self {
            kind: "cypher".into(),
            cypher: cypher.into(),
            params,
            graph_uri: None,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_vec(self).unwrap_or_default())
    }

    pub fn from_bytes(b: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(b)
    }
}

/// Unified ticket — dispatches to Lance scan or Cypher query based on the `kind` field.
pub enum AnyTicket {
    Scan(ScanTicket),
    Cypher(CypherTicket),
}

impl AnyTicket {
    /// Parse ticket bytes, routing by presence of `"kind":"cypher"`.
    pub fn from_bytes(b: &[u8]) -> Result<Self, serde_json::Error> {
        let v: serde_json::Value = serde_json::from_slice(b)?;
        if v.get("kind").and_then(|k| k.as_str()) == Some("cypher") {
            Ok(AnyTicket::Cypher(serde_json::from_value(v)?))
        } else {
            Ok(AnyTicket::Scan(serde_json::from_value(v)?))
        }
    }
}

// ---- Write tickets (do_put / do_action) ---------------------------------

/// Ticket for `do_put`: direct Lance table batch append.
///
/// Client sends Arrow IPC RecordBatches matching the target table's schema.
/// Server collects all batches and appends in a single Lance write (batch-optimized).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteTicket {
    /// Discriminant — must be "write".
    pub kind: String,
    /// Target Lance table name (e.g. "yata_messages").
    pub table: String,
}

impl WriteTicket {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            kind: "write".into(),
            table: table.into(),
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_vec(self).unwrap_or_default())
    }

    pub fn from_bytes(b: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(b)
    }
}

/// Ticket for `do_put`: batch write to graph tables (vertices or edges).
///
/// - `target = "vertices"`: client sends RecordBatches matching `graph_vertices` schema.
/// - `target = "edges"`: client sends RecordBatches matching `graph_edges` schema.
///   Server auto-generates `graph_adj` rows from edge data.
///
/// Arrow-optimized: all batches collected → single Lance append per table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphWriteTicket {
    /// Discriminant — must be "graph_write".
    pub kind: String,
    /// Target: "vertices" or "edges".
    pub target: String,
    /// Graph store base URI override. Falls back to server-configured graph_base_uri.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph_uri: Option<String>,
}

impl GraphWriteTicket {
    pub fn vertices() -> Self {
        Self {
            kind: "graph_write".into(),
            target: "vertices".into(),
            graph_uri: None,
        }
    }

    pub fn edges() -> Self {
        Self {
            kind: "graph_write".into(),
            target: "edges".into(),
            graph_uri: None,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_vec(self).unwrap_or_default())
    }

    pub fn from_bytes(b: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(b)
    }
}

/// Ticket for `do_action` type `"cypher_mutate"`: execute a Cypher mutation.
///
/// Flow: load graph from Lance → execute Cypher (CREATE/MERGE/DELETE/SET) →
/// diff before/after MemoryGraph → batch write delta vertices + edges to Lance.
///
/// Returns `CypherMutateResult` as JSON in the action result body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CypherMutateTicket {
    /// Cypher mutation string (e.g. "CREATE (n:Person {name: $name})").
    pub cypher: String,
    /// Query parameters as (name, JSON-encoded value) pairs.
    #[serde(default)]
    pub params: Vec<(String, String)>,
    /// Graph store base URI override. Falls back to server-configured graph_base_uri.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph_uri: Option<String>,
}

impl CypherMutateTicket {
    pub fn new(cypher: impl Into<String>, params: Vec<(String, String)>) -> Self {
        Self {
            cypher: cypher.into(),
            params,
            graph_uri: None,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_vec(self).unwrap_or_default())
    }

    pub fn from_bytes(b: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(b)
    }
}

/// Result returned by `cypher_mutate` action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CypherMutateResult {
    pub nodes_created: usize,
    pub nodes_modified: usize,
    pub nodes_deleted: usize,
    pub edges_created: usize,
    pub edges_modified: usize,
    pub edges_deleted: usize,
}

/// Unified put-ticket — dispatches `do_put` to Lance write or graph batch write.
pub enum AnyPutTicket {
    Write(WriteTicket),
    GraphWrite(GraphWriteTicket),
}

impl AnyPutTicket {
    pub fn from_bytes(b: &[u8]) -> Result<Self, serde_json::Error> {
        let v: serde_json::Value = serde_json::from_slice(b)?;
        match v.get("kind").and_then(|k| k.as_str()) {
            Some("graph_write") => Ok(AnyPutTicket::GraphWrite(serde_json::from_value(v)?)),
            _ => Ok(AnyPutTicket::Write(serde_json::from_value(v)?)),
        }
    }
}
