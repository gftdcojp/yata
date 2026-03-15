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
