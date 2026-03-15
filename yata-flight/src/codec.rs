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
