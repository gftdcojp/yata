//! Bolt v4 TCP server backed by `LanceGraphStore` + `yata-cypher`.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use yata_graph::LanceGraphStore;

use crate::packstream::{self, BoltStruct, Value};

/// Bolt magic preamble: 0x6060B017
const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

/// We support Bolt v4.4 (major=4, minor=4, range=0).
const BOLT_VERSION_RESPONSE: [u8; 4] = [0x00, 0x00, 0x04, 0x04];
const BOLT_VERSION_NONE: [u8; 4] = [0x00, 0x00, 0x00, 0x00];

pub struct BoltServer {
    graph: Arc<LanceGraphStore>,
}

impl BoltServer {
    pub fn new(graph: Arc<LanceGraphStore>) -> Self {
        Self { graph }
    }

    pub async fn listen(&self, addr: SocketAddr) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!(%addr, "yata-bolt listening");

        loop {
            let (stream, peer) = listener.accept().await?;
            tracing::debug!(%peer, "bolt connection accepted");
            let graph = self.graph.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, graph).await {
                    tracing::warn!(%peer, "bolt session error: {e}");
                }
            });
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    graph: Arc<LanceGraphStore>,
) -> anyhow::Result<()> {
    // ── Handshake ────────────────────────────────────────────────────────────
    let mut preamble = [0u8; 20]; // 4 magic + 4×4 version proposals
    stream.read_exact(&mut preamble).await?;

    if preamble[..4] != BOLT_MAGIC {
        anyhow::bail!("invalid bolt magic");
    }

    // Check version proposals (4 slots of 4 bytes each)
    let mut accepted = false;
    for i in 0..4 {
        let off = 4 + i * 4;
        let major = preamble[off + 3];
        if major == 4 {
            accepted = true;
            break;
        }
    }

    if accepted {
        stream.write_all(&BOLT_VERSION_RESPONSE).await?;
    } else {
        stream.write_all(&BOLT_VERSION_NONE).await?;
        return Ok(());
    }

    // ── Message loop ─────────────────────────────────────────────────────────
    let mut read_buf = BytesMut::with_capacity(8192);

    loop {
        if stream.read_buf(&mut read_buf).await? == 0 {
            return Ok(());
        }

        while let Some(msg_data) = packstream::read_chunked_message(&mut read_buf) {
            if msg_data.is_empty() {
                continue;
            }

            let bolt_struct = packstream::decode_struct(&msg_data)?;
            match bolt_struct.signature {
                packstream::SIG_HELLO | packstream::SIG_LOGON => {
                    let meta = vec![
                        ("server".to_string(), Value::String("yata-bolt/0.1.0".to_string())),
                        ("connection_id".to_string(), Value::String("yata-1".to_string())),
                    ];
                    let resp =
                        packstream::encode_struct_message(packstream::SIG_SUCCESS, &[Value::Map(meta)]);
                    stream.write_all(&resp).await?;
                }
                packstream::SIG_RUN => {
                    handle_run(&mut stream, &graph, &bolt_struct).await?;
                }
                packstream::SIG_PULL => {
                    // Records already streamed in handle_run — send final SUCCESS.
                    let meta = vec![
                        ("has_more".to_string(), Value::Bool(false)),
                        ("t_last".to_string(), Value::Int(0)),
                        ("type".to_string(), Value::String("r".to_string())),
                    ];
                    let resp = packstream::encode_struct_message(
                        packstream::SIG_SUCCESS,
                        &[Value::Map(meta)],
                    );
                    stream.write_all(&resp).await?;
                }
                packstream::SIG_DISCARD
                | packstream::SIG_BEGIN
                | packstream::SIG_COMMIT
                | packstream::SIG_ROLLBACK
                | packstream::SIG_RESET => {
                    let resp = packstream::encode_struct_message(
                        packstream::SIG_SUCCESS,
                        &[Value::Map(vec![])],
                    );
                    stream.write_all(&resp).await?;
                }
                packstream::SIG_ROUTE => {
                    let meta = vec![
                        ("rt".to_string(), Value::Map(vec![
                            ("ttl".to_string(), Value::Int(300)),
                            ("servers".to_string(), Value::List(vec![
                                Value::Map(vec![
                                    ("addresses".to_string(), Value::List(vec![Value::String("localhost:7687".to_string())])),
                                    ("role".to_string(), Value::String("WRITE".to_string())),
                                ]),
                                Value::Map(vec![
                                    ("addresses".to_string(), Value::List(vec![Value::String("localhost:7687".to_string())])),
                                    ("role".to_string(), Value::String("READ".to_string())),
                                ]),
                                Value::Map(vec![
                                    ("addresses".to_string(), Value::List(vec![Value::String("localhost:7687".to_string())])),
                                    ("role".to_string(), Value::String("ROUTE".to_string())),
                                ]),
                            ])),
                            ("db".to_string(), Value::String("yata".to_string())),
                        ])),
                    ];
                    let resp = packstream::encode_struct_message(packstream::SIG_SUCCESS, &[Value::Map(meta)]);
                    stream.write_all(&resp).await?;
                }
                packstream::SIG_GOODBYE => {
                    return Ok(());
                }
                sig => {
                    tracing::warn!(sig, "unhandled bolt message");
                    let resp = packstream::encode_struct_message(
                        packstream::SIG_IGNORED,
                        &[],
                    );
                    stream.write_all(&resp).await?;
                }
            }
        }
    }
}

/// Handle RUN: load graph → execute Cypher → stream RECORD rows.
async fn handle_run(
    stream: &mut TcpStream,
    graph: &LanceGraphStore,
    bolt_struct: &BoltStruct,
) -> anyhow::Result<()> {
    let cypher = bolt_struct
        .fields
        .first()
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // Extract parameters (field[1] is params map)
    let params: Vec<(String, String)> = bolt_struct
        .fields
        .get(1)
        .and_then(|v| v.as_map())
        .map(|entries| {
            entries
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect()
        })
        .unwrap_or_default();

    tracing::debug!(cypher, "bolt RUN");

    // Load graph from Lance and execute Cypher
    let query_result = graph.to_memory_graph().await;
    match query_result {
        Ok(mut qg) => {
            use yata_cypher::Graph;

            // Snapshot before state for delta detection on mutations
            let before_nodes: indexmap::IndexMap<String, yata_cypher::NodeRef> = qg
                .0
                .nodes()
                .into_iter()
                .map(|n| (n.id.clone(), n))
                .collect();
            let before_edges: indexmap::IndexMap<String, yata_cypher::RelRef> = qg
                .0
                .rels()
                .into_iter()
                .map(|e| (e.id.clone(), e))
                .collect();

            match qg.query(cypher, &params) {
            Ok(rows) => {
                // Write-back delta for mutations
                let upper = cypher.to_uppercase();
                if upper.contains("CREATE")
                    || upper.contains("MERGE")
                    || upper.contains("DELETE")
                    || upper.contains("SET ")
                    || upper.contains("REMOVE ")
                {
                    match graph
                        .write_delta(&before_nodes, &before_edges, &qg.0)
                        .await
                    {
                        Ok(stats) => tracing::info!(?stats, "bolt: graph delta written"),
                        Err(e) => tracing::warn!("bolt: write_delta failed: {e}"),
                    }
                }

                // Extract column names from first row (or empty)
                let columns: Vec<String> = rows
                    .first()
                    .map(|row| row.iter().map(|(col, _)| col.clone()).collect())
                    .unwrap_or_default();

                let fields_val: Vec<Value> = columns
                    .iter()
                    .map(|c| Value::String(c.clone()))
                    .collect();

                // SUCCESS with fields metadata
                let meta = vec![
                    ("fields".to_string(), Value::List(fields_val)),
                    ("t_first".to_string(), Value::Int(0)),
                ];
                let resp =
                    packstream::encode_struct_message(packstream::SIG_SUCCESS, &[Value::Map(meta)]);
                stream.write_all(&resp).await?;

                // Stream RECORD rows
                for row in &rows {
                    let record_values: Vec<Value> = row
                        .iter()
                        .map(|(_, val)| Value::String(val.clone()))
                        .collect();
                    let record = packstream::encode_struct_message(
                        packstream::SIG_RECORD,
                        &[Value::List(record_values)],
                    );
                    stream.write_all(&record).await?;
                }
            }
            Err(e) => {
                send_failure(stream, "Neo.ClientError.Statement.SyntaxError", &e.to_string())
                    .await?;
            }
        }},
        Err(e) => {
            send_failure(stream, "Neo.DatabaseError.General.UnknownError", &e.to_string())
                .await?;
        }
    }

    Ok(())
}

async fn send_failure(
    stream: &mut TcpStream,
    code: &str,
    message: &str,
) -> anyhow::Result<()> {
    let meta = vec![
        ("code".to_string(), Value::String(code.to_string())),
        ("message".to_string(), Value::String(message.to_string())),
    ];
    let resp = packstream::encode_struct_message(packstream::SIG_FAILURE, &[Value::Map(meta)]);
    stream.write_all(&resp).await?;
    Ok(())
}

fn value_to_json(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => serde_json::to_string(s).unwrap_or_else(|_| s.clone()),
        Value::List(items) => {
            let inner: Vec<String> = items.iter().map(value_to_json).collect();
            format!("[{}]", inner.join(","))
        }
        Value::Map(entries) => {
            let inner: Vec<String> = entries
                .iter()
                .map(|(k, v)| format!("{}:{}", serde_json::to_string(k).unwrap_or_default(), value_to_json(v)))
                .collect();
            format!("{{{}}}", inner.join(","))
        }
    }
}
