//! TCP transport for Raft node-to-node RPC.
//!
//! Each message is framed as: [4-byte len LE][JSON payload].
//! Connections are established per-request (no persistent connection pool in Phase 1).

use crate::network::{PeerAddr, RaftMessage, RaftTransport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// TCP-based Raft transport. Sends JSON-encoded messages over TCP.
pub struct TcpRaftTransport;

impl TcpRaftTransport {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TcpRaftTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl RaftTransport for TcpRaftTransport {
    async fn send(&self, target: &PeerAddr, msg: RaftMessage) -> Result<RaftMessage, String> {
        let mut stream = TcpStream::connect(&target.addr)
            .await
            .map_err(|e| format!("tcp connect {}: {e}", target.addr))?;

        // Serialize request
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| format!("serialize: {e}"))?;
        let len = payload.len() as u32;
        stream
            .write_all(&len.to_le_bytes())
            .await
            .map_err(|e| format!("write len: {e}"))?;
        stream
            .write_all(&payload)
            .await
            .map_err(|e| format!("write payload: {e}"))?;
        stream
            .flush()
            .await
            .map_err(|e| format!("flush: {e}"))?;

        // Read response
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| format!("read resp len: {e}"))?;
        let resp_len = u32::from_le_bytes(len_buf) as usize;

        let mut resp_buf = vec![0u8; resp_len];
        stream
            .read_exact(&mut resp_buf)
            .await
            .map_err(|e| format!("read resp: {e}"))?;

        serde_json::from_slice(&resp_buf)
            .map_err(|e| format!("deserialize resp: {e}"))
    }
}

/// TCP listener that accepts incoming Raft RPCs and dispatches to a RaftNode.
pub struct TcpRaftListener;

impl TcpRaftListener {
    /// Start listening on `bind_addr` and dispatch messages to `node`.
    /// Blocks forever. Spawn this in a tokio task.
    pub async fn run(
        bind_addr: &str,
        node: std::sync::Arc<crate::node::RaftNode>,
    ) -> Result<(), String> {
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .map_err(|e| format!("bind {bind_addr}: {e}"))?;

        tracing::info!(addr = %bind_addr, "raft TCP listener started");

        loop {
            let (mut stream, peer) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("raft accept error: {e}");
                    continue;
                }
            };

            let node = node.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(&mut stream, &node).await {
                    tracing::debug!(peer = %peer, "raft connection error: {e}");
                }
            });
        }
    }
}

async fn handle_connection(
    stream: &mut TcpStream,
    node: &crate::node::RaftNode,
) -> Result<(), String> {
    // Read request
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| format!("read len: {e}"))?;
    let msg_len = u32::from_le_bytes(len_buf) as usize;

    let mut msg_buf = vec![0u8; msg_len];
    stream
        .read_exact(&mut msg_buf)
        .await
        .map_err(|e| format!("read msg: {e}"))?;

    let msg: RaftMessage = serde_json::from_slice(&msg_buf)
        .map_err(|e| format!("deserialize: {e}"))?;

    // Dispatch to node
    let resp = node.handle_message(msg).await;

    // Write response
    let resp_payload = serde_json::to_vec(&resp)
        .map_err(|e| format!("serialize resp: {e}"))?;
    let resp_len = resp_payload.len() as u32;
    stream
        .write_all(&resp_len.to_le_bytes())
        .await
        .map_err(|e| format!("write resp len: {e}"))?;
    stream
        .write_all(&resp_payload)
        .await
        .map_err(|e| format!("write resp: {e}"))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("flush resp: {e}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::StateMachineApplier;
    use std::sync::Arc;

    struct MockApplier;

    #[async_trait::async_trait]
    impl StateMachineApplier for MockApplier {
        async fn apply_publish(&self, _: &str, _: u32, _: &str, _: Option<&str>, _: &str, _: Option<&str>, ts: i64, _: &[u8]) -> Result<(u64, i64), String> { Ok((0, ts)) }
        async fn apply_create_topic(&self, _: &str, _: u32, _: u32, _: Option<u64>) -> Result<(), String> { Ok(()) }
        async fn apply_delete_topic(&self, _: &str) -> Result<(), String> { Ok(()) }
        async fn apply_commit_offset(&self, _: &str, _: &str, _: u32, _: u64) -> Result<(), String> { Ok(()) }
    }

    #[tokio::test]
    async fn test_tcp_roundtrip() {
        let node = Arc::new(crate::node::RaftNode::new(1, vec![], Arc::new(MockApplier)));
        node.start().await.unwrap();

        let listener_node = node.clone();
        let addr = "127.0.0.1:0";
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let actual_addr = listener.local_addr().unwrap();

        // Spawn listener
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_connection(&mut stream, &listener_node).await.unwrap();
        });

        // Send a vote request
        let transport = TcpRaftTransport::new();
        let peer = PeerAddr {
            node_id: 1,
            addr: actual_addr.to_string(),
        };
        let msg = RaftMessage::VoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        let resp = transport.send(&peer, msg).await.unwrap();
        match resp {
            RaftMessage::VoteResponse { term, .. } => assert!(term >= 1),
            other => panic!("unexpected response: {:?}", other),
        }
    }
}
