//! Minimal Bolt v4 wire protocol server wrapping yata-cypher.
//!
//! Implements enough of the Bolt v4.x handshake + RUN/PULL to serve
//! standard Neo4j drivers (Java, Python, Go, JS) and tools like
//! Neo4j Browser / Cypher Shell.
//!
//! Wire format: <https://neo4j.com/docs/bolt/current/>

mod packstream;
mod server;

pub use server::BoltServer;

use std::net::SocketAddr;
use std::sync::Arc;

/// Start a Bolt v4 listener backed by a LanceGraphStore.
pub async fn serve(
    graph: Arc<yata_graph::LanceGraphStore>,
    addr: SocketAddr,
) -> anyhow::Result<()> {
    let server = BoltServer::new(graph);
    server.listen(addr).await
}
