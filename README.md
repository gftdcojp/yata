# yata

Arrow-native graph database and event store with Raft consensus.

```
┌─────────────────────────────────────────────────────────────┐
│                     Layer 7: Binaries                       │
│  yata-gateway    yata-cli    yata-bench                     │
├─────────────────────────────────────────────────────────────┤
│                  Layer 6: Composite Services                │
│  yata-server (embedded broker)    yata-at (AT Protocol)     │
├─────────────────────────────────────────────────────────────┤
│                    Layer 5: Sharding                        │
│  yata-coordinator (label-based partitioning, Rayon)         │
├─────────────────────────────────────────────────────────────┤
│                  Layer 4: Graph Engine                      │
│  yata-engine  yata-gie  yata-cdc  yata-mdag                │
│  yata-flight (Flight SQL)    yata-bolt (Bolt v4)            │
├─────────────────────────────────────────────────────────────┤
│                   Layer 3: Graph + KV                       │
│  yata-store (MutableCSR)  yata-graph (Lance)  yata-kv       │
│  yata-signal (Signal Protocol)                              │
├─────────────────────────────────────────────────────────────┤
│                    Layer 2: Storage                         │
│  yata-cypher  yata-log  yata-b2  yata-lance  yata-client    │
├─────────────────────────────────────────────────────────────┤
│                 Layer 1: Core Primitives                    │
│  yata-arrow  yata-cbor  yata-cas  yata-object  yata-ocel    │
├─────────────────────────────────────────────────────────────┤
│                   Layer 0: Zero Deps                        │
│  yata-core    yata-raft    yata-grin                        │
└─────────────────────────────────────────────────────────────┘
```

## Crates

| Crate | Description |
|---|---|
| **yata-core** | Core types, error handling, and storage traits |
| **yata-raft** | Raft consensus (leader election, log replication) |
| **yata-grin** | GRIN storage-agnostic graph traits (Topology, Property, Schema, Mutable, Partitioned) |
| **yata-arrow** | Arrow IPC encode/decode and versioned SchemaRegistry |
| **yata-cbor** | CBOR serialization for AT Protocol dag-cbor |
| **yata-cas** | Content-addressable storage (Blake3) |
| **yata-object** | Object storage abstraction with tiered write-through |
| **yata-ocel** | OCEL 2.0 event log types and Arrow schema |
| **yata-cypher** | Pure-Rust Cypher parser and execution engine |
| **yata-log** | Append-only segmented event log (CRC32, compaction) |
| **yata-b2** | Backblaze B2 / S3-compatible storage client |
| **yata-lance** | LanceDB table I/O, RecordBatch conversion, vector search |
| **yata-client** | Async client API for KV, Log, and Lance stores |
| **yata-store** | MutableCSR in-memory graph (WAL, MVCC snapshots) |
| **yata-kv** | KV store with TTL, backed by append-only log |
| **yata-graph** | Lance-backed graph store with CSR cache and query LRU |
| **yata-signal** | Signal Protocol crypto (X3DH, Double Ratchet, Sender Keys) |
| **yata-gie** | Graph Interactive Engine (IR operators, push-based executor) |
| **yata-engine** | Tiered HTAP engine: HOT (CSR) → WARM (Lance) routing |
| **yata-cdc** | CDC emitter for real-time graph change propagation |
| **yata-mdag** | Merkle DAG graph sync (CBOR blocks, time-travel checkout) |
| **yata-flight** | Arrow Flight SQL service for Cypher and graph ops |
| **yata-bolt** | Bolt v4 wire protocol (Neo4j driver compatible) |
| **yata-coordinator** | Label-based graph partitioning with Rayon parallel execution |
| **yata-server** | Embedded broker with Raft consensus and Prometheus metrics |
| **yata-at** | AT Protocol types, Firehose client, bridge |
| **yata-gateway** | Standalone query gateway: Bolt + Flight SQL + Neo4j Query API v2 |
| **yata-cli** | CLI tool for broker administration |
| **yata-bench** | Integration benchmarks |

## Quick Start

```bash
cargo check --workspace
cargo test --workspace
```

## Benchmarks

Measured on 1,000 nodes / 3,000 edges graph:

| Configuration | Avg Latency | Overhead/req |
|---|---|---|
| Embedded (in-process) | 583 µs | 400 B |
| gRPC (Flight SQL) | 1,678 µs (2.9x) | 2,214 B |

MutableCSR micro-benchmarks:

| Operation | Latency |
|---|---|
| Property lookup | 304 ns |
| 1-hop traversal | 463 ns |
| Write throughput | 1.07M ops/sec |

## License

Apache-2.0 — see [LICENSE](LICENSE).
