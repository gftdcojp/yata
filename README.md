# yata

Lance-backed graph database and event store with Raft consensus.

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
│  yata-engine  yata-gie                                     │
│  yata-bolt (Bolt v4)                                        │
├─────────────────────────────────────────────────────────────┤
│                   Layer 4: Server                            │
│  yata-server                                                │
├─────────────────────────────────────────────────────────────┤
│                   Layer 3: Engine                            │
│  yata-engine  yata-gie  yata-vex  yata-s3                   │
├─────────────────────────────────────────────────────────────┤
│                   Layer 2: Graph                             │
│  yata-cypher             yata-lance                        │
├─────────────────────────────────────────────────────────────┤
│                 Layer 1: Core Primitives                     │
│  yata-arrow  yata-object                                    │
├─────────────────────────────────────────────────────────────┤
│                   Layer 0: Zero Deps                         │
│  yata-core    yata-grin                                     │
└─────────────────────────────────────────────────────────────┘
```

## Crates

| Crate | Description |
|---|---|
| **yata-core** | Core types (GlobalVid, PartitionId), error handling |
| **yata-grin** | GRIN storage-agnostic graph traits (Topology, Property, Schema, Mutable) |
| **yata-arrow** | Arrow IPC encode/decode |
| **yata-object** | Object storage abstraction (CAS + S3 write-through) |
| **yata-cypher** | Pure-Rust Cypher parser and execution engine |
| **yata-gie** | Graph Interactive Engine (IR operators, push-based executor) |
| **yata-engine** | TieredGraphEngine — Lance-backed cold start, WAL projection, partition routing |
| **yata-lance** | Lance-table-compatible persistence and vector store (fragments, manifests, typed Arrow schema) |
| **yata-s3** | S3/R2 adapter (sync ureq+rustls, SigV4) |
| **yata-vex** | Vector index (IVF_PQ + DiskANN) |
| **yata-server** | XRPC API server (Cypher, mergeRecord, triggerSnapshot) |
| **yata-bench** | Benchmarks |
| **yata-bolt** | Bolt v4 wire protocol (Neo4j driver compatible) |
| **yata-coordinator** | Label-based graph partitioning with Rayon parallel execution |
| **yata-at** | AT Protocol types, Firehose client, bridge |
| **yata-gateway** | Standalone query gateway: Bolt + Neo4j Query API v2 |
| **yata-cli** | CLI tool for broker administration |

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

MutableCSR micro-benchmarks:

| Operation | Latency |
|---|---|
| Property lookup | 304 ns |
| 1-hop traversal | 463 ns |
| Write throughput | 1.07M ops/sec |

## License

Apache-2.0 — see [LICENSE](LICENSE).
