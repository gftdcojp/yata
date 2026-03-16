# packages/rust/yata

yata broker — NATS JetStream 互換 event store。magatama-host から `YataClient` / `LanceGraphStore` 経由で使われる。

## Crate 役割分担

| Crate | 役割 |
|---|---|
| `yata-core` | 共通型 (`YataError`, `PayloadRef`, traits: `AppendLog`, `KvStore`, `ObjectStorage`) + `ConsumerConfig`, `CdcEvent` |
| `yata-client` | `YataClient` trait — KV / Log / Lance への async API |
| `yata-server` | `Broker` + `BrokerBackend` — embedded broker (magatama-server に埋め込み)。NATS 有無で backend 自動切替。Prometheus metrics (`/metrics` port 9090) |
| `yata-nats` | **NATS JetStream backend** — `AppendLog`/`KvStore`/`ObjectStorage` の NATS 実装 + Arrow IPC produce/consume + `NatsLanceWriter` + `NatsConsumerGroup` + `NatsCdcPublisher`/`NatsCdcConsumer` |
| `yata-kv` | KV store (local: in-memory snapshot + append-only log + **TTL enforcement** — `ttl_expires_at_ns` lazy-check + reaper) |
| `yata-log` | Append log (local: filesystem segment files + CBOR + CRC32 + **segment rotation** + **compaction**) |
| `yata-lance` | Lance table I/O ヘルパー (Arrow RecordBatch 変換、`pub` conversion functions) + **vector search** (`vector_search()`, `create_vector_index()`) |
| `yata-arrow` | Arrow IPC encode/decode (`batch_to_ipc`, `ipc_to_batch`) + **SchemaRegistry** (versioned, backward/forward/full compatibility) |
| `yata-cypher` | **Cypher パーサ + 実行エンジン** (pure Rust, Lance 非依存。variable-hop, regex, STARTS WITH/ENDS WITH/CONTAINS) |
| `yata-graph` | **Lance-backed graph store** (`LanceGraphStore` + `QueryableGraph`) |
| `yata-flight` | Arrow Flight gRPC サービス — `ScanTicket` (Lance scan) + `CypherTicket` (Cypher via graph) + `VectorSearchTicket` (ANN) |
| `yata-at` | AT Protocol types, Firehose client, `AtFirehoseBridge` |
| `yata-signal` | Signal Protocol crypto (X3DH, Double Ratchet, Sender Keys) |

## yata-cypher / yata-graph の分離原則

```
yata-cypher          — Cypher パーサ + MemoryGraph + Executor
                       Lance・magatama を知らない。pure Rust。

yata-graph           — LanceGraphStore (graph_vertices / graph_edges / graph_adj テーブル)
                       + QueryableGraph (yata-cypher を呼ぶ thin wrapper)
                       write_vertices / write_edges で Lance に append。

magatama-host        — GraphStore trait を LanceGraphStore で実装。
  graph_host.rs        query() = load Lance → Cypher exec → delta write (mut only)
                       block_in_place で sync WIT host 関数契約を満たす。
```

**CRITICAL**: `yata-cypher` は `yata-graph` を import しない。`yata-graph` は magatama を import しない。

## yata-flight CypherTicket

```json
{"kind":"cypher","cypher":"MATCH (n:Person) RETURN n.name","params":[]}
```

- `kind="cypher"` で `AnyTicket::Cypher` にルーティング
- `execute_cypher_scan`: `LanceGraphStore` → `to_memory_graph()` → `QueryableGraph.query()` → Arrow IPC stream
- 列名はスキーマに1回だけ送信 (Shannon 最適: N行×M列×L bytes の冗長排除)
- 全列 `Utf8` (JSON エンコード値)

## WIT cypher interface (Shannon-optimal layout)

```wit
record cypher-result {
    columns: list<string>,      // 列名は1回だけ
    rows:    list<list<string>>, // values[row][col] — インデックス揃え
}
query: func(cypher: string, params: list<cypher-param>) -> result<cypher-result, string>;
```

旧 `list<cypher-row>` (列名N回繰り返し) から変更済み。TinyGo binding は `magatama-go/imports.go`。

## Graph テーブルスキーマ

| Table | 列 |
|---|---|
| `graph_vertices` | `vid`, `labels_json`, `props_json`, `created_ns` |
| `graph_edges` | `eid`, `src`, `dst`, `rel_type`, `props_json`, `created_ns` |
| `graph_adj` | `vid`, `direction` (OUT/IN), `edge_label`, `neighbor_vid`, `eid`, `created_ns` |

すべて append-only。`to_memory_graph()` でロード時に dedup は `vid`/`eid` 単位で行われる。

## yata-nats: NATS JetStream Arrow payload transport

`BrokerConfig.nats` が設定されると全 write path が NATS Arrow IPC 経由になる。

### Producer → NATS → Consumer → LanceDB

```
Producer (Broker.flush_lance / KvStore.put / ObjectStore.put)
  → Arrow IPC batch (yata_arrow::batch_to_ipc)
  → NATS publish: yata.arrow.<table> (fire-and-forget, ack pipeline)
  → Header: Yata-Lance-Table, Yata-Arrow-Rows
      ↓
NatsLanceWriter (consumer, durable: yata-lance-writer)
  → subscribe: yata.arrow.>  (WorkQueue retention)
  → batch accumulation: 4096 rows or 1s flush interval
  → concat_batches → lance_conn.open_table(table).add(merged_batch)
  → table handle cache (HashMap)
```

### Subject → Lance テーブル mapping

| NATS Subject | Lance Table | Producer |
|---|---|---|
| `yata.arrow.yata_events` | `yata_events` | `flush_lance` |
| `yata.arrow.yata_objects` | `yata_objects` | `flush_lance` |
| `yata.arrow.yata_event_object_edges` | `yata_event_object_edges` | `flush_lance` |
| `yata.arrow.yata_object_object_edges` | `yata_object_object_edges` | `flush_lance` |
| `yata.arrow.yata_messages` | `yata_messages` | `publish_arrow` |
| `yata.arrow.kv_history` | `yata_kv_history` | `NatsKvStore` (buffered 64 entries / 500ms) |
| `yata.arrow.blobs` | `yata_blobs` | `NatsObjectStore` |
| `yata.arrow.graph_vertices` | `graph_vertices` | graph write path |
| `yata.arrow.graph_edges` | `graph_edges` | graph write path |

### 最適化ルール (CRITICAL)

- **1行/batch 禁止**: Arrow IPC schema overhead は ~1.9KB/batch。1行 batch は 2% efficiency。最低 64 行で batch すること
- **NatsLanceWriter の batch accumulation**: 4096行 or 1秒で flush。1 msg = 1 Lance `add()` は fragment 爆発 (183x 性能差)
- **Publisher fire-and-forget**: `publish_batch()` は ack を await しない。durability は JetStream が保証
- **Table handle cache**: `open_table` を毎回呼ばない。`HashMap<String, lancedb::Table>` でキャッシュ
- **Bucket/stream ensure cache**: `NatsKvStore` は bucket handle を `HashMap` でキャッシュ

### Dual-write architecture (KV / Object)

`NatsKvStore`: JetStream KV (point read/watch) + Arrow IPC batch (Lance history)
`NatsObjectStore`: JetStream Object Store (blob get/put) + Arrow IPC manifest (Lance index)

## KV TTL Enforcement

- `KvPutRequest.ttl_secs` → `KvEntry.ttl_expires_at_ns` (absolute nanosecond timestamp)
- `get()` lazy-check: expired entries return `None`
- Background reaper (5s interval) sweeps all buckets and deletes expired entries
- Lance `yata_kv_history` includes `ttl_expires_at_ns` column

## Log Segment Rotation & Compaction

- `LogConfig.max_segment_bytes` (default 64MB): rotates to new segment file when exceeded
- `LogConfig.retention_count` (default 8): compaction removes oldest segments beyond this count
- `BrokerConfig.log_compact_interval_ms` (default 60s): background compaction interval

## Prometheus Metrics

`yata-server` exposes `/metrics` on port 9090 via `metrics-exporter-prometheus`.

Key metrics: `yata_log_appends_total`, `yata_kv_ops_total`, `yata_lance_flushes_total`, `yata_lance_flush_duration_seconds`, `yata_nats_arrow_published_total`, `yata_flight_requests_total`

## Consumer Group API

`NatsConsumerGroup` (yata-nats): durable pull consumers for Arrow stream subscriptions.

```rust
let cg = nats_backend.consumer_group();
let stream = cg.subscribe(ConsumerConfig { group_name: "my-app".into(), .. }).await?;
// stream yields ConsumedBatch { table, batch, ack }
```

## Vector Search

`VectorSearchTicket` for Arrow Flight `do_get`:

```json
{"kind":"vector_search","table":"embeddings","column":"vector","vector":[0.1,0.2,...],"limit":10}
```

`LocalLanceSink.vector_search()` / `create_vector_index()` for direct Lance API.

## Schema Registry

`SchemaRegistry` (yata-arrow): versioned schema tracking with compatibility modes (`Backward`, `Forward`, `Full`, `None`). Built-in schemas pre-registered at Broker startup. `validate_batch()` checks incoming data against registered schemas.

## CDC (Change Data Capture)

`NatsCdcPublisher` / `NatsCdcConsumer` (yata-nats): CDC events on `yata.cdc.<table>` NATS subjects.

```rust
let cdc = nats_backend.cdc_consumer();
let stream = cdc.subscribe("my-cdc-group", Some("yata_events")).await?;
// stream yields CdcEvent { table, op, row_count, ts_ns }
```
