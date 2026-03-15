# packages/rust/yata

yata broker — NATS JetStream 互換 event store。magatama-host から `YataClient` / `LanceGraphStore` 経由で使われる。

## Crate 役割分担

| Crate | 役割 |
|---|---|
| `yata-core` | 共通型 (`YataError`, `PayloadRef`, traits: `AppendLog`, `KvStore`, `ObjectStorage`) |
| `yata-client` | `YataClient` trait — KV / Log / Lance への async API |
| `yata-server` | `Broker` + `BrokerBackend` — embedded broker (magatama-server に埋め込み)。NATS 有無で backend 自動切替 |
| `yata-nats` | **NATS JetStream backend** — `AppendLog`/`KvStore`/`ObjectStorage` の NATS 実装 + Arrow IPC produce/consume + `NatsLanceWriter` |
| `yata-kv` | KV store (local: in-memory snapshot + append-only log) |
| `yata-log` | Append log (local: filesystem segment files + CBOR + CRC32) |
| `yata-lance` | Lance table I/O ヘルパー (Arrow RecordBatch 変換、`pub` conversion functions) |
| `yata-arrow` | Arrow IPC encode/decode (`batch_to_ipc`, `ipc_to_batch`) |
| `yata-cypher` | **Cypher パーサ + 実行エンジン** (pure Rust, Lance 非依存。variable-hop, regex, STARTS WITH/ENDS WITH/CONTAINS) |
| `yata-graph` | **Lance-backed graph store** (`LanceGraphStore` + `QueryableGraph`) |
| `yata-flight` | Arrow Flight gRPC サービス — `ScanTicket` (Lance scan) + `CypherTicket` (Cypher via graph) |
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
