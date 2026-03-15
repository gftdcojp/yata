# packages/rust/yata

yata broker — NATS JetStream 互換 event store。magatama-host から `YataClient` / `LanceGraphStore` 経由で使われる。

## Crate 役割分担

| Crate | 役割 |
|---|---|
| `yata-core` | 共通型 (`YataError`, `PayloadRef`, etc.) |
| `yata-client` | `YataClient` trait — KV / Log / Lance への async API |
| `yata-server` | `Broker` + `BrokerBackend` — embedded broker (magatama-server に埋め込み) |
| `yata-kv` | KV store (sled/RocksDB backed) |
| `yata-log` | Append log (Lance backed) |
| `yata-lance` | Lance table I/O ヘルパー (Arrow RecordBatch 変換) |
| `yata-arrow` | Arrow schema utilities |
| `yata-cypher` | **Cypher パーサ + 実行エンジン** (pure Rust, Lance 非依存) |
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
