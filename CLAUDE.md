# packages/rust/yata

yata broker — Arrow-native distributed event store with Raft consensus。magatama-host から `YataClient` / `LanceGraphStore` 経由で使われる。PVC 永続化 + B2 tiered storage。

## Crate 役割分担

| Crate | 役割 |
|---|---|
| `yata-core` | 共通型 (`YataError`, `PayloadRef`, traits: `AppendLog`, `KvStore`, `ObjectStorage`) + `ConsumerConfig`, `CdcEvent` |
| `yata-client` | `YataClient` trait — KV / Log / Lance への async API |
| `yata-server` | `Broker` + `BrokerBackend` — embedded broker (magatama-server に埋め込み)。Raft consensus。Prometheus metrics (`/metrics` port 9090) |
| `yata-raft` | **Raft consensus** — leader election, AppendEntries replication, `StateMachineApplier` trait。single-node は即 leader |
| `yata-nats` | **(legacy, yata-server から除去済み)** — NATS JetStream backend。crate は残存するが Broker は使わない |
| `yata-kv` | KV store (local: in-memory snapshot + append-only log + **TTL enforcement** — `ttl_expires_at_ns` lazy-check + reaper) |
| `yata-log` | Append log (local: filesystem segment files + CBOR + CRC32 + **segment rotation** + **compaction**) |
| `yata-lance` | Lance table I/O ヘルパー (Arrow RecordBatch 変換、`pub` conversion functions) + **vector search** (`vector_search()`, `create_vector_index()`) |
| `yata-arrow` | Arrow IPC encode/decode (`batch_to_ipc`, `ipc_to_batch`) + **SchemaRegistry** (versioned, backward/forward/full compatibility) |
| `yata-cypher` | **Cypher パーサ + 実行エンジン** (pure Rust, Lance 非依存。variable-hop, regex, STARTS WITH/ENDS WITH/CONTAINS)。`MemoryGraph: Clone` 対応 |
| `yata-graph` | **Lance-backed graph store** (`LanceGraphStore` + `QueryableGraph` + `GraphCache`)。`cached_query()` = CSR cache + query LRU + write-back delta + S3 fresh_conn。全消費者はこの API を使う |
| `yata-flight` | Arrow Flight SQL gRPC サービス — `FlightSqlService` trait 完全実装 (Catalogs/Schemas/Tables/SqlInfo/PrimaryKeys/ExportedKeys/ImportedKeys/CrossRef/XdbcTypeInfo/Statement/PreparedStatement + custom Cypher/VectorSearch/GraphWrite fallback) |
| `yata-bolt` | **Bolt v4 wire protocol server** — PackStream v1 encoder/decoder、Bolt handshake/RUN/PULL/DISCARD、Neo4j driver 互換 (Java/Python/Go/JS)。`LanceGraphStore::cached_query()` 経由 |
| `yata-gateway` | **Standalone query gateway** — Bolt (:7687) + Arrow Flight SQL (:32010) + Neo4j Query API v2 (:7474) + Prometheus (:9090)。S3-native LanceDB (Linode ObjStore 同一 DC)。k8s Deployment (`yata` namespace) |
| `yata-grin` | **GRIN trait** — storage-agnostic graph access (Topology/Property/Schema/Scannable/Mutable/Partitioned + **Tiered/Versioned/DistributedPartition**). Zero deps (serde only) |
| `yata-store` | **MutableCSR** — GraphScope Flex-inspired in-memory graph store. CSR adjacency O(degree), label bitmap index, property eq index, WAL, MVCC snapshots. Implements Tiered (HOT) + Versioned |
| `yata-gie` | **Graph Interactive Engine** — IR operators, predicate pushdown optimizer, push-based streaming executor. Depends on yata-grin + yata-store |
| `yata-coordinator` | **ShardedGraphStore** — label-based partitioning, Rayon parallel shard execution, cross-shard aggregation merge |
| `yata-engine` | **TieredGraphEngine** — HOT (CSR) → WARM (Lance) query routing, QueryCache (LRU+TTL), RLS filter, delta writer, Lance SQL pushdown. Absorbs all logic from magatama-host/graph_host.rs |
| `yata-cdc` | **CDC emitter** — WalEntry → GraphCdcEvent broadcast (tokio broadcast channel). GART-inspired CDC for real-time graph change propagation |
| `yata-at` | AT Protocol types, Firehose client, `AtFirehoseBridge` |
| `yata-signal` | Signal Protocol crypto (X3DH, Double Ratchet, Sender Keys) |

## Tiered HTAP Graph Engine (GraphScope 5-engine 統合)

GraphScope の Vineyard/GRIN/GART/GraphAr/Groot の設計を Lance + MutableCSR + Raft の 3 primitives に統合。
Design doc: `docs/260316-yata-graphscope-flex-design.md`

```
yata-grin          — GRIN trait (Topology/Property/Schema/Scannable/Mutable/Partitioned
                     + Tiered/Versioned/DistributedPartition)
                     serde のみ依存。

yata-store         — MutableCSR (HOT tier, GraphScope Flex MutableCSR 相当)
                     CSR adjacency O(degree), label bitmap index, property eq index
                     WAL, MVCC snapshot, impl Tiered(Hot) + Versioned
                     Perf: 304ns property lookup, 463ns 1-hop, 1.07M writes/sec

yata-graph         — LanceGraphStore (WARM tier, monolithic tables)
                     + PerLabelLanceStore (per-label Lance tables, Lance versioning)
                     + QueryableGraph (yata-cypher を呼ぶ thin wrapper)

yata-engine        — TieredGraphEngine (全 graph intelligence を集約)
                     HOT (CSR) → WARM (Lance local) → COLD (Lance B2) routing
                     QueryCache (LRU+TTL+generation), RLS filter, delta writer
                     magatama-host/graph_host.rs から全ロジック移行済み

yata-cdc           — CdcEmitter (WalEntry → GraphCdcEvent broadcast)
                     GART-inspired CDC stream for real-time graph change propagation

yata-gie           — Graph Interactive Engine (GraphScope GIE 相当)

yata-coordinator   — Coordinator (label-based sharding, Rayon parallel)
                     Linear scaling: 8 shards → 8.5x speedup (measured)
```

## Crate Dependency DAG (29 crates, zero circular deps)

```
Layer 0 (zero deps):
  yata-core    yata-raft    yata-grin

Layer 1 (→ core):
  yata-arrow ──→ core
  yata-cbor ───→ core
  yata-cas ────→ core
  yata-object ─→ core
  yata-ocel ───→ core
  yata-nats ───→ core, arrow

Layer 2 (→ core + infra):
  yata-cypher ─→ core, ocel
  yata-log ────→ core, arrow
  yata-b2 ─────→ core, object
  yata-lance ──→ core, arrow, ocel
  yata-client ─→ core, arrow, ocel

Layer 3 (→ grin chain):
  yata-store ──→ grin, cypher
  yata-kv ─────→ core, log
  yata-graph ──→ cypher
  yata-signal ─→ core, cbor, kv

Layer 4 (graph engine):
  yata-gie ────→ grin, store
  yata-engine ─→ grin, store, graph, cypher
  yata-cdc ────→ core, store
  yata-flight ─→ core, lance, cypher, graph
  yata-bolt ───→ graph

Layer 5:
  yata-coordinator → grin, store, gie

Layer 6 (composite):
  yata-server ─→ core, arrow, log, kv, object, b2, ocel, lance, client, graph, raft
  yata-at ─────→ core, cbor, cas, server

Layer 7 (binary):
  yata-gateway → server, flight, bolt, graph, b2
  yata-cli ────→ core, lance, ocel, server, client
  yata-bench ──→ 14 crates (integration tester)
```

**CRITICAL**: 循環依存なし。最深パス 4 hop。`yata-grin` は serde のみ依存 (zero yata deps)。

## yata-engine / magatama-host 責務分離 (CRITICAL)

```
magatama-host        — graph_host.rs は thin adapter (47行)。
  graph_host.rs        YataGraphHost { engine: TieredGraphEngine }
                       query() は engine.query() を呼ぶだけ。

yata-engine          — 全 graph intelligence (旧 graph_host.rs 875行から移行)
  engine.rs            TieredGraphEngine: HOT CSR → WARM Lance routing
  cache.rs             QueryCache: LRU + TTL + generation invalidation
  router.rs            is_cypher_mutation, extract_pushdown_hints
  rls.rs               apply_rls_filter (org_id scoping)
  loader.rs            load_csr_from_lance, rebuild_csr_from_graph
  writer.rs            write_delta (Lance persist + mutation tracking)
  config.rs            TieredEngineConfig (thresholds, cache sizes)
```

**CRITICAL**: `yata-cypher` は `yata-graph` を import しない。`yata-graph` は magatama を import しない。`magatama-host` は `yata-engine` のみ import。

## yata-flight Flight SQL (arrow-flight `flight-sql-experimental`)

`FlightSqlService` trait を完全実装。標準 Flight SQL クライアント (JDBC Flight SQL driver 等) から接続可能。

| Flight SQL RPC | 実装 |
|---|---|
| `CommandStatementQuery` / `TicketStatementQuery` | Cypher query → Arrow IPC |
| `CommandStatementUpdate` | Cypher mutation (CREATE/MERGE/DELETE/SET) |
| `CommandGetCatalogs` | `"yata"` |
| `CommandGetDbSchemas` | `"yata"."public"` |
| `CommandGetTables` | 7 known + graph tables |
| `CommandGetTableTypes` | `"TABLE"` |
| `CommandGetSqlInfo` | server name/version/capabilities |
| `CommandGetPrimaryKeys` | empty (Lance has none) |
| `CommandGetExportedKeys/ImportedKeys/CrossReference` | empty |
| `CommandGetXdbcTypeInfo` | empty |
| `PreparedStatement` (create/close/get) | stateless (handle = Cypher text) |
| `Handshake` | no-auth (returns empty token) |
| Custom `do_get_fallback` | `ScanTicket` / `CypherTicket` / `VectorSearchTicket` |
| Custom `do_put_fallback` | `WriteTicket` / `GraphWriteTicket` |
| Custom `do_action_fallback` | `cypher_mutate` |

## yata-gateway (standalone query gateway)

独立 Pod として Bolt / Flight SQL / Neo4j Query API を外部クライアントに公開。embedded yata broker とは別で、外部 query gateway として動作。

| Port | Protocol | 用途 |
|---|---|---|
| 7687 | TCP (Bolt v4) | Neo4j ドライバ / Cypher Shell |
| 32010 | gRPC (Arrow Flight SQL) | JDBC Flight SQL / DBeaver |
| 7474 | HTTP | Neo4j Query API v2 (`POST /db/{db}/query/v2`) |
| 9090 | HTTP | Prometheus metrics |

### S3-native LanceDB (CRITICAL)

lance_uri / graph_uri に `s3://` URI を指定すると LanceDB が S3 に直接読み書きする。B2 sync loop は不要 (自動スキップ)。

| 構成 | lance_uri | レイテンシ |
|---|---|---|
| **同一 DC S3** (推奨) | `s3://ai-gftd-yata/graph` (Linode ObjStore jp-osa) | ~100ms (cold), μs (cache hit) |
| B2 S3 | `s3://ai-gftd-lancedb/yata-gateway/graph` (us-west-004) | ~3.5s (太平洋横断) |
| ローカル PVC | `/data/yata/graph` + B2 sync | ~5ms (local), 30s sync lag |

**AWS env vars** (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`, `AWS_DEFAULT_REGION`) で S3 認証。

### k8s deploy

```yaml
# Namespace: yata
# Secrets: linode-objstore-credentials, yata-b2-credentials, ghcr-pull-secret
# Manifest: infra/k8s/yata-gateway.yaml
# Pulumi component: infra/pulumi/components/yatagateway/
```

## yata-graph Cache (CSR + Query LRU)

`LanceGraphStore` に組み込まれたキャッシュ層。全消費者 (gateway, flight, bolt, magatama-server) が自動的に恩恵を受ける。

```
LanceGraphStore
├── conn: lancedb::Connection      (writes)
├── base_uri: String               (fresh_conn() で read 時に新接続 — S3 consistency)
├── cache: RwLock<GraphCache>
│   ├── csr: Option<MemoryGraph>   — write で invalidate, 次の read で lazy reload
│   ├── generation: u64            — write 毎に bump
│   └── query_results: LRU        — 30s TTL, 256 entries, generation check
└── cached_query(cypher, params)   — 全消費者はこの API を使う
```

| 階層 | レイテンシ | トリガー |
|---|---|---|
| Query cache hit | **μs** | 同一 cypher + params、generation 一致、TTL 内 |
| CSR cache hit | **ms** (clone + execute) | CSR loaded、generation 一致 |
| Cold (S3 load) | **~100-600ms** | CSR = None (初回 or write 後) |

**Mutation**: `cached_query` が自動で `write_delta` → cache invalidate → 次の read で CSR reload。

**S3 read-after-write**: `fresh_conn()` で read 時に新 Connection を作成。Lance の S3 metadata cache 問題を回避。

## CypherTicket (custom fallback)

```json
{"kind":"cypher","cypher":"MATCH (n:Person) RETURN n.name","params":[]}
```

- `kind="cypher"` で `AnyTicket::Cypher` にルーティング (do_get_fallback)
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

## yata-raft: Raft consensus

Single-node (standalone) は即 leader。Multi-node は `RaftConfig.peers` 設定でクラスタリング。

```toml
[raft]
node_id = 1
peers = []  # empty = single-node
```

### Raft コンポーネント

| Module | 役割 |
|---|---|
| `node.rs` | `RaftNode` — election, propose, handle_message, AppendEntries replication |
| `store.rs` | `MemLogStore` (in-memory BTreeMap) + `YataStateMachine` + `StateMachineApplier` trait |
| `network.rs` | `RaftTransport` trait + `PeerAddr` + `RaftMessage` enum |
| `types.rs` | `YataRequest` (Publish/CreateTopic/DeleteTopic/CommitOffset) + `YataResponse` |

### 最適化ルール (CRITICAL)

- **1行/batch 禁止**: Arrow IPC schema overhead は ~1.9KB/batch。1行 batch は 2% efficiency。最低 64 行で batch すること
- **Table handle cache**: `open_table` を毎回呼ばない

## 永続化: PVC + B2 tiered storage (CRITICAL — B2 は必須)

k8s デプロイは PVC (`linode-block-storage-retain`) で yata データを永続化。B2 tiered storage は必須 (`BrokerConfig.b2: B2Config`)。

| Deployment | PVC | サイズ |
|---|---|---|
| `isco-mt-magatama` | `isco-mt-yata-pvc` | 10Gi |
| `states-mt-magatama` | `states-mt-yata-pvc` | 20Gi |

### B2 sync (30s interval, always active)

- `log/` → B2 log segments
- `kv_payloads/` → B2 KV payload blobs
- `lance/` → B2 Lance datasets
- `cas/` + `manifests/` → B2 via TieredObjectStore (write-through async)

### Storage 実装基盤

| Store | yata-log (event bus) 上? | 詳細 |
|---|---|---|
| **yata-kv** | **YES** | 全 put/delete は `_kv.<bucket>` ストリームに append。in-memory snapshot は log replay で再構築。Raft replication 対象 |
| **yata-object** | **NO** | 直接 filesystem CAS (`cas/<hash>`) + manifest CBOR。B2 TieredObjectStore で write-through |

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

Key metrics: `yata_log_appends_total`, `yata_kv_ops_total`, `yata_lance_flushes_total`, `yata_lance_flush_duration_seconds`, `yata_flight_requests_total`, `yata_kv_ttl_reaped_total`, `yata_log_segments_compacted_total`

## Vector Search

`VectorSearchTicket` for Arrow Flight `do_get`:

```json
{"kind":"vector_search","table":"embeddings","column":"vector","vector":[0.1,0.2,...],"limit":10}
```

`LocalLanceSink.vector_search()` / `create_vector_index()` for direct Lance API.

## Schema Registry

`SchemaRegistry` (yata-arrow): versioned schema tracking with compatibility modes (`Backward`, `Forward`, `Full`, `None`). Built-in schemas pre-registered at Broker startup. `validate_batch()` checks incoming data against registered schemas.

## Shannon Efficiency: Embedded vs Separate Pod

### Theoretical analysis

| 指標 | Embedded (現構成) | Separate Pod |
|---|---|---|
| Overhead bytes/request | **400 B** | 2,738 B (6.8x) |
| Shannon η (4KB batch) | **91.1%** | 59.9% |
| Shannon η (64KB batch) | **99.4%** | 95.9% |
| Latency (query) | **12-102 μs** | 107-507 μs (5-10x) |
| Memory | **共有** (1 Broker) | +512Mi (2 Broker) |

### Measured load test (2026-03-17, 1000 nodes / 3000 edges)

Benchmark: `cargo run -p yata-bench --bin embedded-vs-cluster-bench --release`

| 指標 | A: Embedded | B: gRPC (Flight SQL) | C: shmem (zero-copy) |
|---|---|---|---|
| Single-client latency (avg) | **583 µs** | 1,678 µs (**2.9x**) | 580 µs (1.00x) |
| Concurrent ×8 latency (avg) | **1,713 µs** | 4,321 µs (**2.5x**) | — |
| Overhead/request | **400 B** | 2,214 B (**5.5x**) | 74 B |
| Extra memory | **0 MB** | +512 MB | 0 MB |
| Mixed workload (70R/30W) | **506 µs** | 1,777 µs (3.5x) | — |

gRPC overhead decomposition: Arrow IPC schema ~1,900 B/batch + gRPC/HTTP2 framing ~64 B + TCP ~160 B = ~2,200 B/req。

shmem zero-copy は Embedded と同等性能だが failure domain が同一のためメリットなし。

**結論: Embedded が全帯域で最適。** Separate Pod (gRPC cluster) は 2.9x latency 劣化 + 512MB memory 追加 + 5.5x protocol redundancy。PVC + Raft で durability は十分。
