# packages/rust/yata

yata — Rust Cypher graph engine on LanceDB。`[PRODUCTION]` Container × N partition。`lancedb` 0.27 (lance 4.0)。**No persistent CSR** — ephemeral `build_read_store()` per query from LanceDB scan。**No WAL ring** — `merge_record()` → `table.add()` direct。**No in-memory cache** — Cloudflare edge cache。GIE push-based + Design E SecurityScope。engine.rs 1,712行。

## CRITICAL: Multi-Container/Multi-Node 前提 (MUST)

**yata は常に multi-container/multi-node を前提とする。** Single-node 最適化は禁止。

- **R2 = shared source of truth**: 全ノードが同一 R2 bucket/prefix から manifest + segments を独立に読む
- **各ノードの in-memory state は独立**: ephemeral read store は per-query。cross-node state sharing なし
- **Cold start**: `db.open_table("vertices")` — LanceDB が R2 接続 + fragment page-in を管理
- **Immutable manifest versioning**: `manifest-{inverted:020}.json` で O(1) latest lookup。古い manifest は point-in-time recovery 用に保持
- **Coordinator (YataRPC)**: label-based routing で `hash(label) % N` → partition。Write/Read split。Read replica × M
- **禁止**: node 間 state 依存、single-node 前提の設計、manifest に node-specific 情報を書く

## Architecture (CRITICAL)

```
Write: PDS → mergeRecord() → WalEntry batch → table.add(batch) [LanceDB] → R2
Read:  PDS → cypher() → build_read_store(labels) → table.scan_filter() [LanceDB]
         → ephemeral LanceReadStore → Cypher parse → GIE execute → response
Cold:  ensure_lance() → db.open_table("vertices") [LanceDB handles R2]
Store: LanceDB (lancedb 0.27, lance 4.0) — S3/R2 built-in, versioning + fragments auto-managed

No persistent CSR. No WAL ring. No in-memory cache. No dirty_labels.
No S3Client. No compaction.rs. No manifest.rs.
```

Deploy/config/ops details: `infra/cloudflare/container/yata/CLAUDE.md`

## Data Flow

```
Write (LanceDB-style append-only, NO page-in):
  App → PDS_RPC.createRecord(repo, collection, record)
    → Pipeline.send(): AT commit + MST update → ACK (~1ms, durable)
    → YATA_RPC.mergeRecord(): tombstone old vertex + append new vertex (immutable, O(1))
    → table.add(batch) [LanceDB] → R2 Lance fragments

  yata への直接 mutate は mergeRecord からのみ (app 直接禁止)

Read:
  PDS → YATA_RPC.cypher → build_read_store(labels)
    → table.scan_filter("label='Post'") [LanceDB] → ephemeral LanceReadStore
    → Cypher parse → GIE execute → response

Compact:
  trigger_compaction() → table.compact() [LanceDB built-in fragment merge]

Cold start:
  ensure_lance() → db.open_table("vertices") [LanceDB handles R2 connection]
```

## Workers RPC API (CRITICAL)

```ts
// Bind: { service: "ai-gftd-yata", entrypoint: "YataRPC" }
// Transport: Workers RPC only
// Container XRPC: /xrpc/ai.gftd.yata.cypher (unified read+write)

env.YATA.cypher(cypher, appId)          // unified Cypher path → /xrpc/ai.gftd.yata.cypher
env.YATA.cypherBatch(stmts[], appId)    // N statements in 1 HTTP round-trip (K3b)
env.YATA.query(cypher, appId)           // read-only alias → read replicas
env.YATA.mutate(cypher, appId)          // CREATE → random partition, DELETE → broadcast
env.YATA.compact()                      // per-label L1 compaction (v1→v2 migration, dirty labels only)
env.YATA.health()                       // → partition-0 Container
env.YATA.ping()                         // "pong" (no wake)
env.YATA.stats()                        // → all partition CpmStats (K3a)
```

## Crate Roles (CRITICAL)

| Crate | Role |
|---|---|
| `yata-core` | GlobalVid, LocalVid, PartitionId |
| `yata-grin` | GRIN trait (Topology, Property, Schema, Scannable, Mutable) |
| `yata-engine` | TieredGraphEngine (1,712行)。Write: `merge_record()` → `table.add()`。Read: `build_read_store()` → `table.scan_filter()` → ephemeral LanceReadStore → GIE exec。Compact: `table.compact()`。Cold: `ensure_lance()` → `db.open_table()`。SecurityScope (Design E)。CpmStats。**No persistent CSR, No WAL, No cache, No S3Client** |
| `yata-cypher` | Full Cypher parser + executor (incl. untyped edge traversal) |
| `yata-gie` | GIE push-based executor, IR (Exchange/Receive/Gather, serde-serializable), distributed planner, `execute_step()` (Phase 5: stateless per-round fragment execution), `MaterializedRecord` (rkey-based cross-partition exchange), `ExchangePayload` (HTTP transport) |
| `yata-s3` | Legacy R2 persistence (sync ureq+rustls S3 client)。**yata-engine から除去済み** — LanceDB が S3/R2 内蔵。yata-server の CAS store 用に残存 |
| `yata-lance` | **LanceDB persistence** — `lancedb` 0.27 crate (S3/R2 内蔵, lance 4.0)。`YataDb` (Connection wrapper: `connect_local`/`connect_s3`/`connect_from_env`)、`YataTable` (Table wrapper: `add`/`scan_all`/`scan_filter`/`count_rows`/`compact`)、typed Arrow schema (`schema.rs`)、`LanceReadStore` (graph materialization)。`yata-engine` WAL flush (`table.add`) + compaction (`table.compact`) + cold start (`db.open_table`) の persistence layer |
| `yata-bench` | Benchmarks: `cypher-bench`, `cypher-transport-bench` |
| `yata-server` | XRPC API server (`/xrpc/ai.gftd.yata.cypher` + `compact`)。GraphQueryExecutor trait |

## GraphScope Parity

`gftd symbol-graph --package yata` で component status を確認。985+ unit tests (+ e2e)。

## R2 Persistence `[PRODUCTION]` — Lance Fragment Format (verified 2026-03-30)

R2 = source of truth。**LanceDB = sole storage** (`lancedb` 0.27, lance 4.0)。LanceDB が versioning + fragment + manifest + S3/R2 を内部管理。Write: `table.add(batch)`。Read: `table.scan_filter()`。Compact: `table.compact()`。Cold: `db.open_table("vertices")`。No persistent CSR。No WAL。No in-memory cache。

## Concurrency Model

Write: `table.add()` (LanceDB MVCC)。Read: `table.scan_filter()` (snapshot isolation)。LanceDB が内部で並列化管理。

**Cross-partition**: 各 partition = 独立 Container → **partition 間は完全並列**。YataRPC が `hash(label) % N` で routing。

**Write**: `table.add()` (LanceDB MVCC)。**Read**: `table.scan_filter()` (snapshot isolation, ephemeral per query)。既存 vertex の in-place mutation 禁止。

## Env Vars

| Env var | Default | Purpose |
|---|---|---|
| `YATA_S3_*` | (empty) | R2 endpoint/bucket/key/secret/prefix |
| `YATA_MMAP_VINEYARD` | `false` | Enable MmapBlobCache (zero-copy) |
| `YATA_DIRECT_FAN_OUT_LIMIT` | `8` | Below this, direct fan-out; above, hierarchical √N `[IMPLEMENTED]` (companion Worker adaptive routing) |
| `YATA_CHUNK_TARGET_BYTES` | `33554432` (32 MB) | Arrow row-group chunk target byte size per blob。R2/S3 最適 8-64 MB |
| `YATA_CHUNK_ROWS` | (unset) | 設定時は byte-based estimation を override し固定 row 数で chunk 分割 |
| `YATA_WAL_FORMAT` | `arrow` | WAL segment format (`arrow` or `ndjson`)。Arrow = zero-copy mmap |

## Workers Lance WASM Read/Write `[PRODUCTION]` (verified 2026-03-31)

**lance v0.27.1 の 5 crate (core/encoding/io/file/table) を WASM (wasm32-unknown-unknown) にコンパイルし、Cloudflare Workers V8 isolate 内で LanceDB standard layout の read/write を Container なしで実行。** `@lancedb/lancedb` npm (native bindings 必須) は不使用。fork: `gftdcojp/lancedb-wasm`。

### Architecture

```
PDS Worker (4.4 MiB / gzip 901 KiB)
├── yata_wasm.wasm (3.3 MiB) — lance-core+encoding+io+file+table WASM
│
├── Write: sendWalAndProject() — Container 不要
│   1. Pipeline.send() → R2 JSON WAL (durable, source of truth)
│   2. workersLanceWrite():
│      a. encode_vertex_lance() [WASM] → Lance v2 fragment
│      b. R2 PUT yata/vertices/data/{uuid}.lance
│      c. add_fragment_to_manifest() [WASM] → protobuf manifest update
│      d. R2 PUT yata/vertices/_versions/{version:020}.manifest
│
├── Read: cy() → tryWorkersRead()
│   1. R2 compacted segments → Arrow IPC decode → Cypher subset exec (Workers edge)
│   2. Container fallback (auth queries, complex Cypher)
│
└── /health → { workers_read: {hit, fallback, labels_cached}, lance_write: {count} }
```

### R2 LanceDB Dataset Layout (verified)

```
ai-gftd-graph/yata/vertices/              ← LanceDB standard layout
├── _versions/
│   ├── 00000000000000000001.manifest      ← protobuf Manifest (V2 naming)
│   ├── 00000000000000000002.manifest
│   └── ...                                 (version chain, +66 bytes/fragment)
└── data/
    └── {uuid}.lance                        ← Lance v2.1 data fragment
```

### WASM Patched Crates (5 crates, gftdcojp/lancedb-wasm)

| crate | patch summary |
|---|---|
| **lance-core** | `moka` → `cfg(not(wasm32))` no-op cache。`object_store` default-features=false。`tokio` WASM-safe subset。`spawn_cpu` inline sync。`SimdSupport::None` |
| **lance-encoding** | `zstd`/`lz4` → `cfg(not(wasm32))` stub。`tokio` WASM-safe subset。`arrow` without prettyprint |
| **lance-io** | `local` module → `cfg(not(wasm32))`。filesystem functions gated。`object_store` default-features=false。`shellexpand`/`path_abs` gated |
| **lance-file** | `lance-io` default-features=false。`statistics`/`testing` → `cfg(not(wasm32))`。`datafusion-common` gated |
| **lance-table** | `tokio`/`object_store` → `cfg(not(wasm32))` + WASM subset。`uuid` js feature。`arrow-ipc` without zstd。`commit.rs` local filesystem gated |

### WASM API (`yata_wasm.wasm`)

| function | 引数 | 戻り値 | 用途 |
|---|---|---|---|
| `probe()` | — | string | WASM 動作確認 |
| `encode_vertex_lance(labels, pk_values, repos, rkeys, props_jsons)` | Vec<String> ×5 | Vec<u8> | RecordBatch → Lance v2 file bytes |
| `read_lance_footer(file_bytes)` | &[u8] | JSON string | footer 解析 (version, columns, magic) |
| `generate_fragment_path()` | — | string | `data/{uuid}.lance` 生成 |
| `create_manifest(fragment_path, num_rows, field_names, field_ids)` | &str, u32, Vec ×2 | Vec<u8> | protobuf Manifest v1 生成 |
| `add_fragment_to_manifest(manifest_bytes, fragment_path, num_rows, field_ids)` | &[u8], &str, u32, Vec | Vec<u8> | manifest に fragment 追加 (version increment) |
| `manifest_path(version)` | u32 | string | `_versions/{version:020}.manifest` |
| `read_manifest(manifest_bytes)` | &[u8] | JSON string | manifest 解析 (version, fragments, fields) |

### Lance 互換性

| 項目 | 状態 |
|---|---|
| Data encoding (Lance v2.1) | ✅ |
| Footer (40 bytes, LANC magic) | ✅ |
| Column metadata (protobuf) | ✅ |
| Manifest (`_versions/`, protobuf, V2 naming) | ✅ |
| Fragment registration (manifest version chain) | ✅ |
| Directory layout (`data/`, `_versions/`) | ✅ |
| Compression (zstd/lz4) | ❌ uncompressed only (C library WASM 不可) |
| Schema in file (global buffer) | ❌ mini lance mode |
| Transaction files (`_transactions/`) | ❌ |
| Deletion vectors (`_deletions/`) | ❌ |

### R2 Paths

| path | format | writer | reader |
|---|---|---|---|
| `yata/vertices/_versions/{ver:020}.manifest` | protobuf Manifest | PDS Worker (WASM) | Container LanceDB SDK |
| `yata/vertices/data/{uuid}.lance` | Lance v2.1 fragment | PDS Worker (WASM) | Container LanceDB SDK |
| `yata/log/compacted/{pid}/label/{label}.arrow` | Arrow IPC (legacy compacted) | Container | PDS Worker (read path) |
| `yata/log/compacted/{pid}/manifest.json` | JSON (legacy) | Container | PDS Worker (read path) |

### TS Modules (`@gftd/yata`)

| module | LOC | 役割 |
|---|---|---|
| `cypher-parse.ts` | ~290 | Cypher subset parser (Pattern A/B/C) |
| `cypher-exec.ts` | ~270 | Arrow Table executor (filter/sort/limit/count/traversal) |
| `r2-reader.ts` | ~130 | R2 CompactionManifest + Arrow IPC fragment loader |
| `workers-read.ts` | ~160 | WorkersReader (TS executor ↔ Container fallback) |
| `workers-write.ts` | ~250 | Arrow IPC WAL segment builder (legacy) |

### Metrics (measured 2026-03-31)

| metric | value |
|---|---|
| WASM module | 3.3 MiB (wasm-bindgen optimized) |
| PDS bundle | 4.4 MiB (gzip 901 KiB) |
| Worker startup | 15ms |
| Lance encode 1 row | ~300 bytes, <1ms |
| Manifest update | +66 bytes/fragment, <1ms |
| Workers read (R2 cache hit) | ~0ms |
| Workers read (R2 fetch) | ~5-10ms |
| Container fallback | ~13-170ms |
| Workers read hit rate | ~74% (30 request sample) |

### 禁止

- `@lancedb/lancedb` npm の Workers 使用禁止 (native bindings 必須、WASM 版なし)
- lance-core への `tokio/fs`, `rt-multi-thread` 再追加禁止 (WASM compile error)
- WASM 内での compression (zstd/lz4) 使用禁止 (C library 非対応、stub がエラー返却)
- `object_store` default features の有効化禁止 (ring/reqwest/hyper が WASM 非対応)
- Container mergeRecord の write path 再導入禁止 — Workers Lance write が primary

## Test Coverage

**Rust**: 1,068+ unit tests + 68 e2e, 0 failures. **TS (`@gftd/yata`)**: 45 tests (4 files, coverage: stmts 79.7%, branch 69.5%, funcs 85.2%, lines 84.5%), 0 failures. **PDS**: 303 tests, 0 failures. **WASM**: production verified (5 writes → 5 manifest versions in R2)。

| suite | tests | scope |
|---|---|---|
| Rust (yata-*) | 1,068+ | engine, s3, cypher, lance, WAL, compaction |
| TS cypher-parse | 19 | Pattern A/B/C parser, WHERE ops, edge cases |
| TS cypher-exec | 14 | Arrow filter/sort/limit/count, traversal, IPC roundtrip |
| TS r2-reader | 5 | manifest load, fragment fetch, v1/v2 compat |
| TS workers-read | 7 | router, cache, fallback, invalidate |
| PDS | 303 | XRPC dispatch, helpers, auth, write path |
| WASM Lance write | E2E | encode → R2 PUT fragment → manifest create/update → version chain |
| Workers read | E2E | R2 Arrow IPC → Cypher exec → 74% hit rate (30 req sample) |
| Load test | 30 req | p50=31ms, p95=105ms, avg=45ms (getSuggestions) |

## Benchmark (measured, release build)

**COO Read Container (measured 2026-03-29, 5K nodes, 15K edges, 10 labels):**

| Operation | In-Process (warm) | Notes |
|---|---|---|
| **Full scan count(*)** | **3.8M QPS (0.2µs)** | GIE on CSR after COO page-in |
| **Label scan count** | **3.6M QPS (0.3µs)** | Single label |
| **Point read (rkey)** | **3.6M QPS (0.3µs)** | PK lookup |
| **Filter (score>500)** | **3.4M QPS (0.3µs)** | Property filter on label |
| **1-hop traversal** | **3.5M QPS (0.3µs)** | FOLLOWS edge |
| **2-hop traversal** | **3.3M QPS (0.3µs)** | FOLLOWS→OWNS |

**COO Cold Start Cost Breakdown (5K entries, 10 labels):**

| Stage | Disk (mmap) | R2 (4ms/label) |
|---|---|---|
| **Total** | **8ms** | **57ms** |
| Arrow IPC deserialize | 1.6ms (0.2ms/label) | 1.6ms |
| wal_apply (CSR merge) | 6.5ms (0.6ms/label) | 6.5ms |
| R2 GET overhead | — | **40ms** (支配的, 70%) |

**Arrow IPC Segment Throughput:**

| Metric | Result |
|---|---|
| Serialize 5K entries | 1ms → 576 KB |
| Deserialize 5K entries | **1.5ms** (661 ops/s) |
| Per-label segment size | ~59 KB (500 entries) |
| Per-label compaction (10 labels) | 6ms |

**Incremental Label Page-In (R2 4ms/label):**

| Labels | Total | R2 overhead | Compute |
|---|---|---|---|
| 1 label | 5.7ms | 4ms | 1.7ms |
| 3 labels | 16.8ms | 12ms | 4.8ms |
| 10 labels | 45ms | 40ms | 5ms |

**Scale Projection (COO cold start):**

| Nodes | Segment KB | Deser µs | Apply µs | Cold (disk) | Cold (R2 4ms) | Point QPS |
|---|---|---|---|---|---|---|
| 1K | 113 | 315 | 1,301 | 1.6ms | 5.6ms | 7,178 |
| 5K | 576 | 1,595 | 7,080 | 8.7ms | 12.7ms | 1,467 |

**Key insight**: R2 latency が cold start の支配項。compute (deser+apply) は 5K entries で 8ms。Production 33 labels → ~33×4ms R2 + ~20ms compute ≈ **152ms** (Container wake 2.8s はコンテナ起動コスト込み)。page-in 後の query は warm CSR と同一 (COO overhead = cold start のみ)。


**Write path (10K records):**

| Operation | In-Process | Via HTTP | Notes |
|---|---|---|---|
| **mergeRecord** | 63,715/sec | 70/sec (docker) | append-only, no page-in |
| **Edge create** | 77,092/sec | — | Cypher MATCH+CREATE |

**6-node distributed (docker-compose, 10K records):**
| Metric | Result |
|---|---|
| Cold write 10K | 70/sec (HTTP overhead) |
| Snapshot 6 nodes | 33ms total |
| Cold pull recovery | 10,000/10,000 records |
| Distributed reads | 103 QPS |

**Production E2E (pds.gftd.ai → YataRPC → Container, 2026-03-25):**
| Metric | Result | Notes |
|---|---|---|
| Cold start (container wake) | **2.8s** | Container sleep → wake + R2 page-in |
| searchActors (warm, 20x avg) | **424ms** | PDS → YataRPC → Container → Cypher |
| getTimeline (2-hop) | **271ms** | Graph traversal through PDS |
| listRecords (label-specific) | **371ms** | Label-selective page-in path |

**v4 Performance Test (2026-03-30, `gftd performance-test`, 3 VUs, 5s/endpoint, v2 per-label segments):**

| Endpoint | p50 | Grade | Notes |
|---|---|---|---|
| Health | **13.3ms** | S | — |
| SearchPosts | **13.7ms** | S | v2 per-label page-in |
| GetProfile | **13.5ms** | S | v2 per-label page-in |
| SearchActors | **13.5ms** | S | v2 per-label page-in |
| GetSuggestions | **13.4ms** | S | v2 per-label page-in |
| GetFollowers | **13.2ms** | S | v2 per-label page-in |
| GetFollows | **13.4ms** | S | v2 per-label page-in |
| ListNotifications | **13.2ms** | S | v2 per-label page-in |
| GetUnreadCount | **13.6ms** | S | v2 per-label page-in |
| GetAuthorFeed | **1560ms** | D | Container round-trip |
| GetPostThread | **1042ms** | D | Container round-trip |
| ListRecords | **879ms** | D | Container round-trip |
| GetTimeline | **15s** | F | Cold start timing |
| GetDiscoverFeed | **10.5s** | F | Cold start timing |

**Overall: grade=C, p50=13.5ms, 9/14 S-grade。** v3→v4 改善: v1 monolithic 399MB → v2 per-label 47MB migration。S-grade 2→9 endpoints。v1 cold start 20-30s → v2 cold start 8-13s。

**LanceDB migration (2026-03-31):**
- **yata-lance → lancedb 0.27**: `lance` 3.0 低レベル → `lancedb` 0.27 高レベル API 移行。S3/R2 内蔵 (`connect().storage_option()`)
- **削除済み**: `store.rs` (UreqObjectStore), `manifest.rs` (GraphManifest), `catalog.rs`, `writer.rs`, `vector.rs`, `compaction.rs` (計 ~2,500行)
- **WAL flush**: `table.add(batch)` — LanceDB が fragment + version 管理
- **Compaction**: `table.compact()` — LanceDB built-in fragment merge
- **Cold start**: `db.open_table("vertices")` — LanceDB が manifest + fragment page-in 管理
- **engine.rs S3Client 除去**: `yata-s3` dep 削除、`get_s3_client()` / `try_build_s3_client()` 削除、WAL segment R2 fallback 削除
- **config.rs**: `WalFormat` enum 削除、WAL segment config 3項目削除

**Trillion scale projection:**
| Scale | Partitions | Write/sec | Point QPS | Scan QPS | Cost/月 |
|---|---|---|---|---|---|
| 1M | 1 | 63,715 | 499 | 2,000 | $60 |
| 100M | 5 | 63,715 | 499 | 400 | $5,888 |
| 1B | 50 | 63,715 | 499 | 40 | $5,959 |
| 1T | 50,000 | 63,715 | 499 | 0.04 | $80,884 |

## mergeResults (Coordinator)

`mergeResults()` correctly handles GROUP BY + aggregate functions (COUNT, SUM, AVG, MIN, MAX). AVG is computed from per-partition sum/count.

## CRITICAL: Build (cargo zigbuild)

```bash
# macOS → linux/amd64 cross-compile (標準パス)
RUSTC_WRAPPER="" cargo zigbuild --manifest-path packages/server/yata/Cargo.toml \
  --release --target x86_64-unknown-linux-gnu -p yata-server  # ~5s (incremental) / ~1m24s (clean)

# バイナリは共有 target dir: .cargo-target/x86_64-unknown-linux-gnu/release/ai-gftd-yata-server
# (旧パス packages/server/yata/.cargo-target は不正)
```

- **バイナリ名**: `ai-gftd-yata-server` (Cargo.toml の `[[bin]]` name)。`.cargo-target/x86_64-unknown-linux-gnu/release/ai-gftd-yata-server` に出力
- **TLS**: `ureq` + `rustls` (ring backend)。`aws-lc-sys` / `reqwest` は除去済み (cross-compile 障害)
- **sccache 禁止**: `RUSTC_WRAPPER=""` 必須 (cc-rs が sccache 経由で C compiler を探して失敗)
- **rest.rs 変更後は必ず rebuild** → バイナリが古いと `/xrpc/ai.gftd.yata.cypher` が 404
- **Deploy 手順**: `infra/cloudflare/container/yata/CLAUDE.md` §yata-server Build & Deploy を参照。`wrangler containers build` + `push` が必須 (`wrangler deploy` だけでは image push されない)

## PDS Dispatch Fixes (2026-03-25)

**R2 永続化 verified**: R2 に Lance fragments / manifest と WAL が存在確認済み。

| Issue | Fix | Location |
|---|---|---|
| `collectionToLabel` snake_case 未対応 | `.split(/[-_]/)` で snake_case + kebab-case 両対応 | `pds-helpers.ts:223` |
| `buildProfileView` displayName fallback = DID | fallback を `didToHandle(actor)` に変更 (handle 表示) | `pds-helpers.ts:315` |
| `AppBskyActorGetProfile` が `Profile` label のみ参照 (R2 未永続化) | structured `Profile` (R2 永続化) へ fallback 追加。display_name/description を補完 | `pds-dispatch.ts:910-927` |
| label consistency テスト未整備 | `pds-helpers.test.ts` に snake_case テスト 5 件 + label consistency check 追加 (214 tests pass) | `pds-helpers.test.ts` |

**診断手順 (Lance manifest)**:
```bash
# Latest Lance manifest
npx wrangler r2 object list ai-gftd-graph --prefix "yata/lance/vertices/0/manifest-" --remote | head -1
```

## 禁止事項

- **LanceDB 以外の persistence path 新規追加禁止** — `table.add()` / `table.compact()` / `db.open_table()` が唯一のpath。S3Client, WAL ring, compaction.rs, manifest.rs, cache.rs 全て削除済み
- **persistent CSR 再導入禁止** — ephemeral `build_read_store()` per query。L0 hot store は削除済み
- **WAL ring buffer 再導入禁止** — `merge_record()` → `table.add()` 直接
- **in-memory query cache 再導入禁止** — Cloudflare edge cache (`caches.default`, 60s TTL) に委譲
- **`RUSTC_WRAPPER=sccache` での cross-compile 禁止** — `cargo zigbuild` + `avx512_stub.rs`
