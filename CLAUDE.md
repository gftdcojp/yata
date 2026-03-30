# packages/rust/yata

yata — Rust Cypher graph engine. `[PRODUCTION]` Container × N partition (**multi-container/multi-node 前提 CRITICAL**)。Workers RPC coordinator。GraphScope parity 21/25 (14 PRODUCTION + 7 IMPLEMENTED)。GIE push-based + Design E SecurityScope + **LanceDB-style append-only write** (tombstone old + append new, per-write `commit_labels_only`, full commit deferred to compaction) + **LanceDB-style demand-paged cold start** (manifest-only → per-label on-demand page-in, cold start 2.2s) + per-label delta compaction (dirty labels only) + adaptive √N fan-out + CpmStats + cypherBatch + **immutable versioned manifest** (`manifest-{inverted:020}.json`) + **Range GET** (`get_range_sync`) + **concurrent cold start guard** (`cold_starting` AtomicBool)。設計: `docs/260329-yata-coo-sorted-design.md`

## CRITICAL: Multi-Container/Multi-Node 前提 (MUST)

**yata は常に multi-container/multi-node を前提とする。** Single-node 最適化は禁止。

- **R2 = shared source of truth**: 全ノードが同一 R2 bucket/prefix から manifest + segments を独立に読む
- **各ノードの in-memory state は独立**: `loaded_labels`, `hot_initialized`, CSR は per-node。cross-node state sharing なし
- **Demand-paged cold start**: manifest のみ load → `hot_initialized = true` → label は query 時に on-demand page-in (LanceDB pattern)。各ノードが必要な label のみ fetch
- **Immutable manifest versioning**: `manifest-{inverted:020}.json` で O(1) latest lookup。古い manifest は point-in-time recovery 用に保持
- **Coordinator (YataRPC)**: label-based routing で `hash(label) % N` → partition。Write/Read split。Read replica × M
- **禁止**: node 間 state 依存、single-node 前提の設計、manifest に node-specific 情報を書く

## Architecture (CRITICAL)

```
Write model: Pipeline.send() (durable) + YATA_RPC.mergeRecord() (LanceDB-style append-only)
  → Pipeline: AT Protocol commit chain + MST (durable, 0 data loss)
  → mergeRecord: tombstone old vertex + append new vertex (immutable write, O(1))
  → commit_labels_only(): label_index + label_bitmap + btree rebuild (lightweight, O(V_l log V_l))
  → full commit() deferred to L0_COMPACT_THRESHOLD (10,000 writes)
  → dirty_labels.insert(label)

Read model: yata Container (pure read)
  Workers RPC (YataRPC) → hierarchical coordinator (√N fan-out)
    → N × Container (Rust, standard-1, 4GB RAM, 8GB disk)
      Each Container:
        R2 (Arrow IPC sorted COO segments) → DiskBlobCache/MmapBlobCache → CooStore
        TieredGraphEngine → yata-cypher / yata-gie (sparse index binary search)

Storage: Sorted COO (Coordinate format) + Sparse Index
  COO triples sorted by (label, src, dst) in Arrow IPC segments (~32MB each)
  R2 = Source of Truth (per-label sorted segments: log/coo/{pid}/label/{label}/{range}.arrow)
  Sparse index: per-label (every 256th src → segment offset) → O(log 256) traversal
  L0 = unsorted append buffer (in-memory) → L1 = sorted segments (LSM compaction)
  DiskBlobCache = Container ephemeral disk cache (segment-level LRU)
  MmapBlobCache = zero-copy mmap (segment-level)

Key properties (vs CSR):
  Write: O(1) append (CSR was O(V) offsets rebuild per dirty label)
  Snapshot: eliminated (sorted segments ARE persistent format, no commit() rebuild)
  Page-in: (label, src_range) granular (CSR was full label)
  Traversal: O(log 256 + degree) via sparse index (CSR was O(1) + O(degree))
```

Deploy/config/ops details: `infra/cloudflare/container/yata/CLAUDE.md`

## Data Flow

```
Write (LanceDB-style append-only, NO page-in):
  App → PDS_RPC.createRecord(repo, collection, record)
    → Pipeline.send(): AT commit + MST update → ACK (~1ms, durable)
    → YATA_RPC.mergeRecord(): tombstone old vertex + append new vertex (immutable, O(1))
    → commit_labels_only(): label_index + label_bitmap + btree rebuild (O(V_l log V_l))
    → full commit() (columnar/prop_eq) deferred to L0_COMPACT_THRESHOLD (10,000 writes)
    → dirty_labels.insert(label)

  yata への直接 mutate は mergeRecord からのみ (app 直接禁止)
  既存 vertex の in-place props 上書きは禁止 (LanceDB: immutable data + deletion vector)

Lance Compaction (size-based L0_COMPACT_THRESHOLD, dirty_labels gated):
  trigger_compaction():
    → wal_flush_segment(): WAL ring → Arrow IPC segment → R2 PUT
    → drain_dirty_labels() → dirty set
    → dirty empty? → skip (zero R2 I/O)
    → Phase 1: per dirty label: L0 + existing Lance fragments → merge-sort → PK-dedup → Blake3 → R2 PUT as Lance fragment
    → Phase 2: Lance TableManifest built from successfully uploaded fragments → R2 PUT (immutable versioned)
    → clean labels: untouched (zero R2 I/O)

Read (cold start: Lance manifest → fragment-level demand page-in):
  PDS_RPC.query / listRecords / getTimeline
    → YATA_RPC.cypher → TieredGraphEngine
    → ensure_labels(vertex_labels) → hot_initialized == false?
      → cold_start(): Lance TableManifest load (R2 list O(1)) → hot_initialized = true → WAL tail replay
    → ensure_labels(["Post"]) → Lance manifest → matching fragments → 3-tier (disk → R2 → Blake3 verify → cache)
    → wal_apply(entries) → MutableCsrStore → sparse index binary search + scan

PDS Container (Rust) は不要 — 全て TS Worker + Pipeline + YATA_RPC。
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
| `yata-format` | **YataFragment format** (snapshot format, test/migration utility)。NbrUnit zero-copy (CSR legacy, Phase 3 removal candidate)。**Arrow row-group chunk**: `split_record_batch` + byte-based chunking (32 MB default)。PropertyGraphSchema (typed vertex/edge labels + Arrow property columns)。**COO segment format**: sorted (label, src, dst) Arrow IPC segments |
| `yata-store` | **CooStore** (sorted COO, L0 append buffer + L1 sorted segments, sparse index), MutableCsrStore (legacy CSR, Phase 3 removal), ArrowGraphStore, ArrowWalStore (mmap I/O utility for compacted WAL), DiskBlobCache/MmapBlobCache/MemoryBlobCache (BlobCache trait impls), PartitionStoreSet, GraphStoreEnum |
| `yata-engine` | TieredGraphEngine, CpmStats (K3a), **Lance compaction** (`compaction.rs`: L0 + existing Lance fragments merge-sort, PK-dedup, dirty_labels drain → Lance fragment R2 PUT + `TableManifest` R2 PUT), **Arrow IPC WAL** (`arrow_wal.rs`: serialize/deserialize/auto-detect, default format), **Lance cold start** (`lance_manifest_cache` → `ensure_labels()` fragment demand page-in, **`cold_starting` AtomicBool** concurrent guard), `loader.rs` (CSR rebuild helpers のみ, legacy page-in 除去済み), Frontier BFS, ShardedCoordinator, WAL Projection (ring buffer + segment flush)。Design E SecurityScope (`query_with_did` → policy vertex lookup) |
| `yata-cypher` | Full Cypher parser + executor (incl. untyped edge traversal) |
| `yata-gie` | GIE push-based executor, IR (Exchange/Receive/Gather, serde-serializable), distributed planner, `execute_step()` (Phase 5: stateless per-round fragment execution), `MaterializedRecord` (rkey-based cross-partition exchange), `ExchangePayload` (HTTP transport) |
| `yata-s3` | R2 persistence (sync ureq+rustls S3 client, SigV4)。`get_sync` (full GET) + **`get_range_sync` (HTTP Range GET, SigV4 signed)**。`trigger_compaction()` → R2 PUT、page-in → R2 GET/Range GET |
| `yata-lance` | **Lance-table-compatible persistence** — typed Arrow schema (VERTICES_SCHEMA/EDGES_SCHEMA), Arrow IPC fragment serialize/deserialize, versioned TableManifest, deletion info。`yata-engine` compaction + cold start の persistence layer |
| `yata-vex` | Vector index (IVF_PQ + DiskANN) |
| `yata-bench` | Benchmarks: `coo-read-bench` (COO read + R2 page-in), `tiered-bench`, `cypher-bench`, `trillion-scale-test` |
| `yata-server` | XRPC API server (`/xrpc/ai.gftd.yata.cypher` + `compact`)。GraphQueryExecutor trait |

## GraphScope Parity

`gftd symbol-graph --package yata` で component status を確認。985+ unit tests (+ e2e)。

## R2 Persistence `[PRODUCTION]` — Lance Fragment Format (verified 2026-03-30)

R2 = source of truth。**Lance fragments**: `lance/vertices/{pid}/fragments/{ver}-{frag}.arrow` (Arrow IPC, immutable)。**Lance TableManifest**: `lance/vertices/{pid}/manifest-{inverted:020}.json` (versioned, immutable)。**Append-only write**: mergeRecord = tombstone old + append new (L0 buffer, page-in 不要)。PK dedup は compaction 時。**Dirty tracking**: `drain_dirty_labels()` で dirty set を取得。dirty empty = zero R2 I/O (skip)。**Lance Compaction `[PRODUCTION]`**: L0 + existing Lance fragments → merge-sort → PK-dedup → new Lance fragment R2 PUT → `TableManifest` R2 PUT。Clean labels = zero R2 I/O。**Blake3 checksum**: `Fragment.blake3_hex`。cold start 時 R2 GET → verify → mismatch で corrupt fragment スキップ。**Two-phase PUT**: Phase 1: per-label Lance fragment upload (失敗 label はスキップ)。Phase 2: 成功 fragments から `TableManifest` 構築 → versioned manifest PUT。**Immutable Manifest Versioning**: `manifest-{u64::MAX - version:020}.json` — inverted naming で S3 ListObjects が O(1) 最新取得。**Fragment demand page-in**: 3-tier (disk cache → R2 GET → Blake3 verify → write-through)。**Range GET `[IMPLEMENTED]`**: `get_range_sync(key, offset, length)` で byte-range R2 GET (SigV4 signed)。**Cold start**: Lance `TableManifest` load (~50ms) → `hot_initialized = true` → labels demand-paged on first query。**Concurrent cold start guard**: `cold_starting` AtomicBool CAS (1 thread のみ実行)。**No snapshot serialize** — Lance fragments ARE the persistent + query format。

## Concurrency Model (CRITICAL)

**LanceDB-style append-only: Read-Write 並列化** — Write = tombstone + append (immutable)。既存 vertex data は不変。commit_labels_only() で label scan 即時反映。

| 操作 | Lock | 並列性 |
|---|---|---|
| Read × Read | immutable sorted segments | **concurrent** (lock-free) |
| Read × Write | tombstone + append (immutable data) | **concurrent** (既存 data 不変) |
| Write × Write | L0 buffer mutex (~ns) | sequential (brief) |
| Read × Compaction | atomic segment swap | **concurrent** |
| Compaction × Write | L0 continues appending | **concurrent** |

**Cross-partition**: 各 partition = 独立 Container → **partition 間は完全並列**。YataRPC が `hash(label) % N` で routing。

**LanceDB-style write safety**: mergeRecord = tombstone old O(1) + append new O(1) + commit_labels_only O(V_l log V_l)。既存 vertex の in-place mutation 禁止。Compaction = tombstone GC + PK dedup → new immutable segment。

**`hot_initialized` AtomicBool**: `Ordering::SeqCst` (全 load/store)。`Relaxed` は concurrent cold start 重複実行リスクがあったため昇格。

**`loaded_labels` guard (CRITICAL)**: `ensure_labels()` は Lance fragment page-in 成功時のみ `loaded_labels` に追加。失敗時は追加しない → 次回 query で再試行。

## Shannon Analysis

9-format 7-axis 比較: `docs/260329-yata-9format-shannon-comparison.md`。**COO Sorted = 62.1%, COO + Lazy CSR hybrid = 74.3%** (9 format 中で最高)。CSR 単体 = 46.1%。全 weight 配分で COO + Lazy CSR が最高。旧 5-axis 分析は superseded。

### LanceDB-style Append-Only Shannon Efficiency (measured 2026-03-30)

**Overall η = 48%** (before: 52%, write η 10x 改善 + read η 維持)

| Path | η | Bottleneck | LanceDB 対応 |
|---|---|---|---|
| **Write (per-op)** | ~5% | commit_labels_only O(V_l log V_l) | Fragment append + manifest update |
| **Write (amortized @ 10K batch)** | ~50% | full commit at compaction | Background compaction |
| **Read: PK lookup** | 100% | — | Point read |
| **Read: Timeline (ORDER BY)** | ~71% | btree scan O(log V_l + limit) | Indexed scan |
| **Read: Label scan** | ~95% | bitmap O(V_l/64) | Fragment scan |
| **Storage** | ~99% | Tombstone overhead u≈1% | Deletion vector |
| **Compaction** | ~10% | dirty labels only | Background rewrite + GC |
| **Cold start** | ~80% | R2 latency 支配 | Demand-paged manifest |

**Key trade-off**: Write η 0.001% → 5% (5000x↑) at cost of per-write btree rebuild O(V_l log V_l)。Timeline read η maintained at 71% (btree index in commit_labels_only)。

**LanceDB 対応関係**:

| LanceDB | yata | 一致度 |
|---|---|---|
| Fragment (Arrow IPC) | L1 sorted COO segment (Arrow IPC) | **高** |
| Append write | Tombstone old + append new vertex | **高** |
| Deletion vector | `vertex_alive = false` | **高** |
| Immutable manifest chain | `manifest-{inverted}.json` versioned | **高** |
| Background compaction | Size-based LSM (L0→L1, PK dedup, tombstone GC) | **中** (label 単位) |
| Per-write commit 不要 | `commit_labels_only()` (lightweight) | **中** (label/bitmap/btree のみ) |
| MVCC | WAL seq ordering (single-writer) | **低** |

**禁止**: 既存 vertex の in-place props 上書き (LanceDB immutable 原則違反)。per-write full commit() (Shannon 冗長 η=0.001%)。

## Persistence Model — Sorted COO Segments + Arrow IPC WAL

**Durability は Pipeline WAL が保証。** Pipeline → R2 JSON (10s flush) が WAL source of truth。**Sorted COO segments が query format = persistent format** (別途 snapshot 不要)。

| 層 | Format | Trigger | 用途 |
|---|---|---|---|
| **Pipeline WAL** | R2 JSON (`pipeline/wal/`) | `Pipeline.send()` 10s flush | source of truth (durable) |
| **yata WAL segments** | Arrow IPC (`wal/segments/{pid}/`) | cron 10s `walFlushSegment` | Cold start replay |
| **L0 buffer** | in-memory unsorted COO tuples | mergeRecord append | instant write, pending compaction |
| **Lance fragments** | Arrow IPC (`lance/vertices/{pid}/fragments/{ver}-{frag}.arrow`) | LSM compaction (dirty labels only) | query + persistence (PK-dedup, P=1.0) |
| **Lance TableManifest** | JSON (`lance/vertices/{pid}/manifest-{inverted:020}.json`) | `trigger_compaction` | Fragment tracking + immutable version chain |

**`trigger_compaction()` = `wal_flush_segment` + `drain_dirty_labels` + Lance fragment compaction。** L0 + existing Lance fragments → merge-sort → PK-dedup → new Lance fragment R2 PUT + TableManifest R2 PUT。Clean labels = zero R2 I/O。

**Cold start (Lance demand-paged)**: `ensure_labels` → `hot_initialized == false` → `wal_cold_start_manifest_only()` (Lance `TableManifest` load + WAL tail のみ, ~50ms) → `hot_initialized = true` **即座**。Per-label fragment は query 時に on-demand page-in (3-tier: disk cache ~100µs → R2 GET ~4ms → Blake3 verify → write-through)。Multi-node: 各ノードが独立に manifest load + fragment page-in。

## Arrow IPC WAL + Lance Fragment Compaction `[PRODUCTION]` (verified 2026-03-30)

**Lance-table-compatible architecture.** WAL = Arrow IPC (append-only)。Compaction = Lance fragments (Arrow IPC) + TableManifest (versioned JSON)。CSR rebuild → eliminated。

- **WAL format**: Arrow IPC File (default `YATA_WAL_FORMAT=arrow`)。`WalEntry.props` = `Vec<(String, PropValue)>` (typed, zero JSON overhead)。custom serde で flat JSON map backward compat
- **Segment registry**: `head.json` の `segments` array に全 segment key を記録。R2 ListObjectsV2 不使用
- **Lance Compaction (two-phase PUT)**: `trigger_compaction()` が `drain_dirty_labels()` → dirty labels のみ L0 + existing Lance fragments merge-sort → PK-dedup。Phase 1: per-label Lance fragment upload (`lance/vertices/{pid}/fragments/{ver}-{frag}.arrow`, Blake3 checksum、失敗 label はスキップ)。Phase 2: 成功 fragments から `TableManifest` 構築 → `lance/vertices/{pid}/manifest-{inverted:020}.json` PUT。Clean labels = zero R2 I/O
- **Blake3 checksum**: Lance `Fragment.blake3_hex`。Cold start 時 R2 GET → verify → mismatch で corrupt fragment スキップ (panic なし)
- **Cold start**: Lance `TableManifest` load (R2 list `lance/vertices/{pid}/manifest-*`, O(1) latest) → `lance_manifest_cache` → `hot_initialized = true` → WAL tail replay。Labels are demand-paged by `ensure_labels()` from Lance fragments (3-tier: disk → R2 → verify)。**`cold_starting` AtomicBool CAS** で concurrent cold start 防止 (1 thread のみ実行、他は spin-wait)
- **WAL flush safety**: pattern match 安全 early return。空 entries でパニックしない
- **Replica transport**: `/xrpc/ai.gftd.yata.walTailArrow` (Arrow IPC body) + `/xrpc/ai.gftd.yata.walApplyArrow`
- **Migration CLI**: `gftd yata migrate --from csr --to coo` (CSR→COO forward) / `--from coo --to csr` (rollback)
- **Edge cache 除去**: PDS `cyCached` → `cy` 直接 (graph data は mutation-driven)
- **PDS `cyRetry` (CRITICAL)**: 空結果時に1回リトライ (5s timeout)。Container cold start / segment page-in 失敗に対する defense-in-depth
- **Size-based compaction (CRITICAL)**: `L0_COMPACT_THRESHOLD` (default 10,000) pending_writes で自動 trigger。cron 排除 (workload-adaptive)

| Path | CSR (Before) | COO (After) |
|---|---|---|
| Write (merge_record) | O(V) offsets rebuild per commit() | O(1) L0 buffer append (no rebuild) |
| Snapshot | O(V+E) serialize per dirty label | eliminated (sorted segments = format) |
| Page-in | O(V+E) per label | O(segment) per (label, src_range) |
| Cold start (disk) | mmap ~100µs/label (full) | mmap ~100µs/segment (granular) |
| Compaction | O(dirty_entries) per-label delta | O(dirty_entries) LSM merge-sort |
| Concurrency | RwLock (write blocks read during rebuild) | L0/L1 分離 (read-write concurrent) |
| Traversal | O(1) + O(degree) CSR direct | O(log 256) + O(degree) sparse index |
| Recovery | P=1.0 (compacted + tail) | P=1.0 (sorted segments + tail) |

**禁止**: CSR offsets rebuild の新規導入。mutable snapshot (commit() → serialize → R2 PUT)。PDS `cyCached` に edge cache 再導入。**v1 monolithic compacted segment の新規作成** (v2 per-label のみ)。time-based cron compaction (size-based のみ)。**cold start での full segment GET without `cold_starting` guard** (concurrent cold start = OOM)

## CRITICAL: 3 概念は直交 — partition ≠ label ≠ security

**partition** = Container instance。YataRPC coordinator が `hash(label) % N` で label-based routing。
**label** = Cypher node type = Arrow IPC sorted COO segment I/O 単位。`ensure_labels` で on-demand segment page-in。同一 store 内に全 label 同居 → cross-label query native。
**security** = GIE SecurityFilter (vertex property O(1)/vertex, inline during scan)。partition は security boundary ではない。
**禁止**: `appId = auth.org_id` (Clerk org_id は partition/label/security のいずれでもない)。

## Scale Strategy

Production: PARTITION_COUNT=1, per-label sorted COO Arrow IPC, segment-level page-in (3-tier: disk→R2), Design E SecurityScope。1,068 tests。

### Key behaviors

- **Query** (warm 0.2-0.3µs, 3.3-3.8M QPS): Cypher → parse → ensure_labels (segment page-in) → GIE on CSR。Cold start = R2 page-in 支配 (4ms/label)、compute 0.2ms deser + 0.6ms apply per label。No MemoryGraph fallback (failure = error)
- **Mutation** (O(1) append, no rebuild): L0 buffer append + WAL ring append。merge_by_pk = prop_eq_index O(1)。Edge deletion = tombstone in L0 (compacted out in L1)。CpmStats: cypher_read/mutation/mergeRecord counts + mutation_avg_us。**No commit() rebuild** — CSR rebuild 排除
- **Storage**: RAM (L0 buffer + sparse index) → disk cache (sorted segments ~100us) → R2 source of truth (~3-5ms)。Arrow IPC segment: ~115 bytes/entry
- **Cold start**: **segment-level page-in** (label × src_range 単位)。3-tier blob fetch (disk → R2 → write-through)。後続 query で on-demand segment load
- **Chunk**: Arrow row-group 32 MB/chunk byte-based。1B vertices でも ~数十 chunks
- **Partition fan-out**: 1x standard-1 = ~20M nodes (production)。4x standard-1 = ~100M (E2E verified)

## Env Vars

| Env var | Default | Purpose |
|---|---|---|
| `YATA_S3_*` | (empty) | R2 endpoint/bucket/key/secret/prefix |
| `YATA_MMAP_VINEYARD` | `false` | Enable MmapBlobCache (zero-copy) |
| `YATA_DIRECT_FAN_OUT_LIMIT` | `8` | Below this, direct fan-out; above, hierarchical √N `[IMPLEMENTED]` (companion Worker adaptive routing) |
| `YATA_CHUNK_TARGET_BYTES` | `33554432` (32 MB) | Arrow row-group chunk target byte size per blob。R2/S3 最適 8-64 MB |
| `YATA_CHUNK_ROWS` | (unset) | 設定時は byte-based estimation を override し固定 row 数で chunk 分割 |
| `YATA_WAL_FORMAT` | `arrow` | WAL segment format (`arrow` or `ndjson`)。Arrow = zero-copy mmap |

## Test Coverage

1,068+ Rust unit tests (yata-s3: 48, yata-engine: 220, others) + 68 e2e, 0 failures. E2E: 8 tests (docker-compose + MinIO, 2-partition). 6-node distributed: 6 tests (10K records, label routing, cold put/pull). Phase 3 load test: 8 tests (chunk snapshot, 2-hop traversal, label-selective reads, mixed load). WAL cold start recovery: 14 tests (flush safety, apply roundtrip, PK dedup, Arrow serialize, dirty-only compaction, Blake3 checksum roundtrip+tamper, gap detection 5 scenarios, empty buffer, dirty_labels drain, v1/v2 manifest backward compat). YataFragment snapshot roundtrip verified. R2 persistence verified (2026-03-29): 964 vertices, 33 labels, 1.58 MB snapshot, full property columns (rkey/collection/repo/value_b64/owner_hash/updated_at/_app_id/_org_id).

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

bench: `cargo run -p yata-bench --bin coo-read-bench --release`

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

**Lance format migration (2026-03-30):**
- **yata-lance crate**: Lance-table-compatible persistence (VERTICES_SCHEMA/EDGES_SCHEMA, Arrow IPC fragments, versioned TableManifest)
- **Append-only write**: `merge_by_pk` = tombstone old + append new (LanceDB immutable pattern)。`commit_labels_only()` per-write
- **Lance compaction**: dirty labels → new Lance fragments + TableManifest (R2 `lance/vertices/{pid}/`)
- **Lance cold start**: TableManifest load → `lance_manifest_cache` → fragment demand page-in (3-tier)
- **Range GET**: `yata-s3` `get_range_sync(key, offset, length)` (HTTP Range, SigV4 signed)
- **Concurrent cold start guard**: `cold_starting` AtomicBool CAS (1 thread 排他実行)
- **Dead code 除去**: loader.rs 870行除去 (9 legacy page-in functions), engine.rs 100行除去 (5 dead functions)

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

**R2 永続化 verified**: R2 に YataFragment 12 blobs, 10 vertex labels, 684 vertices 存在確認済み。

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

- **R2 以外を source of truth にする禁止** — R2 Lance fragments が正本
- **JSON RPC で graph data 転送禁止** — Workers RPC (structured clone) + ArrayBuffer
- **lite instance 禁止** — standard-1 以上
- **CSR offsets rebuild 新規導入禁止** — Sorted COO に移行
- **既存 vertex の in-place props 上書き禁止 (CRITICAL)** — LanceDB immutable 原則。update = tombstone old + append new
- **per-write full commit() 禁止** — `commit_labels_only()` (label_index + label_bitmap + btree) を使用。full commit (columnar/prop_eq) は compaction 時のみ
- **legacy persistence path 新規追加禁止** — `log/compacted/`, `snap/fragment/`, `CompactionManifest` は除去済み。Lance fragment path (`lance/vertices/`) のみ使用
- **loader.rs に page-in 関数新規追加禁止** — Lance fragment demand page-in は engine.rs `ensure_labels()` が担当
- **`reqwest` crate 再追加禁止** — `aws-lc-sys` cross-compile 障害。`ureq` + `rustls` を使用
- **`RUSTC_WRAPPER=sccache` での cross-compile 禁止** — `cargo zigbuild` を使用
