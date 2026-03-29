# packages/rust/yata

yata — Rust Cypher graph engine. `[PRODUCTION]` Container (CSR + DiskBlobCache/MmapBlobCache) × 1 partition. Workers RPC coordinator. GraphScope parity 21/25 (14 PRODUCTION + 7 IMPLEMENTED)。GIE push-based + Design E SecurityScope (CSR policy vertex lookup, parameter-based RLS 除去済み) + label-selective page-in + edge property lookup + edge tombstone deletion + **per-label delta compaction** (dirty labels only, clean labels = zero R2 I/O) + adaptive √N fan-out + CpmStats observability + cypherBatch + delta-apply mutation (CP5)。

## Architecture (CRITICAL)

```
Write model: Pipeline.send() (durable) + YATA_RPC.mergeRecord() (instant projection)
  → Pipeline: AT Protocol commit chain + MST (durable, 0 data loss)
  → mergeRecord: Cypher projection → yata Container → WAL append + CSR merge
  → dirty_labels.insert(label)

Read model: yata Container (pure read)
  Workers RPC (YataRPC) → hierarchical coordinator (√N fan-out)
    → N × Container (Rust, standard-1, 4GB RAM, 8GB disk)
      Each Container:
        R2 (Arrow IPC) → DiskBlobCache/MmapBlobCache → MutableCsrStore (<1ms)
        TieredGraphEngine → yata-cypher / yata-gie (push-based)

Persistence: Arrow IPC WAL + per-label L1 Compaction
  R2 = Source of Truth (per-label compacted segments: log/compacted/{pid}/label/{label}.arrow)
  DiskBlobCache = Container ephemeral disk cache (page-in/out, LRU evict)
  MmapBlobCache = zero-copy mmap (500M edges/Container)
  CSR = In-memory graph topology (<1ms query)
```

Deploy/config/ops details: `infra/cloudflare/container/yata/CLAUDE.md`

## Data Flow

```
Write (append-only, NO page-in):
  App → PDS_RPC.createRecord(repo, collection, record)
    → Pipeline.send(): AT commit + MST update → ACK (~1ms, durable)
    → YATA_RPC.mergeRecord(): WAL append + CSR merge (PK dedup in-memory only, NO R2 read)
    → dirty_labels.insert(label)

  yata への直接 mutate は mergeRecord からのみ (app 直接禁止)

Per-label L1 Compaction (cron 1min, dirty_labels gated):
  trigger_snapshot():
    → wal_flush_segment(): WAL ring → Arrow IPC segment → R2 PUT
    → drain_dirty_labels() → dirty set
    → dirty empty? → skip (zero R2 I/O)
    → per dirty label: read existing per-label segment + new WAL entries → PK-dedup → R2 PUT
    → clean labels: untouched (zero R2 I/O)
    → upload manifest v2 (per-label tracking)

Read (WAL cold start + per-label page-in):
  PDS_RPC.query / listRecords / getTimeline
    → YATA_RPC.cypher → TieredGraphEngine
    → ensure_labels(vertex_labels) → hot_initialized == false?
      → wal_cold_start(): per-label compacted segments (mmap/R2) → WAL tail replay
      → hot_initialized = true
    → CSR direct query (<1µs)

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
env.YATA.health()                       // → partition-0 Container
env.YATA.ping()                         // "pong" (no wake)
env.YATA.stats()                        // → all partition CpmStats (K3a)
```

## Crate Roles (CRITICAL)

| Crate | Role |
|---|---|
| `yata-core` | GlobalVid, LocalVid, PartitionId |
| `yata-grin` | GRIN trait (Topology, Property, Schema, Scannable, Mutable) |
| `yata-format` | **YataFragment format** (snapshot format, test/migration utility)。NbrUnit zero-copy CSR (25x faster neighbor traversal)。`csr_to_fragment()` (test-only)。**Arrow row-group chunk**: `split_record_batch` + byte-based chunking (32 MB default)。PropertyGraphSchema (typed vertex/edge labels + Arrow property columns) |
| `yata-store` | MutableCsrStore (mutable in-memory CSR, GRIN traits), ArrowGraphStore, ArrowWalStore (mmap I/O utility for compacted WAL), DiskBlobCache/MmapBlobCache/MemoryBlobCache (BlobCache trait impls), PartitionStoreSet, GraphStoreEnum (Single/Partitioned/Arrow — 3 variant のみ、ArrowWalStore は utility で variant ではない) |
| `yata-engine` | TieredGraphEngine, CpmStats (K3a), delta-apply mutation (K3c), **Arrow IPC WAL** (`arrow_wal.rs`: serialize/deserialize/auto-detect, default format), **Per-label L1 Compaction** (`compaction.rs`: PK-dedup per-label rewrite, CompactionManifest v2 per-label tracking, dirty_labels drain, v1→v2 auto-migration), cold start (per-label segment mmap → R2 GET → WAL tail replay), 3-tier blob fetch (`fetch_blob_cached`: disk → R2 → write-through), Frontier BFS, ShardedCoordinator, WAL Projection (ring buffer + segment flush + checkpoint + compaction)。Design E SecurityScope (`query_with_did` → CSR policy vertex lookup) |
| `yata-cypher` | Full Cypher parser + executor (incl. untyped edge traversal) |
| `yata-gie` | GIE push-based executor, IR (Exchange/Receive/Gather), distributed planner |
| `yata-s3` | R2 persistence (sync ureq+rustls S3 client, SigV4)。`trigger_snapshot()` → R2 PUT、page-in → R2 GET |
| `yata-vex` | Vector index (IVF_PQ + DiskANN) |
| `yata-bench` | Benchmarks + trillion-scale test |
| `yata-server` | XRPC API server (`/xrpc/ai.gftd.yata.cypher` + `triggerSnapshot`)。GraphQueryExecutor trait |

## GraphScope Parity

`gftd symbol-graph --package yata` で component status を確認。985+ unit tests (+ e2e)。

## R2 Persistence `[PRODUCTION]` (verified 2026-03-29: 964v, 33 labels, 1.58 MB, full properties)

R2 = source of truth。**Per-label compacted segments**: `log/compacted/{pid}/label/{label}.arrow` (Arrow IPC per label)。**Append-only write**: mergeRecord は page-in 不要 (in-memory CSR に append + WAL ring append + `dirty_labels.insert(label)`)。PK dedup は per-label compaction 時。**Dirty tracking**: `drain_dirty_labels()` で dirty set を取得し compaction 後にクリア。dirty empty = zero R2 I/O (skip)。**Per-label compaction `[PRODUCTION]`**: dirty labels のみ: existing per-label segment + new WAL entries → PK-dedup → per-label R2 PUT。Clean labels は untouched。CompactionManifest v2 (`label_segments: HashMap<String, LabelSegmentState>`)。v1 monolithic manifest からの auto-migration。**3-tier page-in `[PRODUCTION]`**: `fetch_blob_cached()` — disk cache → R2 GET → write-through。Cold start: per-label segment mmap (~100µs/label) → R2 GET fallback。**Snapshot monitoring (K3d)**: `last_snapshot_serialize_ms` in CpmStats。

## Concurrency Model (CRITICAL)

**RwLock<GraphStoreEnum>**: read 並列 / write 排他。`Mutex` から `RwLock` に移行済み。

| 操作 | Lock | 並列性 |
|---|---|---|
| Read × Read | `hot.read()` | **concurrent** |
| Read × Write | read wait | blocked (write waits for reads) |
| Write × Write | `hot.write()` | sequential |
| Read × Snapshot serialize | `hot.read()` | **concurrent** |
| Write × Snapshot compaction | `hot.write()` | sequential (brief) |

**Cross-partition**: 各 partition = 独立 Container = 独立 RwLock → **partition 間は完全並列**。YataRPC が `hash(label) % N` で routing → 同一 label は同一 partition。異なる label への concurrent writes = zero contention。

**CF Container**: 1 vCPU + Workers RPC sequential → 実質 single-thread で contention なし。axum tokio multi-thread 時は RwLock で read 並列化。

**Append-only write safety**: mergeRecord は page-in 不要 → write lock scope は merge_by_pk + commit のみ (~µs)。snapshot compaction が write lock を取るのは R2 既存データの CSR merge 時のみ (初回 1 回)。

## Arrow IPC Shannon Analysis

WAL + query storage schema の Shannon 情報効率比較: `docs/260329-yata-arrow-ipc-shannon-analysis.md`。結論: WAL=Edge List Arrow IPC + Query=CSR が Shannon 最適 (加重 71.8%)。NDJSON→Arrow IPC 移行で +9.3%。YataFragment 抽象は memmap2 直接置き換え候補。

## Persistence Model — Arrow IPC WAL + Per-label L1 Compaction

**Durability は Pipeline WAL が保証。** Pipeline → R2 JSON (10s flush) が WAL source of truth。

| 層 | Format | Trigger | 用途 |
|---|---|---|---|
| **Pipeline WAL** | R2 JSON (`pipeline/wal/`) | `Pipeline.send()` 10s flush | source of truth (durable) |
| **yata WAL segments** | Arrow IPC (`wal/segments/{pid}/`) | cron 10s `walFlushSegment` | Cold start replay |
| **Per-label compacted segments** | Arrow IPC (`log/compacted/{pid}/label/{label}.arrow`) | `trigger_compaction` (dirty labels only) | PK-dedup recovery (P=1.0) |
| **CompactionManifest v2** | JSON (`log/compacted/{pid}/manifest.json`) | `trigger_compaction` | Per-label state tracking |

**`trigger_snapshot()` = `wal_flush_segment` + `drain_dirty_labels` + per-label `trigger_compaction`。** Clean labels = zero R2 I/O。v1 monolithic manifest → v2 per-label auto-migration。

**Cold start**: `ensure_labels` → `hot_initialized == false` → `wal_cold_start()` 自動実行。v2: per-label segment mmap (~100µs/label) → R2 GET fallback。v1: monolithic segment (backward compat)。WAL tail replay (segment registry from `head.json`)。Read replica は初 query で自動 cold start。

## Arrow IPC WAL + L1 Compaction `[PRODUCTION]` (verified 2026-03-29)

**Shannon-optimal zero-copy WAL architecture.** NDJSON → Arrow IPC。JSON intermediary 全除去。

- **WAL format**: Arrow IPC File (default `YATA_WAL_FORMAT=arrow`)。`WalEntry.props` = `Vec<(String, PropValue)>` (typed, zero JSON overhead)。custom serde で flat JSON map backward compat
- **Segment registry**: `head.json` の `segments` array に全 segment key を記録。R2 ListObjectsV2 不使用 (S3 signing issue 回避)
- **Per-label L1 Compaction**: `trigger_compaction()` が `drain_dirty_labels()` → dirty labels のみ per-label PK-dedup。R2 key: `log/compacted/{pid}/label/{label}.arrow` + `log/compacted/{pid}/manifest.json` (v2)。Clean labels = zero R2 I/O。v1→v2 auto-migration (v1 monolithic 検出時は全 label を dirty 扱い)。XRPC: `/xrpc/ai.gftd.yata.compact`
- **Cold start**: v2 per-label segments (disk cache mmap ~100µs/label → R2 GET fallback) → WAL tail replay (segment registry)。v1 monolithic segment backward compat。`ArrowWalStore::from_file()` (mmap I/O utility)。Read replica は初 query で `ensure_labels` → `wal_cold_start` 自動実行 (cron 依存不要)
- **Replica transport**: `/xrpc/ai.gftd.yata.walTailArrow` (Arrow IPC body) + `/xrpc/ai.gftd.yata.walApplyArrow`。JSON endpoints 維持 (backward compat)
- **Migration CLI**: `gftd yata migrate --from snapshot --to arrow-wal` (forward) / `--from arrow-wal --to snapshot` (rollback)
- **Edge cache 除去**: PDS `cyCached` → `cy` 直接 (graph data は mutation-driven、edge cache は stale 原因)

| Path | Before | After |
|---|---|---|
| Write (merge_record) | PropValue→JSON→Map | PropValue→Vec clone (0 JSON) |
| Read (wal_apply) | JSON→PropValue | PropValue.clone() (direct) |
| WAL format | NDJSON | Arrow IPC (mmap-ready) |
| Cold start (disk) | R2 GET ~5ms | mmap ~100µs/label |
| Recovery | P=0.7 (delta snapshot) | P=1.0 (compacted + tail) |
| Compaction | O(total_entries) monolithic | O(dirty_entries) per-label delta |
| dirty_labels | tracked but never drained (leak) | drain_dirty_labels() per cycle |
| PDS read cache | CF edge cache 60s (stale) | No cache (always fresh from yata) |

**禁止**: `restore_from_r2` (legacy ArrowFragment page-in、除去済み)。`trigger_snapshot_inner` (legacy snapshot serialize、除去済み)。PDS `cyCached` に edge cache 再導入 (stale 原因)。monolithic compacted segment 新規作成 (v2 per-label のみ)

## CRITICAL: 3 概念は直交 — partition ≠ label ≠ security

**partition** = Container instance。YataRPC coordinator が `hash(label) % N` で label-based routing。
**label** = Cypher node type = Arrow IPC blob I/O 単位。`ensure_labels` で on-demand page-in。同一 CSR 内に全 label 同居 → cross-label query native。
**security** = GIE SecurityFilter (vertex property O(1)/vertex, CSR inline)。partition は security boundary ではない。
**禁止**: `appId = auth.org_id` (Clerk org_id は partition/label/security のいずれでもない)。

## Scale Strategy

Production: PARTITION_COUNT=1, per-label Arrow IPC, full page-in (3-tier: disk→R2), Design E SecurityScope (CSR policy vertex lookup)。1,068 tests。

### Key behaviors

- **Query** (GIE, <1us): Cypher → parse → ensure_labels (selective page-in) → GIE transpile → CSR push-based execute. No MemoryGraph fallback (GIE transpile failure = error)
- **Mutation** (~55ms with delta-apply, fallback ~500ms): MemoryGraph copy → mutate → delta-apply O(delta) for <50% change, full CSR rebuild fallback。merge_by_pk = prop_eq_index O(1)。Edge deletion = tombstone HashSet (O(1) lookup in neighbor iteration)。CpmStats: cypher_read/mutation/mergeRecord counts + mutation_avg_us + last_snapshot_serialize_ms
- **Storage**: RAM (CSR <1us) → disk cache (~100us) → R2 source of truth (~3-5ms)
- **Cold start**: **label-selective page-in** (topology + query-needed labels only)。3-tier blob fetch (disk → R2 → write-through)。後続 query で on-demand enrich (enrich_new_labels)
- **Chunk**: Arrow row-group 32 MB/chunk byte-based。1B vertices でも ~数十 chunks
- **Partition fan-out**: 1x standard-1 = ~20M nodes (production)。4x standard-1 = ~100M (E2E verified)

## Env Vars

| Env var | Default | Purpose |
|---|---|---|
| `YATA_S3_*` | (empty) | R2 endpoint/bucket/key/secret/prefix |
| `YATA_MMAP_VINEYARD` | `false` | Enable MmapBlobCache (zero-copy) |
| `YATA_DIRECT_FAN_OUT_LIMIT` | `8` | Below this, direct fan-out; above, hierarchical √N `[IMPLEMENTED]` (companion Worker adaptive routing) |
| `YATA_BATCH_COMMIT_THRESHOLD` | `10` | R2 snapshot skipped if pending_writes < threshold `[IMPLEMENTED]` (engine.rs pending_writes gate) |
| `YATA_CHUNK_TARGET_BYTES` | `33554432` (32 MB) | Arrow row-group chunk target byte size per blob。R2/S3 最適 8-64 MB |
| `YATA_CHUNK_ROWS` | (unset) | 設定時は byte-based estimation を override し固定 row 数で chunk 分割 |
| `YATA_WAL_FORMAT` | `arrow` | WAL segment format (`arrow` or `ndjson`)。Arrow = zero-copy mmap |

## Test Coverage

1,000 Rust unit tests + 68 e2e, 0 failures. E2E: 8 tests (docker-compose + MinIO, 2-partition). 6-node distributed: 6 tests (10K records, label routing, cold put/pull). Phase 3 load test: 8 tests (chunk snapshot, 2-hop traversal, label-selective reads, mixed load). YataFragment snapshot roundtrip verified. R2 persistence verified (2026-03-29): 964 vertices, 33 labels, 1.58 MB snapshot, full property columns (rkey/collection/repo/value_b64/owner_hash/updated_at/_app_id/_org_id).

## Benchmark (measured, release build, 10K records)

| Operation | In-Process | Via HTTP | Notes |
|---|---|---|---|
| **Write (mergeRecord)** | 63,715/sec | 70/sec (docker) | append-only, no page-in |
| **Edge create** | 77,092/sec | — | Cypher MATCH+CREATE |
| **Point read** | 205,539 QPS (4.9µs) | 103 QPS (6-node) | coordinator overhead 2ms |
| **1-hop traversal** | 165,397 QPS (6.0µs) | — | NbrUnit zero-copy |
| **Full scan** | 1,686,597 QPS | — | COUNT aggregate |
| **Snapshot serialize** | 15ms (10K) | 33ms (6-node total) | ArrowFragment, 2000x vs legacy |

**ArrowFragment vs Legacy (10K vertices + 20K edges):**
| | Legacy SnapshotBundle | ArrowFragment | Speedup |
|---|---|---|---|
| Serialize | 34,661ms | 15ms | **2,311x** |
| Deserialize | 57ms | <1ms | **>57x** |
| Neighbor traversal | 76ns/iter | 3ns/iter | **25.3x** |
| Blob size | 3.8MB | 1.8MB | **53% smaller** |

**6-node distributed (docker-compose, 10K records):**
| Metric | Result |
|---|---|
| Cold write 10K | 70/sec (HTTP overhead) |
| Snapshot 6 nodes | 33ms total |
| Cold pull recovery | 10,000/10,000 records |
| Distributed reads | 103 QPS |
| Partition isolation | 0 violations |

**Phase 3 load test (docker-compose, 1-partition, 1,250 vertices + 300 edges, 5 labels):**
| Metric | Result |
|---|---|
| Seed (mergeRecord via HTTP) | 178 nodes/sec |
| 2-hop traversal (Person→KNOWS→Person→WORKS_AT→Company) | **541 QPS** |
| Label-selective point reads (Person/Company) | **1,126 QPS** |
| Mixed read/write (300R + 100W) | 260 ops/sec |
| Full scan (50x) | 1,416 QPS |
| Label scan (50x) | **1,837 QPS** (30% faster than full scan) |
| Chunked snapshot (1,250 vertices) | 73-111ms |
| test: `yata-server/tests/e2e_phase3_loadtest.rs` (8 tests pass) |

**Production E2E (pds.gftd.ai → YataRPC → Container, 2026-03-25, image `20260325-1015`):**
| Metric | Result | Notes |
|---|---|---|
| Cold start (container wake) | **2.8s** | Container sleep → wake + R2 page-in |
| searchActors (warm, 20x avg) | **424ms** | PDS → YataRPC → Container → Cypher → edge cache |
| getTimeline (2-hop) | **271ms** | Graph traversal through PDS |
| listRecords (label-specific) | **371ms** | Label-selective page-in path |
| createRecord | 63ms (auth required) | External write blocked — correct security |

**Trillion scale projection (from 10K benchmark):**
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
  --release --target x86_64-unknown-linux-gnu -p yata-server  # ~1m24s
```

- **TLS**: `ureq` + `rustls` (ring backend)。`aws-lc-sys` / `reqwest` は除去済み (cross-compile 障害)
- **sccache 禁止**: `RUSTC_WRAPPER=""` 必須 (cc-rs が sccache 経由で C compiler を探して失敗)
- **rest.rs 変更後は必ず rebuild** → バイナリが古いと `/xrpc/ai.gftd.yata.cypher` が 404

## PDS Dispatch Fixes (2026-03-25)

**R2 永続化 verified**: R2 に YataFragment 12 blobs, 10 vertex labels, 684 vertices 存在確認済み。

| Issue | Fix | Location |
|---|---|---|
| `collectionToLabel` snake_case 未対応 | `.split(/[-_]/)` で snake_case + kebab-case 両対応 | `pds-helpers.ts:223` |
| `buildProfileView` displayName fallback = DID | fallback を `didToHandle(actor)` に変更 (handle 表示) | `pds-helpers.ts:315` |
| `AppBskyActorGetProfile` が `Profile` label のみ参照 (R2 未永続化) | structured `Profile` (R2 永続化) へ fallback 追加。display_name/description を補完 | `pds-dispatch.ts:910-927` |
| label consistency テスト未整備 | `pds-helpers.test.ts` に snake_case テスト 5 件 + label consistency check 追加 (214 tests pass) | `pds-helpers.test.ts` |

**診断手順 (label ↔ collection mismatch)**:
```bash
# R2 schema の label 一覧
npx wrangler r2 object get ai-gftd-graph/yata/snap/fragment/schema --remote --file /tmp/schema.json
python3 -c "import json; [print(e['label']) for e in json.load(open('/tmp/schema.json'))['vertex_entries']]"
```

## 禁止事項

- **R2 以外を source of truth にする禁止** — R2 が正本
- **JSON RPC で graph data 転送禁止** — Workers RPC (structured clone) + ArrayBuffer
- **lite instance 禁止** — standard-1 以上 (lite は CSR rebuild 遅すぎ)
- **`reqwest` crate 再追加禁止** — `aws-lc-sys` (OpenSSL/BoringSSL C cross-compile) を引き込む。`ureq` + `rustls` を使用
- **`RUSTC_WRAPPER=sccache` での cross-compile 禁止** — `cargo zigbuild` を使用
