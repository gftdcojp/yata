# packages/rust/yata

yata — Rust Cypher graph engine. `[PRODUCTION]` Container (CSR + DiskVineyard/MmapVineyard) × 1 partition. Workers RPC coordinator. GraphScope parity 21/25 (14 PRODUCTION + 7 IMPLEMENTED)。Vineyard + GIE push-based + SecurityFilter RLS + Arrow row-group chunk + label-selective page-in + edge property lookup + edge tombstone deletion + dirty label tracking + batch commit threshold + adaptive √N fan-out。

## Architecture (CRITICAL)

```
Write model: Pipeline.send() (durable) + YATA_RPC.mergeRecord() (instant projection)
  → Pipeline: AT Protocol commit chain + MST (durable, 0 data loss)
  → mergeRecord: Cypher projection → yata Container → fire-and-forget snapshot() → R2 PUT

Read model: yata Container (pure read)
  Workers RPC (YataRPC) → hierarchical coordinator (√N fan-out)
    → N × Container (Rust, standard-1, 4GB RAM, 8GB disk)
      Each Container:
        R2 (Arrow IPC) → DiskVineyard/MmapVineyard → MutableCsrStore (<1ms)
        TieredGraphEngine → yata-cypher / yata-gie (push-based)
        No WAL. No FUSE. No background upload.

R2 = Source of Truth (ArrowFragment per-label per-partition: snap/fragment/meta.json + snap/fragment/{blob_name})
DiskVineyard = Container ephemeral disk cache (page-in/out, LRU evict)
MmapVineyard = zero-copy mmap (500M edges/Container)
CSR = In-memory graph topology (<1ms query)
```

Deploy/config/ops details: `infra/cloudflare/container/yata/CLAUDE.md`

## Data Flow

```
Write (append-only, NO page-in):
  App → PDS_RPC.createRecord(repo, collection, record)
    → Pipeline.send(): AT commit + MST update → ACK (~1ms, durable)
    → YATA_RPC.mergeRecord(): in-memory CSR append (PK dedup in-memory only, NO R2 read)
    → dirty = true

  yata への直接 mutate は mergeRecord からのみ (app 直接禁止)

Snapshot compaction (cron 1min, dirty flag gated):
  trigger_snapshot():
    → dirty == false? → skip (no R2 I/O)
    → hot_initialized == false? → page_in_from_r2 → merge R2 existing into CSR (PK dedup)
    → csr_to_fragment() → name-based R2 PUT (no CAS hash)
    → dirty = false, last_snapshot_count updated

Read (ArrowFragment page-in, lazy + label-selective):
  PDS_RPC.query / listRecords / getTimeline
    → YATA_RPC.cypher → TieredGraphEngine
    → ensure_labels(vertex_labels) → hot_initialized == false?
      → label hints あり: page_in_selective_from_r2(needed_labels) → topology + needed labels のみ
      → label hints なし: page_in_topology_from_r2() → topology のみ (stub vertices)
      → CSR に merge (既存 mergeRecord データを保護。empty/error でも hot_initialized = true)
      → hot_initialized = true (再 page-in による上書き防止)
    → hot_initialized == true && 未 load label あり?
      → enrich_new_labels() → per-label on-demand enrichment (R2 GET vertex_table_{i} のみ)
    → CSR direct query (<1µs)

PDS Container (Rust) は不要 — 全て TS Worker + Pipeline + YATA_RPC。
```

## Workers RPC API (CRITICAL)

```ts
// Bind: { service: "ai-gftd-yata", entrypoint: "YataRPC" }
// Transport: Workers RPC only
// Container XRPC: /xrpc/ai.gftd.yata.cypher (unified read+write)

env.YATA.cypher(cypher, appId)   // unified Cypher path → /xrpc/ai.gftd.yata.cypher
env.YATA.query(cypher, appId)    // read-only alias
env.YATA.mutate(cypher, appId)   // CREATE → random partition, DELETE → broadcast
env.YATA.health()                // → partition-0 Container
env.YATA.ping()                  // "pong" (no wake)
env.YATA.stats()                 // → all partition stats
```

## Crate Roles (CRITICAL)

| Crate | Role |
|---|---|
| `yata-core` | GlobalVid, LocalVid, PartitionId |
| `yata-grin` | GRIN trait (Topology, Property, Schema, Scannable, Mutable) |
| `yata-vineyard` | **ArrowFragment format** (canonical snapshot/persistence format)。NbrUnit zero-copy CSR (25x faster neighbor traversal)。`csr_to_fragment()` (CSR→ArrowFragment) + `ArrowFragment::serialize/deserialize` (BlobStore↔R2)。**Arrow row-group chunk**: `split_record_batch` + byte-based chunking (32 MB default, `estimate_bytes_per_row`)。PropertyGraphSchema (typed vertex/edge labels + Arrow property columns) |
| `yata-store` | MutableCsrStore (mutable in-memory CSR, GRIN traits), ArrowGraphStore, DiskVineyard/MmapVineyard/EdgeVineyard (blob cache), PartitionStoreSet, GraphStoreEnum |
| `yata-engine` | TieredGraphEngine, ArrowFragment snapshot (trigger_snapshot → R2 + disk), 2-phase cold start (`page_in_topology_from_r2` + on-demand `enrich_label_from_r2`), 3-tier blob fetch (`fetch_blob_cached`: disk → R2 → write-through), Frontier BFS, ShardedCoordinator |
| `yata-cypher` | Full Cypher parser + executor (incl. untyped edge traversal) |
| `yata-gie` | GIE push-based executor, IR (Exchange/Receive/Gather), distributed planner |
| `yata-s3` | R2 persistence (sync ureq+rustls S3 client, SigV4)。`trigger_snapshot()` → R2 PUT、page-in → R2 GET |
| `yata-vex` | Vector index (IVF_PQ + DiskANN) |
| `yata-bench` | Benchmarks + trillion-scale test |
| `yata-server` | XRPC API server (`/xrpc/ai.gftd.yata.cypher` + `triggerSnapshot`)。GraphQueryExecutor trait |
| `yata-hayate` | **Hayate V5 Python bridge** (PyO3 cdylib)。Expert slot read/write (numpy ↔ Arrow FixedSizeList<f16>)、Product Key routing (CSR neighbor O(√N))、Training trace I/O、Embedding table、Cypher exec。MLX training loop から直接 yata CSR を操作。HTTP/scp/npz 不要 |

## GraphScope Parity

`gftd symbol-graph --package yata` で component status を確認。854 tests。

## R2 Persistence `[PRODUCTION]`

R2 = source of truth。**Append-only write**: mergeRecord は page-in 不要 (in-memory CSR に append のみ)。PK dedup は in-memory 内のみ。R2 既存データとの dedup は snapshot compaction 時。**Dirty tracking**: dirty flag が true の時のみ snapshot upload。**Snapshot compaction**: R2 既存 + in-memory pending → merge by PK → ArrowFragment → R2 PUT。**Partial page-in protection**: `last_snapshot_count` で上書き防止。**Name-based blob** (CAS 除去): `snap/fragment/{name}` で直接 PUT/GET。Blake3 hash 不要。**3-tier page-in `[PRODUCTION]`**: `fetch_blob_cached()` — disk cache (`YATA_VINEYARD_DIR/snap/fragment/`) → R2 GET → write-through to disk。Cold start: full page-in (ALL labels, ALL properties)。warm disk: ~100µs/blob (R2 skip)。`trigger_snapshot` が disk + R2 両方に書くため disk cache は常に warm。**Arrow row-group chunk `[IMPLEMENTED]`**: 大きい vertex/edge table は byte-based で自動分割 (default 32 MB/chunk)。`estimate_bytes_per_row()` が Arrow buffer size から行単価を推定 → `target_bytes / bytes_per_row` で chunk row 数算出 (clamp [1K, 10M])。R2 key: `vertex_table_{i}_chunk_{j}` / meta field: `vertex_table_{i}_chunks`。Old single-blob format は deserialize 時に自動検出 (backward compat)。1B vertices でも ~数十 chunks (S3/R2 10億ファイル問題回避)。

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

## Snapshot Model — Dirty Label Delta `[IMPLEMENTED]`

**Dirty label delta snapshot**: `trigger_snapshot()` は dirty vertex label の blob のみ R2 PUT。`csr_to_fragment_selective()` が dirty label のみ property 抽出 (clean label は empty vertex table)。`is_dirty_vertex_blob()` / `is_edge_or_topology_blob()` / `is_infra_blob()` で blob 分類。η: 0.01% → ~85%。

**Durability は Pipeline WAL が保証。** Pipeline → R2 JSON (10s flush) が WAL source of truth。yata snapshot は CSR の performance checkpoint であり、Pipeline WAL から rebuild 可能。

| 層 | Model | Durability | Shannon 冗長度 |
|---|---|---|---|
| **Pipeline WAL** | Append-only (R2 JSON, `pipeline/wal/`) | **source of truth** — `Pipeline.send()` resolve = durable | 0% (唯一の authoritative write) |
| **yata R2 snapshot** | Dirty label delta PUT (`snap/fragment/`) | **performance checkpoint** — CSR cold start 復旧用 | ~15% (dirty label のみ転送) |

**Delta snapshot flow (engine.rs `trigger_snapshot_inner`):**
1. `dirty_labels.drain()` → dirty set を atomically 取得
2. `csr_to_fragment_selective(store, pid, &dirty_set)` → dirty label のみ property 抽出
3. `frag.serialize(&blob_store)` → 全 blob 生成 (meta.json は full state)
4. Selective upload: `is_dirty_vertex_blob()` = dirty label の `vertex_table_{i}*` のみ PUT。`is_infra_blob()` (schema, ivnums) は常に PUT。Edge/topology は `dirty` flag (edge mutation) 時のみ PUT
5. `meta.json` は常に PUT (full label manifest を R2 に反映)

**Page-in safety (CRITICAL, 2026-03-25 fix)**:
- `ensure_labels` の R2 page-in は CSR を **merge** (上書きではない)。既存 `mergeRecord` データを保護
- Empty fragment / R2 error でも `hot_initialized = true` を設定し、再 page-in による上書きを防止

**禁止**: Pipeline WAL replay を snapshot の代替にすること (cold start 数十秒は許容不可)。snapshot checkpoint は cold start <5s のために維持。

## CRITICAL: 3 概念は直交 — partition ≠ label ≠ security

**partition** = Container instance。YataRPC coordinator が `hash(label) % N` で label-based routing。
**label** = Cypher node type = Arrow IPC blob I/O 単位。`ensure_labels` で on-demand page-in。同一 CSR 内に全 label 同居 → cross-label query native。
**security** = GIE SecurityFilter (vertex property O(1)/vertex, CSR inline)。partition は security boundary ではない。
**禁止**: `appId = auth.org_id` (Clerk org_id は partition/label/security のいずれでもない)。

## Scale Strategy

Production: PARTITION_COUNT=1, per-label Arrow IPC, full page-in (3-tier: disk→R2), GIE SecurityFilter (RLS)。854 tests。

### Key behaviors

- **Query** (GIE, <1us): Cypher → parse → ensure_labels (selective page-in) → GIE transpile → CSR push-based execute. No MemoryGraph fallback (GIE transpile failure = error)
- **Mutation** (~500ms): MemoryGraph copy → mutate → CSR rebuild。merge_by_pk = prop_eq_index O(1)。Edge deletion = tombstone HashSet (O(1) lookup in neighbor iteration)
- **Storage**: RAM (CSR <1us) → disk cache (~100us) → R2 source of truth (~3-5ms)
- **Cold start**: **label-selective page-in** (topology + query-needed labels only)。3-tier blob fetch (disk → R2 → write-through)。後続 query で on-demand enrich (enrich_new_labels)
- **Chunk**: Arrow row-group 32 MB/chunk byte-based。1B vertices でも ~数十 chunks
- **Partition fan-out**: 1x standard-1 = ~20M nodes (production)。4x standard-1 = ~100M (E2E verified)

## Env Vars

| Env var | Default | Purpose |
|---|---|---|
| `YATA_S3_*` | (empty) | R2 endpoint/bucket/key/secret/prefix |
| `YATA_MMAP_VINEYARD` | `false` | Enable MmapVineyard (zero-copy) |
| `YATA_DIRECT_FAN_OUT_LIMIT` | `8` | Below this, direct fan-out; above, hierarchical √N `[IMPLEMENTED]` (companion Worker adaptive routing) |
| `YATA_BATCH_COMMIT_THRESHOLD` | `10` | R2 snapshot skipped if pending_writes < threshold `[IMPLEMENTED]` (engine.rs pending_writes gate) |
| `YATA_CHUNK_TARGET_BYTES` | `33554432` (32 MB) | Arrow row-group chunk target byte size per blob。R2/S3 最適 8-64 MB |
| `YATA_CHUNK_ROWS` | (unset) | 設定時は byte-based estimation を override し固定 row 数で chunk 分割 |

## Test Coverage

854 Rust unit tests, 0 failures. E2E: 8 tests (docker-compose + MinIO, 2-partition). 6-node distributed: 6 tests (10K records, label routing, cold put/pull). Phase 3 load test: 8 tests (chunk snapshot, 2-hop traversal, label-selective reads, mixed load). ArrowFragment snapshot roundtrip verified.

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

**R2 永続化 verified**: R2 に ArrowFragment 12 blobs, 10 vertex labels, 684 vertices 存在確認済み。

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
