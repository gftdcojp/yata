# packages/rust/yata

yata — Rust Cypher graph engine. `[PRODUCTION]` Container (CSR + DiskVineyard/MmapVineyard) × 1 partition. Workers RPC coordinator. GraphScope parity 21/25 (14 PRODUCTION + 7 IMPLEMENTED)。Vineyard + GIE push-based + SecurityFilter RLS + Arrow row-group chunk + label-selective page-in。

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

Read (ArrowFragment page-in, lazy):
  PDS_RPC.query / listRecords / getTimeline
    → YATA_RPC.cypher → TieredGraphEngine
    → ensure_labels() → hot_initialized == false?
      → page_in_from_r2() → R2 GET snap/fragment/{name} → ArrowFragment → NbrUnit zero-copy CSR
      → hot_initialized = true
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
| `yata-engine` | TieredGraphEngine, ArrowFragment snapshot (trigger_snapshot → R2 + disk), 3-tier page-in (disk cache → R2 → CSR, `fetch_blob_cached`), label-selective page-in (`page_in_selective_from_r2`), incremental enrichment (`enrich_label_from_r2`), Frontier BFS, ShardedCoordinator |
| `yata-cypher` | Full Cypher parser + executor (incl. untyped edge traversal) |
| `yata-gie` | GIE push-based executor, IR (Exchange/Receive/Gather), distributed planner |
| `yata-s3` | R2 persistence (sync ureq+rustls S3 client, SigV4)。`trigger_snapshot()` → R2 PUT、page-in → R2 GET |
| `yata-vex` | Vector index (IVF_PQ + DiskANN) |
| `yata-bench` | Benchmarks + trillion-scale test |
| `yata-server` | XRPC API server (`/xrpc/ai.gftd.yata.cypher` + `triggerSnapshot`)。GraphQueryExecutor trait |
| ~~`yata-flight`~~ | **除去済み** — Cypher direct が標準 |
| ~~`yata-cbor`~~ | **除去済み** — AT Protocol dag-cbor。`wproto::cbor` が authoritative |
| ~~`yata-git`~~ | **除去済み** — k8s era legacy。`infra/workers/git-server/` に移行 |
| ~~`yata-at/signal/mdag/cas`~~ | **除去済み** — AT/Signal/MDAG/CAS は TS `wproto` に移行 |

## GraphScope Parity

| GraphScope | Role | yata | Status Tag |
|---|---|---|---|
| Cypher | query language | yata-cypher | `[PRODUCTION]` |
| GAIA compiler | Cypher→IR | yata-gie/transpile | `[PRODUCTION]` |
| GraphIR | intermediate repr | yata-gie/ir (Exchange/Receive/Gather) | `[PRODUCTION]` |
| Optimizer | query optimization | yata-gie/optimizer | `[PRODUCTION]` |
| Hiactor (OLTP) | low-latency exec | yata-gie/executor (push-based) | `[PRODUCTION]` |
| Pegasus (OLAP) | distributed dataflow | yata-gie/distributed_executor | `[STUB]` コード存在、呼び出し元なし |
| GRIN | storage interface | yata-grin | `[PRODUCTION]` |
| Vineyard (shm) | memory store | MmapVineyard (memmap2) | `[PRODUCTION]` |
| Vineyard (Arrow) | data format | Arrow IPC blobs | `[PRODUCTION]` |
| mCSR | in-memory topology | MutableCsrStore | `[PRODUCTION]` |
| Groot (WAL) | persistent WAL | Pipeline + mergeRecord (PDS Worker) | `[PRODUCTION]` |
| Groot MVCC | versioned snapshots | Snapshot (Arrow IPC → R2) | `[PRODUCTION]` |
| GraphAr | file format | R2 Arrow IPC | `[PRODUCTION]` |
| HDFS/S3 backend | external storage | R2 (S3 compatible) | `[PRODUCTION]` |
| Edge-cut partition | partitioning | PartitionStoreSet (hash) | `[IMPLEMENTED]` docker-compose E2E |
| Mirror vertex | cross-partition | MirrorRegistry (2-hop) | `[IMPLEMENTED]` |
| Frontier exchange | distributed BFS | frontier.rs | `[IMPLEMENTED]` |
| Data shuffle | partition exchange | yata-gie distributed_executor | `[STUB]` コード存在、transport なし |
| Hierarchical coord | query routing | YataRPC coordinator | `[IMPLEMENTED]` √N logic 存在、production は 1 partition |
| Groot PK index | O(1) vertex lookup | merge_by_pk + prop_eq_index | `[IMPLEMENTED]` mergeRecord のみ、Cypher MERGE 未対応 |
| Hash partition routing | vertex → partition | partition.rs PartitionAssignment::Hash | `[STUB]` 定義のみ、mutation flow 未結線 |
| Chunk page-in | granular I/O | ArrowFragment per-label blob | `[PRODUCTION]` per-label 単位 (chunk 単位は `[STUB]`) |
| Arrow row-group chunk | sub-label granular I/O | `split_record_batch` + byte-based chunked serialize/deserialize | `[IMPLEMENTED]` 32 MB/chunk default (byte-based), 9 tests pass。Selective chunk page-in API ready (`page_in_vertex_chunk_from_r2`) |
| ~~LSMGraph tiered storage~~ | ~~multi-level auto~~ | — | `[DEPRECATED]` Arrow + snapshot compaction が LSM 本質を実現済み。置換: Arrow row-group chunk |
| GART mutable CSR | HTGAP | MutableCsrStore + merge_by_pk | `[IMPLEMENTED]` mergeRecord のみ |
| Distributed disk pool | cross-partition I/O | — | `[DESIGN]` コード 0 行 |
| Gremlin | query language | — | not planned |
| GRAPE (analytics) | vertex-centric | — | not planned |
| GLE (learning) | GNN | — | not planned |

**GraphScope parity summary**: 14 `[PRODUCTION]` + 7 `[IMPLEMENTED]` + 2 `[STUB]` + 1 `[DESIGN]` + 1 `[DEPRECATED]` + 3 not planned.

## R2 Persistence `[PRODUCTION]`

R2 = source of truth。**Append-only write**: mergeRecord は page-in 不要 (in-memory CSR に append のみ)。PK dedup は in-memory 内のみ。R2 既存データとの dedup は snapshot compaction 時。**Dirty tracking**: dirty flag が true の時のみ snapshot upload。**Snapshot compaction**: R2 既存 + in-memory pending → merge by PK → ArrowFragment → R2 PUT。**Partial page-in protection**: `last_snapshot_count` で上書き防止。**Name-based blob** (CAS 除去): `snap/fragment/{name}` で直接 PUT/GET。Blake3 hash 不要。**3-tier page-in `[IMPLEMENTED]`**: `fetch_blob_cached()` が disk cache (`YATA_VINEYARD_DIR/snap/fragment/`) → R2 GET → write-through to disk の順で blob を取得。Cold start with warm disk: ~100µs/blob (vs R2 ~3-5ms)。`trigger_snapshot` が disk にも書くため、Container restart 時は disk cache → R2 skip で高速復旧。**Arrow row-group chunk `[IMPLEMENTED]`**: 大きい vertex/edge table は byte-based で自動分割 (default 32 MB/chunk)。`estimate_bytes_per_row()` が Arrow buffer size から行単価を推定 → `target_bytes / bytes_per_row` で chunk row 数算出 (clamp [1K, 10M])。R2 key: `vertex_table_{i}_chunk_{j}` / meta field: `vertex_table_{i}_chunks`。Old single-blob format は deserialize 時に自動検出 (backward compat)。1B vertices でも ~数十 chunks (S3/R2 10億ファイル問題回避)。

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

## CRITICAL: 3 概念は直交 — partition ≠ label ≠ security

**partition** = Container instance。YataRPC coordinator が `hash(label) % N` で label-based routing。
**label** = Cypher node type = Arrow IPC blob I/O 単位。`ensure_labels` で on-demand page-in。同一 CSR 内に全 label 同居 → cross-label query native。
**security** = GIE SecurityFilter (vertex property O(1)/vertex, CSR inline)。partition は security boundary ではない。
**禁止**: `appId = auth.org_id` (Clerk org_id は partition/label/security のいずれでもない)。

## Scale Strategy — 4 Phase

### Facts (検証済み — `[PRODUCTION]` / `[IMPLEMENTED]`)

```
`[PRODUCTION]` Phase 1: Per-label Arrow IPC (PARTITION_COUNT=1)
  evidence: wrangler.jsonc → YATA_PARTITION_COUNT: "1"
  evidence: yata-engine/src/engine.rs (page_in_from_r2, trigger_snapshot)
  test: 852 unit tests + yata-server/tests/e2e_minio.rs (8 tests)

  `[PRODUCTION]` per-label R2 page-in
    evidence: yata-engine/src/engine.rs (ensure_labels → page_in_from_r2)
    test: yata-server/tests/e2e_minio.rs

  `[IMPLEMENTED]` label-based multi-partition routing (E2E verified at PARTITION_COUNT=2, docker-compose)
    evidence: infra/cloudflare/container/yata/src/index.ts (extractLabelHints + labelToPartition)
    test: yata-server/tests/e2e_6node.rs (6 tests, 10K records)
    note: production は PARTITION_COUNT=1 で運用中

  `[PRODUCTION]` ArrowFragment snapshot + cold restart page-in
    evidence: yata-vineyard/src/fragment.rs (csr_to_fragment, ArrowFragment::serialize/deserialize)
    evidence: yata-s3/src/s3.rs (R2 PUT/GET)
    test: yata-server/tests/e2e_minio.rs + yata-vineyard/src/tests.rs (9 tests)

  `[PRODUCTION]` GIE SecurityFilter (RLS)
    evidence: yata-gie/src/security.rs (transpile_secured, vertex_passes_security)
    test: 852 tests pass (sensitivity_ord + owner_hash auto-injected)

  `[IMPLEMENTED]` merge_by_pk with prop_eq_index (HashMap O(1) lookup)
    evidence: yata-store/src/mutable_csr.rs (merge_by_pk)
    note: single mergeRecord のみ。Cypher MERGE は MemoryGraph copy + CSR rebuild (~500ms)
```

### Design (未実装 — `[STUB]` / `[DESIGN]`)

```
`[STUB]` Phase 2: hash(pk_value) % partition_count routing
  evidence: yata-engine/src/partition.rs (PartitionAssignment::Hash 定義あり)
  status: 定義のみ。mutation flow に結線されていない。呼び出し元 0

`[STUB]` Phase 3: serialize_snapshot_chunked
  evidence: yata-engine/src/engine.rs (関数定義あり)
  status: 関数定義のみ。呼び出し元 0。現在は per-label 全体 load

`[DESIGN]` Phase 3a: Read replica routing
  status: コード 0 行。wrangler.jsonc YATA_READ_REPLICA_COUNT=0。index.ts に routing logic なし

`[IMPLEMENTED]` Phase 3b: Arrow row-group chunk page-in
  evidence: yata-vineyard/src/fragment.rs (split_record_batch, serialize_with_byte_target, resolve_chunk_rows, serialize_table_chunked)
  evidence: yata-engine/src/loader.rs (page_in_vertex_chunk_from_r2, fetch_fragment_meta, vertex_label_chunk_count)
  test: yata-vineyard/src/tests.rs (9 new tests: split_*, chunked_*, byte_based_*)
  default: **byte-based** — 32 MB target per chunk (DEFAULT_CHUNK_TARGET_BYTES)。
    estimate_bytes_per_row() が Arrow buffer size から行あたりバイト数を推定 →
    target_bytes / bytes_per_row → chunk row 数を算出。clamp [1K, 10M] rows。
    YATA_CHUNK_TARGET_BYTES (byte target) / YATA_CHUNK_ROWS (row override) で上書き可能。
  R2 最適化: 32 MB/chunk → 1B vertices でも ~数十 chunks (10億ファイル問題回避)
  backward-compat: old single-blob format auto-detected on deserialize
  note: production は default byte target (32MB) で snapshot

`[IMPLEMENTED]` Phase 3c: Label-selective page-in (ensure_labels)
  evidence: yata-engine/src/loader.rs (page_in_selective_from_r2, restore_csr_selective, enrich_label_from_r2, apply_batch_properties, extract_arrow_value)
  evidence: yata-engine/src/engine.rs (ensure_labels_vineyard_only rewritten, enrich_new_labels, cached_meta/cached_schema/label_vid_offsets)
  mechanism:
    1st call: fetch meta+schema+ivnums+ALL CSR topology + only requested label vertex_tables
              non-requested labels → stub vertices (VID ordering preserved, 0 properties, 0 R2 fetch)
    subsequent: new label → fetch vertex_table → set_vertex_prop on existing stubs (incremental)
    no label hints → full page-in fallback (backward compat)
  state tracking: loaded_labels (property-loaded labels), cached_meta/schema (for incremental enrichment), label_vid_offsets
  3-tier blob fetch: fetch_blob_cached() — disk cache (YATA_VINEYARD_DIR) → R2 GET → write-through。Container restart with warm disk: ~100µs/blob (R2 skip)
  test: 854 workspace unit tests pass + e2e_phase3_loadtest (8 tests, docker-compose: 2-hop 541 QPS, label-selective 1,126 QPS)

`[DEPRECATED]` LSMGraph 4-level tiered storage
  reason: 現 ArrowFragment + snapshot compaction が LSM の本質 (immutable sorted runs + compaction) を
  既に実現。Arrow columnar format と LSM row-oriented sorted runs の impedance mismatch。
  CF Container ephemeral disk で multi-level local storage の意味が薄い。
  置換先: Arrow row-group chunk page-in (Phase 3b)

`[DESIGN]` Phase 4: Distributed disk pool (cross-partition Workers RPC I/O)
  status: コード 0 行
```

### Implementation State (CRITICAL)

```
Query fast path (GIE, <1µs): [PRODUCTION]
  Cypher → parse → extract_pushdown_hints → ensure_labels (selective page-in) →
  GIE transpile → IR Plan → CSR direct push-based execute
  CONDITIONS: single CSR partition
  RLS: transpile_secured() injects SecurityFilter after Scan/Expand ops
  Page-in: label-selective (Phase 3c) — needed labels get properties, others get stubs

Query GIE fallback → MemoryGraph (~1-200ms): [PRODUCTION]
  GIE transpile fails (mutation or unsupported Cypher) → MemoryGraph copy → Cypher execute

Mutation (~500ms): [PRODUCTION]
  Cypher → MemoryGraph copy → mutate → inject metadata → CSR rebuild
  OR: merge_by_pk → prop_eq_index O(1) → property update → CSR.commit()

Storage (3-tier + R2): [IMPLEMENTED]
  Level 0: RAM (MutableCsrStore CSR) — <1µs
  Level 1: Vineyard ephemeral (DiskVineyard/MmapVineyard) — ~100µs
  R2: Source of truth (Arrow IPC per-label) — ~3-5ms page-in
  Level 2 (distributed disk pool): [DESIGN] コード 0 行
  Level 3 (Arrow row-group chunk page-in): [IMPLEMENTED] 32 MB/chunk byte-based, selective API ready

Partition fan-out: [PRODUCTION] = Level 0 + Level 1 のみ
  1 × standard-1     = ~20M nodes   (production 運用中)
  4 × standard-1     = ~100M nodes  (docker-compose E2E verified)
  20+ × standard-1   = ~500M+ nodes (未検証、理論値)

Read latency:
  Level 0 hit: <1µs (CSR) — [PRODUCTION]
  Level 1 hit: ~100µs (mmap) — [PRODUCTION]
  Level 2 hit: ~1ms (Workers RPC) — [DESIGN]
  Level 3 hit: ~2-5ms (R2 GET, Arrow row-group chunk) — [IMPLEMENTED]
```

## Env Vars

| Env var | Default | Purpose |
|---|---|---|
| `YATA_S3_*` | (empty) | R2 endpoint/bucket/key/secret/prefix |
| `YATA_MMAP_VINEYARD` | `false` | Enable MmapVineyard (zero-copy) |
| `YATA_DIRECT_FAN_OUT_LIMIT` | `8` | Below this, direct fan-out; above, hierarchical √N |
| `YATA_BATCH_COMMIT_THRESHOLD` | `10` | R2 snapshot upload triggers after this many WAL commits |
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

## 禁止事項

- **R2 以外を source of truth にする禁止** — R2 が正本
- **JSON RPC で graph data 転送禁止** — Workers RPC (structured clone) + ArrayBuffer
- **lite instance 禁止** — standard-1 以上 (lite は CSR rebuild 遅すぎ)
- **`reqwest` crate 再追加禁止** — `aws-lc-sys` (OpenSSL/BoringSSL C cross-compile) を引き込む。`ureq` + `rustls` を使用
- **`RUSTC_WRAPPER=sccache` での cross-compile 禁止** — `cargo zigbuild` を使用
