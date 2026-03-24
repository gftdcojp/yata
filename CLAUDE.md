# packages/rust/yata

yata — Rust Cypher graph engine. Container (CSR + DiskVineyard/MmapVineyard) × N partition. Workers RPC coordinator with hierarchical fan-out. GraphScope partial parity 16/23 (Vineyard + GIE push-based + SecurityFilter RLS).

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
// Transport: Workers RPC only (gRPC 除去済み)
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
| `yata-vineyard` | **ArrowFragment format** (canonical snapshot/persistence format)。NbrUnit zero-copy CSR (25x faster neighbor traversal)。`csr_to_fragment()` (CSR→ArrowFragment) + `ArrowFragment::serialize/deserialize` (BlobStore↔R2)。PropertyGraphSchema (typed vertex/edge labels + Arrow property columns) |
| `yata-store` | MutableCsrStore (mutable in-memory CSR, GRIN traits), ArrowGraphStore, DiskVineyard/MmapVineyard/EdgeVineyard (blob cache), PartitionStoreSet, GraphStoreEnum |
| `yata-engine` | TieredGraphEngine, ArrowFragment snapshot (trigger_snapshot → R2), page-in (R2 → ArrowFragment → CSR), Frontier BFS, ShardedCoordinator |
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

| GraphScope | Role | yata | Status |
|---|---|---|---|
| Cypher | query language | yata-cypher | done |
| GAIA compiler | Cypher→IR | yata-gie/transpile | done |
| GraphIR | intermediate repr | yata-gie/ir (Exchange/Receive/Gather) | done |
| Optimizer | query optimization | yata-gie/optimizer | done |
| Hiactor (OLTP) | low-latency exec | yata-gie/executor (push-based) | done |
| Pegasus (OLAP) | distributed dataflow | yata-gie/distributed_executor | code exists, NOT WIRED (single-partition only in practice) |
| GRIN | storage interface | yata-grin (Topology/Property/Schema/Scannable/Mutable) | done |
| Vineyard (shm) | memory store | MmapVineyard (memmap2) | done |
| Vineyard (Arrow) | data format | Arrow IPC blobs | done |
| mCSR | in-memory topology | MutableCsrStore | done |
| Groot (RocksDB WAL) | persistent WAL | Pipeline + mergeRecord (PDS Worker) | done |
| Groot MVCC | versioned snapshots | Snapshot (Arrow IPC → R2) | done |
| GraphAr | file format | R2 Arrow IPC | done |
| HDFS/S3 backend | external storage | R2 (S3 compatible) | done |
| Edge-cut partition | partitioning | PartitionStoreSet (hash) | done |
| Mirror vertex | cross-partition | MirrorRegistry (2-hop) | done |
| Frontier exchange | distributed BFS | frontier.rs | done |
| Data shuffle | partition exchange | yata-gie distributed_executor (yata-exchange removed) | code exists, NOT WIRED (no transport) |
| Hierarchical coord | query routing | YataRPC (√N fan-out) | done |
| R2 FUSE mount | persistence I/O | **除去済み** — R2 direct GET (page-in) + trigger_snapshot (PUT) | done (FUSE removed) |
| Groot PK index | O(1) vertex lookup | `merge_by_pk` + `prop_eq_index` (HashMap) | partial (Phase 2) — single mergeRecord only, NOT Cypher MERGE |
| Hash partition routing | vertex → partition | `hash(pk_value) % partition_count` code in partition.rs | NOT WIRED — code exists but not wired into mutation flow |
| Chunk page-in | granular I/O | ArrowFragment per-(vlabel,elabel) blob — NbrUnit zero-copy CSR | done (ArrowFragment replaces legacy chunked Arrow IPC) |
| LSMGraph tiered storage | multi-level auto | Level 0 RAM → Level 1 disk → Level 2 remote disk → Level 3 R2 | NOT IMPLEMENTED (design only) |
| GART mutable CSR | HTGAP | MutableCsrStore + merge_by_pk + Pipeline epoch | partial (Phase 2) |
| Distributed disk pool | cross-partition I/O | YataRPC.getChunk → remote partition MmapVineyard | NOT IMPLEMENTED (design only) |
| Gremlin | query language | — | not planned |
| GRAPE (analytics) | vertex-centric | — | not planned |
| GLE (learning) | GNN | — | not planned |

**GraphScope parity: 17/23 done** + 2 partial (Groot PK, GART) + 3 code-exists-NOT-WIRED (Pegasus, Data shuffle, Hash routing) + 2 design-only (LSMGraph, Distributed disk pool). 3 not planned (Gremlin/GRAPE/GLE).

## R2 Persistence (VERIFIED)

R2 = source of truth。**Append-only write**: mergeRecord は page-in 不要 (in-memory CSR に append のみ)。PK dedup は in-memory 内のみ。R2 既存データとの dedup は snapshot compaction 時。**Dirty tracking**: dirty flag が true の時のみ snapshot upload。**Snapshot compaction**: R2 既存 + in-memory pending → merge by PK → ArrowFragment → R2 PUT。**Partial page-in protection**: `last_snapshot_count` で上書き防止。**Name-based blob** (CAS 除去): `snap/fragment/{name}` で直接 PUT/GET。Blake3 hash 不要。**Read page-in**: `hot_initialized=false` (default) → first query triggers `page_in_from_r2()` → R2 GET → ArrowFragment → CSR。

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

```
Phase 1 (数億): Per-label Arrow IPC ✓ IMPLEMENTED
  collection → label (Post, Like, Article)
  per-label Arrow IPC blob in R2
  ensure_labels loads only needed label O(label size) — VERIFIED

  Label-based multi-partition routing — E2E VERIFIED (2-partition)
    extractLabelHints + hash(label) % N in YataRPC coordinator — IMPLEMENTED
    PARTITION_COUNT=2 E2E: MERGE + query across partitions PASS (cold start ~30s)
    Production: PARTITION_COUNT=1 (single Container, <20M nodes 推奨)

  ArrowFragment snapshot — E2E VERIFIED (2026-03-24, docker-compose + MinIO)
    trigger_snapshot → csr_to_fragment → ArrowFragment::serialize → R2 PUT (2000x faster)
    page_in_from_r2 → R2 GET → ArrowFragment::deserialize → restore_csr_from_fragment (NbrUnit zero-copy)
    Per-partition snapshot: each Container writes to yata/partitions/{N}/snap/fragment/
    Cold restart page-in: verified via docker-compose restart
    mergeRecord XRPC: /xrpc/ai.gftd.yata.mergeRecord for label-routed writes

  GIE SecurityFilter — E2E VERIFIED (2026-03-23)
    transpile_secured + vertex_passes_security — 844 tests pass
    PDS → rlsScope (全 ~100 query 接続済み) → X-RLS-Scope → engine _rls_clearance → GIE path
    sensitivity_ord / owner_hash — AUTO-INJECTED on all writes (fnv1a32)
    E2E verified: sens=0 visible to all, sens=1 invisible to anon/public Clerk
    2-partition verified: PARTITION_COUNT=2 deploy + MERGE + query across partitions OK (cold start ~30s)
    Batch verified: 200/200 posts projected to CSR successfully

Phase 2 (1000億): PK Index — PARTIAL
  merge_by_pk: O(1) lookup via prop_eq_index (HashMap) — IMPLEMENTED (single mergeRecord only)
  hash(pk_value) % partition_count — CODE EXISTS (partition.rs) but NOT WIRED into mutation flow
  Cypher MERGE still goes through MemoryGraph copy + CSR rebuild (~500ms)

Phase 3 (1兆): Chunk-level page-in — NOT IMPLEMENTED
  serialize_snapshot_chunked: code stub exists but NOT WIRED
  Currently loads entire per-label Arrow blob (not chunked)

Phase 4 (1兆+): LSMGraph + Distributed Disk Pool — NOT IMPLEMENTED (design only)
  LSMGraph: 4-level tiered storage — design only, no code
  GIE SecurityFilter: IMPLEMENTED — transpile_secured() + SecurityFilter IR op + vertex_passes_security()
  Distributed disk pool: cross-partition I/O — design only, no code

## Actual Implementation State (CRITICAL — overrides above)

Query fast path (GIE, <1µs):
  Cypher → parse → GIE transpile → IR Plan → CSR direct push-based execute
  CONDITIONS: single CSR partition
  RLS: transpile_secured() injects SecurityFilter after Scan/Expand ops
       vertex_passes_security() checks sensitivity_ord, owner_hash, collection_scopes inline

Query GIE fallback → MemoryGraph (~1-200ms):
  GIE transpile fails (mutation or unsupported Cypher) → MemoryGraph copy → Cypher execute
  RLS: apply_rls_filter_scoped on MemoryGraph (legacy path)

Mutation (~500ms):
  Cypher → MemoryGraph copy → mutate → inject metadata → CSR rebuild from scratch
  OR: merge_by_pk → prop_eq_index O(1) lookup → property update → CSR.commit()

Storage (2 level + R2, NOT 4):
  Level 0: RAM (MutableCsrStore CSR) — <1µs, vertex_alive + vertex_props_map + CsrSegment
  Level 1: Vineyard ephemeral (DiskVineyard/MmapVineyard) — ~100µs, Arrow IPC blobs on Container disk
  R2: Source of truth (Arrow IPC per-label) — ~3-5ms, page-in via ensure_vineyard_blobs_from_r2
  Level 2 (distributed disk pool): NOT IMPLEMENTED
  Level 3 (chunk-level R2 page-in): code stub exists, NOT WIRED

  Chunk index: HashMap<(label, pk), (level, partition, chunk_id)>
    → O(1) locate any vertex across all levels — NOT IMPLEMENTED (design only)

Partition fan-out (ACTUAL = Level 0 + Level 1 only, Level 2/3 NOT IMPLEMENTED):
  1 × standard-1     = ~20M nodes   (<1µs, Level 0 only)
  4 × standard-1     = ~100M nodes  (Level 0 + Level 1)
  20 × standard-1    = ~500M nodes  (Level 0 + Level 1, no distributed disk yet)
  400 × standard-1   = ~10B nodes   (Level 0 + Level 1, hierarchical fan-out)
  5000 × standard-1  = ~100B nodes  (Level 0 + Level 1 + R2 page-in)
  5000+ partitions   = 1T+ nodes    (REQUIRES Phase 3/4: chunk page-in + LSMGraph — NOT IMPLEMENTED)

Capacity per tier (5000 partitions, THEORETICAL — Level 2/3 NOT IMPLEMENTED):
  Level 0: 20TB RAM (hot 15%)
  Level 1: 40TB local disk (warm 31%)
  Level 2: 40TB distributed disk pool (warm, shared) — NOT IMPLEMENTED
  Level 3: unlimited R2 (cold archive) — page-in exists but chunk-level NOT WIRED

MERGE complexity:
  Phase 1: O(label size per partition)
  Phase 2: O(1) — merge_by_pk
  Phase 3: O(1) — chunk page-in
  Phase 4: O(1) — multi-level, automatic tiering

Read latency (Level 0/1 ACTUAL, Level 2/3 THEORETICAL):
  Level 0 hit: <1µs (CSR) — ACTUAL
  Level 1 hit: ~100µs (mmap) — ACTUAL
  Level 2 hit: ~1ms (Workers RPC) — NOT IMPLEMENTED
  Level 3 hit: ~2-5ms (R2 GET) — R2 page-in exists but chunk-level NOT WIRED

Hierarchical coordinator: √N fan-out
MmapVineyard: 500M edges per Container (zero-copy, Level 1)
Mirror vertex: MirrorRegistry for 2-hop cross-partition
CSR neighbor lookup: O(1) any scale
```

## Env Vars

| Env var | Default | Purpose |
|---|---|---|
| `YATA_S3_*` | (empty) | R2 endpoint/bucket/key/secret/prefix |
| `YATA_MMAP_VINEYARD` | `false` | Enable MmapVineyard (zero-copy) |
| `YATA_DIRECT_FAN_OUT_LIMIT` | `8` | Below this, direct fan-out; above, hierarchical √N |
| `YATA_BATCH_COMMIT_THRESHOLD` | `10` | R2 snapshot upload triggers after this many WAL commits |

## Test Coverage

844 Rust unit tests, 0 failures. E2E: 8 tests (docker-compose + MinIO, 2-partition). 6-node distributed: 6 tests (10K records, label routing, cold put/pull). ArrowFragment snapshot roundtrip verified.

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
