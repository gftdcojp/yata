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

R2 = Source of Truth (Arrow IPC per-label per-partition)
DiskVineyard = Container ephemeral disk cache (page-in/out, LRU evict)
MmapVineyard = zero-copy mmap (500M edges/Container)
CSR = In-memory graph topology (<1ms query)
```

Deploy/config/ops details: `infra/cloudflare/container/yata/CLAUDE.md`

## Data Flow

```
Write (Pipeline + mergeRecord, PDS Worker):
  App → PDS_RPC.createRecord(repo, collection, record)
    → Pipeline.send(): AT commit + MST update → ACK (~1ms, durable)
    → YATA_RPC.mergeRecord(): instant Cypher projection (changed records only)
    → fire-and-forget snapshot() + cron (every 1 min):
        1. trigger_snapshot → R2 PUT (Arrow IPC per-label)
        2. R2 repo persistence (commits + records + CAS blocks + MST)
    → firehose: SSE (XRPC compat only, disabled for active use)

  yata への直接 mutate は mergeRecord からのみ (app 直接禁止)

Read (yata Container = pure read model):
  PDS_RPC.query / listRecords / getTimeline
    → YATA_RPC.cypher → TieredGraphEngine
    → ensure_labels()
    → Vineyard blob hit (ephemeral disk cache) → CSR (0ms)
    → Vineyard miss → R2 GET per-label Arrow IPC → Vineyard → CSR (lazy page-in)

Cold start (after Container sleep):
  → first query triggers ensure_labels()
  → R2 GET manifest.json → R2 GET per-label Arrow IPC → Vineyard → CSR

PDS Container (Rust) は不要 — 全て TS Worker + Pipeline + YATA_RPC。
```

## Workers RPC API (CRITICAL)

```ts
// Bind: { service: "magatama-yata", entrypoint: "YataRPC" }
// Transport: Workers RPC only (gRPC 除去済み)
// Container XRPC: /xrpc/ai.gftd.yata.cypher (unified read+write)

env.YATA.cypher(cypher, appId)   // unified Cypher path → /xrpc/ai.gftd.yata.cypher
env.YATA.query(cypher, appId)    // read-only alias
env.YATA.mutate(cypher, appId)   // CREATE → random partition, DELETE → broadcast
env.YATA.sql(query, appId)       // Flight SQL → /xrpc/ai.gftd.yata.flightSqlQuery
env.YATA.health()                // → partition-0 Container
env.YATA.ping()                  // "pong" (no wake)
env.YATA.stats()                 // → all partition stats
```

## Crate Roles (CRITICAL)

| Crate | Role |
|---|---|
| `yata-core` | GlobalVid, LocalVid, PartitionId |
| `yata-grin` | GRIN trait (Topology, Property, Schema, Scannable, Mutable) |
| `yata-store` | MutableCsrStore, DiskVineyard, MmapVineyard, EdgeVineyard, PartitionStoreSet, GraphStoreEnum, MemoryBudget, StreamingAdjacency trait (streaming.rs), METIS greedy BFS partitioner (metis.rs) |
| `yata-engine` | TieredGraphEngine, Frontier cross-partition BFS, ShardedCoordinator (sharded_coordinator.rs) |
| `yata-cypher` | Full Cypher parser + executor (incl. untyped edge traversal) |
| `yata-gie` | GIE push-based executor, IR (Exchange/Receive/Gather), distributed planner, distributed executor (distributed_executor.rs: push-based fragment execution with ExchangeTransport) |
| `yata-s3` | R2 persistence (renamed from yata-b2). Read: sync R2 GET for page-in (`ensure_vineyard_blobs_from_r2`). Write: `trigger_snapshot()` → R2 PUT (called by mergeRecord fire-and-forget + cron) |
| `yata-vex` | Vector index (IVF_PQ + DiskANN) |
| `yata-bench` | Benchmarks + trillion-scale test + 10K-10M scale benchmarks (scale_benchmark.rs) |
| ~~`yata-at`~~ | **除去済み** — AT Protocol integration は `wproto-compat` に移行 |
| ~~`yata-signal`~~ | **除去済み** — Signal Protocol は `wproto-signal` に移行 |
| `yata-server` | XRPC API server (`/xrpc/ai.gftd.yata.*`)。Workers RPC only transport。`GraphQueryExecutor` trait (standalone, inline query method + snapshot ops) |

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
| Chunk page-in | granular I/O | `serialize_snapshot_chunked` (100K records/chunk, per-label) | NOT WIRED — code stub exists but not wired into page-in |
| LSMGraph tiered storage | multi-level auto | Level 0 RAM → Level 1 disk → Level 2 remote disk → Level 3 R2 | NOT IMPLEMENTED (design only) |
| GART mutable CSR | HTGAP | MutableCsrStore + merge_by_pk + Pipeline epoch | partial (Phase 2) |
| Distributed disk pool | cross-partition I/O | YataRPC.getChunk → remote partition MmapVineyard | NOT IMPLEMENTED (design only) |
| Gremlin | query language | — | not planned |
| GRAPE (analytics) | vertex-centric | — | not planned |
| GLE (learning) | GNN | — | not planned |

**GraphScope parity: 16/23 done** + 2 partial (Groot PK, GART) + 4 code-exists-NOT-WIRED (Pegasus, Data shuffle, Hash routing, Chunk page-in) + 2 design-only (LSMGraph, Distributed disk pool). 3 not planned (Gremlin/GRAPE/GLE).

## R2 Persistence (VERIFIED)

R2 = source of truth. Write: mergeRecord fire-and-forget + cron (every 1 min) → `trigger_snapshot()` → R2 PUT (`ureq+rustls` sync). Read: `ensure_vineyard_blobs_from_r2()` → R2 GET per-label. `@cloudflare/containers` >=0.1.1 required. R2 repo persistence (commit chain + records + CAS blocks) for replay. FUSE/WAL/background upload/DO alarm 全除去済み。

WAL truncation: after R2 snapshot commit, `mark_committed(lsn)` + `truncate()` removes persisted entries.

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

  Chunk-level page-in — UNIT VERIFIED, E2E UNVERIFIABLE at current scale
    trigger_snapshot → serialize_snapshot_chunked_to_vineyard — WIRED
    ensure_vineyard_blobs_from_r2 → chunk fallback — CODE ADDED
    Unit tests: test_chunked_serialize_small_label + test_chunked_serialize_roundtrip PASS
    E2E: >100K nodes per label required to trigger, impractical via API

  GIE SecurityFilter — E2E VERIFIED (2026-03-23)
    transpile_secured + vertex_passes_security — 1005 tests pass (9 SecurityFilter + 2 chunk)
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

988+ Rust unit tests, 0 warnings, 0 failures. Production: 32/32 E2E pass, p50=28ms. 2-partition: verified. 4-partition: cold start timeout (known limitation — sleeping Container wake exceeds Worker timeout).

## Benchmark (measured, release build)

| Operation | 10K | 100K | Scaling |
|---|---|---|---|
| Build (CSR) | 553ms | 544ms | O(n) with const overhead |
| Scan | 358µs | 89µs | warm cache |
| Neighbor (typed) | <1µs | <1µs | **O(1)** |
| COUNT | 35ms | 340ms | linear (partition fan-out) |
| Memory | 102MB | 782MB | ~8 bytes/edge |

| Vineyard | 10K | 100K |
|---|---|---|
| DiskVineyard restore | 327ms | 1052ms |
| MmapVineyard restore | 268ms | 1102ms |
| MmapVineyard cold query | 155µs | — |

Key: Neighbor O(1) any scale. CSR Build <1s at 100K. Memory ~782MB/100K → standard-1 (4GB) supports ~500K nodes. 4-partition verified: 2M nodes capacity.

## mergeResults (Coordinator)

`mergeResults()` correctly handles GROUP BY + aggregate functions (COUNT, SUM, AVG, MIN, MAX). AVG is computed from per-partition sum/count.

## 禁止事項

- **R2 以外を source of truth にする禁止** — R2 が正本
- **JSON RPC で graph data 転送禁止** — Workers RPC (structured clone) + ArrayBuffer
- **lite instance 禁止** — standard-1 以上 (lite は CSR rebuild 遅すぎ)
