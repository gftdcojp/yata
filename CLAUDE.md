# packages/server/yata

yata — Rust Cypher graph engine on LanceDB。`lancedb` 0.27 (lance 4.0)。No CSR。No WAL。No MemoryGraph。No query cache。

**Role (2026-04-02)**: Query index (materialized view)。KV (PDS_KV) が authoritative source of truth。yata は fire-and-forget で index 更新される。Container crash/restart でもデータは KV に永続化済み。Graceful shutdown で final compaction → R2 flush。AUTO_COMPACT_FRAGMENT_THRESHOLD=8。

## Architecture

```
Schema: Format D (10-col typed vertex, 10-col edge, no props_json)
  Vertex: op, label, pk_value, timestamp_ms, repo, owner_did, name, app_id, rkey, val_json
  Edge:   op, edge_label, eid, src_vid, dst_vid, src_label, dst_label, timestamp_ms, app_id, val_json

Write: merge_record() → build_lance_batch() (Format D) → table.add() [LanceDB append-only, O(1)]
       Edge detection (pk_key=="eid") → edge table (separate)
       Cypher CREATE/MERGE → execute_mutation_direct() → merge_record() per node/edge
       Cypher MATCH+DELETE → scan ArrowStore → delete_record() per matched
       Auto-compact: every 32 merges (background thread, non-blocking)

Read:  query_inner() → build_read_store_pushdown() → ArrowStore (zero-copy, lazy val_json)
       → GIE push-based executor (Topology/Property/Scannable on Arrow columns)
       ArrowStore auto-detects Format D (10-col) vs legacy (7-col) schema
       Lance pushdown: label IN + WHERE promoted-col conditions + LIMIT → scan_filter_limit()
       Edge pushdown: edge_label + src_label + dst_label + src_vid IN (...) + dst_vid IN (...) on separate `edges` table
       GIE Filter is alias-aware: source-side `WHERE a.*` is evaluated on binding `a`, not any bound vertex
       Cypher source/destination predicates (inline props + simple WHERE comparisons) feed `src_vid` / `dst_vid` candidate extraction before edge scan
       GIE optimizer annotates traversal with `TraversalStrategy` (`PreferStaged` / `PreferGie` / `Auto`)
       Engine refines traversal strategy with Lance `count_rows(filter)` cardinality on source-node filters and `edge_label`/`src_label` edge spread
       Engine keeps tombstone-aware pressure stats cache (`raw_rows`, `live_rows`, `dead_rows`, `dead_ratio`) for vertices/edges, globally and per label, persists snapshots in Lance `stats_catalog`, and exposes planner aggregate view (`avg_out_degree`, `dead_ratio`, `last_compacted_at_ms`)
       `EXPLAIN MATCH ...` returns annotated plan rows for all ops (`Scan`/`Filter`/`Expand`/`Project`/`Limit`...), including traversal strategy/debug fields (`strategy`, `avg_out_degree`, `source_live_rows`, `edge_dead_ratio`, etc.)
       Selective traversal ops are engine-led when hint + runtime frontier allow it: staged Expand/PathExpand scan edge frontier first, rebuild narrow ArrowStore, then resume GIE
       Chained traversals reuse touched alias vids across stages so later Expand/PathExpand keep prior bindings
       Cost gate: large frontier / over-limit intermediate sets fall back to normal GIE execution
       Delete-aware compaction: periodic compact + dead-row pressure compact when tombstones accumulate
       Early LIMIT: execute_with_limit() caps intermediate results (no aggregate blowup)
       Disk spill: ArrowStore exceeding YATA_ARROWSTORE_BUDGET_MB → IPC file in YATA_VINEYARD_DIR

Cold:  ensure_lance() → open_table("vertices") + open_table("edges")
       LanceDB shared `Session` reused across tables (index/metadata cache shared)
       OSS scalar index auto-ensure:
         vertices: `pk_value`/`rkey` = BTREE, `label`/`repo`/`owner_did`/`app_id` = BITMAP
         edges: `eid`/`src_vid`/`dst_vid` = BTREE, `edge_label`/`src_label`/`dst_label`/`app_id` = BITMAP
       R2 connect uses timeout/retry storage options (`request_timeout`, `connect_timeout`, `download_retry_count`, `client_max_retries`, `client_retry_timeout`)
       Legacy 7-col table → auto drop + recreate as Format D
```

## Crate Roles

| Crate | Role |
|---|---|
| `yata-core` | GlobalVid, LocalVid, PartitionId |
| `yata-grin` | GRIN trait (Topology, Property, Schema, Scannable, Mutable) |
| `yata-engine` | TieredGraphEngine。Format D write/read/compact。Edge separation。MERGE clause。Selective staged traversal for Expand/PathExpand。SecurityScope (Design E)。61 tests |
| `yata-cypher` | Cypher parser + executor。286 tests |
| `yata-gie` | GIE push-based executor + SecurityFilter + alias-aware Filter pushdown + traversal strategy annotation。214 tests |
| `yata-lance` | LanceDB wrapper。YataDb + YataTable + ArrowStore (zero-copy, Format D auto-detect) |
| `yata-server` | XRPC API + JWT auth + operation timeouts (query 5s, cold start 30s, compact/repair 60s) |

## TS Wrapper snake_case Policy

**`yata/ts/` の interface property 名は Rust Arrow schema (`yata-lance/src/schema.rs`) と 1:1 一致必須。** `partition_id`, `edge_label`, `updated_at_ms`, `dirty_labels`, `generated_at_ms` 等は Lance DB カラム名であり snake_case 維持。TS ローカル変数・パラメータ名は camelCase を使用する (例: `manifestPrefix`)

## 禁止事項

- **LanceDB 以外の persistence path 新規追加禁止**
- **MemoryGraph 再導入禁止** — mutation は execute_mutation_direct (Lance 直接)。read は ArrowStore (zero-copy)
- **WAL 再導入禁止** — Lance は append-only。WAL は Shannon 冗長
- **props_json 再導入禁止** — Format D は val_json (overflow のみ)。core 6 property は個別 column
- **eager props parse 禁止** — ArrowStore は lazy parse
- **in-memory query cache 再導入禁止** — PDS in-memory cache に委譲
- **`RUSTC_WRAPPER=sccache` 禁止** — `cargo zigbuild` + `avx512_stub.rs`

## Build

```bash
RUSTC_WRAPPER="" cargo zigbuild --manifest-path packages/server/yata/Cargo.toml \
  --release --target x86_64-unknown-linux-gnu -p yata-server
```

Deploy: kagami infra Worker (`infra/cloudflare/workers/kagami/`) が graph backend。yata Rust engine は kagami 内部で将来的に利用可能。
