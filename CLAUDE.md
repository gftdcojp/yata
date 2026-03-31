# packages/rust/yata

yata — Rust Cypher graph engine on LanceDB。`[PRODUCTION]` Container。`lancedb` 0.27 (lance 4.0)。No persistent CSR。No WAL。No cache。No S3Client。engine.rs 1,712行。

## Architecture

```
Write: mergeRecord() → WalEntry batch → table.add(batch) [LanceDB] → R2
Read:  cypher() → build_read_store(labels) → table.scan_filter() → ephemeral LanceReadStore → GIE
Cold:  ensure_lance() → db.open_table("vertices") [LanceDB handles R2]
```

## Crate Roles

| Crate | Role |
|---|---|
| `yata-core` | GlobalVid, LocalVid, PartitionId |
| `yata-grin` | GRIN trait (Topology, Property, Schema, Scannable, Mutable) |
| `yata-engine` | TieredGraphEngine (1,712行)。LanceDB write/read/compact。SecurityScope (Design E) |
| `yata-cypher` | Cypher parser + CSR executor |
| `yata-gie` | GIE push-based executor + SecurityFilter |
| `yata-lance` | LanceDB wrapper (1,074行)。YataDb + YataTable + LanceReadStore + Arrow schema |
| `yata-server` | XRPC API + JWT auth |
| `yata-hayate` | Hayate V5 Python bridge (PyO3, workspace excluded — deps on deleted crates) |

## 禁止事項

- **LanceDB 以外の persistence path 新規追加禁止**
- **persistent CSR 再導入禁止** — ephemeral `build_read_store()` per query
- **WAL ring buffer 再導入禁止** — `merge_record()` → `table.add()` 直接
- **Arrow IPC transport 再導入禁止** — `walTailArrow`/`walApplyArrow` 除去済み。`arrow-ipc` dep 除去済み
- **in-memory query cache 再導入禁止** — Cloudflare edge cache に委譲
- **`RUSTC_WRAPPER=sccache` 禁止** — `cargo zigbuild` + `avx512_stub.rs`

## Production (2026-03-31, lancedb 0.27)

| Endpoint | p50 | Grade |
|---|---|---|
| SearchPosts | 9.0ms | S |
| GetProfile | 9.3ms | S |
| GetTimeline | 653ms | D (was 15s F) |
| GetDiscoverFeed | 704ms | D (was 10.5s F) |

## Build

```bash
RUSTC_WRAPPER="" cargo zigbuild --manifest-path packages/server/yata/Cargo.toml \
  --release --target x86_64-unknown-linux-gnu -p yata-server
```

Deploy: `infra/cloudflare/container/yata/CLAUDE.md`
