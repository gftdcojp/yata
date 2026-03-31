# packages/rust/yata

yata — Rust Cypher graph engine on LanceDB。`[PRODUCTION]` Container。`lancedb` 0.27 (lance 4.0)。No persistent CSR。No WAL。No cache。No S3Client。

## Architecture

```
Write: merge_record() → build_lance_batch() → table.add(batch) [LanceDB append-only]
       Cypher CREATE (literal) → try_direct_lance_create() → merge_record() O(1)
       Cypher MATCH+SET/DELETE → MemoryGraph fallback → write-back → table.add()
Read:  cypher() → build_read_store(labels) → table.scan_filter() → from_lance_batches() → GIE
Cold:  ensure_lance() → db.open_table("vertices") or create_empty_table()
```

## Crate Roles

| Crate | Role |
|---|---|
| `yata-core` | GlobalVid, LocalVid, PartitionId |
| `yata-grin` | GRIN trait (Topology, Property, Schema, Scannable, Mutable) |
| `yata-engine` | TieredGraphEngine。LanceDB write/read/compact。SecurityScope (Design E) |
| `yata-cypher` | Cypher parser + CSR executor |
| `yata-gie` | GIE push-based executor + SecurityFilter |
| `yata-lance` | LanceDB wrapper。YataDb + YataTable + LanceReadStore (from_lance_batches) |
| `yata-server` | XRPC API + JWT auth |

## 禁止事項

- **LanceDB 以外の persistence path 新規追加禁止**
- **persistent CSR 再導入禁止** — ephemeral `build_read_store()` per query
- **WAL 再導入禁止** — Lance は append-only。WAL は Shannon 冗長
- **in-memory query cache 再導入禁止** — Cloudflare edge cache に委譲
- **`RUSTC_WRAPPER=sccache` 禁止** — `cargo zigbuild` + `avx512_stub.rs`

## Build

```bash
RUSTC_WRAPPER="" cargo zigbuild --manifest-path packages/server/yata/Cargo.toml \
  --release --target x86_64-unknown-linux-gnu -p yata-server
```

Deploy: `infra/cloudflare/container/yata/CLAUDE.md`
