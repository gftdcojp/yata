# packages/rust/yata

yata — Rust Cypher graph engine on LanceDB。`[PRODUCTION]` Container。`lancedb` 0.27 (lance 4.0)。No CSR。No WAL。No MemoryGraph。No cache。

## Architecture

```
Write: merge_record() → build_lance_batch() → table.add() [LanceDB append-only, O(1)]
       Cypher CREATE → execute_mutation_direct() → merge_record() per node/edge
       Cypher MATCH+DELETE → scan ArrowStore → delete_record() per matched
Read:  query_inner() → build_read_store() → ArrowStore (zero-copy Arrow, lazy props)
       → GIE push-based executor (Topology/Property/Scannable on Arrow columns)
Cold:  ensure_lance() → open_table("vertices") or create_empty_table()
```

## Crate Roles

| Crate | Role |
|---|---|
| `yata-core` | GlobalVid, LocalVid, PartitionId |
| `yata-grin` | GRIN trait (Topology, Property, Schema, Scannable, Mutable) |
| `yata-engine` | TieredGraphEngine。LanceDB write/read/compact。SecurityScope (Design E)。55 tests |
| `yata-cypher` | Cypher parser + executor。286 tests |
| `yata-gie` | GIE push-based executor + SecurityFilter。208 tests |
| `yata-lance` | LanceDB wrapper。YataDb + YataTable + ArrowStore (zero-copy, lazy props) |
| `yata-server` | XRPC API + JWT auth |

## 禁止事項

- **LanceDB 以外の persistence path 新規追加禁止**
- **MemoryGraph 再導入禁止** — mutation は execute_mutation_direct (Lance 直接)。read は ArrowStore (zero-copy)
- **WAL 再導入禁止** — Lance は append-only。WAL は Shannon 冗長
- **eager props parse 禁止** — ArrowStore は lazy parse。from_lance_batches (eager) は LanceReadStore 用 legacy
- **in-memory query cache 再導入禁止** — Cloudflare edge cache に委譲
- **`RUSTC_WRAPPER=sccache` 禁止** — `cargo zigbuild` + `avx512_stub.rs`

## Build

```bash
RUSTC_WRAPPER="" cargo zigbuild --manifest-path packages/server/yata/Cargo.toml \
  --release --target x86_64-unknown-linux-gnu -p yata-server
```

Deploy: `infra/cloudflare/container/yata/CLAUDE.md`
