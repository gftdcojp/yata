/**
 * @gftd/yata — Workers-side R2 direct write path.
 *
 * Writes Arrow IPC WAL micro-segments directly to R2 from Workers,
 * bypassing the Container mergeRecord path entirely.
 *
 * Architecture:
 *   Worker write → Arrow IPC segment → R2 PUT (instant projection)
 *   Container compaction merges pending segments → compacted per-label segments
 *
 * R2 paths:
 *   Pending WAL: {prefix}wal/pending/{timestamp}-{uuid}.arrow
 *   WAL index:   {prefix}wal/pending/index.json
 */

import {
  Bool,
  Field,
  Schema,
  Struct,
  Table,
  Uint32,
  Uint64,
  Utf8,
  makeData,
  tableFromIPC,
  tableToIPC,
  vectorFromArray,
} from "apache-arrow";
import type { R2BucketLike } from "./manifest.js";
import type { LabelData } from "./r2-reader.js";

// ── Arrow Schema (vertex_live compatible) ──

const VERTEX_WAL_SCHEMA = new Schema([
  new Field("partition_id", new Uint32(), false),
  new Field("label", new Utf8(), false),
  new Field("pk_key", new Utf8(), false),
  new Field("pk_value", new Utf8(), false),
  new Field("vid", new Utf8(), false),
  new Field("alive", new Bool(), false),
  new Field("latest_seq", new Uint64(), false),
  new Field("repo", new Utf8(), true),
  new Field("rkey", new Utf8(), true),
  new Field("updated_at_ms", new Uint64(), false),
  new Field("props_json", new Utf8(), true),
]);

// ── Write Entry ──

export interface WorkersWriteEntry {
  label: string;
  pk_key: string;
  pk_value: string;
  repo: string;
  rkey: string;
  props: Record<string, string>;
  alive?: boolean;
}

// ── Arrow IPC Segment Builder ──

/**
 * Build an Arrow IPC segment from write entries.
 * Each segment is a self-contained Arrow IPC File that can be decoded independently.
 */
export function buildArrowSegment(entries: WorkersWriteEntry[]): Uint8Array {
  const now = BigInt(Date.now());
  const columns: Record<string, unknown[]> = {
    partition_id: entries.map(() => 0),
    label: entries.map(e => e.label),
    pk_key: entries.map(e => e.pk_key),
    pk_value: entries.map(e => e.pk_value),
    vid: entries.map(e => `w-${e.pk_value}`),
    alive: entries.map(e => e.alive !== false),
    latest_seq: entries.map(() => now),
    repo: entries.map(e => e.repo),
    rkey: entries.map(e => e.rkey),
    updated_at_ms: entries.map(() => now),
    props_json: entries.map(e => JSON.stringify(e.props)),
  };

  const len = entries.length;
  const children = VERTEX_WAL_SCHEMA.fields.map(f => {
    const vals = columns[f.name] ?? [];
    return vectorFromArray(vals as any, f.type).data[0];
  });
  const structData = makeData({
    type: new Struct(VERTEX_WAL_SCHEMA.fields),
    length: len,
    children,
  });
  const table = new Table(structData);
  return tableToIPC(table);
}

// ── R2 WAL Writer ──

export interface R2WalWriter {
  writeSegment(entries: WorkersWriteEntry[]): Promise<string>;
  deleteSegment(key: string): Promise<void>;
}

interface R2BucketPut extends R2BucketLike {
  put(key: string, value: ArrayBuffer | Uint8Array | string, options?: any): Promise<unknown>;
  delete(key: string): Promise<void>;
}

export function createR2WalWriter(
  bucket: R2BucketPut,
  prefix = "yata/",
): R2WalWriter {
  const p = prefix.endsWith("/") ? prefix : prefix + "/";

  return {
    async writeSegment(entries: WorkersWriteEntry[]): Promise<string> {
      const segment = buildArrowSegment(entries);
      const ts = Date.now();
      const uuid = crypto.randomUUID().slice(0, 8);
      const key = `${p}wal/pending/${ts}-${uuid}.arrow`;
      await bucket.put(key, segment.buffer, {
        httpMetadata: { contentType: "application/vnd.apache.arrow.file" },
      });
      return key;
    },

    async deleteSegment(key: string): Promise<void> {
      await bucket.delete(key);
    },
  };
}

// ── WAL Index (pending segments registry) ──

export interface WalIndex {
  segments: WalIndexEntry[];
  updated_at_ms: number;
}

export interface WalIndexEntry {
  key: string;
  labels: string[];
  entry_count: number;
  created_at_ms: number;
}

const WAL_INDEX_KEY_SUFFIX = "wal/pending/index.json";

export async function loadWalIndex(
  bucket: R2BucketLike,
  prefix = "yata/",
): Promise<WalIndex> {
  const p = prefix.endsWith("/") ? prefix : prefix + "/";
  const obj = await bucket.get(`${p}${WAL_INDEX_KEY_SUFFIX}`);
  if (!obj) return { segments: [], updated_at_ms: 0 };
  const text = await obj.text();
  return JSON.parse(text) as WalIndex;
}

export async function appendWalIndex(
  bucket: R2BucketPut,
  prefix: string,
  entry: WalIndexEntry,
): Promise<void> {
  const p = prefix.endsWith("/") ? prefix : prefix + "/";
  const key = `${p}${WAL_INDEX_KEY_SUFFIX}`;
  const index = await loadWalIndex(bucket, prefix);
  index.segments.push(entry);
  index.updated_at_ms = Date.now();
  await bucket.put(key, JSON.stringify(index), {
    httpMetadata: { contentType: "application/json" },
  });
}

// ── Pending WAL Loader (for read merge) ──

/**
 * Load pending WAL segments from R2 and decode into LabelData maps.
 * Returns vertex and edge data keyed by label.
 */
export async function loadPendingWal(
  bucket: R2BucketLike,
  prefix = "yata/",
): Promise<Map<string, LabelData>> {
  const index = await loadWalIndex(bucket, prefix);
  if (index.segments.length === 0) return new Map();

  const bucketAB = bucket as any;
  const fetches = index.segments.map(async (seg) => {
    const obj = await bucketAB.get(seg.key);
    if (!obj) return null;
    let buf: ArrayBuffer;
    if ("arrayBuffer" in obj && typeof obj.arrayBuffer === "function") {
      buf = await obj.arrayBuffer();
    } else {
      const text = await obj.text();
      buf = new TextEncoder().encode(text).buffer as ArrayBuffer;
    }
    return tableFromIPC(new Uint8Array(buf));
  });

  const tables = (await Promise.all(fetches)).filter(Boolean) as Table[];
  return groupTablesByLabel(tables);
}

/**
 * Group Arrow Tables by label column, producing per-label LabelData.
 */
function groupTablesByLabel(tables: Table[]): Map<string, LabelData> {
  const rowsByLabel = new Map<string, Record<string, unknown>[]>();

  for (const table of tables) {
    const labelCol = table.getChild("label");
    if (!labelCol) continue;

    for (let i = 0; i < table.numRows; i++) {
      const label = String(labelCol.get(i) ?? "");
      if (!label) continue;

      let rows = rowsByLabel.get(label);
      if (!rows) { rows = []; rowsByLabel.set(label, rows); }

      const row: Record<string, unknown> = {};
      for (const field of table.schema.fields) {
        const col = table.getChild(field.name);
        row[field.name] = col ? col.get(i) : null;
      }
      rows.push(row);
    }
  }

  const result = new Map<string, LabelData>();
  for (const [label, rows] of rowsByLabel) {
    const columns: Record<string, unknown[]> = {};
    for (const field of VERTEX_WAL_SCHEMA.fields) {
      columns[field.name] = rows.map(r => r[field.name] ?? null);
    }

    const len = rows.length;
    const children = VERTEX_WAL_SCHEMA.fields.map(f => {
      const vals = columns[f.name] ?? [];
      return vectorFromArray(vals as any, f.type).data[0];
    });
    const structData = makeData({
      type: new Struct(VERTEX_WAL_SCHEMA.fields),
      length: len,
      children,
    });
    const table = new Table(structData);
    result.set(label, { label, table, entryCount: len });
  }

  return result;
}

// ── Read Merge (compacted + pending WAL dedup by PK) ──

/**
 * Merge compacted label data with pending WAL entries.
 * Deduplicates by pk_value (latest_seq wins). WAL entries override compacted.
 */
export function mergeLabelData(
  compacted: LabelData | undefined,
  pending: LabelData | undefined,
): LabelData | undefined {
  if (!pending) return compacted;
  if (!compacted) return pending;

  // Build a map of pk_value → row index from compacted, then overlay pending
  const pkCol = compacted.table.getChild("pk_value");
  const aliveCol = compacted.table.getChild("alive");
  if (!pkCol) return pending;

  // Collect all rows: compacted first, then pending (pending overrides)
  const seen = new Map<string, { source: "compacted" | "pending"; idx: number }>();

  for (let i = 0; i < compacted.table.numRows; i++) {
    if (aliveCol && !aliveCol.get(i)) continue;
    const pk = String(pkCol.get(i) ?? "");
    seen.set(pk, { source: "compacted", idx: i });
  }

  const pendingPkCol = pending.table.getChild("pk_value");
  const pendingAliveCol = pending.table.getChild("alive");
  if (pendingPkCol) {
    for (let i = 0; i < pending.table.numRows; i++) {
      const pk = String(pendingPkCol.get(i) ?? "");
      if (pendingAliveCol && !pendingAliveCol.get(i)) {
        seen.delete(pk); // delete = remove from result
      } else {
        seen.set(pk, { source: "pending", idx: i });
      }
    }
  }

  // Rebuild merged table
  const rows: Record<string, unknown>[] = [];
  for (const { source, idx } of seen.values()) {
    const table = source === "compacted" ? compacted.table : pending.table;
    const row: Record<string, unknown> = {};
    for (const field of VERTEX_WAL_SCHEMA.fields) {
      const col = table.getChild(field.name);
      row[field.name] = col ? col.get(idx) : null;
    }
    rows.push(row);
  }

  if (rows.length === 0) return undefined;

  const columns: Record<string, unknown[]> = {};
  for (const field of VERTEX_WAL_SCHEMA.fields) {
    columns[field.name] = rows.map(r => r[field.name] ?? null);
  }

  const len = rows.length;
  const children = VERTEX_WAL_SCHEMA.fields.map(f => {
    const vals = columns[f.name] ?? [];
    return vectorFromArray(vals as any, f.type).data[0];
  });
  const structData = makeData({
    type: new Struct(VERTEX_WAL_SCHEMA.fields),
    length: len,
    children,
  });
  const table = new Table(structData);
  const label = compacted.label || pending.label;
  return { label, table, entryCount: len };
}
