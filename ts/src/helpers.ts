/**
 * @gftd/yata — Helper functions for yata graph interaction.
 *
 * Encoding, escaping, label conversion, merge props, record decode.
 */

import type { MergeProps } from "./types.js";

// ── Cypher escaping ──

/** Escape a string for safe use in Cypher string literals. */
export function esc(s: string): string {
  return s.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

/** Build a multi-DID WHERE filter (exact + path-based STARTS WITH). */
export function multiDidFilter(field: string, did: string): string {
  return `(${field} = "${esc(did)}" OR ${field} STARTS WITH "${esc(did)}:")`;
}

// ── Collection ↔ Label conversion ──

/**
 * Convert AT Protocol collection (NSID) to yata Cypher label.
 * "app.bsky.feed.post" → "Post"
 * "ai.gftd.apps.news.article" → "Article"
 * "app.bsky.graph.follow" → "Follow"
 */
export function collectionToLabel(collection: string): string {
  const parts = collection.split(".");
  const last = parts[parts.length - 1];
  return last
    .split(/[-_]/)
    .map((seg) => seg.charAt(0).toUpperCase() + seg.slice(1))
    .join("");
}

/**
 * Expand a short kind to full AT Protocol collection NSID.
 * "article" → "ai.gftd.apps.news.article" (if appName="news")
 * "app.bsky.feed.post" → "app.bsky.feed.post" (passthrough)
 */
export function expandCollection(kind: string, appName?: string): string {
  if (!kind || kind.includes(".")) return kind;
  return appName ? `ai.gftd.apps.${appName}.${kind}` : kind;
}

// ── Encoding / Decoding ──

/** Encode string to base64 (browser + Worker compatible). */
export function toBase64(str: string): string {
  return btoa(String.fromCodePoint(...new TextEncoder().encode(str)));
}

/** Decode base64-encoded value_b64 to a record object. Returns {} on failure. */
export function tryDecodeRecord(b64: unknown): Record<string, unknown> {
  if (!b64 || typeof b64 !== "string") return {};
  try {
    const clean = b64.replace(/"/g, "");
    if (!clean) return {};
    const bin = atob(clean);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    return {};
  }
}

/** Clean a Cypher result value (strip surrounding quotes). */
export function cl(v: unknown): string {
  return typeof v === "string" ? v.replace(/"/g, "") : String(v ?? "");
}

// ── FNV-1a hash (for owner_hash property) ──

/** FNV-1a 32-bit hash. Used for owner_hash vertex property. */
export function fnv1a32(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

// ── Merge Props ──

/**
 * Build merge properties for YATA_RPC.mergeRecord().
 * Computes sensitivity_ord and owner_hash from repo DID.
 */
export function buildMergeProps(
  collection: string,
  json: string,
  repo: string
): MergeProps {
  return {
    collection,
    value_b64: toBase64(json),
    repo,
    sensitivity_ord: "0",
    owner_hash: String(fnv1a32(repo)),
  };
}

// ── Row mapping ──

/**
 * Map CypherResult columns/rows to Record[] for easier consumption.
 *
 * { columns: ["name", "age"], rows: [["Alice", 30]] }
 *  → [{ name: "Alice", age: 30 }]
 */
export function mapCypherRows(
  columns: string[],
  rows: unknown[][]
): Record<string, unknown>[] {
  return rows.map((row) => {
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < columns.length; i++) {
      obj[columns[i]] = row[i];
    }
    return obj;
  });
}
