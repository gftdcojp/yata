/**
 * @gftd/yata — Helper functions for yata graph interaction.
 *
 * Encoding, escaping, merge props, record decode, row mapping.
 * Primitives (generateTid, toBase64, fnv1a32, cl, collectionToLabel) are
 * re-exported from @gftd/xrpc (Single Source of Truth).
 */

import type { MergeProps } from "./types.js";
import { toBase64, fnv1a32 } from "@gftd/xrpc";

// Re-export from @gftd/xrpc (Single Source of Truth — Shannon: no local copies)
export { collectionToLabel, expandCollection, generateTid, toBase64, fnv1a32, cl } from "@gftd/xrpc";

// ── Cypher escaping ──

/** Escape a string for safe use in Cypher string literals. */
export function esc(s: string): string {
  return s.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

/** Build a multi-DID WHERE filter (exact + path-based STARTS WITH). */
export function multiDidFilter(field: string, did: string): string {
  return `(${field} = "${esc(did)}" OR ${field} STARTS WITH "${esc(did)}:")`;
}

// ── Decoding ──

/**
 * Decode a record value from valueJson (fast path) or valueB64 (legacy fallback).
 *
 * Accepts either a raw base64 string (legacy) or an object with optional
 * `valueJson` / `valueB64` fields. When `valueJson` is present, base64
 * decode is skipped entirely. Returns `{}` on failure.
 */
export function tryDecodeRecord(input: unknown): Record<string, unknown> {
  // Fast path: object with valueJson (skip base64 entirely)
  if (input && typeof input === "object" && "valueJson" in (input as any)) {
    const json = (input as any).valueJson;
    if (typeof json === "string") {
      try { return JSON.parse(json); } catch { /* fall through to b64 */ }
    }
  }
  // Resolve the base64 string: direct string or object.valueB64
  const b64 = typeof input === "string" ? input
    : (input && typeof input === "object" ? (input as any).valueB64 : null);
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

// ── Merge Props ──

/**
 * Build merge properties for YATA_RPC.mergeRecord().
 *
 * Includes both `valueJson` (direct JSON, fast decode path) and `valueB64`
 * (legacy base64 encoding) for backward compatibility during rollout.
 */
export function buildMergeProps(
  collection: string,
  json: string,
  repo: string
): MergeProps {
  return {
    collection,
    'valueJson': json,
    'valueB64': toBase64(json),
    repo,
    'sensitivityOrd': "0",
    'ownerHash': String(fnv1a32(repo)),
    'updatedAt': String(Date.now()),
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
  result: { columns: string[]; rows: unknown[][] }
): Record<string, unknown>[] {
  const cols = result.columns || [];
  return (result.rows || []).map((row) => {
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < cols.length; i++) {
      obj[cols[i]] = row[i];
    }
    return obj;
  });
}
