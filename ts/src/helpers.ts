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

// ── Merge Props ──

/** Build merge properties for YATA_RPC.mergeRecord(). */
export function buildMergeProps(
  collection: string,
  json: string,
  repo: string
): MergeProps {
  return {
    collection,
    'value_b64': toBase64(json),
    repo,
    'sensitivity_ord': "0",
    'owner_hash': String(fnv1a32(repo)),
    'updated_at': String(Date.now()),
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
