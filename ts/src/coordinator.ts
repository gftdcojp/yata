/**
 * @gftd/yata — Ballista 4-Stage Pipeline coordinator logic.
 *
 * Pure functions for query planning, merge, and LIMIT pushdown.
 * Used by the YataRPC coordinator (Container Worker index.ts).
 * All Cypher metadata extraction and result merging logic lives here
 * as Single Source of Truth — Container index.ts must not duplicate.
 */

import type { CypherResult } from "./types.js";

// ── Stage 0: Query Plan metadata ──

/** Metadata extracted from a Cypher statement for routing/merge decisions. */
export interface QueryMeta {
  labels: string[];
  limit: number | null;
  offset: number;
  orderByCol: string | null;
  orderDesc: boolean;
  hasRelationship: boolean;
  relPattern: { srcLabel: string; relType: string; dstLabel: string; direction: string } | null;
  isAggregate: boolean;
  isMutation: boolean;
}

// ── Regex patterns (compiled once) ──

const MUTATION_RE = /\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE)\b/i;
const AGG_RE = /\b(count|sum|avg|min|max)\s*\(/i;
const LABEL_RE = /(?:MATCH|MERGE|CREATE)\s*\(\s*\w+\s*:\s*`?(\w+)`?/gi;
const LIMIT_RE = /\bLIMIT\s+(\d+)/i;
const OFFSET_RE = /\bSKIP\s+(\d+)/i;
const ORDER_RE = /\bORDER\s+BY\s+(\w+(?:\.\w+)?)\s*(DESC|ASC)?/i;
const REL_RE = /\(\s*(\w+)\s*:\s*`?(\w+)`?\s*\)\s*-\s*\[\s*\w*\s*:?\s*`?(\w*)`?\s*\]\s*->\s*\(\s*(\w+)\s*:\s*`?(\w+)`?\s*\)/i;

/**
 * Stage 0: Extract query metadata from a Cypher statement.
 * Used for partition routing, mutation detection, and merge strategy.
 */
export function extractQueryMeta(stmt: string): QueryMeta {
  const labels = new Set<string>();
  LABEL_RE.lastIndex = 0;
  let m;
  while ((m = LABEL_RE.exec(stmt)) !== null) labels.add(m[1]);

  const limitMatch = LIMIT_RE.exec(stmt);
  const offsetMatch = OFFSET_RE.exec(stmt);
  const orderMatch = ORDER_RE.exec(stmt);

  const relMatch = REL_RE.exec(stmt);
  let relPattern = null;
  if (relMatch) {
    relPattern = { srcLabel: relMatch[2], relType: relMatch[3] || "", dstLabel: relMatch[5], direction: "->" };
  }

  MUTATION_RE.lastIndex = 0;

  return {
    labels: [...labels],
    limit: limitMatch ? parseInt(limitMatch[1], 10) : null,
    offset: offsetMatch ? parseInt(offsetMatch[1], 10) : 0,
    orderByCol: orderMatch ? orderMatch[1] : null,
    orderDesc: orderMatch ? (orderMatch[2] || "").toUpperCase() === "DESC" : false,
    hasRelationship: !!relMatch,
    relPattern,
    isAggregate: AGG_RE.test(stmt),
    isMutation: MUTATION_RE.test(stmt),
  };
}

/**
 * Stage 3: Merge results from multiple partitions.
 * Handles ORDER BY merge-sort, aggregate SUM, and LIMIT/OFFSET.
 */
export function mergeResults(results: CypherResult[], meta: QueryMeta): CypherResult {
  if (results.length <= 1) return results[0] || { columns: [], rows: [] };
  const cols = results[0]?.columns || [];

  if (meta.isAggregate) {
    const merged = (results[0]?.rows?.[0] || []).map(() => 0);
    for (const r of results) for (const row of r.rows || []) for (let i = 0; i < row.length; i++) merged[i] += Number(row[i]) || 0;
    return { columns: cols, rows: [merged] };
  }

  let rows = results.flatMap(r => r.rows || []);

  if (meta.orderByCol && cols.length > 0) {
    const colIdx = cols.indexOf(meta.orderByCol);
    const dotIdx = meta.orderByCol.indexOf(".");
    const bareCol = dotIdx >= 0 ? meta.orderByCol.slice(dotIdx + 1) : meta.orderByCol;
    const idx = colIdx >= 0 ? colIdx : cols.indexOf(bareCol);
    if (idx >= 0) {
      rows.sort((a, b) => {
        const va = a[idx], vb = b[idx];
        if (va == null && vb == null) return 0;
        if (va == null) return 1;
        if (vb == null) return -1;
        if (typeof va === "number" && typeof vb === "number") return meta.orderDesc ? vb - va : va - vb;
        const sa = String(va), sb = String(vb);
        return meta.orderDesc ? sb.localeCompare(sa) : sa.localeCompare(sb);
      });
    }
  }

  if (meta.offset > 0) rows = rows.slice(meta.offset);
  if (meta.limit !== null) rows = rows.slice(0, meta.limit);

  return { columns: cols, rows };
}

/**
 * Rewrite Cypher LIMIT for pushdown: inflate to LIMIT*(N+1) per partition,
 * re-LIMIT at coordinator after merge.
 */
export function pushdownLimit(stmt: string, meta: QueryMeta, partitionCount: number): string {
  if (meta.limit === null || partitionCount <= 1) return stmt;
  const inflated = meta.limit * (partitionCount + 1) + meta.offset;
  return stmt.replace(LIMIT_RE, `LIMIT ${inflated}`);
}

/**
 * Label-based partition routing: hash(label) % N.
 */
export function labelToPartition(label: string, partitionCount: number): number {
  if (partitionCount <= 1) return 0;
  let h = 0;
  for (let i = 0; i < label.length; i++) h = ((h << 5) - h + label.charCodeAt(i)) | 0;
  return Math.abs(h) % partitionCount;
}

/** Escape string for Cypher property values. */
export function cypherEsc(s: string): string {
  return s.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

/** Build a Cypher MERGE statement for mergeRecord. */
export function buildMergeCypher(label: string, pkKey: string, pkValue: string, props: Record<string, string>): string {
  const propEntries = Object.entries(props).map(([k, v]) => `\`${k}\`: "${cypherEsc(v)}"`).join(", ");
  return `MERGE (r:\`${label}\` {\`${pkKey}\`: "${cypherEsc(pkValue)}"}) SET r += {${propEntries}} RETURN id(r) AS vid`;
}

/** Build a Cypher DETACH DELETE statement for deleteRecord. */
export function buildDeleteCypher(label: string, pkKey: string, pkValue: string): string {
  return `MATCH (r:\`${label}\` {\`${pkKey}\`: "${cypherEsc(pkValue)}"}) DETACH DELETE r`;
}
