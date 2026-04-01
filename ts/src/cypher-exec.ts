/**
 * @gftd/yata — Workers-side Cypher executor.
 *
 * Executes a parsed CypherAST against Arrow Tables loaded from R2.
 * Operates on vertex_live + edge_live_out tables (compacted, per-label segments).
 *
 * Supports:
 *   - Single-label MATCH + WHERE + RETURN + ORDER BY + LIMIT
 *   - 2-hop traversal via edge join
 *   - count() aggregation
 *   - WHERE: =, STARTS WITH, IN, OR, AND
 */

import type { Table, Vector } from "apache-arrow";
import type {
  CypherAST,
  WhereClause,
  Expr,
  PropRef,
  ReturnColumn,
} from "./cypher-parse.js";
import type { CypherResult } from "./types.js";
import type { LabelData } from "./r2-reader.js";

// ── Executor ──

export interface ExecContext {
  vertices: Map<string, LabelData>;
  edges: Map<string, LabelData>;
  params: Record<string, unknown>;
}

/**
 * Execute a CypherAST against loaded Arrow data.
 * Returns CypherResult compatible with YataRPC.cypher() output.
 */
export function execCypher(ast: CypherAST, ctx: ExecContext): CypherResult {
  const { pattern } = ast;

  if (pattern.edge && pattern.dst) {
    return execTraversal(ast, ctx);
  }
  return execNodeScan(ast, ctx);
}

// ── Node Scan (Pattern A + C) ──

function execNodeScan(ast: CypherAST, ctx: ExecContext): CypherResult {
  const label = ast.pattern.src.label;
  const alias = ast.pattern.src.alias;
  const ld = ctx.vertices.get(label);

  if (!ld) return emptyResult(ast);

  const table = ld.table;
  const rowCount = table.numRows;

  // Filter rows
  const matching = filterRows(table, alias, ast.where, ctx.params, rowCount);

  // Count shortcut
  if (ast.isCount) {
    return countResult(ast, matching.length);
  }

  // Sort
  let sorted = matching;
  if (ast.orderBy) {
    sorted = sortRows(table, sorted, ast.orderBy, alias, ast.orderDesc);
  }

  // Limit
  if (ast.limit != null && sorted.length > ast.limit) {
    sorted = sorted.slice(0, ast.limit);
  }

  // Project
  return projectRows(table, sorted, ast.returns, alias, ctx.params);
}

// ── Traversal (Pattern B) ──

function execTraversal(ast: CypherAST, ctx: ExecContext): CypherResult {
  const srcLabel = ast.pattern.src.label;
  const srcAlias = ast.pattern.src.alias;
  const edgeLabel = ast.pattern.edge!.label;
  const dstLabel = ast.pattern.dst!.label;
  const dstAlias = ast.pattern.dst!.alias;

  const srcData = ctx.vertices.get(srcLabel);
  const edgeData = ctx.edges.get(edgeLabel);
  const dstData = ctx.vertices.get(dstLabel);

  if (!srcData || !edgeData || !dstData) return emptyResult(ast);

  // Step 1: Filter source vertices
  const srcTable = srcData.table;
  const srcRows = filterRows(srcTable, srcAlias, ast.where, ctx.params, srcTable.numRows);

  // Collect source VIDs
  const srcVids = new Set<string>();
  const vidCol = srcTable.getChild("vid");
  if (!vidCol) return emptyResult(ast);
  for (const idx of srcRows) {
    const v = vidCol.get(idx);
    if (v != null) srcVids.add(String(v));
  }

  // Step 2: Find matching edges
  const edgeTable = edgeData.table;
  const edgeSrcCol = edgeTable.getChild("src_vid");
  const edgeDstCol = edgeTable.getChild("dst_vid");
  const edgeAliveCol = edgeTable.getChild("alive");
  if (!edgeSrcCol || !edgeDstCol) return emptyResult(ast);

  const dstVids = new Set<string>();
  for (let i = 0; i < edgeTable.numRows; i++) {
    if (edgeAliveCol && !edgeAliveCol.get(i)) continue;
    const sv = edgeSrcCol.get(i);
    if (sv != null && srcVids.has(String(sv))) {
      const dv = edgeDstCol.get(i);
      if (dv != null) dstVids.add(String(dv));
    }
  }

  // Step 3: Filter destination vertices by VID membership
  const dstTable = dstData.table;
  const dstVidCol = dstTable.getChild("vid");
  const dstAliveCol = dstTable.getChild("alive");
  if (!dstVidCol) return emptyResult(ast);

  let dstRows: number[] = [];
  for (let i = 0; i < dstTable.numRows; i++) {
    if (dstAliveCol && !dstAliveCol.get(i)) continue;
    const v = dstVidCol.get(i);
    if (v != null && dstVids.has(String(v))) {
      dstRows.push(i);
    }
  }

  if (ast.isCount) {
    return countResult(ast, dstRows.length);
  }

  // Sort
  if (ast.orderBy) {
    dstRows = sortRows(dstTable, dstRows, ast.orderBy, dstAlias, ast.orderDesc);
  }

  // Limit
  if (ast.limit != null && dstRows.length > ast.limit) {
    dstRows = dstRows.slice(0, ast.limit);
  }

  return projectRows(dstTable, dstRows, ast.returns, dstAlias, ctx.params);
}

// ── Filter ──

function filterRows(
  table: Table,
  alias: string,
  where: WhereClause | null,
  params: Record<string, unknown>,
  rowCount: number,
): number[] {
  // Pre-filter: only alive vertices ('vertex_live': alive column, Lance: op column where 0=upsert)
  const aliveCol = table.getChild("alive");
  const opCol = !aliveCol ? table.getChild("op") : null; // Lance schema fallback

  if (!where) {
    const result: number[] = [];
    for (let i = 0; i < rowCount; i++) {
      if (aliveCol && !aliveCol.get(i)) continue;
      if (opCol && opCol.get(i) === 1) continue; // op=1 is delete tombstone
      result.push(i);
    }
    return result;
  }

  const result: number[] = [];
  for (let i = 0; i < rowCount; i++) {
    if (aliveCol && !aliveCol.get(i)) continue;
    if (evalWhere(table, i, alias, where, params)) {
      result.push(i);
    }
  }
  return result;
}

function evalWhere(
  table: Table,
  row: number,
  alias: string,
  clause: WhereClause,
  params: Record<string, unknown>,
): boolean {
  switch (clause.op) {
    case "false":
      return false;

    case "eq": {
      const left = resolveValue(table, row, alias, clause.left, params);
      const right = resolveExpr(table, row, alias, clause.right, params);
      return left === right;
    }

    case "starts_with": {
      const left = resolveValue(table, row, alias, clause.left, params);
      const right = resolveExpr(table, row, alias, clause.right, params);
      if (typeof left !== "string" || typeof right !== "string") return false;
      return left.startsWith(right);
    }

    case "in": {
      const left = resolveValue(table, row, alias, clause.left, params);
      return clause.values.some(v => resolveExpr(table, row, alias, v, params) === left);
    }

    case "or":
      return clause.clauses.some(c => evalWhere(table, row, alias, c, params));

    case "and":
      return clause.clauses.every(c => evalWhere(table, row, alias, c, params));
  }
}

// Cache parsed props_json per table+row to avoid re-parsing
const _propsCache = new WeakMap<Table, Map<number, Record<string, unknown>>>();

function getProps(table: Table, row: number): Record<string, unknown> {
  let tableCache = _propsCache.get(table);
  if (!tableCache) {
    tableCache = new Map();
    _propsCache.set(table, tableCache);
  }
  let cached = tableCache.get(row);
  if (cached) return cached;

  const propsCol = table.getChild("props_json");
  if (!propsCol) return {};
  const raw = propsCol.get(row);
  if (!raw || typeof raw !== "string") return {};
  try {
    cached = JSON.parse(raw);
    tableCache.set(row, cached!);
    return cached!;
  } catch {
    return {};
  }
}

// Lance schema → vertex_live field aliases
const LANCE_FIELD_ALIASES: Record<string, string> = {
  rkey: "pk_value",
  repo: "repo",              // props_json fallback
  collection: "collection",  // props_json fallback
  'value_b64': "value_b64",   // props_json fallback
  'updated_at': "timestamp_ms",
};

function resolveValue(
  table: Table,
  row: number,
  alias: string,
  prop: PropRef,
  _params: Record<string, unknown>,
): unknown {
  if (prop.alias !== alias) return undefined;
  // 1. Try direct column first
  const col = table.getChild(prop.field);
  if (col) return col.get(row);
  // 2. Try Lance field alias (e.g. rkey → pk_value)
  const aliasField = LANCE_FIELD_ALIASES[prop.field];
  if (aliasField) {
    const aliasCol = table.getChild(aliasField);
    if (aliasCol) return aliasCol.get(row);
  }
  // 3. Fallback: look inside props_json (Lance schema has flattened props here)
  const props = getProps(table, row);
  return props[prop.field];
}

function resolveExpr(
  table: Table,
  row: number,
  alias: string,
  expr: Expr,
  params: Record<string, unknown>,
): unknown {
  switch (expr.kind) {
    case "param":
      return params[expr.name];
    case "literal":
      return expr.value;
    case "prop":
      return resolveValue(table, row, alias, expr, params);
  }
}

// ── Sort ──

function sortRows(
  table: Table,
  rows: number[],
  orderBy: string,
  alias: string,
  desc: boolean,
): number[] {
  // Parse "alias.field" from orderBy
  const field = orderBy.startsWith(alias + ".") ? orderBy.slice(alias.length + 1) : orderBy;
  const col = table.getChild(field);
  if (!col) return rows;

  const sorted = [...rows];
  sorted.sort((a, b) => {
    const va = col.get(a);
    const vb = col.get(b);
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    if (va < vb) return desc ? 1 : -1;
    if (va > vb) return desc ? -1 : 1;
    return 0;
  });
  return sorted;
}

// ── Project ──

function projectRows(
  table: Table,
  rows: number[],
  returns: ReturnColumn[],
  alias: string,
  _params: Record<string, unknown>,
): CypherResult {
  const columns: string[] = returns.map(r => r.alias);
  const resultRows: unknown[][] = [];

  // Collect columns for wildcard expansion
  const expandedReturns = returns.flatMap(ret => {
    if (ret.expr.kind === "prop" && ret.expr.field === "*") {
      return expandWildcard(table, ret.expr.alias);
    }
    return [ret];
  });

  const expandedColumns = expandedReturns.map(r => r.alias);

  for (const rowIdx of rows) {
    const row: unknown[] = [];
    for (const ret of expandedReturns) {
      if (ret.expr.kind === "count") {
        row.push(rows.length);
      } else if (ret.expr.kind === "prop") {
        if (ret.expr.alias === alias || ret.expr.alias === "") {
          row.push(resolveValue(table, rowIdx, alias, ret.expr, {}));
        } else {
          row.push(null);
        }
      }
    }
    resultRows.push(row);
  }

  return { columns: expandedColumns, rows: resultRows };
}

function expandWildcard(table: Table, _alias: string): ReturnColumn[] {
  const fields = table.schema.fields;
  return fields.map(f => ({
    expr: { kind: "prop" as const, alias: "", field: f.name },
    alias: f.name,
  }));
}

// ── Helpers ──

function emptyResult(ast: CypherAST): CypherResult {
  if (ast.isCount) {
    return { columns: [ast.returns[0]?.alias ?? "cnt"], rows: [[0]] };
  }
  return { columns: ast.returns.map(r => r.alias), rows: [] };
}

function countResult(ast: CypherAST, count: number): CypherResult {
  return { columns: [ast.returns[0]?.alias ?? "cnt"], rows: [[count]] };
}
