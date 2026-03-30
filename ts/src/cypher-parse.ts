/**
 * @gftd/yata — Cypher subset parser for Workers-side read path.
 *
 * Parses a subset of Cypher into an AST that can be executed
 * against Arrow RecordBatch data in a Workers V8 isolate.
 *
 * Supported patterns:
 *   A: MATCH (r:Label) WHERE ... RETURN ... ORDER BY ... LIMIT N
 *   B: MATCH (a:L1)-[:EDGE]->(b:L2) WHERE ... RETURN ... LIMIT N
 *   C: MATCH (r:Label) WHERE ... RETURN count(r) AS cnt
 */

// ── AST Types ──

export type CypherValue = string | number | boolean | null;

export interface ParamRef {
  kind: "param";
  name: string;
}

export interface PropRef {
  kind: "prop";
  alias: string;
  field: string;
}

export interface LiteralVal {
  kind: "literal";
  value: CypherValue;
}

export type Expr = ParamRef | PropRef | LiteralVal;

export interface WhereEq {
  op: "eq";
  left: PropRef;
  right: Expr;
}

export interface WhereStartsWith {
  op: "starts_with";
  left: PropRef;
  right: Expr;
}

export interface WhereIn {
  op: "in";
  left: PropRef;
  values: Expr[];
}

export interface WhereOr {
  op: "or";
  clauses: WhereClause[];
}

export interface WhereAnd {
  op: "and";
  clauses: WhereClause[];
}

export interface WhereFalse {
  op: "false";
}

export type WhereClause = WhereEq | WhereStartsWith | WhereIn | WhereOr | WhereAnd | WhereFalse;

export interface ReturnColumn {
  expr: PropRef | CountExpr;
  alias: string;
}

export interface CountExpr {
  kind: "count";
  alias: string;
}

export interface MatchNode {
  alias: string;
  label: string;
}

export interface MatchEdge {
  label: string;
  direction: "out" | "in";
}

export interface MatchPattern {
  src: MatchNode;
  edge?: MatchEdge;
  dst?: MatchNode;
}

export interface CypherAST {
  pattern: MatchPattern;
  where: WhereClause | null;
  returns: ReturnColumn[];
  orderBy: string | null;
  orderDesc: boolean;
  limit: number | null;
  isCount: boolean;
}

// ── Parser ──

export class CypherParseError extends Error {
  constructor(message: string, public cypher: string) {
    super(`CypherParseError: ${message}`);
  }
}

/**
 * Check if a Cypher query can be handled by the Workers-side executor.
 * Returns the AST if parseable, null if the query needs Container fallback.
 */
export function tryParseCypher(
  cypher: string,
  params?: Record<string, unknown>,
): CypherAST | null {
  try {
    return parseCypher(cypher, params);
  } catch {
    return null;
  }
}

/**
 * Parse a Cypher subset query into an AST. Throws CypherParseError for unsupported syntax.
 */
export function parseCypher(
  cypher: string,
  _params?: Record<string, unknown>,
): CypherAST {
  const src = cypher.trim();

  // Tokenize into clauses
  const matchIdx = findClause(src, "MATCH");
  if (matchIdx !== 0) throw new CypherParseError("must start with MATCH", src);

  const whereIdx = findClause(src, "WHERE");
  const returnIdx = findClause(src, "RETURN");
  if (returnIdx < 0) throw new CypherParseError("missing RETURN", src);

  const orderIdx = findClause(src, "ORDER BY");
  const limitIdx = findClause(src, "LIMIT");

  // Parse MATCH pattern
  const matchEnd = whereIdx >= 0 ? whereIdx : returnIdx;
  const matchStr = src.slice(5, matchEnd).trim();
  const pattern = parseMatchPattern(matchStr, src);

  // Parse WHERE
  let whereClause: WhereClause | null = null;
  if (whereIdx >= 0) {
    const whereStr = src.slice(whereIdx + 5, returnIdx).trim();
    whereClause = parseWhere(whereStr, src);
  }

  // Parse RETURN
  const returnEnd = orderIdx >= 0 ? orderIdx : limitIdx >= 0 ? limitIdx : src.length;
  const returnStr = src.slice(returnIdx + 6, returnEnd).trim();
  const { columns, isCount } = parseReturn(returnStr, src);

  // Parse ORDER BY
  let orderBy: string | null = null;
  let orderDesc = false;
  if (orderIdx >= 0) {
    const orderEnd = limitIdx >= 0 ? limitIdx : src.length;
    const orderStr = src.slice(orderIdx + 8, orderEnd).trim();
    if (orderStr.endsWith(" DESC")) {
      orderDesc = true;
      orderBy = orderStr.slice(0, -5).trim();
    } else if (orderStr.endsWith(" ASC")) {
      orderBy = orderStr.slice(0, -4).trim();
    } else {
      orderBy = orderStr;
    }
  }

  // Parse LIMIT
  let limit: number | null = null;
  if (limitIdx >= 0) {
    const limitStr = src.slice(limitIdx + 5).trim();
    limit = parseInt(limitStr, 10);
    if (isNaN(limit)) throw new CypherParseError(`invalid LIMIT: ${limitStr}`, src);
  }

  return { pattern, where: whereClause, returns: columns, orderBy, orderDesc, limit, isCount };
}

// ── Pattern Parser ──

const NODE_RE = /^\((\w+):(\w+)\)/;
const EDGE_RE = /^-\[:(\w+)\]->/;
const EDGE_IN_RE = /^<-\[:(\w+)\]-/;

function parseMatchPattern(s: string, cypher: string): MatchPattern {
  let rest = s;

  const srcMatch = rest.match(NODE_RE);
  if (!srcMatch) throw new CypherParseError(`invalid MATCH node: ${rest}`, cypher);
  const src: MatchNode = { alias: srcMatch[1], label: srcMatch[2] };
  rest = rest.slice(srcMatch[0].length).trim();

  if (!rest) return { src };

  // Try outgoing edge
  let edgeMatch = rest.match(EDGE_RE);
  let direction: "out" | "in" = "out";
  if (!edgeMatch) {
    edgeMatch = rest.match(EDGE_IN_RE);
    direction = "in";
  }
  if (!edgeMatch) throw new CypherParseError(`invalid edge pattern: ${rest}`, cypher);
  const edge: MatchEdge = { label: edgeMatch[1], direction };
  rest = rest.slice(edgeMatch[0].length).trim();

  const dstMatch = rest.match(NODE_RE);
  if (!dstMatch) throw new CypherParseError(`invalid destination node: ${rest}`, cypher);
  const dst: MatchNode = { alias: dstMatch[1], label: dstMatch[2] };

  return { src, edge, dst };
}

// ── WHERE Parser ──

function parseWhere(s: string, cypher: string): WhereClause {
  return parseOrExpr(s.trim(), cypher);
}

function parseOrExpr(s: string, cypher: string): WhereClause {
  const parts = splitTopLevel(s, " OR ");
  if (parts.length === 1) return parseAndExpr(parts[0], cypher);
  return { op: "or", clauses: parts.map(p => parseAndExpr(p, cypher)) };
}

function parseAndExpr(s: string, cypher: string): WhereClause {
  const parts = splitTopLevel(s, " AND ");
  if (parts.length === 1) return parseAtom(parts[0], cypher);
  return { op: "and", clauses: parts.map(p => parseAtom(p, cypher)) };
}

function parseAtom(s: string, cypher: string): WhereClause {
  const trimmed = s.trim();

  // Unwrap parens
  if (trimmed.startsWith("(") && trimmed.endsWith(")")) {
    const inner = trimmed.slice(1, -1).trim();
    return parseOrExpr(inner, cypher);
  }

  // false literal
  if (trimmed === "false") return { op: "false" };

  // STARTS WITH
  const swIdx = trimmed.indexOf(" STARTS WITH ");
  if (swIdx >= 0) {
    const left = parsePropRef(trimmed.slice(0, swIdx).trim(), cypher);
    const right = parseExpr(trimmed.slice(swIdx + 13).trim(), cypher);
    return { op: "starts_with", left, right };
  }

  // IN [...]
  const inIdx = trimmed.indexOf(" IN [");
  if (inIdx >= 0) {
    const left = parsePropRef(trimmed.slice(0, inIdx).trim(), cypher);
    const arrStr = trimmed.slice(inIdx + 4).trim();
    if (!arrStr.startsWith("[") || !arrStr.endsWith("]")) {
      throw new CypherParseError(`invalid IN array: ${arrStr}`, cypher);
    }
    const inner = arrStr.slice(1, -1);
    const values = inner ? inner.split(",").map(v => parseExpr(v.trim(), cypher)) : [];
    return { op: "in", left, values };
  }

  // Equality (=)
  const eqIdx = trimmed.indexOf(" = ");
  if (eqIdx >= 0) {
    const left = parsePropRef(trimmed.slice(0, eqIdx).trim(), cypher);
    const right = parseExpr(trimmed.slice(eqIdx + 3).trim(), cypher);
    return { op: "eq", left, right };
  }

  throw new CypherParseError(`unsupported WHERE clause: ${trimmed}`, cypher);
}

function parsePropRef(s: string, cypher: string): PropRef {
  const dot = s.indexOf(".");
  if (dot < 0) throw new CypherParseError(`expected alias.field, got: ${s}`, cypher);
  return { kind: "prop", alias: s.slice(0, dot), field: s.slice(dot + 1) };
}

function parseExpr(s: string, cypher: string): Expr {
  const trimmed = s.trim();

  // Param ref: $name
  if (trimmed.startsWith("$")) {
    return { kind: "param", name: trimmed.slice(1) };
  }

  // String literal: "..."
  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    return { kind: "literal", value: trimmed.slice(1, -1).replace(/\\"/g, '"').replace(/\\\\/g, "\\") };
  }

  // Number
  const num = Number(trimmed);
  if (!isNaN(num) && trimmed !== "") {
    return { kind: "literal", value: num };
  }

  // Boolean
  if (trimmed === "true") return { kind: "literal", value: true };
  if (trimmed === "false") return { kind: "literal", value: false };
  if (trimmed === "null") return { kind: "literal", value: null };

  throw new CypherParseError(`unsupported expression: ${trimmed}`, cypher);
}

// ── RETURN Parser ──

function parseReturn(
  s: string,
  cypher: string,
): { columns: ReturnColumn[]; isCount: boolean } {
  const parts = s.split(",").map(p => p.trim());
  const columns: ReturnColumn[] = [];
  let isCount = false;

  for (const part of parts) {
    // count(alias) AS name
    const countMatch = part.match(/^count\((\w+)\)\s+AS\s+(\w+)$/i);
    if (countMatch) {
      isCount = true;
      columns.push({
        expr: { kind: "count", alias: countMatch[1] },
        alias: countMatch[2],
      });
      continue;
    }

    // alias.field AS name
    const asMatch = part.match(/^(\w+\.\w+)\s+AS\s+(\w+)$/i);
    if (asMatch) {
      const prop = parsePropRef(asMatch[1], cypher);
      columns.push({ expr: prop, alias: asMatch[2] });
      continue;
    }

    // alias.field (no AS)
    const dotMatch = part.match(/^(\w+)\.(\w+)$/);
    if (dotMatch) {
      columns.push({
        expr: { kind: "prop", alias: dotMatch[1], field: dotMatch[2] },
        alias: dotMatch[2],
      });
      continue;
    }

    // bare alias (RETURN r)
    const bareMatch = part.match(/^(\w+)$/);
    if (bareMatch) {
      columns.push({
        expr: { kind: "prop", alias: bareMatch[1], field: "*" },
        alias: bareMatch[1],
      });
      continue;
    }

    throw new CypherParseError(`unsupported RETURN expression: ${part}`, cypher);
  }

  return { columns, isCount };
}

// ── Helpers ──

/** Find clause keyword at word boundary (case-insensitive). */
function findClause(src: string, keyword: string): number {
  const upper = src.toUpperCase();
  const kw = keyword.toUpperCase();
  let idx = 0;
  while (idx < upper.length) {
    const found = upper.indexOf(kw, idx);
    if (found < 0) return -1;
    // Check word boundary: start of string or preceded by space/paren
    const before = found === 0 || /[\s(]/.test(src[found - 1]);
    const after = found + kw.length >= src.length || /[\s(]/.test(src[found + kw.length]);
    if (before && after) return found;
    idx = found + 1;
  }
  return -1;
}

/**
 * Split by delimiter but only at the top level (not inside parens or brackets).
 */
function splitTopLevel(s: string, delim: string): string[] {
  const results: string[] = [];
  let depth = 0;
  let start = 0;
  const upper = s.toUpperCase();
  const delimUpper = delim.toUpperCase();

  for (let i = 0; i < s.length; i++) {
    if (s[i] === "(" || s[i] === "[") depth++;
    else if (s[i] === ")" || s[i] === "]") depth--;
    else if (depth === 0 && upper.startsWith(delimUpper, i)) {
      results.push(s.slice(start, i).trim());
      i += delim.length - 1;
      start = i + 1;
    }
  }
  results.push(s.slice(start).trim());
  return results.filter(r => r.length > 0);
}
