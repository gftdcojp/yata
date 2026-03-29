/**
 * @gftd/yata — Parameterized Cypher query builder.
 *
 * All queries are parameterized (no string interpolation).
 * LIMIT is mandatory (yata CRITICAL rule — unbounded scan prevention).
 */

export interface CypherQuery {
  cypher: string;
  params: Record<string, unknown>;
}

/**
 * Cypher query builder with parameterized values.
 *
 * Usage:
 *   new G("Post", "p").where("repo", did).ret("p.text AS text").limit(50).build()
 *   → { cypher: "MATCH (p:Post) WHERE p.repo = $p0 RETURN p.text AS text LIMIT 50", params: { p0: did } }
 */
export class G {
  private label: string;
  private alias: string;
  private whereClauses: string[] = [];
  private returnCols: string[] = [];
  private orderByClause = "";
  private limitVal = 0;
  private params: Record<string, unknown> = {};
  private paramCounter = 0;

  constructor(label: string, alias = "r") {
    this.label = label;
    this.alias = alias;
  }

  /** WHERE field = $param (parameterized). */
  where(field: string, value: unknown): this {
    const p = this.nextParam();
    this.whereClauses.push(`${this.alias}.${field} = $${p}`);
    this.params[p] = value;
    return this;
  }

  /** Multi-DID filter: exact match OR STARTS WITH for path-based DIDs. */
  multiDid(field: string, did: string): this {
    const pExact = this.nextParam();
    const pPrefix = this.nextParam();
    this.whereClauses.push(
      `(${this.alias}.${field} = $${pExact} OR ${this.alias}.${field} STARTS WITH $${pPrefix})`
    );
    this.params[pExact] = did;
    this.params[pPrefix] = did + ":";
    return this;
  }

  /** Raw WHERE clause (for static conditions). Use sparingly. */
  whereRaw(clause: string): this {
    this.whereClauses.push(clause);
    return this;
  }

  /** WHERE field IN [...values] (parameterized). */
  whereIn(field: string, values: unknown[]): this {
    if (values.length === 0) {
      this.whereClauses.push("false");
      return this;
    }
    const pNames = values.map((v) => {
      const p = this.nextParam();
      this.params[p] = v;
      return `$${p}`;
    });
    this.whereClauses.push(`${this.alias}.${field} IN [${pNames.join(",")}]`);
    return this;
  }

  /** RETURN columns. Default: rkey, collection, value_b64, updated_at, repo. */
  ret(...cols: string[]): this {
    this.returnCols = cols;
    return this;
  }

  /** RETURN count(alias) AS cnt. */
  count(): this {
    this.returnCols = [`count(${this.alias}) AS cnt`];
    return this;
  }

  /** ORDER BY clause. */
  orderBy(expr: string): this {
    this.orderByClause = expr;
    return this;
  }

  /** LIMIT (mandatory for non-count queries). */
  limit(n: number): this {
    this.limitVal = n;
    return this;
  }

  /** Build parameterized Cypher query. Throws if LIMIT missing (unless count). */
  build(): CypherQuery {
    const a = this.alias;
    const retCols =
      this.returnCols.length > 0
        ? this.returnCols.join(", ")
        : `${a}.rkey AS rkey, ${a}.collection AS collection, ${a}.value_b64 AS value_b64, ${a}.updated_at AS updated_at, ${a}.repo AS repo`;
    const isCount =
      this.returnCols.length === 1 && this.returnCols[0].includes("count(");
    if (this.limitVal <= 0 && !isCount) {
      throw new Error(
        `G(${this.label}).build(): LIMIT is mandatory. Use .limit(N) or .count()`
      );
    }
    const where =
      this.whereClauses.length > 0
        ? ` WHERE ${this.whereClauses.join(" AND ")}`
        : "";
    const order = this.orderByClause
      ? ` ORDER BY ${this.orderByClause}`
      : "";
    const lim = this.limitVal > 0 ? ` LIMIT ${this.limitVal}` : "";
    return {
      cypher: `MATCH (${a}:${this.label})${where} RETURN ${retCols}${order}${lim}`,
      params: this.params,
    };
  }

  private nextParam(): string {
    return `p${this.paramCounter++}`;
  }
}

/** Build a simple label scan Cypher query (non-parameterized filter, use G() for params). */
export function buildLabelCypher(
  label: string,
  limit: number,
  filter = ""
): string {
  const where = filter ? ` WHERE ${filter}` : "";
  return `MATCH (r:${label})${where} RETURN r.rkey AS rkey, r.collection AS collection, r.value_b64 AS value_b64, r.updated_at AS updated_at, r.repo AS repo ORDER BY r.updated_at DESC LIMIT ${limit}`;
}
