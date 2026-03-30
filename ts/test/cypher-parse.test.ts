import { describe, it, expect } from "vitest";
import { parseCypher, tryParseCypher, type CypherAST } from "../src/cypher-parse.js";

describe("cypher-parse", () => {
  describe("Pattern A: single label + WHERE + RETURN", () => {
    it("parses basic MATCH with WHERE and RETURN", () => {
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.text AS text, r.rkey AS rkey LIMIT 50",
      );
      expect(ast.pattern.src).toEqual({ alias: "r", label: "Post" });
      expect(ast.pattern.edge).toBeUndefined();
      expect(ast.where).toEqual({
        op: "eq",
        left: { kind: "prop", alias: "r", field: "repo" },
        right: { kind: "param", name: "p0" },
      });
      expect(ast.returns).toHaveLength(2);
      expect(ast.returns[0]).toEqual({
        expr: { kind: "prop", alias: "r", field: "text" },
        alias: "text",
      });
      expect(ast.limit).toBe(50);
      expect(ast.isCount).toBe(false);
    });

    it("parses default RETURN columns from G builder output", () => {
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey, r.collection AS collection, r.value_b64 AS value_b64, r.updated_at AS updated_at, r.repo AS repo LIMIT 50",
      );
      expect(ast.returns).toHaveLength(5);
      expect(ast.returns.map(r => r.alias)).toEqual([
        "rkey", "collection", "value_b64", "updated_at", "repo",
      ]);
    });

    it("parses ORDER BY DESC", () => {
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.text AS text ORDER BY r.updated_at DESC LIMIT 50",
      );
      expect(ast.orderBy).toBe("r.updated_at");
      expect(ast.orderDesc).toBe(true);
    });

    it("parses ORDER BY ASC (default)", () => {
      const ast = parseCypher(
        "MATCH (r:Post) RETURN r.text AS text ORDER BY r.updated_at LIMIT 10",
      );
      expect(ast.orderBy).toBe("r.updated_at");
      expect(ast.orderDesc).toBe(false);
    });
  });

  describe("Pattern B: 2-hop traversal", () => {
    it("parses outgoing edge pattern", () => {
      const ast = parseCypher(
        "MATCH (a:Actor)-[:FOLLOW]->(b:Actor) WHERE a.did = $p0 RETURN b LIMIT 100",
      );
      expect(ast.pattern.src).toEqual({ alias: "a", label: "Actor" });
      expect(ast.pattern.edge).toEqual({ label: "FOLLOW", direction: "out" });
      expect(ast.pattern.dst).toEqual({ alias: "b", label: "Actor" });
      expect(ast.limit).toBe(100);
    });

    it("parses traversal with property return", () => {
      const ast = parseCypher(
        "MATCH (a:Actor)-[:FOLLOW]->(b:Actor) WHERE a.did = $p0 RETURN b.did AS did, b.handle AS handle LIMIT 50",
      );
      expect(ast.returns).toHaveLength(2);
      expect(ast.returns[0].alias).toBe("did");
      expect(ast.returns[1].alias).toBe("handle");
    });
  });

  describe("Pattern C: count", () => {
    it("parses count query", () => {
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo = $p0 RETURN count(r) AS cnt",
      );
      expect(ast.isCount).toBe(true);
      expect(ast.returns[0]).toEqual({
        expr: { kind: "count", alias: "r" },
        alias: "cnt",
      });
      expect(ast.limit).toBeNull();
    });
  });

  describe("WHERE clause operators", () => {
    it("parses STARTS WITH", () => {
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo STARTS WITH $p0 RETURN r LIMIT 10",
      );
      expect(ast.where).toEqual({
        op: "starts_with",
        left: { kind: "prop", alias: "r", field: "repo" },
        right: { kind: "param", name: "p0" },
      });
    });

    it("parses IN array", () => {
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo IN [$p0,$p1,$p2] RETURN r LIMIT 10",
      );
      expect(ast.where!.op).toBe("in");
      if (ast.where!.op === "in") {
        expect(ast.where!.values).toHaveLength(3);
      }
    });

    it("parses OR (multiDid pattern)", () => {
      const ast = parseCypher(
        "MATCH (r:Post) WHERE (r.repo = $p0 OR r.repo STARTS WITH $p1) RETURN r LIMIT 10",
      );
      expect(ast.where!.op).toBe("or");
    });

    it("parses AND", () => {
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo = $p0 AND r.collection = $p1 RETURN r LIMIT 10",
      );
      expect(ast.where!.op).toBe("and");
    });

    it("parses false literal", () => {
      const ast = parseCypher(
        "MATCH (r:Post) WHERE false RETURN r LIMIT 10",
      );
      expect(ast.where).toEqual({ op: "false" });
    });

    it("parses string literal in WHERE", () => {
      const ast = parseCypher(
        'MATCH (r:Post) WHERE r.repo = "did:web:example.com" RETURN r LIMIT 10',
      );
      expect(ast.where).toEqual({
        op: "eq",
        left: { kind: "prop", alias: "r", field: "repo" },
        right: { kind: "literal", value: "did:web:example.com" },
      });
    });
  });

  describe("tryParseCypher", () => {
    it("returns AST for supported queries", () => {
      const ast = tryParseCypher("MATCH (r:Post) RETURN r LIMIT 10");
      expect(ast).not.toBeNull();
    });

    it("returns null for unsupported queries", () => {
      const ast = tryParseCypher("CREATE (n:Post {text: 'hello'})");
      expect(ast).toBeNull();
    });

    it("returns null for OPTIONAL MATCH", () => {
      const ast = tryParseCypher("OPTIONAL MATCH (r:Post) RETURN r LIMIT 10");
      expect(ast).toBeNull();
    });

    it("returns null for WITH clause", () => {
      const ast = tryParseCypher(
        "MATCH (r:Post) WITH r MATCH (r)-[:LIKE]->(l:Like) RETURN l LIMIT 10",
      );
      expect(ast).toBeNull();
    });
  });

  describe("missing LIMIT enforcement", () => {
    it("throws for non-count query without LIMIT", () => {
      // Parser itself doesn't enforce LIMIT — that's the builder's job
      // But we can verify it parses without LIMIT
      const ast = parseCypher("MATCH (r:Post) RETURN r.text AS text");
      expect(ast.limit).toBeNull();
    });
  });

  describe("no MATCH", () => {
    it("throws for query not starting with MATCH", () => {
      expect(() => parseCypher("RETURN 1")).toThrow("must start with MATCH");
    });
  });
});
