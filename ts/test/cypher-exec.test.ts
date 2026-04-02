import { describe, it, expect } from "vitest";
import { tableToIPC, tableFromIPC } from "apache-arrow";
import { parseCypher } from "../src/cypher-parse.js";
import { execCypher, type ExecContext } from "../src/cypher-exec.js";
import type { LabelData } from "../src/r2-reader.js";
import { makeVertexTable, makeEdgeTable } from "./test-helpers.js";

function ld(label: string, table: ReturnType<typeof makeVertexTable>): LabelData {
  return { label, table, entryCount: table.numRows };
}

function makeCtx(
  vertices: [string, ReturnType<typeof makeVertexTable>][],
  edges: [string, ReturnType<typeof makeEdgeTable>][] = [],
  params: Record<string, unknown> = {},
): ExecContext {
  return {
    vertices: new Map(vertices.map(([k, v]) => [k, ld(k, v)])),
    edges: new Map(edges.map(([k, v]) => [k, ld(k, v)])),
    params,
  };
}

describe("cypher-exec", () => {
  describe("Pattern A: node scan", () => {
    it("filters by equality", () => {
      const table = makeVertexTable([
        { vid: "v1", repo: "did:web:alice", rkey: "r1", 'updatedAtMs': 1000 },
        { vid: "v2", repo: "did:web:bob", rkey: "r2", 'updatedAtMs': 2000 },
        { vid: "v3", repo: "did:web:alice", rkey: "r3", 'updatedAtMs': 3000 },
      ]);
      const ctx = makeCtx([["Post", table]], [], { p0: "did:web:alice" });
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey LIMIT 50",
      );
      const result = execCypher(ast, ctx);
      expect(result.columns).toEqual(["rkey"]);
      expect(result.rows).toHaveLength(2);
      expect(result.rows.map(r => r[0])).toEqual(["r1", "r3"]);
    });

    it("filters dead vertices (alive=false)", () => {
      const table = makeVertexTable([
        { vid: "v1", repo: "did:web:alice", rkey: "r1", alive: true },
        { vid: "v2", repo: "did:web:alice", rkey: "r2", alive: false },
      ]);
      const ctx = makeCtx([["Post", table]], [], { p0: "did:web:alice" });
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey LIMIT 50",
      );
      const result = execCypher(ast, ctx);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0][0]).toBe("r1");
    });

    it("applies LIMIT", () => {
      const table = makeVertexTable(
        Array.from({ length: 100 }, (_, i) => ({
          vid: `v${i}`,
          repo: "did:web:alice",
          rkey: `r${i}`,
        })),
      );
      const ctx = makeCtx([["Post", table]], [], { p0: "did:web:alice" });
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey LIMIT 10",
      );
      const result = execCypher(ast, ctx);
      expect(result.rows).toHaveLength(10);
    });

    it("sorts by ORDER BY DESC", () => {
      const table = makeVertexTable([
        { vid: "v1", repo: "did:web:a", rkey: "r1", 'updatedAtMs': 1000 },
        { vid: "v2", repo: "did:web:a", rkey: "r2", 'updatedAtMs': 3000 },
        { vid: "v3", repo: "did:web:a", rkey: "r3", 'updatedAtMs': 2000 },
      ]);
      const ctx = makeCtx([["Post", table]], [], { p0: "did:web:a" });
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey ORDER BY r.updatedAtMs DESC LIMIT 50",
      );
      const result = execCypher(ast, ctx);
      expect(result.rows.map(r => r[0])).toEqual(["r2", "r3", "r1"]);
    });

    it("returns empty for missing label", () => {
      const ctx = makeCtx([], [], { p0: "x" });
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey LIMIT 10",
      );
      const result = execCypher(ast, ctx);
      expect(result.rows).toHaveLength(0);
    });
  });

  describe("Pattern B: traversal", () => {
    it("traverses edges to find connected vertices", () => {
      const actors = makeVertexTable([
        { vid: "v1", repo: "did:web:alice", rkey: "a1", label: "Actor" },
        { vid: "v2", repo: "did:web:bob", rkey: "a2", label: "Actor" },
        { vid: "v3", repo: "did:web:carol", rkey: "a3", label: "Actor" },
      ]);
      const edges = makeEdgeTable([
        { 'srcVid': "v1", 'dstVid': "v2" },
        { 'srcVid': "v1", 'dstVid': "v3" },
      ]);
      const ctx = makeCtx(
        [["Actor", actors]],
        [["FOLLOW", edges]],
        { p0: "did:web:alice" },
      );
      const ast = parseCypher(
        "MATCH (a:Actor)-[:FOLLOW]->(b:Actor) WHERE a.repo = $p0 RETURN b.repo AS repo LIMIT 100",
      );
      const result = execCypher(ast, ctx);
      expect(result.rows).toHaveLength(2);
      const repos = result.rows.map(r => r[0]);
      expect(repos).toContain("did:web:bob");
      expect(repos).toContain("did:web:carol");
    });

    it("skips dead edges", () => {
      const actors = makeVertexTable([
        { vid: "v1", repo: "did:web:alice", rkey: "a1" },
        { vid: "v2", repo: "did:web:bob", rkey: "a2" },
      ]);
      const edges = makeEdgeTable([
        { 'srcVid': "v1", 'dstVid': "v2", alive: false },
      ]);
      const ctx = makeCtx(
        [["Actor", actors]],
        [["FOLLOW", edges]],
        { p0: "did:web:alice" },
      );
      const ast = parseCypher(
        "MATCH (a:Actor)-[:FOLLOW]->(b:Actor) WHERE a.repo = $p0 RETURN b.repo AS repo LIMIT 100",
      );
      const result = execCypher(ast, ctx);
      expect(result.rows).toHaveLength(0);
    });
  });

  describe("Pattern C: count", () => {
    it("counts matching vertices", () => {
      const table = makeVertexTable([
        { vid: "v1", repo: "did:web:alice", rkey: "r1" },
        { vid: "v2", repo: "did:web:alice", rkey: "r2" },
        { vid: "v3", repo: "did:web:bob", rkey: "r3" },
      ]);
      const ctx = makeCtx([["Post", table]], [], { p0: "did:web:alice" });
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo = $p0 RETURN count(r) AS cnt",
      );
      const result = execCypher(ast, ctx);
      expect(result.columns).toEqual(["cnt"]);
      expect(result.rows).toEqual([[2]]);
    });

    it("returns 0 for missing label", () => {
      const ctx = makeCtx([], [], { p0: "x" });
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo = $p0 RETURN count(r) AS cnt",
      );
      const result = execCypher(ast, ctx);
      expect(result.rows).toEqual([[0]]);
    });
  });

  describe("WHERE operators", () => {
    it("STARTS WITH", () => {
      const table = makeVertexTable([
        { vid: "v1", repo: "did:web:alice:path1", rkey: "r1" },
        { vid: "v2", repo: "did:web:alice", rkey: "r2" },
        { vid: "v3", repo: "did:web:bob", rkey: "r3" },
      ]);
      const ctx = makeCtx([["Post", table]], [], { p0: "did:web:alice" });
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo STARTS WITH $p0 RETURN r.rkey AS rkey LIMIT 50",
      );
      const result = execCypher(ast, ctx);
      expect(result.rows).toHaveLength(2);
    });

    it("OR (multiDid pattern)", () => {
      const table = makeVertexTable([
        { vid: "v1", repo: "did:web:alice", rkey: "r1" },
        { vid: "v2", repo: "did:web:alice:sub", rkey: "r2" },
        { vid: "v3", repo: "did:web:bob", rkey: "r3" },
      ]);
      const ctx = makeCtx([["Post", table]], [], {
        p0: "did:web:alice",
        p1: "did:web:alice:",
      });
      const ast = parseCypher(
        "MATCH (r:Post) WHERE (r.repo = $p0 OR r.repo STARTS WITH $p1) RETURN r.rkey AS rkey LIMIT 50",
      );
      const result = execCypher(ast, ctx);
      expect(result.rows).toHaveLength(2);
      const rkeys = result.rows.map(r => r[0]);
      expect(rkeys).toContain("r1");
      expect(rkeys).toContain("r2");
    });

    it("IN array", () => {
      const table = makeVertexTable([
        { vid: "v1", repo: "did:web:alice", rkey: "r1" },
        { vid: "v2", repo: "did:web:bob", rkey: "r2" },
        { vid: "v3", repo: "did:web:carol", rkey: "r3" },
      ]);
      const ctx = makeCtx([["Post", table]], [], {
        p0: "did:web:alice",
        p1: "did:web:carol",
      });
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo IN [$p0,$p1] RETURN r.rkey AS rkey LIMIT 50",
      );
      const result = execCypher(ast, ctx);
      expect(result.rows).toHaveLength(2);
    });

    it("false literal returns empty", () => {
      const table = makeVertexTable([
        { vid: "v1", repo: "a", rkey: "r1" },
      ]);
      const ctx = makeCtx([["Post", table]]);
      const ast = parseCypher(
        "MATCH (r:Post) WHERE false RETURN r.rkey AS rkey LIMIT 10",
      );
      const result = execCypher(ast, ctx);
      expect(result.rows).toHaveLength(0);
    });
  });

  describe("Arrow IPC roundtrip", () => {
    it("can decode IPC-encoded table and query it", () => {
      const original = makeVertexTable([
        { vid: "v1", repo: "did:web:alice", rkey: "r1", 'updatedAtMs': 1000 },
        { vid: "v2", repo: "did:web:bob", rkey: "r2", 'updatedAtMs': 2000 },
      ]);
      const ipc = tableToIPC(original);
      const decoded = tableFromIPC(ipc);

      const ctx: ExecContext = {
        vertices: new Map([["Post", { label: "Post", table: decoded, entryCount: 2 }]]),
        edges: new Map(),
        params: { p0: "did:web:alice" },
      };
      const ast = parseCypher(
        "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey LIMIT 10",
      );
      const result = execCypher(ast, ctx);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0][0]).toBe("r1");
    });
  });
});
