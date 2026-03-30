import { describe, it, expect, vi } from "vitest";
import { WorkersReader } from "../src/workers-read.js";
import type { FragmentStore, CompactionManifest } from "../src/r2-reader.js";
import type { YataRPC } from "../src/types.js";
import { makeVertexTable, makeArrowBuffer } from "./test-helpers.js";

function makeTestManifest(): CompactionManifest {
  return {
    partition_id: 0,
    version: 2,
    compacted_segment_key: "",
    compacted_seq: 100,
    entry_count: 3,
    labels: ["Post"],
    created_at_ms: Date.now(),
    segment_bytes: 512,
    label_segments: {
      Post: {
        key: "yata/log/compacted/0/label/Post.arrow",
        max_seq: 100,
        entry_count: 3,
        segment_bytes: 512,
        blake3_hex: "abc",
      },
    },
  };
}

function makeTestArrowBuffer(): ArrayBuffer {
  const table = makeVertexTable([
    { vid: "v1", repo: "did:web:alice", rkey: "r1", updated_at_ms: 1000 },
    { vid: "v2", repo: "did:web:bob", rkey: "r2", updated_at_ms: 2000 },
    { vid: "v3", repo: "did:web:alice", rkey: "r3", updated_at_ms: 3000 },
  ]);
  return makeArrowBuffer(table);
}

function createMockStore(): FragmentStore {
  const arrowBuf = makeTestArrowBuffer();
  return {
    async getManifest() {
      return makeTestManifest();
    },
    async getFragment(key: string) {
      if (key === "yata/log/compacted/0/label/Post.arrow") return arrowBuf;
      return null;
    },
  };
}

function createMockContainer(): YataRPC {
  return {
    cypher: vi.fn().mockResolvedValue({ columns: ["cnt"], rows: [[42]] }),
    query: vi.fn().mockResolvedValue({ columns: [], rows: [] }),
    mutate: vi.fn().mockResolvedValue({ columns: [], rows: [] }),
    mergeRecord: vi.fn().mockResolvedValue({ vid: 1 }),
    deleteRecord: vi.fn().mockResolvedValue({ deleted: true }),
    cypherBatch: vi.fn().mockResolvedValue([]),
    walTail: vi.fn().mockResolvedValue({ entries: [], head_seq: 0, count: 0 }),
    walFlushSegments: vi.fn().mockResolvedValue({ flushed: 0 }),
    walCheckpoint: vi.fn().mockResolvedValue({ vertices: 0, edges: 0 }),
    walColdStartReplicas: vi.fn().mockResolvedValue({ started: 0 }),
    health: vi.fn().mockResolvedValue("ok"),
    ping: vi.fn().mockResolvedValue("pong"),
    warmup: vi.fn().mockResolvedValue("warm"),
  };
}

describe("workers-read", () => {
  it("executes simple query locally", async () => {
    const store = createMockStore();
    const container = createMockContainer();
    const reader = new WorkersReader({ store, container });

    const result = await reader.cypher(
      "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey LIMIT 50",
      undefined,
      { p0: "did:web:alice" },
    );

    expect(result.rows).toHaveLength(2);
    expect(reader.stats.workersHit).toBe(1);
    expect(reader.stats.containerFallback).toBe(0);
    expect(container.cypher).not.toHaveBeenCalled();
  });

  it("falls back to container for unsupported queries", async () => {
    const store = createMockStore();
    const container = createMockContainer();
    const reader = new WorkersReader({ store, container });

    const result = await reader.cypher("CREATE (n:Post {text: 'hello'})");

    expect(result).toEqual({ columns: ["cnt"], rows: [[42]] });
    expect(reader.stats.containerFallback).toBe(1);
    expect(container.cypher).toHaveBeenCalledOnce();
  });

  it("mutate always goes to container", async () => {
    const store = createMockStore();
    const container = createMockContainer();
    const reader = new WorkersReader({ store, container });

    await reader.mutate("CREATE (n:Post {text: 'hello'})");
    expect(container.mutate).toHaveBeenCalledOnce();
  });

  it("caches manifest across queries", async () => {
    const store = createMockStore();
    const getManifestSpy = vi.spyOn(store, "getManifest");
    const container = createMockContainer();
    const reader = new WorkersReader({ store, container });

    await reader.cypher(
      "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey LIMIT 10",
      undefined,
      { p0: "did:web:alice" },
    );
    await reader.cypher(
      "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey LIMIT 10",
      undefined,
      { p0: "did:web:bob" },
    );

    expect(getManifestSpy).toHaveBeenCalledTimes(1);
    expect(reader.stats.workersHit).toBe(2);
  });

  it("caches label data across queries", async () => {
    const store = createMockStore();
    const getFragmentSpy = vi.spyOn(store, "getFragment");
    const container = createMockContainer();
    const reader = new WorkersReader({ store, container });

    await reader.cypher(
      "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey LIMIT 10",
      undefined,
      { p0: "did:web:alice" },
    );
    await reader.cypher(
      "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey LIMIT 10",
      undefined,
      { p0: "did:web:bob" },
    );

    expect(getFragmentSpy).toHaveBeenCalledTimes(1);
  });

  it("invalidateLabels forces refetch", async () => {
    const store = createMockStore();
    const getFragmentSpy = vi.spyOn(store, "getFragment");
    const container = createMockContainer();
    const reader = new WorkersReader({ store, container });

    await reader.cypher(
      "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey LIMIT 10",
      undefined,
      { p0: "did:web:alice" },
    );

    reader.invalidateLabels();

    await reader.cypher(
      "MATCH (r:Post) WHERE r.repo = $p0 RETURN r.rkey AS rkey LIMIT 10",
      undefined,
      { p0: "did:web:alice" },
    );

    expect(getFragmentSpy).toHaveBeenCalledTimes(2);
  });

  it("count query works locally", async () => {
    const store = createMockStore();
    const container = createMockContainer();
    const reader = new WorkersReader({ store, container });

    const result = await reader.cypher(
      "MATCH (r:Post) WHERE r.repo = $p0 RETURN count(r) AS cnt",
      undefined,
      { p0: "did:web:alice" },
    );

    expect(result.columns).toEqual(["cnt"]);
    expect(result.rows).toEqual([[2]]);
    expect(reader.stats.workersHit).toBe(1);
  });
});
