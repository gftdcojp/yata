import { describe, it, expect } from "vitest";
import {
  loadLabels,
  type CompactionManifest,
  type FragmentStore,
} from "../src/r2-reader.js";
import { makeVertexTable, makeArrowBuffer } from "./test-helpers.js";

function makeTestManifest(): CompactionManifest {
  return {
    partition_id: 0,
    version: 2,
    compacted_segment_key: "",
    compacted_seq: 100,
    entry_count: 5,
    labels: ["Post", "Profile"],
    created_at_ms: Date.now(),
    segment_bytes: 1024,
    label_segments: {
      Post: {
        key: "yata/log/compacted/0/label/Post.arrow",
        max_seq: 100,
        entry_count: 3,
        segment_bytes: 512,
        blake3_hex: "abc",
      },
      Profile: {
        key: "yata/log/compacted/0/label/Profile.arrow",
        max_seq: 80,
        entry_count: 2,
        segment_bytes: 256,
        blake3_hex: "def",
      },
    },
  };
}

function makeTestArrowBuffer(): ArrayBuffer {
  const table = makeVertexTable([
    { vid: "v1", repo: "did:web:alice", rkey: "r1", updated_at_ms: 1000 },
    { vid: "v2", repo: "did:web:bob", rkey: "r2", updated_at_ms: 2000 },
  ]);
  return makeArrowBuffer(table);
}

function createMockStore(fragments: Map<string, ArrayBuffer>): FragmentStore {
  return {
    async getManifest() {
      return makeTestManifest();
    },
    async getFragment(key: string) {
      return fragments.get(key) ?? null;
    },
  };
}

describe("r2-reader", () => {
  it("loads labels from v2 manifest", async () => {
    const arrowBuf = makeTestArrowBuffer();
    const store = createMockStore(
      new Map([["yata/log/compacted/0/label/Post.arrow", arrowBuf]]),
    );
    const manifest = makeTestManifest();

    const result = await loadLabels(store, manifest, ["Post"]);
    expect(result.has("Post")).toBe(true);
    const postData = result.get("Post")!;
    expect(postData.table.numRows).toBe(2);
    expect(postData.entryCount).toBe(3);
  });

  it("skips labels not in manifest", async () => {
    const store = createMockStore(new Map());
    const manifest = makeTestManifest();

    const result = await loadLabels(store, manifest, ["NonExistent"]);
    expect(result.size).toBe(0);
  });

  it("skips labels where fragment fetch fails", async () => {
    const store = createMockStore(new Map());
    const manifest = makeTestManifest();

    const result = await loadLabels(store, manifest, ["Post"]);
    expect(result.has("Post")).toBe(false);
  });

  it("loads multiple labels in parallel", async () => {
    const arrowBuf = makeTestArrowBuffer();
    const store = createMockStore(
      new Map([
        ["yata/log/compacted/0/label/Post.arrow", arrowBuf],
        ["yata/log/compacted/0/label/Profile.arrow", arrowBuf],
      ]),
    );
    const manifest = makeTestManifest();

    const result = await loadLabels(store, manifest, ["Post", "Profile"]);
    expect(result.size).toBe(2);
  });

  it("returns empty for v1 manifest", async () => {
    const store = createMockStore(new Map());
    const manifest: CompactionManifest = {
      ...makeTestManifest(),
      version: 1,
      label_segments: {},
    };

    const result = await loadLabels(store, manifest, ["Post"]);
    expect(result.size).toBe(0);
  });
});
