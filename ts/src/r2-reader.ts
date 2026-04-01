/**
 * @gftd/yata — Arrow IPC R2 reader for Workers-side read path.
 *
 * Reads compaction manifest + per-label Arrow IPC segments from R2.
 * Pure TS, Workers-compatible (no native bindings, no filesystem).
 *
 * R2 paths (source of truth — matches Rust yata-engine/compaction.rs):
 *   Manifest: {prefix}log/compacted/{pid}/manifest.json
 *   Per-label: {prefix}log/compacted/{pid}/label/{label}.arrow
 */

import { tableFromIPC, type Table } from "apache-arrow";
import type { R2BucketLike } from "./manifest.js";

// ── CompactionManifest (mirrors Rust CompactionManifest in compaction.rs) ──

export interface LabelSegmentState {
  key: string;
  'max_seq': number;
  'entry_count': number;
  'segment_bytes': number;
  blake3_hex: string;
}

export interface CompactionManifest {
  'partition_id': number;
  version: number;
  'compacted_segment_key': string;
  'compacted_seq': number;
  'entry_count': number;
  labels: string[];
  'created_at_ms': number;
  'segment_bytes': number;
  'label_segments': Record<string, LabelSegmentState>;
}

// ── R2 Fragment Store ──

export interface FragmentStore {
  getManifest(pid: number): Promise<CompactionManifest | null>;
  getFragment(key: string): Promise<ArrayBuffer | null>;
}

export function createR2FragmentStore(
  bucket: R2BucketLike,
  prefix = "yata/",
): FragmentStore {
  const p = prefix.endsWith("/") ? prefix : prefix + "/";

  return {
    async getManifest(pid: number): Promise<CompactionManifest | null> {
      const key = `${p}log/compacted/${pid}/manifest.json`;
      const obj = await bucket.get(key);
      if (!obj) return null;
      const text = await obj.text();
      return JSON.parse(text) as CompactionManifest;
    },

    async getFragment(key: string): Promise<ArrayBuffer | null> {
      const obj = await (bucket as R2BucketWithArrayBuffer).get(key);
      if (!obj) return null;
      if ("arrayBuffer" in obj && typeof obj.arrayBuffer === "function") {
        return obj.arrayBuffer();
      }
      // Fallback: text → Uint8Array (for test mocks)
      const text = await obj.text();
      const encoder = new TextEncoder();
      return encoder.encode(text).buffer as ArrayBuffer;
    },
  };
}

interface R2BucketWithArrayBuffer {
  get(key: string): Promise<{ text(): Promise<string>; arrayBuffer(): Promise<ArrayBuffer> } | null>;
}

export function createFetchFragmentStore(
  baseUrl: string,
): FragmentStore {
  const base = baseUrl.replace(/\/+$/, "");

  return {
    async getManifest(pid: number): Promise<CompactionManifest | null> {
      const res = await fetch(`${base}/log/compacted/${pid}/manifest.json`);
      if (res.status === 404) return null;
      if (!res.ok) throw new Error(`manifest fetch failed: ${res.status}`);
      return res.json() as Promise<CompactionManifest>;
    },

    async getFragment(key: string): Promise<ArrayBuffer | null> {
      const res = await fetch(`${base}/${key}`);
      if (res.status === 404) return null;
      if (!res.ok) throw new Error(`fragment fetch failed: ${res.status}`);
      return res.arrayBuffer();
    },
  };
}

// ── Label Loader ──

export interface LabelData {
  label: string;
  table: Table;
  entryCount: number;
}

/**
 * Load per-label Arrow IPC segments from R2.
 * Returns decoded Arrow Tables keyed by label name.
 */
export async function loadLabels(
  store: FragmentStore,
  manifest: CompactionManifest,
  labels: string[],
): Promise<Map<string, LabelData>> {
  const result = new Map<string, LabelData>();

  if (manifest.version < 2 || !manifest.label_segments) {
    // v1 monolithic — not supported in Workers read path (too large)
    return result;
  }

  const fetches = labels
    .filter(label => label in manifest.label_segments)
    .map(async (label) => {
      const seg = manifest.label_segments[label];
      const buf = await store.getFragment(seg.key);
      if (!buf) return null;
      const table = tableFromIPC(new Uint8Array(buf));
      return { label, table, entryCount: seg.entry_count } as LabelData;
    });

  const results = await Promise.all(fetches);
  for (const ld of results) {
    if (ld) result.set(ld.label, ld);
  }

  return result;
}

/**
 * Load all labels from the compaction manifest.
 */
export async function loadAllLabels(
  store: FragmentStore,
  manifest: CompactionManifest,
): Promise<Map<string, LabelData>> {
  return loadLabels(store, manifest, manifest.labels);
}
