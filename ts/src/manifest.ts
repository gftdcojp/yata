import {
  EDGE_LIVE_IN_TABLE,
  EDGE_LIVE_OUT_TABLE,
  EDGE_LOG_TABLE,
  GRAPH_FORMAT,
  VERTEX_LIVE_TABLE,
  VERTEX_LOG_TABLE,
} from "./schema.js";

export interface GraphManifestTableRef {
  'tableName': string;
  uri: string;
  version?: number;
  etag?: string | null;
}

export interface GraphManifestTables {
  'vertexLog': GraphManifestTableRef;
  'edgeLog': GraphManifestTableRef;
  'vertexLive': GraphManifestTableRef;
  'edgeLiveOut': GraphManifestTableRef;
  'edgeLiveIn': GraphManifestTableRef;
}

export interface GraphManifestSeqRange {
  min: number;
  max: number;
}

export interface GraphManifest {
  'manifestVersion': number;
  'graphFormat': typeof GRAPH_FORMAT;
  'contractVersion': number;
  'partitionId': number;
  version: number;
  'keyLayout': GraphManifestKeyLayout;
  tables: GraphManifestTables;
  seq: GraphManifestSeqRange;
  dirtyLabels?: string[];
  generatedAtMs?: number;
}

export interface GraphManifestKeyLayout {
  'manifestPrefix': string;
  'latestKey': string;
  'pointerKey': string;
  'versionedKey': string;
}

export interface GraphManifestLatestPointer {
  'graphFormat': typeof GRAPH_FORMAT;
  'contractVersion': number;
  'partitionId': number;
  version: number;
  'versionedKey': string;
  etag?: string | null;
  updatedAtMs?: number;
}

export interface ManifestStore {
  get(key: string): Promise<string | null>;
  put(key: string, value: string): Promise<void>;
}

export interface R2BucketLike {
  get(key: string): Promise<{ text(): Promise<string> } | null>;
  put(
    key: string,
    value: string,
    options?: {
      httpMetadata?: {
        contentType?: string;
      };
    },
  ): Promise<unknown>;
}

export function makeManifestTableRef(tableName: string, uri: string): GraphManifestTableRef {
  return { 'tableName': tableName, uri };
}

export function createGraphManifest(
  partitionId: number,
  version: number,
  seq: GraphManifestSeqRange,
  baseUri: string,
): GraphManifest {
  const base = baseUri.replace(/\/+$/, "");
  return {
    'manifestVersion': 1,
    'graphFormat': GRAPH_FORMAT,
    'contractVersion': 1,
    'partitionId': partitionId,
    version,
    'keyLayout': createManifestKeyLayout(partitionId, version),
    tables: {
      'vertexLog': makeManifestTableRef(VERTEX_LOG_TABLE, `${base}/${VERTEX_LOG_TABLE}`),
      'edgeLog': makeManifestTableRef(EDGE_LOG_TABLE, `${base}/${EDGE_LOG_TABLE}`),
      'vertexLive': makeManifestTableRef(VERTEX_LIVE_TABLE, `${base}/${VERTEX_LIVE_TABLE}`),
      'edgeLiveOut': makeManifestTableRef(EDGE_LIVE_OUT_TABLE, `${base}/${EDGE_LIVE_OUT_TABLE}`),
      'edgeLiveIn': makeManifestTableRef(EDGE_LIVE_IN_TABLE, `${base}/${EDGE_LIVE_IN_TABLE}`),
    },
    seq,
  };
}

export function createManifestKeyLayout(
  partitionId: number,
  version: number,
): GraphManifestKeyLayout {
  const manifestPrefix = `manifests/partitions/${partitionId}`;
  return {
    'manifestPrefix': manifestPrefix,
    'latestKey': `${manifestPrefix}/latest.json`,
    'pointerKey': `${manifestPrefix}/latest.json`,
    'versionedKey': `${manifestPrefix}/manifest-${String(version).padStart(20, "0")}.json`,
  };
}

export function stringifyManifest(manifest: GraphManifest): string {
  return JSON.stringify(manifest, null, 2);
}

export function parseManifest(json: string): GraphManifest {
  return JSON.parse(json) as GraphManifest;
}

export async function saveManifest(
  store: ManifestStore,
  key: string,
  manifest: GraphManifest,
): Promise<void> {
  await store.put(key, stringifyManifest(manifest));
}

export async function loadManifest(
  store: ManifestStore,
  key: string,
): Promise<GraphManifest | null> {
  const json = await store.get(key);
  return json == null ? null : parseManifest(json);
}

export async function loadLatestPointer(
  store: ManifestStore,
  key: string,
): Promise<GraphManifestLatestPointer | null> {
  const json = await store.get(key);
  return json == null ? null : parseLatestPointer(json);
}

export async function loadManifestViaLatest(
  store: ManifestStore,
  latestKey: string,
): Promise<GraphManifest | null> {
  const pointer = await loadLatestPointer(store, latestKey);
  return pointer == null ? null : loadManifest(store, pointer.versionedKey);
}

/**
 * Publish a manifest with atomic pointer update.
 *
 * Writes the immutable versioned manifest first, then updates the latest
 * pointer with retry + read-after-write verification to guard against
 * partial R2 writes.
 */
export async function publishManifest(
  store: ManifestStore,
  manifest: GraphManifest,
): Promise<void> {
  // 1. Write versioned manifest (immutable, idempotent)
  await saveManifest(store, manifest.keyLayout.versionedKey, manifest);

  // 2. Atomic pointer update with retry + read-after-write verify
  const pointerBody = stringifyLatestPointer(createLatestPointer(manifest));
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      await store.put(manifest.keyLayout.pointerKey, pointerBody);
      // Verify: read-after-write consistency check
      const verify = await store.get(manifest.keyLayout.pointerKey);
      if (verify) {
        try {
          const parsed = JSON.parse(verify);
          if (parsed.version === manifest.version) return;
        } catch { /* parse error, retry */ }
      }
    } catch (e) {
      if (attempt === 2) throw e;
    }
    // Exponential backoff
    await new Promise(r => setTimeout(r, 100 * Math.pow(2, attempt)));
  }
  console.error(`publishManifest: pointer verify failed after 3 attempts (v=${manifest.version})`);
}

export function createLatestPointer(
  manifest: GraphManifest,
): GraphManifestLatestPointer {
  return {
    'graphFormat': GRAPH_FORMAT,
    'contractVersion': manifest.contractVersion,
    'partitionId': manifest.partitionId,
    version: manifest.version,
    'versionedKey': manifest.keyLayout.versionedKey,
    etag: manifest.tables.vertexLive.etag ?? manifest.tables.edgeLiveOut.etag,
    'updatedAtMs': manifest.generatedAtMs,
  };
}

export function stringifyLatestPointer(pointer: GraphManifestLatestPointer): string {
  return JSON.stringify(pointer, null, 2);
}

export function parseLatestPointer(json: string): GraphManifestLatestPointer {
  return JSON.parse(json) as GraphManifestLatestPointer;
}

export function createFetchManifestStore(fetchImpl: typeof fetch = fetch): ManifestStore {
  return {
    async get(key: string): Promise<string | null> {
      const res = await fetchImpl(key, { method: "GET" });
      if (res.status === 404) return null;
      if (!res.ok) throw new Error(`manifest GET failed: ${res.status}`);
      return res.text();
    },
    async put(key: string, value: string): Promise<void> {
      const res = await fetchImpl(key, {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: value,
      });
      if (!res.ok) throw new Error(`manifest PUT failed: ${res.status}`);
    },
  };
}

export function createR2ManifestStore(
  bucket: R2BucketLike,
  prefix = "",
): ManifestStore {
  const normalizedPrefix = prefix.replace(/\/+$/, "");
  const objectKey = (key: string) => {
    const trimmed = key.replace(/^\/+/, "");
    return normalizedPrefix ? `${normalizedPrefix}/${trimmed}` : trimmed;
  };

  return {
    async get(key: string): Promise<string | null> {
      const obj = await bucket.get(objectKey(key));
      return obj ? obj.text() : null;
    },
    async put(key: string, value: string): Promise<void> {
      await bucket.put(objectKey(key), value, {
        httpMetadata: { contentType: "application/json" },
      });
    },
  };
}
