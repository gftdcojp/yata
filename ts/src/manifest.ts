import {
  EDGE_LIVE_IN_TABLE,
  EDGE_LIVE_OUT_TABLE,
  EDGE_LOG_TABLE,
  GRAPH_FORMAT,
  VERTEX_LIVE_TABLE,
  VERTEX_LOG_TABLE,
} from "./schema.js";

export interface GraphManifestTableRef {
  table_name: string;
  uri: string;
  version?: number;
  etag?: string | null;
}

export interface GraphManifestTables {
  vertex_log: GraphManifestTableRef;
  edge_log: GraphManifestTableRef;
  vertex_live: GraphManifestTableRef;
  edge_live_out: GraphManifestTableRef;
  edge_live_in: GraphManifestTableRef;
}

export interface GraphManifestSeqRange {
  min: number;
  max: number;
}

export interface GraphManifest {
  manifest_version: number;
  graph_format: typeof GRAPH_FORMAT;
  contract_version: number;
  partition_id: number;
  version: number;
  key_layout: GraphManifestKeyLayout;
  tables: GraphManifestTables;
  seq: GraphManifestSeqRange;
  dirty_labels?: string[];
  generated_at_ms?: number;
}

export interface GraphManifestKeyLayout {
  manifest_prefix: string;
  latest_key: string;
  pointer_key: string;
  versioned_key: string;
}

export interface GraphManifestLatestPointer {
  graph_format: typeof GRAPH_FORMAT;
  contract_version: number;
  partition_id: number;
  version: number;
  versioned_key: string;
  etag?: string | null;
  updated_at_ms?: number;
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

export function makeManifestTableRef(table_name: string, uri: string): GraphManifestTableRef {
  return { table_name, uri };
}

export function createGraphManifest(
  partitionId: number,
  version: number,
  seq: GraphManifestSeqRange,
  baseUri: string,
): GraphManifest {
  const base = baseUri.replace(/\/+$/, "");
  return {
    manifest_version: 1,
    graph_format: GRAPH_FORMAT,
    contract_version: 1,
    partition_id: partitionId,
    version,
    key_layout: createManifestKeyLayout(partitionId, version),
    tables: {
      vertex_log: makeManifestTableRef(VERTEX_LOG_TABLE, `${base}/${VERTEX_LOG_TABLE}`),
      edge_log: makeManifestTableRef(EDGE_LOG_TABLE, `${base}/${EDGE_LOG_TABLE}`),
      vertex_live: makeManifestTableRef(VERTEX_LIVE_TABLE, `${base}/${VERTEX_LIVE_TABLE}`),
      edge_live_out: makeManifestTableRef(EDGE_LIVE_OUT_TABLE, `${base}/${EDGE_LIVE_OUT_TABLE}`),
      edge_live_in: makeManifestTableRef(EDGE_LIVE_IN_TABLE, `${base}/${EDGE_LIVE_IN_TABLE}`),
    },
    seq,
  };
}

export function createManifestKeyLayout(
  partitionId: number,
  version: number,
): GraphManifestKeyLayout {
  const manifest_prefix = `manifests/partitions/${partitionId}`;
  return {
    manifest_prefix,
    latest_key: `${manifest_prefix}/latest.json`,
    pointer_key: `${manifest_prefix}/latest.json`,
    versioned_key: `${manifest_prefix}/manifest-${String(version).padStart(20, "0")}.json`,
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
  return pointer == null ? null : loadManifest(store, pointer.versioned_key);
}

export async function publishManifest(
  store: ManifestStore,
  manifest: GraphManifest,
): Promise<void> {
  await saveManifest(store, manifest.key_layout.versioned_key, manifest);
  await store.put(
    manifest.key_layout.pointer_key,
    stringifyLatestPointer(createLatestPointer(manifest)),
  );
}

export function createLatestPointer(
  manifest: GraphManifest,
): GraphManifestLatestPointer {
  return {
    graph_format: GRAPH_FORMAT,
    contract_version: manifest.contract_version,
    partition_id: manifest.partition_id,
    version: manifest.version,
    versioned_key: manifest.key_layout.versioned_key,
    etag: manifest.tables.vertex_live.etag ?? manifest.tables.edge_live_out.etag,
    updated_at_ms: manifest.generated_at_ms,
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
