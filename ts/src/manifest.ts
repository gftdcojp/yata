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
  tables: GraphManifestTables;
  seq: GraphManifestSeqRange;
  dirty_labels?: string[];
  generated_at_ms?: number;
}

export interface ManifestStore {
  get(key: string): Promise<string | null>;
  put(key: string, value: string): Promise<void>;
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
