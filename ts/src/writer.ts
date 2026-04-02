import type { GraphManifest, ManifestStore } from "./manifest.js";
import { GraphCatalog } from "./catalog.js";

/**
 * Projector/materializer-side Lance writer.
 *
 * User-facing write ACK should come from Pipeline.send().
 * These helpers are for the async projection stage that consumes
 * ordered pipeline events and appends them into Lance tables.
 */
export interface LanceWritableTableLike {
  add(rows: Record<string, unknown>[]): Promise<unknown>;
}

export interface YataWritableTables {
  'vertexLog': LanceWritableTableLike;
  'edgeLog': LanceWritableTableLike;
  'vertexLive': LanceWritableTableLike;
  'edgeLiveOut': LanceWritableTableLike;
  'edgeLiveIn': LanceWritableTableLike;
}

export interface VertexMutation {
  'partitionId': number;
  seq: number;
  'txId': string;
  op: "upsert" | "delete";
  label: string;
  'pkKey': string;
  'pkValue': string;
  vid: string;
  repo?: string | null;
  rkey?: string | null;
  ownerDid?: string | null;
  'createdAtMs': number;
  'updatedAtMs': number;
  tombstone: boolean;
  propsJson?: string | null;
  propsHash?: string | null;
}

export interface EdgeMutation {
  'partitionId': number;
  seq: number;
  'txId': string;
  op: "upsert" | "delete";
  'edgeLabel': string;
  'pkKey': string;
  'pkValue': string;
  eid: string;
  'srcVid': string;
  'dstVid': string;
  srcLabel?: string | null;
  dstLabel?: string | null;
  'createdAtMs': number;
  'updatedAtMs': number;
  tombstone: boolean;
  propsJson?: string | null;
  propsHash?: string | null;
}

function baseUriFromTableUri(uri: string): string {
  const idx = uri.lastIndexOf("/");
  return idx >= 0 ? uri.slice(0, idx) : uri;
}

export async function projectVertexMutation(
  tables: YataWritableTables,
  store: ManifestStore,
  currentManifest: GraphManifest,
  mutation: VertexMutation,
): Promise<GraphManifest> {
  await tables.vertexLog.add([{ ...mutation }]);
  await tables.vertexLive.add([{
    'partitionId': mutation.partitionId,
    label: mutation.label,
    'pkKey': mutation.pkKey,
    'pkValue': mutation.pkValue,
    vid: mutation.vid,
    alive: !mutation.tombstone,
    'latestSeq': mutation.seq,
    repo: mutation.repo ?? null,
    rkey: mutation.rkey ?? null,
    'ownerDid': mutation.ownerDid ?? null,
    'updatedAtMs': mutation.updatedAtMs,
    'propsJson': mutation.propsJson ?? null,
  }]);

  const next = GraphCatalog.createManifest(
    currentManifest.partitionId,
    currentManifest.version + 1,
    {
      min: Math.min(currentManifest.seq.min, mutation.seq),
      max: Math.max(currentManifest.seq.max, mutation.seq),
    },
    baseUriFromTableUri(currentManifest.tables.vertexLog.uri),
  );
  next.dirtyLabels = [mutation.label];
  next.generatedAtMs = mutation.updatedAtMs;
  await GraphCatalog.publish(store, next);
  return next;
}

export async function projectEdgeMutation(
  tables: YataWritableTables,
  store: ManifestStore,
  currentManifest: GraphManifest,
  mutation: EdgeMutation,
): Promise<GraphManifest> {
  await tables.edgeLog.add([{ ...mutation }]);
  await tables.edgeLiveOut.add([{
    'partitionId': mutation.partitionId,
    'edgeLabel': mutation.edgeLabel,
    'pkKey': mutation.pkKey,
    'pkValue': mutation.pkValue,
    eid: mutation.eid,
    'srcVid': mutation.srcVid,
    'dstVid': mutation.dstVid,
    'srcLabel': mutation.srcLabel ?? null,
    'dstLabel': mutation.dstLabel ?? null,
    alive: !mutation.tombstone,
    'latestSeq': mutation.seq,
    'updatedAtMs': mutation.updatedAtMs,
    'propsJson': mutation.propsJson ?? null,
  }]);
  await tables.edgeLiveIn.add([{
    'partitionId': mutation.partitionId,
    'edgeLabel': mutation.edgeLabel,
    'pkKey': mutation.pkKey,
    'pkValue': mutation.pkValue,
    eid: mutation.eid,
    'dstVid': mutation.dstVid,
    'srcVid': mutation.srcVid,
    'srcLabel': mutation.srcLabel ?? null,
    'dstLabel': mutation.dstLabel ?? null,
    alive: !mutation.tombstone,
    'latestSeq': mutation.seq,
    'updatedAtMs': mutation.updatedAtMs,
    'propsJson': mutation.propsJson ?? null,
  }]);

  const next = GraphCatalog.createManifest(
    currentManifest.partitionId,
    currentManifest.version + 1,
    {
      min: Math.min(currentManifest.seq.min, mutation.seq),
      max: Math.max(currentManifest.seq.max, mutation.seq),
    },
    baseUriFromTableUri(currentManifest.tables.edgeLog.uri),
  );
  next.dirtyLabels = [mutation.edgeLabel];
  next.generatedAtMs = mutation.updatedAtMs;
  await GraphCatalog.publish(store, next);
  return next;
}

/**
 * Backward-compatible aliases.
 * Prefer `projectVertexMutation` / `projectEdgeMutation` for new code.
 */
export async function applyVertexMutation(
  tables: YataWritableTables,
  store: ManifestStore,
  currentManifest: GraphManifest,
  mutation: VertexMutation,
): Promise<GraphManifest> {
  return projectVertexMutation(tables, store, currentManifest, mutation);
}

export async function applyEdgeMutation(
  tables: YataWritableTables,
  store: ManifestStore,
  currentManifest: GraphManifest,
  mutation: EdgeMutation,
): Promise<GraphManifest> {
  return projectEdgeMutation(tables, store, currentManifest, mutation);
}
