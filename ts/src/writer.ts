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
  vertex_log: LanceWritableTableLike;
  edge_log: LanceWritableTableLike;
  vertex_live: LanceWritableTableLike;
  edge_live_out: LanceWritableTableLike;
  edge_live_in: LanceWritableTableLike;
}

export interface VertexMutation {
  partition_id: number;
  seq: number;
  tx_id: string;
  op: "upsert" | "delete";
  label: string;
  pk_key: string;
  pk_value: string;
  vid: string;
  repo?: string | null;
  rkey?: string | null;
  owner_did?: string | null;
  created_at_ms: number;
  updated_at_ms: number;
  tombstone: boolean;
  props_json?: string | null;
  props_hash?: string | null;
}

export interface EdgeMutation {
  partition_id: number;
  seq: number;
  tx_id: string;
  op: "upsert" | "delete";
  edge_label: string;
  pk_key: string;
  pk_value: string;
  eid: string;
  src_vid: string;
  dst_vid: string;
  src_label?: string | null;
  dst_label?: string | null;
  created_at_ms: number;
  updated_at_ms: number;
  tombstone: boolean;
  props_json?: string | null;
  props_hash?: string | null;
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
  await tables.vertex_log.add([{ ...mutation }]);
  await tables.vertex_live.add([{
    partition_id: mutation.partition_id,
    label: mutation.label,
    pk_key: mutation.pk_key,
    pk_value: mutation.pk_value,
    vid: mutation.vid,
    alive: !mutation.tombstone,
    latest_seq: mutation.seq,
    repo: mutation.repo ?? null,
    rkey: mutation.rkey ?? null,
    owner_did: mutation.owner_did ?? null,
    updated_at_ms: mutation.updated_at_ms,
    props_json: mutation.props_json ?? null,
  }]);

  const next = GraphCatalog.createManifest(
    currentManifest.partition_id,
    currentManifest.version + 1,
    {
      min: Math.min(currentManifest.seq.min, mutation.seq),
      max: Math.max(currentManifest.seq.max, mutation.seq),
    },
    baseUriFromTableUri(currentManifest.tables.vertex_log.uri),
  );
  next.dirty_labels = [mutation.label];
  next.generated_at_ms = mutation.updated_at_ms;
  await GraphCatalog.publish(store, next);
  return next;
}

export async function projectEdgeMutation(
  tables: YataWritableTables,
  store: ManifestStore,
  currentManifest: GraphManifest,
  mutation: EdgeMutation,
): Promise<GraphManifest> {
  await tables.edge_log.add([{ ...mutation }]);
  await tables.edge_live_out.add([{
    partition_id: mutation.partition_id,
    edge_label: mutation.edge_label,
    pk_key: mutation.pk_key,
    pk_value: mutation.pk_value,
    eid: mutation.eid,
    src_vid: mutation.src_vid,
    dst_vid: mutation.dst_vid,
    src_label: mutation.src_label ?? null,
    dst_label: mutation.dst_label ?? null,
    alive: !mutation.tombstone,
    latest_seq: mutation.seq,
    updated_at_ms: mutation.updated_at_ms,
    props_json: mutation.props_json ?? null,
  }]);
  await tables.edge_live_in.add([{
    partition_id: mutation.partition_id,
    edge_label: mutation.edge_label,
    pk_key: mutation.pk_key,
    pk_value: mutation.pk_value,
    eid: mutation.eid,
    dst_vid: mutation.dst_vid,
    src_vid: mutation.src_vid,
    src_label: mutation.src_label ?? null,
    dst_label: mutation.dst_label ?? null,
    alive: !mutation.tombstone,
    latest_seq: mutation.seq,
    updated_at_ms: mutation.updated_at_ms,
    props_json: mutation.props_json ?? null,
  }]);

  const next = GraphCatalog.createManifest(
    currentManifest.partition_id,
    currentManifest.version + 1,
    {
      min: Math.min(currentManifest.seq.min, mutation.seq),
      max: Math.max(currentManifest.seq.max, mutation.seq),
    },
    baseUriFromTableUri(currentManifest.tables.edge_log.uri),
  );
  next.dirty_labels = [mutation.edge_label];
  next.generated_at_ms = mutation.updated_at_ms;
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
