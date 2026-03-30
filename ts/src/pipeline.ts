import type { GraphManifest, ManifestStore } from "./manifest.js";
import {
  projectEdgeMutation,
  projectVertexMutation,
  type EdgeMutation,
  type VertexMutation,
  type YataWritableTables,
} from "./writer.js";

export interface PipelineWriterLike {
  send(records: unknown[]): Promise<void>;
}

export interface GraphPipelineEnvelopeBase {
  partition_id: number;
  tx_id: string;
  emitted_at_ms: number;
  source?: string | null;
}

export interface VertexPipelineMutation
  extends Omit<VertexMutation, "seq"> {}

export interface EdgePipelineMutation
  extends Omit<EdgeMutation, "seq"> {}

export interface GraphVertexPipelineEnvelope extends GraphPipelineEnvelopeBase {
  kind: "vertex";
  mutation: VertexPipelineMutation;
}

export interface GraphEdgePipelineEnvelope extends GraphPipelineEnvelopeBase {
  kind: "edge";
  mutation: EdgePipelineMutation;
}

export type GraphPipelineEnvelope =
  | GraphVertexPipelineEnvelope
  | GraphEdgePipelineEnvelope;

/**
 * Ordered projector input. `seq` must come from Pipeline delivery order,
 * not from the request path.
 */
export interface GraphProjectedRecord {
  seq: number;
  envelope: GraphPipelineEnvelope;
}

export interface GraphPipelineConsumerRecord {
  seq: number;
  envelope?: GraphPipelineEnvelope;
  payload?: GraphPipelineEnvelope | string;
}

export interface PipelineWalOp {
  action: "create" | "update" | "delete";
  collection: string;
  rkey: string;
}

export interface PipelineWalRecordLike {
  seq: number;
  rev?: string;
  cid?: string;
  repo: string;
  ops: string | PipelineWalOp[];
  created?: number;
  records?: string | Record<string, string | undefined>;
}

export interface PipelineWalVertexDefaults {
  partition_id: number;
  pk_key?: string;
  owner_did?: string | null;
  source?: string | null;
}

function defaultCollectionToLabel(collection: string): string {
  const last = collection.split(".").pop() ?? collection;
  return last
    .split("_")
    .filter(Boolean)
    .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
    .join("");
}

function parseWalOps(
  ops: string | PipelineWalOp[],
): PipelineWalOp[] {
  return typeof ops === "string"
    ? JSON.parse(ops) as PipelineWalOp[]
    : ops;
}

function parseWalRecords(
  records: string | Record<string, string | undefined> | undefined,
): Record<string, string | undefined> {
  if (records == null) return {};
  return typeof records === "string"
    ? JSON.parse(records) as Record<string, string | undefined>
    : records;
}

export function createVertexPipelineEnvelope(
  mutation: VertexPipelineMutation,
  source?: string | null,
): GraphVertexPipelineEnvelope {
  return {
    kind: "vertex",
    partition_id: mutation.partition_id,
    tx_id: mutation.tx_id,
    emitted_at_ms: mutation.updated_at_ms,
    source: source ?? null,
    mutation,
  };
}

export function createEdgePipelineEnvelope(
  mutation: EdgePipelineMutation,
  source?: string | null,
): GraphEdgePipelineEnvelope {
  return {
    kind: "edge",
    partition_id: mutation.partition_id,
    tx_id: mutation.tx_id,
    emitted_at_ms: mutation.updated_at_ms,
    source: source ?? null,
    mutation,
  };
}

export async function emitGraphPipelineEnvelope(
  pipeline: PipelineWriterLike,
  envelope: GraphPipelineEnvelope,
): Promise<void> {
  await pipeline.send([envelope]);
}

export function stringifyGraphPipelineEnvelope(
  envelope: GraphPipelineEnvelope,
): string {
  return JSON.stringify(envelope);
}

export function parseGraphPipelineEnvelope(
  value: string,
): GraphPipelineEnvelope {
  return JSON.parse(value) as GraphPipelineEnvelope;
}

export function decodeGraphPipelineEnvelope(
  value: GraphPipelineEnvelope | string,
): GraphPipelineEnvelope {
  return typeof value === "string"
    ? parseGraphPipelineEnvelope(value)
    : value;
}

export function walRecordToVertexEnvelopes(
  record: PipelineWalRecordLike,
  defaults: PipelineWalVertexDefaults,
  labelFromCollection: (collection: string) => string = defaultCollectionToLabel,
): GraphVertexPipelineEnvelope[] {
  const ops = parseWalOps(record.ops);
  const records = parseWalRecords(record.records);
  const pkKey = defaults.pk_key ?? "rkey";
  const emittedAt = record.created ?? Date.now();

  return ops.map((op) => {
    const json = records[`${op.collection}/${op.rkey}`];
    return createVertexPipelineEnvelope(
      {
        partition_id: defaults.partition_id,
        tx_id: record.rev ?? record.cid ?? `${record.repo}:${op.collection}:${op.rkey}`,
        op: op.action === "delete" ? "delete" : "upsert",
        label: labelFromCollection(op.collection),
        pk_key: pkKey,
        pk_value: op.rkey,
        vid: `${record.repo}/${op.collection}/${op.rkey}`,
        repo: record.repo,
        rkey: op.rkey,
        owner_did: defaults.owner_did ?? record.repo,
        created_at_ms: emittedAt,
        updated_at_ms: emittedAt,
        tombstone: op.action === "delete",
        props_json: op.action === "delete" ? null : (json ?? null),
        props_hash: null,
      },
      defaults.source ?? "pipeline-wal",
    );
  });
}

export function walRecordToProjectedRecords(
  record: PipelineWalRecordLike,
  defaults: PipelineWalVertexDefaults,
  labelFromCollection?: (collection: string) => string,
): GraphProjectedRecord[] {
  return walRecordToVertexEnvelopes(
    record,
    defaults,
    labelFromCollection,
  ).map((envelope) => ({
    seq: record.seq,
    envelope,
  }));
}

export function toGraphProjectedRecord(
  record: GraphPipelineConsumerRecord,
): GraphProjectedRecord {
  const payload = record.envelope ?? record.payload;
  if (payload == null) {
    throw new Error("graph pipeline consumer record requires envelope or payload");
  }
  const envelope = decodeGraphPipelineEnvelope(payload);
  return {
    seq: record.seq,
    envelope,
  };
}

export function toGraphProjectedRecords(
  records: GraphPipelineConsumerRecord[],
): GraphProjectedRecord[] {
  return records.map(toGraphProjectedRecord);
}

export function hydrateVertexMutation(
  seq: number,
  envelope: GraphVertexPipelineEnvelope,
): VertexMutation {
  return {
    ...envelope.mutation,
    seq,
  };
}

export function hydrateEdgeMutation(
  seq: number,
  envelope: GraphEdgePipelineEnvelope,
): EdgeMutation {
  return {
    ...envelope.mutation,
    seq,
  };
}

export async function projectGraphPipelineRecord(
  tables: YataWritableTables,
  store: ManifestStore,
  currentManifest: GraphManifest,
  record: GraphProjectedRecord,
): Promise<GraphManifest> {
  if (record.envelope.kind === "vertex") {
    return projectVertexMutation(
      tables,
      store,
      currentManifest,
      hydrateVertexMutation(record.seq, record.envelope),
    );
  }

  return projectEdgeMutation(
    tables,
    store,
    currentManifest,
    hydrateEdgeMutation(record.seq, record.envelope),
  );
}

export async function projectGraphPipelineBatch(
  tables: YataWritableTables,
  store: ManifestStore,
  currentManifest: GraphManifest,
  records: GraphProjectedRecord[],
): Promise<GraphManifest> {
  let manifest = currentManifest;
  for (const record of records) {
    manifest = await projectGraphPipelineRecord(
      tables,
      store,
      manifest,
      record,
    );
  }
  return manifest;
}
