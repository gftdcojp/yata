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
  'partition_id': number;
  'tx_id': string;
  'emitted_at_ms': number;
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

export function createVertexPipelineEnvelope(
  mutation: VertexPipelineMutation,
  source?: string | null,
): GraphVertexPipelineEnvelope {
  return {
    kind: "vertex",
    'partition_id': mutation.partition_id,
    'tx_id': mutation.tx_id,
    'emitted_at_ms': mutation.updated_at_ms,
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
    'partition_id': mutation.partition_id,
    'tx_id': mutation.tx_id,
    'emitted_at_ms': mutation.updated_at_ms,
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
