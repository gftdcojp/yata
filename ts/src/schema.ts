/**
 * Shared yata Lance graph table contract for TypeScript runtimes.
 *
 * This mirrors the Rust Arrow schema in `yata-lance/src/schema.rs`.
 */

export const GRAPH_FORMAT = "yata-lance-graph-v1" as const;

export const VERTEX_LOG_TABLE = "yataVertexLog" as const;
export const EDGE_LOG_TABLE = "yataEdgeLog" as const;
export const VERTEX_LIVE_TABLE = "yataVertexLive" as const;
export const EDGE_LIVE_OUT_TABLE = "yataEdgeLiveOut" as const;
export const EDGE_LIVE_IN_TABLE = "yataEdgeLiveIn" as const;

export type YataScalarType = "uint32" | "uint64" | "utf8" | "bool";

export interface YataField {
  name: string;
  type: YataScalarType;
  nullable: boolean;
}

export interface YataTableSchema {
  tableName: string;
  primaryKey: string[];
  sortKey: string[];
  compactionKey: string[];
  fields: YataField[];
}

function field(name: string, type: YataScalarType, nullable = false): YataField {
  return { name, type, nullable };
}

export const vertexLogSchema: YataTableSchema = {
  tableName: VERTEX_LOG_TABLE,
  primaryKey: ["partitionId", "seq"],
  sortKey: ["partitionId", "label", "pkValue", "seq"],
  compactionKey: ["partitionId", "label", "pkKey", "pkValue"],
  fields: [
    field("partitionId", "uint32"),
    field("seq", "uint64"),
    field("txId", "utf8"),
    field("op", "utf8"),
    field("label", "utf8"),
    field("pkKey", "utf8"),
    field("pkValue", "utf8"),
    field("vid", "utf8"),
    field("repo", "utf8", true),
    field("rkey", "utf8", true),
    field("ownerDid", "utf8", true),
    field("createdAtMs", "uint64"),
    field("updatedAtMs", "uint64"),
    field("tombstone", "bool"),
    field("propsJson", "utf8", true),
    field("propsHash", "utf8", true),
  ],
};

export const edgeLogSchema: YataTableSchema = {
  tableName: EDGE_LOG_TABLE,
  primaryKey: ["partitionId", "seq"],
  sortKey: ["partitionId", "edgeLabel", "srcVid", "dstVid", "seq"],
  compactionKey: ["partitionId", "edgeLabel", "pkKey", "pkValue"],
  fields: [
    field("partitionId", "uint32"),
    field("seq", "uint64"),
    field("txId", "utf8"),
    field("op", "utf8"),
    field("edgeLabel", "utf8"),
    field("pkKey", "utf8"),
    field("pkValue", "utf8"),
    field("eid", "utf8"),
    field("srcVid", "utf8"),
    field("dstVid", "utf8"),
    field("srcLabel", "utf8", true),
    field("dstLabel", "utf8", true),
    field("createdAtMs", "uint64"),
    field("updatedAtMs", "uint64"),
    field("tombstone", "bool"),
    field("propsJson", "utf8", true),
    field("propsHash", "utf8", true),
  ],
};

export const vertexLiveSchema: YataTableSchema = {
  tableName: VERTEX_LIVE_TABLE,
  primaryKey: ["partitionId", "label", "pkKey", "pkValue"],
  sortKey: ["partitionId", "label", "pkValue", "latestSeq"],
  compactionKey: ["partitionId", "label", "pkKey", "pkValue"],
  fields: [
    field("partitionId", "uint32"),
    field("label", "utf8"),
    field("pkKey", "utf8"),
    field("pkValue", "utf8"),
    field("vid", "utf8"),
    field("alive", "bool"),
    field("latestSeq", "uint64"),
    field("repo", "utf8", true),
    field("rkey", "utf8", true),
    field("ownerDid", "utf8", true),
    field("updatedAtMs", "uint64"),
    field("propsJson", "utf8", true),
  ],
};

export const edgeLiveOutSchema: YataTableSchema = {
  tableName: EDGE_LIVE_OUT_TABLE,
  primaryKey: ["partitionId", "edgeLabel", "pkKey", "pkValue"],
  sortKey: ["partitionId", "edgeLabel", "srcVid", "dstVid", "latestSeq"],
  compactionKey: ["partitionId", "edgeLabel", "pkKey", "pkValue"],
  fields: [
    field("partitionId", "uint32"),
    field("edgeLabel", "utf8"),
    field("pkKey", "utf8"),
    field("pkValue", "utf8"),
    field("eid", "utf8"),
    field("srcVid", "utf8"),
    field("dstVid", "utf8"),
    field("srcLabel", "utf8", true),
    field("dstLabel", "utf8", true),
    field("alive", "bool"),
    field("latestSeq", "uint64"),
    field("updatedAtMs", "uint64"),
    field("propsJson", "utf8", true),
  ],
};

export const edgeLiveInSchema: YataTableSchema = {
  tableName: EDGE_LIVE_IN_TABLE,
  primaryKey: ["partitionId", "edgeLabel", "pkKey", "pkValue"],
  sortKey: ["partitionId", "edgeLabel", "dstVid", "srcVid", "latestSeq"],
  compactionKey: ["partitionId", "edgeLabel", "pkKey", "pkValue"],
  fields: [
    field("partitionId", "uint32"),
    field("edgeLabel", "utf8"),
    field("pkKey", "utf8"),
    field("pkValue", "utf8"),
    field("eid", "utf8"),
    field("dstVid", "utf8"),
    field("srcVid", "utf8"),
    field("srcLabel", "utf8", true),
    field("dstLabel", "utf8", true),
    field("alive", "bool"),
    field("latestSeq", "uint64"),
    field("updatedAtMs", "uint64"),
    field("propsJson", "utf8", true),
  ],
};

export const yataGraphSchemas = {
  [VERTEX_LOG_TABLE]: vertexLogSchema,
  [EDGE_LOG_TABLE]: edgeLogSchema,
  [VERTEX_LIVE_TABLE]: vertexLiveSchema,
  [EDGE_LIVE_OUT_TABLE]: edgeLiveOutSchema,
  [EDGE_LIVE_IN_TABLE]: edgeLiveInSchema,
} as const;
