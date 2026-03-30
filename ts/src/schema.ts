/**
 * Shared yata Lance graph table contract for TypeScript runtimes.
 *
 * This mirrors the Rust Arrow schema in `yata-lance/src/schema.rs`.
 */

export const GRAPH_FORMAT = "yata-lance-graph-v1" as const;

export const VERTEX_LOG_TABLE = "yata_vertex_log" as const;
export const EDGE_LOG_TABLE = "yata_edge_log" as const;
export const VERTEX_LIVE_TABLE = "yata_vertex_live" as const;
export const EDGE_LIVE_OUT_TABLE = "yata_edge_live_out" as const;
export const EDGE_LIVE_IN_TABLE = "yata_edge_live_in" as const;

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
  primaryKey: ["partition_id", "seq"],
  sortKey: ["partition_id", "label", "pk_value", "seq"],
  compactionKey: ["partition_id", "label", "pk_key", "pk_value"],
  fields: [
    field("partition_id", "uint32"),
    field("seq", "uint64"),
    field("tx_id", "utf8"),
    field("op", "utf8"),
    field("label", "utf8"),
    field("pk_key", "utf8"),
    field("pk_value", "utf8"),
    field("vid", "utf8"),
    field("repo", "utf8", true),
    field("rkey", "utf8", true),
    field("owner_did", "utf8", true),
    field("created_at_ms", "uint64"),
    field("updated_at_ms", "uint64"),
    field("tombstone", "bool"),
    field("props_json", "utf8", true),
    field("props_hash", "utf8", true),
  ],
};

export const edgeLogSchema: YataTableSchema = {
  tableName: EDGE_LOG_TABLE,
  primaryKey: ["partition_id", "seq"],
  sortKey: ["partition_id", "edge_label", "src_vid", "dst_vid", "seq"],
  compactionKey: ["partition_id", "edge_label", "pk_key", "pk_value"],
  fields: [
    field("partition_id", "uint32"),
    field("seq", "uint64"),
    field("tx_id", "utf8"),
    field("op", "utf8"),
    field("edge_label", "utf8"),
    field("pk_key", "utf8"),
    field("pk_value", "utf8"),
    field("eid", "utf8"),
    field("src_vid", "utf8"),
    field("dst_vid", "utf8"),
    field("src_label", "utf8", true),
    field("dst_label", "utf8", true),
    field("created_at_ms", "uint64"),
    field("updated_at_ms", "uint64"),
    field("tombstone", "bool"),
    field("props_json", "utf8", true),
    field("props_hash", "utf8", true),
  ],
};

export const vertexLiveSchema: YataTableSchema = {
  tableName: VERTEX_LIVE_TABLE,
  primaryKey: ["partition_id", "label", "pk_key", "pk_value"],
  sortKey: ["partition_id", "label", "pk_value", "latest_seq"],
  compactionKey: ["partition_id", "label", "pk_key", "pk_value"],
  fields: [
    field("partition_id", "uint32"),
    field("label", "utf8"),
    field("pk_key", "utf8"),
    field("pk_value", "utf8"),
    field("vid", "utf8"),
    field("alive", "bool"),
    field("latest_seq", "uint64"),
    field("repo", "utf8", true),
    field("rkey", "utf8", true),
    field("owner_did", "utf8", true),
    field("updated_at_ms", "uint64"),
    field("props_json", "utf8", true),
  ],
};

export const edgeLiveOutSchema: YataTableSchema = {
  tableName: EDGE_LIVE_OUT_TABLE,
  primaryKey: ["partition_id", "edge_label", "pk_key", "pk_value"],
  sortKey: ["partition_id", "edge_label", "src_vid", "dst_vid", "latest_seq"],
  compactionKey: ["partition_id", "edge_label", "pk_key", "pk_value"],
  fields: [
    field("partition_id", "uint32"),
    field("edge_label", "utf8"),
    field("pk_key", "utf8"),
    field("pk_value", "utf8"),
    field("eid", "utf8"),
    field("src_vid", "utf8"),
    field("dst_vid", "utf8"),
    field("src_label", "utf8", true),
    field("dst_label", "utf8", true),
    field("alive", "bool"),
    field("latest_seq", "uint64"),
    field("updated_at_ms", "uint64"),
    field("props_json", "utf8", true),
  ],
};

export const edgeLiveInSchema: YataTableSchema = {
  tableName: EDGE_LIVE_IN_TABLE,
  primaryKey: ["partition_id", "edge_label", "pk_key", "pk_value"],
  sortKey: ["partition_id", "edge_label", "dst_vid", "src_vid", "latest_seq"],
  compactionKey: ["partition_id", "edge_label", "pk_key", "pk_value"],
  fields: [
    field("partition_id", "uint32"),
    field("edge_label", "utf8"),
    field("pk_key", "utf8"),
    field("pk_value", "utf8"),
    field("eid", "utf8"),
    field("dst_vid", "utf8"),
    field("src_vid", "utf8"),
    field("src_label", "utf8", true),
    field("dst_label", "utf8", true),
    field("alive", "bool"),
    field("latest_seq", "uint64"),
    field("updated_at_ms", "uint64"),
    field("props_json", "utf8", true),
  ],
};

export const yataGraphSchemas = {
  [VERTEX_LOG_TABLE]: vertexLogSchema,
  [EDGE_LOG_TABLE]: edgeLogSchema,
  [VERTEX_LIVE_TABLE]: vertexLiveSchema,
  [EDGE_LIVE_OUT_TABLE]: edgeLiveOutSchema,
  [EDGE_LIVE_IN_TABLE]: edgeLiveInSchema,
} as const;
