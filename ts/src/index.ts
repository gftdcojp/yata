/**
 * @gftd/yata — yata graph engine TS client.
 *
 * Single Source of Truth for all yata TS types, Cypher builder, WAL, and helpers.
 * PDS, dispatcher, and apps import from this package.
 *
 * @example
 *   import { G, CypherResult, collectionToLabel, sendWalAndProject } from "@gftd/yata";
 *   const { cypher, params } = new G("Post", "p").where("repo", did).limit(50).build();
 */

// Types
export type {
  CypherResult,
  WalEntry,
  WalTailResult,
  WalFlushResult,
  WalCheckpointResult,
  WalColdStartResult,
  AppId,
  YataRPC,
  MergeProps,
} from "./types.js";

export { CROSS_PARTITION } from "./types.js";
export {
  GRAPH_FORMAT,
  VERTEX_LOG_TABLE,
  EDGE_LOG_TABLE,
  VERTEX_LIVE_TABLE,
  EDGE_LIVE_OUT_TABLE,
  EDGE_LIVE_IN_TABLE,
  vertexLogSchema,
  edgeLogSchema,
  vertexLiveSchema,
  edgeLiveOutSchema,
  edgeLiveInSchema,
  yataGraphSchemas,
} from "./schema.js";
export type { YataScalarType, YataField, YataTableSchema } from "./schema.js";
export {
  toArrowField,
  toArrowSchema,
  yataGraphArrowSchemas,
  createEmptyYataGraphTables,
  openYataGraphTables,
  openYataGraphTablesFromManifest,
} from "./lance.js";
export type {
  ArrowSchemaFactory,
  LanceConnectionLike,
  LanceCreateTableOptionsLike,
  YataCreatedTables,
  YataGraphTableName,
  YataOpenedTables,
} from "./lance.js";
export {
  arrowJsFactory,
  createArrowSchema,
  createEmptyYataGraphTablesWithArrow,
} from "./lance-arrow.js";
export {
  createGraphManifest,
  createFetchManifestStore,
  loadManifest,
  makeManifestTableRef,
  parseManifest,
  saveManifest,
  stringifyManifest,
} from "./manifest.js";
export type {
  GraphManifest,
  GraphManifestSeqRange,
  GraphManifestTableRef,
  GraphManifestTables,
  ManifestStore,
} from "./manifest.js";

// Cypher builder
export { G, buildLabelCypher } from "./cypher.js";
export type { CypherQuery } from "./cypher.js";

// Helpers
export {
  generateTid,
  esc,
  multiDidFilter,
  collectionToLabel,
  expandCollection,
  toBase64,
  tryDecodeRecord,
  cl,
  fnv1a32,
  buildMergeProps,
  mapCypherRows,
} from "./helpers.js";

// WAL Projection
export {
  buildWalRecord,
  sendWalAndProject,
  sendWalAndProjectFireAndForget,
} from "./wal.js";
