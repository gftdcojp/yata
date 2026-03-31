/**
 * @gftd/yata — yata graph engine TS client.
 *
 * Single Source of Truth for all yata TS types, Cypher builder, and helpers.
 * PDS, dispatcher, and apps import from this package.
 *
 * @example
 *   import { G, CypherResult, collectionToLabel } from "@gftd/yata";
 *   const { cypher, params } = new G("Post", "p").where("repo", did).limit(50).build();
 */

// Types
export type {
  CypherResult,
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
  createGraphManifest,
  createFetchManifestStore,
  createLatestPointer,
  createManifestKeyLayout,
  createR2ManifestStore,
  loadManifest,
  loadManifestViaLatest,
  loadLatestPointer,
  makeManifestTableRef,
  parseLatestPointer,
  parseManifest,
  publishManifest,
  saveManifest,
  stringifyLatestPointer,
  stringifyManifest,
} from "./manifest.js";
export type {
  GraphManifest,
  GraphManifestKeyLayout,
  GraphManifestLatestPointer,
  GraphManifestSeqRange,
  GraphManifestTableRef,
  GraphManifestTables,
  ManifestStore,
  R2BucketLike,
} from "./manifest.js";
export { GraphCatalog } from "./catalog.js";
export {
  applyEdgeMutation,
  applyVertexMutation,
  projectEdgeMutation,
  projectVertexMutation,
} from "./writer.js";
export type {
  EdgeMutation,
  LanceWritableTableLike,
  VertexMutation,
  YataWritableTables,
} from "./writer.js";
export {
  createEdgePipelineEnvelope,
  createVertexPipelineEnvelope,
  decodeGraphPipelineEnvelope,
  emitGraphPipelineEnvelope,
  hydrateEdgeMutation,
  hydrateVertexMutation,
  parseGraphPipelineEnvelope,
  projectGraphPipelineBatch,
  projectGraphPipelineRecord,
  stringifyGraphPipelineEnvelope,
  toGraphProjectedRecord,
  toGraphProjectedRecords,
} from "./pipeline.js";
export type {
  EdgePipelineMutation,
  GraphEdgePipelineEnvelope,
  GraphPipelineConsumerRecord,
  GraphPipelineEnvelope,
  GraphPipelineEnvelopeBase,
  GraphProjectedRecord,
  GraphVertexPipelineEnvelope,
  PipelineWriterLike,
  VertexPipelineMutation,
} from "./pipeline.js";

// Cypher builder
export { G, buildLabelCypher } from "./cypher.js";
export type { CypherQuery } from "./cypher.js";

// Cypher parser (Workers-side subset)
export { parseCypher, tryParseCypher, CypherParseError } from "./cypher-parse.js";
export type {
  CypherAST,
  CypherValue,
  MatchNode,
  MatchEdge,
  MatchPattern,
  WhereClause,
  ReturnColumn,
  PropRef,
  ParamRef,
  LiteralVal,
  Expr,
} from "./cypher-parse.js";

// Legacy Arrow/Workers modules are intentionally excluded from root exports.
// Import explicitly via subpaths:
//   @gftd/yata/lance-arrow
//   @gftd/yata/cypher-exec
//   @gftd/yata/r2-reader
//   @gftd/yata/workers-read

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

