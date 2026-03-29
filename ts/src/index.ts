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
  RlsScopeHeader,
  RlsScopeData,
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
