/**
 * @gftd/yata — Core types for yata graph engine interaction.
 *
 * Single Source of Truth for all yata TS types.
 * PDS, dispatcher, and apps import from here.
 */

// ── Cypher Result ──

export interface CypherResult {
  columns: string[];
  rows: unknown[][];
  orgId?: string;
}

// ── AppId (branded type for partition routing) ──

export type AppId = string & { readonly __brand: "AppId" };

/** Cross-partition query (no partition affinity). */
export const CROSS_PARTITION = "" as AppId;

// ── YataRPC interface (Workers RPC service binding) ──

export interface YataRPC {
  // Core (LanceDB append-only — all writes via Cypher MERGE/DELETE)
  cypher(statement: string, appId?: string, parameters?: Record<string, unknown>, authJwt?: string): Promise<CypherResult>;
  query(statement: string, appId?: string, parameters?: Record<string, unknown>, authJwt?: string): Promise<CypherResult>;
  mergeRecord(label: string, pkKey: string, pkValue: string, props: Record<string, string>, appId?: string): Promise<{ vid: number }>;
  deleteRecord(label: string, pkKey: string, pkValue: string, appId?: string): Promise<{ deleted: boolean }>;
  cypherBatch(statements: string[], appId?: string): Promise<CypherResult[]>;
  cypherBatchRead(statements: Array<{ statement: string; parameters?: Record<string, unknown> }>, authJwt?: string): Promise<CypherResult[]>;

  // Compaction
  compact(): Promise<{ 'compactedSeq': number; 'labelsCompacted': number }>;

  // Lifecycle
  health(): Promise<string>;
  ping(): Promise<string>;
  warmup(): Promise<string>;
  stats(): Promise<Record<string, unknown>>;
}

// ── Merge Props ──

export interface MergeProps {
  collection: string;
  'valueB64': string;
  repo: string;
  'sensitivityOrd': string;
  'ownerHash': string;
  [key: string]: string;
}
