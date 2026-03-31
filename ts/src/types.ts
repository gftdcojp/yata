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
  org_id?: string;
}

// ── AppId (branded type for partition routing) ──

export type AppId = string & { readonly __brand: "AppId" };

/** Cross-partition query (no partition affinity). */
export const CROSS_PARTITION = "" as AppId;

// ── YataRPC interface (Workers RPC service binding) ──

export interface YataRPC {
  // Core
  cypher(statement: string, appId?: string, parameters?: Record<string, unknown>): Promise<CypherResult>;
  query(statement: string, appId?: string, parameters?: Record<string, unknown>): Promise<CypherResult>;
  mutate(statement: string, appId?: string, parameters?: Record<string, unknown>): Promise<CypherResult>;
  mergeRecord(label: string, pkKey: string, pkValue: string, props: Record<string, string>, appId?: string): Promise<{ vid: number }>;
  deleteRecord(label: string, pkKey: string, pkValue: string, appId?: string): Promise<{ deleted: boolean }>;
  cypherBatch(statements: string[], appId?: string): Promise<CypherResult[]>;

  // Lifecycle
  health(): Promise<string>;
  ping(): Promise<string>;
  warmup(): Promise<string>;
}

// ── Merge Props ──

export interface MergeProps {
  collection: string;
  value_b64: string;
  repo: string;
  sensitivity_ord: string;
  owner_hash: string;
  [key: string]: string;
}
