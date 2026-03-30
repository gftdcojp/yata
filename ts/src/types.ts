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

// ── WAL Entry (Kafka-style projection) ──

export interface WalEntry {
  seq: number;
  op: "upsert" | "delete";
  label: string;
  pk_key: string;
  pk_value: string;
  props: Record<string, unknown>;
  timestamp_ms: number;
}

export interface WalTailResult {
  entries: WalEntry[];
  head_seq: number;
  count: number;
}

export interface WalFlushResult {
  seq_start: number;
  seq_end: number;
  bytes: number;
}

export interface WalCheckpointResult {
  vertices: number;
  edges: number;
}

export interface WalColdStartResult {
  checkpoint_seq: number;
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

  // WAL Projection
  walTail(pid: number, afterSeq: number, limit?: number): Promise<WalTailResult>;
  walFlushSegments(): Promise<{ flushed: number }>;
  walCheckpoint(): Promise<WalCheckpointResult>;
  walColdStartReplicas(): Promise<{ started: number }>;

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
