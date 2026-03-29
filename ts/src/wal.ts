/**
 * @gftd/yata — WAL (Write-Ahead Log) Projection utilities.
 *
 * WAL Projection: Write Container appends to WAL ring buffer.
 * Read Containers consume WAL entries for incremental CSR merge.
 * R2 segments provide durability for cold start recovery.
 */

import type {
  WalEntry,
  WalTailResult,
  WalFlushResult,
  WalCheckpointResult,
  WalColdStartResult,
  YataRPC,
} from "./types.js";
import { collectionToLabel, buildMergeProps, generateTid } from "./helpers.js";

/**
 * Build a WAL record for Pipeline.send().
 * This is the durable write (source of truth).
 */
export function buildWalRecord(
  repo: string,
  collection: string,
  rkey: string,
  action: "create" | "update" | "delete",
  json?: string
): {
  seq: number;
  rev: string;
  cid: string;
  repo: string;
  ops: string;
  created: number;
  records: string;
} {
  const rev = action === "create" ? rkey : generateTid();
  return {
    seq: 0,
    rev,
    cid: rev,
    repo,
    ops: JSON.stringify([{ action, collection, rkey }]),
    created: Date.now(),
    records:
      action === "delete"
        ? JSON.stringify({})
        : JSON.stringify({ [`${collection}/${rkey}`]: json }),
  };
}

/**
 * Pipeline WAL write + yata projection (Single Source for all write paths).
 *
 * 1. Pipeline.send() — durable WAL (source of truth)
 * 2. YATA_RPC.mergeRecord() — instant projection (fire-and-forget on failure)
 *
 * mergeRecord failure is recoverable via wal-replay.
 */
export async function sendWalAndProject(
  walStream: { send(records: unknown[]): Promise<void> },
  yataRpc: YataRPC,
  repo: string,
  collection: string,
  rkey: string,
  action: "create" | "update" | "delete",
  json?: string,
  appId = "",
  extraMergeProps?: Record<string, string>
): Promise<{ rev: string; mergeError?: string }> {
  const walRecord = buildWalRecord(repo, collection, rkey, action, json);
  const label = collectionToLabel(collection);
  let mergeError: string | undefined;

  const doMerge = async () => {
    if (action === "delete") {
      await yataRpc.deleteRecord(label, "rkey", rkey, appId);
    } else {
      await yataRpc.mergeRecord(
        label,
        "rkey",
        rkey,
        { ...buildMergeProps(collection, json!, repo), ...extraMergeProps },
        appId
      );
    }
  };

  const doMergeWithRetry = async () => {
    try {
      await Promise.race([
        doMerge(),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("mergeRecord timeout (30s)")), 30000)
        ),
      ]);
    } catch (e1) {
      mergeError = e1 instanceof Error ? e1.message : String(e1);
      if (!mergeError.includes("timeout")) {
        try {
          await new Promise((r) => setTimeout(r, 500));
          await Promise.race([
            doMerge(),
            new Promise((_, reject) =>
              setTimeout(
                () => reject(new Error("mergeRecord retry timeout (15s)")),
                15000
              )
            ),
          ]);
          mergeError = undefined;
        } catch (e2) {
          mergeError = e2 instanceof Error ? e2.message : String(e2);
        }
      }
      if (mergeError) {
        console.error(
          `[sendWalAndProject] mergeRecord FAILED (${label}/${rkey}): ${mergeError} — WAL is durable, wal-replay will recover`
        );
      }
    }
  };

  // Pipeline (durable) + mergeRecord (projection) are independent — run in parallel.
  // Pipeline.send resolve = durable. mergeRecord failure is recoverable via wal-replay.
  await Promise.all([walStream.send([walRecord]), doMergeWithRetry()]);

  return { rev: walRecord.rev, mergeError };
}

/**
 * Fire-and-forget variant — WAL send + merge projection, errors logged but not propagated.
 */
export function sendWalAndProjectFireAndForget(
  walStream: { send(records: unknown[]): Promise<void> },
  yataRpc: YataRPC,
  repo: string,
  collection: string,
  rkey: string,
  action: "create" | "update" | "delete",
  json?: string,
  appId = "",
  extraMergeProps?: Record<string, string>,
  mergePk?: string
): void {
  const walRecord = buildWalRecord(repo, collection, rkey, action, json);
  walStream
    .send([walRecord])
    .catch((e: unknown) =>
      console.warn(`[sendWalAndProject] WAL send failed (${collection}):`, e)
    );

  if (action !== "delete" && json) {
    const label = collectionToLabel(collection);
    const pk = mergePk || "rkey";
    const pkValue = mergePk ? (extraMergeProps?.[mergePk] ?? rkey) : rkey;
    yataRpc
      .mergeRecord(
        label,
        pk,
        pkValue,
        { ...buildMergeProps(collection, json, repo), ...extraMergeProps },
        appId
      )
      .catch((e: unknown) =>
        console.error(
          `[sendWalAndProject] mergeRecord ${label} projection failed:`,
          e
        )
      );
  }
}

// Re-export WAL types for convenience
export type {
  WalEntry,
  WalTailResult,
  WalFlushResult,
  WalCheckpointResult,
  WalColdStartResult,
};
