/**
 * @gftd/yata — Workers-side read path with Container fallback.
 *
 * Routes simple Cypher queries to the TS Arrow IPC executor (Workers edge),
 * falls back to Rust Container via YataRPC for complex/unsupported queries.
 *
 * Architecture:
 *   Workers (TS) ──→ R2 Arrow IPC compacted segments (simple read)
 *   Workers (TS) ──→ Container (Rust, YataRPC) (complex read fallback)
 */

import { tryParseCypher, type CypherAST } from "./cypher-parse.js";
import { execCypher, type ExecContext } from "./cypher-exec.js";
import {
  loadLabels,
  type CompactionManifest,
  type FragmentStore,
  type LabelData,
} from "./r2-reader.js";
import type { CypherResult, YataRPC } from "./types.js";

// ── Workers Read Router ──

export interface WorkersReadConfig {
  store: FragmentStore;
  container: YataRPC;
  partitionId?: number;
}

export interface WorkersReadStats {
  workersHit: number;
  containerFallback: number;
}

/**
 * Workers-side read router.
 *
 * - Caches CompactionManifest + loaded label data per partition.
 * - Routes parseable queries to TS executor, others to Container.
 * - Lance-only read: no pending WAL merge path.
 */
export class WorkersReader {
  private store: FragmentStore;
  private container: YataRPC;
  private pid: number;
  private manifest: CompactionManifest | null = null;
  private labelCache: Map<string, LabelData> = new Map();
  private edgeCache: Map<string, LabelData> = new Map();
  readonly stats: WorkersReadStats = { workersHit: 0, containerFallback: 0 };

  constructor(config: WorkersReadConfig) {
    this.store = config.store;
    this.container = config.container;
    this.pid = config.partitionId ?? 0;
  }

  /**
   * Execute a Cypher query. Routes to Workers executor or Container fallback.
   */
  async cypher(
    statement: string,
    appId?: string,
    parameters?: Record<string, unknown>,
  ): Promise<CypherResult> {
    const params = parameters ?? {};
    const ast = tryParseCypher(statement, params);

    if (!ast) {
      this.stats.containerFallback++;
      return this.container.cypher(statement, appId, params);
    }

    try {
      const result = await this.execLocal(ast, params);
      this.stats.workersHit++;
      return result;
    } catch {
      // Fallback to Container on any executor error
      this.stats.containerFallback++;
      return this.container.cypher(statement, appId, params);
    }
  }

  /**
   * Read-only query alias. Same routing as cypher().
   */
  async query(
    statement: string,
    appId?: string,
    parameters?: Record<string, unknown>,
  ): Promise<CypherResult> {
    return this.cypher(statement, appId, parameters);
  }

  /**
   * Write operations always go to Container.
   */
  async mutate(
    statement: string,
    appId?: string,
    parameters?: Record<string, unknown>,
  ): Promise<CypherResult> {
    return this.container.mutate(statement, appId, parameters);
  }

  /**
   * Refresh manifest from R2 (call on cache miss or periodically).
   */
  async refreshManifest(): Promise<CompactionManifest | null> {
    this.manifest = await this.store.getManifest(this.pid);
    return this.manifest;
  }

  /**
   * Invalidate cached label data. Next query will re-fetch from R2.
   */
  invalidateLabels(): void {
    this.labelCache.clear();
    this.edgeCache.clear();
  }

  // ── Internal ──

  private async execLocal(
    ast: CypherAST,
    params: Record<string, unknown>,
  ): Promise<CypherResult> {
    if (!this.manifest) {
      await this.refreshManifest();
    }
    if (!this.manifest) {
      throw new Error("no manifest available");
    }

    // Determine which labels we need
    const neededVertex = extractVertexLabels(ast);
    const neededEdge = extractEdgeLabels(ast);

    // Load missing vertex labels
    const missingVertex = neededVertex.filter(l => !this.labelCache.has(l));
    if (missingVertex.length > 0) {
      const loaded = await loadLabels(this.store, this.manifest, missingVertex);
      for (const label of missingVertex) {
        const compacted = loaded.get(label);
        if (compacted) this.labelCache.set(label, compacted);
      }
    }

    // Load missing edge labels
    const missingEdge = neededEdge.filter(l => !this.edgeCache.has(l));
    if (missingEdge.length > 0) {
      const loaded = await loadLabels(this.store, this.manifest, missingEdge);
      for (const [k, v] of loaded) this.edgeCache.set(k, v);
    }

    const ctx: ExecContext = {
      vertices: this.labelCache,
      edges: this.edgeCache,
      params,
    };

    return execCypher(ast, ctx);
  }
}

// ── Label Extraction ──

function extractVertexLabels(ast: CypherAST): string[] {
  const labels: string[] = [ast.pattern.src.label];
  if (ast.pattern.dst) labels.push(ast.pattern.dst.label);
  return labels;
}

function extractEdgeLabels(ast: CypherAST): string[] {
  if (ast.pattern.edge) return [ast.pattern.edge.label];
  return [];
}

/**
 * Create a WorkersReader with the given configuration.
 * Convenience factory for the common case.
 */
export function createWorkersReader(config: WorkersReadConfig): WorkersReader {
  return new WorkersReader(config);
}
