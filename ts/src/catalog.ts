import type { LanceConnectionLike, YataOpenedTables } from "./lance.js";
import { openYataGraphTablesFromManifest } from "./lance.js";
import type { GraphManifest, ManifestStore } from "./manifest.js";
import {
  createGraphManifest,
  loadManifestViaLatest,
  publishManifest,
} from "./manifest.js";

export class GraphCatalog {
  static latestKey(partitionId: number): string {
    return `manifests/partitions/${partitionId}/latest.json`;
  }

  static versionedKey(partitionId: number, version: number): string {
    return `manifests/partitions/${partitionId}/manifest-${String(version).padStart(20, "0")}.json`;
  }

  static createManifest(
    partitionId: number,
    version: number,
    seq: { min: number; max: number },
    baseUri: string,
  ): GraphManifest {
    return createGraphManifest(partitionId, version, seq, baseUri);
  }

  static async publish(
    store: ManifestStore,
    manifest: GraphManifest,
  ): Promise<void> {
    await publishManifest(store, manifest);
  }

  static async loadLatest(
    store: ManifestStore,
    partitionId: number,
  ): Promise<GraphManifest | null> {
    return loadManifestViaLatest(store, this.latestKey(partitionId));
  }

  static async openLatest<TTable>(
    connection: LanceConnectionLike<unknown, TTable>,
    store: ManifestStore,
    partitionId: number,
  ): Promise<YataOpenedTables<TTable> | null> {
    const manifest = await this.loadLatest(store, partitionId);
    return manifest ? openYataGraphTablesFromManifest(connection, manifest) : null;
  }
}
