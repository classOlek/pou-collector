/**
 * Checkpoint store: loads/saves the SnapshotManifest at state/<league>/current.json.
 * A single-writer GH Actions concurrency group protects it, so no locking here
 * (docs/ARCHITECTURE.md §5).
 *
 * `load` validates the schema version and required shape: a checkpoint from an
 * older/foreign shape is treated as *no checkpoint* (the caller starts a fresh
 * snapshot) rather than trusted, which would otherwise produce e.g. an
 * `undefined` shard index that silently overwrites shards.
 */
import type { SnapshotManifest } from '@classolek/shared';
import { STATE_PREFIX, SCHEMA_VERSION, checkpointPath, classifyKey } from '@classolek/shared';
import { listKeys, putJson, type ObjectStore } from './object-store.js';

export class CheckpointStore {
  constructor(private readonly store: ObjectStore) {}

  async load(league: string): Promise<SnapshotManifest | undefined> {
    return this.loadAt(checkpointPath(league), league);
  }

  private async loadAt(key: string, label: string): Promise<SnapshotManifest | undefined> {
    const bytes = await this.store.get(key);
    if (!bytes) return undefined;

    let parsed: unknown;
    try {
      parsed = JSON.parse(new TextDecoder().decode(bytes));
    } catch {
      console.warn(`checkpoint for "${label}" is not valid JSON; starting fresh`);
      return undefined;
    }
    if (!isValidManifest(parsed)) {
      console.warn(`checkpoint for "${label}" failed schema validation; starting fresh`);
      return undefined;
    }
    return parsed;
  }

  /**
   * Every league's current checkpoint (skips the cached tree under state/tree/).
   * Used to find an in-flight snapshot in another league — e.g. one started by a
   * workflow_dispatch override — so scheduled runs can resume it (ARCHITECTURE §5).
   */
  async listAll(): Promise<SnapshotManifest[]> {
    const keys = await listKeys(this.store, STATE_PREFIX);
    const manifests: SnapshotManifest[] = [];
    for (const key of keys) {
      if (classifyKey(key) !== 'checkpoint') continue;
      const manifest = await this.loadAt(key, key);
      if (manifest) manifests.push(manifest);
    }
    return manifests;
  }

  async save(manifest: SnapshotManifest): Promise<void> {
    await putJson(this.store, checkpointPath(manifest.league), manifest);
  }

  async clear(league: string): Promise<void> {
    await this.store.delete(checkpointPath(league));
  }
}

function isValidManifest(value: unknown): value is SnapshotManifest {
  if (typeof value !== 'object' || value === null) return false;
  const m = value as Record<string, unknown>;
  return (
    m['schemaVersion'] === SCHEMA_VERSION &&
    typeof m['snapshotId'] === 'string' &&
    typeof m['league'] === 'string' &&
    typeof m['phase'] === 'string' &&
    typeof m['ladderCapturedAt'] === 'string' &&
    typeof m['chunkSize'] === 'number' &&
    typeof m['chunkCount'] === 'number' &&
    typeof m['totalCharacters'] === 'number' &&
    typeof m['resolvedChunks'] === 'number' &&
    typeof m['outcomes'] === 'object' &&
    m['outcomes'] !== null
  );
}
