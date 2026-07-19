/**
 * Snapshot chunk store (redesign step 2). A snapshot's queue lives as chunk
 * files at state/<league>/chunks/<snapshotId>/<nnnnn>.json — the unit of work
 * distribution across parallel workers.
 *
 * Ownership discipline (no locking anywhere):
 *  - the coordinator writes all chunk files ONCE at seed time;
 *  - during a workflow run, each pending chunk is owned by exactly one worker
 *    (workers partition the pending list disjointly — see assignedChunkIndices);
 *  - finalize only reads chunks (and deletes them when the snapshot leaves the
 *    in-flight phases).
 * Workflow-level concurrency serializes runs, so these phases never overlap.
 */
import type { QueuedCharacter, RosterCharacter, SnapshotChunk } from '@pou/shared';
import { SCHEMA_VERSION, chunkPath, chunkPrefix, isChunkResolved } from '@pou/shared';
import { getJson, listKeys, putJson, type ObjectStore } from '../checkpoint/object-store.js';

/**
 * Split the roster into pending chunks for a new snapshot (pure). Every known
 * character enters the snapshot as `pending` ("not computed"); roster order is
 * preserved (rank ascending), so chunk 0 is the top of the ladder.
 */
export function planChunks(
  league: string,
  snapshotId: string,
  characters: readonly RosterCharacter[],
  chunkSize: number,
): SnapshotChunk[] {
  const chunks: SnapshotChunk[] = [];
  for (let start = 0; start < characters.length; start += chunkSize) {
    const queued: QueuedCharacter[] = characters.slice(start, start + chunkSize).map((c) => ({
      rank: c.rank,
      account: c.account,
      character: c.character,
      class: c.class,
      level: c.level,
      outcome: 'pending',
      attempts: 0,
    }));
    chunks.push({
      schemaVersion: SCHEMA_VERSION,
      league,
      snapshotId,
      chunkIndex: chunks.length,
      characters: queued,
      shardsWritten: 0,
    });
  }
  return chunks;
}

/**
 * The pending chunks a worker slot owns this run: pending chunk indices where
 * `chunkIndex % workerCount === workerIndex`. Keyed on the chunk index itself
 * (not the position in the pending list) so ownership is STABLE — it never
 * depends on when a worker read the chunk states — and disjoint across slots
 * by construction: two workers can never own the same chunk, and every pending
 * chunk is owned by exactly one worker.
 */
export function assignedChunkIndices(
  pendingIndices: readonly number[],
  workerIndex: number,
  workerCount: number,
): number[] {
  return pendingIndices.filter((chunkIndex) => chunkIndex % workerCount === workerIndex);
}

/** Indices of chunks that still contain not-yet-computed characters. */
export function pendingChunkIndices(chunks: readonly SnapshotChunk[]): number[] {
  return chunks.filter((c) => !isChunkResolved(c)).map((c) => c.chunkIndex);
}

export class ChunkStore {
  constructor(private readonly store: ObjectStore) {}

  /** Load one chunk; a missing/corrupt chunk is a hard error (seeded once, never absent). */
  async load(league: string, snapshotId: string, chunkIndex: number): Promise<SnapshotChunk> {
    const key = chunkPath(league, snapshotId, chunkIndex);
    const chunk = await getJson<SnapshotChunk>(this.store, key);
    if (!chunk || chunk.schemaVersion !== SCHEMA_VERSION) {
      throw new Error(`chunk ${key} is missing or has a foreign schema`);
    }
    return chunk;
  }

  /** Load every chunk of a snapshot, ordered by chunk index. */
  async loadAll(league: string, snapshotId: string, chunkCount: number): Promise<SnapshotChunk[]> {
    const chunks: SnapshotChunk[] = [];
    for (let i = 0; i < chunkCount; i += 1) chunks.push(await this.load(league, snapshotId, i));
    return chunks;
  }

  async save(chunk: SnapshotChunk): Promise<void> {
    await putJson(this.store, chunkPath(chunk.league, chunk.snapshotId, chunk.chunkIndex), chunk);
  }

  /** Delete all chunk files of a snapshot (abort / post-publish cleanup). */
  async deleteAll(league: string, snapshotId: string): Promise<number> {
    const keys = await listKeys(this.store, chunkPrefix(league, snapshotId));
    for (const key of keys) await this.store.delete(key);
    return keys.length;
  }
}
