/**
 * Abort cleanup for an in-flight snapshot. With incremental publishing, an
 * aborted snapshot may already have (incomplete, mutable) published files and
 * an index entry — all of it is discarded together with the v4 state file, the
 * transient per-worker result files, and the legacy raw shards / chunk files, so
 * the frontend never lists a snapshot that will never finish. Completed
 * snapshots are immutable and are NEVER passed through here.
 */
import {
  chunkPrefix,
  rawShardPrefix,
  snapshotPrefix,
  snapshotStatePath,
  workerResultPrefix,
} from '@classolek/shared';
import type { Clock } from '../rate-limit/clock.js';
import { listKeys, type ObjectStore } from '../checkpoint/object-store.js';
import { readIndex, removeSnapshot, writeIndex } from '../index-file.js';

export async function discardSnapshotArtifacts(
  store: ObjectStore,
  clock: Clock,
  league: string,
  snapshotId: string,
): Promise<void> {
  for (const prefix of [
    // v4 private state: the transient per-worker result files…
    workerResultPrefix(league, snapshotId),
    // …and (until the sweep in Phase 6) the legacy raw shards + chunk files.
    rawShardPrefix(league, snapshotId),
    chunkPrefix(league, snapshotId),
    // Any incomplete published files (aborts can happen mid-incremental-publish).
    snapshotPrefix(league, snapshotId),
  ]) {
    for (const key of await listKeys(store, prefix)) await store.delete(key);
  }
  // The single NDJSON.gz state-file object (the v4 raw) — deleted by exact key,
  // not a prefix. Deleting an absent key is a no-op.
  await store.delete(snapshotStatePath(league, snapshotId));

  const index = await readIndex(store);
  if (removeSnapshot(index, league, snapshotId)) {
    await writeIndex(store, index, clock);
  }
}
