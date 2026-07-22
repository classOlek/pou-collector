/**
 * Abort cleanup for an in-flight snapshot. With incremental publishing, an
 * aborted snapshot may already have (incomplete, mutable) published files and
 * an index entry — all of it is discarded together with the raw shards and
 * chunk files, so the frontend never lists a snapshot that will never finish.
 * Completed snapshots are immutable and are NEVER passed through here.
 */
import { chunkPrefix, rawShardPrefix, snapshotPrefix } from '@classolek/shared';
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
    rawShardPrefix(league, snapshotId),
    chunkPrefix(league, snapshotId),
    snapshotPrefix(league, snapshotId),
  ]) {
    for (const key of await listKeys(store, prefix)) await store.delete(key);
  }
  const index = await readIndex(store);
  if (removeSnapshot(index, league, snapshotId)) {
    await writeIndex(store, index, clock);
  }
}
