/**
 * Closing an in-flight snapshot (step a of the create-snapshot workflow).
 *
 * Every character still awaiting computation (pending/retryable) is marked
 * `skipped` — a terminal "deliberately not collected" outcome, distinct from
 * `dead` (unfetchable). That resolves every chunk, so the ordinary finalize
 * path then publishes the snapshot with whatever was collected (or cleanly
 * aborts it when nothing was). A snapshot still in `ladder_capture` never
 * finished seeding and holds nothing worth publishing — it is discarded.
 *
 * A final-transform failure during the close propagates to the caller: the
 * create workflow fails loudly (alert issue) with the phase left
 * `transforming`, and the next create fire resumes the close.
 */
import type { SnapshotManifest } from '@pou/shared';
import type { Clock } from '../rate-limit/clock.js';
import type { CheckpointStore } from '../checkpoint/store.js';
import type { ObjectStore } from '../checkpoint/object-store.js';
import { ChunkStore } from '../chunks/chunk-store.js';
import type { Finalizer } from './finalize.js';
import { discardSnapshotArtifacts } from './discard.js';

export interface CloseSummary {
  league: string;
  snapshotId: string;
  result: 'discarded' | 'published' | 'aborted';
  /** pending/retryable characters this close marked `skipped`. */
  skippedMarked: number;
}

export interface CloseDeps {
  clock: Clock;
  checkpointStore: CheckpointStore;
  objectStore: ObjectStore;
  /** Finalizer for the closed league (carries that league's tree version). */
  finalizerFor: (league: string) => Finalizer;
  log?: ((message: string) => void) | undefined;
}

export async function closeInFlightSnapshot(
  manifest: SnapshotManifest,
  deps: CloseDeps,
): Promise<CloseSummary> {
  const { league, snapshotId } = manifest;

  if (manifest.phase === 'ladder_capture') {
    // The seed never completed durably; there are no outcomes to publish.
    deps.log?.(`close: discarding never-seeded snapshot ${snapshotId} (${league})`);
    await discardSnapshotArtifacts(deps.objectStore, deps.clock, league, snapshotId);
    await deps.checkpointStore.clear(league);
    return { league, snapshotId, result: 'discarded', skippedMarked: 0 };
  }

  let skippedMarked = 0;
  if (manifest.phase === 'collecting') {
    const chunks = new ChunkStore(deps.objectStore);
    const all = await chunks.loadAll(league, snapshotId, manifest.chunkCount);
    for (const chunk of all) {
      let changed = false;
      for (const entry of chunk.characters) {
        if (entry.outcome === 'pending' || entry.outcome === 'retryable') {
          entry.outcome = 'skipped';
          changed = true;
          skippedMarked += 1;
        }
      }
      if (changed) await chunks.save(chunk);
    }
    deps.log?.(
      `close: marked ${skippedMarked} uncollected character(s) skipped in ${snapshotId} (${league})`,
    );
  }

  // With every chunk resolved, the ordinary finalize pass takes it the rest of
  // the way: rollup → final transform → published, or a clean abort when zero
  // characters were collected (also the over-age hard block, unchanged).
  const finalize = await deps.finalizerFor(league).runOnce();
  return {
    league,
    snapshotId,
    result: finalize.phase === 'published' ? 'published' : 'aborted',
    skippedMarked,
  };
}
