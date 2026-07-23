/**
 * Closing an in-flight snapshot (step a of the new-snapshot workflow).
 *
 * Every character still awaiting computation (pending/retryable) is marked
 * `skipped` — a terminal "deliberately not collected" outcome, distinct from
 * `dead` (unfetchable). That leaves no pending lines in the state file, so the
 * ordinary finalize path then publishes the snapshot with whatever was collected
 * (or cleanly aborts it when nothing was). A snapshot still in `ladder_capture`
 * never finished seeding and holds nothing worth publishing — it is discarded.
 *
 * A final-transform failure during the close propagates to the caller: the
 * create workflow fails loudly (alert issue) with the phase left
 * `transforming`, and the next create fire resumes the close.
 */
import type { SnapshotCharacter, SnapshotManifest } from '@classolek/shared';
import type { Clock } from '../rate-limit/clock.js';
import type { CheckpointStore } from '../checkpoint/store.js';
import type { ObjectStore } from '../checkpoint/object-store.js';
import { readState, writeState } from '../snapshot-state/state-store.js';
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
    // v4: the state file is the authoritative queue (finalize merges the worker
    // result files into it). Rewrite it in one streamed pass, marking every
    // still-pending/retryable line `skipped` and counting them as we go — never
    // JSON.parse the whole file (see state-store.ts). `readState` fetches the
    // compressed body whole before any line is pulled and `writeState` puts only
    // after the source fully drains, so this same-key read-modify-write never
    // races itself.
    const counter = { skipped: 0 };
    await writeState(
      deps.objectStore,
      league,
      snapshotId,
      markRemainingSkipped(readState(deps.objectStore, league, snapshotId), counter),
    );
    skippedMarked = counter.skipped;
    deps.log?.(
      `close: marked ${skippedMarked} uncollected character(s) skipped in ${snapshotId} (${league})`,
    );
  }

  // With nothing left pending, the ordinary finalize pass takes it the rest of
  // the way: merge (nothing new) → recompute tally → final transform →
  // published, or a clean abort when zero characters were collected (also the
  // over-age hard block, unchanged).
  const finalize = await deps.finalizerFor(league).runOnce();
  return {
    league,
    snapshotId,
    result: finalize.phase === 'published' ? 'published' : 'aborted',
    skippedMarked,
  };
}

/**
 * Streamed rewrite tagging every still-uncollected line (`pending`/`retryable`)
 * as the terminal `skipped` outcome, leaving already-resolved lines (and their
 * raw payloads) untouched. One line is materialized at a time, so a
 * hundreds-of-MB state file closes within the heap. `counter.skipped` accrues
 * how many lines were flipped — the honest skip count for the close summary.
 */
async function* markRemainingSkipped(
  state: AsyncIterable<SnapshotCharacter>,
  counter: { skipped: number },
): AsyncGenerator<SnapshotCharacter> {
  for await (const line of state) {
    if (line.outcome === 'pending' || line.outcome === 'retryable') {
      counter.skipped += 1;
      yield { ...line, outcome: 'skipped' };
    } else {
      yield line;
    }
  }
}
