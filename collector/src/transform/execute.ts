/**
 * Transform-step driver: wraps runTransform with the manifest bookkeeping that
 * keeps a deterministically-failing transform from wedging a league forever
 * (docs/ARCHITECTURE.md §5/§7).
 *
 *  - A drained snapshot with zero collected characters is unpublishable → abort
 *    (belt-and-suspenders; the orchestrator already aborts this at drain time).
 *  - Each failed transform advances `transformAttempts`; after the configured
 *    ceiling the snapshot aborts (raw deleted, cooldown applies) instead of the
 *    cron retrying the same failure every 30 min.
 *  - Below the ceiling the failure is surfaced (the workflow alerts) with the
 *    phase left `transforming` so the next run retries.
 */
import type { SnapshotManifest } from '@pou/shared';
import { rawShardPrefix } from '@pou/shared';
import type { Clock } from '../rate-limit/clock.js';
import type { CheckpointStore } from '../checkpoint/store.js';
import { listKeys, type ObjectStore } from '../checkpoint/object-store.js';
import { runTransform, type TransformDeps, type TransformSummary } from './transform.js';

export interface TransformStepConfig {
  treeVersion: string;
  /** Failed transform attempts before the snapshot aborts. */
  maxTransformAttempts: number;
}

export type TransformOutcome =
  | { kind: 'published'; summary: TransformSummary }
  | { kind: 'aborted'; reason: 'no_characters' | 'max_transform_attempts' };

async function abortSnapshot(
  manifest: SnapshotManifest,
  deps: { clock: Clock; objectStore: ObjectStore; checkpointStore: CheckpointStore },
  attempts: number,
): Promise<void> {
  await deps.checkpointStore.save({
    ...manifest,
    phase: 'aborted',
    abortedAt: new Date(deps.clock.now()).toISOString(),
    transformAttempts: attempts,
  });
  // Unpublishable raw is garbage — delete it (the abort cooldown then gates retry).
  const keys = await listKeys(
    deps.objectStore,
    rawShardPrefix(manifest.league, manifest.snapshotId),
  );
  await Promise.all(keys.map((key) => deps.objectStore.delete(key)));
}

export async function executeTransform(
  manifest: SnapshotManifest,
  config: TransformStepConfig,
  deps: TransformDeps,
): Promise<TransformOutcome> {
  if (manifest.outcomes.ok === 0) {
    await abortSnapshot(manifest, deps, manifest.transformAttempts ?? 0);
    return { kind: 'aborted', reason: 'no_characters' };
  }

  const attempt = (manifest.transformAttempts ?? 0) + 1;
  try {
    const summary = await runTransform(
      manifest,
      { treeVersion: config.treeVersion, complete: true },
      deps,
    );
    return { kind: 'published', summary };
  } catch (err) {
    if (attempt >= config.maxTransformAttempts) {
      await abortSnapshot(manifest, deps, attempt);
      return { kind: 'aborted', reason: 'max_transform_attempts' };
    }
    // Record the failed attempt (phase stays transforming) and surface the error
    // so the workflow alerts; the next scheduled run retries.
    await deps.checkpointStore.save({ ...manifest, transformAttempts: attempt });
    throw err;
  }
}
