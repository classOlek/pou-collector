/**
 * Finalize step: runs after the worker matrix in every workflow fire
 * (docs/ARCHITECTURE.md §5). The single writer of the manifest + state file at
 * this point (v4 — result-file merge, docs/PLAN_SNAPSHOT_STATE_REWORK.md §5):
 *
 *  1. merges this fire's transient per-worker result files into the snapshot's
 *     single NDJSON.gz state file (streamed, idempotent — last write per
 *     identity wins), recomputes the outcome tally by streaming the merged
 *     state into the manifest, and ONLY THEN deletes the result files. A crash
 *     before the delete re-merges idempotently next fire; a crash before the
 *     manifest write re-merges too — merge is the recovery path, no special
 *     cases;
 *  2. aborts an over-age snapshot (discarding the state file, results, any
 *     incomplete published files and its index entry, plus any legacy
 *     chunks/raw a pre-v4 snapshot left behind — see discardSnapshotArtifacts);
 *  3. while characters remain pending: publishes the data collected SO FAR as an
 *     incomplete snapshot (complete: false) — immediately visible in the web
 *     app, republished in place each run. A partial-publish failure only warns:
 *     collection must keep going regardless;
 *  4. when every character is resolved: hands the snapshot to the final
 *     transform (executeTransform: bounded attempts → published, immutable from
 *     then on), which deletes the state file (the raw) + results.
 */
import type {
  OutcomeTally,
  SnapshotCharacter,
  SnapshotManifest,
  SnapshotPhase,
} from '@classolek/shared';
import { emptyTally, pendingOfTally, workerResultPrefix } from '@classolek/shared';
import type { CheckpointStore } from '../checkpoint/store.js';
import { listKeys, type ObjectStore } from '../checkpoint/object-store.js';
import { PaceStateStore } from '../rate-limit/pace-store.js';
import {
  mergeResults,
  readState,
  readWorkerResultFile,
  writeState,
} from '../snapshot-state/state-store.js';
import { executeTransform, type TransformStepConfig } from '../transform/execute.js';
import { runTransform, type TransformDeps, type TransformSummary } from '../transform/transform.js';
import { HOUR_MS } from './config.js';
import { discardSnapshotArtifacts } from './discard.js';

export interface FinalizeConfig extends TransformStepConfig {
  league: string;
  maxAgeHours: number;
  /**
   * How long a shared per-IP pace file (state/<league>/ips/<ip>.json) is kept
   * after its last write before the sweep reaps it. Must comfortably exceed
   * GGG's longest rate-limit window (~2 h) — past that horizon every timestamp
   * in the file has aged out and it models an empty window, so deleting it loses
   * no pacing information. Purely storage hygiene: correctness never depends on
   * the sweep, since a stale file restores to an empty window anyway.
   */
  paceFileTtlHours: number;
}

export interface FinalizeDeps extends TransformDeps {
  checkpointStore: CheckpointStore;
  objectStore: ObjectStore;
  log?: (message: string) => void;
}

export type FinalizeStopReason =
  | 'idle'
  | 'collecting'
  | 'published_partial'
  | 'partial_publish_failed'
  | 'published_final'
  | 'aborted'
  | 'aborted_no_characters';

export interface FinalizeSummary {
  phase: SnapshotPhase | 'none';
  stopReason: FinalizeStopReason;
  outcomes: OutcomeTally;
  transform?: TransformSummary;
}

export class Finalizer {
  private readonly pace: PaceStateStore;

  constructor(
    private readonly config: FinalizeConfig,
    private readonly deps: FinalizeDeps,
  ) {
    this.pace = new PaceStateStore(deps.objectStore);
  }

  private log(message: string): void {
    this.deps.log?.(message);
  }

  async runOnce(): Promise<FinalizeSummary> {
    // Best-effort hygiene every fire, independent of the manifest branch: reap
    // per-IP pace files whose spend has aged out. A failure here must never fail
    // finalize (the published snapshot is the deliverable), so it only warns.
    try {
      await this.sweepStalePaceFiles();
    } catch (err) {
      this.log(`pace sweep skipped: ${err instanceof Error ? err.message : String(err)}`);
    }

    const manifest = await this.deps.checkpointStore.load(this.config.league);
    if (!manifest) {
      return { phase: 'none', stopReason: 'idle', outcomes: emptyTally() };
    }
    if (manifest.phase === 'transforming') {
      // Parked by a previous run (final transform failed or crashed): the age
      // gate keeps a deterministically-failing transform from wedging forever.
      if (this.aged(manifest)) return this.abort(manifest, 'aborted');
      return this.finalTransform(manifest);
    }
    if (manifest.phase !== 'collecting') {
      return {
        phase: manifest.phase,
        stopReason: 'idle',
        outcomes: manifest.outcomes,
      };
    }

    // 1. Merge this fire's worker result files into the state file and roll the
    //    outcome tally up into the manifest. The result files are deleted only
    //    after the merged state + manifest are durable (see mergeStateAndRollup).
    const { rolled, outcomes } = await this.mergeStateAndRollup(manifest);
    this.log(
      `rollup: ok=${outcomes.ok} private=${outcomes.private} dead=${outcomes.dead} ` +
        `retry=${outcomes.retryable} pending=${outcomes.pending} skipped=${outcomes.skipped}`,
    );

    // 2. Over-age abort (hard block): discard everything, cooldown applies.
    if (this.aged(rolled)) return this.abort(rolled, 'aborted');

    // 3. Still collecting → incremental publish of what exists so far.
    if (pendingOfTally(outcomes) > 0) {
      if (outcomes.ok === 0) {
        // Nothing collected yet — nothing to publish this round.
        return this.summarizeCollecting(rolled, 'collecting');
      }
      try {
        const transform = await runTransform(
          rolled,
          { treeVersion: this.config.treeVersion, complete: false },
          this.deps,
        );
        this.log(
          `partial publish: ${transform.characterCount} characters visible ` +
            `(${pendingOfTally(outcomes)} still pending)`,
        );
        return { ...this.summarizeCollecting(rolled, 'published_partial'), transform };
      } catch (err) {
        // Incremental visibility is best-effort: log and keep collecting. The
        // FINAL transform is the one whose failures are bounded and alerted.
        this.log(
          `partial publish failed (collection continues): ` +
            `${err instanceof Error ? err.message : String(err)}`,
        );
        return this.summarizeCollecting(rolled, 'partial_publish_failed');
      }
    }

    // 4. Drained. Zero public profiles → nothing to publish, clean abort.
    if (outcomes.ok === 0) return this.abort(rolled, 'aborted_no_characters');

    const drained: SnapshotManifest = {
      ...rolled,
      phase: 'transforming',
      completedAt: new Date(this.deps.clock.now()).toISOString(),
    };
    await this.deps.checkpointStore.save(drained);
    this.log(`drained: ${outcomes.ok} characters collected — running final transform`);
    return this.finalTransform(drained);
  }

  /** Final transform: bounded attempts; published snapshots are immutable. */
  private async finalTransform(manifest: SnapshotManifest): Promise<FinalizeSummary> {
    const outcome = await executeTransform(
      manifest,
      {
        treeVersion: this.config.treeVersion,
        maxTransformAttempts: this.config.maxTransformAttempts,
      },
      this.deps,
    );
    if (outcome.kind === 'published') {
      // runTransform already deleted the state file + result files (the raw) on
      // the final publish — the published snapshot is the record now.
      return {
        phase: 'published',
        stopReason: 'published_final',
        outcomes: manifest.outcomes,
        transform: outcome.summary,
      };
    }
    // executeTransform aborted the snapshot (no chars / attempts exhausted):
    // finish the discard (state file, results, any incomplete published files +
    // index entry, plus legacy chunks/raw).
    await discardSnapshotArtifacts(
      this.deps.objectStore,
      this.deps.clock,
      manifest.league,
      manifest.snapshotId,
    );
    return {
      phase: 'aborted',
      stopReason: outcome.reason === 'no_characters' ? 'aborted_no_characters' : 'aborted',
      outcomes: manifest.outcomes,
    };
  }

  private async abort(
    manifest: SnapshotManifest,
    stopReason: FinalizeStopReason,
  ): Promise<FinalizeSummary> {
    this.log(
      stopReason === 'aborted_no_characters'
        ? 'abort: snapshot drained with 0 public profiles (nothing to publish)'
        : 'abort: snapshot aged past max age',
    );
    const aborted: SnapshotManifest = {
      ...manifest,
      phase: 'aborted',
      abortedAt: new Date(this.deps.clock.now()).toISOString(),
    };
    await this.deps.checkpointStore.save(aborted);
    await discardSnapshotArtifacts(
      this.deps.objectStore,
      this.deps.clock,
      manifest.league,
      manifest.snapshotId,
    );
    return {
      phase: 'aborted',
      stopReason,
      outcomes: manifest.outcomes,
    };
  }

  /**
   * Merge this fire's transient per-worker result files into the state file,
   * then recompute the outcome tally by streaming the merged state into the
   * manifest. Order (crash-safe, design decision 3):
   *
   *   list results → mergeResults into the state file → recompute tally →
   *   write manifest → ONLY THEN delete the result files.
   *
   * `mergeResults` is idempotent (last write per identity wins, matched lines
   * replaced by the same record every pass), so a crash anywhere before the
   * delete simply re-merges the still-present results next fire — merge is the
   * recovery path, there is no special case. A result file that fails to decode
   * is skipped (it is transient and re-derivable — its characters stay
   * pending/retryable in the state file until merged) but still deleted with the
   * wave, so one bad transient object never wedges the snapshot.
   *
   * The same-key streamed read-modify-write is safe: `readState` fetches the
   * whole compressed body up front, and `writeState` puts only after the merge
   * generator fully drains (see the writeState doc-comment).
   */
  private async mergeStateAndRollup(
    manifest: SnapshotManifest,
  ): Promise<{ rolled: SnapshotManifest; outcomes: OutcomeTally }> {
    const { league, snapshotId } = manifest;
    const resultKeys = await listKeys(
      this.deps.objectStore,
      workerResultPrefix(league, snapshotId),
    );
    const results: SnapshotCharacter[] = [];
    for (const key of resultKeys) {
      try {
        for await (const line of readWorkerResultFile(this.deps.objectStore, key)) {
          results.push(line);
        }
      } catch (err) {
        this.log(
          `skipping unreadable result file ${key}: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }

    // Merge the results into the state file (streamed, idempotent) and tally the
    // merged outcomes inline as each line is written — one streamed read + one
    // write, no second pass. `outcomes` is complete once writeState resolves
    // (the merge generator is fully drained before the put).
    const outcomes = emptyTally();
    const merged = mergeResults(readState(this.deps.objectStore, league, snapshotId), results);
    const tallying = async function* (): AsyncGenerator<SnapshotCharacter> {
      for await (const line of merged) {
        outcomes[line.outcome] += 1;
        yield line;
      }
    };
    await writeState(this.deps.objectStore, league, snapshotId, tallying());
    const rolled: SnapshotManifest = { ...manifest, outcomes };
    await this.deps.checkpointStore.save(rolled);

    // The merged state + manifest are durable — only now sweep the result files.
    for (const key of resultKeys) await this.deps.objectStore.delete(key);
    return { rolled, outcomes };
  }

  /**
   * Reap per-IP pace files older than paceFileTtlHours. Race-free by
   * construction: this fire's workers already wrote their IP files with a fresh
   * `updatedAt` (not stale → never swept), and the next fire is serialized by
   * the shared concurrency group. A file missing/with an unparseable body is
   * treated as junk and removed; one with a future/NaN timestamp is left alone.
   */
  private async sweepStalePaceFiles(): Promise<void> {
    const now = this.deps.clock.now();
    const ttlMs = this.config.paceFileTtlHours * HOUR_MS;
    let swept = 0;
    for (const { key, state } of await this.pace.list(this.config.league)) {
      const writtenAt = state ? Date.parse(state.updatedAt) : Number.NaN;
      const stale = Number.isNaN(writtenAt) ? state === undefined : now - writtenAt > ttlMs;
      if (stale) {
        await this.deps.objectStore.delete(key);
        swept += 1;
      }
    }
    if (swept > 0) this.log(`pace sweep: removed ${swept} stale per-IP file(s)`);
  }

  private aged(manifest: SnapshotManifest): boolean {
    return (
      this.deps.clock.now() - Date.parse(manifest.ladderCapturedAt) >
      this.config.maxAgeHours * HOUR_MS
    );
  }

  private summarizeCollecting(
    manifest: SnapshotManifest,
    stopReason: FinalizeStopReason,
  ): FinalizeSummary {
    return {
      phase: manifest.phase,
      stopReason,
      outcomes: manifest.outcomes,
    };
  }
}
