/**
 * Finalize step: runs after the worker matrix in every workflow fire
 * (docs/ARCHITECTURE.md §5). The single writer of the manifest at this point:
 *
 *  1. rolls the per-chunk outcomes up into the manifest (the snapshot's one
 *     authoritative tally);
 *  2. aborts an over-age snapshot (discarding raw, chunks, any incomplete
 *     published files and its index entry);
 *  3. while chunks remain pending: publishes the data collected SO FAR as an
 *     incomplete snapshot (complete: false) — immediately visible in the web
 *     app, republished in place each run. A partial-publish failure only warns:
 *     collection must keep going regardless;
 *  4. when every chunk is resolved: hands the snapshot to the final transform
 *     (executeTransform: bounded attempts → published, immutable from then on)
 *     and deletes the chunk files.
 */
import type { OutcomeTally, SnapshotManifest, SnapshotPhase } from '@pou/shared';
import {
  addTallies,
  emptyTally,
  isChunkResolved,
  pendingOfTally,
  rawShardPrefix,
  tallyOutcomes,
} from '@pou/shared';
import type { CheckpointStore } from '../checkpoint/store.js';
import type { ObjectStore } from '../checkpoint/object-store.js';
import { ChunkStore } from '../chunks/chunk-store.js';
import { executeTransform, type TransformStepConfig } from '../transform/execute.js';
import { runTransform, type TransformDeps, type TransformSummary } from '../transform/transform.js';
import { HOUR_MS } from './config.js';
import { discardSnapshotArtifacts } from './discard.js';

export interface FinalizeConfig extends TransformStepConfig {
  league: string;
  maxAgeHours: number;
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
  resolvedChunks: number;
  chunkCount: number;
  transform?: TransformSummary;
}

export class Finalizer {
  private readonly chunks: ChunkStore;

  constructor(
    private readonly config: FinalizeConfig,
    private readonly deps: FinalizeDeps,
  ) {
    this.chunks = new ChunkStore(deps.objectStore);
  }

  private log(message: string): void {
    this.deps.log?.(message);
  }

  async runOnce(): Promise<FinalizeSummary> {
    const manifest = await this.deps.checkpointStore.load(this.config.league);
    if (!manifest) {
      return { phase: 'none', stopReason: 'idle', ...emptyProgress() };
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
        resolvedChunks: manifest.resolvedChunks,
        chunkCount: manifest.chunkCount,
      };
    }

    // 1. Roll chunk outcomes up into the manifest (+ orphan-shard cleanup so
    //    raw and the rollup agree before any publish).
    const chunks = await this.chunks.loadAll(
      manifest.league,
      manifest.snapshotId,
      manifest.chunkCount,
    );
    await this.deleteOrphanShards(manifest, chunks);
    const outcomes = emptyTally();
    let resolvedChunks = 0;
    for (const chunk of chunks) {
      addTallies(outcomes, tallyOutcomes(chunk.characters));
      if (isChunkResolved(chunk)) resolvedChunks += 1;
    }
    const rolled: SnapshotManifest = { ...manifest, outcomes, resolvedChunks };
    await this.deps.checkpointStore.save(rolled);
    this.log(
      `rollup: ${resolvedChunks}/${rolled.chunkCount} chunks resolved ` +
        `(ok=${outcomes.ok} private=${outcomes.private} dead=${outcomes.dead} ` +
        `retry=${outcomes.retryable} pending=${outcomes.pending} skipped=${outcomes.skipped})`,
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
      // The chunk queue is spent state now — the published snapshot is the record.
      await this.chunks.deleteAll(manifest.league, manifest.snapshotId);
      return {
        phase: 'published',
        stopReason: 'published_final',
        outcomes: manifest.outcomes,
        resolvedChunks: manifest.resolvedChunks,
        chunkCount: manifest.chunkCount,
        transform: outcome.summary,
      };
    }
    // executeTransform aborted the snapshot (no chars / attempts exhausted):
    // finish the discard (chunks + any incomplete published files + index entry).
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
      resolvedChunks: manifest.resolvedChunks,
      chunkCount: manifest.chunkCount,
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
      resolvedChunks: manifest.resolvedChunks,
      chunkCount: manifest.chunkCount,
    };
  }

  /**
   * Remove raw shards past any chunk's committed cursor. A worker crash can
   * leave one orphan shard per chunk (shard written, chunk checkpoint not);
   * cleaning here keeps the rollup and raw consistent so validation of the
   * incremental publish doesn't trip over records no chunk accounts for.
   */
  private async deleteOrphanShards(
    manifest: SnapshotManifest,
    chunks: readonly { chunkIndex: number; shardsWritten: number }[],
  ): Promise<void> {
    const keys = await this.deps.objectStore.listDetailed(
      rawShardPrefix(manifest.league, manifest.snapshotId),
    );
    const cursorByChunk = new Map(chunks.map((c) => [c.chunkIndex, c.shardsWritten]));
    for (const { key } of keys) {
      const match = /chunk-(\d+)-(\d+)\.ndjson\.gz$/.exec(key);
      if (!match) continue;
      const cursor = cursorByChunk.get(Number(match[1]));
      if (cursor === undefined || Number(match[2]) >= cursor) {
        await this.deps.objectStore.delete(key);
      }
    }
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
      resolvedChunks: manifest.resolvedChunks,
      chunkCount: manifest.chunkCount,
    };
  }
}

function emptyProgress(): Pick<FinalizeSummary, 'outcomes' | 'resolvedChunks' | 'chunkCount'> {
  return { outcomes: emptyTally(), resolvedChunks: 0, chunkCount: 0 };
}
