/**
 * Worker step: one of N parallel chunk consumers (docs/ARCHITECTURE.md §5).
 *
 * Workers run as a GitHub Actions matrix after the coordinator. Each worker:
 *  1. loads the manifest (must be `collecting`) and every chunk file;
 *  2. takes the pending chunks it OWNS this run — the pending chunk list dealt
 *     round-robin by worker index, so ownership is disjoint by construction and
 *     no two workers ever touch the same chunk (the redesign's no-shared-chunk
 *     requirement, with no locks or claims needed);
 *  3. resolves each owned chunk's not-yet-computed characters, writes the raw
 *     records as ONE shard per chunk visit, then checkpoints the chunk file
 *     (shard before chunk: a crash in between leaves an orphan shard that
 *     finalize/next-visit cleanup removes — never lost outcomes);
 *  4. persists its own limiter memory under its slot (each runner has its own
 *     IP, so rate-limit adaptation is per slot).
 *
 * A worker never writes the manifest, the roster, another slot's state or
 * another worker's chunks — every object keeps exactly one writer.
 */
import { gzipSync } from 'node:zlib';
import type { OutcomeTally, SnapshotChunk, SnapshotManifest } from '@pou/shared';
import {
  addTallies,
  emptyTally,
  isChunkResolved,
  rawChunkShardPath,
  rawChunkShardPrefix,
  tallyOutcomes,
} from '@pou/shared';
import type { Clock } from '../rate-limit/clock.js';
import type { RateLimiter } from '../rate-limit/limiter.js';
import { LimiterStateStore, workerSlot } from '../rate-limit/limiter-store.js';
import type { CheckpointStore } from '../checkpoint/store.js';
import { listKeys, type ObjectStore } from '../checkpoint/object-store.js';
import type { CharacterSource } from '../sources/types.js';
import { ChunkStore, assignedChunkIndices, pendingChunkIndices } from '../chunks/chunk-store.js';
import { HOUR_MS } from './config.js';
import { resolveCharacter, type WaitReporter } from './resolve-character.js';

export interface WorkerConfig {
  league: string;
  workerIndex: number;
  workerCount: number;
  maxRunMillis: number;
  maxAgeHours: number;
  maxAttempts: number;
}

export interface WorkerDeps {
  clock: Clock;
  characterSource: CharacterSource;
  checkpointStore: CheckpointStore;
  objectStore: ObjectStore;
  limiter: RateLimiter;
  log?: (message: string) => void;
}

export type WorkerStopReason = 'no_work' | 'assigned_drained' | 'budget_exhausted' | 'rate_limited';

export interface WorkerSummary {
  workerIndex: number;
  stopReason: WorkerStopReason;
  assignedChunks: number;
  /** Chunks this run brought to full resolution. */
  chunksResolved: number;
  requests: number;
  shardsWritten: number;
  /** Outcome tally across the chunks this worker touched (after this run). */
  outcomes: OutcomeTally;
}

export class Worker {
  private readonly chunks: ChunkStore;
  private readonly limiterStates: LimiterStateStore;

  constructor(
    private readonly config: WorkerConfig,
    private readonly deps: WorkerDeps,
  ) {
    this.chunks = new ChunkStore(deps.objectStore);
    this.limiterStates = new LimiterStateStore(deps.objectStore);
  }

  private log(message: string): void {
    this.deps.log?.(message);
  }

  private readonly onWait: WaitReporter = (ms, reason) => {
    if (ms >= 2000) this.log(`rate-limit: waiting ${(ms / 1000).toFixed(1)}s (${reason})`);
  };

  async runOnce(): Promise<WorkerSummary> {
    const runStart = this.deps.clock.now();
    const slot = workerSlot(this.config.workerIndex);
    const manifest = await this.deps.checkpointStore.load(this.config.league);

    if (!manifest || manifest.phase !== 'collecting') {
      return this.summarize('no_work', 0, 0, 0, 0, emptyTally());
    }
    // An over-age snapshot is finalize's to abort — don't spend requests on it.
    if (runStart - Date.parse(manifest.ladderCapturedAt) > this.config.maxAgeHours * HOUR_MS) {
      this.log('worker: snapshot aged past max age — leaving it to finalize');
      return this.summarize('no_work', 0, 0, 0, 0, emptyTally());
    }

    const restored = await this.limiterStates.load(this.config.league, slot);
    if (restored) this.deps.limiter.restore(restored);

    const all = await this.chunks.loadAll(
      manifest.league,
      manifest.snapshotId,
      manifest.chunkCount,
    );
    const assigned = assignedChunkIndices(
      pendingChunkIndices(all),
      this.config.workerIndex,
      this.config.workerCount,
    );
    this.log(
      `worker ${slot}: ${assigned.length} pending chunk(s) assigned of ` +
        `${manifest.chunkCount} total`,
    );

    let requests = 0;
    let shardsWritten = 0;
    let chunksResolved = 0;
    const touched = emptyTally();
    let stop: WorkerStopReason = 'assigned_drained';

    for (const chunkIndex of assigned) {
      const chunk = all[chunkIndex];
      if (!chunk) continue;
      if (this.deps.limiter.isAborted) {
        stop = 'rate_limited';
        break;
      }
      if (this.deps.clock.now() - runStart >= this.config.maxRunMillis) {
        stop = 'budget_exhausted';
        break;
      }

      const visit = await this.workChunk(manifest, chunk, runStart);
      requests += visit.requests;
      shardsWritten += visit.shardsWritten;
      if (visit.resolved) chunksResolved += 1;
      addTallies(touched, tallyOutcomes(chunk.characters));
      if (visit.stopped) {
        stop = this.deps.limiter.isAborted ? 'rate_limited' : 'budget_exhausted';
        break;
      }
    }

    await this.limiterStates.save(
      this.config.league,
      slot,
      this.deps.limiter.toMemory(),
      new Date(this.deps.clock.now()).toISOString(),
    );
    this.log(`worker ${slot}: stop=${stop} chunksResolved=${chunksResolved} requests=${requests}`);
    return this.summarize(stop, assigned.length, chunksResolved, requests, shardsWritten, touched);
  }

  /**
   * One visit to one owned chunk: clean orphan shards, resolve what fits in the
   * budget (one pass — a still-retryable character heals across runs, not by
   * being hammered in place), write the visit's records as one shard, then
   * checkpoint the chunk.
   */
  private async workChunk(
    manifest: SnapshotManifest,
    chunk: SnapshotChunk,
    runStart: number,
  ): Promise<{ requests: number; shardsWritten: number; resolved: boolean; stopped: boolean }> {
    await this.deleteOrphanShards(manifest, chunk);

    const records: unknown[] = [];
    let requests = 0;
    let stopped = false;

    for (const entry of chunk.characters) {
      // Only not-yet-computed characters are workable; every other outcome
      // (ok/private/dead/skipped) is terminal.
      if (entry.outcome !== 'pending' && entry.outcome !== 'retryable') {
        continue;
      }
      if (this.deps.limiter.isAborted) {
        stopped = true;
        break;
      }
      if (this.deps.clock.now() - runStart >= this.config.maxRunMillis) {
        stopped = true;
        break;
      }
      this.log(
        `chunk ${chunk.chunkIndex}: fetching #${entry.rank} ${entry.account}/${entry.character}`,
      );
      const result = await resolveCharacter(entry, this.config.maxAttempts, {
        clock: this.deps.clock,
        characterSource: this.deps.characterSource,
        limiter: this.deps.limiter,
        onWait: this.onWait,
      });
      requests += result.requests;
      if (result.record !== undefined) records.push(result.record);
    }

    // Shard first, then the chunk checkpoint that owns it (crash in between
    // leaves an orphan shard at seq == shardsWritten, removed on next visit).
    let shardsWritten = 0;
    if (records.length > 0) {
      const key = rawChunkShardPath(
        manifest.league,
        manifest.snapshotId,
        chunk.chunkIndex,
        chunk.shardsWritten,
      );
      const ndjson = records.map((r) => JSON.stringify(r)).join('\n') + '\n';
      await this.deps.objectStore.put(key, gzipSync(Buffer.from(ndjson, 'utf8')));
      chunk.shardsWritten += 1;
      shardsWritten = 1;
    }
    chunk.workerIndex = this.config.workerIndex;
    await this.chunks.save(chunk);

    return { requests, shardsWritten, resolved: isChunkResolved(chunk), stopped };
  }

  /** Remove shards at/past the chunk's committed cursor (crash-resume safety). */
  private async deleteOrphanShards(
    manifest: SnapshotManifest,
    chunk: SnapshotChunk,
  ): Promise<void> {
    const prefix = rawChunkShardPrefix(manifest.league, manifest.snapshotId, chunk.chunkIndex);
    for (const key of await listKeys(this.deps.objectStore, prefix)) {
      const match = /-(\d+)\.ndjson\.gz$/.exec(key);
      if (match && Number(match[1]) >= chunk.shardsWritten) {
        await this.deps.objectStore.delete(key);
      }
    }
  }

  private summarize(
    stopReason: WorkerStopReason,
    assignedChunks: number,
    chunksResolved: number,
    requests: number,
    shardsWritten: number,
    outcomes: OutcomeTally,
  ): WorkerSummary {
    return {
      workerIndex: this.config.workerIndex,
      stopReason,
      assignedChunks,
      chunksResolved,
      requests,
      shardsWritten,
      outcomes,
    };
  }
}
