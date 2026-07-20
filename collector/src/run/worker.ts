/**
 * Worker step: one of N parallel chunk consumers (docs/ARCHITECTURE.md §5).
 *
 * Workers run as a GitHub Actions matrix after the coordinator. Each worker:
 *  1. loads the manifest (must be `collecting`) and ONLY the chunk files it
 *     owns — the indices where `chunkIndex % workerCount === workerIndex`,
 *     enumerated from the manifest's chunkCount without reading anything;
 *  2. takes the still-pending chunks among those it owns — ownership is
 *     round-robin by worker index, so it is disjoint by construction and no two
 *     workers ever touch (or even read for) the same chunk (the redesign's
 *     no-shared-chunk requirement, with no locks or claims needed);
 *  3. resolves each owned chunk's not-yet-computed characters, writes the raw
 *     records as ONE shard per chunk visit, then checkpoints the chunk file
 *     (shard before chunk: a crash in between leaves an orphan shard that
 *     finalize/next-visit cleanup removes — never lost outcomes);
 *  4. persists its own limiter memory under its slot (each runner has its own
 *     IP, so rate-limit adaptation is per slot). The pace state inside it is
 *     additionally scoped to the runner's public IP (deps.publicIp →
 *     limiter.adoptIp): a new IP starts the per-IP windows fresh, while
 *     penalties/streaks — client-scoped signals — always carry across runs.
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
  tallyOutcomes,
} from '@pou/shared';
import type { Clock } from '../rate-limit/clock.js';
import type { RateLimiter } from '../rate-limit/limiter.js';
import { workerSlot } from '../rate-limit/limiter-store.js';
import { LimiterPersistence } from '../rate-limit/limiter-persistence.js';
import type { CheckpointStore } from '../checkpoint/store.js';
import type { ObjectStore } from '../checkpoint/object-store.js';
import type { CharacterSource } from '../sources/types.js';
import { ChunkStore, ownedChunkIndices, pendingChunkIndices } from '../chunks/chunk-store.js';
import { HOUR_MS } from './config.js';
import { QuorumMonitor } from './worker-quorum.js';
import { resolveCharacter, type WaitReporter } from './resolve-character.js';

export interface WorkerConfig {
  league: string;
  workerIndex: number;
  workerCount: number;
  maxRunMillis: number;
  /** Longest rate-limit wait worth sleeping through (see RunConfig). */
  maxWaitMillis: number;
  maxAgeHours: number;
  maxAttempts: number;
  /**
   * The workflow fire's id (GITHUB_RUN_ID), scoping worker done markers to one
   * fire. Empty string = unavailable (local run) → early stop is inert.
   */
  runId: string;
  /**
   * Early stop: once at least this many workers have finished their run this
   * fire (any clean stop — drained, budget spent, rate-limit stall), the rest
   * checkpoint and stop instead of dragging the fire out (0 = disabled). Safe
   * by construction: a stopped worker's chunks stay pending and resume under
   * the same slot on the next fire.
   */
  earlyStopQuorum: number;
  /** Marker sweep throttle override (test seam); see QUORUM_CHECK_INTERVAL_MS. */
  quorumCheckIntervalMillis?: number | undefined;
}

export interface WorkerDeps {
  clock: Clock;
  characterSource: CharacterSource;
  checkpointStore: CheckpointStore;
  objectStore: ObjectStore;
  limiter: RateLimiter;
  /**
   * This runner's public IP (discoverPublicIp), scoping the restored pace
   * state via limiter.adoptIp. undefined = discovery failed/skipped — the
   * limiter then keeps the checkpointed pace state (conservative).
   */
  publicIp?: string | undefined;
  log?: (message: string) => void;
}

export type WorkerStopReason =
  | 'no_work'
  | 'assigned_drained'
  | 'budget_exhausted'
  | 'rate_limited'
  /** The limiter's next request slot was maxWaitMillis+ away — a saturated
   *  long window; checkpoint and let a later run resume, don't idle. */
  | 'rate_limit_stall'
  /** Enough sibling workers finished their run this fire (earlyStopQuorum):
   *  checkpoint and stop so one straggler doesn't hold finalize — and the next
   *  cron fire, via the shared concurrency group — hostage. */
  | 'quorum_stopped';

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
  private readonly limiterState: LimiterPersistence;
  private readonly quorum: QuorumMonitor;

  constructor(
    private readonly config: WorkerConfig,
    private readonly deps: WorkerDeps,
  ) {
    this.chunks = new ChunkStore(deps.objectStore);
    this.limiterState = new LimiterPersistence(deps.objectStore);
    this.quorum = new QuorumMonitor(
      {
        league: config.league,
        workerIndex: config.workerIndex,
        workerCount: config.workerCount,
        runId: config.runId,
        earlyStopQuorum: config.earlyStopQuorum,
        checkIntervalMillis: config.quorumCheckIntervalMillis,
      },
      { clock: deps.clock, objectStore: deps.objectStore, log: deps.log },
    );
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

    const scope = { league: this.config.league, slot, ip: this.deps.publicIp };
    if (await this.limiterState.loadInto(this.deps.limiter, scope)) {
      this.log(
        'rate-limit: runner IP changed since the checkpoint — pace windows start fresh, penalties kept',
      );
    }

    // Load only the chunks this slot owns (not the whole snapshot): the reads
    // scale with a worker's share of the queue, and a near-drained snapshot
    // stops costing every worker a full-snapshot read on every fire.
    const owned = await this.chunks.loadMany(
      manifest.league,
      manifest.snapshotId,
      ownedChunkIndices(manifest.chunkCount, this.config.workerIndex, this.config.workerCount),
    );
    const ownedByIndex = new Map(owned.map((chunk) => [chunk.chunkIndex, chunk]));
    const assigned = pendingChunkIndices(owned);
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
      const chunk = ownedByIndex.get(chunkIndex);
      if (!chunk) continue;
      if (this.deps.limiter.isAborted) {
        stop = 'rate_limited';
        break;
      }
      if (this.deps.clock.now() - runStart >= this.config.maxRunMillis) {
        stop = 'budget_exhausted';
        break;
      }
      if (await this.quorum.shouldStop()) {
        stop = 'quorum_stopped';
        break;
      }

      const visit = await this.workChunk(manifest, chunk, runStart);
      requests += visit.requests;
      shardsWritten += visit.shardsWritten;
      if (visit.resolved) chunksResolved += 1;
      addTallies(touched, tallyOutcomes(chunk.characters));
      if (visit.stopped) {
        stop = visit.stopped;
        break;
      }
    }

    // Every clean stop ends this slot's job for the fire, so every one counts
    // toward the early-stop quorum — not just a drained assignment. (A wave
    // where most workers stall on saturated rate-limit windows would otherwise
    // write no markers at all, and the stragglers would grind out their full
    // budget while every finished sibling job waits on them.)
    await this.quorum.markSelfDone(new Date(this.deps.clock.now()).toISOString(), stop);

    await this.limiterState.save(
      this.deps.limiter,
      scope,
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
  ): Promise<{
    requests: number;
    shardsWritten: number;
    resolved: boolean;
    stopped: WorkerStopReason | undefined;
  }> {
    await this.deleteOrphanShards(manifest, chunk);

    const records: unknown[] = [];
    let requests = 0;
    let stopped: WorkerStopReason | undefined;

    for (const entry of chunk.characters) {
      // Only not-yet-computed characters are workable; every other outcome
      // (ok/private/dead/skipped) is terminal.
      if (entry.outcome !== 'pending' && entry.outcome !== 'retryable') {
        continue;
      }
      if (this.deps.limiter.isAborted) {
        stopped = 'rate_limited';
        break;
      }
      if (this.deps.clock.now() - runStart >= this.config.maxRunMillis) {
        stopped = 'budget_exhausted';
        break;
      }
      if (await this.quorum.shouldStop()) {
        stopped = 'quorum_stopped';
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
        deadlineMs: runStart + this.config.maxRunMillis,
        maxWaitMs: this.config.maxWaitMillis,
      });
      requests += result.requests;
      if (result.record !== undefined) records.push(result.record);
      if (result.deferred !== undefined) {
        // A stall guard tripped: sleeping would idle the runner for nothing
        // (past the budget) or for a saturated long window (>= maxWaitMillis).
        // Checkpoint what we have; the next scheduled fire resumes with the
        // window drained.
        const waitSec = (this.deps.limiter.nextAcquireAt() - this.deps.clock.now()) / 1000;
        this.log(
          `rate-limit: next request slot in ${waitSec.toFixed(1)}s — ` +
            (result.deferred === 'max_wait'
              ? 'too long to idle, checkpointing for the next run'
              : 'past the run budget, checkpointing'),
        );
        stopped = result.deferred === 'max_wait' ? 'rate_limit_stall' : 'budget_exhausted';
        break;
      }
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

  /**
   * Clear the one shard a crash between the shard PUT and the chunk checkpoint
   * can orphan. A visit writes exactly one shard at seq == shardsWritten and
   * only then bumps the cursor and saves the chunk, so a resumed chunk has at
   * most one orphan, always at seq == shardsWritten. A targeted DELETE (free in
   * R2, a no-op when the key is absent) replaces the prefix LIST that scanning
   * for it used to cost — one Class A operation saved per chunk visit. Any
   * stray shard past the cursor (a legacy multi-orphan state) is still swept by
   * finalize's full pre-publish scan before it can reach the transform.
   */
  private async deleteOrphanShards(
    manifest: SnapshotManifest,
    chunk: SnapshotChunk,
  ): Promise<void> {
    await this.deps.objectStore.delete(
      rawChunkShardPath(
        manifest.league,
        manifest.snapshotId,
        chunk.chunkIndex,
        chunk.shardsWritten,
      ),
    );
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
