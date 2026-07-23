/**
 * Worker step: one of N parallel collectors (docs/ARCHITECTURE.md §5), v4
 * state-file model (docs/PLAN_SNAPSHOT_STATE_REWORK.md §5).
 *
 * Workers run as a GitHub Actions matrix after the coordinator. Each worker:
 *  1. loads the manifest (must be `collecting`) and streams the snapshot's
 *     single NDJSON.gz state file ONCE to derive the still-pending identities
 *     (payload-free — no raw payloads are held), keeping only the share it owns:
 *     the pending ordinals where `ordinal % workerCount === workerIndex`;
 *  2. that split is round-robin by worker index over the state file's stable
 *     line order, so it is disjoint by construction and no two workers ever
 *     resolve (or fetch for) the same character (the redesign's single-writer
 *     requirement, with no locks or claims needed);
 *  3. resolves each owned pending character and buffers every resolution into
 *     this slot's ONE transient result object (state/<league>/results/<id>/
 *     w<NN>.ndjson.gz) — a gzipped NDJSON `SnapshotCharacter` line per character
 *     it resolved this run, carrying the raw items/passives inline for the `ok`
 *     ones. The result object is overwritten in place periodically (every
 *     `resultCheckpointEvery` resolved characters) and once more on every clean
 *     stop, so a worker crash loses at most a few characters' fetches, not the
 *     whole run (design decision 4);
 *  4. persists its own limiter memory under its slot (each runner has its own
 *     IP, so rate-limit adaptation is per slot). The pace state inside it is
 *     additionally scoped to the runner's public IP (deps.publicIp →
 *     limiter.adoptIp): a new IP starts the per-IP windows fresh, while
 *     penalties/streaks — client-scoped signals — always carry across runs.
 *
 * The worker NEVER writes the state file, the manifest, another slot's objects
 * or another worker's result file — every object keeps exactly one writer (its
 * own `w<NN>` result object). Finalize is the single writer that merges the
 * result files back into the state file; a worker re-derives its pending work
 * from the SAME state file finalize merges into, so a finalize that crashed
 * before merging a result file finds those characters still pending next fire
 * and re-resolves them — they re-merge idempotently, nothing is lost (the
 * recovery invariant, design decision 3).
 */
import type {
  OutcomeTally,
  QueuedCharacter,
  SnapshotCharacter,
  SnapshotManifest,
} from '@classolek/shared';
import { tallyOutcomes } from '@classolek/shared';
import type { Clock } from '../rate-limit/clock.js';
import type { RateLimiter } from '../rate-limit/limiter.js';
import { workerSlot } from '../rate-limit/limiter-store.js';
import { LimiterPersistence } from '../rate-limit/limiter-persistence.js';
import type { CheckpointStore } from '../checkpoint/store.js';
import type { ObjectStore } from '../checkpoint/object-store.js';
import type { CharacterSource } from '../sources/types.js';
import {
  assignedTo,
  pendingIdentities,
  readState,
  writeWorkerResults,
  type PendingIdentity,
} from '../snapshot-state/state-store.js';
import { HOUR_MS } from './config.js';
import { QuorumMonitor } from './worker-quorum.js';
import { resolveCharacter, type WaitReporter } from './resolve-character.js';

/** Result-file checkpoint cadence when a worker leaves it unset: overwrite the
 *  slot's result object every this-many resolved characters (design decision 4). */
const DEFAULT_RESULT_CHECKPOINT_EVERY = 50;

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
   * by construction: a stopped worker's owned characters stay pending in the
   * state file and resume under the same slot on the next fire.
   */
  earlyStopQuorum: number;
  /** Marker sweep throttle override (test seam); see QUORUM_CHECK_INTERVAL_MS. */
  quorumCheckIntervalMillis?: number | undefined;
  /**
   * Overwrite this slot's result object every N resolved characters
   * (DEFAULT_RESULT_CHECKPOINT_EVERY when unset). Bounds the fetches lost if the
   * runner crashes mid-run to at most N characters; the final flush on any clean
   * stop always brings the file fully current.
   */
  resultCheckpointEvery?: number | undefined;
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
  /**
   * Pending characters this slot owned this run. (Named `assignedChunks` from
   * the chunk-model era; it is a character count now — Phase 7 renames the
   * display wording.)
   */
  assignedChunks: number;
  /**
   * Characters this run brought to a terminal outcome (ok/private/dead/skipped).
   * (Named `chunksResolved` from the chunk-model era; Phase 7 renames it.)
   */
  chunksResolved: number;
  requests: number;
  /**
   * Result-file checkpoints written this run (the periodic overwrites plus the
   * final flush). (Named `shardsWritten` from the chunk-model era — there are no
   * raw shards now; Phase 7 renames it.)
   */
  shardsWritten: number;
  /** Outcome tally across the characters this worker resolved this run. */
  outcomes: OutcomeTally;
}

export class Worker {
  private readonly limiterState: LimiterPersistence;
  private readonly quorum: QuorumMonitor;

  constructor(
    private readonly config: WorkerConfig,
    private readonly deps: WorkerDeps,
  ) {
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
      return this.summarize('no_work', 0, 0, 0, 0, []);
    }
    // An over-age snapshot is finalize's to abort — don't spend requests on it.
    if (runStart - Date.parse(manifest.ladderCapturedAt) > this.config.maxAgeHours * HOUR_MS) {
      this.log('worker: snapshot aged past max age — leaving it to finalize');
      return this.summarize('no_work', 0, 0, 0, 0, []);
    }

    const scope = { league: this.config.league, slot, ip: this.deps.publicIp };
    if (await this.limiterState.loadInto(this.deps.limiter, scope)) {
      this.log(
        'rate-limit: runner IP changed since the checkpoint — pace windows start fresh, penalties kept',
      );
    }

    // Stream the state file ONCE to enumerate the still-pending identities, then
    // keep only the ordinals this slot owns (ordinal % workerCount === index).
    // The read scales with the whole file but holds no payloads — only the small
    // owned-identity list survives. A near-drained snapshot still costs one
    // streamed read per worker per fire (design decision 5).
    const pending = await pendingIdentities(
      readState(this.deps.objectStore, manifest.league, manifest.snapshotId),
    );
    const owned = ownedPending(pending, this.config.workerIndex, this.config.workerCount);
    this.log(
      `worker ${slot}: ${owned.length} pending character(s) assigned of ${pending.length} total`,
    );

    let requests = 0;
    let stop: WorkerStopReason = 'assigned_drained';

    // The v4 result file: every character this run resolves (any outcome change)
    // is buffered here and the whole slot object is overwritten periodically —
    // `checkpointEvery` resolved characters — and once more on the clean stop
    // below. `resultsAtLastFlush` tracks what is already durable so the final
    // flush is skipped when a periodic one already caught up.
    const results: SnapshotCharacter[] = [];
    let resultsAtLastFlush = 0;
    let flushes = 0;
    const checkpointEvery = this.config.resultCheckpointEvery ?? DEFAULT_RESULT_CHECKPOINT_EVERY;
    const flush = async (): Promise<void> => {
      await this.flushResults(manifest, results);
      resultsAtLastFlush = results.length;
      flushes += 1;
    };
    const checkpointResults = async (): Promise<void> => {
      if (results.length - resultsAtLastFlush >= checkpointEvery) await flush();
    };

    for (const identity of owned) {
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

      this.log(`resolving #${identity.rank} ${identity.account}/${identity.character}`);
      const entry = entryOf(identity);
      const before = { outcome: entry.outcome, attempts: entry.attempts };
      const result = await resolveCharacter(entry, this.config.maxAttempts, {
        clock: this.deps.clock,
        characterSource: this.deps.characterSource,
        limiter: this.deps.limiter,
        onWait: this.onWait,
        deadlineMs: runStart + this.config.maxRunMillis,
        maxWaitMs: this.config.maxWaitMillis,
      });
      requests += result.requests;
      // Record the resolution whenever this attempt moved the character (a new
      // terminal outcome, or a burned retry attempt). A pure rate-limit/stall
      // leaves the entry unchanged and unrecorded — it stays pending in the
      // state file and is re-derived by a later run.
      if (entry.outcome !== before.outcome || entry.attempts !== before.attempts) {
        results.push(toResultLine(entry, result.record));
        await checkpointResults();
      }
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
        stop = result.deferred === 'max_wait' ? 'rate_limit_stall' : 'budget_exhausted';
        break;
      }
    }

    // Final result checkpoint on the clean stop: the slot object reflects every
    // character resolved this run (skipped when a periodic flush already did).
    if (results.length > resultsAtLastFlush) await flush();

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
    const resolved = results.filter((r) => r.outcome !== 'retryable').length;
    this.log(`worker ${slot}: stop=${stop} resolved=${resolved} requests=${requests}`);
    return this.summarize(stop, owned.length, resolved, requests, flushes, results);
  }

  /**
   * Overwrite this slot's single result object with the whole run so far. A
   * full put (atomic on R2), single writer by construction — no other slot ever
   * writes this `w<NN>` key — so a periodic checkpoint just re-puts the grown
   * buffer, and a resumed/failed run's stale file is harmlessly replaced.
   */
  private async flushResults(
    manifest: SnapshotManifest,
    results: readonly SnapshotCharacter[],
  ): Promise<void> {
    await writeWorkerResults(
      this.deps.objectStore,
      manifest.league,
      manifest.snapshotId,
      this.config.workerIndex,
      results,
    );
  }

  private summarize(
    stopReason: WorkerStopReason,
    assignedChunks: number,
    chunksResolved: number,
    requests: number,
    shardsWritten: number,
    resolved: readonly SnapshotCharacter[],
  ): WorkerSummary {
    return {
      workerIndex: this.config.workerIndex,
      stopReason,
      assignedChunks,
      chunksResolved,
      requests,
      shardsWritten,
      outcomes: tallyOutcomes(resolved),
    };
  }
}

/** The pending identities a worker slot owns this run (ordinal round-robin). */
function ownedPending(
  pending: readonly PendingIdentity[],
  workerIndex: number,
  workerCount: number,
): PendingIdentity[] {
  const owned = new Set(
    assignedTo(
      pending.map((p) => p.ordinal),
      workerIndex,
      workerCount,
    ),
  );
  return pending.filter((p) => owned.has(p.ordinal));
}

/** A resolvable QueuedCharacter from a pending identity (resolveCharacter mutates it in place). */
function entryOf(identity: PendingIdentity): QueuedCharacter {
  return {
    rank: identity.rank,
    account: identity.account,
    character: identity.character,
    class: identity.class,
    level: identity.level,
    outcome: identity.outcome,
    attempts: identity.attempts,
  };
}

/**
 * Build one v4 result line from a just-resolved entry. Copies the queued
 * identity + the outcome/attempts the resolution wrote in place; for an `ok`
 * character it also inlines the raw payloads from `resolveCharacter`'s record
 * (`items` → `characterData`, `passives` → `passiveTree`), matching
 * `SnapshotCharacter`. Non-`ok` lines carry no payloads (there are none).
 */
function toResultLine(entry: QueuedCharacter, record: unknown): SnapshotCharacter {
  const line: SnapshotCharacter = {
    rank: entry.rank,
    account: entry.account,
    character: entry.character,
    class: entry.class,
    level: entry.level,
    outcome: entry.outcome,
    attempts: entry.attempts,
    ...(entry.fetchedAt !== undefined ? { fetchedAt: entry.fetchedAt } : {}),
  };
  if (record !== undefined) {
    const raw = record as { items?: unknown; passives?: unknown };
    line.characterData = raw.items;
    line.passiveTree = raw.passives;
  }
  return line;
}
