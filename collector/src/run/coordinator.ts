/**
 * Coordinator step: the snapshot lifecycle driver (docs/ARCHITECTURE.md §5).
 *
 *   Idle ─▶ LadderCapture (capture → roster merge → seed chunks) ─▶ Collecting
 *                └────────────────────────────────────────────────▶ Aborted
 *
 * The coordinator runs first in every workflow fire, alone (single writer of
 * the manifest/roster/chunks at this point). It:
 *  1. starts a new snapshot when none is in flight and the interval/cooldown
 *     has elapsed;
 *  2. on ladder_capture: reads the ladder (one atomic pass), MERGES the entries
 *     into the per-league roster (the growing character database), then seeds
 *     the snapshot queue from the ENTIRE roster as pending chunks — this is
 *     what lets snapshots grow past the 15k ladder window over time;
 *  3. reports whether worker jobs should fan out this run (has_work).
 *
 * Character collection itself belongs to the worker step; rollup, publishing
 * and aborts of an in-flight snapshot belong to the finalize step.
 */
import type { LadderEntry, LadderResult, LadderSource } from '../sources/types.js';
import type { SnapshotManifest, SnapshotPhase } from '@pou/shared';
import { SCHEMA_VERSION, chunkCountFor, emptyTally, pendingOfTally } from '@pou/shared';
import type { Clock } from '../rate-limit/clock.js';
import type { RateLimiter } from '../rate-limit/limiter.js';
import { COORDINATOR_SLOT, LimiterStateStore } from '../rate-limit/limiter-store.js';
import type { CheckpointStore } from '../checkpoint/store.js';
import type { ObjectStore } from '../checkpoint/object-store.js';
import { RosterStore, mergeLadder } from '../roster/roster-store.js';
import { ChunkStore, planChunks } from '../chunks/chunk-store.js';
import { HOUR_MS, type RunConfig } from './config.js';
import type { WaitReporter } from './resolve-character.js';

export interface CoordinatorDeps {
  clock: Clock;
  ladderSource: LadderSource;
  checkpointStore: CheckpointStore;
  objectStore: ObjectStore;
  limiter: RateLimiter;
  /** Injected id generator so snapshot ids are deterministic in tests. */
  newSnapshotId?: (now: number) => string;
  log?: (message: string) => void;
}

export type CoordinatorStopReason =
  'idle' | 'aborted' | 'budget_exhausted' | 'seeded' | 'work_pending';

export interface CoordinatorSummary {
  phase: SnapshotPhase;
  stopReason: CoordinatorStopReason;
  /** Whether the workflow should fan worker jobs out this run. */
  hasWork: boolean;
  /** Worker slot indices for the workflow matrix ([] when hasWork is false). */
  workers: number[];
  requests: number;
  rosterSize: number;
  rosterAdded: number;
  totalCharacters: number;
  chunkCount: number;
}

export class Coordinator {
  private readonly limiterStates: LimiterStateStore;
  private readonly rosters: RosterStore;
  private readonly chunks: ChunkStore;

  constructor(
    private readonly config: RunConfig,
    private readonly deps: CoordinatorDeps,
  ) {
    this.limiterStates = new LimiterStateStore(deps.objectStore);
    this.rosters = new RosterStore(deps.objectStore);
    this.chunks = new ChunkStore(deps.objectStore);
  }

  private log(message: string): void {
    this.deps.log?.(message);
  }

  private readonly onWait: WaitReporter = (ms, reason) => {
    if (ms >= 2000) this.log(`rate-limit: waiting ${(ms / 1000).toFixed(1)}s (${reason})`);
  };

  async runOnce(): Promise<CoordinatorSummary> {
    const runStart = this.deps.clock.now();
    const restored = await this.limiterStates.load(this.config.league, COORDINATOR_SLOT);
    if (restored) this.deps.limiter.restore(restored);

    let manifest = await this.deps.checkpointStore.load(this.config.league);

    if (!manifest) {
      manifest = await this.begin();
    } else if (manifest.phase === 'published') {
      if (this.within(manifest.completedAt, runStart, this.config.snapshotIntervalHours)) {
        return this.summarize(manifest, 'idle', 0);
      }
      manifest = await this.begin();
    } else if (manifest.phase === 'aborted') {
      if (this.within(manifest.abortedAt, runStart, this.config.abortCooldownHours)) {
        return this.summarize(manifest, 'idle', 0);
      }
      manifest = await this.begin();
    } else if (manifest.phase === 'transforming') {
      // All chunks resolved in a prior run; the finalize step owns the final
      // transform (and the max-age abort of a wedged one). No worker work.
      return this.summarize(manifest, 'idle', 0);
    } else if (manifest.phase === 'collecting') {
      // Workers continue the in-flight snapshot; the rollup in the manifest is
      // finalize's last word on what is still pending.
      return this.summarize(manifest, 'work_pending', 0);
    }

    if (manifest.phase === 'ladder_capture') {
      return this.captureAndSeed(manifest, runStart);
    }
    return this.summarize(manifest, 'idle', 0);
  }

  private async begin(): Promise<SnapshotManifest> {
    const now = this.deps.clock.now();
    const snapshotId = (this.deps.newSnapshotId ?? defaultSnapshotId)(now);
    const manifest: SnapshotManifest = {
      schemaVersion: SCHEMA_VERSION,
      snapshotId,
      league: this.config.league,
      depth: this.config.depth,
      phase: 'ladder_capture',
      ladderCapturedAt: new Date(now).toISOString(),
      chunkSize: this.config.chunkSize,
      chunkCount: 0,
      totalCharacters: 0,
      outcomes: emptyTally(),
      resolvedChunks: 0,
    };
    await this.deps.checkpointStore.save(manifest);
    return manifest;
  }

  /** Capture the ladder (atomic pass), merge the roster, seed pending chunks. */
  private async captureAndSeed(
    manifest: SnapshotManifest,
    runStart: number,
  ): Promise<CoordinatorSummary> {
    const captured = await this.captureLadder(manifest, runStart);
    if (captured.kind !== 'ok') {
      if (captured.kind === 'aborted') {
        const aborted: SnapshotManifest = {
          ...manifest,
          phase: 'aborted',
          abortedAt: this.nowIso(),
        };
        await this.deps.checkpointStore.save(aborted);
        await this.saveLimiter();
        return this.summarize(aborted, 'aborted', captured.requests);
      }
      // Budget ran out mid-capture: stay in ladder_capture, restart next run.
      await this.saveLimiter();
      return this.summarize(manifest, 'budget_exhausted', captured.requests);
    }

    // Redesign step 1: every ladder read appends what it saw to the per-league
    // character database. New entrants grow the roster; the first capture seeds
    // it with the full ladder window.
    const nowIso = this.nowIso();
    const roster = await this.rosters.load(this.config.league);
    const merged = mergeLadder(roster, captured.entries, nowIso);
    await this.rosters.save(merged.roster);
    this.log(
      `roster: ${merged.roster.characters.length} characters ` +
        `(+${merged.added} new, ${merged.refreshed} refreshed)`,
    );

    // Redesign step 2: the snapshot queue is the ENTIRE roster, every character
    // entering as `pending` (not computed), split into worker-sized chunks.
    // Chunks are re-seeded wholesale if a crash interrupted a previous seed
    // (manifest only moves to `collecting` after every chunk file is durable).
    await this.chunks.deleteAll(this.config.league, manifest.snapshotId);
    const chunks = planChunks(
      this.config.league,
      manifest.snapshotId,
      merged.roster.characters,
      this.config.chunkSize,
    );
    for (const chunk of chunks) await this.chunks.save(chunk);

    const total = merged.roster.characters.length;
    const collecting: SnapshotManifest = {
      ...manifest,
      phase: 'collecting',
      chunkCount: chunkCountFor(total, this.config.chunkSize),
      totalCharacters: total,
      outcomes: { ...emptyTally(), pending: total },
      resolvedChunks: 0,
    };
    await this.deps.checkpointStore.save(collecting);
    await this.saveLimiter();
    this.log(
      `seed: snapshot=${collecting.snapshotId} characters=${total} ` +
        `chunks=${collecting.chunkCount} (size ${this.config.chunkSize})`,
    );
    return {
      ...this.summarize(collecting, 'seeded', captured.requests),
      rosterSize: merged.roster.characters.length,
      rosterAdded: merged.added,
    };
  }

  private async captureLadder(
    manifest: SnapshotManifest,
    runStart: number,
  ): Promise<
    | { kind: 'ok'; entries: LadderEntry[]; requests: number }
    | { kind: 'aborted'; requests: number }
    | { kind: 'budget'; requests: number }
  > {
    const entries: LadderEntry[] = [];
    const { depth, ladderPageSize, league, maxAttempts, maxRunMillis } = this.config;
    let requests = 0;

    for (let offset = 0; offset < depth; offset += ladderPageSize) {
      let page: Extract<LadderResult, { kind: 'ok' }> | undefined;
      let attempts = 0;

      // Retry the page until it succeeds, is exhausted, aborts, or budget ends.
      while (page === undefined) {
        if (this.deps.limiter.isAborted) return { kind: 'aborted', requests };
        if (this.deps.clock.now() - runStart >= maxRunMillis) {
          // Capture is one atomic pass; a partial ladder would skew the roster
          // ranks, so restart cleanly next run.
          return { kind: 'budget', requests };
        }

        const limit = Math.min(ladderPageSize, depth - offset);
        await this.deps.limiter.acquire(this.onWait);
        this.log(`ladder: fetching ${league} offset=${offset} limit=${limit}`);
        const { result, observation } = await this.deps.ladderSource.fetchPage({
          league,
          offset,
          limit,
        });
        this.deps.limiter.observe(observation);
        requests += 1;

        if (result.kind === 'ok') {
          page = result;
          this.log(
            `ladder: got ${result.entries.length} entries at offset=${offset} (total=${result.total})`,
          );
          break;
        }
        this.log(`ladder: offset=${offset} not ok (${result.kind}); retrying`);
        // Ladder is not per-profile: a fatal status is a misconfiguration.
        if (result.kind === 'fatal') return { kind: 'aborted', requests };
        // rate_limited / retryable (incl. malformed ladder): bounded retry. The
        // limiter has already backed off; giving up after maxAttempts stops us
        // hammering a persistently-failing page (hard rules #1/#4).
        attempts += 1;
        if (attempts >= maxAttempts) return { kind: 'aborted', requests };
      }

      entries.push(...page.entries);
      if (page.entries.length === 0 || entries.length >= page.total) break;
    }

    this.log(`ladder: captured ${entries.length} characters for ${league} (depth ${depth})`);
    return { kind: 'ok', entries, requests };
  }

  private async saveLimiter(): Promise<void> {
    await this.limiterStates.save(
      this.config.league,
      COORDINATOR_SLOT,
      this.deps.limiter.toMemory(),
      this.nowIso(),
    );
  }

  private within(iso: string | undefined, now: number, hours: number): boolean {
    if (iso === undefined) return false;
    return now - Date.parse(iso) < hours * HOUR_MS;
  }

  private nowIso(): string {
    return new Date(this.deps.clock.now()).toISOString();
  }

  private summarize(
    manifest: SnapshotManifest,
    stopReason: CoordinatorStopReason,
    requests: number,
  ): CoordinatorSummary {
    const hasWork = manifest.phase === 'collecting' && pendingOfTally(manifest.outcomes) > 0;
    return {
      phase: manifest.phase,
      stopReason,
      hasWork,
      workers: hasWork ? Array.from({ length: this.config.workerCount }, (_, i) => i) : [],
      requests,
      rosterSize: manifest.totalCharacters,
      rosterAdded: 0,
      totalCharacters: manifest.totalCharacters,
      chunkCount: manifest.chunkCount,
    };
  }
}

function defaultSnapshotId(now: number): string {
  return new Date(now).toISOString().replace(/[:.]/g, '-');
}
