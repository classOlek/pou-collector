/**
 * Create-snapshot step — its own workflow, whose cron IS the snapshot cadence
 * (the collect workflow only drains what this one seeds):
 *
 *  a) CLOSE the previous in-flight snapshot: uncollected characters are marked
 *     `skipped` and the snapshot publishes with what it has (close-snapshot.ts);
 *  b) CREATE the new snapshot: capture the ladder (one atomic pass), MERGE the
 *     entries into the per-league roster (the growing character database), then
 *     seed the snapshot queue from the ENTIRE roster as pending chunks — this
 *     is what lets snapshots grow past the ladder window over time.
 *
 * Scheduled fires respect two guards, both bypassed by a dispatch fire (force):
 *  - snapshotIntervalHours since the previous snapshot began (or completed):
 *    double-fire / misconfigured-cron protection;
 *  - abortCooldownHours after an abort (hard rule #1: no failure hammering).
 *
 * Runs alone in the shared `snapshot-collector` concurrency group, so a close
 * never overlaps a collect fire's workers (single writer per object, no locks).
 */
import type { LadderEntry, LadderResult, LadderSource } from '../sources/types.js';
import type { SnapshotManifest } from '@pou/shared';
import { SCHEMA_VERSION, chunkCountFor, emptyTally, isInFlight } from '@pou/shared';
import type { Clock } from '../rate-limit/clock.js';
import type { RateLimiter } from '../rate-limit/limiter.js';
import { COORDINATOR_SLOT, LimiterStateStore } from '../rate-limit/limiter-store.js';
import type { CheckpointStore } from '../checkpoint/store.js';
import type { ObjectStore } from '../checkpoint/object-store.js';
import { RosterStore, mergeLadder } from '../roster/roster-store.js';
import { ChunkStore, planChunks } from '../chunks/chunk-store.js';
import { HOUR_MS, type RunConfig } from './config.js';
import type { WaitReporter } from './resolve-character.js';
import type { Finalizer } from './finalize.js';
import { closeInFlightSnapshot, type CloseSummary } from './close-snapshot.js';

export interface CreatorDeps {
  clock: Clock;
  ladderSource: LadderSource;
  checkpointStore: CheckpointStore;
  objectStore: ObjectStore;
  limiter: RateLimiter;
  /** Finalizer for a league being closed (carries that league's tree version). */
  finalizerFor: (league: string) => Finalizer;
  /** Bypass the cadence guards (workflow_dispatch fires). */
  force?: boolean;
  /** Injected id generator so snapshot ids are deterministic in tests. */
  newSnapshotId?: (now: number) => string;
  log?: (message: string) => void;
}

export type CreateStopReason =
  'created' | 'too_recent' | 'cooldown' | 'aborted' | 'budget_exhausted';

export interface CreateSummary {
  stopReason: CreateStopReason;
  /** What closing the previous snapshot did (absent when nothing was in flight). */
  closed?: CloseSummary;
  requests: number;
  rosterSize: number;
  rosterAdded: number;
  totalCharacters: number;
  chunkCount: number;
}

export class SnapshotCreator {
  private readonly limiterStates: LimiterStateStore;
  private readonly rosters: RosterStore;
  private readonly chunks: ChunkStore;

  constructor(
    private readonly config: RunConfig,
    private readonly deps: CreatorDeps,
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

  async runOnce(): Promise<CreateSummary> {
    const runStart = this.deps.clock.now();
    const restored = await this.limiterStates.load(this.config.league, COORDINATOR_SLOT);
    if (restored) this.deps.limiter.restore(restored);

    const inFlight = (await this.deps.checkpointStore.listAll()).filter((m) => isInFlight(m.phase));
    const target = await this.deps.checkpointStore.load(this.config.league);

    // Cadence guards, decided BEFORE closing (a close sets completedAt to "now",
    // which must not then block the create half of this same fire).
    if (!this.deps.force) {
      const anchor =
        inFlight[0]?.ladderCapturedAt ??
        (target?.phase === 'published' ? target.completedAt : undefined);
      if (this.within(anchor, runStart, this.config.snapshotIntervalHours)) {
        this.log('create: previous snapshot is younger than the snapshot interval — skipping');
        return this.idleSummary('too_recent', target);
      }
      if (
        target?.phase === 'aborted' &&
        this.within(target.abortedAt, runStart, this.config.abortCooldownHours)
      ) {
        this.log('create: abort cooldown still active — skipping');
        return this.idleSummary('cooldown', target);
      }
    }

    // a) Close the previous in-flight snapshot (≤1 by design; loop for safety).
    //    A transform failure inside the close throws: the workflow alerts and
    //    the next create fire resumes the close (phase left `transforming`).
    let closed: CloseSummary | undefined;
    for (const manifest of inFlight) {
      closed = await closeInFlightSnapshot(manifest, {
        clock: this.deps.clock,
        checkpointStore: this.deps.checkpointStore,
        objectStore: this.deps.objectStore,
        finalizerFor: this.deps.finalizerFor,
        log: this.deps.log,
      });
    }

    // b) Create the new snapshot for the configured league.
    const manifest = await this.begin();
    const summary = await this.captureAndSeed(manifest, runStart);
    return closed ? { ...summary, closed } : summary;
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
  ): Promise<CreateSummary> {
    const captured = await this.captureLadder(manifest, runStart);
    if (captured.kind !== 'ok') {
      if (captured.kind === 'aborted') {
        await this.deps.checkpointStore.save({
          ...manifest,
          phase: 'aborted',
          abortedAt: this.nowIso(),
        });
        await this.saveLimiter();
        return { ...this.idleSummary('aborted'), requests: captured.requests };
      }
      // Budget ran out mid-capture: the manifest stays in ladder_capture; the
      // next create fire discards it and recaptures cleanly.
      await this.saveLimiter();
      return { ...this.idleSummary('budget_exhausted'), requests: captured.requests };
    }

    // Every ladder read appends what it saw to the per-league character
    // database. New entrants grow the roster; the first capture seeds it.
    const nowIso = this.nowIso();
    const roster = await this.rosters.load(this.config.league);
    const merged = mergeLadder(roster, captured.entries, nowIso);
    await this.rosters.save(merged.roster);
    this.log(
      `roster: ${merged.roster.characters.length} characters ` +
        `(+${merged.added} new, ${merged.refreshed} refreshed)`,
    );

    // The snapshot queue is the ENTIRE roster, every character entering as
    // `pending`, split into worker-sized chunks. Chunks are re-seeded wholesale
    // if a crash interrupted a previous seed (the manifest only moves to
    // `collecting` after every chunk file is durable).
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
      stopReason: 'created',
      requests: captured.requests,
      rosterSize: merged.roster.characters.length,
      rosterAdded: merged.added,
      totalCharacters: total,
      chunkCount: collecting.chunkCount,
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

  private idleSummary(stopReason: CreateStopReason, target?: SnapshotManifest | undefined) {
    return {
      stopReason,
      requests: 0,
      rosterSize: target?.totalCharacters ?? 0,
      rosterAdded: 0,
      totalCharacters: target?.totalCharacters ?? 0,
      chunkCount: target?.chunkCount ?? 0,
    };
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
}

function defaultSnapshotId(now: number): string {
  return new Date(now).toISOString().replace(/[:.]/g, '-');
}
