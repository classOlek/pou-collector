/**
 * New-snapshot step — its own workflow, whose cron IS the snapshot cadence
 * (the collect workflow only drains what this one seeds; the build-roster
 * workflow keeps the roster it seeds from fresh):
 *
 *  a) CLOSE the previous in-flight snapshot: uncollected characters are marked
 *     `skipped` and the snapshot publishes with what it has (close-snapshot.ts);
 *  b) CREATE the new snapshot: seed the snapshot queue from the ENTIRE current
 *     roster (the growing character database that build-roster maintains), every
 *     character entering as `pending`, split into worker-sized chunks — this is
 *     what lets snapshots grow past the ladder window over time.
 *
 * This step is REQUEST-FREE: it never reads the GGG ladder. All ladder capture
 * and roster merging lives in build-roster.ts; new-snapshot seeds from whatever
 * roster that step last produced. An empty roster (build-roster has not run
 * yet) is a clean skip — nothing is seeded until there are characters.
 *
 * Scheduled fires respect two guards, both bypassed by a dispatch fire (force):
 *  - snapshotIntervalHours since the previous snapshot began (or completed):
 *    double-fire / misconfigured-cron protection;
 *  - abortCooldownHours after an abort (hard rule #1: no failure hammering).
 *
 * Runs alone in the shared `snapshot-collector` concurrency group, so a close
 * never overlaps a collect fire's workers (single writer per object, no locks).
 */
import type { RosterCharacter, SnapshotManifest } from '@pou/shared';
import { SCHEMA_VERSION, chunkCountFor, emptyTally, isInFlight } from '@pou/shared';
import type { Clock } from '../rate-limit/clock.js';
import type { CheckpointStore } from '../checkpoint/store.js';
import type { ObjectStore } from '../checkpoint/object-store.js';
import { RosterStore } from '../roster/roster-store.js';
import { ChunkStore, planChunks } from '../chunks/chunk-store.js';
import { HOUR_MS, type RunConfig } from './config.js';
import type { Finalizer } from './finalize.js';
import { closeInFlightSnapshot, type CloseSummary } from './close-snapshot.js';

export interface CreatorDeps {
  clock: Clock;
  checkpointStore: CheckpointStore;
  objectStore: ObjectStore;
  /** Finalizer for a league being closed (carries that league's tree version). */
  finalizerFor: (league: string) => Finalizer;
  /** Bypass the cadence guards (workflow_dispatch fires). */
  force?: boolean;
  /** Injected id generator so snapshot ids are deterministic in tests. */
  newSnapshotId?: (now: number) => string;
  log?: (message: string) => void;
}

export type CreateStopReason = 'created' | 'too_recent' | 'cooldown' | 'empty_roster';

export interface CreateSummary {
  stopReason: CreateStopReason;
  /** What closing the previous snapshot did (absent when nothing was in flight). */
  closed?: CloseSummary;
  rosterSize: number;
  totalCharacters: number;
  chunkCount: number;
}

export class SnapshotCreator {
  private readonly rosters: RosterStore;
  private readonly chunks: ChunkStore;

  constructor(
    private readonly config: RunConfig,
    private readonly deps: CreatorDeps,
  ) {
    this.rosters = new RosterStore(deps.objectStore);
    this.chunks = new ChunkStore(deps.objectStore);
  }

  private log(message: string): void {
    this.deps.log?.(message);
  }

  async runOnce(): Promise<CreateSummary> {
    const runStart = this.deps.clock.now();

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

    // b) Seed the new snapshot from the current roster. An empty roster means
    //    build-roster has not populated it yet — skip cleanly (leaving the just
    //    closed snapshot as the newest) rather than churn an empty snapshot.
    const roster = await this.rosters.load(this.config.league);
    if (roster.characters.length === 0) {
      this.log('create: roster is empty — nothing to seed yet');
      const summary: CreateSummary = {
        stopReason: 'empty_roster',
        rosterSize: 0,
        totalCharacters: 0,
        chunkCount: 0,
      };
      return closed ? { ...summary, closed } : summary;
    }

    const manifest = await this.begin();
    const summary = await this.seedFromRoster(manifest, roster.characters);
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
      // The moment this snapshot began; anchors both the age hard-block and the
      // interval guard. (No ladder is captured here — build-roster owns that.)
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

  /** Seed the whole roster as pending chunks and move the manifest to collecting. */
  private async seedFromRoster(
    manifest: SnapshotManifest,
    characters: readonly RosterCharacter[],
  ): Promise<CreateSummary> {
    const total = characters.length;

    // The snapshot queue is the ENTIRE roster, every character entering as
    // `pending`, split into worker-sized chunks. Chunks are re-seeded wholesale
    // if a crash interrupted a previous seed (the manifest only moves to
    // `collecting` after every chunk file is durable).
    await this.chunks.deleteAll(this.config.league, manifest.snapshotId);
    const chunks = planChunks(
      this.config.league,
      manifest.snapshotId,
      characters,
      this.config.chunkSize,
    );
    for (const chunk of chunks) await this.chunks.save(chunk);

    const collecting: SnapshotManifest = {
      ...manifest,
      phase: 'collecting',
      chunkCount: chunkCountFor(total, this.config.chunkSize),
      totalCharacters: total,
      outcomes: { ...emptyTally(), pending: total },
      resolvedChunks: 0,
    };
    await this.deps.checkpointStore.save(collecting);
    this.log(
      `seed: snapshot=${collecting.snapshotId} characters=${total} ` +
        `chunks=${collecting.chunkCount} (size ${this.config.chunkSize})`,
    );
    return {
      stopReason: 'created',
      rosterSize: total,
      totalCharacters: total,
      chunkCount: collecting.chunkCount,
    };
  }

  private idleSummary(stopReason: CreateStopReason, target?: SnapshotManifest | undefined) {
    return {
      stopReason,
      rosterSize: target?.totalCharacters ?? 0,
      totalCharacters: target?.totalCharacters ?? 0,
      chunkCount: target?.chunkCount ?? 0,
    };
  }

  private within(iso: string | undefined, now: number, hours: number): boolean {
    if (iso === undefined) return false;
    return now - Date.parse(iso) < hours * HOUR_MS;
  }
}

function defaultSnapshotId(now: number): string {
  return new Date(now).toISOString().replace(/[:.]/g, '-');
}
