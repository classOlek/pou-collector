/**
 * New-snapshot step — idle-gated, back-to-back snapshots (no wall-clock
 * cadence). The build-roster workflow dispatches a fire after every roster
 * build; the fire seeds a NEW snapshot only when no snapshot is live:
 *
 *  - A LIVE snapshot (phase collecting/transforming) means the collect
 *    workflow is still draining or publishing it — the fire is a clean no-op
 *    (`in_flight`). It never closes live work; the previous snapshot always
 *    finishes naturally before the next one starts.
 *  - A `ladder_capture` remnant is a seed that crashed mid-write (create fires
 *    are serialized by the concurrency group, so it is never a concurrent
 *    seed) — it holds nothing worth publishing and is discarded before
 *    reseeding.
 *  - CREATE seeds the snapshot queue from the ENTIRE current roster (the
 *    growing character database that build-roster maintains), every character
 *    entering as `pending`, split into worker-sized chunks — this is what lets
 *    snapshots grow past the ladder window over time.
 *
 * This step is REQUEST-FREE: it never reads the GGG ladder. All ladder capture
 * and roster merging lives in build-roster.ts; new-snapshot seeds from whatever
 * roster that step last produced. An empty roster (build-roster has not run
 * yet) is a clean skip — nothing is seeded until there are characters.
 *
 * Guards on an unforced fire:
 *  - the idle gate above (`in_flight`);
 *  - abortCooldownHours after an abort (hard rule #1: no failure hammering).
 * A FORCED fire (operator dispatch with force=true) bypasses both: it CLOSES
 * the live snapshot — uncollected characters marked `skipped`, published with
 * what it has (close-snapshot.ts) — and seeds a fresh one immediately.
 *
 * Runs alone in the shared `snapshot-collector` concurrency group, so a close
 * never overlaps a collect fire's workers (single writer per object, no locks).
 */
import type { RosterCharacter, SnapshotManifest } from '@classolek/shared';
import { SCHEMA_VERSION, chunkCountFor, emptyTally, isInFlight } from '@classolek/shared';
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

export type CreateStopReason = 'created' | 'in_flight' | 'cooldown' | 'empty_roster';

/**
 * In-flight snapshots with LIVE work: collecting/transforming, i.e. the collect
 * workflow still owns them. A `ladder_capture` manifest is excluded — it is a
 * crashed seed the next create fire discards and reseeds, not live work (the
 * snapshot-idle check must not report it as busy, or the reseed never fires).
 */
export function liveSnapshots(manifests: readonly SnapshotManifest[]): SnapshotManifest[] {
  return manifests.filter((m) => isInFlight(m.phase) && m.phase !== 'ladder_capture');
}

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

    if (!this.deps.force) {
      // Idle gate: a live snapshot (collecting/transforming, any league) still
      // belongs to the collect workflow — leave it alone and no-op. The next
      // roster-triggered fire re-checks; a new snapshot starts the moment the
      // previous one finishes naturally.
      const live = liveSnapshots(inFlight);
      if (live.length > 0) {
        const s = live[0]!;
        this.log(`create: snapshot ${s.snapshotId} (${s.league}) is still ${s.phase} — skipping`);
        return this.idleSummary('in_flight', target);
      }
      if (
        target?.phase === 'aborted' &&
        this.within(target.abortedAt, runStart, this.config.abortCooldownHours)
      ) {
        this.log('create: abort cooldown still active — skipping');
        return this.idleSummary('cooldown', target);
      }
    }

    // Close whatever this fire may touch: a FORCED fire closes live snapshots
    // (skipped-marking + publish); an unforced fire reaches here only with
    // `ladder_capture` remnants, which close as a discard. A transform failure
    // inside a forced close throws (workflow alerts, phase left `transforming`);
    // recovery needs no operator: with every chunk resolved the collect
    // workflow's next finalize retries the transform and publishes.
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
