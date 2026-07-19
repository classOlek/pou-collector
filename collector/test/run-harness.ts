/**
 * Harness for the coordinator / worker / finalize suites: wires the mock PoE
 * API, in-memory object store and fake clock into the three run classes the
 * way main.ts wires production (fresh limiter per run, restored from the
 * per-slot state — exactly as stateless GH runners would).
 */
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { MemoryObjectStore } from '../src/checkpoint/object-store.js';
import { CheckpointStore } from '../src/checkpoint/store.js';
import { FakeClock } from '../src/rate-limit/clock.js';
import {
  DEFAULT_LIMITER_CONFIG,
  type LimiterConfig,
  RateLimiter,
} from '../src/rate-limit/limiter.js';
import { LegacyCharacterSource, LegacyLadderSource } from '../src/sources/legacy.js';
import type { HttpClient } from '../src/sources/types.js';
import { Coordinator, type CoordinatorSummary } from '../src/run/coordinator.js';
import { SnapshotCreator, type CreateSummary } from '../src/run/create-snapshot.js';
import { Worker, type WorkerSummary } from '../src/run/worker.js';
import { Finalizer, type FinalizeSummary } from '../src/run/finalize.js';
import type { RunConfig } from '../src/run/config.js';
import { CachedTreeSource } from '../src/transform/tree-source.js';
import type { TreeOrigin } from '../src/transform/tree-source.js';
import { FakeTreeOrigin } from './transform-fixtures.js';
import { MockPoeApi, type MockEntry } from './mock-api.js';

export const LEAGUE = 'TestLeague';
const UA = 'poe-ladder-stats/test';

export interface RunHarnessOptions {
  entries: MockEntry[];
  config?: Partial<RunConfig>;
  limiterConfig?: Partial<LimiterConfig>;
  ladderThrottleFirst?: number;
  treeOrigin?: TreeOrigin;
}

export function makeRunHarness(opts: RunHarnessOptions) {
  const clock = new FakeClock(Date.parse('2026-07-17T00:00:00.000Z'));
  const objectStore = new MemoryObjectStore();
  const checkpointStore = new CheckpointStore(objectStore);
  const api = new MockPoeApi({
    league: LEAGUE,
    entries: opts.entries,
    ...(opts.ladderThrottleFirst !== undefined
      ? { ladderThrottleFirst: opts.ladderThrottleFirst }
      : {}),
  });
  const limiterConfig: LimiterConfig = { ...DEFAULT_LIMITER_CONFIG, ...opts.limiterConfig };
  const config: RunConfig = {
    league: LEAGUE,
    depth: opts.entries.length,
    ladderPageSize: 200,
    maxRunMillis: 700_000,
    maxAgeHours: 48,
    maxAttempts: 3,
    chunkSize: 5,
    workerCount: 2,
    snapshotIntervalHours: 12,
    abortCooldownHours: 6,
    ...opts.config,
  };
  const tmpRoot = mkdtempSync(join(tmpdir(), 'pou-run-'));

  const logs: string[] = [];
  const log = (message: string): void => {
    logs.push(message);
  };
  const newLimiter = (): RateLimiter => new RateLimiter(clock, limiterConfig);

  const newCoordinator = (): Coordinator =>
    new Coordinator(
      { league: config.league, workerCount: config.workerCount },
      {
        checkpointStore,
        log,
      },
    );

  const newCreator = (client: HttpClient = api.client, force = false): SnapshotCreator =>
    new SnapshotCreator(config, {
      clock,
      ladderSource: new LegacyLadderSource(client, { userAgent: UA }),
      checkpointStore,
      objectStore,
      limiter: newLimiter(),
      finalizerFor: () => newFinalizer(),
      force,
      newSnapshotId: () => 'snap-fixed',
      log,
    });

  /**
   * A creator seeing a DIFFERENT ladder against the same stores/clock —
   * for tests where the ladder rolls between snapshots (roster growth).
   */
  const newCreatorFor = (
    rolledEntries: MockEntry[],
    snapshotId: string,
    force = false,
  ): SnapshotCreator => {
    const rolledApi = new MockPoeApi({ league: LEAGUE, entries: rolledEntries });
    return new SnapshotCreator(
      { ...config, depth: rolledEntries.length },
      {
        clock,
        ladderSource: new LegacyLadderSource(rolledApi.client, { userAgent: UA }),
        checkpointStore,
        objectStore,
        limiter: newLimiter(),
        finalizerFor: () => newFinalizer(),
        force,
        newSnapshotId: () => snapshotId,
        log,
      },
    );
  };

  const newWorker = (workerIndex: number, client: HttpClient = api.client): Worker =>
    new Worker(
      {
        league: config.league,
        workerIndex,
        workerCount: config.workerCount,
        maxRunMillis: config.maxRunMillis,
        maxAgeHours: config.maxAgeHours,
        maxAttempts: config.maxAttempts,
      },
      {
        clock,
        characterSource: new LegacyCharacterSource(client, { userAgent: UA }),
        checkpointStore,
        objectStore,
        limiter: newLimiter(),
        log,
      },
    );

  const newFinalizer = (): Finalizer =>
    new Finalizer(
      {
        league: config.league,
        maxAgeHours: config.maxAgeHours,
        treeVersion: '3.25-test',
        maxTransformAttempts: 3,
      },
      {
        clock,
        objectStore,
        checkpointStore,
        treeSource: new CachedTreeSource(objectStore, opts.treeOrigin ?? new FakeTreeOrigin()),
        tmpRoot,
        log,
      },
    );

  /** One create-snapshot workflow fire: close previous + create/seed new. */
  const createFire = (force = false): Promise<CreateSummary> =>
    newCreator(api.client, force).runOnce();

  /** One collect workflow fire: coordinate → every worker → finalize. */
  const runCycle = async (): Promise<{
    coordinate: CoordinatorSummary;
    workers: WorkerSummary[];
    finalize: FinalizeSummary;
  }> => {
    const coordinate = await newCoordinator().runOnce();
    const workers: WorkerSummary[] = [];
    for (const index of coordinate.workers) {
      workers.push(await newWorker(index).runOnce());
    }
    const finalize = await newFinalizer().runOnce();
    return { coordinate, workers, finalize };
  };

  return {
    clock,
    objectStore,
    checkpointStore,
    api,
    config,
    logs,
    newCoordinator,
    newCreator,
    newCreatorFor,
    newWorker,
    newFinalizer,
    createFire,
    runCycle,
  };
}

export type RunHarness = ReturnType<typeof makeRunHarness>;

/** Drive full cycles until the snapshot publishes, aborts or idles. */
export async function runToSettle(
  h: RunHarness,
  maxCycles = 30,
): Promise<Awaited<ReturnType<RunHarness['runCycle']>>[]> {
  const cycles: Awaited<ReturnType<RunHarness['runCycle']>>[] = [];
  for (let n = 0; n < maxCycles; n += 1) {
    const cycle = await h.runCycle();
    cycles.push(cycle);
    const settled =
      cycle.finalize.phase === 'published' ||
      cycle.finalize.phase === 'aborted' ||
      (cycle.finalize.phase === 'none' && !cycle.coordinate.hasWork) ||
      cycle.finalize.stopReason === 'idle';
    if (settled) break;
  }
  return cycles;
}

export const entry = (
  name: string,
  behavior: MockEntry['behavior'],
  cls = 'Juggernaut',
): MockEntry => ({
  account: `acct-${name}`,
  character: `char-${name}`,
  class: cls,
  level: 100,
  behavior,
});
