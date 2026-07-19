#!/usr/bin/env tsx
/**
 * Collector CLI. One entry point, five subcommands matching the two workflows'
 * jobs, wired from the environment and the checked-in config files:
 *
 *   create-snapshot — the create workflow (its cron IS the snapshot cadence):
 *                close the previous in-flight snapshot (uncollected characters
 *                marked `skipped`, snapshot published with what it has), then
 *                capture the ladder, merge it into the per-league roster and
 *                seed the new snapshot's pending chunks.
 *                COLLECTOR_RESET_ABORTED=true clears aborted checkpoints first;
 *                COLLECTOR_FORCE_CREATE=true (dispatch) bypasses cadence guards.
 *   coordinate — the collect workflow's request-free check: report whether the
 *                newest snapshot still has uncollected characters
 *                (`has_work` / `workers` outputs). Resumes an in-flight league
 *                first (a dispatch-created league is never stranded).
 *   work       — one parallel worker (WORKER_INDEX from the job matrix):
 *                resolves the pending chunks that slot owns this run. Exits 0
 *                on resumable stops (budget / rate-limit) — the next fire
 *                continues.
 *   finalize   — roll chunk outcomes up, publish the collected-so-far data as
 *                an incomplete snapshot (or the final immutable snapshot once
 *                every chunk resolved), then run retention.
 *   retention  — usage accounting, orphaned-raw sweep, trim oldest detail.
 *
 * All GGG/R2 access is injected (systemClock, real-fetch HttpClient, S3
 * ObjectStore from R2_* env). The contact email is required (hard rule #1):
 * buildUserAgent throws without COLLECTOR_CONTACT_EMAIL — intended, so a
 * misconfigured workflow fails loudly instead of shipping an anonymous UA.
 */
import { buildUserAgent } from './config.js';
import { loadConfig, treeVersionFor, type CollectorConfig } from './config-file.js';
import { selectCollectLeague } from './select-league.js';
import { systemClock } from './rate-limit/clock.js';
import { DEFAULT_LIMITER_CONFIG, RateLimiter } from './rate-limit/limiter.js';
import { CheckpointStore } from './checkpoint/store.js';
import { S3ObjectStore } from './checkpoint/s3-store.js';
import type { ObjectStore } from './checkpoint/object-store.js';
import { createFetchHttpClient } from './http/fetch-client.js';
import { LegacyCharacterSource, LegacyLadderSource } from './sources/legacy.js';
import { Coordinator } from './run/coordinator.js';
import { SnapshotCreator } from './run/create-snapshot.js';
import { Worker } from './run/worker.js';
import { Finalizer } from './run/finalize.js';
import { CachedTreeSource } from './transform/tree-source.js';
import { HttpTreeOrigin } from './transform/tree-origin.js';
import { runRetention } from './retention/retention.js';
import { resetAbortedCheckpoints, shouldResetAborted } from './reset-aborted.js';
import {
  coordinateExitCode,
  createExitCode,
  emitSummary,
  finalizeExitCode,
  renderCoordinateSummary,
  renderCreateSummary,
  renderFinalizeSummary,
  renderRetentionSummary,
  renderWorkerSummary,
} from './run-summary.js';

function requireEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function makeObjectStore(): ObjectStore {
  return new S3ObjectStore({
    bucket: requireEnv('R2_BUCKET'),
    endpoint: requireEnv('R2_ENDPOINT'),
    accessKeyId: requireEnv('R2_ACCESS_KEY_ID'),
    secretAccessKey: requireEnv('R2_SECRET_ACCESS_KEY'),
  });
}

function makeFinalizerFactory(
  config: CollectorConfig,
  store: ObjectStore,
  checkpointStore: CheckpointStore,
): (league: string) => Finalizer {
  const treeSource = new CachedTreeSource(
    store,
    new HttpTreeOrigin(createFetchHttpClient(), {
      treeUrl: config.treeUrl,
      userAgent: buildUserAgent(),
    }),
  );
  return (league) =>
    new Finalizer(
      {
        league,
        maxAgeHours: config.maxAgeHours,
        treeVersion: treeVersionFor(config.leagues, league),
        maxTransformAttempts: config.maxTransformAttempts,
      },
      {
        clock: systemClock,
        objectStore: store,
        checkpointStore,
        treeSource,
        log: (message) => console.log(`[finalize] ${message}`),
      },
    );
}

async function createSnapshot(config: CollectorConfig, store: ObjectStore): Promise<number> {
  const userAgent = buildUserAgent();
  const http = createFetchHttpClient();
  const checkpointStore = new CheckpointStore(store);
  if (shouldResetAborted(process.env['COLLECTOR_RESET_ABORTED'])) {
    const cleared = await resetAbortedCheckpoints(checkpointStore);
    console.log(JSON.stringify({ kind: 'reset_aborted', cleared }));
  }
  const limiter = new RateLimiter(systemClock, DEFAULT_LIMITER_CONFIG);
  const creator = new SnapshotCreator(config, {
    clock: systemClock,
    ladderSource: new LegacyLadderSource(http, { userAgent }),
    checkpointStore,
    objectStore: store,
    limiter,
    finalizerFor: makeFinalizerFactory(config, store, checkpointStore),
    // Dispatch fires bypass the cadence guards; scheduled fires respect them.
    force: process.env['COLLECTOR_FORCE_CREATE'] === 'true',
    log: (message) => console.log(`[create] ${message}`),
  });

  const summary = await creator.runOnce();
  emitSummary(renderCreateSummary(summary, limiter.toMemory()));
  return createExitCode(summary);
}

async function coordinate(config: CollectorConfig, store: ObjectStore): Promise<number> {
  const checkpointStore = new CheckpointStore(store);
  const league = await selectCollectLeague(checkpointStore, config.league);
  const coordinator = new Coordinator(
    { league, workerCount: config.workerCount },
    {
      checkpointStore,
      log: (message) => console.log(`[coordinate] ${message}`),
    },
  );

  const summary = await coordinator.runOnce();
  emitSummary(renderCoordinateSummary(summary));
  return coordinateExitCode();
}

async function work(config: CollectorConfig, store: ObjectStore): Promise<number> {
  const userAgent = buildUserAgent();
  const http = createFetchHttpClient();
  const checkpointStore = new CheckpointStore(store);
  const league = await selectCollectLeague(checkpointStore, config.league);
  const workerIndex = Number.parseInt(requireEnv('WORKER_INDEX'), 10);
  if (!Number.isInteger(workerIndex) || workerIndex < 0 || workerIndex >= config.workerCount) {
    throw new Error(`WORKER_INDEX must be in [0, ${config.workerCount}): got ${workerIndex}`);
  }
  const limiter = new RateLimiter(systemClock, DEFAULT_LIMITER_CONFIG);
  const worker = new Worker(
    {
      league,
      workerIndex,
      workerCount: config.workerCount,
      maxRunMillis: config.maxRunMillis,
      maxAgeHours: config.maxAgeHours,
      maxAttempts: config.maxAttempts,
    },
    {
      clock: systemClock,
      characterSource: new LegacyCharacterSource(http, { userAgent }),
      checkpointStore,
      objectStore: store,
      limiter,
      log: (message) => console.log(`[work w${workerIndex}] ${message}`),
    },
  );

  const summary = await worker.runOnce();
  emitSummary(renderWorkerSummary(summary, limiter.toMemory()));
  // Workers never fail the workflow for resumable stops: budget and rate-limit
  // stops are normal, and finalize/next runs pick the remaining chunks up.
  return 0;
}

async function finalize(config: CollectorConfig, store: ObjectStore): Promise<number> {
  const checkpointStore = new CheckpointStore(store);
  // Resolved for the league actually being finalized (which may be an
  // in-flight league, not config.league); unmapped league → loud failure.
  const league = await selectCollectLeague(checkpointStore, config.league);
  const finalizer = makeFinalizerFactory(config, store, checkpointStore)(league);

  const summary = await finalizer.runOnce();
  emitSummary(renderFinalizeSummary(summary));
  await retention(config, store);
  return finalizeExitCode(summary);
}

async function retention(config: CollectorConfig, store: ObjectStore): Promise<number> {
  const summary = await runRetention(
    { budgetBytes: config.retentionBudgetBytes, keepRecentDetail: config.keepRecentDetail },
    { clock: systemClock, objectStore: store, checkpointStore: new CheckpointStore(store) },
  );
  emitSummary(renderRetentionSummary(summary));
  return 0;
}

const HANDLERS: Record<string, (cfg: CollectorConfig, store: ObjectStore) => Promise<number>> = {
  'create-snapshot': createSnapshot,
  coordinate,
  work,
  finalize,
  retention,
};

async function main(): Promise<number> {
  const command = process.argv[2] ?? '';
  const handler = HANDLERS[command];
  if (!handler) {
    throw new Error(`unknown command "${command}" (expected ${Object.keys(HANDLERS).join(' | ')})`);
  }
  return handler(loadConfig(), makeObjectStore());
}

main()
  .then((code) => {
    process.exitCode = code;
  })
  .catch((err: unknown) => {
    console.error(err instanceof Error ? (err.stack ?? err.message) : String(err));
    process.exitCode = 1;
  });
