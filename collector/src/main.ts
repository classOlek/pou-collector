#!/usr/bin/env tsx
/**
 * Collector CLI. One entry point, one subcommand per workflow job, wired from
 * the environment and the checked-in config files:
 *
 *   build-roster — the build-roster workflow (every 30 min): one atomic ladder
 *                capture merged into the per-league roster (the growing
 *                character database). The ONLY step that reads the GGG ladder.
 *   create-snapshot — the new-snapshot workflow (dispatched by build-roster
 *                after each roster build): idle-gated — seed the new snapshot's
 *                pending chunks from the CURRENT roster, but only when no
 *                snapshot is live; otherwise a clean no-op. Request-free — no
 *                ladder capture here.
 *                COLLECTOR_RESET_ABORTED=true clears aborted checkpoints first;
 *                COLLECTOR_FORCE_CREATE=true (operator dispatch) closes the
 *                live snapshot (uncollected marked `skipped`, published with
 *                what it has) and seeds a fresh one immediately.
 *   snapshot-idle — request-free check the build-roster workflow runs to decide
 *                whether to dispatch a new-snapshot fire (`idle` output).
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
 *   economy    — the ECONOMY POE.NINJA workflow (hourly): cache every
 *                documented poe.ninja poe1 economy category into one file per
 *                league at economy/<league>.json. Talks to poe.ninja only —
 *                never GGG — and needs no collector config beyond the contact
 *                email for the User-Agent.
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
import { discoverPublicIp } from './http/public-ip.js';
import { LegacyCharacterSource, LegacyLadderSource } from './sources/legacy.js';
import { Coordinator } from './run/coordinator.js';
import { RosterBuilder } from './run/build-roster.js';
import { SnapshotCreator, liveSnapshots } from './run/create-snapshot.js';
import { Worker } from './run/worker.js';
import { Finalizer } from './run/finalize.js';
import { CachedTreeSource } from './transform/tree-source.js';
import { HttpTreeOrigin } from './transform/tree-origin.js';
import { runRetention } from './retention/retention.js';
import { EconomyCollector } from './economy/poe-ninja.js';
import { fetchEconomyLeagues } from './economy/leagues-source.js';
import { resetAbortedCheckpoints, shouldResetAborted } from './reset-aborted.js';
import {
  buildExitCode,
  coordinateExitCode,
  createExitCode,
  economyExitCode,
  emitSummary,
  finalizeExitCode,
  renderBuildSummary,
  renderCoordinateSummary,
  renderCreateSummary,
  renderEconomySummary,
  renderFinalizeSummary,
  renderRetentionSummary,
  renderSnapshotIdleSummary,
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
        paceFileTtlHours: config.paceFileTtlHours,
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

async function buildRoster(config: CollectorConfig, store: ObjectStore): Promise<number> {
  const userAgent = buildUserAgent();
  const http = createFetchHttpClient();
  const limiter = new RateLimiter(systemClock, DEFAULT_LIMITER_CONFIG);
  const builder = new RosterBuilder(config, {
    clock: systemClock,
    ladderSource: new LegacyLadderSource(http, { userAgent }),
    objectStore: store,
    limiter,
    publicIp: await discoverPublicIp(),
    log: (message) => console.log(`[build] ${message}`),
  });

  const summary = await builder.runOnce();
  emitSummary(renderBuildSummary(summary, limiter.toMemory()));
  return buildExitCode(summary);
}

async function createSnapshot(config: CollectorConfig, store: ObjectStore): Promise<number> {
  const checkpointStore = new CheckpointStore(store);
  if (shouldResetAborted(process.env['COLLECTOR_RESET_ABORTED'])) {
    const cleared = await resetAbortedCheckpoints(checkpointStore);
    console.log(JSON.stringify({ kind: 'reset_aborted', cleared }));
  }
  // Request-free: seeds from the roster build-roster maintains — no GGG access,
  // no rate limiter, no public-IP discovery.
  const creator = new SnapshotCreator(config, {
    clock: systemClock,
    checkpointStore,
    objectStore: store,
    finalizerFor: makeFinalizerFactory(config, store, checkpointStore),
    // Dispatch fires bypass the cadence guards; scheduled fires respect them.
    force: process.env['COLLECTOR_FORCE_CREATE'] === 'true',
    log: (message) => console.log(`[create] ${message}`),
  });

  const summary = await creator.runOnce();
  emitSummary(renderCreateSummary(summary));
  return createExitCode();
}

/**
 * Request-free pre-check for the build-roster workflow: report whether any
 * snapshot is still live (collecting/transforming, any league) so the workflow
 * only dispatches a new-snapshot fire that would actually seed. The check is
 * advisory — the creator re-checks under the concurrency group — but it keeps
 * pointless new-snapshot runs from occupying the group's single pending slot.
 */
async function snapshotIdle(_config: CollectorConfig, store: ObjectStore): Promise<number> {
  const checkpointStore = new CheckpointStore(store);
  const live = liveSnapshots(await checkpointStore.listAll());
  emitSummary(
    renderSnapshotIdleSummary({
      idle: live.length === 0,
      live: live.map((m) => ({ league: m.league, snapshotId: m.snapshotId, phase: m.phase })),
    }),
  );
  return 0;
}

async function coordinate(config: CollectorConfig, store: ObjectStore): Promise<number> {
  const checkpointStore = new CheckpointStore(store);
  const league = await selectCollectLeague(checkpointStore, config.league);
  const coordinator = new Coordinator(
    {
      league,
      workerCount: config.workerCount,
      maxWaitMillis: config.maxWaitMillis,
      collectCooldownMillis: config.collectCooldownMinutes * 60_000,
    },
    {
      checkpointStore,
      objectStore: store,
      clock: systemClock,
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
  // Scope the restored pace state to this runner's IP: GGG enforces the
  // windows per IP and every hosted runner gets a fresh one, so a stale spend
  // must not self-throttle this run (penalties still carry — see adoptIp).
  const publicIp = await discoverPublicIp();
  const worker = new Worker(
    {
      league,
      workerIndex,
      workerCount: config.workerCount,
      maxRunMillis: config.maxRunMillis,
      maxWaitMillis: config.maxWaitMillis,
      maxAgeHours: config.maxAgeHours,
      maxAttempts: config.maxAttempts,
      // GITHUB_RUN_ID scopes the early-stop done markers to one workflow fire
      // (all matrix jobs of a fire share it). Absent (local run) → inert.
      runId: process.env['GITHUB_RUN_ID']?.trim() ?? '',
      earlyStopQuorum: config.earlyStopQuorum,
    },
    {
      clock: systemClock,
      characterSource: new LegacyCharacterSource(http, { userAgent }),
      checkpointStore,
      objectStore: store,
      limiter,
      publicIp,
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

async function economy(_config: CollectorConfig, store: ObjectStore): Promise<number> {
  // Which leagues to cache is derived from the Supabase leagues endpoint
  // (active temporary leagues + Standard) rather than hardcoded; a failure to
  // resolve it fails the run loudly so the alert job fires.
  const leagues = await fetchEconomyLeagues(process.env, {
    http: createFetchHttpClient(),
    now: () => systemClock.now(),
  });
  console.log(`[economy] leagues from endpoint: ${JSON.stringify(leagues)}`);

  const collector = new EconomyCollector(
    { userAgent: buildUserAgent(), leagues },
    {
      clock: systemClock,
      http: createFetchHttpClient(),
      objectStore: store,
      log: (message) => console.log(`[economy] ${message}`),
    },
  );

  const summary = await collector.runOnce();
  emitSummary(renderEconomySummary(summary));
  return economyExitCode(summary);
}

const HANDLERS: Record<string, (cfg: CollectorConfig, store: ObjectStore) => Promise<number>> = {
  'build-roster': buildRoster,
  'create-snapshot': createSnapshot,
  'snapshot-idle': snapshotIdle,
  coordinate,
  work,
  finalize,
  retention,
  economy,
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
