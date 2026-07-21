/**
 * Machine- and human-readable run summaries for the GitHub Actions workflow.
 *
 * Each `render*` function is pure — it turns a result into a Markdown block (for
 * `$GITHUB_STEP_SUMMARY`) plus key/value outputs (for `$GITHUB_OUTPUT`, which the
 * workflow reads to decide whether to fan the worker matrix out). `emitSummary`
 * does the IO: appends the Markdown, appends the outputs, and prints one JSON
 * line to stdout so a run is greppable in the logs.
 */
import { appendFileSync } from 'node:fs';
import type { LimiterMemory } from '@pou/shared';
import type { CoordinatorSummary } from './run/coordinator.js';
import type { BuildSummary } from './run/build-roster.js';
import type { CreateSummary } from './run/create-snapshot.js';
import type { WorkerSummary } from './run/worker.js';
import type { FinalizeSummary } from './run/finalize.js';
import type { TransformSummary } from './transform/transform.js';
import type { RetentionSummary } from './retention/retention.js';
import type { EconomySummary } from './economy/poe-ninja.js';

/**
 * The coordinate→worker hand-off contract, in one place. snapshot.yml fans the
 * worker matrix out on `steps.coordinate.outputs.<HAS_WORK_OUTPUT_KEY> ==
 * '<HAS_WORK_TRUE>'` with `fromJSON(steps.coordinate.outputs.<WORKERS_OUTPUT_KEY>)`
 * as the matrix; a run-summary test asserts these exact keys, so renaming any of
 * them fails a test rather than silently never fanning workers out.
 */
export const HAS_WORK_OUTPUT_KEY = 'has_work';
export const HAS_WORK_TRUE = 'true';
export const WORKERS_OUTPUT_KEY = 'workers';

/**
 * Exit policy for the collect workflow's coordinate step: it only reads the
 * manifest and reports the fan-out, so it always exits clean — failures here
 * are thrown errors (R2 unreachable), not summary states.
 */
export function coordinateExitCode(): number {
  return 0;
}

/**
 * Exit policy for the build-roster step: fail only when THIS run aborted its
 * ladder capture (hard block / a page failing past maxAttempts). A
 * budget-exhausted capture is a clean exit — the next fire recaptures.
 */
export function buildExitCode(summary: BuildSummary): number {
  return summary.stopReason === 'aborted' ? 1 : 0;
}

/**
 * Exit policy for the new-snapshot step: always clean. The step is request-free
 * (it seeds from the roster), so it has no abort of its own; cadence skips and
 * an empty roster are expected no-ops, and a close-transform failure throws
 * before a summary exists and fails the job on its own.
 */
export function createExitCode(): number {
  return 0;
}

/**
 * Exit policy for the finalize step: fail only on a real abort (max-age hard
 * block or final-transform attempts exhausted). A drained snapshot with zero
 * public profiles is a clean, expected abort; a failed INCREMENTAL publish is
 * only a warning (collection continues and the final transform is the gate).
 */
export function finalizeExitCode(summary: FinalizeSummary): number {
  return summary.stopReason === 'aborted' ? 1 : 0;
}

/**
 * Exit policy for the ECONOMY POE.NINJA step: fail when any league's pass fell
 * short of a complete snapshot (its file is all-or-nothing, so the last good
 * cache was kept) — the alert job surfaces it and the next fire retries.
 */
export function economyExitCode(summary: EconomySummary): number {
  return summary.leagues.every((league) => league.written) ? 0 : 1;
}

export interface RenderedSummary {
  markdown: string;
  outputs: Record<string, string>;
  /** Compact object logged as one JSON line to stdout. */
  json: Record<string, unknown>;
}

function table(headers: string[], rows: (string | number)[][]): string {
  const head = `| ${headers.join(' | ')} |`;
  const sep = `| ${headers.map(() => '---').join(' | ')} |`;
  const body = rows.map((r) => `| ${r.join(' | ')} |`).join('\n');
  return [head, sep, body].join('\n');
}

/** Observed X-Rate-Limit rules from limiter memory (Phase 2: measure, don't guess). */
export function renderObservedLimits(memory: LimiterMemory): string {
  if (memory.observedRules.length === 0) return '_No X-Rate-Limit-* headers observed this run._';
  const rows = memory.observedRules.map((rule) => [
    rule.name,
    rule.limits.map((t) => `${t.hits}:${t.periodSec}:${t.penaltySec}`).join(', ') || '—',
    rule.state.map((t) => `${t.hits}:${t.periodSec}:${t.penaltySec}`).join(', ') || '—',
  ]);
  return table(['rule', 'limit (hits:period:penalty)', 'state'], rows);
}

export function renderCoordinateSummary(summary: CoordinatorSummary): RenderedSummary {
  const blocks = [
    '## Coordinate run',
    '',
    table(
      ['phase', 'stop reason', 'has work', 'workers', 'pending', 'total characters', 'chunks'],
      [
        [
          summary.phase,
          summary.stopReason,
          String(summary.hasWork),
          summary.workers.length,
          summary.pendingCount,
          summary.totalCharacters,
          summary.chunkCount,
        ],
      ],
    ),
  ];
  if (summary.blockedUntil !== undefined) {
    // Answer "why is nothing collecting?" from the run page: which politeness
    // gate skipped the wave and when the next fire can be productive.
    const until = new Date(summary.blockedUntil).toISOString();
    blocks.push('', `_Wave skipped (${summary.stopReason}); next eligible fire after ${until}._`);
  }

  return {
    markdown: blocks.join('\n'),
    outputs: {
      [HAS_WORK_OUTPUT_KEY]: summary.hasWork ? HAS_WORK_TRUE : 'false',
      [WORKERS_OUTPUT_KEY]: JSON.stringify(summary.workers),
      phase: summary.phase,
      stop_reason: summary.stopReason,
      // Epoch ms a closed gate reopens (absent when no gate blocked). No job
      // consumes it anymore (the scheduler Worker paces fires now); kept as a
      // diagnostic — the job summary shows when the next fire can be productive.
      ...(summary.blockedUntil !== undefined
        ? { blocked_until: String(summary.blockedUntil) }
        : {}),
    },
    json: {
      kind: 'coordinate',
      phase: summary.phase,
      stopReason: summary.stopReason,
      hasWork: summary.hasWork,
      workers: summary.workers,
      pendingCount: summary.pendingCount,
      totalCharacters: summary.totalCharacters,
      chunkCount: summary.chunkCount,
      ...(summary.blockedUntil !== undefined ? { blockedUntil: summary.blockedUntil } : {}),
    },
  };
}

export function renderBuildSummary(summary: BuildSummary, memory: LimiterMemory): RenderedSummary {
  const markdown = [
    '## Build roster run',
    '',
    table(
      ['stop reason', 'requests', 'roster size', 'added', 'refreshed'],
      [
        [
          summary.stopReason,
          summary.requests,
          summary.rosterSize,
          summary.rosterAdded,
          summary.rosterRefreshed,
        ],
      ],
    ),
    '',
    '### Observed rate limits',
    renderObservedLimits(memory),
  ].join('\n');

  return {
    markdown,
    outputs: { stop_reason: summary.stopReason },
    json: {
      kind: 'build_roster',
      stopReason: summary.stopReason,
      requests: summary.requests,
      rosterSize: summary.rosterSize,
      rosterAdded: summary.rosterAdded,
      rosterRefreshed: summary.rosterRefreshed,
      penaltyUntil: memory.penaltyUntil,
    },
  };
}

export function renderCreateSummary(summary: CreateSummary): RenderedSummary {
  const closedRow = summary.closed
    ? [
        [
          summary.closed.league,
          summary.closed.snapshotId,
          summary.closed.result,
          summary.closed.skippedMarked,
        ],
      ]
    : [];
  const markdown = [
    '## New snapshot run',
    '',
    table(['stop reason'], [[summary.stopReason]]),
    '',
    '### Closed previous snapshot',
    summary.closed
      ? table(['league', 'snapshot', 'result', 'marked skipped'], closedRow)
      : '_No in-flight snapshot to close._',
    '',
    '### Seeded snapshot queue',
    table(
      ['roster size', 'total characters', 'chunks'],
      [[summary.rosterSize, summary.totalCharacters, summary.chunkCount]],
    ),
  ].join('\n');

  return {
    markdown,
    outputs: {
      stop_reason: summary.stopReason,
      ...(summary.closed
        ? {
            closed_snapshot: summary.closed.snapshotId,
            closed_result: summary.closed.result,
            marked_skipped: String(summary.closed.skippedMarked),
          }
        : {}),
    },
    json: {
      kind: 'new_snapshot',
      stopReason: summary.stopReason,
      closed: summary.closed,
      rosterSize: summary.rosterSize,
      totalCharacters: summary.totalCharacters,
      chunkCount: summary.chunkCount,
    },
  };
}

/**
 * The build-roster→new-snapshot hand-off contract: build-roster.yml dispatches
 * a new-snapshot fire only when `steps.<idle step>.outputs.<SNAPSHOT_IDLE_OUTPUT_KEY>
 * == 'true'` — i.e. no live snapshot exists, so the fire would actually seed
 * instead of no-op (a queued no-op run would still hold the shared concurrency
 * group's single pending slot and starve collect fires). A run-summary test
 * asserts the exact key, so renaming it fails a test rather than silently
 * never triggering a snapshot again.
 */
export const SNAPSHOT_IDLE_OUTPUT_KEY = 'idle';

export interface SnapshotIdleSummary {
  idle: boolean;
  /** The live snapshots blocking creation (empty when idle). */
  live: { league: string; snapshotId: string; phase: string }[];
}

export function renderSnapshotIdleSummary(summary: SnapshotIdleSummary): RenderedSummary {
  const markdown = [
    '## Snapshot idle check',
    '',
    summary.idle
      ? '_No live snapshot — a new-snapshot fire will seed._'
      : table(
          ['league', 'snapshot', 'phase'],
          summary.live.map((s) => [s.league, s.snapshotId, s.phase]),
        ),
  ].join('\n');

  return {
    markdown,
    outputs: { [SNAPSHOT_IDLE_OUTPUT_KEY]: String(summary.idle) },
    json: { kind: 'snapshot_idle', idle: summary.idle, live: summary.live },
  };
}

export function renderWorkerSummary(
  summary: WorkerSummary,
  memory: LimiterMemory,
): RenderedSummary {
  const o = summary.outcomes;
  const markdown = [
    `## Worker w${summary.workerIndex}`,
    '',
    table(
      ['stop reason', 'assigned chunks', 'chunks resolved', 'requests', 'shards'],
      [
        [
          summary.stopReason,
          summary.assignedChunks,
          summary.chunksResolved,
          summary.requests,
          summary.shardsWritten,
        ],
      ],
    ),
    '',
    '### Outcomes across touched chunks',
    table(
      ['ok', 'private', 'dead', 'retryable', 'pending', 'skipped'],
      [[o.ok, o.private, o.dead, o.retryable, o.pending, o.skipped]],
    ),
    '',
    '### Observed rate limits',
    renderObservedLimits(memory),
  ].join('\n');

  return {
    markdown,
    outputs: {
      stop_reason: summary.stopReason,
      requests: String(summary.requests),
      chunks_resolved: String(summary.chunksResolved),
    },
    json: {
      kind: 'worker',
      workerIndex: summary.workerIndex,
      stopReason: summary.stopReason,
      assignedChunks: summary.assignedChunks,
      chunksResolved: summary.chunksResolved,
      requests: summary.requests,
      shardsWritten: summary.shardsWritten,
      outcomes: summary.outcomes,
      penaltyUntil: memory.penaltyUntil,
    },
  };
}

export function renderFinalizeSummary(summary: FinalizeSummary): RenderedSummary {
  const o = summary.outcomes;
  const blocks = [
    '## Finalize run',
    '',
    table(
      ['phase', 'stop reason', 'chunks resolved', 'chunks total'],
      [[summary.phase, summary.stopReason, summary.resolvedChunks, summary.chunkCount]],
    ),
    '',
    '### Outcomes',
    table(
      ['ok', 'private', 'dead', 'retryable', 'pending', 'skipped'],
      [[o.ok, o.private, o.dead, o.retryable, o.pending, o.skipped]],
    ),
  ];
  if (summary.transform) blocks.push('', renderTransformBlock(summary.transform));

  return {
    markdown: blocks.join('\n'),
    outputs: {
      phase: summary.phase,
      stop_reason: summary.stopReason,
      ...(summary.transform
        ? {
            snapshot_id: summary.transform.snapshotId,
            characters: String(summary.transform.characterCount),
            complete: String(summary.transform.complete),
          }
        : {}),
    },
    json: { kind: 'finalize', ...summary },
  };
}

function renderTransformBlock(summary: TransformSummary): string {
  return [
    `### ${summary.complete ? 'Final' : 'Incremental'} publish`,
    '',
    table(
      [
        'snapshot',
        'complete',
        'characters',
        'ok',
        'private',
        'dead',
        'pending',
        'skipped',
        'raw deleted',
      ],
      [
        [
          summary.snapshotId,
          String(summary.complete),
          summary.characterCount,
          summary.coverage.ok,
          summary.coverage.private,
          summary.coverage.dead,
          summary.pendingCount,
          summary.skippedCount,
          summary.rawShardsDeleted,
        ],
      ],
    ),
    '',
    '#### Detail Parquet (bytes)',
    table(Object.keys(summary.detailBytes), [Object.values(summary.detailBytes)]),
    '',
    '#### Aggregate rows',
    table(Object.keys(summary.aggregateRows), [Object.values(summary.aggregateRows)]),
  ].join('\n');
}

export function renderEconomySummary(summary: EconomySummary): RenderedSummary {
  const rows = summary.leagues.map((league) => [
    league.league,
    league.categoriesFetched,
    league.failures.length,
    league.written ? 'written' : 'kept last good file',
  ]);
  const blocks = [
    '## Economy poe.ninja cache',
    '',
    table(['league', 'categories fetched', 'failures', 'economy file'], rows),
    '',
    `_${summary.requestCount} requests this pass._`,
  ];
  const failures = summary.leagues.flatMap((league) =>
    league.failures.map((f) => `- ${league.league}: ${f.endpoint}/${f.type} — ${f.reason}`),
  );
  if (failures.length > 0) blocks.push('', '### Failed categories', ...failures);

  return {
    markdown: blocks.join('\n'),
    outputs: {
      leagues_written: String(summary.leagues.filter((l) => l.written).length),
      leagues_failed: String(summary.leagues.filter((l) => !l.written).length),
    },
    json: { kind: 'economy', ...summary },
  };
}

const MB = 1024 * 1024;

export function renderRetentionSummary(summary: RetentionSummary): RenderedSummary {
  const mb = (n: number): string => (n / MB).toFixed(2);
  // Derive the usage table from the record so a new bucket can't be silently dropped.
  const usageHeaders = Object.keys(summary.usageByPrefix);
  const usageRow = Object.values(summary.usageByPrefix).map((n) => mb(n));
  const markdown = [
    '## Retention & R2 usage',
    '',
    table(
      ['total (MB)', 'budget (MB)', 'freed (MB)', 'detail trimmed', 'raw swept'],
      [
        [
          mb(summary.totalBytes),
          mb(summary.budgetBytes),
          mb(summary.bytesFreed),
          summary.detailSnapshotsTrimmed.length,
          summary.rawSnapshotsSwept.length,
        ],
      ],
    ),
    '',
    '### Usage by prefix (MB)',
    table(usageHeaders, [usageRow]),
  ].join('\n');

  return {
    markdown,
    outputs: { total_bytes: String(summary.totalBytes), bytes_freed: String(summary.bytesFreed) },
    json: { kind: 'retention', ...summary },
  };
}

export interface SummaryEnv {
  GITHUB_STEP_SUMMARY?: string | undefined;
  GITHUB_OUTPUT?: string | undefined;
}

/** Write a rendered summary out: stdout JSON line + GH step summary + GH outputs. */
export function emitSummary(rendered: RenderedSummary, env: SummaryEnv = process.env): void {
  console.log(JSON.stringify(rendered.json));
  if (env.GITHUB_STEP_SUMMARY) {
    appendFileSync(env.GITHUB_STEP_SUMMARY, rendered.markdown + '\n\n');
  }
  if (env.GITHUB_OUTPUT) {
    const lines = Object.entries(rendered.outputs)
      .map(([k, v]) => `${k}=${v}`)
      .join('\n');
    appendFileSync(env.GITHUB_OUTPUT, lines + '\n');
  }
}
