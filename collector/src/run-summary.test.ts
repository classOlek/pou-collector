import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { LimiterMemory } from '@pou/shared';
import type { CoordinatorSummary } from './run/coordinator.js';
import type { CreateSummary } from './run/create-snapshot.js';
import type { FinalizeSummary } from './run/finalize.js';
import type { WorkerSummary } from './run/worker.js';
import {
  HAS_WORK_OUTPUT_KEY,
  HAS_WORK_TRUE,
  WORKERS_OUTPUT_KEY,
  coordinateExitCode,
  createExitCode,
  emitSummary,
  finalizeExitCode,
  renderCoordinateSummary,
  renderCreateSummary,
  renderFinalizeSummary,
  renderObservedLimits,
  renderRetentionSummary,
  renderWorkerSummary,
} from './run-summary.js';

const seedMemory: LimiterMemory = {
  observedRules: [],
  penaltyUntil: 0,
  consecutiveThrottles: 0,
  consecutiveErrors: 0,
  recentAcquires: [],
};

const coordinateSummary: CoordinatorSummary = {
  phase: 'collecting',
  stopReason: 'work_pending',
  hasWork: true,
  workers: [0, 1, 2, 3],
  totalCharacters: 15720,
  chunkCount: 315,
  pendingCount: 12000,
};

const createSummary: CreateSummary = {
  stopReason: 'created',
  closed: {
    league: 'Standard',
    snapshotId: 'snap-0',
    result: 'published',
    skippedMarked: 220,
  },
  requests: 75,
  rosterSize: 15720,
  rosterAdded: 240,
  totalCharacters: 15720,
  chunkCount: 315,
};

const workerSummary: WorkerSummary = {
  workerIndex: 2,
  stopReason: 'budget_exhausted',
  assignedChunks: 79,
  chunksResolved: 11,
  requests: 1100,
  shardsWritten: 11,
  outcomes: { pending: 3000, ok: 480, private: 15, dead: 5, retryable: 2, skipped: 0 },
};

const finalizeSummary: FinalizeSummary = {
  phase: 'collecting',
  stopReason: 'published_partial',
  outcomes: { pending: 12000, ok: 3600, private: 100, dead: 20, retryable: 0, skipped: 0 },
  resolvedChunks: 74,
  chunkCount: 315,
  transform: {
    snapshotId: 'snap-1',
    league: 'Standard',
    complete: false,
    coverage: { ok: 3600, private: 100, dead: 20 },
    pendingCount: 12000,
    skippedCount: 0,
    characterCount: 3600,
    detailBytes: { characters: 1000, items: 2000 },
    aggregateRows: { class_distribution: 12 },
    rawShardsDeleted: 0,
  },
};

describe('exit codes', () => {
  it('coordinate always exits clean (request-free manifest check)', () => {
    expect(coordinateExitCode()).toBe(0);
  });

  it('create-snapshot fails only a run that itself aborted the new snapshot', () => {
    expect(createExitCode({ ...createSummary, stopReason: 'aborted' })).toBe(1);
    expect(createExitCode({ ...createSummary, stopReason: 'too_recent' })).toBe(0);
    expect(createExitCode({ ...createSummary, stopReason: 'cooldown' })).toBe(0);
    expect(createExitCode({ ...createSummary, stopReason: 'budget_exhausted' })).toBe(0);
    expect(createExitCode(createSummary)).toBe(0);
  });

  it('finalize fails only real aborts, not clean no-character aborts or partial-publish warnings', () => {
    expect(finalizeExitCode({ ...finalizeSummary, phase: 'aborted', stopReason: 'aborted' })).toBe(
      1,
    );
    expect(
      finalizeExitCode({
        ...finalizeSummary,
        phase: 'aborted',
        stopReason: 'aborted_no_characters',
      }),
    ).toBe(0);
    expect(finalizeExitCode({ ...finalizeSummary, stopReason: 'partial_publish_failed' })).toBe(0);
    expect(finalizeExitCode(finalizeSummary)).toBe(0);
  });
});

describe('renderCoordinateSummary', () => {
  it('emits the exact coordinate→worker fan-out contract snapshot.yml depends on', () => {
    // snapshot.yml fans workers out on steps.coordinate.outputs.<HAS_WORK_OUTPUT_KEY>
    // == '<HAS_WORK_TRUE>' with fromJSON(outputs.<WORKERS_OUTPUT_KEY>) as the matrix.
    // Renaming any of these must break this test, not silently idle the workers.
    expect(HAS_WORK_OUTPUT_KEY).toBe('has_work');
    expect(HAS_WORK_TRUE).toBe('true');
    expect(WORKERS_OUTPUT_KEY).toBe('workers');

    const rendered = renderCoordinateSummary(coordinateSummary);
    expect(rendered.outputs[HAS_WORK_OUTPUT_KEY]).toBe(HAS_WORK_TRUE);
    expect(JSON.parse(rendered.outputs[WORKERS_OUTPUT_KEY]!)).toEqual([0, 1, 2, 3]);
    expect(rendered.json.kind).toBe('coordinate');

    const idle = renderCoordinateSummary({
      ...coordinateSummary,
      hasWork: false,
      workers: [],
      stopReason: 'idle',
      pendingCount: 0,
    });
    expect(idle.outputs[HAS_WORK_OUTPUT_KEY]).toBe('false');
    expect(JSON.parse(idle.outputs[WORKERS_OUTPUT_KEY]!)).toEqual([]);
  });

  it('renders the create-snapshot summary with the close result', () => {
    const rendered = renderCreateSummary(createSummary, seedMemory);
    expect(rendered.json.kind).toBe('create_snapshot');
    expect(rendered.outputs.stop_reason).toBe('created');
    expect(rendered.outputs.closed_snapshot).toBe('snap-0');
    expect(rendered.outputs.closed_result).toBe('published');
    expect(rendered.outputs.marked_skipped).toBe('220');
    expect(rendered.markdown).toContain('Closed previous snapshot');

    const noClose: CreateSummary = { ...createSummary };
    delete (noClose as Partial<CreateSummary>).closed;
    const fresh = renderCreateSummary(noClose, seedMemory);
    expect(fresh.outputs.closed_snapshot).toBeUndefined();
    expect(fresh.markdown).toContain('No in-flight snapshot to close');
  });

  it('renders observed X-Rate-Limit rules, or a placeholder when none were seen', () => {
    expect(renderObservedLimits(seedMemory)).toMatch(/No X-Rate-Limit/);
    const withRules: LimiterMemory = {
      ...seedMemory,
      observedRules: [
        {
          name: 'Ip',
          limits: [{ hits: 8, periodSec: 10, penaltySec: 60 }],
          state: [{ hits: 1, periodSec: 10, penaltySec: 0 }],
        },
      ],
    };
    const md = renderObservedLimits(withRules);
    expect(md).toContain('Ip');
    expect(md).toContain('8:10:60');
  });
});

describe('renderWorkerSummary / renderFinalizeSummary', () => {
  it('surfaces the worker slot, chunk progress and outcome tallies', () => {
    const rendered = renderWorkerSummary(workerSummary, seedMemory);
    expect(rendered.markdown).toContain('Worker w2');
    expect(rendered.outputs.chunks_resolved).toBe('11');
    expect(rendered.markdown).toContain('480');
    expect(rendered.json.kind).toBe('worker');
  });

  it('marks an incremental publish as incomplete and a final one as complete', () => {
    const partial = renderFinalizeSummary(finalizeSummary);
    expect(partial.outputs.complete).toBe('false');
    expect(partial.outputs.snapshot_id).toBe('snap-1');
    expect(partial.markdown).toContain('Incremental publish');

    const final = renderFinalizeSummary({
      ...finalizeSummary,
      phase: 'published',
      stopReason: 'published_final',
      transform: { ...finalizeSummary.transform!, complete: true, pendingCount: 0 },
    });
    expect(final.outputs.complete).toBe('true');
    expect(final.markdown).toContain('Final publish');
  });

  it('renders a finalize run with no publish (nothing collected yet)', () => {
    const noPublish = { ...finalizeSummary, stopReason: 'collecting' as const };
    delete (noPublish as Partial<FinalizeSummary>).transform;
    const rendered = renderFinalizeSummary(noPublish);
    expect(rendered.outputs.snapshot_id).toBeUndefined();
    expect(rendered.outputs.stop_reason).toBe('collecting');
  });
});

describe('renderRetentionSummary', () => {
  it('reports usage and trim counts', () => {
    const rendered = renderRetentionSummary({
      usageByPrefix: {
        raw: 0,
        detail: 5 * 1024 * 1024,
        agg: 1024,
        meta: 512,
        tree: 64,
        checkpoint: 256,
        roster: 900,
        chunk: 300,
        worker: 100,
        index: 128,
        other: 0,
      },
      totalBytes: 5 * 1024 * 1024 + 3284,
      budgetBytes: 9_000_000_000,
      detailSnapshotsTrimmed: ['Standard/old'],
      rawSnapshotsSwept: ['Standard/orphan'],
      bytesFreed: 1024,
    });
    expect(rendered.outputs.bytes_freed).toBe('1024');
    expect(rendered.markdown).toContain('Usage by prefix');
    // The usage table is derived from the record, so new buckets appear.
    expect(rendered.markdown).toContain('roster');
  });
});

describe('emitSummary', () => {
  let dir: string;
  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'pou-emit-'));
  });
  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it('appends the markdown and key=value outputs to the GitHub files', async () => {
    const stepSummary = join(dir, 'summary.md');
    const output = join(dir, 'output.txt');
    emitSummary(renderCoordinateSummary(coordinateSummary), {
      GITHUB_STEP_SUMMARY: stepSummary,
      GITHUB_OUTPUT: output,
    });
    expect(await readFile(stepSummary, 'utf8')).toContain('Coordinate run');
    const out = await readFile(output, 'utf8');
    expect(out).toContain('has_work=true');
    expect(out).toContain('workers=[0,1,2,3]');
  });
});
