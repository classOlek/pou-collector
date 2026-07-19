import { describe, expect, it } from 'vitest';
import { emptyTally } from '@pou/shared';
import { MemoryObjectStore } from '../checkpoint/object-store.js';
import { CheckpointStore } from '../checkpoint/store.js';
import { fixtureManifest } from '../../test/helpers.js';
import { Coordinator } from './coordinator.js';

const LEAGUE = 'TestLeague';

function makeCoordinator(workerCount = 3) {
  const objectStore = new MemoryObjectStore();
  const checkpointStore = new CheckpointStore(objectStore);
  const coordinator = new Coordinator({ league: LEAGUE, workerCount }, { checkpointStore });
  return { checkpointStore, coordinator };
}

describe('Coordinator: request-free fan-out check of the newest snapshot', () => {
  it('reports no work when the league has no checkpoint at all', async () => {
    const { coordinator } = makeCoordinator();
    const summary = await coordinator.runOnce();
    expect(summary.phase).toBe('none');
    expect(summary.stopReason).toBe('idle');
    expect(summary.hasWork).toBe(false);
    expect(summary.workers).toEqual([]);
  });

  it('fans the worker matrix out while the snapshot has uncollected characters', async () => {
    const { checkpointStore, coordinator } = makeCoordinator(3);
    await checkpointStore.save(
      fixtureManifest({
        phase: 'collecting',
        totalCharacters: 10,
        chunkCount: 2,
        outcomes: { ...emptyTally(), pending: 4, retryable: 1, ok: 5 },
      }),
    );

    const summary = await coordinator.runOnce();
    expect(summary.stopReason).toBe('work_pending');
    expect(summary.hasWork).toBe(true);
    expect(summary.workers).toEqual([0, 1, 2]);
    expect(summary.pendingCount).toBe(5); // pending + retryable
    expect(summary.totalCharacters).toBe(10);
  });

  it('no-ops when the collecting snapshot has nothing left to compute', async () => {
    const { checkpointStore, coordinator } = makeCoordinator();
    await checkpointStore.save(
      fixtureManifest({
        phase: 'collecting',
        totalCharacters: 5,
        outcomes: { ...emptyTally(), ok: 3, dead: 1, skipped: 1 },
      }),
    );

    const summary = await coordinator.runOnce();
    expect(summary.stopReason).toBe('idle');
    expect(summary.hasWork).toBe(false);
    expect(summary.pendingCount).toBe(0);
  });

  it('never fans out for non-collecting phases (creation belongs to the create workflow)', async () => {
    const { checkpointStore, coordinator } = makeCoordinator();
    for (const phase of ['ladder_capture', 'transforming', 'published', 'aborted'] as const) {
      await checkpointStore.save(
        fixtureManifest({ phase, outcomes: { ...emptyTally(), pending: 5 } }),
      );
      const summary = await coordinator.runOnce();
      expect(summary.phase).toBe(phase);
      expect(summary.hasWork).toBe(false);
      expect(summary.stopReason).toBe('idle');
    }
  });
});
