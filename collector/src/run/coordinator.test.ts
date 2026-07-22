import { describe, expect, it } from 'vitest';
import type { LimiterMemory } from '@classolek/shared';
import { emptyTally } from '@classolek/shared';
import { MemoryObjectStore } from '../checkpoint/object-store.js';
import { CheckpointStore } from '../checkpoint/store.js';
import { FakeClock } from '../rate-limit/clock.js';
import { COORDINATOR_SLOT, LimiterStateStore, workerSlot } from '../rate-limit/limiter-store.js';
import { fixtureManifest } from '../../test/helpers.js';
import { Coordinator } from './coordinator.js';

const LEAGUE = 'TestLeague';
const NOW = Date.parse('2026-07-17T00:00:00.000Z');
const MAX_WAIT_MS = 300_000;

const memory = (overrides: Partial<LimiterMemory> = {}): LimiterMemory => ({
  observedRules: [],
  penaltyUntil: 0,
  consecutiveThrottles: 0,
  consecutiveErrors: 0,
  recentAcquires: [],
  ...overrides,
});

function makeCoordinator(workerCount = 3, collectCooldownMillis = 0) {
  const objectStore = new MemoryObjectStore();
  const checkpointStore = new CheckpointStore(objectStore);
  const clock = new FakeClock(NOW);
  const limiterStates = new LimiterStateStore(objectStore);
  const coordinator = new Coordinator(
    { league: LEAGUE, workerCount, maxWaitMillis: MAX_WAIT_MS, collectCooldownMillis },
    { checkpointStore, objectStore, clock },
  );
  return { checkpointStore, clock, limiterStates, coordinator };
}

const collectingManifest = () =>
  fixtureManifest({
    phase: 'collecting',
    totalCharacters: 10,
    chunkCount: 2,
    outcomes: { ...emptyTally(), pending: 4, retryable: 1, ok: 5 },
  });

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
    await checkpointStore.save(collectingManifest());

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

describe('Coordinator penalty gate (skip the wave instead of spawning stalled workers)', () => {
  it('skips the fan-out while any worker slot serves a penalty beyond maxWaitMillis', async () => {
    const { checkpointStore, clock, limiterStates, coordinator } = makeCoordinator(3);
    await checkpointStore.save(collectingManifest());
    const penaltyUntil = clock.now() + MAX_WAIT_MS + 60_000;
    await limiterStates.save(
      LEAGUE,
      workerSlot(1),
      memory({ penaltyUntil }),
      new Date(clock.now()).toISOString(),
    );

    const summary = await coordinator.runOnce();
    expect(summary.stopReason).toBe('penalty_active');
    expect(summary.hasWork).toBe(false);
    expect(summary.workers).toEqual([]);
    expect(summary.blockedUntil).toBe(penaltyUntil);
  });

  it("counts the coordinator slot's penalty too (429s are client-scoped)", async () => {
    const { checkpointStore, clock, limiterStates, coordinator } = makeCoordinator(2);
    await checkpointStore.save(collectingManifest());
    await limiterStates.save(
      LEAGUE,
      COORDINATOR_SLOT,
      memory({ penaltyUntil: clock.now() + MAX_WAIT_MS + 1 }),
      new Date(clock.now()).toISOString(),
    );

    const summary = await coordinator.runOnce();
    expect(summary.stopReason).toBe('penalty_active');
    expect(summary.hasWork).toBe(false);
  });

  it('still fans out when the penalty ends within maxWaitMillis (workers just sleep it)', async () => {
    const { checkpointStore, clock, limiterStates, coordinator } = makeCoordinator(2);
    await checkpointStore.save(collectingManifest());
    await limiterStates.save(
      LEAGUE,
      workerSlot(0),
      memory({ penaltyUntil: clock.now() + MAX_WAIT_MS - 1 }),
      new Date(clock.now()).toISOString(),
    );

    const summary = await coordinator.runOnce();
    expect(summary.stopReason).toBe('work_pending');
    expect(summary.hasWork).toBe(true);
  });
});

describe('Coordinator cooldown gate (explicit wave cadence = aggregate politeness)', () => {
  const COOLDOWN_MS = 30 * 60_000;

  it('skips the fan-out until collectCooldownMillis after the last wave checkpointed', async () => {
    const { checkpointStore, clock, limiterStates, coordinator } = makeCoordinator(2, COOLDOWN_MS);
    await checkpointStore.save(collectingManifest());
    const lastWaveAt = clock.now();
    await limiterStates.save(LEAGUE, workerSlot(0), memory(), new Date(lastWaveAt).toISOString());

    clock.advance(COOLDOWN_MS - 1);
    const blocked = await coordinator.runOnce();
    expect(blocked.stopReason).toBe('cooldown');
    expect(blocked.hasWork).toBe(false);
    expect(blocked.blockedUntil).toBe(lastWaveAt + COOLDOWN_MS);

    clock.advance(1);
    const open = await coordinator.runOnce();
    expect(open.stopReason).toBe('work_pending');
    expect(open.hasWork).toBe(true);
  });

  it('anchors on the NEWEST worker save and ignores the coordinator slot', async () => {
    const { checkpointStore, clock, limiterStates, coordinator } = makeCoordinator(2, COOLDOWN_MS);
    await checkpointStore.save(collectingManifest());
    const oldWaveAt = clock.now() - 2 * COOLDOWN_MS;
    const newWaveAt = clock.now() - COOLDOWN_MS / 2;
    await limiterStates.save(LEAGUE, workerSlot(0), memory(), new Date(oldWaveAt).toISOString());
    await limiterStates.save(LEAGUE, workerSlot(1), memory(), new Date(newWaveAt).toISOString());
    // A fresh create-snapshot save must not delay the FIRST wave of its snapshot.
    await limiterStates.save(
      LEAGUE,
      COORDINATOR_SLOT,
      memory(),
      new Date(clock.now()).toISOString(),
    );

    const summary = await coordinator.runOnce();
    expect(summary.stopReason).toBe('cooldown');
    expect(summary.blockedUntil).toBe(newWaveAt + COOLDOWN_MS);
  });

  it('does not gate the first wave (no worker state yet) and is inert when disabled', async () => {
    const first = makeCoordinator(2, COOLDOWN_MS);
    await first.checkpointStore.save(collectingManifest());
    expect((await first.coordinator.runOnce()).hasWork).toBe(true);

    const disabled = makeCoordinator(2, 0);
    await disabled.checkpointStore.save(collectingManifest());
    await disabled.limiterStates.save(
      LEAGUE,
      workerSlot(0),
      memory(),
      new Date(disabled.clock.now()).toISOString(),
    );
    expect((await disabled.coordinator.runOnce()).hasWork).toBe(true);
  });
});
