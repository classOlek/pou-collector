import { describe, expect, it } from 'vitest';
import type { LimiterMemory } from '@classolek/shared';
import { MemoryObjectStore } from '../checkpoint/object-store.js';
import { FakeClock } from './clock.js';
import { RateLimiter } from './limiter.js';
import { LimiterPersistence } from './limiter-persistence.js';
import { LimiterStateStore, workerSlot } from './limiter-store.js';
import { PaceStateStore } from './pace-store.js';

const LEAGUE = 'TestLeague';
const NOW = Date.parse('2026-07-20T00:00:00.000Z');
const ISO = new Date(NOW).toISOString();

function limiterWith(memory: Partial<LimiterMemory>): RateLimiter {
  const l = new RateLimiter(new FakeClock(NOW));
  l.restore({
    observedRules: [],
    penaltyUntil: 0,
    consecutiveThrottles: 0,
    consecutiveErrors: 0,
    recentAcquires: [],
    ...memory,
  });
  return l;
}

describe('LimiterPersistence split storage', () => {
  it('routes pace to the shared per-IP file and client state to the per-slot file', async () => {
    const store = new MemoryObjectStore();
    const persistence = new LimiterPersistence(store);

    const limiter = limiterWith({ recentAcquires: [100, 200], penaltyUntil: NOW + 5_000 });
    limiter.adoptIp('203.0.113.7');
    await persistence.save(
      limiter,
      { league: LEAGUE, slot: workerSlot(0), ip: '203.0.113.7' },
      ISO,
    );

    // Pace lives in the IP file…
    expect(await new PaceStateStore(store).load(LEAGUE, '203.0.113.7')).toEqual([100, 200]);
    // …and the slot file keeps the client-scoped state with the pace blanked out.
    const slot = await new LimiterStateStore(store).load(LEAGUE, workerSlot(0));
    expect(slot?.penaltyUntil).toBe(NOW + 5_000);
    expect(slot?.recentAcquires).toEqual([]);
    expect(slot?.originIp).toBe('203.0.113.7');
  });

  it('shares one IP budget across slots: slot 3 inherits slot 0’s spend on the same IP', async () => {
    // The bug this closes: IP X used by w0 in one fire, then by w3 in the next,
    // must pace against w0’s recorded spend instead of starting blind.
    const store = new MemoryObjectStore();
    const persistence = new LimiterPersistence(store);

    const w0 = limiterWith({ recentAcquires: [100, 200, 300] });
    w0.adoptIp('203.0.113.7');
    await persistence.save(w0, { league: LEAGUE, slot: workerSlot(0), ip: '203.0.113.7' }, ISO);

    // A different slot, no prior state of its own, lands on the same IP.
    const w3 = new RateLimiter(new FakeClock(NOW));
    const changed = await persistence.loadInto(w3, {
      league: LEAGUE,
      slot: workerSlot(3),
      ip: '203.0.113.7',
    });
    expect(changed).toBe(false); // same IP → spend kept, not reset
    expect(w3.toMemory().recentAcquires).toEqual([100, 200, 300]);
  });

  it('starts pace fresh on a new IP but keeps the client penalty (no Retry-After evasion)', async () => {
    const store = new MemoryObjectStore();
    const persistence = new LimiterPersistence(store);

    // A pre-split slot file: pace + originIp for the OLD IP, plus a live penalty.
    await new LimiterStateStore(store).save(
      LEAGUE,
      workerSlot(0),
      {
        observedRules: [],
        penaltyUntil: NOW + 3_600_000,
        consecutiveThrottles: 1,
        consecutiveErrors: 0,
        recentAcquires: [100, 200, 300],
        originIp: '203.0.113.7',
      },
      ISO,
    );

    const limiter = new RateLimiter(new FakeClock(NOW));
    const changed = await persistence.loadInto(limiter, {
      league: LEAGUE,
      slot: workerSlot(0),
      ip: '198.51.100.9', // fresh runner IP
    });

    expect(changed).toBe(true); // IP changed → pace reset
    const mem = limiter.toMemory();
    expect(mem.recentAcquires).toEqual([]); // old IP’s spend dropped
    expect(mem.penaltyUntil).toBe(NOW + 3_600_000); // penalty carried across the IP change
  });

  it('keeps pace in the slot file when the IP is unknown (discovery failed)', async () => {
    const store = new MemoryObjectStore();
    const persistence = new LimiterPersistence(store);

    const limiter = limiterWith({ recentAcquires: [100, 200] });
    // No adoptIp — originIp stays undefined, mirroring a failed IP discovery.
    await persistence.save(limiter, { league: LEAGUE, slot: workerSlot(0), ip: undefined }, ISO);

    // No IP file was written; the pace rode in the slot file (pre-split behavior).
    expect(await new PaceStateStore(store).load(LEAGUE, '203.0.113.7')).toBeUndefined();
    const slot = await new LimiterStateStore(store).load(LEAGUE, workerSlot(0));
    expect(slot?.recentAcquires).toEqual([100, 200]);

    // And it round-trips back into a limiter on the next unknown-IP run.
    const resumed = new RateLimiter(new FakeClock(NOW));
    await persistence.loadInto(resumed, { league: LEAGUE, slot: workerSlot(0), ip: undefined });
    expect(resumed.toMemory().recentAcquires).toEqual([100, 200]);
  });
});
