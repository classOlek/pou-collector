import { describe, expect, it } from 'vitest';
import { SCHEMA_VERSION, ipPacePath } from '@classolek/shared';
import { MemoryObjectStore, putJson } from '../checkpoint/object-store.js';
import { PaceStateStore } from './pace-store.js';

const LEAGUE = 'TestLeague';

describe('PaceStateStore', () => {
  it('round-trips a per-IP spend keyed by runner IP', async () => {
    const store = new MemoryObjectStore();
    const pace = new PaceStateStore(store);

    await pace.save(LEAGUE, '203.0.113.7', [1000, 2000, 3000], '2026-07-20T00:00:00.000Z');

    expect(await pace.load(LEAGUE, '203.0.113.7')).toEqual([1000, 2000, 3000]);
    // Distinct IPs are distinct files — no cross-IP bleed.
    expect(await pace.load(LEAGUE, '198.51.100.9')).toBeUndefined();
  });

  it('fails open (undefined) on an absent, corrupt or foreign-schema file', async () => {
    const store = new MemoryObjectStore();
    const pace = new PaceStateStore(store);

    expect(await pace.load(LEAGUE, '203.0.113.7')).toBeUndefined(); // absent

    await store.put(ipPacePath(LEAGUE, '203.0.113.7'), new TextEncoder().encode('{not json'));
    expect(await pace.load(LEAGUE, '203.0.113.7')).toBeUndefined(); // corrupt

    await putJson(store, ipPacePath(LEAGUE, '198.51.100.9'), {
      schemaVersion: SCHEMA_VERSION + 99,
      ip: '198.51.100.9',
      updatedAt: '2026-07-20T00:00:00.000Z',
      recentAcquires: [1],
    });
    expect(await pace.load(LEAGUE, '198.51.100.9')).toBeUndefined(); // foreign schema
  });

  it('lists every IP file for a league, surfacing corrupt ones for the sweep', async () => {
    const store = new MemoryObjectStore();
    const pace = new PaceStateStore(store);
    await pace.save(LEAGUE, '203.0.113.7', [1], '2026-07-20T00:00:00.000Z');
    await pace.save(LEAGUE, '198.51.100.9', [2], '2026-07-20T01:00:00.000Z');
    await store.put(ipPacePath(LEAGUE, '10.0.0.1'), new TextEncoder().encode('garbage'));
    // A different league's file must not appear.
    await pace.save('OtherLeague', '203.0.113.7', [9], '2026-07-20T00:00:00.000Z');

    const listed = await pace.list(LEAGUE);
    expect(listed).toHaveLength(3);
    expect(
      listed
        .filter((e) => e.state !== undefined)
        .map((e) => e.state?.ip)
        .sort(),
    ).toEqual(['198.51.100.9', '203.0.113.7']);
    // The corrupt file is surfaced with state: undefined so the sweep can reap it.
    expect(listed.some((e) => e.state === undefined)).toBe(true);
  });
});
