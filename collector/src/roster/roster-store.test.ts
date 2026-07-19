import { describe, expect, it } from 'vitest';
import { SCHEMA_VERSION, rosterPath } from '@pou/shared';
import { MemoryObjectStore, putJson } from '../checkpoint/object-store.js';
import type { LadderEntry } from '../sources/types.js';
import { RosterStore, emptyRoster, mergeLadder } from './roster-store.js';

function entry(rank: number, account: string, character: string, level = 90): LadderEntry {
  return { rank, account, character, class: 'Juggernaut', level };
}

describe('mergeLadder', () => {
  it('appends unseen characters and refreshes known ones', () => {
    const t0 = '2026-07-01T00:00:00.000Z';
    const first = mergeLadder(emptyRoster('Std'), [entry(1, 'a', 'A'), entry(2, 'b', 'B')], t0);
    expect(first.added).toBe(2);
    expect(first.refreshed).toBe(0);

    // Next capture: A climbed and leveled, B left the ladder, C is new.
    const t1 = '2026-07-02T00:00:00.000Z';
    const next = mergeLadder(
      first.roster,
      [entry(1, 'c', 'C', 100), { ...entry(2, 'a', 'A'), level: 95 }],
      t1,
    );
    expect(next.added).toBe(1);
    expect(next.refreshed).toBe(1);
    expect(next.roster.characters).toHaveLength(3);

    const a = next.roster.characters.find((c) => c.account === 'a');
    expect(a).toMatchObject({ rank: 2, level: 95, firstSeenAt: t0, lastSeenAt: t1 });
    // B keeps its last-seen ladder facts — leaving the ladder never evicts.
    const b = next.roster.characters.find((c) => c.account === 'b');
    expect(b).toMatchObject({ rank: 2, level: 90, firstSeenAt: t0, lastSeenAt: t0 });
  });

  it('orders by last seen rank so the queue front is the fresh ladder top', () => {
    const t0 = '2026-07-01T00:00:00.000Z';
    const first = mergeLadder(emptyRoster('Std'), [entry(3, 'a', 'A'), entry(2, 'b', 'B')], t0);
    const next = mergeLadder(first.roster, [entry(1, 'c', 'C'), { ...entry(2, 'a', 'A') }], t0);
    // c leads at rank 1; a and b tie at rank 2 and fall back to a stable order.
    expect(next.roster.characters.map((c) => c.account)).toEqual(['c', 'a', 'b']);
  });

  it('treats a same-named character on another account as a different character', () => {
    const t0 = '2026-07-01T00:00:00.000Z';
    const merged = mergeLadder(
      emptyRoster('Std'),
      [entry(1, 'a', 'Twin'), entry(2, 'b', 'Twin')],
      t0,
    );
    expect(merged.added).toBe(2);
  });
});

describe('RosterStore', () => {
  it('round-trips a roster and starts empty when absent', async () => {
    const store = new MemoryObjectStore();
    const rosters = new RosterStore(store);
    expect((await rosters.load('Std')).characters).toEqual([]);

    const { roster } = mergeLadder(
      emptyRoster('Std'),
      [entry(1, 'a', 'A')],
      '2026-07-01T00:00:00Z',
    );
    await rosters.save(roster);
    expect(await rosters.load('Std')).toEqual(roster);
  });

  it('treats a foreign-schema roster as empty instead of trusting it', async () => {
    const store = new MemoryObjectStore();
    await putJson(store, rosterPath('Std'), {
      schemaVersion: SCHEMA_VERSION - 1,
      league: 'Std',
      characters: [{ account: 'a' }],
    });
    expect((await new RosterStore(store).load('Std')).characters).toEqual([]);
  });
});
