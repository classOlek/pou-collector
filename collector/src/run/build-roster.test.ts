/**
 * RosterBuilder: the build-roster workflow's ladder capture → roster merge pass
 * (the only step that reads the GGG ladder; the new-snapshot step seeds from
 * the roster this produces).
 */
import { describe, expect, it } from 'vitest';
import type { RosterFile } from '@classolek/shared';
import { rosterPath } from '@classolek/shared';
import { getJson } from '../checkpoint/object-store.js';
import { HOUR_MS } from './config.js';
import { LEAGUE, entry, makeRunHarness } from '../../test/run-harness.js';
import { buildLadder } from '../../test/mock-api.js';

describe('RosterBuilder: ladder capture → roster merge', () => {
  it('captures the ladder and seeds the roster', async () => {
    const h = makeRunHarness({ entries: buildLadder(23) });

    const summary = await h.buildFire();

    expect(summary.stopReason).toBe('built');
    expect(summary.rosterSize).toBe(23);
    expect(summary.rosterAdded).toBe(23);
    expect(summary.rosterRefreshed).toBe(0);

    // The roster is the per-league character database (redesign step 1).
    const roster = await getJson<RosterFile>(h.objectStore, rosterPath(LEAGUE));
    expect(roster?.characters).toHaveLength(23);
    // No snapshot is created by a build — that is the new-snapshot step's job.
    expect(await h.checkpointStore.load(LEAGUE)).toBeUndefined();
    expect(h.objectStore.keys().some((k) => k.includes('/chunks/'))).toBe(false);
  });

  it('grows the roster across builds: new entrants join, leavers stay', async () => {
    // Build 1 sees characters 0..9.
    const h = makeRunHarness({ entries: buildLadder(10) });
    await h.buildFire();
    h.clock.advance(1 * HOUR_MS);

    // Build 2: the ladder rolled — five new entrants pushed five old ones out
    // of the window. The roster holds the union (leavers stay collectable).
    const rolled = [
      ...Array.from({ length: 5 }, (_, i) => entry(`new-${i}`, { kind: 'ok' })),
      ...buildLadder(10).slice(0, 5),
    ];
    const summary = await h.newBuilderFor(rolled).runOnce();

    expect(summary.stopReason).toBe('built');
    expect(summary.rosterAdded).toBe(5);
    expect(summary.rosterRefreshed).toBe(5);
    expect(summary.rosterSize).toBe(15);
    const roster = await getJson<RosterFile>(h.objectStore, rosterPath(LEAGUE));
    const accounts = new Set(roster?.characters.map((c) => c.account));
    expect(accounts.has('acct-new-0')).toBe(true); // new entrant
    expect(accounts.has('acct-9')).toBe(true); // left the ladder, still known
  });

  it('recovers from transient ladder throttling and still merges the full roster', async () => {
    const h = makeRunHarness({ entries: buildLadder(5), ladderThrottleFirst: 2 });
    const summary = await h.buildFire();
    expect(summary.stopReason).toBe('built');
    expect(summary.rosterSize).toBe(5);
  });

  it('aborts capture when a page fails past maxAttempts (no unbounded retry)', async () => {
    const h = makeRunHarness({
      entries: buildLadder(5),
      config: { maxAttempts: 2 },
      ladderThrottleFirst: 50,
    });

    const summary = await h.buildFire();

    expect(summary.stopReason).toBe('aborted');
    // Nothing merged: the roster stays empty.
    expect(summary.rosterSize).toBe(0);
    expect(await getJson<RosterFile>(h.objectStore, rosterPath(LEAGUE))).toBeUndefined();
  });

  it('leaves the roster untouched when the run budget expires mid-capture', async () => {
    // Three ladder pages, each "taking" 600 ms of wall clock: the budget check
    // before page 3 trips and the capture restarts cleanly next build fire.
    const h = makeRunHarness({
      entries: buildLadder(600),
      config: { maxRunMillis: 1000, ladderPageSize: 200 },
    });
    const slowClient = (req: Parameters<typeof h.api.client>[0]) => {
      h.clock.advance(600);
      return h.api.client(req);
    };
    const summary = await h.newRosterBuilder(slowClient).runOnce();

    expect(summary.stopReason).toBe('budget_exhausted');
    // A partial ladder never becomes a roster — nothing was written.
    expect(await getJson<RosterFile>(h.objectStore, rosterPath(LEAGUE))).toBeUndefined();
  });
});
