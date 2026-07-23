import { describe, expect, it } from 'vitest';
import type { QueuedCharacter, SnapshotCharacter } from './contracts.js';
import {
  addTallies,
  coverageOf,
  coverageOfTally,
  emptyTally,
  isInFlight,
  pendingOfTally,
  percentage,
  tallyOutcomes,
} from './contracts.js';

describe('percentage (shared by collector transform and web)', () => {
  it('is a 0–100 share rounded to the requested decimals', () => {
    expect(percentage(1800, 12000)).toBe(15); // 2 decimals default
    expect(percentage(2500, 15000, 1)).toBe(16.7);
    expect(percentage(1, 3, 2)).toBe(33.33);
  });

  it('returns 0 for a zero total (no division by zero)', () => {
    expect(percentage(0, 0)).toBe(0);
    expect(percentage(5, 0, 1)).toBe(0);
  });
});

function q(outcome: QueuedCharacter['outcome']): QueuedCharacter {
  return {
    rank: 1,
    account: 'a',
    character: 'c',
    class: 'Juggernaut',
    level: 100,
    outcome,
    attempts: 1,
  };
}

/** A state-file line: identity + outcome, with raw payloads only when `ok`. */
function sc(outcome: SnapshotCharacter['outcome']): SnapshotCharacter {
  const entry: SnapshotCharacter = {
    rank: 1,
    account: 'a',
    character: 'c',
    class: 'Witch',
    level: 98,
    outcome,
    attempts: 1,
  };
  if (outcome === 'ok') {
    entry.fetchedAt = '2026-07-23T00:00:00.000Z';
    entry.characterData = { items: [] };
    entry.passiveTree = { hashes: [] };
  }
  return entry;
}

describe('outcome tallies (single production implementation)', () => {
  it('tallies every outcome and derives coverage from it', () => {
    const queue = [
      q('ok'),
      q('ok'),
      q('private'),
      q('dead'),
      q('retryable'),
      q('pending'),
      q('skipped'),
    ];
    expect(tallyOutcomes(queue)).toEqual({
      pending: 1,
      ok: 2,
      private: 1,
      retryable: 1,
      dead: 1,
      skipped: 1,
    });
    // Coverage is the FETCHED subset (ok/private/dead) — skipped is excluded.
    expect(coverageOf(queue)).toEqual({ ok: 2, private: 1, dead: 1 });
  });

  it('is empty for an empty queue', () => {
    expect(tallyOutcomes([])).toEqual({
      pending: 0,
      ok: 0,
      private: 0,
      retryable: 0,
      dead: 0,
      skipped: 0,
    });
    expect(coverageOf([])).toEqual({ ok: 0, private: 0, dead: 0 });
  });

  it('carries over to SnapshotCharacter (state-file lines) unchanged', () => {
    // The v4 state file is SnapshotCharacter[]; the tally helpers read only the
    // `outcome` field, so they apply to it without a separate implementation —
    // the raw characterData/passiveTree payloads on `ok` lines are ignored.
    const lines: SnapshotCharacter[] = [sc('ok'), sc('ok'), sc('private'), sc('pending')];
    expect(tallyOutcomes(lines)).toEqual({
      pending: 1,
      ok: 2,
      private: 1,
      retryable: 0,
      dead: 0,
      skipped: 0,
    });
    expect(coverageOf(lines)).toEqual({ ok: 2, private: 1, dead: 0 });
    expect(pendingOfTally(tallyOutcomes(lines))).toBe(1);
  });

  it('sums per-chunk tallies into a rollup (finalize path)', () => {
    const rollup = emptyTally();
    addTallies(rollup, tallyOutcomes([q('ok'), q('pending')]));
    addTallies(rollup, tallyOutcomes([q('ok'), q('retryable'), q('private')]));
    expect(rollup).toEqual({ pending: 1, ok: 2, private: 1, retryable: 1, dead: 0, skipped: 0 });
    expect(coverageOfTally(rollup)).toEqual({ ok: 2, private: 1, dead: 0 });
    // Not-yet-computed = pending + retryable (both get another sweep).
    expect(pendingOfTally(rollup)).toBe(2);
  });
});

describe('isInFlight', () => {
  it('is true only for phases with live work', () => {
    expect(isInFlight('ladder_capture')).toBe(true);
    expect(isInFlight('collecting')).toBe(true);
    expect(isInFlight('transforming')).toBe(true);
    expect(isInFlight('published')).toBe(false);
    expect(isInFlight('aborted')).toBe(false);
  });
});
