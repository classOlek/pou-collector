import { describe, expect, it } from 'vitest';
import type { QueuedCharacter } from './contracts.js';
import {
  addTallies,
  chunkCountFor,
  coverageOf,
  coverageOfTally,
  emptyTally,
  isChunkResolved,
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

describe('chunk helpers', () => {
  it('resolves a chunk only when no character awaits computation', () => {
    expect(isChunkResolved({ characters: [q('ok'), q('private'), q('dead')] })).toBe(true);
    expect(isChunkResolved({ characters: [q('ok'), q('pending')] })).toBe(false);
    expect(isChunkResolved({ characters: [q('ok'), q('retryable')] })).toBe(false);
    // Skipped is terminal: closing a snapshot resolves its chunks.
    expect(isChunkResolved({ characters: [q('ok'), q('skipped')] })).toBe(true);
    expect(isChunkResolved({ characters: [] })).toBe(true);
  });

  it('splits a character total into ceil(total / chunkSize) chunks', () => {
    expect(chunkCountFor(15000, 50)).toBe(300);
    expect(chunkCountFor(151, 50)).toBe(4);
    expect(chunkCountFor(0, 50)).toBe(0);
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
