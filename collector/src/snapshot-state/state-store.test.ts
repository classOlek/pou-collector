import { describe, expect, it } from 'vitest';
import { gzipSync } from 'node:zlib';
import type { SnapshotCharacter } from '@classolek/shared';
import { snapshotStatePath, workerResultPath } from '@classolek/shared';
import { MemoryObjectStore } from '../checkpoint/object-store.js';
import {
  assignedTo,
  identityKey,
  indexResults,
  mergeResults,
  pendingIdentities,
  readState,
  writeState,
  writeWorkerResults,
} from './state-store.js';

const LEAGUE = 'Std';
const SNAP = 's1';

/** A pending state line (no payloads), overridable. */
function pending(i: number, overrides: Partial<SnapshotCharacter> = {}): SnapshotCharacter {
  return {
    rank: i + 1,
    account: `acct-${i}`,
    character: `char-${i}`,
    class: 'Juggernaut',
    level: 90,
    outcome: 'pending',
    attempts: 0,
    ...overrides,
  };
}

/** A resolved (`ok`) result line carrying raw payloads. */
function resolved(i: number, payloadBytes = 0): SnapshotCharacter {
  return {
    ...pending(i),
    outcome: 'ok',
    attempts: 1,
    fetchedAt: '2026-07-20T00:00:00.000Z',
    characterData: { items: 'x'.repeat(payloadBytes) },
    passiveTree: { hashes: [i, i + 1, i + 2] },
  };
}

async function collect(state: AsyncIterable<SnapshotCharacter>): Promise<SnapshotCharacter[]> {
  const out: SnapshotCharacter[] = [];
  for await (const c of state) out.push(c);
  return out;
}

describe('writeState / readState round-trip', () => {
  it('preserves lines and order, including raw payloads', async () => {
    const store = new MemoryObjectStore();
    const lines = [pending(0), resolved(1, 32), pending(2, { outcome: 'private' })];
    await writeState(store, LEAGUE, SNAP, lines);

    // It is a gzipped object at the canonical path (not stored as plain text).
    expect(store.keys()).toEqual([snapshotStatePath(LEAGUE, SNAP)]);
    const raw = await store.get(snapshotStatePath(LEAGUE, SNAP));
    expect(raw?.[0]).toBe(0x1f); // gzip magic
    expect(raw?.[1]).toBe(0x8b);

    expect(await collect(readState(store, LEAGUE, SNAP))).toEqual(lines);
  });

  it('round-trips at size without materializing the whole file', async () => {
    const store = new MemoryObjectStore();
    // 2000 lines × ~2 KB payload = a few MB of NDJSON crossing many gzip chunks.
    const lines = Array.from({ length: 2000 }, (_, i) => resolved(i, 2048));
    await writeState(store, LEAGUE, SNAP, lines);

    let count = 0;
    let lastAccount = '';
    for await (const c of readState(store, LEAGUE, SNAP)) {
      count += 1;
      lastAccount = c.account;
    }
    expect(count).toBe(2000);
    expect(lastAccount).toBe('acct-1999');
  });

  it('round-trips an empty state file', async () => {
    const store = new MemoryObjectStore();
    await writeState(store, LEAGUE, SNAP, []);
    expect(await collect(readState(store, LEAGUE, SNAP))).toEqual([]);
  });

  it('accepts an async iterable source', async () => {
    const store = new MemoryObjectStore();
    async function* gen(): AsyncGenerator<SnapshotCharacter> {
      yield pending(0);
      yield resolved(1);
    }
    await writeState(store, LEAGUE, SNAP, gen());
    expect((await collect(readState(store, LEAGUE, SNAP))).map((c) => c.outcome)).toEqual([
      'pending',
      'ok',
    ]);
  });
});

describe('writeWorkerResults', () => {
  it('writes a slot result file the state readers can decode, feeding mergeResults', async () => {
    const store = new MemoryObjectStore();
    const results = [resolved(1, 8), resolved(3, 8)];
    await writeWorkerResults(store, LEAGUE, SNAP, 4, results);

    // Same gzipped-NDJSON format at the slot's own key (single writer per w<NN>).
    const key = workerResultPath(LEAGUE, SNAP, 4);
    expect(store.keys()).toEqual([key]);
    const raw = await store.get(key);
    expect(raw?.[0]).toBe(0x1f); // gzip magic
    expect(raw?.[1]).toBe(0x8b);

    // The bytes round-trip through the state file's own merge join.
    await writeState(store, LEAGUE, SNAP, [pending(0), pending(1), pending(2), pending(3)]);
    const merged = await collect(mergeResults(readState(store, LEAGUE, SNAP), results));
    expect(merged.map((c) => c.outcome)).toEqual(['pending', 'ok', 'pending', 'ok']);
    expect(merged[1]).toMatchObject({ passiveTree: { hashes: [1, 2, 3] } });
  });
});

describe('readState failure modes', () => {
  it('throws on a missing state file (seeded once, never absent)', async () => {
    const store = new MemoryObjectStore();
    await expect(collect(readState(store, LEAGUE, SNAP))).rejects.toThrow(/missing/);
  });

  it('throws on a corrupt (non-JSON) line rather than skipping it', async () => {
    const store = new MemoryObjectStore();
    const body =
      JSON.stringify(pending(0)) + '\n{ not json }\n' + JSON.stringify(pending(2)) + '\n';
    await store.put(snapshotStatePath(LEAGUE, SNAP), gzipSync(Buffer.from(body, 'utf8')));
    await expect(collect(readState(store, LEAGUE, SNAP))).rejects.toThrow(/corrupt line 1/);
  });

  it('throws on a body that is not gzip', async () => {
    const store = new MemoryObjectStore();
    await store.put(snapshotStatePath(LEAGUE, SNAP), new TextEncoder().encode('not-gzip'));
    await expect(collect(readState(store, LEAGUE, SNAP))).rejects.toThrow();
  });
});

describe('pendingIdentities', () => {
  it('returns pending + retryable identities with ordinals and no payloads', async () => {
    const store = new MemoryObjectStore();
    await writeState(store, LEAGUE, SNAP, [
      pending(0), // ordinal 0 — pending
      resolved(1), // ordinal 1 — ok (skipped)
      pending(2, { outcome: 'retryable', attempts: 1 }), // ordinal 2 — retryable
      pending(3, { outcome: 'dead' }), // ordinal 3 — terminal (skipped)
      pending(4, { outcome: 'skipped' }), // ordinal 4 — terminal (skipped)
      pending(5), // ordinal 5 — pending
    ]);

    const ids = await pendingIdentities(readState(store, LEAGUE, SNAP));
    expect(ids.map((p) => p.ordinal)).toEqual([0, 2, 5]);
    expect(ids.map((p) => p.outcome)).toEqual(['pending', 'retryable', 'pending']);
    expect(ids[1]).toMatchObject({ account: 'acct-2', character: 'char-2', attempts: 1 });
    // No raw payloads leak into the identity list.
    expect(ids.every((p) => !('characterData' in p) && !('passiveTree' in p))).toBe(true);
  });
});

describe('assignedTo', () => {
  it('partitions non-contiguous pending ordinals disjointly and completely', () => {
    const ordinals = [0, 2, 5, 7, 8, 11];
    const w0 = assignedTo(ordinals, 0, 3);
    const w1 = assignedTo(ordinals, 1, 3);
    const w2 = assignedTo(ordinals, 2, 3);
    expect(w0).toEqual([0]); // 0,9,... ≡0
    expect(w1).toEqual([7]); // 7 ≡1 (mod 3)
    expect(w2).toEqual([2, 5, 8, 11]); // ≡2
    expect([...w0, ...w1, ...w2].sort((a, b) => a - b)).toEqual(ordinals);
  });

  it('is stable regardless of the pending list order (keyed on the ordinal)', () => {
    const ordinals = [0, 2, 5, 7, 8, 11];
    const shuffled = [11, 0, 8, 2, 7, 5];
    for (let w = 0; w < 3; w += 1) {
      expect(assignedTo(shuffled, w, 3).sort((a, b) => a - b)).toEqual(assignedTo(ordinals, w, 3));
    }
  });

  it('is empty when a slot owns nothing', () => {
    expect(assignedTo([0, 3, 6], 1, 3)).toEqual([]);
    expect(assignedTo([], 0, 15)).toEqual([]);
  });
});

describe('identityKey / indexResults', () => {
  it('keys by account + character and keeps the last write per identity', () => {
    const first = resolved(0);
    const second = { ...resolved(0), attempts: 2 };
    const map = indexResults([first, second, resolved(1)]);
    expect(map.size).toBe(2);
    expect(map.get(identityKey(first))?.attempts).toBe(2);
  });
});

describe('mergeResults', () => {
  it('patches matched identities and leaves the rest untouched', async () => {
    const store = new MemoryObjectStore();
    await writeState(store, LEAGUE, SNAP, [pending(0), pending(1), pending(2)]);

    const results = [resolved(1, 16)];
    await writeState(store, LEAGUE, SNAP, mergeResults(readState(store, LEAGUE, SNAP), results));

    const merged = await collect(readState(store, LEAGUE, SNAP));
    expect(merged.map((c) => c.outcome)).toEqual(['pending', 'ok', 'pending']);
    expect(merged[1]).toMatchObject({ outcome: 'ok', characterData: { items: 'x'.repeat(16) } });
    // Order and unmatched identities are preserved exactly.
    expect(merged.map((c) => c.account)).toEqual(['acct-0', 'acct-1', 'acct-2']);
  });

  it('ignores a result whose identity is not in the state', async () => {
    const store = new MemoryObjectStore();
    await writeState(store, LEAGUE, SNAP, [pending(0), pending(1)]);
    const merged = await collect(mergeResults(readState(store, LEAGUE, SNAP), [resolved(99)]));
    expect(merged.map((c) => c.account)).toEqual(['acct-0', 'acct-1']);
    expect(merged.every((c) => c.outcome === 'pending')).toBe(true);
  });

  it('is idempotent: re-merging the same results is a byte-for-byte no-op', async () => {
    const store = new MemoryObjectStore();
    await writeState(
      store,
      LEAGUE,
      SNAP,
      Array.from({ length: 6 }, (_, i) => pending(i)),
    );
    const results = [resolved(1, 8), resolved(4, 8)];

    await writeState(store, LEAGUE, SNAP, mergeResults(readState(store, LEAGUE, SNAP), results));
    const afterFirst = await store.get(snapshotStatePath(LEAGUE, SNAP));

    await writeState(store, LEAGUE, SNAP, mergeResults(readState(store, LEAGUE, SNAP), results));
    const afterSecond = await store.get(snapshotStatePath(LEAGUE, SNAP));

    expect(Buffer.from(afterSecond!)).toEqual(Buffer.from(afterFirst!));
    const merged = await collect(readState(store, LEAGUE, SNAP));
    expect(merged.filter((c) => c.outcome === 'ok').map((c) => c.account)).toEqual([
      'acct-1',
      'acct-4',
    ]);
  });
});
