import { describe, expect, it } from 'vitest';
import type { RosterCharacter } from '@pou/shared';
import { chunkPath } from '@pou/shared';
import { MemoryObjectStore } from '../checkpoint/object-store.js';
import { ChunkStore, ownedChunkIndices, pendingChunkIndices, planChunks } from './chunk-store.js';

function roster(n: number): RosterCharacter[] {
  return Array.from({ length: n }, (_, i) => ({
    account: `acct-${i}`,
    character: `char-${i}`,
    class: 'Juggernaut',
    level: 90,
    rank: i + 1,
    firstSeenAt: '2026-07-01T00:00:00.000Z',
    lastSeenAt: '2026-07-01T00:00:00.000Z',
  }));
}

describe('planChunks', () => {
  it('splits the roster into pending chunks preserving order', () => {
    const chunks = planChunks('Std', 's1', roster(12), 5);
    expect(chunks.map((c) => c.characters.length)).toEqual([5, 5, 2]);
    expect(chunks.map((c) => c.chunkIndex)).toEqual([0, 1, 2]);
    expect(chunks[0]!.characters[0]!.account).toBe('acct-0');
    expect(chunks[2]!.characters.at(-1)!.account).toBe('acct-11');
    expect(chunks.every((c) => c.characters.every((q) => q.outcome === 'pending'))).toBe(true);
    expect(chunks.every((c) => c.shardsWritten === 0)).toBe(true);
  });

  it('plans no chunks for an empty roster', () => {
    expect(planChunks('Std', 's1', [], 5)).toEqual([]);
  });
});

describe('ownedChunkIndices', () => {
  it('partitions the index space disjointly and completely across workers', () => {
    const w0 = ownedChunkIndices(10, 0, 3);
    const w1 = ownedChunkIndices(10, 1, 3);
    const w2 = ownedChunkIndices(10, 2, 3);
    expect(w0).toEqual([0, 3, 6, 9]);
    expect(w1).toEqual([1, 4, 7]);
    expect(w2).toEqual([2, 5, 8]);
    expect([...w0, ...w1, ...w2].sort((a, b) => a - b)).toEqual([...Array(10).keys()]);
  });

  it('is empty when a slot owns nothing (fewer chunks than workers)', () => {
    expect(ownedChunkIndices(2, 5, 15)).toEqual([]);
    expect(ownedChunkIndices(0, 0, 15)).toEqual([]);
  });
});

describe('ChunkStore', () => {
  it('round-trips chunks at their canonical paths and deletes them wholesale', async () => {
    const store = new MemoryObjectStore();
    const chunks = new ChunkStore(store);
    for (const chunk of planChunks('Std', 's1', roster(7), 5)) await chunks.save(chunk);

    expect(store.keys().sort()).toEqual([chunkPath('Std', 's1', 0), chunkPath('Std', 's1', 1)]);
    const all = await chunks.loadAll('Std', 's1', 2);
    expect(all.map((c) => c.characters.length)).toEqual([5, 2]);
    expect(pendingChunkIndices(all)).toEqual([0, 1]);

    all[0]!.characters.forEach((q) => {
      q.outcome = 'ok';
    });
    await chunks.save(all[0]!);
    expect(pendingChunkIndices(await chunks.loadAll('Std', 's1', 2))).toEqual([1]);

    expect(await chunks.deleteAll('Std', 's1')).toBe(2);
    expect(store.keys()).toEqual([]);
  });

  it('fails loudly on a missing chunk (seeded once, never absent)', async () => {
    const chunks = new ChunkStore(new MemoryObjectStore());
    await expect(chunks.load('Std', 's1', 0)).rejects.toThrow(/missing/);
  });
});
