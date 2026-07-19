import { describe, expect, it } from 'vitest';
import type { RosterCharacter } from '@pou/shared';
import { chunkPath } from '@pou/shared';
import { MemoryObjectStore } from '../checkpoint/object-store.js';
import { ChunkStore, pendingChunkIndices, planChunks } from './chunk-store.js';

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
