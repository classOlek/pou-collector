import { describe, expect, it } from 'vitest';
import { treeCachePath } from '@pou/shared';
import { MemoryObjectStore } from '../checkpoint/object-store.js';
import { CachedTreeSource } from './tree-source.js';
import { FakeTreeOrigin } from '../../test/transform-fixtures.js';

describe('CachedTreeSource', () => {
  it('fetches the origin once, caches it in R2, and reuses the cache thereafter', async () => {
    const store = new MemoryObjectStore();
    const origin = new FakeTreeOrigin();
    const source = new CachedTreeSource(store, origin);

    const first = await source.load('3.25.1');
    expect(origin.fetches).toBe(1);
    expect(store.keys()).toContain(treeCachePath('3.25.1'));
    expect(first.nodes.length).toBeGreaterThan(10);

    // A fresh source (new run) reads the R2 cache — no second origin fetch.
    const second = await new CachedTreeSource(store, origin).load('3.25.1');
    expect(origin.fetches).toBe(1);
    expect(second.nodes).toEqual(first.nodes);
  });

  it('fetches again for a different pinned tree version', async () => {
    const store = new MemoryObjectStore();
    const origin = new FakeTreeOrigin();
    const source = new CachedTreeSource(store, origin);

    await source.load('3.25.1');
    await source.load('3.26.0');
    expect(origin.fetches).toBe(2);
    expect(store.keys()).toEqual(
      expect.arrayContaining([treeCachePath('3.25.1'), treeCachePath('3.26.0')]),
    );
  });
});
