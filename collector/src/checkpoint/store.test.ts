import { describe, expect, it } from 'vitest';
import type { SnapshotManifest } from '@classolek/shared';
import { checkpointPath } from '@classolek/shared';
import { MemoryObjectStore } from './object-store.js';
import { CheckpointStore } from './store.js';
import { fixtureManifest } from '../../test/helpers.js';

function manifest(overrides: Partial<SnapshotManifest> = {}): SnapshotManifest {
  return fixtureManifest({ snapshotId: 's-1', league: 'Settlers of Kalguur', ...overrides });
}

async function seedRaw(store: MemoryObjectStore, league: string, body: string): Promise<void> {
  await store.put(checkpointPath(league), new TextEncoder().encode(body));
}

describe('CheckpointStore', () => {
  it('returns undefined when no checkpoint exists', async () => {
    const store = new CheckpointStore(new MemoryObjectStore());
    expect(await store.load('Standard')).toBeUndefined();
  });

  it('round-trips a manifest at the canonical checkpoint path', async () => {
    const objects = new MemoryObjectStore();
    const store = new CheckpointStore(objects);
    const m = manifest();

    await store.save(m);
    expect(objects.keys()).toEqual([checkpointPath(m.league)]);
    expect(await store.load(m.league)).toEqual(m);
  });

  it('overwrites the previous checkpoint on save (single writer)', async () => {
    const store = new CheckpointStore(new MemoryObjectStore());
    await store.save(manifest());
    await store.save(manifest({ phase: 'transforming', resolvedChunks: 5 }));
    const loaded = await store.load('Settlers of Kalguur');
    expect(loaded?.phase).toBe('transforming');
    expect(loaded?.resolvedChunks).toBe(5);
  });

  it('clears a checkpoint', async () => {
    const store = new CheckpointStore(new MemoryObjectStore());
    await store.save(manifest());
    await store.clear('Settlers of Kalguur');
    expect(await store.load('Settlers of Kalguur')).toBeUndefined();
  });

  it('treats a foreign/older-schema checkpoint as no checkpoint (never trusts the shape)', async () => {
    const objects = new MemoryObjectStore();
    const store = new CheckpointStore(objects);
    // Missing chunk fields + wrong schemaVersion — trusting it would produce
    // an `undefined` chunk count that silently skips the whole queue.
    await seedRaw(objects, 'Legacy', JSON.stringify({ schemaVersion: 0, league: 'Legacy' }));
    expect(await store.load('Legacy')).toBeUndefined();
  });

  it('treats a non-JSON checkpoint body as no checkpoint', async () => {
    const objects = new MemoryObjectStore();
    const store = new CheckpointStore(objects);
    await seedRaw(objects, 'Corrupt', 'not json at all');
    expect(await store.load('Corrupt')).toBeUndefined();
  });
});
