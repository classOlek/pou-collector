import { describe, expect, it } from 'vitest';
import type { SnapshotManifest, SnapshotPhase } from '@pou/shared';
import { MemoryObjectStore } from './checkpoint/object-store.js';
import { CheckpointStore } from './checkpoint/store.js';
import { selectCollectLeague } from './select-league.js';
import { fixtureManifest } from '../test/helpers.js';

function manifest(league: string, phase: SnapshotPhase): SnapshotManifest {
  return fixtureManifest({ snapshotId: `${league}-snap`, league, phase });
}

describe('selectCollectLeague', () => {
  it('resumes an in-flight league started by a workflow_dispatch override (finding 8)', async () => {
    const store = new MemoryObjectStore();
    const cs = new CheckpointStore(store);
    // Configured league is Standard, but a dispatch left an in-flight snapshot for another league.
    await cs.save(manifest('Overridden', 'collecting'));

    expect(await selectCollectLeague(cs, 'Standard')).toBe('Overridden');
  });

  it('prefers the configured league when it is itself in-flight', async () => {
    const store = new MemoryObjectStore();
    const cs = new CheckpointStore(store);
    await cs.save(manifest('Standard', 'collecting'));
    await cs.save(manifest('Other', 'transforming'));

    expect(await selectCollectLeague(cs, 'Standard')).toBe('Standard');
  });

  it('falls back to the configured league when nothing is in-flight', async () => {
    const store = new MemoryObjectStore();
    const cs = new CheckpointStore(store);
    await cs.save(manifest('Old', 'published')); // not in-flight
    await cs.save(manifest('Dead', 'aborted')); // not in-flight

    expect(await selectCollectLeague(cs, 'Standard')).toBe('Standard');
  });
});
