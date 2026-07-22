import { describe, expect, it } from 'vitest';
import type { SnapshotManifest, SnapshotPhase } from '@classolek/shared';
import { MemoryObjectStore } from './checkpoint/object-store.js';
import { CheckpointStore } from './checkpoint/store.js';
import { resetAbortedCheckpoints, shouldResetAborted } from './reset-aborted.js';
import { fixtureManifest } from '../test/helpers.js';

function manifest(league: string, phase: SnapshotPhase): SnapshotManifest {
  return fixtureManifest({ snapshotId: `${league}-snap`, league, phase });
}

describe('shouldResetAborted', () => {
  it('is true only for an explicit true-ish input', () => {
    expect(shouldResetAborted('true')).toBe(true);
    expect(shouldResetAborted('1')).toBe(true);
    // A boolean workflow input renders 'false' when unchecked; schedule runs
    // leave the env empty — neither must trigger a reset.
    expect(shouldResetAborted('false')).toBe(false);
    expect(shouldResetAborted('')).toBe(false);
    expect(shouldResetAborted(undefined)).toBe(false);
  });
});

describe('resetAbortedCheckpoints', () => {
  it('clears aborted checkpoints and leaves every other phase untouched', async () => {
    const cs = new CheckpointStore(new MemoryObjectStore());
    await cs.save(manifest('Dead', 'aborted'));
    await cs.save(manifest('Busy', 'collecting'));
    await cs.save(manifest('Done', 'published'));

    const cleared = await resetAbortedCheckpoints(cs);

    expect(cleared).toEqual(['Dead']);
    expect(await cs.load('Dead')).toBeUndefined();
    expect((await cs.load('Busy'))?.phase).toBe('collecting');
    expect((await cs.load('Done'))?.phase).toBe('published');
  });

  it('is a no-op when nothing is aborted', async () => {
    const cs = new CheckpointStore(new MemoryObjectStore());
    await cs.save(manifest('Busy', 'collecting'));

    expect(await resetAbortedCheckpoints(cs)).toEqual([]);
    expect((await cs.load('Busy'))?.phase).toBe('collecting');
  });
});
