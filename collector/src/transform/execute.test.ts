import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { snapshotStatePath } from '@classolek/shared';
import { MemoryObjectStore } from '../checkpoint/object-store.js';
import { CheckpointStore } from '../checkpoint/store.js';
import { FakeClock } from '../rate-limit/clock.js';
import { writeState } from '../snapshot-state/state-store.js';
import { CachedTreeSource, type PassiveTree, type TreeOrigin } from './tree-source.js';
import { executeTransform, type TransformStepConfig } from './execute.js';
import type { TransformDeps } from './transform.js';
import {
  buildStateLine,
  FakeTreeOrigin,
  transformingManifest,
} from '../../test/transform-fixtures.js';

const LEAGUE = 'TestLeague';
const SNAP = 'snap-x';
const OK_SPECS = [
  {
    rank: 1,
    account: 'a',
    character: 'A',
    class: 'Juggernaut',
    mainSkill: 'Cyclone',
    nodes: [123, 4271],
  },
];

/** A tree origin that always fails — forces runTransform to reject. */
class FailingOrigin implements TreeOrigin {
  fetch(): Promise<PassiveTree> {
    return Promise.reject(new Error('tree unreachable'));
  }
}

let tmpRoot: string;
beforeEach(async () => {
  tmpRoot = await mkdtemp(join(tmpdir(), 'pou-exec-'));
});
afterEach(async () => {
  await rm(tmpRoot, { recursive: true, force: true });
});

function makeDeps(store: MemoryObjectStore, origin: TreeOrigin): TransformDeps {
  return {
    clock: new FakeClock(Date.parse('2026-07-18T00:00:00.000Z')),
    objectStore: store,
    checkpointStore: new CheckpointStore(store),
    treeSource: new CachedTreeSource(store, origin),
    tmpRoot,
  };
}

const CONFIG: TransformStepConfig = { treeVersion: '3.25-test', maxTransformAttempts: 2 };

describe('executeTransform anti-wedge', () => {
  it('aborts a drained snapshot with zero collected characters (all private/dead)', async () => {
    const store = new MemoryObjectStore();
    // Queue is all private/dead → ok === 0. A state file exists to be cleaned.
    const manifest = transformingManifest(LEAGUE, SNAP, [], { private: 2, dead: 1 });
    await writeState(store, LEAGUE, SNAP, []);
    const deps = makeDeps(store, new FakeTreeOrigin());
    await deps.checkpointStore.save(manifest);

    const outcome = await executeTransform(manifest, CONFIG, deps);

    expect(outcome).toEqual({ kind: 'aborted', reason: 'no_characters' });
    const cp = await deps.checkpointStore.load(LEAGUE);
    expect(cp?.phase).toBe('aborted');
    expect(cp?.abortedAt).toBeDefined();
    expect(store.keys()).not.toContain(snapshotStatePath(LEAGUE, SNAP));
  });

  it('advances transformAttempts and surfaces the error below the ceiling, then aborts at it', async () => {
    const store = new MemoryObjectStore();
    await writeState(
      store,
      LEAGUE,
      SNAP,
      OK_SPECS.map((s) => buildStateLine(s)),
    );
    const manifest = transformingManifest(LEAGUE, SNAP, OK_SPECS);
    const deps = makeDeps(store, new FailingOrigin());
    await deps.checkpointStore.save(manifest);

    // Attempt 1: below max → records the attempt, rethrows, phase stays
    // transforming, the state file kept.
    await expect(executeTransform(manifest, CONFIG, deps)).rejects.toThrow(/tree unreachable/);
    const afterFirst = await deps.checkpointStore.load(LEAGUE);
    expect(afterFirst?.transformAttempts).toBe(1);
    expect(afterFirst?.phase).toBe('transforming');
    expect(store.keys()).toContain(snapshotStatePath(LEAGUE, SNAP));

    // Attempt 2 reaches the ceiling → clean abort, state deleted (unpublishable).
    const outcome = await executeTransform(afterFirst!, CONFIG, deps);
    expect(outcome).toEqual({ kind: 'aborted', reason: 'max_transform_attempts' });
    const afterSecond = await deps.checkpointStore.load(LEAGUE);
    expect(afterSecond?.phase).toBe('aborted');
    expect(afterSecond?.abortedAt).toBeDefined();
    expect(store.keys()).not.toContain(snapshotStatePath(LEAGUE, SNAP));
  });

  it('publishes on a healthy transform', async () => {
    const store = new MemoryObjectStore();
    await writeState(
      store,
      LEAGUE,
      SNAP,
      OK_SPECS.map((s) => buildStateLine(s)),
    );
    const manifest = transformingManifest(LEAGUE, SNAP, OK_SPECS);
    const deps = makeDeps(store, new FakeTreeOrigin());
    await deps.checkpointStore.save(manifest);

    const outcome = await executeTransform(manifest, CONFIG, deps);
    expect(outcome.kind).toBe('published');
    expect((await deps.checkpointStore.load(LEAGUE))?.phase).toBe('published');
  });
});
