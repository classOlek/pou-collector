import { describe, expect, it } from 'vitest';
import { chunkPath, isChunkResolved } from '@pou/shared';
import { getJson } from '../checkpoint/object-store.js';
import type { RosterFile, SnapshotChunk } from '@pou/shared';
import { rosterPath } from '@pou/shared';
import { ChunkStore } from '../chunks/chunk-store.js';
import { LEAGUE, entry, makeRunHarness } from '../../test/run-harness.js';
import { buildLadder } from '../../test/mock-api.js';
import { fixtureManifest } from '../../test/helpers.js';

describe('Coordinator: ladder capture → roster merge → chunk seeding', () => {
  it('seeds a fresh snapshot from the whole roster as pending chunks and fans workers out', async () => {
    const entries = buildLadder(23);
    const h = makeRunHarness({ entries, config: { chunkSize: 5, workerCount: 3 } });

    const summary = await h.newCoordinator().runOnce();

    expect(summary.phase).toBe('collecting');
    expect(summary.stopReason).toBe('seeded');
    expect(summary.hasWork).toBe(true);
    expect(summary.workers).toEqual([0, 1, 2]);
    expect(summary.totalCharacters).toBe(23);
    expect(summary.chunkCount).toBe(5); // ceil(23 / 5)
    expect(summary.rosterSize).toBe(23);
    expect(summary.rosterAdded).toBe(23);

    // The roster is the per-league character database (redesign step 1).
    const roster = await getJson<RosterFile>(h.objectStore, rosterPath(LEAGUE));
    expect(roster?.characters).toHaveLength(23);

    // Every character entered the snapshot as `pending` (not computed).
    const chunks = await new ChunkStore(h.objectStore).loadAll(LEAGUE, 'snap-fixed', 5);
    expect(chunks.flatMap((c) => c.characters)).toHaveLength(23);
    expect(chunks.every((c) => c.characters.every((q) => q.outcome === 'pending'))).toBe(true);
    expect(chunks.every((c) => !isChunkResolved(c) && c.shardsWritten === 0)).toBe(true);

    // The manifest rollup starts all-pending.
    const manifest = await h.checkpointStore.load(LEAGUE);
    expect(manifest?.outcomes).toEqual({ pending: 23, ok: 0, private: 0, retryable: 0, dead: 0 });
  });

  it('grows the roster across snapshots: new ladder entrants join, leavers stay', async () => {
    // Snapshot 1 sees characters 0..9.
    const h = makeRunHarness({ entries: buildLadder(10), config: { chunkSize: 4 } });
    await h.newCoordinator().runOnce();

    // Finish snapshot 1 and move past the interval.
    const manifest = await h.checkpointStore.load(LEAGUE);
    await h.checkpointStore.save({
      ...manifest!,
      phase: 'published',
      completedAt: new Date(h.clock.now()).toISOString(),
    });
    h.clock.advance(13 * 3_600_000);

    // Snapshot 2: the ladder rolled — five new entrants pushed five old ones
    // out of the window. The roster (and the new snapshot) hold the union.
    const rolled = [
      ...Array.from({ length: 5 }, (_, i) => entry(`new-${i}`, { kind: 'ok' })),
      ...buildLadder(10).slice(0, 5),
    ];
    const summary = await h.newCoordinatorFor(rolled, 'snap-2').runOnce();

    expect(summary.stopReason).toBe('seeded');
    expect(summary.rosterAdded).toBe(5);
    expect(summary.rosterSize).toBe(15);
    // Every known character — including the five no longer on the ladder —
    // enters the new snapshot as not computed.
    expect(summary.totalCharacters).toBe(15);
    expect(summary.chunkCount).toBe(4); // ceil(15 / 4)
    const roster = await getJson<RosterFile>(h.objectStore, rosterPath(LEAGUE));
    const accounts = new Set(roster?.characters.map((c) => c.account));
    expect(accounts.has('acct-new-0')).toBe(true); // new entrant
    expect(accounts.has('acct-9')).toBe(true); // left the ladder, still known
  });

  it('idles inside the snapshot interval and starts fresh after it', async () => {
    const h = makeRunHarness({ entries: buildLadder(5) });
    const manifest = fixtureManifest({
      league: LEAGUE,
      snapshotId: 'old-snap',
      phase: 'published',
      completedAt: new Date(h.clock.now()).toISOString(),
    });
    await h.checkpointStore.save(manifest);

    const idle = await h.newCoordinator().runOnce();
    expect(idle.stopReason).toBe('idle');
    expect(idle.hasWork).toBe(false);
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('old-snap');

    h.clock.advance(13 * 3_600_000);
    const fresh = await h.newCoordinator().runOnce();
    expect(fresh.stopReason).toBe('seeded');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('snap-fixed');
  });

  it('honors the abort cooldown before retrying an aborted snapshot', async () => {
    const h = makeRunHarness({ entries: buildLadder(5) });
    await h.checkpointStore.save(
      fixtureManifest({
        league: LEAGUE,
        snapshotId: 'dead-snap',
        phase: 'aborted',
        abortedAt: new Date(h.clock.now()).toISOString(),
      }),
    );

    expect((await h.newCoordinator().runOnce()).stopReason).toBe('idle');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('dead-snap');

    h.clock.advance(7 * 3_600_000);
    const fresh = await h.newCoordinator().runOnce();
    expect(fresh.stopReason).toBe('seeded');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('snap-fixed');
  });

  it('recovers from transient ladder throttling and still seeds the full queue', async () => {
    const h = makeRunHarness({ entries: buildLadder(5), ladderThrottleFirst: 2 });
    const summary = await h.newCoordinator().runOnce();
    expect(summary.stopReason).toBe('seeded');
    expect(summary.totalCharacters).toBe(5);
  });

  it('aborts ladder capture when a page fails past maxAttempts (no unbounded retry)', async () => {
    const h = makeRunHarness({
      entries: buildLadder(5),
      config: { maxAttempts: 2 },
      ladderThrottleFirst: 50,
    });

    const summary = await h.newCoordinator().runOnce();

    expect(summary.phase).toBe('aborted');
    expect(summary.stopReason).toBe('aborted');
    expect((await h.checkpointStore.load(LEAGUE))?.abortedAt).toBeDefined();
  });

  it('stays in ladder_capture (no partial queue) when the run budget expires mid-capture', async () => {
    // Three ladder pages, each "taking" 600 ms of wall clock: the budget check
    // before page 3 trips and the capture restarts cleanly next run.
    const h = makeRunHarness({ entries: buildLadder(600), config: { maxRunMillis: 1000 } });
    const slowClient: Parameters<typeof h.newCoordinator>[0] = (req) => {
      h.clock.advance(600);
      return h.api.client(req);
    };
    const summary = await h.newCoordinator(slowClient).runOnce();
    expect(summary.stopReason).toBe('budget_exhausted');
    expect((await h.checkpointStore.load(LEAGUE))?.phase).toBe('ladder_capture');
    // No chunks were seeded — a partial ladder never becomes a queue.
    expect(h.objectStore.keys().some((k) => k.includes('/chunks/'))).toBe(false);
  });

  it('reports work pending (no reseed) for an in-flight collecting snapshot', async () => {
    const h = makeRunHarness({ entries: buildLadder(6), config: { chunkSize: 3 } });
    await h.newCoordinator().runOnce();
    const before = await getJson<SnapshotChunk>(h.objectStore, chunkPath(LEAGUE, 'snap-fixed', 0));

    const again = await h.newCoordinator().runOnce();

    expect(again.stopReason).toBe('work_pending');
    expect(again.hasWork).toBe(true);
    // Chunks were not reseeded (same object, still pending).
    const after = await getJson<SnapshotChunk>(h.objectStore, chunkPath(LEAGUE, 'snap-fixed', 0));
    expect(after).toEqual(before);
  });
});
