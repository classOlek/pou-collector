/**
 * SnapshotCreator: the create workflow's close-previous + create-new pass
 * (ladder capture → roster merge → chunk seeding moved here from the old
 * coordinator, plus closing via `skipped` marking).
 */
import { describe, expect, it } from 'vitest';
import type { RosterFile, SnapshotChunk, SnapshotMeta } from '@pou/shared';
import { chunkPath, isChunkResolved, rosterPath, snapshotMetaPath } from '@pou/shared';
import { getJson } from '../checkpoint/object-store.js';
import { ChunkStore } from '../chunks/chunk-store.js';
import { HOUR_MS } from './config.js';
import { LEAGUE, entry, makeRunHarness } from '../../test/run-harness.js';
import { buildLadder } from '../../test/mock-api.js';
import { fixtureManifest } from '../../test/helpers.js';

const okLadder = (n: number) =>
  Array.from({ length: n }, (_, i) => entry(String(i), { kind: 'ok' }));

describe('SnapshotCreator: ladder capture → roster merge → chunk seeding', () => {
  it('seeds a fresh snapshot from the whole roster as pending chunks', async () => {
    const entries = buildLadder(23);
    const h = makeRunHarness({ entries, config: { chunkSize: 5, workerCount: 3 } });

    const summary = await h.createFire();

    expect(summary.stopReason).toBe('created');
    expect(summary.closed).toBeUndefined();
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

    // The manifest rollup starts all-pending; the collect coordinate fans out.
    const manifest = await h.checkpointStore.load(LEAGUE);
    expect(manifest?.phase).toBe('collecting');
    expect(manifest?.outcomes.pending).toBe(23);
    const coordinate = await h.newCoordinator().runOnce();
    expect(coordinate.stopReason).toBe('work_pending');
    expect(coordinate.workers).toEqual([0, 1, 2]);
  });

  it('grows the roster across snapshots: new ladder entrants join, leavers stay', async () => {
    // Snapshot 1 sees characters 0..9.
    const h = makeRunHarness({ entries: buildLadder(10), config: { chunkSize: 4 } });
    await h.createFire();

    // Finish snapshot 1 and move past the interval.
    const manifest = await h.checkpointStore.load(LEAGUE);
    await h.checkpointStore.save({
      ...manifest!,
      phase: 'published',
      completedAt: new Date(h.clock.now()).toISOString(),
    });
    h.clock.advance(13 * HOUR_MS);

    // Snapshot 2: the ladder rolled — five new entrants pushed five old ones
    // out of the window. The roster (and the new snapshot) hold the union.
    const rolled = [
      ...Array.from({ length: 5 }, (_, i) => entry(`new-${i}`, { kind: 'ok' })),
      ...buildLadder(10).slice(0, 5),
    ];
    const summary = await h.newCreatorFor(rolled, 'snap-2').runOnce();

    expect(summary.stopReason).toBe('created');
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

  it('scheduled fires skip inside the snapshot interval; later fires create', async () => {
    const h = makeRunHarness({ entries: buildLadder(5) });
    await h.checkpointStore.save(
      fixtureManifest({
        league: LEAGUE,
        snapshotId: 'old-snap',
        phase: 'published',
        completedAt: new Date(h.clock.now()).toISOString(),
      }),
    );

    const idle = await h.createFire();
    expect(idle.stopReason).toBe('too_recent');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('old-snap');

    h.clock.advance(13 * HOUR_MS);
    const fresh = await h.createFire();
    expect(fresh.stopReason).toBe('created');
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

    expect((await h.createFire()).stopReason).toBe('cooldown');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('dead-snap');

    h.clock.advance(7 * HOUR_MS);
    const fresh = await h.createFire();
    expect(fresh.stopReason).toBe('created');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('snap-fixed');
  });

  it('recovers from transient ladder throttling and still seeds the full queue', async () => {
    const h = makeRunHarness({ entries: buildLadder(5), ladderThrottleFirst: 2 });
    const summary = await h.createFire();
    expect(summary.stopReason).toBe('created');
    expect(summary.totalCharacters).toBe(5);
  });

  it('aborts ladder capture when a page fails past maxAttempts (no unbounded retry)', async () => {
    const h = makeRunHarness({
      entries: buildLadder(5),
      config: { maxAttempts: 2 },
      ladderThrottleFirst: 50,
    });

    const summary = await h.createFire();

    expect(summary.stopReason).toBe('aborted');
    expect((await h.checkpointStore.load(LEAGUE))?.abortedAt).toBeDefined();
  });

  it('stays in ladder_capture (no partial queue) when the run budget expires mid-capture', async () => {
    // Three ladder pages, each "taking" 600 ms of wall clock: the budget check
    // before page 3 trips and the capture restarts cleanly next create fire.
    const h = makeRunHarness({ entries: buildLadder(600), config: { maxRunMillis: 1000 } });
    const slowClient: Parameters<typeof h.newCreator>[0] = (req) => {
      h.clock.advance(600);
      return h.api.client(req);
    };
    const summary = await h.newCreator(slowClient).runOnce();
    expect(summary.stopReason).toBe('budget_exhausted');
    expect((await h.checkpointStore.load(LEAGUE))?.phase).toBe('ladder_capture');
    // No chunks were seeded — a partial ladder never becomes a queue.
    expect(h.objectStore.keys().some((k) => k.includes('/chunks/'))).toBe(false);
  });
});

describe('SnapshotCreator: closing the previous snapshot', () => {
  it('marks uncollected characters skipped and publishes the closed snapshot', async () => {
    const entries = okLadder(10);
    const h = makeRunHarness({ entries, config: { chunkSize: 5, workerCount: 2 } });
    await h.createFire(); // snap-fixed: chunk 0 → w0, chunk 1 → w1

    // Only worker 0 runs: chunk 0 (5 chars) resolves, chunk 1 stays pending.
    await h.newWorker(0).runOnce();
    await h.newFinalizer().runOnce(); // rollup + incremental publish

    h.clock.advance(13 * HOUR_MS);
    const summary = await h.newCreatorFor(entries, 'snap-2').runOnce();

    expect(summary.stopReason).toBe('created');
    expect(summary.closed).toEqual({
      league: LEAGUE,
      snapshotId: 'snap-fixed',
      result: 'published',
      skippedMarked: 5,
    });

    // The closed snapshot published immutably with honest skip accounting.
    const meta = await getJson<SnapshotMeta>(h.objectStore, snapshotMetaPath(LEAGUE, 'snap-fixed'));
    expect(meta?.complete).toBe(true);
    expect(meta?.pendingCount).toBe(0);
    expect(meta?.skippedCount).toBe(5);
    expect(meta?.coverage.ok).toBe(5);
    expect(meta?.totalCharacters).toBe(10);

    // Its chunk queue is spent; the NEW snapshot's chunks exist and are pending.
    expect(h.objectStore.keys().some((k) => k.includes('/chunks/snap-fixed/'))).toBe(false);
    const chunk = await getJson<SnapshotChunk>(h.objectStore, chunkPath(LEAGUE, 'snap-2', 0));
    expect(chunk?.characters.every((q) => q.outcome === 'pending')).toBe(true);
    const manifest = await h.checkpointStore.load(LEAGUE);
    expect(manifest?.snapshotId).toBe('snap-2');
    expect(manifest?.phase).toBe('collecting');
    expect(manifest?.totalCharacters).toBe(10);
  });

  it('closes a snapshot with zero collected characters as a clean abort, then creates', async () => {
    const entries = okLadder(6);
    const h = makeRunHarness({ entries, config: { chunkSize: 3 } });
    await h.createFire(); // nothing collected afterwards

    h.clock.advance(13 * HOUR_MS);
    const summary = await h.newCreatorFor(entries, 'snap-2').runOnce();

    expect(summary.closed?.result).toBe('aborted');
    expect(summary.closed?.skippedMarked).toBe(6);
    // Nothing published for the aborted snapshot…
    expect(
      await getJson<SnapshotMeta>(h.objectStore, snapshotMetaPath(LEAGUE, 'snap-fixed')),
    ).toBeUndefined();
    // …and the new snapshot still got created.
    expect(summary.stopReason).toBe('created');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('snap-2');
  });

  it('a dispatch fire (force) closes and recreates regardless of the interval guard', async () => {
    const entries = okLadder(4);
    const h = makeRunHarness({ entries, config: { chunkSize: 2 } });
    await h.createFire();

    // Same hour — a scheduled fire would skip with too_recent…
    expect((await h.newCreatorFor(entries, 'snap-x').runOnce()).stopReason).toBe('too_recent');

    // …but a forced (workflow_dispatch) fire closes and recreates now.
    const forced = await h.newCreatorFor(entries, 'snap-2', true).runOnce();
    expect(forced.stopReason).toBe('created');
    expect(forced.closed?.snapshotId).toBe('snap-fixed');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('snap-2');
  });

  it('discards a never-seeded snapshot stuck in ladder_capture and recaptures', async () => {
    const entries = okLadder(4);
    const h = makeRunHarness({ entries, config: { chunkSize: 2 } });
    await h.checkpointStore.save(
      fixtureManifest({
        league: LEAGUE,
        snapshotId: 'stuck-snap',
        phase: 'ladder_capture',
        ladderCapturedAt: new Date(h.clock.now() - 14 * HOUR_MS).toISOString(),
      }),
    );

    const summary = await h.createFire();
    expect(summary.closed).toEqual({
      league: LEAGUE,
      snapshotId: 'stuck-snap',
      result: 'discarded',
      skippedMarked: 0,
    });
    expect(summary.stopReason).toBe('created');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('snap-fixed');
  });
});
