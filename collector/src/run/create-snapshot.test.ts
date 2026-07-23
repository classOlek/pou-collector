/**
 * SnapshotCreator: the new-snapshot workflow's idle-gated seed pass.
 * Request-free — it seeds the queue from the roster the build-roster step
 * maintains (chunk seeding). Unforced fires no-op while a snapshot is live;
 * only a FORCED fire closes the previous snapshot via `skipped` marking.
 * Ladder capture / roster merge live in build-roster.test.ts.
 */
import { describe, expect, it } from 'vitest';
import type { SnapshotChunk, SnapshotCharacter, SnapshotMeta } from '@classolek/shared';
import {
  chunkPath,
  isChunkResolved,
  pendingOfTally,
  snapshotMetaPath,
  snapshotStatePath,
  workerResultPath,
} from '@classolek/shared';
import { getJson, putJson } from '../checkpoint/object-store.js';
import { liveSnapshots } from './create-snapshot.js';
import { ChunkStore } from '../chunks/chunk-store.js';
import { readState } from '../snapshot-state/state-store.js';
import { HOUR_MS } from './config.js';
import { LEAGUE, entry, makeRunHarness } from '../../test/run-harness.js';
import { buildLadder } from '../../test/mock-api.js';
import { fixtureManifest, tallyOutcomes } from '../../test/helpers.js';

/** Drain a state-file stream into an array (small in these fixtures). */
async function readAllState(
  h: ReturnType<typeof makeRunHarness>,
  snapshotId: string,
): Promise<SnapshotCharacter[]> {
  const out: SnapshotCharacter[] = [];
  for await (const c of readState(h.objectStore, LEAGUE, snapshotId)) out.push(c);
  return out;
}

const okLadder = (n: number) =>
  Array.from({ length: n }, (_, i) => entry(String(i), { kind: 'ok' }));

describe('SnapshotCreator: seeding the queue from the roster', () => {
  it('seeds a fresh snapshot from the whole roster as pending chunks', async () => {
    const h = makeRunHarness({
      entries: buildLadder(23),
      config: { chunkSize: 5, workerCount: 3 },
    });

    // createFire builds the roster, then seeds the new snapshot from it.
    const summary = await h.createFire();

    expect(summary.stopReason).toBe('created');
    expect(summary.closed).toBeUndefined();
    expect(summary.totalCharacters).toBe(23);
    expect(summary.chunkCount).toBe(5); // ceil(23 / 5)
    expect(summary.rosterSize).toBe(23);

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

  it('seeds the single NDJSON.gz state file with every character pending in roster order', async () => {
    const h = makeRunHarness({
      entries: buildLadder(23),
      config: { chunkSize: 5, workerCount: 3 },
    });

    await h.createFire();

    // The v4 state file exists as one gzipped object at the canonical path…
    const key = snapshotStatePath(LEAGUE, 'snap-fixed');
    expect(h.objectStore.keys()).toContain(key);
    const raw = await h.objectStore.get(key);
    expect(raw?.[0]).toBe(0x1f); // gzip magic
    expect(raw?.[1]).toBe(0x8b);

    // …one line per character, every one pending with zero attempts and NO raw
    // payloads, in the same roster order the chunks were seeded in.
    const state = await readAllState(h, 'snap-fixed');
    expect(state).toHaveLength(23);
    expect(state.every((c) => c.outcome === 'pending' && c.attempts === 0)).toBe(true);
    expect(state.every((c) => !('characterData' in c) && !('passiveTree' in c))).toBe(true);
    const chunks = await new ChunkStore(h.objectStore).loadAll(LEAGUE, 'snap-fixed', 5);
    expect(state.map((c) => c.account)).toEqual(
      chunks.flatMap((c) => c.characters).map((q) => q.account),
    );
  });

  it('skips cleanly when the roster is empty (build-roster has not run yet)', async () => {
    const h = makeRunHarness({ entries: buildLadder(5) });

    // No build ran, so the roster is empty — nothing to seed.
    const summary = await h.newCreator().runOnce();

    expect(summary.stopReason).toBe('empty_roster');
    expect(summary.totalCharacters).toBe(0);
    expect(summary.chunkCount).toBe(0);
    // No snapshot manifest, no chunks and no state file were written.
    expect(await h.checkpointStore.load(LEAGUE)).toBeUndefined();
    expect(h.objectStore.keys().some((k) => k.includes('/chunks/'))).toBe(false);
    expect(h.objectStore.keys().some((k) => k.includes('/snapshots/'))).toBe(false);
  });

  it('seeds the whole current roster, including characters no longer on the ladder', async () => {
    // Build 1: 10 characters. Roll the ladder and build again: the roster grows
    // to 15, and the new snapshot seeds all of them.
    const h = makeRunHarness({ entries: buildLadder(10), config: { chunkSize: 4 } });
    await h.buildFire();
    const rolled = [
      ...Array.from({ length: 5 }, (_, i) => entry(`new-${i}`, { kind: 'ok' })),
      ...buildLadder(10).slice(0, 5),
    ];
    await h.newBuilderFor(rolled).runOnce();

    const summary = await h.newCreator().runOnce();

    expect(summary.stopReason).toBe('created');
    expect(summary.totalCharacters).toBe(15);
    expect(summary.chunkCount).toBe(4); // ceil(15 / 4)
    const chunk = await getJson<SnapshotChunk>(h.objectStore, chunkPath(LEAGUE, 'snap-fixed', 0));
    expect(chunk?.characters.every((q) => q.outcome === 'pending')).toBe(true);
  });

  it('creates immediately once the previous snapshot has published (back-to-back)', async () => {
    const h = makeRunHarness({ entries: buildLadder(5) });
    await h.buildFire(); // roster is non-empty
    await h.checkpointStore.save(
      fixtureManifest({
        league: LEAGUE,
        snapshotId: 'old-snap',
        phase: 'published',
        completedAt: new Date(h.clock.now()).toISOString(),
      }),
    );

    // No wall-clock cadence: a published previous snapshot means idle, so the
    // very next fire seeds — snapshots run back-to-back.
    const fresh = await h.newCreator().runOnce();
    expect(fresh.stopReason).toBe('created');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('snap-fixed');
  });

  it('no-ops while a snapshot is live, leaving its queue untouched', async () => {
    const h = makeRunHarness({ entries: buildLadder(6), config: { chunkSize: 3 } });
    await h.createFire(); // snap-fixed is now collecting

    // Fires while the snapshot is live never close it — hours later included.
    h.clock.advance(30 * HOUR_MS);
    const summary = await h.newCreatorFor('snap-2').runOnce();

    expect(summary.stopReason).toBe('in_flight');
    expect(summary.closed).toBeUndefined();
    const manifest = await h.checkpointStore.load(LEAGUE);
    expect(manifest?.snapshotId).toBe('snap-fixed');
    expect(manifest?.phase).toBe('collecting');
    // Every queued character is still pending — nothing was marked skipped.
    const chunk = await getJson<SnapshotChunk>(h.objectStore, chunkPath(LEAGUE, 'snap-fixed', 0));
    expect(chunk?.characters.every((q) => q.outcome === 'pending')).toBe(true);
  });

  it('honors the abort cooldown before retrying an aborted snapshot', async () => {
    const h = makeRunHarness({ entries: buildLadder(5) });
    await h.buildFire();
    await h.checkpointStore.save(
      fixtureManifest({
        league: LEAGUE,
        snapshotId: 'dead-snap',
        phase: 'aborted',
        abortedAt: new Date(h.clock.now()).toISOString(),
      }),
    );

    expect((await h.newCreator().runOnce()).stopReason).toBe('cooldown');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('dead-snap');

    h.clock.advance(7 * HOUR_MS);
    const fresh = await h.newCreator().runOnce();
    expect(fresh.stopReason).toBe('created');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('snap-fixed');
  });
});

describe('SnapshotCreator: closing the previous snapshot (forced fires only)', () => {
  it('a forced fire marks uncollected characters skipped and publishes the closed snapshot', async () => {
    const entries = okLadder(10);
    const h = makeRunHarness({ entries, config: { chunkSize: 5, workerCount: 2 } });
    await h.createFire(); // snap-fixed: chunk 0 → w0, chunk 1 → w1

    // Only worker 0 runs: chunk 0 (5 chars) resolves, chunk 1 stays pending.
    await h.newWorker(0).runOnce();
    await h.newFinalizer().runOnce(); // rollup + incremental publish

    const summary = await h.newCreatorFor('snap-2', true).runOnce();

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

    // The closed snapshot's state file was rewritten so NOTHING remains pending
    // — every line reached a terminal outcome. (In this phase the worker still
    // updates chunks, not the state file, so every seeded-pending line closes to
    // `skipped`; once Phase 4 wires workers→state the collected lines stay `ok`.)
    const closedState = await readAllState(h, 'snap-fixed');
    expect(pendingOfTally(tallyOutcomes(closedState))).toBe(0);
    expect(tallyOutcomes(closedState).skipped).toBeGreaterThan(0);
    // The fresh snapshot's state file is seeded all-pending.
    expect((await readAllState(h, 'snap-2')).every((c) => c.outcome === 'pending')).toBe(true);
  });

  it('a forced fire closes a snapshot with zero collected characters as a clean abort, then creates', async () => {
    const entries = okLadder(6);
    const h = makeRunHarness({ entries, config: { chunkSize: 3 } });
    await h.createFire(); // nothing collected afterwards

    // Give the closed snapshot a stray result file to prove discard sweeps it.
    await h.objectStore.put(workerResultPath(LEAGUE, 'snap-fixed', 0), new Uint8Array([1]));

    const summary = await h.newCreatorFor('snap-2', true).runOnce();

    expect(summary.closed?.result).toBe('aborted');
    expect(summary.closed?.skippedMarked).toBe(6);
    // Nothing published for the aborted snapshot…
    expect(
      await getJson<SnapshotMeta>(h.objectStore, snapshotMetaPath(LEAGUE, 'snap-fixed')),
    ).toBeUndefined();
    // …the aborted snapshot's state file and result files were discarded…
    expect(h.objectStore.keys()).not.toContain(snapshotStatePath(LEAGUE, 'snap-fixed'));
    expect(h.objectStore.keys().some((k) => k.includes('/results/snap-fixed/'))).toBe(false);
    // …and the new snapshot still got created (with its own seeded state file).
    expect(summary.stopReason).toBe('created');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('snap-2');
    expect(h.objectStore.keys()).toContain(snapshotStatePath(LEAGUE, 'snap-2'));
  });

  it('a forced fire closes and recreates where an unforced fire no-ops', async () => {
    const entries = okLadder(4);
    const h = makeRunHarness({ entries, config: { chunkSize: 2 } });
    await h.createFire();

    // The snapshot is live — an unforced fire is idle-gated…
    expect((await h.newCreatorFor('snap-x').runOnce()).stopReason).toBe('in_flight');

    // …but a forced (operator dispatch) fire closes and recreates now.
    const forced = await h.newCreatorFor('snap-2', true).runOnce();
    expect(forced.stopReason).toBe('created');
    expect(forced.closed?.snapshotId).toBe('snap-fixed');
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('snap-2');
  });

  it('an unforced fire discards a never-seeded snapshot stuck in ladder_capture and reseeds', async () => {
    // ladder_capture is a crashed seed, not live work — the idle gate lets an
    // ordinary roster-triggered fire clean it up without operator force.
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

  it('discards a foreign-schema (v3) in-flight snapshot and reseeds fresh (v3→v4 migration)', async () => {
    // A v3 snapshot in flight at deploy: its current.json fails v4 validation, so
    // it is invisible to load/listAll and would never trip the idle gate or the
    // close loop. The create fire must still discard its orphaned artifacts.
    const h = makeRunHarness({ entries: okLadder(4), config: { chunkSize: 2 } });
    await h.buildFire(); // roster is non-empty so the reseed can proceed

    await putJson(h.objectStore, chunkPath(LEAGUE, 'v3-snap', 0), { legacy: true });
    await putJson(h.objectStore, snapshotStatePath(LEAGUE, 'v3-snap'), { legacy: true });
    // A v3 manifest (foreign schemaVersion) parked in `collecting`.
    await putJson(h.objectStore, `state/${LEAGUE}/current.json`, {
      schemaVersion: 3,
      snapshotId: 'v3-snap',
      league: LEAGUE,
      depth: 4,
      phase: 'collecting',
      ladderCapturedAt: new Date(h.clock.now()).toISOString(),
    });

    // Sanity: the v3 manifest is opaque to the v4 loader before the fire.
    expect(await h.checkpointStore.load(LEAGUE)).toBeUndefined();

    const summary = await h.newCreator().runOnce();

    expect(summary.stopReason).toBe('created');
    // The v3 snapshot's artifacts are gone…
    expect(h.objectStore.keys().some((k) => k.includes('/chunks/v3-snap/'))).toBe(false);
    expect(h.objectStore.keys()).not.toContain(snapshotStatePath(LEAGUE, 'v3-snap'));
    // …and a fresh v4 snapshot was seeded in its place.
    const manifest = await h.checkpointStore.load(LEAGUE);
    expect(manifest?.snapshotId).toBe('snap-fixed');
    expect(manifest?.phase).toBe('collecting');
    expect((await readAllState(h, 'snap-fixed')).every((c) => c.outcome === 'pending')).toBe(true);
  });

  it('leaves a foreign-schema PUBLISHED snapshot untouched and seeds over it (immutable)', async () => {
    // A v3 PUBLISHED snapshot is immutable (hard rule #4): its files and index
    // entry stay for v3 readers; only the checkpoint pointer is overwritten.
    const h = makeRunHarness({ entries: okLadder(4), config: { chunkSize: 2 } });
    await h.buildFire();

    const legacyMeta = snapshotMetaPath(LEAGUE, 'v3-done');
    await putJson(h.objectStore, legacyMeta, { schemaVersion: 3, complete: true });
    await putJson(h.objectStore, `state/${LEAGUE}/current.json`, {
      schemaVersion: 3,
      snapshotId: 'v3-done',
      league: LEAGUE,
      depth: 4,
      phase: 'published',
      ladderCapturedAt: new Date(h.clock.now()).toISOString(),
    });

    const summary = await h.newCreator().runOnce();

    expect(summary.stopReason).toBe('created');
    // The published v3 files are preserved — nothing discarded.
    expect(h.objectStore.keys()).toContain(legacyMeta);
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('snap-fixed');
  });
});

describe('liveSnapshots (the idle gate / snapshot-idle predicate)', () => {
  it('counts collecting/transforming as live; remnants and settled phases as idle', () => {
    const live = liveSnapshots([
      fixtureManifest({ snapshotId: 'a', phase: 'collecting' }),
      fixtureManifest({ snapshotId: 'b', phase: 'transforming' }),
      // A ladder_capture remnant is a crashed seed the next fire discards —
      // reporting it live would wedge creation forever.
      fixtureManifest({ snapshotId: 'c', phase: 'ladder_capture' }),
      fixtureManifest({ snapshotId: 'd', phase: 'published' }),
      fixtureManifest({ snapshotId: 'e', phase: 'aborted' }),
    ]);
    expect(live.map((s) => s.snapshotId)).toEqual(['a', 'b']);
  });
});
