import { describe, expect, it } from 'vitest';
import type { IndexFile, SnapshotMeta } from '@classolek/shared';
import {
  INDEX_PATH,
  ipPacePath,
  snapshotMetaPath,
  snapshotStatePath,
  workerStatePath,
} from '@classolek/shared';
import { getJson } from '../checkpoint/object-store.js';
import { PaceStateStore } from '../rate-limit/pace-store.js';
import type { PassiveTree, TreeOrigin } from '../transform/tree-source.js';
import { HOUR_MS } from './config.js';
import { LEAGUE, entry, makeRunHarness } from '../../test/run-harness.js';
import { buildLadder } from '../../test/mock-api.js';

/** A tree origin that always fails — forces every transform to reject. */
class FailingOrigin implements TreeOrigin {
  fetch(): Promise<PassiveTree> {
    return Promise.reject(new Error('tree unreachable'));
  }
}

describe('Finalizer rollup and incremental publish', () => {
  it('publishes collected-so-far data as an incomplete snapshot while chunks remain', async () => {
    const entries = buildLadder(20);
    // One worker with a tight budget: only part of the queue resolves this run.
    const h = makeRunHarness({
      entries,
      config: { chunkSize: 5, workerCount: 1, maxRunMillis: 30_000 },
    });
    await h.createFire();
    const worker = await h.newWorker(0).runOnce();
    expect(worker.stopReason).toBe('budget_exhausted');

    const summary = await h.newFinalizer().runOnce();

    expect(summary.stopReason).toBe('published_partial');
    expect(summary.phase).toBe('collecting');
    expect(summary.transform?.complete).toBe(false);

    // The rollup landed in the manifest…
    const manifest = await h.checkpointStore.load(LEAGUE);
    expect(manifest?.outcomes.ok).toBeGreaterThan(0);
    expect(manifest?.outcomes.pending).toBeGreaterThan(0);
    // …and the snapshot is immediately visible, marked incomplete.
    const meta = await getJson<SnapshotMeta>(h.objectStore, snapshotMetaPath(LEAGUE, 'snap-fixed'));
    expect(meta?.complete).toBe(false);
    expect(meta?.pendingCount).toBeGreaterThan(0);
    expect(meta?.characterCount).toBe(manifest?.outcomes.ok);
    const index = await getJson<IndexFile>(h.objectStore, INDEX_PATH);
    expect(index?.leagues[0]?.snapshots[0]?.complete).toBe(false);
    // The state file (the v4 raw) stays — the final transform still needs it.
    expect(h.objectStore.keys()).toContain(snapshotStatePath(LEAGUE, 'snap-fixed'));
  });

  it('skips publishing when nothing has been collected yet', async () => {
    const h = makeRunHarness({ entries: buildLadder(5) });
    await h.createFire();
    // No worker ran — everything is still pending.
    const summary = await h.newFinalizer().runOnce();
    expect(summary.stopReason).toBe('collecting');
    expect(h.objectStore.keys().some((k) => k.startsWith('snapshots/'))).toBe(false);
  });

  it('treats a failed incremental publish as a warning, not an abort', async () => {
    const h = makeRunHarness({
      entries: buildLadder(20),
      config: { chunkSize: 5, workerCount: 1, maxRunMillis: 30_000 },
      treeOrigin: new FailingOrigin(),
    });
    await h.createFire();
    await h.newWorker(0).runOnce();

    const summary = await h.newFinalizer().runOnce();

    expect(summary.stopReason).toBe('partial_publish_failed');
    const manifest = await h.checkpointStore.load(LEAGUE);
    expect(manifest?.phase).toBe('collecting'); // collection continues
    expect(manifest?.transformAttempts ?? 0).toBe(0); // never counts toward the final ceiling
  });

  it('runs the final transform when the last chunk resolves: published, immutable, cleaned up', async () => {
    const entries = buildLadder(10);
    const h = makeRunHarness({ entries, config: { chunkSize: 5, workerCount: 2 } });
    await h.createFire();
    await h.newWorker(0).runOnce();
    await h.newWorker(1).runOnce();

    const summary = await h.newFinalizer().runOnce();

    expect(summary.stopReason).toBe('published_final');
    expect(summary.phase).toBe('published');
    expect(summary.transform?.complete).toBe(true);

    const manifest = await h.checkpointStore.load(LEAGUE);
    expect(manifest?.phase).toBe('published');
    expect(manifest?.completedAt).toBeDefined();
    const meta = await getJson<SnapshotMeta>(h.objectStore, snapshotMetaPath(LEAGUE, 'snap-fixed'));
    expect(meta?.complete).toBe(true);
    expect(meta?.pendingCount).toBe(0);
    // Raw and chunk files are spent state — gone after the final publish.
    expect(h.objectStore.keys().some((k) => k.startsWith('raw/'))).toBe(false);
    expect(h.objectStore.keys().some((k) => k.includes('/chunks/'))).toBe(false);
  });

  it('aborts a drained snapshot with zero public profiles and discards its artifacts', async () => {
    const entries = Array.from({ length: 5 }, (_, i) => entry(`${i}`, { kind: 'private' }));
    const h = makeRunHarness({ entries, config: { chunkSize: 5, workerCount: 1 } });
    await h.createFire();
    await h.newWorker(0).runOnce();

    const summary = await h.newFinalizer().runOnce();

    expect(summary.stopReason).toBe('aborted_no_characters');
    expect((await h.checkpointStore.load(LEAGUE))?.phase).toBe('aborted');
    expect(h.objectStore.keys().some((k) => k.startsWith('raw/'))).toBe(false);
    expect(h.objectStore.keys().some((k) => k.includes('/chunks/'))).toBe(false);
  });

  it('aborts an over-age snapshot, removing its incomplete published files and index entry', async () => {
    const h = makeRunHarness({
      entries: buildLadder(20),
      config: { chunkSize: 5, workerCount: 1, maxRunMillis: 30_000, maxAgeHours: 1 },
    });
    await h.createFire();
    await h.newWorker(0).runOnce();
    // An incremental publish made the partial snapshot visible…
    await h.newFinalizer().runOnce();
    expect(h.objectStore.keys().some((k) => k.startsWith('snapshots/'))).toBe(true);

    // …then the snapshot ages past max_age before it can finish.
    h.clock.advance(2 * 3_600_000);
    const summary = await h.newFinalizer().runOnce();

    expect(summary.stopReason).toBe('aborted');
    expect(summary.phase).toBe('aborted');
    // The zombie snapshot is fully discarded: raw, chunks, published files, index.
    expect(h.objectStore.keys().some((k) => k.startsWith('raw/'))).toBe(false);
    expect(h.objectStore.keys().some((k) => k.includes('/chunks/'))).toBe(false);
    expect(h.objectStore.keys().some((k) => k.startsWith('snapshots/'))).toBe(false);
    const index = await getJson<IndexFile>(h.objectStore, INDEX_PATH);
    expect(index?.leagues.some((l) => l.snapshots.length > 0)).toBe(false);
  });

  it('idles when there is nothing to finalize', async () => {
    const h = makeRunHarness({ entries: buildLadder(5) });
    const summary = await h.newFinalizer().runOnce();
    expect(summary.stopReason).toBe('idle');
    expect(summary.phase).toBe('none');
  });
});

describe('Finalizer per-IP pace-file sweep', () => {
  it('reaps IP files aged past the TTL, keeps fresh ones, leaves other state alone', async () => {
    // paceFileTtlHours defaults to 3 in the harness.
    const h = makeRunHarness({
      entries: buildLadder(10),
      config: { chunkSize: 5, workerCount: 1 },
    });
    await h.createFire(); // seeds the manifest, chunks and a coordinator slot file
    const pace = new PaceStateStore(h.objectStore);

    const fresh = new Date(h.clock.now()).toISOString();
    const stale = new Date(h.clock.now() - 4 * HOUR_MS).toISOString(); // > 3 h TTL
    await pace.save(LEAGUE, '203.0.113.7', [1], fresh);
    await pace.save(LEAGUE, '198.51.100.9', [2], stale);
    // A corrupt IP file (unparseable body) is junk and should be reaped too.
    await h.objectStore.put(ipPacePath(LEAGUE, '10.0.0.1'), new TextEncoder().encode('garbage'));

    await h.newFinalizer().runOnce();

    expect(await pace.load(LEAGUE, '203.0.113.7')).toEqual([1]); // fresh kept
    expect(h.objectStore.keys()).not.toContain(ipPacePath(LEAGUE, '198.51.100.9')); // stale gone
    expect(h.objectStore.keys()).not.toContain(ipPacePath(LEAGUE, '10.0.0.1')); // corrupt gone
    // Unrelated state is untouched: chunk files and the coordinator slot file.
    expect(h.objectStore.keys().some((k) => k.includes('/chunks/'))).toBe(true);
    expect(h.objectStore.keys()).toContain(workerStatePath(LEAGUE, 'coordinator'));
  });

  it('treats a sweep failure as a warning — finalize still publishes', async () => {
    const h = makeRunHarness({
      entries: buildLadder(20),
      config: { chunkSize: 5, workerCount: 1, maxRunMillis: 30_000 },
    });
    await h.createFire();
    await h.newWorker(0).runOnce();

    // Break ONLY the IP-prefix listing the sweep uses; finalize's own listings
    // (raw shard cleanup) keep working.
    const realList = h.objectStore.listDetailed.bind(h.objectStore);
    h.objectStore.listDetailed = (prefix: string) =>
      prefix.includes('/ips/') ? Promise.reject(new Error('boom')) : realList(prefix);

    const summary = await h.newFinalizer().runOnce();

    expect(summary.stopReason).toBe('published_partial'); // finalize proceeded normally
    expect(h.logs.some((l) => l.includes('pace sweep skipped'))).toBe(true);
  });
});
