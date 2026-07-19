/**
 * End-to-end lifecycle across both workflows, across multiple simulated fires:
 *
 *   create-snapshot (capture → roster → seed chunks)
 *     → collect fires: coordinate (fan-out check)
 *         → workers (parallel disjoint chunk resolution)
 *           → finalize (incremental publish while incomplete)
 *   … repeated until …
 *           → finalize (final transform → published, immutable) → idle
 *
 * Everything runs against the MockPoeApi and MemoryObjectStore — zero network.
 */
import { describe, expect, it } from 'vitest';
import type { IndexFile, SnapshotMeta } from '@pou/shared';
import {
  INDEX_PATH,
  rawShardPrefix,
  snapshotAggPath,
  snapshotDetailPath,
  snapshotMetaPath,
} from '@pou/shared';
import { getJson } from '../checkpoint/object-store.js';
import { LEAGUE, makeRunHarness, runToSettle } from '../../test/run-harness.js';
import { buildLadder } from '../../test/mock-api.js';

describe('create → collect fires (workers → finalize) → published → idle', () => {
  it('collects across multiple fires with visible incomplete snapshots, then publishes immutably', async () => {
    const entries = buildLadder(30);
    // Two workers, small chunks, and a budget that forces multiple fires.
    const h = makeRunHarness({
      entries,
      config: { chunkSize: 5, workerCount: 2, maxRunMillis: 25_000 },
    });

    // The create workflow seeds the snapshot; collect fires then drain it.
    const created = await h.createFire();
    expect(created.stopReason).toBe('created');

    const cycles = await runToSettle(h);
    const last = cycles.at(-1)!;

    // It took more than one workflow fire, and intermediate fires published
    // incomplete snapshots (immediately-visible partial data).
    expect(cycles.length).toBeGreaterThanOrEqual(2);
    const partials = cycles.filter((c) => c.finalize.stopReason === 'published_partial');
    expect(partials.length).toBeGreaterThanOrEqual(1);

    // Final fire: the snapshot completed and published immutably.
    expect(last.finalize.stopReason).toBe('published_final');
    expect(last.finalize.transform?.complete).toBe(true);
    const meta = await getJson<SnapshotMeta>(h.objectStore, snapshotMetaPath(LEAGUE, 'snap-fixed'));
    expect(meta?.complete).toBe(true);
    expect(meta?.pendingCount).toBe(0);
    expect(meta?.totalCharacters).toBe(30);
    expect(
      (meta?.coverage.ok ?? 0) + (meta?.coverage.private ?? 0) + (meta?.coverage.dead ?? 0),
    ).toBe(30);

    // Published artifacts exist; raw + chunks are gone; checkpoint is published.
    expect(h.objectStore.keys()).toContain(snapshotDetailPath(LEAGUE, 'snap-fixed', 'characters'));
    expect(h.objectStore.keys()).toContain(
      snapshotAggPath(LEAGUE, 'snap-fixed', 'class_distribution'),
    );
    expect(
      h.objectStore.keys().some((k) => k.startsWith(rawShardPrefix(LEAGUE, 'snap-fixed'))),
    ).toBe(false);
    expect(h.objectStore.keys().some((k) => k.includes('/chunks/'))).toBe(false);
    expect((await h.checkpointStore.load(LEAGUE))?.phase).toBe('published');

    const index = await getJson<IndexFile>(h.objectStore, INDEX_PATH);
    expect(index?.leagues[0]?.snapshots).toHaveLength(1);
    expect(index?.leagues[0]?.snapshots[0]?.complete).toBe(true);

    // No character was ever fetched twice — across every worker and fire.
    for (const [, count] of h.api.itemCalls) {
      expect(count).toBeLessThanOrEqual(2); // flaky chars retry once, others once
    }
    expect(h.api.itemCalls.get('acct-1/char-1')).toBe(1);

    // The next fire inside the snapshot interval idles.
    h.clock.advance(3_600_000); // 1 h < 12 h interval
    const idle = await h.runCycle();
    expect(idle.coordinate.stopReason).toBe('idle');
    expect(idle.coordinate.hasWork).toBe(false);
    expect((await h.checkpointStore.load(LEAGUE))?.snapshotId).toBe('snap-fixed');
  });
});
