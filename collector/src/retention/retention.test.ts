import { describe, expect, it } from 'vitest';
import type { IndexFile, SnapshotManifest, SnapshotMeta } from '@pou/shared';
import {
  INDEX_PATH,
  SCHEMA_VERSION,
  chunkPath,
  rawChunkShardPath,
  snapshotAggPath,
  snapshotDetailPath,
  snapshotMetaPath,
  treeCachePath,
} from '@pou/shared';
import { MemoryObjectStore, getJson, putJson } from '../checkpoint/object-store.js';
import { CheckpointStore } from '../checkpoint/store.js';
import { FakeClock } from '../rate-limit/clock.js';
import { runRetention, type RetentionDeps } from './retention.js';
import { fixtureManifest } from '../../test/helpers.js';

function bytes(n: number): Uint8Array {
  return new Uint8Array(n);
}

function makeDeps(store: MemoryObjectStore): RetentionDeps {
  return {
    clock: new FakeClock(Date.parse('2026-07-18T00:00:00.000Z')),
    objectStore: store,
    checkpointStore: new CheckpointStore(store),
  };
}

/** Seed a published snapshot: detail Parquet + aggregates + a real meta.json. */
async function seedSnapshot(
  store: MemoryObjectStore,
  league: string,
  id: string,
  completedAt: string,
  detailBytes: number,
): Promise<void> {
  for (const table of ['characters', 'items']) {
    await store.put(snapshotDetailPath(league, id, table), bytes(detailBytes));
  }
  await store.put(snapshotAggPath(league, id, 'class_distribution'), bytes(20));
  const meta: SnapshotMeta = {
    schemaVersion: SCHEMA_VERSION,
    snapshotId: id,
    league,
    depth: 100,
    ladderCapturedAt: completedAt,
    updatedAt: completedAt,
    completedAt,
    complete: true,
    coverage: { ok: 1, private: 0, dead: 0 },
    pendingCount: 0,
    skippedCount: 0,
    totalCharacters: 1,
    characterCount: 1,
    treeVersion: 't',
  };
  await putJson(store, snapshotMetaPath(league, id), meta, true);
}

async function seedIndexEntry(
  store: MemoryObjectStore,
  league: string,
  id: string,
  completedAt: string,
): Promise<void> {
  const existing = (await getJson<IndexFile>(store, INDEX_PATH)) ?? {
    schemaVersion: SCHEMA_VERSION,
    updatedAt: '',
    leagues: [],
  };
  let entry = existing.leagues.find((l) => l.league === league);
  if (!entry) {
    entry = { league, snapshots: [] };
    existing.leagues.push(entry);
  }
  entry.snapshots.push({
    schemaVersion: SCHEMA_VERSION,
    snapshotId: id,
    ladderCapturedAt: completedAt,
    updatedAt: completedAt,
    completedAt,
    complete: true,
    depth: 100,
    totalCharacters: 1,
    coverage: { ok: 1, private: 0, dead: 0 },
    hasDetail: true,
  });
  await putJson(store, INDEX_PATH, existing, true);
}

function inflightManifest(league: string, id: string): SnapshotManifest {
  return fixtureManifest({
    snapshotId: id,
    league,
    depth: 100,
    ladderCapturedAt: '2026-07-18T00:00:00.000Z',
  });
}

describe('runRetention detail trimming', () => {
  it('trims the OLDEST detail first across leagues and marks index hasDetail=false', async () => {
    const store = new MemoryObjectStore();
    // Ages: Aleague/a1 (oldest) < Zleague/b1 < Aleague/a2 (newest). Name must NOT dominate.
    // Detail dominates usage (100 KB/table × 2 = 200 KB/snapshot) so meta/index
    // sizes don't perturb the budget arithmetic.
    await seedSnapshot(store, 'Aleague', 'a1', '2026-07-01T00:00:00.000Z', 100_000);
    await seedSnapshot(store, 'Zleague', 'b1', '2026-07-05T00:00:00.000Z', 100_000);
    await seedSnapshot(store, 'Aleague', 'a2', '2026-07-09T00:00:00.000Z', 100_000);
    await seedIndexEntry(store, 'Aleague', 'a1', '2026-07-01T00:00:00.000Z');
    await seedIndexEntry(store, 'Zleague', 'b1', '2026-07-05T00:00:00.000Z');
    await seedIndexEntry(store, 'Aleague', 'a2', '2026-07-09T00:00:00.000Z');

    // Total detail ≈ 600 KB; budget 350 KB → trim the two oldest, keep the newest.
    const summary = await runRetention(
      { budgetBytes: 350_000, keepRecentDetail: 0 },
      makeDeps(store),
    );

    // Oldest-across-leagues order, not alphabetical: a1 then b1; a2 (newest) kept.
    expect(summary.detailSnapshotsTrimmed).toEqual(['Aleague/a1', 'Zleague/b1']);
    expect(store.keys().some((k) => k.startsWith('snapshots/Aleague/a2/detail/'))).toBe(true);
    // Aggregates + meta never trimmed.
    expect(store.keys().filter((k) => k.includes('/agg/')).length).toBe(3);
    expect(store.keys().filter((k) => k.endsWith('/meta.json')).length).toBe(3);

    // Index reflects trimmed detail; kept snapshot still advertises detail.
    const index = (await getJson<IndexFile>(store, INDEX_PATH))!;
    const flag = (l: string, id: string): boolean =>
      index.leagues.find((x) => x.league === l)!.snapshots.find((s) => s.snapshotId === id)!
        .hasDetail;
    expect(flag('Aleague', 'a1')).toBe(false);
    expect(flag('Zleague', 'b1')).toBe(false);
    expect(flag('Aleague', 'a2')).toBe(true);
  });

  it('respects per-league keepRecentDetail even when over budget', async () => {
    const store = new MemoryObjectStore();
    await seedSnapshot(store, 'Aleague', 'a1', '2026-07-01T00:00:00.000Z', 1000);
    await seedSnapshot(store, 'Aleague', 'a2', '2026-07-09T00:00:00.000Z', 1000);
    await seedSnapshot(store, 'Zleague', 'b1', '2026-07-05T00:00:00.000Z', 1000);

    const summary = await runRetention({ budgetBytes: 1, keepRecentDetail: 1 }, makeDeps(store));

    // A keeps its newest (a2); B keeps its only (b1) though older than a2. Only a1 trims.
    expect(summary.detailSnapshotsTrimmed).toEqual(['Aleague/a1']);
    expect(store.keys().some((k) => k.startsWith('snapshots/Aleague/a2/detail/'))).toBe(true);
    expect(store.keys().some((k) => k.startsWith('snapshots/Zleague/b1/detail/'))).toBe(true);
  });
});

describe('runRetention raw sweep', () => {
  it('sweeps orphaned raw from a crashed publish, keeping the in-flight snapshot', async () => {
    const store = new MemoryObjectStore();
    const cs = new CheckpointStore(store);
    await cs.save(inflightManifest('Live', 'current'));
    await store.put(rawChunkShardPath('Live', 'current', 0, 0), bytes(500));
    await store.put(rawChunkShardPath('Live', 'orphan', 0, 0), bytes(700));
    await store.put(rawChunkShardPath('Live', 'orphan', 1, 0), bytes(300));

    const summary = await runRetention(
      { budgetBytes: 1_000_000, keepRecentDetail: 5 },
      makeDeps(store),
    );

    expect(summary.rawSnapshotsSwept).toEqual(['Live/orphan']);
    expect(summary.bytesFreed).toBe(1000);
    expect(store.keys().some((k) => k.startsWith('raw/Live/orphan/'))).toBe(false);
    expect(store.keys().some((k) => k.startsWith('raw/Live/current/'))).toBe(true);
  });

  it('sweeps orphaned chunk files from a crashed abort, keeping the in-flight snapshot', async () => {
    const store = new MemoryObjectStore();
    const cs = new CheckpointStore(store);
    await cs.save(inflightManifest('Live', 'current'));
    await store.put(chunkPath('Live', 'current', 0), bytes(400));
    await store.put(chunkPath('Live', 'stale', 0), bytes(250));

    const summary = await runRetention(
      { budgetBytes: 1_000_000, keepRecentDetail: 5 },
      makeDeps(store),
    );

    expect(summary.rawSnapshotsSwept).toEqual(['Live/stale']);
    expect(summary.bytesFreed).toBe(250);
    expect(store.keys()).toContain(chunkPath('Live', 'current', 0));
    expect(store.keys()).not.toContain(chunkPath('Live', 'stale', 0));
  });
});

describe('runRetention usage accounting', () => {
  it('reports usage by category and trims nothing under budget', async () => {
    const store = new MemoryObjectStore();
    const cs = new CheckpointStore(store);
    await seedSnapshot(store, 'Std', 's-1', '2026-07-01T00:00:00.000Z', 500);
    await cs.save(inflightManifest('Std', 's-live'));
    await store.put(rawChunkShardPath('Std', 's-live', 0, 0), bytes(300));
    await store.put(treeCachePath('3.25'), bytes(64));
    await store.put(INDEX_PATH, bytes(15));

    const summary = await runRetention(
      { budgetBytes: 1_000_000, keepRecentDetail: 2 },
      makeDeps(store),
    );

    expect(summary.detailSnapshotsTrimmed).toEqual([]);
    expect(summary.rawSnapshotsSwept).toEqual([]); // s-live raw is in-flight
    expect(summary.usageByPrefix.detail).toBe(1000);
    expect(summary.usageByPrefix.agg).toBe(20);
    expect(summary.usageByPrefix.meta).toBeGreaterThan(0);
    expect(summary.usageByPrefix.raw).toBe(300);
    expect(summary.usageByPrefix.tree).toBe(64);
    expect(summary.usageByPrefix.checkpoint).toBeGreaterThan(0);
    expect(summary.usageByPrefix.index).toBe(15);
  });
});
