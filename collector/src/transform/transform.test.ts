import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gzipSync } from 'node:zlib';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { AggregateFile, AggregateKind, IndexFile, SnapshotMeta } from '@classolek/shared';
import {
  INDEX_PATH,
  SCHEMA_VERSION,
  rawChunkShardPath,
  rawShardPrefix,
  snapshotAggPath,
  snapshotDetailPath,
  snapshotMetaPath,
} from '@classolek/shared';
import { MemoryObjectStore, getJson } from '../checkpoint/object-store.js';
import { CheckpointStore } from '../checkpoint/store.js';
import { FakeClock } from '../rate-limit/clock.js';
import { DuckDb } from './duckdb.js';
import { CachedTreeSource } from './tree-source.js';
import { runTransform, type TransformDeps } from './transform.js';
import { TransformValidationError } from './validate.js';
import {
  buildRawRecord,
  type CharSpec,
  FakeTreeOrigin,
  transformingManifest,
} from '../../test/transform-fixtures.js';
import { putRawShard } from '../../test/helpers.js';

const LEAGUE = 'TestLeague';
const SNAP = 'snap-1';

const SPECS: CharSpec[] = [
  {
    rank: 1,
    account: 'a',
    character: 'A',
    class: 'Juggernaut',
    mainSkill: 'Cyclone',
    supports: ['Melee Physical Damage Support', 'Infused Channelling Support'],
    unique: 'Brood Star',
    nodes: [123, 4271, 55833],
  },
  {
    rank: 2,
    account: 'b',
    character: 'B',
    class: 'Juggernaut',
    mainSkill: 'Cyclone',
    supports: ['Melee Physical Damage Support'],
    unique: 'Brood Star',
    nodes: [123, 4271],
  },
  {
    rank: 3,
    account: 'c',
    character: 'C',
    class: 'Necromancer',
    mainSkill: 'Raise Spectre',
    supports: ['Spell Echo Support'],
    nodes: [123, 11455],
  },
  {
    rank: 4,
    account: 'd',
    character: 'D',
    class: 'Deadeye',
    mainSkill: 'Tornado Shot',
    unique: 'Brood Star',
    nodes: [123],
  },
  {
    rank: 5,
    account: 'e',
    character: 'E',
    class: 'Deadeye',
    mainSkill: 'Tornado Shot',
    supports: ['Greater Multiple Projectiles Support'],
    nodes: [123, 63490],
  },
];

let tmpRoot: string;

beforeEach(async () => {
  tmpRoot = await mkdtemp(join(tmpdir(), 'pou-tt-'));
});
afterEach(async () => {
  await rm(tmpRoot, { recursive: true, force: true });
});

function makeDeps(
  store: MemoryObjectStore,
  clock = new FakeClock(Date.parse('2026-07-17T01:00:00.000Z')),
): {
  deps: TransformDeps;
  origin: FakeTreeOrigin;
  checkpointStore: CheckpointStore;
} {
  const origin = new FakeTreeOrigin();
  const checkpointStore = new CheckpointStore(store);
  const deps: TransformDeps = {
    clock,
    objectStore: store,
    checkpointStore,
    treeSource: new CachedTreeSource(store, origin),
    tmpRoot,
  };
  return { deps, origin, checkpointStore };
}

async function seedGolden(store: MemoryObjectStore): Promise<void> {
  await putRawShard(store, LEAGUE, SNAP, 0, SPECS.slice(0, 3).map(buildRawRecord));
  await putRawShard(store, LEAGUE, SNAP, 1, SPECS.slice(3).map(buildRawRecord));
}

async function readJson<T>(store: MemoryObjectStore, key: string): Promise<T> {
  const value = await getJson<T>(store, key);
  if (value === undefined) throw new Error(`missing ${key}`);
  return value;
}

async function queryParquet(
  store: MemoryObjectStore,
  key: string,
  sql: string,
): Promise<unknown[][]> {
  const bytes = await store.get(key);
  if (!bytes) throw new Error(`missing parquet ${key}`);
  const file = join(tmpRoot, `read-${key.replace(/[^a-z0-9]/gi, '_')}.parquet`);
  await writeFile(file, Buffer.from(bytes));
  const db = await DuckDb.open();
  try {
    return await db.rows(sql.replace('$FILE', `'${file.replace(/'/g, "''")}'`));
  } finally {
    db.close();
  }
}

async function parquetRowCount(store: MemoryObjectStore, key: string): Promise<number> {
  const [[n] = [0]] = await queryParquet(store, key, 'SELECT count(*) FROM read_parquet($FILE)');
  return Number(n ?? 0);
}

describe('runTransform golden file', () => {
  it('normalizes shards into Parquet with the expected row counts and aggregate values', async () => {
    const store = new MemoryObjectStore();
    await seedGolden(store);
    const manifest = transformingManifest(LEAGUE, SNAP, SPECS, { private: 2, dead: 1 });
    const { deps } = makeDeps(store);

    const summary = await runTransform(
      manifest,
      { treeVersion: '3.25-test', complete: true },
      deps,
    );

    expect(summary.characterCount).toBe(5);
    expect(summary.coverage).toEqual({ ok: 5, private: 2, dead: 1 });

    // Parquet row counts (read back from the published objects).
    expect(await parquetRowCount(store, snapshotDetailPath(LEAGUE, SNAP, 'characters'))).toBe(5);
    expect(await parquetRowCount(store, snapshotDetailPath(LEAGUE, SNAP, 'items'))).toBe(10);
    expect(await parquetRowCount(store, snapshotDetailPath(LEAGUE, SNAP, 'skills'))).toBe(5);
    expect(await parquetRowCount(store, snapshotDetailPath(LEAGUE, SNAP, 'passives'))).toBe(10);
    // item_mods: BodyArmour (1 explicit) + Weapon (1 explicit) per char = 10.
    expect(await parquetRowCount(store, snapshotDetailPath(LEAGUE, SNAP, 'item_mods'))).toBe(10);

    const agg = async (kind: AggregateKind): Promise<AggregateFile> =>
      readJson<AggregateFile>(store, snapshotAggPath(LEAGUE, SNAP, kind));

    expect((await agg('class_distribution')).rows).toEqual([
      { name: 'Deadeye', count: 2, percentage: 40 },
      { name: 'Juggernaut', count: 2, percentage: 40 },
      { name: 'Necromancer', count: 1, percentage: 20 },
    ]);
    expect((await agg('skill_popularity')).rows).toEqual([
      { name: 'Cyclone', count: 2, percentage: 40 },
      { name: 'Tornado Shot', count: 2, percentage: 40 },
      { name: 'Raise Spectre', count: 1, percentage: 20 },
    ]);
    expect((await agg('unique_usage')).rows).toEqual([
      { name: 'Brood Star', count: 3, percentage: 60 },
    ]);
    expect((await agg('keystone_usage')).rows).toEqual([
      { name: 'Resolute Technique', count: 2, percentage: 40 },
      { name: 'Elemental Overload', count: 1, percentage: 20 },
      { name: 'Iron Reflexes', count: 1, percentage: 20 },
      { name: 'Unwavering Stance', count: 1, percentage: 20 },
    ]);

    // meta + index published and consistent; checkpoint advanced to published.
    const meta = await readJson<SnapshotMeta>(store, snapshotMetaPath(LEAGUE, SNAP));
    expect(meta.coverage).toEqual({ ok: 5, private: 2, dead: 1 });
    expect(meta.characterCount).toBe(5);
    expect(meta.completedAt).toBe('2026-07-17T00:30:00.000Z');
    // Provenance: the tree version that resolved passives is recorded (finding 10).
    expect(meta.treeVersion).toBe('3.25-test');

    // characters.parquet carries human-readable class + ascendancy (finding 11).
    const charCols = await queryParquet(
      store,
      snapshotDetailPath(LEAGUE, SNAP, 'characters'),
      "SELECT class, ascendancy FROM read_parquet($FILE) WHERE character_key = 'a/A'",
    );
    expect(charCols[0]).toEqual(['Juggernaut', 'Juggernaut']);

    const index = await readJson<IndexFile>(store, INDEX_PATH);
    expect(index.leagues[0]?.snapshots[0]?.snapshotId).toBe(SNAP);
    // Published with detail available (retention flips this false on trim, finding 7).
    expect(index.leagues[0]?.snapshots[0]?.hasDetail).toBe(true);
    // Schema version is stamped per entry so the web app can grey out
    // incompatible snapshots after a future bump (finding 5).
    expect(index.leagues[0]?.snapshots[0]?.schemaVersion).toBe(SCHEMA_VERSION);

    expect((await deps.checkpointStore.load(LEAGUE))?.phase).toBe('published');
    // Raw deleted only after a validated publish.
    expect(store.keys().some((k) => k.startsWith(rawShardPrefix(LEAGUE, SNAP)))).toBe(false);
    expect(summary.rawShardsDeleted).toBe(2);
  });

  it('dedupes a character across shards, keeping the latest fetchedAt', async () => {
    const store = new MemoryObjectStore();
    const older = buildRawRecord({
      ...SPECS[0]!,
      fetchedAt: '2026-07-17T00:00:00.000Z',
      mainSkill: 'Cyclone',
    });
    const newer = buildRawRecord({
      ...SPECS[0]!,
      fetchedAt: '2026-07-17T00:20:00.000Z',
      mainSkill: 'Lightning Strike',
    });
    // Same (account, character) in two shards — orphan/re-collect edge case.
    await putRawShard(store, LEAGUE, SNAP, 0, [older]);
    await putRawShard(store, LEAGUE, SNAP, 1, [newer]);
    const manifest = transformingManifest(LEAGUE, SNAP, [SPECS[0]!]);
    const { deps } = makeDeps(store);

    const summary = await runTransform(
      manifest,
      { treeVersion: '3.25-test', complete: true },
      deps,
    );

    expect(summary.characterCount).toBe(1);
    expect(await parquetRowCount(store, snapshotDetailPath(LEAGUE, SNAP, 'characters'))).toBe(1);
    // The newer record won: its main skill is the one that appears.
    const skill = await readJson<AggregateFile>(
      store,
      snapshotAggPath(LEAGUE, SNAP, 'skill_popularity'),
    );
    expect(skill.rows).toEqual([{ name: 'Lightning Strike', count: 1, percentage: 100 }]);
  });

  it('is idempotent: two runs over identical raw publish logically identical output', async () => {
    const build = async (): Promise<MemoryObjectStore> => {
      const store = new MemoryObjectStore();
      await seedGolden(store);
      const { deps } = makeDeps(store);
      await runTransform(
        transformingManifest(LEAGUE, SNAP, SPECS, { private: 1 }),
        { treeVersion: '3.25-test', complete: true },
        deps,
      );
      return store;
    };
    const a = await build();
    const b = await build();

    // JSON outputs are byte-identical (deterministic ordering + fixed clock).
    for (const kind of [
      'class_distribution',
      'skill_popularity',
      'unique_usage',
      'keystone_usage',
    ] as const) {
      const ka = snapshotAggPath(LEAGUE, SNAP, kind);
      expect(await a.get(ka)).toEqual(await b.get(ka));
    }
    expect(await a.get(snapshotMetaPath(LEAGUE, SNAP))).toEqual(
      await b.get(snapshotMetaPath(LEAGUE, SNAP)),
    );
    // Parquet is logically identical (same row counts).
    for (const table of ['characters', 'items', 'skills', 'passives', 'item_mods'] as const) {
      const key = snapshotDetailPath(LEAGUE, SNAP, table);
      expect(await parquetRowCount(a, key)).toEqual(await parquetRowCount(b, key));
    }
  });
});

describe('runTransform incremental (incomplete) publish', () => {
  it('publishes collected-so-far data marked incomplete, keeping raw and the checkpoint', async () => {
    const store = new MemoryObjectStore();
    // Only the first three characters are computed so far.
    await putRawShard(store, LEAGUE, SNAP, 0, SPECS.slice(0, 3).map(buildRawRecord));
    const partial = transformingManifest(LEAGUE, SNAP, SPECS.slice(0, 3), {
      private: 1,
      pending: 4,
    });
    const { deps } = makeDeps(store);
    await deps.checkpointStore.save(partial);

    const summary = await runTransform(
      partial,
      { treeVersion: '3.25-test', complete: false },
      deps,
    );

    expect(summary.complete).toBe(false);
    expect(summary.characterCount).toBe(3);
    expect(summary.pendingCount).toBe(4);
    expect(summary.rawShardsDeleted).toBe(0);

    // The snapshot is visible immediately, marked incomplete.
    const meta = await readJson<SnapshotMeta>(store, snapshotMetaPath(LEAGUE, SNAP));
    expect(meta.complete).toBe(false);
    expect(meta.completedAt).toBeUndefined();
    expect(meta.pendingCount).toBe(4);
    expect(meta.totalCharacters).toBe(8);
    const index = await readJson<IndexFile>(store, INDEX_PATH);
    expect(index.leagues[0]?.snapshots[0]?.complete).toBe(false);

    // Raw and the checkpoint are untouched — collection continues.
    expect(store.keys().some((k) => k.startsWith(rawShardPrefix(LEAGUE, SNAP)))).toBe(true);
    expect((await deps.checkpointStore.load(LEAGUE))?.phase).toBe('collecting');

    // Later, the drained snapshot republishes the same id as complete/immutable.
    await putRawShard(store, LEAGUE, SNAP, 1, SPECS.slice(3).map(buildRawRecord), 1);
    const drained = transformingManifest(LEAGUE, SNAP, SPECS, { private: 3 });
    await deps.checkpointStore.save(drained);
    const finalSummary = await runTransform(
      drained,
      { treeVersion: '3.25-test', complete: true },
      deps,
    );
    expect(finalSummary.complete).toBe(true);
    expect(finalSummary.characterCount).toBe(5);
    const finalMeta = await readJson<SnapshotMeta>(store, snapshotMetaPath(LEAGUE, SNAP));
    expect(finalMeta.complete).toBe(true);
    expect(finalMeta.completedAt).toBeDefined();
    const finalIndex = await readJson<IndexFile>(store, INDEX_PATH);
    expect(finalIndex.leagues[0]?.snapshots).toHaveLength(1);
    expect(finalIndex.leagues[0]?.snapshots[0]?.complete).toBe(true);
    expect(store.keys().some((k) => k.startsWith(rawShardPrefix(LEAGUE, SNAP)))).toBe(false);
  });
});

describe('runTransform validation gate (validate before delete)', () => {
  it('blocks publish and keeps raw when coverage disagrees with the shards', async () => {
    const store = new MemoryObjectStore();
    await seedGolden(store);
    // Manifest claims 6 ok characters but the shards only hold 5.
    const manifest = transformingManifest(LEAGUE, SNAP, [
      ...SPECS,
      {
        rank: 6,
        account: 'ghost',
        character: 'Ghost',
        class: 'Trickster',
        mainSkill: 'Ghost Skill',
      },
    ]);
    const { deps } = makeDeps(store);
    await deps.checkpointStore.save(manifest);

    await expect(
      runTransform(manifest, { treeVersion: '3.25-test', complete: true }, deps),
    ).rejects.toBeInstanceOf(TransformValidationError);

    // Nothing published; raw kept; checkpoint still transforming.
    expect(store.keys().some((k) => k.startsWith('snapshots/'))).toBe(false);
    expect(store.keys().filter((k) => k.startsWith(rawShardPrefix(LEAGUE, SNAP))).length).toBe(2);
    expect((await deps.checkpointStore.load(LEAGUE))?.phase).toBe('transforming');
  });

  it('blocks publish and keeps raw on a truncated (corrupt) gz shard', async () => {
    const store = new MemoryObjectStore();
    await putRawShard(store, LEAGUE, SNAP, 0, SPECS.slice(0, 2).map(buildRawRecord));
    // Overwrite shard 1 with a non-gzip body (a crash mid-write / corruption).
    await store.put(rawChunkShardPath(LEAGUE, SNAP, 1, 0), new TextEncoder().encode('not-gzip'));
    const manifest = transformingManifest(LEAGUE, SNAP, SPECS.slice(0, 2));
    const { deps } = makeDeps(store);

    await expect(
      runTransform(manifest, { treeVersion: '3.25-test', complete: true }, deps),
    ).rejects.toThrow();
    expect(store.keys().some((k) => k.startsWith('snapshots/'))).toBe(false);
    expect(store.keys().filter((k) => k.startsWith(rawShardPrefix(LEAGUE, SNAP))).length).toBe(2);
  });

  it('blocks publish and keeps raw on an invalid JSON line', async () => {
    const store = new MemoryObjectStore();
    const goodLine = JSON.stringify(buildRawRecord(SPECS[0]!));
    // A shard whose second line is not valid JSON.
    await store.put(
      rawChunkShardPath(LEAGUE, SNAP, 0, 0),
      gzipSync(Buffer.from(goodLine + '\n{ this is not json }\n', 'utf8')),
    );
    const manifest = transformingManifest(LEAGUE, SNAP, [SPECS[0]!]);
    const { deps } = makeDeps(store);

    await expect(
      runTransform(manifest, { treeVersion: '3.25-test', complete: true }, deps),
    ).rejects.toThrow();
    expect(store.keys().some((k) => k.startsWith('snapshots/'))).toBe(false);
    expect(store.keys().filter((k) => k.startsWith(rawShardPrefix(LEAGUE, SNAP))).length).toBe(1);
  });

  it('blocks publish cleanly with zero shards (no DuckDB read_json([]) crash)', async () => {
    const store = new MemoryObjectStore();
    // Manifest claims one ok character but no shards exist at all (lost raw).
    const manifest = transformingManifest(LEAGUE, SNAP, [SPECS[0]!]);
    const { deps } = makeDeps(store);

    // A clean validation failure (characters 0 != coverage.ok 1), not a binder error.
    await expect(
      runTransform(manifest, { treeVersion: '3.25-test', complete: true }, deps),
    ).rejects.toBeInstanceOf(TransformValidationError);
    expect(store.keys().some((k) => k.startsWith('snapshots/'))).toBe(false);
  });
});
