import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gzipSync } from 'node:zlib';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type {
  AggregateFile,
  AggregateKind,
  IndexFile,
  SnapshotCharacter,
  SnapshotMeta,
} from '@classolek/shared';
import {
  INDEX_PATH,
  SCHEMA_VERSION,
  snapshotAggPath,
  snapshotDetailPath,
  snapshotMetaPath,
  snapshotStatePath,
} from '@classolek/shared';
import { MemoryObjectStore, getJson } from '../checkpoint/object-store.js';
import { CheckpointStore } from '../checkpoint/store.js';
import { FakeClock } from '../rate-limit/clock.js';
import { writeState } from '../snapshot-state/state-store.js';
import { DuckDb } from './duckdb.js';
import { CachedTreeSource } from './tree-source.js';
import { runTransform, type TransformDeps } from './transform.js';
import { TransformValidationError } from './validate.js';
import {
  buildStateLine,
  type CharSpec,
  FakeTreeOrigin,
  transformingManifest,
} from '../../test/transform-fixtures.js';

/** Seed the snapshot's single state file with `ok` lines (+ optional extra lines). */
async function seedState(
  store: MemoryObjectStore,
  league: string,
  snapshotId: string,
  okSpecs: CharSpec[],
  extra: SnapshotCharacter[] = [],
): Promise<void> {
  await writeState(store, league, snapshotId, [...okSpecs.map((s) => buildStateLine(s)), ...extra]);
}

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
  await seedState(store, LEAGUE, SNAP, SPECS);
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
    // The state file (the v4 raw) is deleted only after a validated publish.
    expect(store.keys()).not.toContain(snapshotStatePath(LEAGUE, SNAP));
    expect(summary.stateFilesDeleted).toBe(1); // the one state file
  });

  it('dedupes a character with two state lines, keeping the latest fetchedAt', async () => {
    const store = new MemoryObjectStore();
    const older = buildStateLine({
      ...SPECS[0]!,
      fetchedAt: '2026-07-17T00:00:00.000Z',
      mainSkill: 'Cyclone',
    });
    const newer = buildStateLine({
      ...SPECS[0]!,
      fetchedAt: '2026-07-17T00:20:00.000Z',
      mainSkill: 'Lightning Strike',
    });
    // Two `ok` lines for the same (account, character) — the dedup SQL keeps one.
    await writeState(store, LEAGUE, SNAP, [older, newer]);
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

describe('runTransform v5 enrichment (cluster jewels, extra fields, raw net)', () => {
  it('captures cluster jewels, resolves their nodes, and keeps a raw safety copy', async () => {
    const store = new MemoryObjectStore();
    const spec: CharSpec = {
      rank: 1,
      account: 'z',
      character: 'Z',
      class: 'Juggernaut',
      mainSkill: 'Cyclone',
      unique: 'Brood Star',
      nodes: [123, 4271], // base-tree hashes (incl. a keystone from the fixture tree)
      cluster: {
        socketHash: 53960,
        name: 'Foe Glisten',
        baseType: 'Large Cluster Jewel',
        quality: 12,
        reqLevel: 54,
        explicitMods: ['Adds 3 Passive Skills', '1 Added Passive Skill is Feast of Flesh'],
        fracturedMods: ['Adds 2 Passive Skills'],
        nodes: [
          {
            hash: 900001,
            name: 'Feast of Flesh',
            stats: ['Notable cluster stat'],
            isNotable: true,
          },
          { hash: 900002, name: 'Cluster Small', stats: ['Small cluster stat'] },
        ],
      },
      masteries: { 4271: 88001 },
    };
    await seedState(store, LEAGUE, SNAP, [spec]);
    const manifest = transformingManifest(LEAGUE, SNAP, [spec]);
    const { deps } = makeDeps(store);

    await runTransform(manifest, { treeVersion: '3.25-test', complete: true }, deps);

    // The cluster jewel is an item (inventoryId PassiveJewels) with v5 detail.
    const jewel = await queryParquet(
      store,
      snapshotDetailPath(LEAGUE, SNAP, 'items'),
      'SELECT base_type, quality, req_level, identified, sockets FROM read_parquet($FILE) ' +
        "WHERE slot = 'PassiveJewels'",
    );
    expect(jewel).toHaveLength(1);
    expect(jewel[0]?.[0]).toBe('Large Cluster Jewel');
    expect(Number(jewel[0]?.[1])).toBe(12); // quality
    expect(Number(jewel[0]?.[2])).toBe(54); // req_level
    expect(jewel[0]?.[3]).toBe(true); // identified
    expect(jewel[0]?.[4]).toBe('[]'); // sockets JSON text, lossless

    // The jewel's explicit AND fractured mods land in item_mods (extra v5 domains).
    const jewelMods = await queryParquet(
      store,
      snapshotDetailPath(LEAGUE, SNAP, 'item_mods'),
      "SELECT mod_domain, mod_text FROM read_parquet($FILE) WHERE item_key = 'PassiveJewels' " +
        'ORDER BY mod_domain, mod_text',
    );
    expect(jewelMods.map((r) => [r[0], r[1]])).toEqual([
      ['explicit', '1 Added Passive Skill is Feast of Flesh'],
      ['explicit', 'Adds 3 Passive Skills'],
      ['fractured', 'Adds 2 Passive Skills'],
    ]);

    // Cluster nodes resolve to name/stats/notability from jewel_data.subgraph.
    const cluster = await queryParquet(
      store,
      snapshotDetailPath(LEAGUE, SNAP, 'passives'),
      'SELECT node_hash, node_name, is_notable, node_stats FROM read_parquet($FILE) ' +
        "WHERE source = 'cluster' ORDER BY node_hash",
    );
    expect(cluster).toHaveLength(2);
    expect(cluster[0]?.[1]).toBe('Feast of Flesh');
    expect(cluster[0]?.[2]).toBe(true);
    expect(cluster[0]?.[3]).toEqual(['Notable cluster stat']);
    expect(cluster[1]?.[1]).toBe('Cluster Small');
    expect(cluster[1]?.[2]).toBe(false);

    // Base-tree nodes still carry source 'tree' (2 allocated) — nothing regressed.
    const [[treeCount] = [0]] = await queryParquet(
      store,
      snapshotDetailPath(LEAGUE, SNAP, 'passives'),
      "SELECT count(*) FROM read_parquet($FILE) WHERE source = 'tree'",
    );
    expect(Number(treeCount)).toBe(2);

    // The chosen mastery is captured (node hash → effect hash).
    const masteries = await queryParquet(
      store,
      snapshotDetailPath(LEAGUE, SNAP, 'masteries'),
      'SELECT node_hash, effect_hash FROM read_parquet($FILE)',
    );
    expect(masteries).toHaveLength(1);
    expect(Number(masteries[0]?.[0])).toBe(4271);
    expect(Number(masteries[0]?.[1])).toBe(88001);

    // The raw safety net holds the verbatim payloads for this character.
    const raw = await queryParquet(
      store,
      snapshotDetailPath(LEAGUE, SNAP, 'raw'),
      'SELECT character_key, items, passives FROM read_parquet($FILE)',
    );
    expect(raw).toHaveLength(1);
    expect(raw[0]?.[0]).toBe('z/Z');
    // Nothing is skipped: fields the normalized tables omit survive verbatim here.
    expect(String(raw[0]?.[1])).toContain('PassiveJewels');
    expect(String(raw[0]?.[2])).toContain('jewel_data');
  });
});

describe('runTransform incremental (incomplete) publish', () => {
  it('publishes collected-so-far data marked incomplete, keeping raw and the checkpoint', async () => {
    const store = new MemoryObjectStore();
    // Only the first three characters are computed so far (the rest still pending
    // lines in the state file — the transform emits only the `ok` ones).
    await seedState(
      store,
      LEAGUE,
      SNAP,
      SPECS.slice(0, 3),
      SPECS.slice(3).map((s) => buildStateLine(s, 'pending')),
    );
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
    expect(summary.stateFilesDeleted).toBe(0);

    // The snapshot is visible immediately, marked incomplete.
    const meta = await readJson<SnapshotMeta>(store, snapshotMetaPath(LEAGUE, SNAP));
    expect(meta.complete).toBe(false);
    expect(meta.completedAt).toBeUndefined();
    expect(meta.pendingCount).toBe(4);
    expect(meta.totalCharacters).toBe(8);
    const index = await readJson<IndexFile>(store, INDEX_PATH);
    expect(index.leagues[0]?.snapshots[0]?.complete).toBe(false);

    // The state file and the checkpoint are untouched — collection continues.
    expect(store.keys()).toContain(snapshotStatePath(LEAGUE, SNAP));
    expect((await deps.checkpointStore.load(LEAGUE))?.phase).toBe('collecting');

    // Later, the drained snapshot republishes the same id as complete/immutable.
    // The state file now holds all five as `ok`.
    await seedState(store, LEAGUE, SNAP, SPECS);
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
    expect(store.keys()).not.toContain(snapshotStatePath(LEAGUE, SNAP));
  });
});

describe('runTransform validation gate (validate before delete)', () => {
  it('blocks publish and keeps the state file when coverage disagrees with it', async () => {
    const store = new MemoryObjectStore();
    await seedGolden(store); // 5 ok lines
    // Manifest claims 6 ok characters but the state file only holds 5.
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

    // Nothing published; the state file kept; checkpoint still transforming.
    expect(store.keys().some((k) => k.startsWith('snapshots/'))).toBe(false);
    expect(store.keys()).toContain(snapshotStatePath(LEAGUE, SNAP));
    expect((await deps.checkpointStore.load(LEAGUE))?.phase).toBe('transforming');
  });

  it('blocks publish and keeps the state file on a truncated (corrupt) gz body', async () => {
    const store = new MemoryObjectStore();
    // A non-gzip state-file body (a crash mid-write / corruption).
    await store.put(snapshotStatePath(LEAGUE, SNAP), new TextEncoder().encode('not-gzip'));
    const manifest = transformingManifest(LEAGUE, SNAP, SPECS.slice(0, 2));
    const { deps } = makeDeps(store);

    await expect(
      runTransform(manifest, { treeVersion: '3.25-test', complete: true }, deps),
    ).rejects.toThrow();
    expect(store.keys().some((k) => k.startsWith('snapshots/'))).toBe(false);
    expect(store.keys()).toContain(snapshotStatePath(LEAGUE, SNAP));
  });

  it('blocks publish and keeps the state file on an invalid JSON line', async () => {
    const store = new MemoryObjectStore();
    const goodLine = JSON.stringify(buildStateLine(SPECS[0]!));
    // A state file whose second line is not valid JSON.
    await store.put(
      snapshotStatePath(LEAGUE, SNAP),
      gzipSync(Buffer.from(goodLine + '\n{ this is not json }\n', 'utf8')),
    );
    const manifest = transformingManifest(LEAGUE, SNAP, [SPECS[0]!]);
    const { deps } = makeDeps(store);

    await expect(
      runTransform(manifest, { treeVersion: '3.25-test', complete: true }, deps),
    ).rejects.toThrow();
    expect(store.keys().some((k) => k.startsWith('snapshots/'))).toBe(false);
    expect(store.keys()).toContain(snapshotStatePath(LEAGUE, SNAP));
  });

  it('blocks publish cleanly with zero ok lines (no DuckDB read_json([]) crash)', async () => {
    const store = new MemoryObjectStore();
    // Manifest claims one ok character but the state file holds no `ok` lines.
    await writeState(store, LEAGUE, SNAP, []);
    const manifest = transformingManifest(LEAGUE, SNAP, [SPECS[0]!]);
    const { deps } = makeDeps(store);

    // A clean validation failure (characters 0 != coverage.ok 1), not a binder error.
    await expect(
      runTransform(manifest, { treeVersion: '3.25-test', complete: true }, deps),
    ).rejects.toBeInstanceOf(TransformValidationError);
    expect(store.keys().some((k) => k.startsWith('snapshots/'))).toBe(false);
  });
});
