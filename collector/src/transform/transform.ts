/**
 * Transform & publish (Phase 3 + the incremental-publish redesign; v4 single
 * state file). A pure batch job, idempotent and safe to re-run on the same
 * state (docs/ARCHITECTURE.md §7):
 *
 *   stream state file (ok lines) ─▶ temp NDJSON ─▶ DuckDB normalize
 *     ─▶ build aggregates + meta ─▶ VALIDATE (before any write)
 *     ─▶ COPY zstd Parquet ─▶ publish ─▶ update index
 *     ─▶ [final only] phase transforming→published ─▶ delete state file + results
 *
 * The snapshot's raw IS its single NDJSON.gz state file now
 * (snapshotStatePath): step 1 streams it a line at a time and emits every
 * `outcome == 'ok'` line — converted back to the raw get-items / get-passive
 * record shape the SQL expects (`characterData` → items, `passiveTree` →
 * passives) — into one temp NDJSON file DuckDB ingests. There are no per-chunk
 * raw shards; the SQL, aggregates, validation gate, meta and index logic are
 * untouched.
 *
 * Two modes, chosen by `config.complete`:
 *  - complete: false — the INCREMENTAL publish of a still-collecting snapshot.
 *    Publishes whatever `ok` lines exist so far with meta/index marked
 *    incomplete (pendingCount > 0), keeps the state file, and leaves the
 *    checkpoint untouched. The same snapshot's files are overwritten in place
 *    on the next pass — incomplete snapshots are mutable by design.
 *  - complete: true — the FINAL publish. From here the snapshot is immutable
 *    (hard rule #5): checkpoint moves to `published` and the state file + any
 *    transient result files are deleted (the state file is the raw).
 *
 * Validation runs before anything is published or deleted; on failure the state
 * file is kept and nothing is published (the caller exits nonzero). Any thrown
 * error before the final delete leaves the state file intact by construction, so
 * a truncated gz or an invalid JSON line can never destroy the only copy of the
 * data. (A crash in the publish→delete window leaks the state file + result
 * files, but by then the manifest is already `published` — no longer in-flight —
 * so retention's orphan sweep owns them and reaps them next run.)
 *
 * Memory at 15k depth: the state file is streamed (never JSON.parsed whole); the
 * DB is file-backed with a spill dir; the big JSON tables (chars/item_rows) are
 * dropped before aggregation; Parquet is streamed to disk and re-read one file
 * at a time at upload (never five buffers at once).
 */
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createWriteStream } from 'node:fs';
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type {
  AggregateFile,
  AggregateKind,
  Coverage,
  SnapshotCharacter,
  SnapshotManifest,
  SnapshotMeta,
} from '@classolek/shared';
import {
  AGGREGATE_KINDS,
  SCHEMA_VERSION,
  coverageOfTally,
  pendingOfTally,
  percentage,
  snapshotAggPath,
  snapshotDetailPath,
  snapshotMetaPath,
  snapshotStatePath,
  workerResultPrefix,
} from '@classolek/shared';
import type { Clock } from '../rate-limit/clock.js';
import type { CheckpointStore } from '../checkpoint/store.js';
import { listKeys, putJson, type ObjectStore } from '../checkpoint/object-store.js';
import { readState } from '../snapshot-state/state-store.js';
import { readIndex, upsertSnapshot, writeIndex } from '../index-file.js';
import { DuckDb } from './duckdb.js';
import type { TreeSource } from './tree-source.js';
import {
  AGGREGATE_SQL,
  DETAIL_TABLES,
  copyToParquetSql,
  createCharactersSql,
  createCharsSql,
  createItemModsSql,
  createItemRowsSql,
  createItemsSql,
  createMasteriesSql,
  createPassivesSql,
  createRawSql,
  createSkillsSql,
  createTreeNodesSql,
} from './sql.js';
import { TransformValidationError, validateTransform } from './validate.js';

/** Bounded parallelism for R2 reads/writes (R2 has no rate concern; only GGG does). */
const IO_CONCURRENCY = 6;

export interface TransformConfig {
  /** Passive-tree version to resolve node hashes against (pinned per league). */
  treeVersion: string;
  /**
   * true → final immutable publish (checkpoint → published, raw deleted);
   * false → incremental publish of a still-collecting snapshot (mutable).
   */
  complete: boolean;
}

export interface TransformDeps {
  clock: Clock;
  objectStore: ObjectStore;
  checkpointStore: CheckpointStore;
  treeSource: TreeSource;
  /** Override the temp root (tests); defaults to the OS tmpdir. */
  tmpRoot?: string;
}

export interface TransformSummary {
  snapshotId: string;
  league: string;
  complete: boolean;
  coverage: Coverage;
  /** Characters still awaiting computation (0 on the final publish). */
  pendingCount: number;
  /** Characters marked skipped when the snapshot was closed. */
  skippedCount: number;
  characterCount: number;
  detailBytes: Record<string, number>;
  aggregateRows: Record<string, number>;
  /**
   * Raw objects deleted on the final publish: the snapshot's single state file
   * plus any lingering per-worker result files (the v4 raw). 0 on an incremental
   * publish, which keeps the state file.
   */
  stateFilesDeleted: number;
}

/**
 * One `ok` state line → the raw get-items / get-passive record the SQL reads.
 * The state file stores the payloads as `characterData` (items response) and
 * `passiveTree` (passives response); this restores the `{ items, passives }`
 * shape createCharsSql / RAW_COLUMNS expect, carrying the identity columns
 * through unchanged. Only called for `outcome === 'ok'` lines (the only ones
 * with payloads).
 */
function rawRecordOf(line: SnapshotCharacter): Record<string, unknown> {
  return {
    rank: line.rank,
    account: line.account,
    character: line.character,
    class: line.class,
    level: line.level,
    fetchedAt: line.fetchedAt,
    items: line.characterData,
    passives: line.passiveTree,
  };
}

/** Run an async fn over items with bounded concurrency, preserving input order. */
async function mapLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, i: number) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let cursor = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    for (let i = cursor++; i < items.length; i = cursor++) {
      results[i] = await fn(items[i] as T, i);
    }
  });
  await Promise.all(workers);
  return results;
}

/**
 * Run the transform for a snapshot (final or incremental — see TransformConfig).
 * On success publishes the snapshot files, updates the index and — final mode
 * only — moves the checkpoint to `published` and deletes raw. On validation
 * failure throws TransformValidationError with raw untouched.
 */
export async function runTransform(
  manifest: SnapshotManifest,
  config: TransformConfig,
  deps: TransformDeps,
): Promise<TransformSummary> {
  const { league, snapshotId } = manifest;
  const coverage = coverageOfTally(manifest.outcomes);
  const pendingCount = pendingOfTally(manifest.outcomes);
  const skippedCount = manifest.outcomes.skipped;
  const workDir = await mkdtemp(join(deps.tmpRoot ?? tmpdir(), 'pou-transform-'));

  try {
    // 1. Stream the single state file (never JSON.parsed whole), emitting every
    //    `ok` line — converted back to the raw record shape the SQL reads — into
    //    one temp NDJSON file. A truncated gz or an invalid line throws here
    //    (readState is a hard error), leaving the state file intact. With zero
    //    `ok` lines the file list is empty, so createCharsSql emits a typed empty
    //    table and the validation gate blocks publish cleanly (no read_json([])).
    let okCount = 0;
    const stateNdjson = join(workDir, 'state.ndjson');
    await pipeline(
      Readable.from(
        (async function* () {
          for await (const line of readState(deps.objectStore, league, snapshotId)) {
            if (line.outcome !== 'ok') continue;
            okCount += 1;
            yield JSON.stringify(rawRecordOf(line)) + '\n';
          }
        })(),
      ),
      createWriteStream(stateNdjson),
    );
    const shardFiles = okCount > 0 ? [stateNdjson] : [];

    // 2. Resolve + cache the passive tree, write it as NDJSON for read_json.
    const tree = await deps.treeSource.load(config.treeVersion);
    const treeFile = join(workDir, 'tree.ndjson');
    await writeFile(
      treeFile,
      tree.nodes
        .map((n) =>
          JSON.stringify({ hash: n.hash, name: n.name, stats: n.stats, is_keystone: n.isKeystone }),
        )
        .join('\n') + '\n',
    );

    // 3. DuckDB (file-backed + spill dir): normalize, aggregate, validate, COPY.
    const db = await DuckDb.open({ path: join(workDir, 'transform.duckdb'), tempDir: workDir });
    const detailBytes: Record<string, number> = {};
    let characterCount = 0;
    const aggregates: AggregateFile[] = [];
    try {
      await db.run(createCharsSql(shardFiles));
      await db.run(createTreeNodesSql(treeFile));
      await db.run(createItemRowsSql()); // the biggest JSON parse, done once
      await db.run(createCharactersSql(snapshotId));
      await db.run(createItemsSql());
      await db.run(createItemModsSql());
      await db.run(createSkillsSql());
      await db.run(createPassivesSql());
      await db.run(createMasteriesSql());
      // `raw` reads chars' JSON columns, so build it before the drop below.
      await db.run(createRawSql());

      // Free the big JSON source tables before aggregation (normalized tables hold).
      await db.dropTable('item_rows');
      await db.dropTable('chars');
      await db.dropTable('tree_nodes');

      characterCount = await db.count('SELECT count(*) FROM characters');

      // 4. Aggregates (name/count → +percentage against the character total).
      for (const kind of AGGREGATE_KINDS) {
        const rows = await db.rows(AGGREGATE_SQL[kind]);
        aggregates.push({
          schemaVersion: SCHEMA_VERSION,
          snapshotId,
          league,
          kind,
          total: characterCount,
          rows: rows.map(([name, count]) => ({
            name: String(name),
            count: Number(count),
            percentage: percentage(Number(count), characterCount),
          })),
        });
      }
      const classDistributionTotal =
        aggregates
          .find((a) => a.kind === 'class_distribution')
          ?.rows.reduce((s, r) => s + r.count, 0) ?? 0;

      // 5. meta.json (records the tree version that resolved passives — provenance).
      const nowIso = new Date(deps.clock.now()).toISOString();
      const meta: SnapshotMeta = {
        schemaVersion: SCHEMA_VERSION,
        snapshotId,
        league,
        depth: manifest.depth,
        ladderCapturedAt: manifest.ladderCapturedAt,
        updatedAt: nowIso,
        ...(config.complete ? { completedAt: manifest.completedAt ?? nowIso } : {}),
        complete: config.complete,
        coverage,
        pendingCount,
        skippedCount,
        totalCharacters: manifest.totalCharacters,
        characterCount,
        treeVersion: tree.version,
      };

      // 6. VALIDATE before writing any Parquet, publishing, or deleting anything.
      const validation = validateTransform({
        meta,
        coverage,
        pendingCount,
        skippedCount,
        totalCharacters: manifest.totalCharacters,
        characterRowCount: characterCount,
        aggregates,
        classDistributionTotal,
      });
      if (!validation.ok) throw new TransformValidationError(validation.errors);

      // 7. COPY each detail table to Parquet on disk, then DROP it (bounds RAM).
      for (const table of DETAIL_TABLES) {
        const parquetPath = join(workDir, `${table}.parquet`);
        await db.run(copyToParquetSql(table, parquetPath));
        detailBytes[table] = (await stat(parquetPath)).size;
        await db.dropTable(table);
      }

      // 8. Publish (immutable snapshot): detail Parquet re-read one file at a
      //    time (never all five in memory), then aggregates + meta.
      await mapLimit(
        DETAIL_TABLES as readonly string[] as string[],
        IO_CONCURRENCY,
        async (table) => {
          const buf = await readFile(join(workDir, `${table}.parquet`));
          await deps.objectStore.put(snapshotDetailPath(league, snapshotId, table), buf);
        },
      );
      await mapLimit(aggregates, IO_CONCURRENCY, (agg) =>
        putJson(deps.objectStore, snapshotAggPath(league, snapshotId, agg.kind), agg, true),
      );
      await putJson(deps.objectStore, snapshotMetaPath(league, snapshotId), meta, true);

      // 9. Update the mutable index (entry point). Upsert keeps re-runs idempotent
      //    and preserves entries from other leagues / schema versions verbatim.
      const index = await readIndex(deps.objectStore);
      upsertSnapshot(index, league, {
        schemaVersion: SCHEMA_VERSION,
        snapshotId,
        ladderCapturedAt: meta.ladderCapturedAt,
        updatedAt: meta.updatedAt,
        ...(meta.completedAt !== undefined ? { completedAt: meta.completedAt } : {}),
        complete: meta.complete,
        depth: meta.depth,
        totalCharacters: meta.totalCharacters,
        coverage: meta.coverage,
        hasDetail: true,
      });
      await writeIndex(deps.objectStore, index, deps.clock);
    } finally {
      db.close();
    }

    // 10–11. Final publish only: move the checkpoint transforming → published
    // (the snapshot is immutable from here; the interval gate now applies) and
    // delete the state file + any transient result files after the successful,
    // validated publish (the state file IS the raw). An incremental publish
    // keeps the state file AND the checkpoint — the next pass reworks the growing
    // `ok` set and republishes in place.
    let stateFilesDeleted = 0;
    if (config.complete) {
      const published: SnapshotManifest = {
        ...manifest,
        phase: 'published',
        completedAt: manifest.completedAt ?? new Date(deps.clock.now()).toISOString(),
      };
      await deps.checkpointStore.save(published);
      const resultKeys = await listKeys(deps.objectStore, workerResultPrefix(league, snapshotId));
      await mapLimit(resultKeys, IO_CONCURRENCY, (key) => deps.objectStore.delete(key));
      await deps.objectStore.delete(snapshotStatePath(league, snapshotId));
      stateFilesDeleted = resultKeys.length + 1;
    }

    return {
      snapshotId,
      league,
      complete: config.complete,
      coverage,
      pendingCount,
      skippedCount,
      characterCount,
      detailBytes,
      aggregateRows: Object.fromEntries(aggregates.map((a) => [a.kind, a.rows.length])) as Record<
        AggregateKind,
        number
      >,
      stateFilesDeleted,
    };
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}
