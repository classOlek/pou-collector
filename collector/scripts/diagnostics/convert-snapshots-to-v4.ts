/**
 * One-time schema migration: convert every published v2/v3 snapshot to v4.
 *
 * The published snapshot format (meta.json / agg/<kind>.json / detail/<table>.
 * parquet) is byte-identical from v2 through v4 — the version bumps changed only
 * the collector's INTERNAL collection machinery (roster+chunks → decoupled
 * create → single-file state), never the published output (see contracts.ts:7-24
 * and the empty diff of detail-schema.ts across v2→v4). So a v2/v3 → v4
 * conversion is a pure metadata rewrite; there is no re-transform (the raw GGG
 * payloads those transforms consumed are long gone — retention sweeps them for
 * every non-in-flight snapshot):
 *
 *   - meta.json: stamp schemaVersion = 4. v2 metas predate `skippedCount` (added
 *     in v3); derive it from the fields already present —
 *       skippedCount = totalCharacters - ok - private - dead - pendingCount
 *     (the v2 invariant was coverage + pendingCount == totalCharacters, so this
 *     yields 0 for a genuine v2 snapshot; a v3 meta already carries the honest
 *     value and is kept as-is). Negative results are clamped to 0 and flagged.
 *   - agg/<kind>.json: stamp schemaVersion = 4 (the file shape is unchanged).
 *   - index.json: stamp each converted snapshot's entry schemaVersion = 4 so the
 *     web reader lists them as first-class v4 instead of greying them out.
 *   - detail/<table>.parquet: UNTOUCHED — the columns/types are identical v2→v4
 *     (shared/detail-schema.ts) and the files carry no schemaVersion field.
 *
 * This is a sanctioned one-time exception to hard rule #4 (completed snapshots
 * immutable): the bytes readers actually consume (coverage, aggregates, detail)
 * do not change — only the version label they are filed under. v1 snapshots
 * (which predate this repo's contract entirely) and v4 snapshots are left alone.
 *
 * Mutates state, so it DEFAULTS TO A DRY RUN and requires an explicit --apply
 * (CLAUDE.md diagnostics rule). Run through the ClaudeDiagnostics workflow so the
 * R2 secrets stay in GitHub's encrypted store:
 *
 *   script = scripts/diagnostics/convert-snapshots-to-v4.ts
 *   args   =            # dry run: report every snapshot that would convert
 *   args   = --apply    # write the conversions
 */
import type { AggregateFile, IndexFile, SnapshotMeta } from '@classolek/shared';
import { INDEX_PATH, SCHEMA_VERSION, classifyKey, snapshotPrefix } from '@classolek/shared';
import { S3ObjectStore } from '../../src/checkpoint/s3-store.js';
import { getJson, listKeys, putJson, type ObjectStore } from '../../src/checkpoint/object-store.js';
import { writeIndex } from '../../src/index-file.js';
import { systemClock } from '../../src/rate-limit/clock.js';

/** v2 metas lack `skippedCount`; model the on-disk shape leniently. */
type StoredMeta = Omit<SnapshotMeta, 'schemaVersion' | 'skippedCount'> & {
  schemaVersion: number;
  skippedCount?: number;
};

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var ${name}`);
  return value;
}

/** `snapshots/<league>/<id>/meta.json` → {league (decoded), snapshotId}. */
function parseMetaKey(key: string): { league: string; snapshotId: string } | undefined {
  const m = /^snapshots\/([^/]+)\/([^/]+)\/meta\.json$/.exec(key);
  if (!m) return undefined;
  return { league: decodeURIComponent(m[1] as string), snapshotId: m[2] as string };
}

interface Planned {
  key: string;
  league: string;
  snapshotId: string;
  from: number;
  complete: boolean;
  derivedSkipped: number;
  hadSkipped: boolean;
  clamped: boolean;
}

async function main(): Promise<void> {
  const apply = process.argv.includes('--apply');
  const store: ObjectStore = new S3ObjectStore({
    bucket: requireEnv('R2_BUCKET'),
    endpoint: requireEnv('R2_ENDPOINT'),
    accessKeyId: requireEnv('R2_ACCESS_KEY_ID'),
    secretAccessKey: requireEnv('R2_SECRET_ACCESS_KEY'),
  });

  // Discover published snapshots from their meta.json objects (the source of
  // truth), not the index — so a snapshot missing from the index still converts.
  const allKeys = await listKeys(store, 'snapshots/');
  const metaKeys = allKeys.filter((k) => classifyKey(k) === 'meta');

  const planned: Planned[] = [];
  let v1 = 0;
  let v4 = 0;

  for (const key of metaKeys) {
    const meta = await getJson<StoredMeta>(store, key);
    if (!meta) continue;
    const version = meta.schemaVersion;
    if (version === 4) {
      v4 += 1;
      continue;
    }
    if (version !== 2 && version !== 3) {
      // v1 (or anything else): out of scope for this migration — its published
      // shape is unverified and possibly divergent. Report, never touch.
      v1 += 1;
      console.log(`  SKIP (v${String(version)}, out of scope): ${key}`);
      continue;
    }
    const ref = parseMetaKey(key) ?? { league: meta.league, snapshotId: meta.snapshotId };
    const cov = meta.coverage ?? { ok: 0, private: 0, dead: 0 };
    const raw = meta.totalCharacters - cov.ok - cov.private - cov.dead - (meta.pendingCount ?? 0);
    const derivedSkipped =
      typeof meta.skippedCount === 'number' ? meta.skippedCount : Math.max(0, raw);
    planned.push({
      key,
      league: ref.league,
      snapshotId: ref.snapshotId,
      from: version,
      complete: meta.complete === true,
      derivedSkipped,
      hadSkipped: typeof meta.skippedCount === 'number',
      clamped: typeof meta.skippedCount !== 'number' && raw < 0,
    });
  }

  planned.sort((a, b) => a.key.localeCompare(b.key));

  console.log(`Scanned ${metaKeys.length} published snapshot(s):`);
  console.log(`  already v4: ${v4}`);
  console.log(`  out of scope (v1/other): ${v1}`);
  console.log(`  to convert (v2/v3 → v4): ${planned.length}\n`);
  for (const p of planned) {
    const note = p.hadSkipped
      ? 'skipped kept'
      : `skipped derived=${p.derivedSkipped}${p.clamped ? ' (clamped from negative!)' : ''}`;
    console.log(
      `  v${p.from} → v4  ${p.league}/${p.snapshotId}` +
        `  [${p.complete ? 'complete' : 'INCOMPLETE'}]  ${note}`,
    );
  }

  if (planned.length === 0) {
    console.log('\nNothing to convert.');
    return;
  }

  if (!apply) {
    console.log('\nDry run: re-run with --apply to write meta / agg / index at v4.');
    return;
  }

  // Apply: per snapshot, rewrite meta then its aggregates. Index is stamped last
  // (mirrors the transform's publish order: data first, index as the entry point).
  const converted = new Set<string>();
  for (const p of planned) {
    const meta = await getJson<StoredMeta>(store, p.key);
    if (!meta) continue;
    meta.schemaVersion = SCHEMA_VERSION;
    meta.skippedCount = p.derivedSkipped;
    await putJson(store, p.key, meta, true);

    const aggKeys = (await listKeys(store, `${snapshotPrefix(p.league, p.snapshotId)}agg/`)).filter(
      (k) => k.endsWith('.json'),
    );
    for (const aggKey of aggKeys) {
      const agg = await getJson<AggregateFile>(store, aggKey);
      if (!agg) continue;
      agg.schemaVersion = SCHEMA_VERSION;
      await putJson(store, aggKey, agg, true);
    }
    converted.add(`${p.league}/${p.snapshotId}`);
    console.log(`  wrote ${p.key} (+${aggKeys.length} agg) at v4`);
  }

  // Stamp the index entries so the reader offers these as v4, not greyed out.
  const index = await getJson<IndexFile>(store, INDEX_PATH);
  if (!index || !Array.isArray(index.leagues)) {
    console.log('\nWARNING: index.json missing/corrupt — snapshots converted, index NOT updated.');
    console.log(`Applied: converted ${converted.size} snapshot(s).`);
    return;
  }
  let stamped = 0;
  for (const league of index.leagues) {
    for (const snap of league.snapshots) {
      if (
        converted.has(`${league.league}/${snap.snapshotId}`) &&
        snap.schemaVersion !== SCHEMA_VERSION
      ) {
        snap.schemaVersion = SCHEMA_VERSION;
        stamped += 1;
      }
    }
  }
  await writeIndex(store, index, systemClock);

  console.log(
    `\nApplied: converted ${converted.size} snapshot(s); stamped ${stamped} index entr(y/ies) to v4.`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
