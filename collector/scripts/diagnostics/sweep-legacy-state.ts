/**
 * One-release cleanup: sweep the LEGACY chunk + raw-shard objects the pre-v4
 * (chunk-model) collector left in R2. v4 stores a snapshot as a single NDJSON.gz
 * state file (plus transient per-worker result files) and never writes chunk
 * files (state/<league>/chunks/…) or raw shards (raw/…) again — so once the v4
 * rollout has replaced every in-flight snapshot, everything under those two
 * prefixes is dead weight against the R2 free-tier ceiling (hard rule #5).
 *
 * Retention already reaps orphaned chunk/raw groups on its own schedule; this is
 * the on-demand operator equivalent for verifying and forcing the cleanup during
 * the migration window. It classifies keys through the shared `classifyKey` /
 * parse helpers (never a local regex), and — like retention — leaves alone any
 * chunk/raw group that still belongs to an IN-FLIGHT checkpoint snapshot (a
 * defence-in-depth guard: a v4 snapshot has neither, so in practice nothing is
 * ever protected here, but it means the sweep can never touch live work).
 *
 * Mutates state, so it DEFAULTS TO A DRY RUN and requires an explicit --apply
 * (CLAUDE.md diagnostics rule). Run through the ClaudeDiagnostics workflow so the
 * R2 secrets stay in GitHub's encrypted store:
 *
 *   script = scripts/diagnostics/sweep-legacy-state.ts
 *   args   =            # dry run: report the legacy chunk/raw objects
 *   args   = --apply    # delete them
 */
import { classifyKey, isInFlight, parseChunkKey, parseRawKey } from '@classolek/shared';
import { S3ObjectStore } from '../../src/checkpoint/s3-store.js';
import { CheckpointStore } from '../../src/checkpoint/store.js';
import type { ObjectInfo } from '../../src/checkpoint/object-store.js';

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var ${name}`);
  return value;
}

const MB = 1024 * 1024;

async function main(): Promise<void> {
  const apply = process.argv.includes('--apply');
  const store = new S3ObjectStore({
    bucket: requireEnv('R2_BUCKET'),
    endpoint: requireEnv('R2_ENDPOINT'),
    accessKeyId: requireEnv('R2_ACCESS_KEY_ID'),
    secretAccessKey: requireEnv('R2_SECRET_ACCESS_KEY'),
  });
  const checkpoints = new CheckpointStore(store);

  // Any chunk/raw group belonging to a still-in-flight snapshot is left alone
  // (mirrors retention's guard). A v4 snapshot writes neither, so this only ever
  // matters for a pre-v4 snapshot caught mid-migration.
  const inFlight = new Set(
    (await checkpoints.listAll())
      .filter((m) => isInFlight(m.phase))
      .map((m) => `${m.league}/${m.snapshotId}`),
  );

  const all: ObjectInfo[] = await store.listDetailed('');
  let chunkCount = 0;
  let chunkBytes = 0;
  let rawCount = 0;
  let rawBytes = 0;
  const doomed: string[] = [];
  const protectedRefs = new Set<string>();

  for (const info of all) {
    const category = classifyKey(info.key);
    if (category !== 'chunk' && category !== 'raw') continue;
    const ref = category === 'chunk' ? parseChunkKey(info.key) : parseRawKey(info.key);
    const id = ref ? `${ref.league}/${ref.snapshotId}` : undefined;
    if (id && inFlight.has(id)) {
      protectedRefs.add(id);
      continue;
    }
    if (category === 'chunk') {
      chunkCount += 1;
      chunkBytes += info.size;
    } else {
      rawCount += 1;
      rawBytes += info.size;
    }
    doomed.push(info.key);
  }

  console.log(
    `legacy chunk objects: ${chunkCount} (${(chunkBytes / MB).toFixed(2)} MB)\n` +
      `legacy raw objects:   ${rawCount} (${(rawBytes / MB).toFixed(2)} MB)\n` +
      `total to sweep:       ${doomed.length} (${((chunkBytes + rawBytes) / MB).toFixed(2)} MB)`,
  );
  if (protectedRefs.size > 0) {
    console.log(`protected (in-flight, left untouched): ${[...protectedRefs].sort().join(', ')}`);
  }

  if (!apply) {
    console.log('\nDry run: re-run with --apply to delete the legacy chunk + raw objects.');
    return;
  }

  let deleted = 0;
  for (const key of doomed) {
    await store.delete(key);
    deleted += 1;
  }
  console.log(`\nApplied: deleted ${deleted} legacy object(s).`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
