/**
 * Operator diagnostic: clear a league's snapshot checkpoint (and its private
 * state) so the next collect run BEGINS A FRESH SNAPSHOT instead of resuming the
 * frozen one. Use when you deliberately want to discard an in-flight snapshot —
 * e.g. to re-capture at a different depth, since the ladder queue is frozen at
 * capture time and a depth change only takes effect on a new snapshot.
 *
 * v4 (state-file model): the snapshot's data is its single NDJSON.gz state file
 * plus any transient per-worker result files; this deletes both for the current
 * snapshot, and still sweeps any LEGACY raw shards / chunk files a pre-v4
 * snapshot left behind. The roster and per-slot limiter states are preserved.
 *
 * Unlike the `reset_aborted` workflow input (which only clears `aborted`
 * checkpoints), this clears a checkpoint in ANY phase, so it is gated behind an
 * explicit --league and defaults to a dry run.
 *
 *   script = scripts/diagnostics/clear-checkpoint.ts
 *   args   = --league Mirage            # dry run: report what would be cleared
 *   args   = --league Mirage --apply    # clear the checkpoint + snapshot state
 */
import {
  RAW_PREFIX,
  STATE_PREFIX,
  parseChunkKey,
  snapshotStatePath,
  workerResultPrefix,
} from '@classolek/shared';
import { S3ObjectStore } from '../../src/checkpoint/s3-store.js';
import { CheckpointStore } from '../../src/checkpoint/store.js';
import { listKeys } from '../../src/checkpoint/object-store.js';

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var ${name}`);
  return value;
}

function argValue(flag: string): string | undefined {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

async function main(): Promise<void> {
  const apply = process.argv.includes('--apply');
  const league = argValue('--league');
  if (!league) throw new Error('Missing required argument: --league <name>');

  const store = new S3ObjectStore({
    bucket: requireEnv('R2_BUCKET'),
    endpoint: requireEnv('R2_ENDPOINT'),
    accessKeyId: requireEnv('R2_ACCESS_KEY_ID'),
    secretAccessKey: requireEnv('R2_SECRET_ACCESS_KEY'),
  });
  const checkpoints = new CheckpointStore(store);

  const manifest = await checkpoints.load(league);
  if (manifest) {
    console.log(
      `checkpoint for "${league}": phase=${manifest.phase} snapshotId=${manifest.snapshotId} ` +
        `depth=${manifest.depth} characters=${manifest.totalCharacters}`,
    );
  } else {
    console.log(`no checkpoint found for "${league}"`);
  }

  // v4 private state for the current snapshot: the single state file + its
  // transient per-worker result files (deleted by exact key / prefix).
  const stateKeys: string[] = [];
  if (manifest) {
    stateKeys.push(snapshotStatePath(league, manifest.snapshotId));
    stateKeys.push(...(await listKeys(store, workerResultPrefix(league, manifest.snapshotId))));
  }
  console.log(`v4 snapshot state objects for "${league}": ${stateKeys.length}`);

  const rawPrefix = `${RAW_PREFIX}${encodeURIComponent(league)}/`;
  const rawKeys = await listKeys(store, rawPrefix);
  console.log(`legacy raw shards under ${rawPrefix}: ${rawKeys.length}`);
  // Legacy chunk files live under state/<league>/chunks/ (the roster and worker
  // limiter states are deliberately preserved).
  const chunkKeys = (await listKeys(store, `${STATE_PREFIX}${encodeURIComponent(league)}/`)).filter(
    (key) => parseChunkKey(key) !== undefined,
  );
  console.log(`legacy chunk files for "${league}": ${chunkKeys.length}`);

  if (!apply) {
    console.log(
      '\nDry run: re-run with --apply to clear the checkpoint and delete the ' +
        'snapshot state file + result files (and any legacy raw shards + chunks).',
    );
    return;
  }

  await checkpoints.clear(league);
  for (const key of stateKeys) await store.delete(key);
  for (const key of rawKeys) await store.delete(key);
  for (const key of chunkKeys) await store.delete(key);
  console.log(
    `\nApplied: cleared checkpoint for "${league}", deleted ${stateKeys.length} snapshot ` +
      `state object(s), ${rawKeys.length} legacy raw shard(s) and ${chunkKeys.length} legacy chunk file(s).`,
  );
  console.log('The next coordinate run will begin a fresh snapshot.');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
