/**
 * Operator diagnostic: clear a league's snapshot checkpoint (and its raw shards)
 * so the next collect run BEGINS A FRESH SNAPSHOT instead of resuming the frozen
 * one. Use when you deliberately want to discard an in-flight snapshot — e.g. to
 * re-capture at a different depth, since the ladder queue is frozen at capture
 * time and a depth change only takes effect on a new snapshot.
 *
 * Unlike the `reset_aborted` workflow input (which only clears `aborted`
 * checkpoints), this clears a checkpoint in ANY phase, so it is gated behind an
 * explicit --league and defaults to a dry run.
 *
 *   script = scripts/diagnostics/clear-checkpoint.ts
 *   args   = --league Mirage            # dry run: report what would be cleared
 *   args   = --league Mirage --apply    # clear the checkpoint + raw shards
 */
import { RAW_PREFIX, STATE_PREFIX, parseChunkKey } from '@pou/shared';
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
        `depth=${manifest.depth} characters=${manifest.totalCharacters} ` +
        `chunks=${manifest.resolvedChunks}/${manifest.chunkCount}`,
    );
  } else {
    console.log(`no checkpoint found for "${league}"`);
  }

  const rawPrefix = `${RAW_PREFIX}${encodeURIComponent(league)}/`;
  const rawKeys = await listKeys(store, rawPrefix);
  console.log(`raw shards under ${rawPrefix}: ${rawKeys.length}`);
  // The snapshot's chunk files live under state/<league>/chunks/ (the roster
  // and worker limiter states are deliberately preserved).
  const chunkKeys = (await listKeys(store, `${STATE_PREFIX}${encodeURIComponent(league)}/`)).filter(
    (key) => parseChunkKey(key) !== undefined,
  );
  console.log(`chunk files for "${league}": ${chunkKeys.length}`);

  if (!apply) {
    console.log(
      '\nDry run: re-run with --apply to clear the checkpoint and delete the raw shards + chunks.',
    );
    return;
  }

  await checkpoints.clear(league);
  for (const key of rawKeys) await store.delete(key);
  for (const key of chunkKeys) await store.delete(key);
  console.log(
    `\nApplied: cleared checkpoint for "${league}", deleted ${rawKeys.length} raw shard(s) ` +
      `and ${chunkKeys.length} chunk file(s).`,
  );
  console.log('The next coordinate run will begin a fresh snapshot.');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
