/**
 * Read-only verification of the pinned passive-tree source that resolves each
 * character's passive-node hashes → names/stats (docs/ARCHITECTURE.md §6). It
 * answers the two questions the transform depends on, WITHOUT publishing:
 *
 *   1. Does `config.treeUrl` fetch + `normalizeTree` cleanly (the exact
 *      production path via HttpTreeOrigin) and produce nodes?
 *   2. Do the tree's node hashes actually cover the `hashes` GGG returns for the
 *      real collected characters? A tree that parses but whose ids don't match
 *      `get-passive-skills.hashes` resolves every passive to NULL — the failure
 *      this check exists to catch (handoff §3 "hashes must match").
 *
 * Step 1 runs anywhere (needs only outbound HTTPS). Step 2 runs when R2_* are
 * present (inject them via the ClaudeDiagnostics workflow — never paste secrets)
 * and samples the raw shards of the league's current snapshot straight from R2.
 * It changes nothing; there is no --apply.
 *
 *   script = scripts/diagnostics/verify-tree.ts
 *   args   = [--snapshot=<id>] [--max-shards=<n>]   (both optional)
 */
import { gunzipSync } from 'node:zlib';
import type { SnapshotManifest } from '@classolek/shared';
import { checkpointPath, rawShardPrefix } from '@classolek/shared';
import { loadConfig, treeVersionFor } from '../../src/config-file.js';
import { buildUserAgent } from '../../src/config.js';
import { createFetchHttpClient } from '../../src/http/fetch-client.js';
import { HttpTreeOrigin } from '../../src/transform/tree-origin.js';
import { S3ObjectStore } from '../../src/checkpoint/s3-store.js';
import { getJson, listKeys } from '../../src/checkpoint/object-store.js';

function argValue(flag: string): string | undefined {
  const hit = process.argv.find((a) => a.startsWith(`${flag}=`));
  return hit?.slice(flag.length + 1);
}

/** One raw shard line: only the fields this check reads (see sql.ts RAW_COLUMNS). */
interface RawCharacter {
  account?: string;
  character?: string;
  passives?: { hashes?: number[] };
}

function r2StoreFromEnv(): S3ObjectStore | undefined {
  const bucket = process.env.R2_BUCKET;
  const endpoint = process.env.R2_ENDPOINT;
  const accessKeyId = process.env.R2_ACCESS_KEY_ID;
  const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY;
  if (!bucket || !endpoint || !accessKeyId || !secretAccessKey) return undefined;
  return new S3ObjectStore({ bucket, endpoint, accessKeyId, secretAccessKey });
}

async function main(): Promise<void> {
  const config = loadConfig();
  const treeVersion = treeVersionFor(config.leagues, config.league);
  const treeUrl = config.treeUrl.replace('{version}', encodeURIComponent(treeVersion));

  // 1. Fetch + normalize via the exact production origin the transform uses.
  console.log('=== tree source ===');
  console.log(`league:      ${config.league}`);
  console.log(`treeVersion: ${treeVersion} (config/leagues.json)`);
  console.log(`treeUrl:     ${treeUrl}`);
  const origin = new HttpTreeOrigin(createFetchHttpClient(), {
    treeUrl: config.treeUrl,
    userAgent: buildUserAgent(),
  });
  const tree = await origin.fetch(treeVersion);
  const keystones = tree.nodes.filter((n) => n.isKeystone);
  console.log(`nodes:       ${tree.nodes.length} (${keystones.length} keystones)`);
  for (const n of keystones.slice(0, 3)) {
    console.log(`  keystone ${n.hash}: ${n.name} — ${JSON.stringify(n.stats).slice(0, 120)}`);
  }
  if (tree.nodes.length === 0) throw new Error('tree produced zero nodes');

  const known = new Set(tree.nodes.map((n) => n.hash));

  // 2. Real-character coverage (needs R2). Absent creds → step 1 still ran.
  const store = r2StoreFromEnv();
  if (!store) {
    console.log('\n=== real-character coverage: SKIPPED (no R2_* env) ===');
    console.log('Run via the ClaudeDiagnostics workflow to validate against collected characters.');
    return;
  }

  const league = config.league;
  let snapshotId = argValue('--snapshot');
  if (!snapshotId) {
    const manifest = await getJson<SnapshotManifest>(store, checkpointPath(league));
    if (!manifest)
      throw new Error(`no checkpoint at ${checkpointPath(league)} and no --snapshot given`);
    snapshotId = manifest.snapshotId;
    console.log(
      `\ncheckpoint: ${league} snapshot ${snapshotId} phase=${manifest.phase}` +
        ` transformAttempts=${manifest.transformAttempts ?? 0}`,
    );
  }

  const maxShards = Number(argValue('--max-shards') ?? '0') || Infinity;
  const shardKeys = (await listKeys(store, rawShardPrefix(league, snapshotId))).sort();
  const sampled = shardKeys.slice(0, Number.isFinite(maxShards) ? maxShards : shardKeys.length);
  console.log(`\n=== real-character coverage ===`);
  console.log(
    `raw shards: ${shardKeys.length}${sampled.length < shardKeys.length ? ` (sampling ${sampled.length})` : ''}`,
  );
  if (shardKeys.length === 0)
    throw new Error(`no raw shards under ${rawShardPrefix(league, snapshotId)}`);

  let characters = 0;
  let charsWithHashes = 0;
  let charsFullyResolved = 0;
  const unresolved = new Map<number, number>(); // hash → occurrences
  const allHashes = new Set<number>();

  for (const key of sampled) {
    const gz = await store.get(key);
    if (!gz) continue;
    const text = gunzipSync(Buffer.from(gz)).toString('utf8');
    for (const line of text.split('\n')) {
      if (!line.trim()) continue;
      const rec = JSON.parse(line) as RawCharacter;
      characters += 1;
      const hashes = rec.passives?.hashes ?? [];
      if (hashes.length === 0) continue;
      charsWithHashes += 1;
      let missing = 0;
      for (const h of hashes) {
        allHashes.add(h);
        if (!known.has(h)) {
          missing += 1;
          unresolved.set(h, (unresolved.get(h) ?? 0) + 1);
        }
      }
      if (missing === 0) charsFullyResolved += 1;
    }
  }

  const resolvedDistinct = [...allHashes].filter((h) => known.has(h)).length;
  const rate = allHashes.size > 0 ? resolvedDistinct / allHashes.size : 0;
  console.log(`characters:            ${characters} (${charsWithHashes} with allocated passives)`);
  console.log(
    `distinct hashes:       ${allHashes.size} (${resolvedDistinct} resolve, ${unresolved.size} miss, ${(rate * 100).toFixed(1)}%)`,
  );
  console.log(`fully-resolved chars:  ${charsFullyResolved}/${charsWithHashes}`);
  if (unresolved.size > 0) {
    const worst = [...unresolved.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20);
    console.log(
      `unresolved hashes (top ${worst.length} by frequency): ` +
        worst.map(([h, c]) => `${h}×${c}`).join(', '),
    );
    console.log(
      'note: a small unresolved tail is expected — Timeless-Jewel/tattoo-transformed nodes carry',
    );
    console.log(
      '      alternate ids absent from every static tree export, so the LEFT JOIN keeps them NULL.',
    );
  }

  // The failure this guards against is a WRONG tree (bad url / off-by-a-league /
  // empty source) — that collapses resolution to near zero. An expected
  // jewel/tattoo tail keeps it in the high-90s, so gate on the rate, not on a
  // literal zero misses. Threshold well below normal drift, well above a mismatch.
  const RESOLVE_FLOOR = 0.9;
  const pass = charsWithHashes > 0 && rate >= RESOLVE_FLOOR;
  console.log(
    `\nRESULT: ${
      pass
        ? `PASS — ${(rate * 100).toFixed(1)}% of collected passives resolve against the pinned tree (unresolved tail is jewel/tattoo alternates)`
        : `FAIL — only ${(rate * 100).toFixed(1)}% resolve (< ${RESOLVE_FLOOR * 100}%); the tree version/source does not match GGG hashes`
    }`,
  );
  if (!pass) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
