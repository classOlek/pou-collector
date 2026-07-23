/**
 * Shared test helpers so the run and transform suites read shards, tally
 * outcomes and build manifests the same way.
 */
import { gunzipSync, gzipSync } from 'node:zlib';
import type { LimiterMemory, SnapshotCharacter, SnapshotManifest } from '@classolek/shared';
import {
  SCHEMA_VERSION,
  emptyTally,
  rawChunkShardPath,
  tallyOutcomes,
  workerResultPath,
} from '@classolek/shared';
import type { MemoryObjectStore } from '../src/checkpoint/object-store.js';

// Re-export the single production tally so tests never re-implement it.
export { tallyOutcomes };

/** Seed-state limiter memory used by limiter fixtures across suites. */
export function fixtureLimiterMemory(): LimiterMemory {
  return {
    observedRules: [],
    penaltyUntil: 0,
    consecutiveThrottles: 0,
    consecutiveErrors: 0,
    recentAcquires: [],
  };
}

/** A minimal valid v2 manifest; override what the test cares about. */
export function fixtureManifest(overrides: Partial<SnapshotManifest> = {}): SnapshotManifest {
  return {
    schemaVersion: SCHEMA_VERSION,
    snapshotId: 'snap-fixture',
    league: 'TestLeague',
    depth: 5,
    phase: 'collecting',
    ladderCapturedAt: '2026-07-17T00:00:00.000Z',
    chunkSize: 50,
    chunkCount: 1,
    totalCharacters: 5,
    outcomes: { ...emptyTally(), pending: 5 },
    resolvedChunks: 0,
    ...overrides,
  };
}

/** Gunzip + parse one raw shard into its NDJSON records. */
export async function readShard(store: MemoryObjectStore, key: string): Promise<unknown[]> {
  const bytes = await store.get(key);
  if (!bytes) throw new Error(`no object at ${key}`);
  return gunzipSync(Buffer.from(bytes))
    .toString('utf8')
    .split('\n')
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as unknown);
}

/** All records across every raw shard, in lexicographic shard order. */
export async function readAllShards(store: MemoryObjectStore): Promise<unknown[]> {
  const records: unknown[] = [];
  for (const key of store
    .keys()
    .filter((k) => k.startsWith('raw/'))
    .sort()) {
    records.push(...(await readShard(store, key)));
  }
  return records;
}

/**
 * Decode one worker's v4 result file (the gzipped NDJSON `SnapshotCharacter`
 * lines it wrote this run); undefined when the slot never flushed one.
 */
export async function readWorkerResult(
  store: MemoryObjectStore,
  league: string,
  snapshotId: string,
  workerIndex: number,
): Promise<SnapshotCharacter[] | undefined> {
  const bytes = await store.get(workerResultPath(league, snapshotId, workerIndex));
  if (!bytes) return undefined;
  return gunzipSync(Buffer.from(bytes))
    .toString('utf8')
    .split('\n')
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as SnapshotCharacter);
}

/** Write records as one gzipped NDJSON raw chunk shard at the canonical path. */
export async function putRawShard(
  store: MemoryObjectStore,
  league: string,
  snapshotId: string,
  chunkIndex: number,
  records: unknown[],
  seq = 0,
): Promise<string> {
  const ndjson = records.map((r) => JSON.stringify(r)).join('\n') + '\n';
  const key = rawChunkShardPath(league, snapshotId, chunkIndex, seq);
  await store.put(key, gzipSync(Buffer.from(ndjson, 'utf8')));
  return key;
}
