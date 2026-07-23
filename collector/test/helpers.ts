/**
 * Shared test helpers so the run and transform suites read result files, tally
 * outcomes and build manifests the same way.
 */
import { gunzipSync } from 'node:zlib';
import type { LimiterMemory, SnapshotCharacter, SnapshotManifest } from '@classolek/shared';
import { SCHEMA_VERSION, emptyTally, tallyOutcomes, workerResultPath } from '@classolek/shared';
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

/** A minimal valid v4 manifest; override what the test cares about. */
export function fixtureManifest(overrides: Partial<SnapshotManifest> = {}): SnapshotManifest {
  return {
    schemaVersion: SCHEMA_VERSION,
    snapshotId: 'snap-fixture',
    league: 'TestLeague',
    depth: 5,
    phase: 'collecting',
    ladderCapturedAt: '2026-07-17T00:00:00.000Z',
    totalCharacters: 5,
    outcomes: { ...emptyTally(), pending: 5 },
    ...overrides,
  };
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
