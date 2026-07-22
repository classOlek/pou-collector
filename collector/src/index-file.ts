/**
 * index.json helpers (the web app's mutable entry point). Kept neutral so both
 * the transform (publish: upsert this snapshot) and retention (mark trimmed
 * detail) share one implementation.
 *
 * Lenient by design (hard rule #5 / the sanctioned schema-bump fix): existing
 * leagues and snapshots are preserved verbatim — even entries written under a
 * different schemaVersion — so a bump can never erase published snapshots from
 * the frontend. Only this writer's own snapshot entry is upserted.
 */
import type { Clock } from './rate-limit/clock.js';
import type { IndexFile, IndexSnapshot } from '@classolek/shared';
import { INDEX_PATH, SCHEMA_VERSION } from '@classolek/shared';
import { getJson, putJson, type ObjectStore } from './checkpoint/object-store.js';

export function emptyIndex(): IndexFile {
  return { schemaVersion: SCHEMA_VERSION, updatedAt: '', leagues: [] };
}

/** Read index.json, preserving entries we don't understand; corrupt → empty. */
export async function readIndex(store: ObjectStore): Promise<IndexFile> {
  let parsed: unknown;
  try {
    parsed = await getJson<unknown>(store, INDEX_PATH);
  } catch {
    // Corrupt index — rebuild from what we publish now; snapshots are the durable
    // record, the index is a derived convenience.
    return emptyIndex();
  }
  if (parsed && typeof parsed === 'object' && Array.isArray((parsed as IndexFile).leagues)) {
    const idx = parsed as IndexFile;
    return {
      schemaVersion: SCHEMA_VERSION,
      updatedAt: typeof idx.updatedAt === 'string' ? idx.updatedAt : '',
      leagues: idx.leagues,
    };
  }
  return emptyIndex();
}

/**
 * Upsert one snapshot into its league, newest first. Foreign entries untouched.
 * Sorted by ladderCapturedAt (not completedAt): it exists for every snapshot —
 * an incomplete snapshot has no completedAt yet — and is stable across the
 * incremental republishes of an in-progress snapshot.
 */
export function upsertSnapshot(
  index: IndexFile,
  league: string,
  snapshot: IndexSnapshot,
): IndexFile {
  let entry = index.leagues.find((l) => l.league === league);
  if (!entry) {
    entry = { league, snapshots: [] };
    index.leagues.push(entry);
  }
  entry.snapshots = [
    snapshot,
    ...entry.snapshots.filter((s) => s.snapshotId !== snapshot.snapshotId),
  ].sort((a, b) => b.ladderCapturedAt.localeCompare(a.ladderCapturedAt));
  index.leagues.sort((a, b) => a.league.localeCompare(b.league));
  return index;
}

/**
 * Remove a snapshot's entry (abort of an in-flight, incomplete snapshot — a
 * completed snapshot is immutable and is never removed). Returns whether the
 * entry existed. An emptied league is dropped so the picker doesn't offer it.
 */
export function removeSnapshot(index: IndexFile, league: string, snapshotId: string): boolean {
  const entry = index.leagues.find((l) => l.league === league);
  if (!entry) return false;
  const before = entry.snapshots.length;
  entry.snapshots = entry.snapshots.filter((s) => s.snapshotId !== snapshotId);
  if (entry.snapshots.length === 0) {
    index.leagues = index.leagues.filter((l) => l !== entry);
  }
  return entry.snapshots.length < before;
}

/** Mark a snapshot's detail as trimmed (retention). Returns whether it was found. */
export function markDetailTrimmed(index: IndexFile, league: string, snapshotId: string): boolean {
  const entry = index.leagues
    .find((l) => l.league === league)
    ?.snapshots.find((s) => s.snapshotId === snapshotId);
  if (!entry || entry.hasDetail === false) return false;
  entry.hasDetail = false;
  return true;
}

export async function writeIndex(
  store: ObjectStore,
  index: IndexFile,
  clock: Clock,
): Promise<void> {
  index.updatedAt = new Date(clock.now()).toISOString();
  await putJson(store, INDEX_PATH, index, true);
}
