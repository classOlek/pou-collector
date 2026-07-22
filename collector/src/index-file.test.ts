import { describe, expect, it } from 'vitest';
import type { IndexFile } from '@classolek/shared';
import { INDEX_PATH, SCHEMA_VERSION } from '@classolek/shared';
import { MemoryObjectStore, getJson, putJson } from './checkpoint/object-store.js';
import { FakeClock } from './rate-limit/clock.js';
import {
  markDetailTrimmed,
  readIndex,
  removeSnapshot,
  upsertSnapshot,
  writeIndex,
} from './index-file.js';

function snap(id: string, capturedAt: string, complete = true) {
  return {
    schemaVersion: SCHEMA_VERSION,
    snapshotId: id,
    ladderCapturedAt: capturedAt,
    updatedAt: capturedAt,
    ...(complete ? { completedAt: capturedAt } : {}),
    complete,
    depth: 500,
    totalCharacters: 1,
    coverage: { ok: 1, private: 0, dead: 0 },
    hasDetail: true,
  };
}

describe('index-file', () => {
  it('preserves entries of a foreign schemaVersion across an upsert (finding 5)', async () => {
    const store = new MemoryObjectStore();
    // A pre-existing index written under a *different* schema, with another league.
    const foreign = {
      schemaVersion: 999,
      updatedAt: '2020-01-01T00:00:00.000Z',
      leagues: [
        {
          league: 'Legacy',
          snapshots: [
            {
              snapshotId: 'old-1',
              somethingWeDoNotKnow: true,
              completedAt: '2020-01-01T00:00:00.000Z',
            },
          ],
        },
      ],
    };
    await putJson(store, INDEX_PATH, foreign);

    const index = await readIndex(store);
    upsertSnapshot(index, 'Standard', snap('s-1', '2026-07-17T00:00:00.000Z'));
    await writeIndex(store, index, new FakeClock(Date.parse('2026-07-18T00:00:00.000Z')));

    const after = (await getJson<IndexFile>(store, INDEX_PATH))!;
    // The foreign league + its opaque snapshot survive verbatim; our league is added.
    const legacy = after.leagues.find((l) => l.league === 'Legacy')!;
    expect(legacy.snapshots).toEqual(foreign.leagues[0]!.snapshots);
    expect(after.leagues.find((l) => l.league === 'Standard')!.snapshots[0]!.snapshotId).toBe(
      's-1',
    );
    // Top-level schemaVersion is stamped to the current writer's (the sanctioned bump).
    expect(after.schemaVersion).toBe(SCHEMA_VERSION);
  });

  it('upserts idempotently and keeps snapshots newest-first by capture time', () => {
    const index: IndexFile = { schemaVersion: SCHEMA_VERSION, updatedAt: '', leagues: [] };
    upsertSnapshot(index, 'L', snap('a', '2026-07-01T00:00:00.000Z'));
    upsertSnapshot(index, 'L', snap('b', '2026-07-05T00:00:00.000Z'));
    upsertSnapshot(index, 'L', snap('a', '2026-07-01T00:00:00.000Z')); // re-upsert a
    const snaps = index.leagues[0]!.snapshots;
    expect(snaps.map((s) => s.snapshotId)).toEqual(['b', 'a']); // newest first, no dup
  });

  it('republishes an incomplete snapshot in place, then finalizes it', () => {
    const index: IndexFile = { schemaVersion: SCHEMA_VERSION, updatedAt: '', leagues: [] };
    upsertSnapshot(index, 'L', snap('a', '2026-07-01T00:00:00.000Z', false));
    expect(index.leagues[0]!.snapshots[0]!.complete).toBe(false);
    upsertSnapshot(index, 'L', snap('a', '2026-07-01T00:00:00.000Z', true));
    const snaps = index.leagues[0]!.snapshots;
    expect(snaps).toHaveLength(1);
    expect(snaps[0]!.complete).toBe(true);
  });

  it('removes an aborted in-flight snapshot and drops an emptied league', () => {
    const index: IndexFile = { schemaVersion: SCHEMA_VERSION, updatedAt: '', leagues: [] };
    upsertSnapshot(index, 'L', snap('done', '2026-07-01T00:00:00.000Z'));
    upsertSnapshot(index, 'L', snap('doomed', '2026-07-05T00:00:00.000Z', false));
    upsertSnapshot(index, 'M', snap('only', '2026-07-05T00:00:00.000Z', false));

    expect(removeSnapshot(index, 'L', 'doomed')).toBe(true);
    expect(index.leagues.find((l) => l.league === 'L')!.snapshots.map((s) => s.snapshotId)).toEqual(
      ['done'],
    );
    expect(removeSnapshot(index, 'L', 'missing')).toBe(false);
    // Removing M's only snapshot drops the league entirely.
    expect(removeSnapshot(index, 'M', 'only')).toBe(true);
    expect(index.leagues.some((l) => l.league === 'M')).toBe(false);
  });

  it('marks detail trimmed only once', () => {
    const index: IndexFile = { schemaVersion: SCHEMA_VERSION, updatedAt: '', leagues: [] };
    upsertSnapshot(index, 'L', snap('a', '2026-07-01T00:00:00.000Z'));
    expect(markDetailTrimmed(index, 'L', 'a')).toBe(true);
    expect(index.leagues[0]!.snapshots[0]!.hasDetail).toBe(false);
    expect(markDetailTrimmed(index, 'L', 'a')).toBe(false); // already trimmed
    expect(markDetailTrimmed(index, 'L', 'missing')).toBe(false);
  });
});
