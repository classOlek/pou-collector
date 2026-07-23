/**
 * Retention (Phase 4, docs/ARCHITECTURE.md §6/§7).
 *
 * R2 has a 10 GB free-tier ceiling. Retention:
 *  - reports usage by key category (a single sized listing, no body fetches);
 *  - sweeps orphaned transient state — legacy raw shards / chunk files AND the
 *    v4 snapshot state file / per-worker result files — of any snapshot that is
 *    not the in-flight checkpoint snapshot (all of it is transient by
 *    definition), so this owns whatever a crashed publish or abort could leak
 *    (including the multi-hundred-MB state file a crash in transform's
 *    publish→delete window strands once its phase is already `published`);
 *  - trims the OLDEST detail Parquet first — ordered by snapshot age across ALL
 *    leagues (by meta.completedAt), a whole snapshot at a time — until usage is
 *    under budget, NEVER touching aggregates/meta/index, and always keeping the
 *    newest keepRecentDetail snapshots per league;
 *  - marks index.json entries whose detail it trims `hasDetail: false` so the web
 *    app offers aggregates-only instead of 404-ing the explorer (index is the
 *    mutable entry point; meta.json stays immutable).
 */
import type { KeyCategory, SnapshotMeta, SnapshotRef } from '@classolek/shared';
import {
  chunkPrefix,
  classifyKey,
  isInFlight,
  parseChunkKey,
  parseDetailKey,
  parseRawKey,
  parseSnapshotStateKey,
  parseWorkerResultKey,
  rawShardPrefix,
  snapshotMetaPath,
  snapshotStatePath,
  workerResultPrefix,
} from '@classolek/shared';
import type { Clock } from '../rate-limit/clock.js';
import type { CheckpointStore } from '../checkpoint/store.js';
import { getJson, type ObjectInfo, type ObjectStore } from '../checkpoint/object-store.js';
import { markDetailTrimmed, readIndex, writeIndex } from '../index-file.js';

export interface RetentionConfig {
  /** Trim detail Parquet once total usage exceeds this many bytes. */
  budgetBytes: number;
  /** Never trim detail for the newest N snapshots per league, whatever the budget. */
  keepRecentDetail: number;
}

export interface RetentionDeps {
  clock: Clock;
  objectStore: ObjectStore;
  checkpointStore: CheckpointStore;
}

export interface RetentionSummary {
  usageByPrefix: Record<KeyCategory, number>;
  totalBytes: number;
  budgetBytes: number;
  /** "<league>/<snapshotId>" of every snapshot whose detail was trimmed. */
  detailSnapshotsTrimmed: string[];
  /**
   * "<league>/<snapshotId>" of every orphaned transient group swept (raw /
   * chunk / snapshot-state / worker-result). A snapshot with more than one such
   * group appears once per group, mirroring the pre-existing raw+chunk behavior.
   */
  rawSnapshotsSwept: string[];
  bytesFreed: number;
}

interface DetailSnapshot {
  league: string;
  snapshotId: string;
  /** "<league>/<snapshotId>" — stable id for reporting. */
  ref: string;
  keys: string[];
  bytes: number;
  /** meta.completedAt epoch ms; -Infinity when meta is missing (treated oldest). */
  ageMs: number;
}

function emptyUsage(): Record<KeyCategory, number> {
  return {
    raw: 0,
    detail: 0,
    agg: 0,
    meta: 0,
    tree: 0,
    checkpoint: 0,
    roster: 0,
    'snapshot-state': 0,
    'worker-result': 0,
    chunk: 0,
    worker: 0,
    ip: 0,
    index: 0,
    economy: 0,
    other: 0,
  };
}

export async function runRetention(
  config: RetentionConfig,
  deps: RetentionDeps,
): Promise<RetentionSummary> {
  const store = deps.objectStore;
  const all = await store.listDetailed('');

  const usageByPrefix = emptyUsage();
  for (const info of all) usageByPrefix[classifyKey(info.key)] += info.size;
  let totalBytes = all.reduce((sum, info) => sum + info.size, 0);

  // 1. Sweep orphaned transient state (owns what a crashed publish/abort leaks):
  //    any group whose snapshot is not in-flight is garbage. Covers the v4
  //    state file / per-worker result files AND the legacy raw shards / chunk
  //    files, so a crash in transform's publish→delete window (manifest already
  //    `published`, so not in-flight) can no longer strand the state file
  //    forever. Each toPrefix below must produce the exact string added to the
  //    in-flight guard set for its category — that is the single-owner invariant.
  const inflightManifests = (await deps.checkpointStore.listAll()).filter((m) =>
    isInFlight(m.phase),
  );
  const inflight = new Set(
    inflightManifests.flatMap((m) => [
      rawShardPrefix(m.league, m.snapshotId),
      chunkPrefix(m.league, m.snapshotId),
      snapshotStatePath(m.league, m.snapshotId),
      workerResultPrefix(m.league, m.snapshotId),
    ]),
  );
  const rawSnapshotsSwept: string[] = [];
  let bytesFreed = 0;
  for (const [category, groups] of [
    ['raw', groupByPrefix(all, parseRawKey, rawShardPrefix)],
    ['chunk', groupByPrefix(all, parseChunkKey, chunkPrefix)],
    ['snapshot-state', groupByPrefix(all, parseSnapshotStateKey, snapshotStatePath)],
    ['worker-result', groupByPrefix(all, parseWorkerResultKey, workerResultPrefix)],
  ] as const) {
    for (const [prefix, group] of groups) {
      if (inflight.has(prefix)) continue;
      for (const key of group.keys) await store.delete(key);
      totalBytes -= group.bytes;
      usageByPrefix[category] -= group.bytes;
      bytesFreed += group.bytes;
      rawSnapshotsSwept.push(group.ref);
    }
  }

  // 2. Trim oldest detail across all leagues until under budget.
  const details = await loadDetailSnapshots(store, all);
  const protectedRefs = protectRecentPerLeague(details, config.keepRecentDetail);
  const candidates = details
    .filter((d) => !protectedRefs.has(d.ref))
    .sort((a, b) => a.ageMs - b.ageMs); // oldest first, across leagues

  const detailSnapshotsTrimmed: string[] = [];
  const trimmed: DetailSnapshot[] = [];
  for (const snap of candidates) {
    if (totalBytes <= config.budgetBytes) break;
    for (const key of snap.keys) await store.delete(key);
    totalBytes -= snap.bytes;
    usageByPrefix.detail -= snap.bytes;
    bytesFreed += snap.bytes;
    detailSnapshotsTrimmed.push(snap.ref);
    trimmed.push(snap);
  }

  // 3. Reflect trimmed detail in the index (aggregates-only, never a 404).
  if (trimmed.length > 0) {
    const index = await readIndex(store);
    let changed = false;
    for (const snap of trimmed) {
      if (markDetailTrimmed(index, snap.league, snap.snapshotId)) changed = true;
    }
    if (changed) await writeIndex(store, index, deps.clock);
  }

  return {
    usageByPrefix,
    totalBytes,
    budgetBytes: config.budgetBytes,
    detailSnapshotsTrimmed,
    rawSnapshotsSwept,
    bytesFreed,
  };
}

function groupByPrefix(
  all: ObjectInfo[],
  parse: (key: string) => SnapshotRef | undefined,
  toPrefix: (league: string, snapshotId: string) => string,
): Map<string, { ref: string; keys: string[]; bytes: number }> {
  const groups = new Map<string, { ref: string; keys: string[]; bytes: number }>();
  for (const info of all) {
    const ref = parse(info.key);
    if (!ref) continue;
    const prefix = toPrefix(ref.league, ref.snapshotId);
    const group = groups.get(prefix) ?? {
      ref: `${ref.league}/${ref.snapshotId}`,
      keys: [],
      bytes: 0,
    };
    group.keys.push(info.key);
    group.bytes += info.size;
    groups.set(prefix, group);
  }
  return groups;
}

async function loadDetailSnapshots(
  store: ObjectStore,
  all: ObjectInfo[],
): Promise<DetailSnapshot[]> {
  const groups = new Map<string, DetailSnapshot>();
  for (const info of all) {
    const ref = parseDetailKey(info.key);
    if (!ref) continue;
    const id = `${ref.league}/${ref.snapshotId}`;
    const snap =
      groups.get(id) ??
      ({
        league: ref.league,
        snapshotId: ref.snapshotId,
        ref: id,
        keys: [],
        bytes: 0,
        ageMs: 0,
      } satisfies DetailSnapshot);
    snap.keys.push(info.key);
    snap.bytes += info.size;
    groups.set(id, snap);
  }
  // Age each snapshot by its published meta (robust across id formats). An
  // incomplete snapshot has no completedAt yet — its last publish time stands
  // in, which also keeps the in-flight snapshot newest (never trim-first).
  for (const snap of groups.values()) {
    const meta = await getJson<SnapshotMeta>(
      store,
      snapshotMetaPath(snap.league, snap.snapshotId),
    ).catch(() => undefined);
    const ms = meta ? Date.parse(meta.completedAt ?? meta.updatedAt) : Number.NaN;
    snap.ageMs = Number.isNaN(ms) ? Number.NEGATIVE_INFINITY : ms;
  }
  return [...groups.values()];
}

/** Newest keepN snapshots per league (by age) are never trimmed. */
function protectRecentPerLeague(details: DetailSnapshot[], keepN: number): Set<string> {
  const byLeague = new Map<string, DetailSnapshot[]>();
  for (const snap of details) {
    const list = byLeague.get(snap.league) ?? [];
    list.push(snap);
    byLeague.set(snap.league, list);
  }
  const protectedRefs = new Set<string>();
  for (const list of byLeague.values()) {
    list.sort((a, b) => b.ageMs - a.ageMs); // newest first
    for (const snap of list.slice(0, keepN)) protectedRefs.add(snap.ref);
  }
  return protectedRefs;
}
