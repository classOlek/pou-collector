/**
 * Canonical R2 object layout (docs/ARCHITECTURE.md §6).
 * Both the collector (writes) and the web app (reads) resolve paths
 * through these helpers so the layout can never silently diverge. Retention and
 * the raw sweeper classify keys through `classifyKey` / the parse helpers below
 * rather than re-encoding the layout with local regexes.
 */

export const INDEX_PATH = 'index.json';

export const STATE_PREFIX = 'state/';
export const TREE_PREFIX = 'state/tree/';
export const RAW_PREFIX = 'raw/';
export const SNAPSHOTS_PREFIX = 'snapshots/';
export const ECONOMY_PREFIX = 'economy/';

/**
 * The poe.ninja economy cache for one league (public): ONE file per league,
 * overwritten in place by each ECONOMY POE.NINJA fire (see economy.ts).
 */
export function economyPath(league: string): string {
  return `${ECONOMY_PREFIX}${encodeURIComponent(league)}.json`;
}

export function checkpointPath(league: string): string {
  return `${STATE_PREFIX}${encodeURIComponent(league)}/current.json`;
}

/** Per-league character roster (private): the growing character database. */
export function rosterPath(league: string): string {
  return `${STATE_PREFIX}${encodeURIComponent(league)}/roster.json`;
}

/**
 * A snapshot's single NDJSON.gz state file (private): one line per character
 * (SnapshotCharacter). Written whole by the coordinator/create + finalize steps
 * (serialized by the concurrency group — one writer), streamed a line at a time
 * by every reader/worker. This is the v4 replacement for the per-snapshot
 * chunk-file fan-out (and, for `ok` lines, the raw shards too).
 */
export function snapshotStatePath(league: string, snapshotId: string): string {
  return `${STATE_PREFIX}${encodeURIComponent(league)}/snapshots/${snapshotId}.ndjson.gz`;
}

/** Prefix under which a snapshot's transient per-worker result files live. */
export function workerResultPrefix(league: string, snapshotId: string): string {
  return `${STATE_PREFIX}${encodeURIComponent(league)}/results/${snapshotId}/`;
}

/**
 * One worker slot's transient result file (private): the NDJSON.gz of the
 * characters that slot resolved this fire. Exactly one worker owns any given
 * `w<NN>` object (single writer by construction — the same disjoint-by-slot
 * discipline the chunk model gave chunk files), overwritten in place as the
 * worker checkpoints, merged into the state file by finalize, then swept.
 */
export function workerResultPath(league: string, snapshotId: string, workerIndex: number): string {
  const n = String(workerIndex).padStart(2, '0');
  return `${workerResultPrefix(league, snapshotId)}w${n}.ndjson.gz`;
}

/**
 * Prefix under which a LEGACY snapshot's chunk files lived (v3 and earlier).
 * The chunk model is retired in v4 (state file + result files replace it); kept
 * only so retention / the legacy sweep and abort-discard can still list and reap
 * orphaned chunk keys from a pre-v4 snapshot (§6).
 */
export function chunkPrefix(league: string, snapshotId: string): string {
  return `${STATE_PREFIX}${encodeURIComponent(league)}/chunks/${snapshotId}/`;
}

export function chunkPath(league: string, snapshotId: string, chunkIndex: number): string {
  const n = String(chunkIndex).padStart(5, '0');
  return `${chunkPrefix(league, snapshotId)}${n}.json`;
}

/** Per-runner-slot limiter memory (private). Slot = 'coordinator' | 'w<i>'. */
export function workerStatePath(league: string, slot: string): string {
  return `${STATE_PREFIX}${encodeURIComponent(league)}/workers/${encodeURIComponent(slot)}.json`;
}

/** Prefix under which per-IP pacing state lives (for the finalize sweep). */
export function ipPacePrefix(league: string): string {
  return `${STATE_PREFIX}${encodeURIComponent(league)}/ips/`;
}

/**
 * Shared pacing spend keyed by runner IP (private): state/<league>/ips/<ip>.json.
 * Any slot landing on this IP paces against the same recent spend, closing the
 * cross-slot per-IP blind spot. Reaped by finalize once the spend ages out.
 */
export function ipPacePath(league: string, ip: string): string {
  return `${ipPacePrefix(league)}${encodeURIComponent(ip)}.json`;
}

/**
 * Passive-tree data cached in R2, pinned per league tree version (private).
 * Fetched once from the origin tree source, then reused by every transform for
 * that version (docs/ARCHITECTURE.md §6, Phase 3).
 */
export function treeCachePath(version: string): string {
  return `${TREE_PREFIX}${encodeURIComponent(version)}.json`;
}

/**
 * Prefix under which a LEGACY snapshot's raw shards lived (v3 and earlier).
 * Retired with the chunk model in v4 (the state file is the raw now); kept only
 * so retention / the legacy sweep can still list and reap orphaned raw/ keys
 * left behind by a pre-v4 snapshot (docs/PLAN_SNAPSHOT_STATE_REWORK.md §6).
 */
export function rawShardPrefix(league: string, snapshotId: string): string {
  return `${RAW_PREFIX}${encodeURIComponent(league)}/${snapshotId}/`;
}

export function snapshotPrefix(league: string, snapshotId: string): string {
  return `${SNAPSHOTS_PREFIX}${encodeURIComponent(league)}/${snapshotId}/`;
}

export function snapshotMetaPath(league: string, snapshotId: string): string {
  return `${snapshotPrefix(league, snapshotId)}meta.json`;
}

export function snapshotAggPath(league: string, snapshotId: string, name: string): string {
  return `${snapshotPrefix(league, snapshotId)}agg/${name}.json`;
}

export function snapshotDetailPath(league: string, snapshotId: string, table: string): string {
  return `${snapshotPrefix(league, snapshotId)}detail/${table}.parquet`;
}

/** Coarse category of an R2 key — the single source of truth for key layout. */
export type KeyCategory =
  | 'raw'
  | 'detail'
  | 'agg'
  | 'meta'
  | 'tree'
  | 'checkpoint'
  | 'roster'
  | 'snapshot-state'
  | 'worker-result'
  | 'chunk'
  | 'worker'
  | 'ip'
  | 'index'
  | 'economy'
  | 'other';

/** A snapshot's league + id, recovered from a key (league is URI-decoded). */
export interface SnapshotRef {
  league: string;
  snapshotId: string;
}

function decodeSeg(seg: string | undefined): string | undefined {
  if (seg === undefined) return undefined;
  try {
    return decodeURIComponent(seg);
  } catch {
    return seg;
  }
}

/** Parse `snapshots/<league>/<id>/detail/...` → {league, snapshotId}. */
export function parseDetailKey(key: string): SnapshotRef | undefined {
  const m = /^snapshots\/([^/]+)\/([^/]+)\/detail\//.exec(key);
  const league = decodeSeg(m?.[1]);
  return m && league !== undefined ? { league, snapshotId: m[2] as string } : undefined;
}

/** Parse `raw/<league>/<id>/...` → {league, snapshotId}. */
export function parseRawKey(key: string): SnapshotRef | undefined {
  const m = /^raw\/([^/]+)\/([^/]+)\//.exec(key);
  const league = decodeSeg(m?.[1]);
  return m && league !== undefined ? { league, snapshotId: m[2] as string } : undefined;
}

/** Parse `state/<league>/chunks/<id>/...` → {league, snapshotId}. */
export function parseChunkKey(key: string): SnapshotRef | undefined {
  const m = /^state\/([^/]+)\/chunks\/([^/]+)\//.exec(key);
  const league = decodeSeg(m?.[1]);
  return m && league !== undefined ? { league, snapshotId: m[2] as string } : undefined;
}

/** Parse `state/<league>/snapshots/<id>.ndjson.gz` → {league, snapshotId}. */
export function parseSnapshotStateKey(key: string): SnapshotRef | undefined {
  const m = /^state\/([^/]+)\/snapshots\/(.+)\.ndjson\.gz$/.exec(key);
  const league = decodeSeg(m?.[1]);
  return m && league !== undefined ? { league, snapshotId: m[2] as string } : undefined;
}

/** Parse `state/<league>/results/<id>/...` → {league, snapshotId}. */
export function parseWorkerResultKey(key: string): SnapshotRef | undefined {
  const m = /^state\/([^/]+)\/results\/([^/]+)\//.exec(key);
  const league = decodeSeg(m?.[1]);
  return m && league !== undefined ? { league, snapshotId: m[2] as string } : undefined;
}

export function classifyKey(key: string): KeyCategory {
  if (key === INDEX_PATH) return 'index';
  if (key.startsWith(ECONOMY_PREFIX)) return 'economy';
  if (key.startsWith(RAW_PREFIX)) return 'raw';
  if (key.startsWith(TREE_PREFIX)) return 'tree';
  if (/^state\/[^/]+\/roster\.json$/.test(key)) return 'roster';
  if (parseSnapshotStateKey(key)) return 'snapshot-state';
  if (parseWorkerResultKey(key)) return 'worker-result';
  if (parseChunkKey(key)) return 'chunk';
  if (/^state\/[^/]+\/workers\//.test(key)) return 'worker';
  if (/^state\/[^/]+\/ips\//.test(key)) return 'ip';
  if (key.startsWith(STATE_PREFIX)) return 'checkpoint';
  if (parseDetailKey(key)) return 'detail';
  if (/^snapshots\/[^/]+\/[^/]+\/agg\//.test(key)) return 'agg';
  if (/^snapshots\/[^/]+\/[^/]+\/meta\.json$/.test(key)) return 'meta';
  return 'other';
}
