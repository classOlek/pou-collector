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

/** Prefix under which a snapshot's chunk files live (private). */
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

/** Prefix under which a snapshot's raw shards live (for listing/cleanup). */
export function rawShardPrefix(league: string, snapshotId: string): string {
  return `${RAW_PREFIX}${encodeURIComponent(league)}/${snapshotId}/`;
}

/**
 * One raw shard for one chunk visit. A worker writes at most one shard per
 * chunk per run (`seq` = the chunk's shardsWritten cursor), so shard keys never
 * collide across parallel workers — each chunk has exactly one owner at a time.
 */
export function rawChunkShardPath(
  league: string,
  snapshotId: string,
  chunkIndex: number,
  seq: number,
): string {
  const c = String(chunkIndex).padStart(5, '0');
  return `${rawShardPrefix(league, snapshotId)}chunk-${c}-${String(seq).padStart(3, '0')}.ndjson.gz`;
}

/** Prefix of every raw shard belonging to one chunk (orphan cleanup). */
export function rawChunkShardPrefix(
  league: string,
  snapshotId: string,
  chunkIndex: number,
): string {
  return `${rawShardPrefix(league, snapshotId)}chunk-${String(chunkIndex).padStart(5, '0')}-`;
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

export function classifyKey(key: string): KeyCategory {
  if (key === INDEX_PATH) return 'index';
  if (key.startsWith(ECONOMY_PREFIX)) return 'economy';
  if (key.startsWith(RAW_PREFIX)) return 'raw';
  if (key.startsWith(TREE_PREFIX)) return 'tree';
  if (/^state\/[^/]+\/roster\.json$/.test(key)) return 'roster';
  if (parseChunkKey(key)) return 'chunk';
  if (/^state\/[^/]+\/workers\//.test(key)) return 'worker';
  if (/^state\/[^/]+\/ips\//.test(key)) return 'ip';
  if (key.startsWith(STATE_PREFIX)) return 'checkpoint';
  if (parseDetailKey(key)) return 'detail';
  if (/^snapshots\/[^/]+\/[^/]+\/agg\//.test(key)) return 'agg';
  if (/^snapshots\/[^/]+\/[^/]+\/meta\.json$/.test(key)) return 'meta';
  return 'other';
}
