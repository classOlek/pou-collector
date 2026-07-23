/**
 * Data contracts shared by the collector and the web app.
 * Any change here is a schema change: bump SCHEMA_VERSION and see
 * docs/ARCHITECTURE.md §6 (completed snapshots are immutable).
 */

// v2: the roster + chunked-worker redesign. Characters accumulate in a
// per-league roster; a snapshot seeds ALL known characters as pending chunks
// worked by parallel workers, and is published incrementally (complete: false)
// until every chunk resolves. A completed snapshot is immutable (hard rule #5);
// an incomplete one is republished in place on every finalize pass.
// v3: snapshot creation decoupled from collection. A create fire CLOSES the
// previous snapshot — uncollected characters get the terminal outcome
// `skipped` ("deliberately not collected", distinct from `dead` =
// unfetchable) and the snapshot publishes with what it has; meta gains
// `skippedCount` so readers can show collected vs skipped honestly.
// v4: single-file snapshot state. Introduces `SnapshotCharacter` — a snapshot's
// characters (queued identity + outcome + optional raw `characterData` /
// `passiveTree`) captured one line at a time in a streamed NDJSON.gz state file
// (`snapshotStatePath`), with transient per-worker result files
// (`workerResultPath`) replacing the chunk model as the unit of work. The chunk
// types and the manifest's chunk bookkeeping (chunkSize/chunkCount/
// resolvedChunks) are gone (docs/PLAN_SNAPSHOT_STATE_REWORK.md §6); the
// published snapshot formats (meta / agg / detail / index) are unchanged.
// v6: stable per-item key. Several items on one character share an `inventoryId`
// (all flasks = 'Flask', all jewels = 'PassiveJewels'), so keying `item_mods` on
// `inventoryId` pooled every flask/jewel's mods across the slot. Each item now
// gets a stable per-item `item_id` (`character_key || '#' || <array ordinal>`);
// `items` carries it and `item_mods.item_key` now holds that `item_id` (no longer
// the `inventoryId`), so a mod joins to the exact item it belongs to. `slot` stays
// on `items` (still the `inventoryId`) for display/grouping.
export const SCHEMA_VERSION = 6;

/**
 * Outcome of resolving one queued character. 'pending' = not computed yet;
 * 'skipped' = deliberately left uncollected when the snapshot was closed
 * (terminal — never retried, never counted as coverage).
 */
export type CharacterOutcome = 'pending' | 'ok' | 'private' | 'retryable' | 'dead' | 'skipped';

/** A count for every outcome (one production tally; see tallyOutcomes). */
export type OutcomeTally = Record<CharacterOutcome, number>;

export function emptyTally(): OutcomeTally {
  return { pending: 0, ok: 0, private: 0, retryable: 0, dead: 0, skipped: 0 };
}

/** The single production outcome tally over queued characters. */
export function tallyOutcomes(queue: readonly QueuedCharacter[]): OutcomeTally {
  const tally = emptyTally();
  for (const entry of queue) tally[entry.outcome] += 1;
  return tally;
}

/** Sum tallies (finalize rolls the merged-state tally up into the manifest). */
export function addTallies(into: OutcomeTally, tally: OutcomeTally): OutcomeTally {
  for (const key of Object.keys(into) as CharacterOutcome[]) into[key] += tally[key];
  return into;
}

/** Coverage (ok / private / dead) derived from one outcome tally. */
export function coverageOfTally(tally: OutcomeTally): Coverage {
  return { ok: tally.ok, private: tally.private, dead: tally.dead };
}

/** Coverage (ok / private / dead) of queued characters. */
export function coverageOf(queue: readonly QueuedCharacter[]): Coverage {
  return coverageOfTally(tallyOutcomes(queue));
}

/** Characters still awaiting a terminal outcome (pending + retryable). */
export function pendingOfTally(tally: OutcomeTally): number {
  return tally.pending + tally.retryable;
}

/** Snapshot lifecycle phases (docs/ARCHITECTURE.md §5). */
export type SnapshotPhase =
  'ladder_capture' | 'collecting' | 'transforming' | 'published' | 'aborted';

/** Phases where a snapshot is still being worked on (has live raw / pending work). */
export const IN_FLIGHT_PHASES: readonly SnapshotPhase[] = [
  'ladder_capture',
  'collecting',
  'transforming',
];

export function isInFlight(phase: SnapshotPhase): boolean {
  return IN_FLIGHT_PHASES.includes(phase);
}

/**
 * One character queued for collection inside a snapshot's state file.
 * `rank` is the last ladder rank the character was seen at — with the roster
 * model a snapshot can include characters that have since left the ladder.
 */
export interface QueuedCharacter {
  rank: number;
  account: string;
  character: string;
  class: string;
  level: number;
  outcome: CharacterOutcome;
  attempts: number;
  fetchedAt?: string;
}

/**
 * One line of a snapshot's single NDJSON.gz state file
 * (state/<league>/snapshots/<id>.ndjson.gz). It is a `QueuedCharacter` (the
 * queued identity + outcome/attempts fields the tally helpers operate on) that,
 * once resolved `ok`, also carries the raw GGG payloads inline: `characterData`
 * (the items response) and `passiveTree` (the passives response). Both are the
 * untyped raw JSON the transform ingests — the state file IS the raw now, so
 * there is no separate raw shard. They stay absent for every non-`ok` outcome
 * (pending / private / dead / retryable / skipped), keeping unresolved lines
 * tiny. Because it extends `QueuedCharacter`, `SnapshotCharacter[]` flows into
 * `tallyOutcomes` / `coverageOf` unchanged (the helpers read only `outcome`).
 *
 * The whole file is NEVER `JSON.parse`d as one document (15k+ characters × tens
 * of KB of raw JSON exceeds V8's string cap); every reader/writer streams it a
 * line at a time and only ever holds one `SnapshotCharacter` — with its raw
 * payloads — in memory at once (see docs/PLAN_SNAPSHOT_STATE_REWORK.md §5).
 */
export interface SnapshotCharacter extends QueuedCharacter {
  /** Raw items payload (present only once `outcome === 'ok'`). */
  characterData?: unknown;
  /** Raw passives payload (present only once `outcome === 'ok'`). */
  passiveTree?: unknown;
}

/**
 * One character in the per-league roster (state/<league>/roster.json).
 * The roster is append-only by identity (account + character): every ladder
 * capture upserts what it saw, so the pool grows past the 15k ladder window as
 * players enter and leave the ladder over a league's lifetime.
 */
export interface RosterCharacter {
  account: string;
  character: string;
  /** Last seen ladder class/level/rank (refreshed on every capture). */
  class: string;
  level: number;
  rank: number;
  firstSeenAt: string;
  lastSeenAt: string;
}

/** The per-league character database at state/<league>/roster.json (private). */
export interface RosterFile {
  schemaVersion: typeof SCHEMA_VERSION;
  league: string;
  updatedAt: string;
  characters: RosterCharacter[];
}

/** One "hits:period:penalty" tuple from an X-Rate-Limit-* header. */
export interface RateLimitTuple {
  hits: number;
  periodSec: number;
  penaltySec: number;
}

/** Observed limit + current usage for one named rule (e.g. "ip", "account"). */
export interface RateLimitRule {
  name: string;
  limits: RateLimitTuple[];
  state: RateLimitTuple[];
}

/**
 * Serialized rate-limiter memory (docs/ARCHITECTURE.md §7: "observed limits,
 * penalty until"). Only the durable adaptation survives across runs; the active
 * pacing windows are re-derived from `observedRules` on resume.
 */
export interface LimiterMemory {
  observedRules: RateLimitRule[];
  /** Epoch ms until which requests are held back; 0 when unrestricted. */
  penaltyUntil: number;
  /** Consecutive throttle/challenge signals (rate-block danger). */
  consecutiveThrottles: number;
  /** Consecutive 5xx/other-error signals (persistent-failure danger). */
  consecutiveErrors: number;
  /**
   * Epoch-ms timestamps of recent acquired requests, ascending. The sliding-
   * window limiter counts these per observed window; persisting them lets a
   * resumed run honor GGG's long windows (e.g. 180 requests / 2 h) across the
   * cron-run boundary instead of forgetting its recent spend each run.
   *
   * Scoped to `originIp`: the windows mirror a server-side per-IP counter, so
   * a resume on a different runner IP starts them empty (see RateLimiter
   * .adoptIp). `penaltyUntil` and the streaks are NOT scoped — a 429 /
   * Retry-After addresses the client (our User-Agent + contact), and honoring
   * it must survive the IP rotating between runs.
   */
  recentAcquires: number[];
  /**
   * Public IP the recentAcquires were recorded from, when discovery succeeded.
   * Absent on older checkpoints or when discovery failed — treated as "unknown,
   * keep the pace state" (conservative). Optional field on private state; no
   * schema bump (same precedent as recentAcquires).
   */
  originIp?: string;
}

/**
 * Persisted limiter memory for one runner slot, at
 * state/<league>/workers/<slot>.json (private). Each parallel worker (and the
 * coordinator) runs on its own runner/IP, so limiter memory is per slot, not
 * per snapshot: GGG's limits are enforced per client, and one slot's penalty
 * must not throttle (or be forgotten by) another.
 *
 * Client-scoped state (penalty, streaks, observed rules) lives here; the
 * IP-scoped pace spend (`recentAcquires`) is persisted separately, keyed by IP,
 * as an `IpPaceState` — so two slots that land on the same runner IP across
 * fires share one pacing budget instead of each keeping a private, blind copy.
 * The slot file still carries `recentAcquires` for the IP-discovery-failed
 * fallback (and older checkpoints predating the split), but the per-IP file is
 * authoritative when present.
 */
export interface WorkerState {
  schemaVersion: typeof SCHEMA_VERSION;
  slot: string;
  updatedAt: string;
  limiter: LimiterMemory;
}

/**
 * Shared pacing spend for one runner IP, at state/<league>/ips/<ip>.json
 * (private). The pace windows mirror GGG's per-IP request counters, so keying
 * this by IP (rather than by worker slot) lets every slot that runs on a given
 * IP — across fires, and across the coordinator/worker/create-snapshot steps —
 * pace against the same recent spend, closing the cross-slot blind spot where
 * IP X reused by a different slot would double-spend its window.
 *
 * Single-writer holds under the same reasoning as every other state object:
 * within a fire each matrix job is a distinct runner (distinct IP), and fires
 * are serialized by the shared concurrency group, so no two writers ever touch
 * one IP file at once. A stale file (every timestamp aged past the longest
 * window) restores to an empty window and is harmless; finalize sweeps expired
 * files so the set stays a small rolling window, not an unbounded pool.
 */
export interface IpPaceState {
  schemaVersion: typeof SCHEMA_VERSION;
  /** The runner IP this spend was recorded from (matches the object key). */
  ip: string;
  /** When this file was last written — drives finalize's stale-file sweep. */
  updatedAt: string;
  /** Epoch-ms timestamps of recent acquired requests, ascending (LimiterMemory.recentAcquires). */
  recentAcquires: number[];
}

/**
 * Collector checkpoint stored at state/<league>/current.json (private).
 * The manifest is written ONLY by the coordinator/create and finalize steps
 * (which the workflow serializes); workers write only their own worker-state and
 * transient result objects, so no object ever has two concurrent writers.
 *
 * The manifest is deliberately small (design decision 1): it carries the outcome
 * tally so coordinate's idle check stays request-free and byte-free — an idle
 * tick never downloads the multi-hundred-MB state file.
 */
export interface SnapshotManifest {
  schemaVersion: typeof SCHEMA_VERSION;
  snapshotId: string;
  league: string;
  /** Ladder depth read at capture (the roster can make the queue larger). */
  depth: number;
  phase: SnapshotPhase;
  ladderCapturedAt: string;
  /** Set when the last character resolves (gates the snapshot interval). */
  completedAt?: string;
  /** Set when the snapshot aborts (phase → aborted); gates the retry cooldown. */
  abortedAt?: string;
  /** Total characters seeded from the roster (state-file line count). */
  totalCharacters: number;
  /** Outcome rollup over the state file, refreshed by every finalize pass. */
  outcomes: OutcomeTally;
  /**
   * Failed FINAL transform attempts for this drained snapshot. After a
   * configured ceiling the snapshot aborts instead of retrying forever
   * (docs/ARCHITECTURE §5/§7). Incremental (incomplete) publish failures are
   * logged but never counted — collection continues regardless.
   */
  transformAttempts?: number;
}

/** Coverage tally: how the queued characters resolved (honest collection window). */
export interface Coverage {
  ok: number;
  private: number;
  dead: number;
}

/**
 * Published snapshot metadata at snapshots/<league>/<id>/meta.json.
 * While `complete` is false the file (like every other file of the snapshot) is
 * republished in place by each finalize pass; once complete it is immutable.
 */
export interface SnapshotMeta {
  schemaVersion: typeof SCHEMA_VERSION;
  snapshotId: string;
  league: string;
  depth: number;
  ladderCapturedAt: string;
  /** When this publish pass ran (advances on every incremental republish). */
  updatedAt: string;
  /** Set only on the final (complete) publish. */
  completedAt?: string;
  /** False while characters are still being collected (data is partial but live). */
  complete: boolean;
  /** Characters resolved so far (fetched outcomes only; skipped excluded). */
  coverage: Coverage;
  /** Characters not yet computed (pending + retryable); 0 when complete. */
  pendingCount: number;
  /**
   * Characters deliberately left uncollected when the snapshot was closed by a
   * create fire. coverage + pendingCount + skippedCount == totalCharacters.
   */
  skippedCount: number;
  /** Characters seeded into this snapshot from the roster. */
  totalCharacters: number;
  /** Row count of characters.parquet — must equal coverage.ok (validation gate). */
  characterCount: number;
  /**
   * Passive-tree version that resolved node names/stats, recorded so a
   * re-transform or audit can reproduce the passives table (finding: provenance).
   */
  treeVersion: string;
}

/**
 * Pre-aggregate files powering the default dashboards (no DuckDB-WASM needed).
 * Deliberately tiny (name + count + percentage rows) so they are cheap to fetch
 * and are the permanent "meta over time" record retention never trims.
 */
export type AggregateKind =
  'class_distribution' | 'skill_popularity' | 'unique_usage' | 'keystone_usage';

export const AGGREGATE_KINDS: readonly AggregateKind[] = [
  'class_distribution',
  'skill_popularity',
  'unique_usage',
  'keystone_usage',
];

export interface AggregateRow {
  name: string;
  count: number;
  /** Share of `total`, 0–100, rounded to 2 decimals. */
  percentage: number;
}

/**
 * One percentage helper shared by the collector transform and the web app so the
 * two never round differently. `count / total` as a 0–100 share, rounded to
 * `decimals` places; a zero total yields 0 (no division by zero).
 */
export function percentage(count: number, total: number, decimals = 2): number {
  if (total === 0) return 0;
  const factor = 10 ** decimals;
  return Math.round((count / total) * 100 * factor) / factor;
}

/** One aggregate file at snapshots/<league>/<id>/agg/<kind>.json (public). */
export interface AggregateFile {
  schemaVersion: typeof SCHEMA_VERSION;
  snapshotId: string;
  league: string;
  kind: AggregateKind;
  /** Denominator behind every row's percentage (character count). */
  total: number;
  /** Rows sorted by count descending, then name ascending (stable, testable). */
  rows: AggregateRow[];
}

/** One published snapshot as listed in the index (frontend entry point). */
export interface IndexSnapshot {
  /**
   * Schema version the snapshot's files were published under. The index
   * deliberately preserves snapshots across schema bumps (index-file.ts), so the
   * web app reads this to offer only snapshots it can render and to grey out the
   * rest instead of failing their dashboards forever (hard rule #5). Typed as a
   * plain number (not the current literal) so cross-version comparison is
   * meaningful after a bump.
   */
  schemaVersion: number;
  snapshotId: string;
  ladderCapturedAt: string;
  /** Last publish pass (advances while incomplete; freezes at completion). */
  updatedAt: string;
  /** Set only once the snapshot completed. */
  completedAt?: string;
  /** False while the snapshot is still being computed (browsable but partial). */
  complete: boolean;
  depth: number;
  /** Characters seeded into the snapshot (denominator for progress). */
  totalCharacters: number;
  coverage: Coverage;
  /**
   * Whether the detail Parquet is still available. True at publish; retention
   * sets it false when it trims detail so the web app offers aggregates only
   * (never a 404 on the explorer). Aggregates/meta are never trimmed.
   */
  hasDetail: boolean;
}

/** All published snapshots for one league, newest first. */
export interface IndexLeague {
  league: string;
  snapshots: IndexSnapshot[];
}

/** index.json at INDEX_PATH: the web app's entry point (public). */
export interface IndexFile {
  schemaVersion: typeof SCHEMA_VERSION;
  updatedAt: string;
  leagues: IndexLeague[];
}
