/**
 * Resolve one queued character (items + passives) against the character source,
 * applying the outcome policy (docs/ARCHITECTURE.md §5). Shared by the worker
 * step; kept free of queue/manifest concerns so it stays a pure per-character
 * unit: entry in → entry mutated + optional raw record out.
 */
import type { QueuedCharacter } from '@classolek/shared';
import type { Clock } from '../rate-limit/clock.js';
import type { RateLimiter } from '../rate-limit/limiter.js';
import type { CharacterCallResult, CharacterSource } from '../sources/types.js';

/** Progress reporter for limiter waits (see RateLimiter.acquire). */
export type WaitReporter = (ms: number, reason: 'penalty' | 'pace') => void;

export interface ResolveDeps {
  clock: Clock;
  characterSource: CharacterSource;
  limiter: RateLimiter;
  onWait?: WaitReporter;
  /**
   * Stall guards. Before each request the limiter's next slot is peeked; when
   * it lies at/past `deadlineMs` (epoch-ms, the run's budget end) or `maxWaitMs`
   * or more away, the resolve defers instead of sleeping — the entry stays
   * workable and the caller checkpoints for a later run. Without them a
   * saturated long window (90 req/30 min) paces a >20-minute sleep that idles
   * away the whole run budget.
   */
  deadlineMs?: number;
  maxWaitMs?: number;
}

/** Which stall guard cut a resolve short (see ResolveDeps). */
export type DeferReason = 'max_wait' | 'deadline';

export interface ResolveResult {
  /** GGG requests spent on this character (1 or 2). */
  requests: number;
  /** The raw NDJSON record when the character resolved `ok`. */
  record?: unknown;
  /** Set when a stall guard tripped: the entry is unchanged (still workable). */
  deferred?: DeferReason;
}

/** The tripped stall guard for the limiter's next slot, if any. */
function deferReason(deps: ResolveDeps): DeferReason | undefined {
  const at = deps.limiter.nextAcquireAt();
  if (deps.maxWaitMs !== undefined && at - deps.clock.now() >= deps.maxWaitMs) return 'max_wait';
  if (deps.deadlineMs !== undefined && at >= deps.deadlineMs) return 'deadline';
  return undefined;
}

/** Resolve one character, updating `entry` in place. */
export async function resolveCharacter(
  entry: QueuedCharacter,
  maxAttempts: number,
  deps: ResolveDeps,
): Promise<ResolveResult> {
  const query = { account: entry.account, character: entry.character };

  const stalled = deferReason(deps);
  if (stalled) return { requests: 0, deferred: stalled };
  await deps.limiter.acquire(deps.onWait);
  const items = await deps.characterSource.fetchItems(query);
  deps.limiter.observe(items.observation);

  if (items.result.kind !== 'ok') {
    applyNonOk(entry, items.result, maxAttempts);
    return { requests: 1 };
  }
  const itemsData = items.result.data;

  // Items proved the profile public. If the limiter just aborted or a stall
  // guard tripped, leave the entry pending (don't half-resolve it) — the next
  // run retries from the top.
  if (deps.limiter.isAborted) return { requests: 1 };
  const stalledMid = deferReason(deps);
  if (stalledMid) return { requests: 1, deferred: stalledMid };

  await deps.limiter.acquire(deps.onWait);
  const passives = await deps.characterSource.fetchPassives(query);
  deps.limiter.observe(passives.observation);

  if (passives.result.kind !== 'ok') {
    applyNonOk(entry, passives.result, maxAttempts);
    return { requests: 2 };
  }

  entry.outcome = 'ok';
  entry.attempts += 1;
  entry.fetchedAt = new Date(deps.clock.now()).toISOString();
  return {
    requests: 2,
    record: {
      rank: entry.rank,
      account: entry.account,
      character: entry.character,
      class: entry.class,
      level: entry.level,
      fetchedAt: entry.fetchedAt,
      items: itemsData,
      passives: passives.result.data,
    },
  };
}

/** Apply a non-ok sub-call result to the entry (kind → outcome policy). */
function applyNonOk(
  entry: QueuedCharacter,
  result: CharacterCallResult,
  maxAttempts: number,
): void {
  switch (result.kind) {
    case 'private':
      entry.attempts += 1;
      entry.outcome = 'private';
      return;
    case 'dead':
      entry.attempts += 1;
      entry.outcome = 'dead';
      return;
    case 'retryable':
      // Bounded retries across runs, then terminal `dead`.
      entry.attempts += 1;
      entry.outcome = entry.attempts >= maxAttempts ? 'dead' : 'retryable';
      return;
    case 'rate_limited':
      // Transient: never burns an attempt (ARCHITECTURE §5) — leave the entry
      // pending; the limiter's backoff/abort paces and protects the run.
      return;
    case 'ok':
      return;
  }
}
