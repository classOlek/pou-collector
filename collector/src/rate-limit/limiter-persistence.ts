/**
 * Splits a run's limiter memory across its two storage homes and back:
 *   - CLIENT state (penalty, streaks, observed rules) → the per-slot file
 *     (LimiterStateStore) — GGG enforces penalties per client, so they must not
 *     leak between slots and must survive an IP change.
 *   - PACE state (recentAcquires) → the shared per-IP file (PaceStateStore) —
 *     the windows mirror a per-IP server counter, so every slot on that IP
 *     shares one budget instead of each keeping a blind private copy.
 *
 * This is the fix for the cross-slot blind spot: IP X used by slot 0 in one
 * fire, then by slot 3 in the next, now paces against the same spend.
 *
 * Composition reuses the limiter's existing IP semantics rather than
 * duplicating them: the per-IP file (when present) is stamped as the current
 * IP's spend before restore, and `adoptIp` then does the right thing —
 *   • IP file present  → its spend is authoritative for this IP (kept);
 *   • no IP file, slot carried spend from a DIFFERENT IP → cleared (fresh IP);
 *   • no IP file, slot carried spend from the SAME IP → kept (pre-split resume);
 *   • IP undefined (discovery failed) → slot spend kept (conservative, today's
 *     behavior, never evading).
 */
import type { LimiterMemory } from '@classolek/shared';
import type { RateLimiter } from './limiter.js';
import { LimiterStateStore } from './limiter-store.js';
import { PaceStateStore } from './pace-store.js';
import type { ObjectStore } from '../checkpoint/object-store.js';

export interface LimiterScope {
  league: string;
  /** Per-slot key for the client-scoped state ('coordinator' | 'w<i>'). */
  slot: string;
  /** This runner's public IP; undefined when discovery failed. */
  ip: string | undefined;
}

function emptyMemory(): LimiterMemory {
  return {
    observedRules: [],
    penaltyUntil: 0,
    consecutiveThrottles: 0,
    consecutiveErrors: 0,
    recentAcquires: [],
  };
}

export class LimiterPersistence {
  private readonly slots: LimiterStateStore;
  private readonly pace: PaceStateStore;

  constructor(store: ObjectStore) {
    this.slots = new LimiterStateStore(store);
    this.pace = new PaceStateStore(store);
  }

  /**
   * Restore a limiter from its split state and adopt the current IP. Returns
   * whether adopting the IP cleared carried pace (so the caller can log it).
   */
  async loadInto(limiter: RateLimiter, scope: LimiterScope): Promise<boolean> {
    const client = await this.slots.load(scope.league, scope.slot);
    const ipPace =
      scope.ip !== undefined ? await this.pace.load(scope.league, scope.ip) : undefined;

    let memory = client;
    if (ipPace !== undefined && scope.ip !== undefined) {
      // The shared per-IP spend is authoritative for THIS ip: override whatever
      // the slot carried and stamp originIp = ip so adoptIp keeps it as current.
      memory = { ...(client ?? emptyMemory()), recentAcquires: ipPace, originIp: scope.ip };
    }
    if (memory) limiter.restore(memory);
    return limiter.adoptIp(scope.ip);
  }

  /** Persist client state to the slot file and pace to the shared IP file. */
  async save(limiter: RateLimiter, scope: LimiterScope, nowIso: string): Promise<void> {
    const memory = limiter.toMemory();
    if (scope.ip !== undefined) {
      await this.pace.save(scope.league, scope.ip, memory.recentAcquires, nowIso);
      // The IP file owns the pace now; blank it in the slot file so there is one
      // source of truth (originIp is kept for the discovery-failed fallback).
      await this.slots.save(scope.league, scope.slot, { ...memory, recentAcquires: [] }, nowIso);
      return;
    }
    // IP unknown: keep the pre-split behavior — pace rides in the slot file.
    await this.slots.save(scope.league, scope.slot, memory, nowIso);
  }
}
