/**
 * Header-aware rate limiter (docs/ARCHITECTURE.md §4.1, hard rule #1).
 *
 * A sliding-window pacer seeded *conservatively* paces outbound requests and
 * adapts to the observed `X-Rate-Limit-*` budget. GGG advertises several layered
 * windows per rule (e.g. 30 requests/60s, 90/30m, 180/2h) and enforces each
 * independently, so we model each as its own window — a request waits until it
 * fits under *every* window's cap. This lets a run burst up to the short
 * window's cap and only throttle as it approaches a longer horizon, instead of
 * flattening a 2-hour quota into a single ~1-request-per-44s rate (which
 * throttled even a 20-character run to ~15 minutes).
 *
 * It also tracks two independent danger streaks with different tolerances:
 *   - throttle/challenge (429 or Cloudflare interstitial): hard signal, low
 *     abort threshold, aggressive escalating backoff;
 *   - error (5xx / unexpected non-JSON): transient, high abort threshold,
 *     gentle escalating backoff — blips must not abort, streams must.
 * Any clean response resets both streaks. Crossing either threshold raises the
 * abort flag so the run checkpoints and stops.
 *
 * Durable state (observed rules, penalty-until, both streaks, recent request
 * timestamps) serializes to the checkpoint. `restore` rehydrates it on resume
 * and clears the per-run breaker while keeping the penalty window, the adapted
 * windows, and the recent spend — so the next cron run serves the penalty and
 * honors the long windows across the run boundary rather than forgetting them.
 */
import type { LimiterMemory, RateLimitRule } from '@pou/shared';
import type { Clock } from './clock.js';
import {
  activeRestrictionSec,
  deriveWindows,
  parseRateLimitRules,
  parseRetryAfterMs,
  type PaceWindow,
} from './headers.js';
import type { RateObservation } from '../sources/types.js';

export interface LimiterConfig {
  seedRatePerSec: number;
  seedBurst: number;
  headroom: number;
  /** Consecutive throttle/challenge signals that trip the abort flag. */
  throttleAbortThreshold: number;
  /** Consecutive error signals that trip the abort flag (higher — blip-tolerant). */
  errorAbortThreshold: number;
  /** Base backoff for throttle/challenge; doubles per consecutive hit. */
  throttleBackoffMs: number;
  /** Gentler base backoff for errors; doubles per consecutive hit. */
  errorBackoffMs: number;
  maxBackoffMs: number;
}

export const DEFAULT_LIMITER_CONFIG: LimiterConfig = {
  // ~30 req/min seed (docs/ARCHITECTURE.md §5 conservative table) until the
  // observed headers say we may go faster.
  seedRatePerSec: 0.5,
  seedBurst: 4,
  headroom: 0.9,
  throttleAbortThreshold: 3,
  errorAbortThreshold: 8,
  throttleBackoffMs: 5_000,
  errorBackoffMs: 2_000,
  maxBackoffMs: 5 * 60_000,
};

/**
 * The seed pacing window, used before any header is observed: `seedBurst`
 * requests may burst, then one per `1/seedRatePerSec`s (i.e. cap over a span of
 * `seedBurst / seedRatePerSec` seconds). Headroom is already baked into the
 * seed, so it is applied at full cap.
 */
function seedWindows(config: LimiterConfig): PaceWindow[] {
  return [{ cap: config.seedBurst, periodMs: (config.seedBurst / config.seedRatePerSec) * 1000 }];
}

export class RateLimiter {
  private windows: PaceWindow[];
  /** Ascending epoch-ms of recent acquired requests, trimmed to the longest window. */
  private recent: number[] = [];
  private penaltyUntil = 0;
  private consecutiveThrottles = 0;
  private consecutiveErrors = 0;
  private observedRules: RateLimitRule[] = [];
  private aborted = false;

  constructor(
    private readonly clock: Clock,
    private readonly config: LimiterConfig = DEFAULT_LIMITER_CONFIG,
    memory?: LimiterMemory,
  ) {
    this.windows = seedWindows(config);
    if (memory) this.restore(memory);
  }

  get isAborted(): boolean {
    return this.aborted;
  }

  /** Longest window horizon — recent timestamps older than this can be dropped. */
  private get maxPeriodMs(): number {
    return this.windows.reduce((m, w) => Math.max(m, w.periodMs), 0);
  }

  /**
   * Rehydrate durable state from a checkpoint. Keeps the penalty window, the
   * adapted pacing windows (re-derived from the observed rules), and the recent
   * request spend — so we neither hammer at seed rate nor forget the long
   * windows on resume — but clears the per-run abort breaker: the next run
   * serves the penalty, makes an attempt, and re-aborts only if trouble persists.
   */
  restore(memory: LimiterMemory): void {
    this.observedRules = memory.observedRules;
    this.penaltyUntil = memory.penaltyUntil;
    this.consecutiveThrottles = memory.consecutiveThrottles;
    this.consecutiveErrors = memory.consecutiveErrors;
    const derived = deriveWindows(memory.observedRules, this.config.headroom);
    this.windows = derived.length > 0 ? derived : seedWindows(this.config);
    // Tolerate a checkpoint written before recentAcquires existed (the field was
    // added with the sliding-window pacer, no schema bump): resume with empty
    // spend rather than crashing on `[...undefined]`. The first response re-seeds
    // the windows, so at worst we briefly under-count a long window on resume.
    this.recent = Array.isArray(memory.recentAcquires) ? [...memory.recentAcquires] : [];
    this.aborted = false;
  }

  /**
   * Block until this request fits under every pacing window (and any penalty
   * window has elapsed). `onWait` is invoked before each sleep with its duration
   * and cause, so the caller can surface "waiting Ns for rate limit" progress;
   * it never changes timing. `penalty` = serving an observed rate-limit penalty
   * window; `pace` = a pacing window is full and we wait for the oldest hit in
   * it to age out.
   */
  async acquire(onWait?: (ms: number, reason: 'penalty' | 'pace') => void): Promise<void> {
    const wait = this.penaltyUntil - this.clock.now();
    if (wait > 0) {
      onWait?.(wait, 'penalty');
      await this.clock.sleep(wait);
    }

    const paceWait = this.earliestAcquire() - this.clock.now();
    if (paceWait > 0) {
      onWait?.(paceWait, 'pace');
      await this.clock.sleep(paceWait);
    }
    this.record(this.clock.now());
  }

  /**
   * Epoch-ms when the next `acquire` could issue its request: the later of the
   * penalty window and every pacing window. A pure peek — no waiting, nothing
   * recorded — so a caller can see a long stall coming (a saturated 30-min
   * window can pace >20 min ahead) and checkpoint instead of idling through it.
   */
  nextAcquireAt(): number {
    return Math.max(this.penaltyUntil, this.earliestAcquire());
  }

  /**
   * Earliest time this request may go out without pushing any window over its
   * cap. For a window of `cap` over `periodMs`, if `cap` hits already sit inside
   * the horizon, the request must wait until the cap-th most recent of them ages
   * out (`recent[len - cap] + periodMs`). The binding window is the latest such.
   */
  private earliestAcquire(): number {
    let earliest = this.clock.now();
    for (const w of this.windows) {
      const kth = this.recent[this.recent.length - w.cap];
      if (kth !== undefined) earliest = Math.max(earliest, kth + w.periodMs);
    }
    return earliest;
  }

  /** Record an issued request and drop timestamps aged out of the longest window. */
  private record(at: number): void {
    this.recent.push(at);
    const cutoff = at - this.maxPeriodMs;
    const oldest = this.recent[0];
    if (oldest !== undefined && oldest <= cutoff) {
      this.recent = this.recent.filter((t) => t > cutoff);
    }
  }

  /** Feed a response's status/headers/signal back in to adapt pacing. */
  observe(obs: RateObservation): void {
    const rules = parseRateLimitRules(obs.headers);
    if (rules.length > 0) {
      this.observedRules = rules;
      const windows = deriveWindows(rules, this.config.headroom);
      if (windows.length > 0) this.windows = windows;
      const restrictionSec = activeRestrictionSec(rules);
      if (restrictionSec > 0) this.applyPenalty(restrictionSec * 1000);
    }

    switch (obs.signal) {
      case 'ok':
        this.consecutiveThrottles = 0;
        this.consecutiveErrors = 0;
        return;
      case 'throttled':
      case 'challenge': {
        this.consecutiveErrors = 0;
        this.consecutiveThrottles += 1;
        const escalated = this.config.throttleBackoffMs * 2 ** (this.consecutiveThrottles - 1);
        const backoff = parseRetryAfterMs(obs.headers, this.clock.now()) ?? escalated;
        this.applyPenalty(Math.min(this.config.maxBackoffMs, backoff));
        if (this.consecutiveThrottles >= this.config.throttleAbortThreshold) this.aborted = true;
        return;
      }
      case 'error': {
        this.consecutiveThrottles = 0;
        this.consecutiveErrors += 1;
        const escalated = this.config.errorBackoffMs * 2 ** (this.consecutiveErrors - 1);
        const backoff = parseRetryAfterMs(obs.headers, this.clock.now()) ?? escalated;
        this.applyPenalty(Math.min(this.config.maxBackoffMs, backoff));
        if (this.consecutiveErrors >= this.config.errorAbortThreshold) this.aborted = true;
        return;
      }
    }
  }

  toMemory(): LimiterMemory {
    return {
      observedRules: this.observedRules,
      penaltyUntil: this.penaltyUntil,
      consecutiveThrottles: this.consecutiveThrottles,
      consecutiveErrors: this.consecutiveErrors,
      recentAcquires: [...this.recent],
    };
  }

  private applyPenalty(ms: number): void {
    const until = this.clock.now() + ms;
    if (until > this.penaltyUntil) this.penaltyUntil = until;
  }
}
