import { describe, expect, it } from 'vitest';
import type { RateObservation, RateSignal } from '../sources/types.js';
import { FakeClock } from './clock.js';
import { DEFAULT_LIMITER_CONFIG, RateLimiter } from './limiter.js';

const obs = (signal: RateSignal, headers: Record<string, string> = {}): RateObservation => ({
  status: signal === 'throttled' ? 429 : 200,
  headers,
  signal,
});

/** Largest number of timestamps (ascending) falling within any `periodMs` span. */
function maxInAnyWindow(times: number[], periodMs: number): number {
  let max = 0;
  for (let i = 0; i < times.length; i += 1) {
    const end = times[i];
    if (end === undefined) continue;
    let count = 0;
    for (let j = i; j >= 0; j -= 1) {
      const t = times[j];
      if (t === undefined || end - t >= periodMs) break;
      count += 1;
    }
    max = Math.max(max, count);
  }
  return max;
}

const CHAR_HEADERS = {
  'x-rate-limit-rules': 'Ip',
  // The real GGG character-endpoint budget: 30/60s, 90/30m, 180/2h.
  'x-rate-limit-ip': '30:60:120,90:1800:600,180:7200:3600',
};

describe('RateLimiter pacing', () => {
  it('bursts up to the seed cap, then paces within the seed window', async () => {
    const clock = new FakeClock(0);
    const limiter = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG); // seed: 4 requests / 8s
    for (let i = 0; i < 4; i += 1) await limiter.acquire();
    expect(clock.now()).toBe(0); // burst is free
    await limiter.acquire();
    expect(clock.now()).toBe(8000); // 5th waits for the oldest of the 4 to leave the 8s window
  });

  it('adapts pacing to observed headers — bursts up to the observed cap', async () => {
    const clock = new FakeClock(0);
    const limiter = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG);
    limiter.observe(obs('ok', { 'x-rate-limit-rules': 'Ip', 'x-rate-limit-ip': '60:60:60' }));
    for (let i = 0; i < 54; i += 1) await limiter.acquire(); // cap = floor(60*0.9)
    expect(clock.now()).toBe(0); // all 54 burst free (the seed only allowed 4)
    await limiter.acquire();
    expect(clock.now()).toBe(60_000); // 55th waits for the oldest to leave the 60s window
  });

  it('bursts a short run under the layered windows instead of pacing at the 2h quota', async () => {
    // Regression for the collapse bug: deriveBudget flattened the 180/2h quota
    // into ~1 request/44s, so a 20-character run (40 requests) took ~15 min.
    // The per-window pacer lets it burst under the 30/60s window instead.
    const clock = new FakeClock(0);
    const limiter = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG);
    for (let i = 0; i < 40; i += 1) {
      limiter.observe(obs('ok', CHAR_HEADERS));
      await limiter.acquire();
    }
    // 27 fire at t=0, the other 13 once the 60s window frees — done in 60s,
    // not the ~40 * 44s ≈ 29 min the old single-rate budget would have taken.
    expect(clock.now()).toBe(60_000);
  });

  it('never exceeds any observed window over a long burst', async () => {
    const clock = new FakeClock(0);
    const limiter = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG);
    const times: number[] = [];
    for (let i = 0; i < 400; i += 1) {
      limiter.observe(obs('ok', CHAR_HEADERS));
      await limiter.acquire();
      times.push(clock.now());
    }
    expect(times).toHaveLength(400); // still makes progress (honors the quota, doesn't stall out)
    expect(maxInAnyWindow(times, 60_000)).toBeLessThanOrEqual(27); // floor(30*0.9)
    expect(maxInAnyWindow(times, 1_800_000)).toBeLessThanOrEqual(81); // floor(90*0.9)
    expect(maxInAnyWindow(times, 7_200_000)).toBeLessThanOrEqual(162); // floor(180*0.9)
  });
});

describe('RateLimiter nextAcquireAt', () => {
  it('peeks the next slot without sleeping or recording', async () => {
    const clock = new FakeClock(0);
    const limiter = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG); // seed: 4 requests / 8s
    expect(limiter.nextAcquireAt()).toBe(0); // a free slot reads as "now"
    for (let i = 0; i < 4; i += 1) await limiter.acquire();
    expect(limiter.nextAcquireAt()).toBe(8000); // window full → oldest ages out at 8s
    expect(limiter.nextAcquireAt()).toBe(8000); // idempotent — the peek recorded nothing
    expect(clock.now()).toBe(0); // and never slept
  });

  it('includes an active penalty window', () => {
    const clock = new FakeClock(0);
    const limiter = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG);
    limiter.observe(obs('throttled', { 'retry-after': '30' }));
    expect(limiter.nextAcquireAt()).toBe(30_000);
  });
});

describe('RateLimiter throttle handling', () => {
  it('honors Retry-After (delta-seconds) on a throttle', async () => {
    const clock = new FakeClock(0);
    const limiter = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG);
    limiter.observe(obs('throttled', { 'retry-after': '30' }));
    const before = clock.now();
    await limiter.acquire();
    expect(clock.now() - before).toBe(30_000);
  });

  it('escalates backoff on consecutive throttles', async () => {
    const clock = new FakeClock(0);
    const limiter = new RateLimiter(clock, {
      ...DEFAULT_LIMITER_CONFIG,
      throttleAbortThreshold: 99,
    });
    limiter.observe(obs('throttled'));
    let before = clock.now();
    await limiter.acquire();
    expect(clock.now() - before).toBe(5_000); // base * 2^0
    limiter.observe(obs('throttled'));
    before = clock.now();
    await limiter.acquire();
    expect(clock.now() - before).toBe(10_000); // base * 2^1
  });

  it('aborts after the throttle threshold, and challenges count the same', () => {
    const clock = new FakeClock(0);
    const throttle = new RateLimiter(clock, {
      ...DEFAULT_LIMITER_CONFIG,
      throttleAbortThreshold: 3,
    });
    throttle.observe(obs('throttled'));
    throttle.observe(obs('throttled'));
    expect(throttle.isAborted).toBe(false);
    throttle.observe(obs('throttled'));
    expect(throttle.isAborted).toBe(true);

    const challenge = new RateLimiter(clock, {
      ...DEFAULT_LIMITER_CONFIG,
      throttleAbortThreshold: 2,
    });
    challenge.observe(obs('challenge'));
    challenge.observe(obs('challenge'));
    expect(challenge.isAborted).toBe(true);
  });
});

describe('RateLimiter error handling', () => {
  it('uses a gentler backoff than throttling', async () => {
    const clock = new FakeClock(0);
    const limiter = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG); // errorBackoff 2s
    limiter.observe(obs('error'));
    const before = clock.now();
    await limiter.acquire();
    expect(clock.now() - before).toBe(2_000); // gentler than the 5s throttle base
  });

  it('tolerates a burst of transient errors below the (higher) threshold', () => {
    const clock = new FakeClock(0);
    const limiter = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG); // threshold 8
    for (let i = 0; i < 7; i += 1) limiter.observe(obs('error'));
    expect(limiter.isAborted).toBe(false);
    limiter.observe(obs('error'));
    expect(limiter.isAborted).toBe(true);
  });

  it('keeps throttle and error streaks independent; a clean response resets both', () => {
    const clock = new FakeClock(0);
    const limiter = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG);
    limiter.observe(obs('throttled'));
    limiter.observe(obs('error')); // resets the throttle streak, starts the error streak
    expect(limiter.toMemory().consecutiveThrottles).toBe(0);
    expect(limiter.toMemory().consecutiveErrors).toBe(1);
    limiter.observe(obs('ok'));
    expect(limiter.toMemory().consecutiveThrottles).toBe(0);
    expect(limiter.toMemory().consecutiveErrors).toBe(0);
  });
});

describe('RateLimiter persistence and resume', () => {
  it('round-trips its durable memory', () => {
    const clock = new FakeClock(1_000);
    const limiter = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG);
    limiter.observe(
      obs('throttled', { 'x-rate-limit-rules': 'Ip', 'x-rate-limit-ip': '60:60:60' }),
    );
    const memory = limiter.toMemory();
    const resumed = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG, memory);
    expect(resumed.toMemory()).toEqual(memory);
  });

  it('resumes a pre-recentAcquires checkpoint without crashing (back-compat)', async () => {
    const clock = new FakeClock(0);
    // A checkpoint written by the old token-bucket limiter: ratePerSec/burst,
    // no recentAcquires. It passes manifest validation (no schema bump), so the
    // limiter must tolerate the missing field instead of spreading `undefined`.
    const legacy = {
      observedRules: [],
      penaltyUntil: 0,
      consecutiveThrottles: 0,
      consecutiveErrors: 0,
      ratePerSec: 0.5,
      burst: 4,
    } as unknown as import('@pou/shared').LimiterMemory;
    const limiter = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG, legacy);
    expect(limiter.toMemory().recentAcquires).toEqual([]);
    await expect(limiter.acquire()).resolves.toBeUndefined(); // no throw, paces from seed
  });

  it('continues serving the penalty window after a resume', async () => {
    const clock = new FakeClock(1_000);
    const first = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG);
    first.observe(obs('throttled', { 'retry-after': '30' })); // penaltyUntil = 31_000
    const memory = first.toMemory();

    const resumed = new RateLimiter(clock, DEFAULT_LIMITER_CONFIG);
    resumed.restore(memory);
    const before = clock.now();
    await resumed.acquire();
    expect(clock.now() - before).toBe(30_000); // the penalty persisted across the resume
  });

  it('clears the per-run abort breaker on resume but re-aborts if trouble persists', () => {
    const clock = new FakeClock(0);
    const cfg = { ...DEFAULT_LIMITER_CONFIG, throttleAbortThreshold: 3 };
    const first = new RateLimiter(clock, cfg);
    first.observe(obs('throttled'));
    first.observe(obs('throttled'));
    first.observe(obs('throttled'));
    expect(first.isAborted).toBe(true);

    const resumed = new RateLimiter(clock, cfg);
    resumed.restore(first.toMemory());
    expect(resumed.isAborted).toBe(false); // fresh breaker — gets a chance after the penalty
    resumed.observe(obs('throttled'));
    expect(resumed.isAborted).toBe(true); // still throttled → aborts again promptly
  });
});
