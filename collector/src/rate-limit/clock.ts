/**
 * Injected time. Every wait in the collector goes through a Clock so tests run
 * instantly and deterministically (no real timers), and the run's wall-clock
 * budget is measured against the same source.
 */
export interface Clock {
  /** Milliseconds since the Unix epoch. */
  now(): number;
  /** Resolve after `ms` of (virtual or real) time. */
  sleep(ms: number): Promise<void>;
}

/** Real wall-clock used in production (GitHub Actions). */
export const systemClock: Clock = {
  now: () => Date.now(),
  sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
};

/**
 * Deterministic clock for tests: `sleep` advances virtual time and resolves on
 * the microtask queue, so a multi-hour simulated collection completes in
 * milliseconds.
 */
export class FakeClock implements Clock {
  private current: number;

  constructor(startMs = 0) {
    this.current = startMs;
  }

  now(): number {
    return this.current;
  }

  async sleep(ms: number): Promise<void> {
    // Guard non-finite/negative waits so a limiter bug can't corrupt virtual time.
    if (Number.isFinite(ms) && ms > 0) {
      this.current += ms;
    }
    await Promise.resolve();
  }

  /** Manually advance virtual time (e.g. to age a snapshot past max_age). */
  advance(ms: number): void {
    this.current += ms;
  }
}
