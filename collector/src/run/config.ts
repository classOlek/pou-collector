/**
 * Run configuration shared by the coordinator, worker and finalize steps
 * (config/collector.json; see config-file.ts for parsing/env overrides).
 */
export interface RunConfig {
  league: string;
  /** Ladder depth read per capture (the roster then grows the queue past it). */
  depth: number;
  ladderPageSize: number;
  /** Wall-clock budget for one run/step; then checkpoint and exit cleanly. */
  maxRunMillis: number;
  /** Snapshot older than this (since ladder capture) aborts (hard block). */
  maxAgeHours: number;
  /** Retryable attempts before a character is declared dead. */
  maxAttempts: number;
  /** Characters per snapshot chunk (the unit of worker distribution). */
  chunkSize: number;
  /** Parallel worker jobs the workflow fans out per run. */
  workerCount: number;
  /**
   * Guard for SCHEDULED create-snapshot fires: skip closing/creating when the
   * previous snapshot began (or completed) less than this many hours ago —
   * double-fire / misconfigured-cron protection. The actual snapshot cadence is
   * the create workflow's cron; a dispatch fire bypasses this guard.
   */
  snapshotIntervalHours: number;
  /** Cooldown after an abort before a fresh snapshot is attempted. */
  abortCooldownHours: number;
}

export const HOUR_MS = 3_600_000;
