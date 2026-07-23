/**
 * Worker completion detection (early-stop quorum).
 *
 * With N parallel workers a fire's wall clock is its SLOWEST worker: one
 * straggler with an unlucky assignment (a heavy run of characters, throttled IP)
 * can keep finalize — and, via the shared concurrency group, the next cron fire —
 * waiting for many idle-runner minutes after its siblings finished. The fix
 * leans on resumability (hard rule #3): a straggler that stops early simply
 * checkpoints, its assigned characters stay pending in the state file, and the
 * next fire's worker for the same slot resumes them (ownership is stable by
 * state-line ordinal) with fresh rate-limit windows.
 *
 * Mechanism, single-writer by construction:
 *  - a worker whose run ends — ANY clean stop: assignment drained, budget
 *    spent, rate-limit stall/abort — writes a tiny done marker under ITS OWN
 *    slot (state/<league>/workers/<slot>.done.json — the shared workerStatePath
 *    helper with a ".done"-suffixed slot, so the key stays inside the workers/
 *    prefix retention already classifies and never sweeps). Every clean stop
 *    counts because the marker tracks "this slot's JOB has exited this fire":
 *    a fleet that mostly stalled on rate limits must still release its
 *    stragglers, exactly like a fleet that mostly drained;
 *  - markers carry the workflow fire's run id (GITHUB_RUN_ID): a marker from a
 *    previous fire never counts, so stale markers need no cleanup — each slot
 *    just overwrites its own on the next drained run;
 *  - still-running workers re-count peer markers between character fetches
 *    (throttled to one sweep per QUORUM_CHECK_INTERVAL_MS) and stop with
 *    `quorum_stopped` once at least `earlyStopQuorum` workers are done.
 *
 * Disabled when earlyStopQuorum is 0 (the default) or no run id is available
 * (local runs outside GitHub Actions): no marker writes, no marker reads.
 */
import { workerStatePath } from '@classolek/shared';
import type { Clock } from '../rate-limit/clock.js';
import { getJson, putJson, type ObjectStore } from '../checkpoint/object-store.js';
import { workerSlot } from '../rate-limit/limiter-store.js';

/** One finished-run marker (collector-private, run-id-scoped). */
export interface WorkerDoneMarker {
  slot: string;
  runId: string;
  finishedAt: string;
  /** Why the run ended (WorkerStopReason; diagnostic only, not counted). */
  stopReason?: string;
}

/** Throttle between marker sweeps — a sweep is cheap (one small GET per peer),
 *  but there is no reason to repeat it per character fetch. */
export const QUORUM_CHECK_INTERVAL_MS = 60_000;

export function workerDonePath(league: string, workerIndex: number): string {
  return workerStatePath(league, `${workerSlot(workerIndex)}.done`);
}

export interface QuorumConfig {
  league: string;
  workerIndex: number;
  workerCount: number;
  /** The workflow fire id scoping markers ('' = unavailable → disabled). */
  runId: string;
  /** Finished workers that stop the rest early (0 = disabled). */
  earlyStopQuorum: number;
  /** Sweep throttle override (test seam); defaults to QUORUM_CHECK_INTERVAL_MS. */
  checkIntervalMillis?: number | undefined;
}

export interface QuorumDeps {
  clock: Clock;
  objectStore: ObjectStore;
  log?: ((message: string) => void) | undefined;
}

export class QuorumMonitor {
  private lastSweepAt: number | undefined;
  private reached = false;
  private readonly interval: number;

  constructor(
    private readonly config: QuorumConfig,
    private readonly deps: QuorumDeps,
  ) {
    this.interval = config.checkIntervalMillis ?? QUORUM_CHECK_INTERVAL_MS;
  }

  /** Early stop is live only with a positive quorum AND a fire id to scope by. */
  get enabled(): boolean {
    return this.config.earlyStopQuorum > 0 && this.config.runId !== '';
  }

  /** Record this worker's own finished run (no-op while disabled). */
  async markSelfDone(nowIso: string, stopReason: string): Promise<void> {
    if (!this.enabled) return;
    const marker: WorkerDoneMarker = {
      slot: workerSlot(this.config.workerIndex),
      runId: this.config.runId,
      finishedAt: nowIso,
      stopReason,
    };
    await putJson(
      this.deps.objectStore,
      workerDonePath(this.config.league, this.config.workerIndex),
      marker,
    );
  }

  /**
   * True once enough workers have drained this fire. Sweeps at most once per
   * interval; between sweeps it answers from the last result (a reached quorum
   * never un-reaches within a run — done markers are only ever added).
   */
  async shouldStop(): Promise<boolean> {
    if (!this.enabled || this.reached) return this.reached;
    const now = this.deps.clock.now();
    if (this.lastSweepAt !== undefined && now - this.lastSweepAt < this.interval) return false;
    this.lastSweepAt = now;
    const done = await this.countDonePeers();
    if (done >= this.config.earlyStopQuorum) {
      this.reached = true;
      this.deps.log?.(
        `quorum: ${done}/${this.config.workerCount} workers finished this fire ` +
          `(threshold ${this.config.earlyStopQuorum}) — stopping early; ` +
          `remaining characters resume next fire`,
      );
    }
    return this.reached;
  }

  /** Peers (never self — a checking worker is by definition not done) whose
   *  marker matches this fire's run id. Absent/stale/corrupt = not done. */
  private async countDonePeers(): Promise<number> {
    const peers: number[] = [];
    for (let i = 0; i < this.config.workerCount; i += 1) {
      if (i !== this.config.workerIndex) peers.push(i);
    }
    const done = await Promise.all(
      peers.map(async (index) => {
        try {
          const marker = await getJson<WorkerDoneMarker>(
            this.deps.objectStore,
            workerDonePath(this.config.league, index),
          );
          return marker?.runId === this.config.runId ? 1 : 0;
        } catch {
          return 0;
        }
      }),
    );
    return done.reduce((sum: number, n) => sum + n, 0);
  }
}
