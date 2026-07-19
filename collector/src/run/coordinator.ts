/**
 * Coordinate step of the COLLECT workflow: a cheap, request-free check of the
 * newest snapshot. If it is `collecting` with characters still awaiting
 * computation — and no politeness gate is closed — report the worker matrix so
 * the workflow fans workers out; otherwise the whole fire no-ops.
 *
 * Two politeness gates guard the fan-out (both read-only, from the per-slot
 * limiter states):
 *
 *   penalty  — some slot is still serving a client-scoped penalty (429 /
 *              Retry-After). Workers would spawn only to stall, and hammering
 *              through a penalty on fresh IPs is exactly the evasion hard rule
 *              #1 forbids. Skipped while the penalty reaches further ahead
 *              than a worker would sleep (maxWaitMillis).
 *   cooldown — the previous work wave ended less than collectCooldownMinutes
 *              ago. With pace state scoped per runner IP (limiter.adoptIp),
 *              each wave gets a fresh per-IP budget, so wave cadence — not the
 *              limiter — bounds the fleet's aggregate load on GGG. This gate
 *              makes that bound explicit and keeps back-to-back dispatches
 *              from multiplying it. 0 disables.
 *
 * Creating and closing snapshots — ladder capture, roster merge, chunk
 * seeding, marking leftovers skipped — lives in the create-snapshot workflow
 * (run/create-snapshot.ts). This step never talks to GGG and never writes
 * anything; the rollup in the manifest is finalize's last word on what is
 * still pending.
 */
import type { SnapshotManifest, SnapshotPhase } from '@pou/shared';
import { pendingOfTally } from '@pou/shared';
import type { CheckpointStore } from '../checkpoint/store.js';
import type { ObjectStore } from '../checkpoint/object-store.js';
import type { Clock } from '../rate-limit/clock.js';
import { COORDINATOR_SLOT, LimiterStateStore, workerSlot } from '../rate-limit/limiter-store.js';

export interface CoordinatorDeps {
  checkpointStore: CheckpointStore;
  objectStore: ObjectStore;
  clock: Clock;
  log?: (message: string) => void;
}

export interface CoordinatorConfig {
  league: string;
  workerCount: number;
  /** A penalty ending within this horizon is worth spawning workers for —
   *  they'd sleep it out; further away, the wave is skipped. */
  maxWaitMillis: number;
  /** Minimum gap between the end of one work wave and the start of the next
   *  (the fleet's aggregate-politeness dial); 0 = disabled. */
  collectCooldownMillis: number;
}

export type CoordinatorStopReason =
  | 'idle'
  | 'work_pending'
  /** A slot's client-scoped penalty (429/Retry-After) reaches further ahead
   *  than a worker would sleep — spawning the matrix would only stall. */
  | 'penalty_active'
  /** The previous work wave ended less than collectCooldownMillis ago. */
  | 'cooldown';

export interface CoordinatorSummary {
  phase: SnapshotPhase | 'none';
  stopReason: CoordinatorStopReason;
  /** Whether the workflow should fan worker jobs out this run. */
  hasWork: boolean;
  /** Worker slot indices for the workflow matrix ([] when hasWork is false). */
  workers: number[];
  totalCharacters: number;
  chunkCount: number;
  /** Characters still awaiting computation per the manifest rollup. */
  pendingCount: number;
  /** Epoch ms when a closed gate reopens (penalty end / cooldown end); only
   *  present for the penalty_active / cooldown stop reasons. */
  blockedUntil?: number;
}

interface ClosedGate {
  reason: 'penalty_active' | 'cooldown';
  until: number;
}

export class Coordinator {
  private readonly limiterStates: LimiterStateStore;

  constructor(
    private readonly config: CoordinatorConfig,
    private readonly deps: CoordinatorDeps,
  ) {
    this.limiterStates = new LimiterStateStore(deps.objectStore);
  }

  async runOnce(): Promise<CoordinatorSummary> {
    const manifest = await this.deps.checkpointStore.load(this.config.league);
    if (!manifest) {
      return this.summarize(undefined);
    }
    let summary = this.summarize(manifest);
    if (summary.hasWork) {
      const gate = await this.closedGate();
      if (gate) {
        summary = {
          ...summary,
          hasWork: false,
          workers: [],
          stopReason: gate.reason,
          blockedUntil: gate.until,
        };
      }
    }
    this.deps.log?.(
      `coordinate: ${this.config.league} phase=${summary.phase} ` +
        `pending=${summary.pendingCount} hasWork=${summary.hasWork}` +
        (summary.blockedUntil !== undefined
          ? ` (${summary.stopReason} until ${new Date(summary.blockedUntil).toISOString()})`
          : ''),
    );
    return summary;
  }

  /**
   * The politeness gate currently blocking a work wave, if any. Reads every
   * slot's persisted limiter state: penalties are client-scoped, so the
   * coordinator slot's (ladder capture) counts too; the cooldown anchors on
   * the newest WORKER save — the moment the last wave checkpointed.
   */
  private async closedGate(): Promise<ClosedGate | undefined> {
    const now = this.deps.clock.now();
    const workerStates = await Promise.all(
      Array.from({ length: this.config.workerCount }, (_, i) =>
        this.limiterStates.loadState(this.config.league, workerSlot(i)),
      ),
    );
    const coordinatorState = await this.limiterStates.loadState(
      this.config.league,
      COORDINATOR_SLOT,
    );

    let penaltyUntil = 0;
    for (const state of [...workerStates, coordinatorState]) {
      penaltyUntil = Math.max(penaltyUntil, state?.limiter.penaltyUntil ?? 0);
    }
    if (penaltyUntil - now > this.config.maxWaitMillis) {
      return { reason: 'penalty_active', until: penaltyUntil };
    }

    if (this.config.collectCooldownMillis > 0) {
      let lastWaveAt = 0;
      for (const state of workerStates) {
        const at = state ? Date.parse(state.updatedAt) : Number.NaN;
        if (Number.isFinite(at)) lastWaveAt = Math.max(lastWaveAt, at);
      }
      const until = lastWaveAt + this.config.collectCooldownMillis;
      if (lastWaveAt > 0 && until > now) {
        return { reason: 'cooldown', until };
      }
    }
    return undefined;
  }

  private summarize(manifest: SnapshotManifest | undefined): CoordinatorSummary {
    const pendingCount = manifest?.phase === 'collecting' ? pendingOfTally(manifest.outcomes) : 0;
    const hasWork = pendingCount > 0;
    return {
      phase: manifest?.phase ?? 'none',
      stopReason: hasWork ? 'work_pending' : 'idle',
      hasWork,
      workers: hasWork ? Array.from({ length: this.config.workerCount }, (_, i) => i) : [],
      totalCharacters: manifest?.totalCharacters ?? 0,
      chunkCount: manifest?.chunkCount ?? 0,
      pendingCount,
    };
  }
}
