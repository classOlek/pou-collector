/**
 * Coordinate step of the COLLECT workflow: a cheap, request-free check of the
 * newest snapshot. If it is `collecting` with characters still awaiting
 * computation, report the worker matrix so the workflow fans workers out;
 * otherwise the whole fire no-ops.
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

export interface CoordinatorDeps {
  checkpointStore: CheckpointStore;
  log?: (message: string) => void;
}

export interface CoordinatorConfig {
  league: string;
  workerCount: number;
}

export type CoordinatorStopReason = 'idle' | 'work_pending';

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
}

export class Coordinator {
  constructor(
    private readonly config: CoordinatorConfig,
    private readonly deps: CoordinatorDeps,
  ) {}

  async runOnce(): Promise<CoordinatorSummary> {
    const manifest = await this.deps.checkpointStore.load(this.config.league);
    if (!manifest) {
      return this.summarize(undefined);
    }
    const summary = this.summarize(manifest);
    this.deps.log?.(
      `coordinate: ${this.config.league} phase=${summary.phase} ` +
        `pending=${summary.pendingCount} hasWork=${summary.hasWork}`,
    );
    return summary;
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
