/**
 * Per-runner-slot limiter memory at state/<league>/workers/<slot>.json.
 *
 * With parallel workers each job runs on its own GitHub Actions runner (its own
 * IP), so rate-limit adaptation is per slot: the coordinator and every worker
 * slot persist their own observed rules / penalty / recent-acquire window
 * across cron runs. A slot only ever writes its own file (single writer).
 */
import type { LimiterMemory, WorkerState } from '@pou/shared';
import { SCHEMA_VERSION, workerStatePath } from '@pou/shared';
import { getJson, putJson, type ObjectStore } from '../checkpoint/object-store.js';

/** The coordinator's slot name; workers use `w<index>`. */
export const COORDINATOR_SLOT = 'coordinator';

export function workerSlot(workerIndex: number): string {
  return `w${workerIndex}`;
}

export class LimiterStateStore {
  constructor(private readonly store: ObjectStore) {}

  /** Load a slot's limiter memory; absent/corrupt/foreign → undefined (fresh limiter). */
  async load(league: string, slot: string): Promise<LimiterMemory | undefined> {
    let state: WorkerState | undefined;
    try {
      state = await getJson<WorkerState>(this.store, workerStatePath(league, slot));
    } catch {
      return undefined;
    }
    if (!state || state.schemaVersion !== SCHEMA_VERSION) return undefined;
    return state.limiter;
  }

  async save(league: string, slot: string, limiter: LimiterMemory, nowIso: string): Promise<void> {
    const state: WorkerState = { schemaVersion: SCHEMA_VERSION, slot, updatedAt: nowIso, limiter };
    await putJson(this.store, workerStatePath(league, slot), state);
  }
}
