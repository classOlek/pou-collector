/**
 * Operator override: clear `aborted` checkpoints so the next collect run starts
 * a fresh snapshot immediately instead of serving out the abort cooldown.
 *
 * The cooldown exists to stop a systematically-failing snapshot from hammering
 * GGG in a loop (hard rule #1). When the failure was a code/config bug that has
 * since been fixed, waiting out the remaining cooldown serves no one — but the
 * decision that the cause is fixed is a human judgement, so this only ever runs
 * from an explicit workflow_dispatch input (`reset_aborted`), never on schedule.
 * Only `aborted` checkpoints are cleared; in-flight or published snapshots are
 * untouched.
 */
import type { CheckpointStore } from './checkpoint/store.js';

/** `COLLECTOR_RESET_ABORTED` is set from a boolean workflow input ('true'/'false'). */
export function shouldResetAborted(value: string | undefined): boolean {
  const v = value?.trim();
  return v === 'true' || v === '1';
}

/** Clear every league checkpoint parked in `aborted`; returns the cleared leagues. */
export async function resetAbortedCheckpoints(store: CheckpointStore): Promise<string[]> {
  const aborted = (await store.listAll()).filter((m) => m.phase === 'aborted');
  for (const manifest of aborted) await store.clear(manifest.league);
  return aborted.map((m) => m.league);
}
