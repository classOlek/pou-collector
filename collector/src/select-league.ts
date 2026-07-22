/**
 * Which league a collect run works on (docs/ARCHITECTURE.md §5, finding 8).
 *
 * An in-flight snapshot in ANY league is resumed before a new one is started —
 * otherwise a workflow_dispatch that overrode the league would strand that
 * snapshot forever (scheduled runs only look at the configured league). The
 * configured league wins when it too is in-flight; otherwise the first other
 * in-flight league is continued. One in-flight league at a time is the accepted
 * bounded scope (proper multi-league is Phase 7).
 */
import { isInFlight } from '@classolek/shared';
import type { CheckpointStore } from './checkpoint/store.js';

export async function selectCollectLeague(
  checkpointStore: CheckpointStore,
  configLeague: string,
): Promise<string> {
  const inflight = (await checkpointStore.listAll()).filter((m) => isInFlight(m.phase));
  if (inflight.some((m) => m.league === configLeague)) return configLeague;
  return inflight[0]?.league ?? configLeague;
}
