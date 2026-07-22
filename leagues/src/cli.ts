/**
 * CLI entry for the leagues refresh (run by `pnpm --filter @pou/leagues run
 * refresh`). Wires the real fetch, timers and process env into the pure
 * runRefresh() and maps failure to a nonzero exit so a broken fire fails the
 * GitHub Actions run loudly.
 */
import { resolveEnv, runRefresh, type FetchLike, type RefreshDeps } from './refresh.js';

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

// Adapt the global fetch to the FetchLike seam, adding a per-request timeout so
// a hung connection cannot stall the run indefinitely.
const fetchImpl: FetchLike = (url, init) =>
  fetch(url, { ...(init as RequestInit), signal: AbortSignal.timeout(30_000) });

async function main(): Promise<void> {
  const env = resolveEnv(process.env);
  const deps: RefreshDeps = {
    fetch: fetchImpl,
    sleep,
    log: (message) => console.log(`[leagues-refresh] ${message}`),
  };
  await runRefresh(deps, env);
}

main().catch((err: unknown) => {
  console.error(`[leagues-refresh] failed: ${err instanceof Error ? err.message : String(err)}`);
  process.exitCode = 1;
});
