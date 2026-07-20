/**
 * Build-roster step — its own workflow (its cron runs frequently to keep the
 * per-league roster fresh; the snapshot cadence lives on new-snapshot.yml).
 *
 * One atomic pass over the legacy public ladder, MERGED into the per-league
 * character database (state/<league>/roster.json): new entrants grow the
 * roster, known characters get their class/level/rank/lastSeenAt refreshed,
 * and characters that left the ladder window stay collectable. This is the
 * ONLY step that reads the GGG ladder — the new-snapshot step seeds purely
 * from the roster this step produced (request-free).
 *
 * Rate-limit discipline (hard rule #1): the persisted per-slot limiter state is
 * restored (pace scoped to this runner's IP), the ladder is paced through the
 * limiter, and a hard block aborts the run cleanly (nonzero exit → alert). A
 * capture that runs out of run-budget mid-pass leaves the roster untouched —
 * a partial ladder would skew the ranks — and the next fire recaptures.
 *
 * Runs in its own concurrency group, independent of the collect chain: a build
 * writes only the roster + its own limiter state, so it never shares an R2
 * object with a worker or a new-snapshot fire.
 */
import type { LadderEntry, LadderResult, LadderSource } from '../sources/types.js';
import type { Clock } from '../rate-limit/clock.js';
import type { RateLimiter } from '../rate-limit/limiter.js';
import { COORDINATOR_SLOT } from '../rate-limit/limiter-store.js';
import { LimiterPersistence, type LimiterScope } from '../rate-limit/limiter-persistence.js';
import type { ObjectStore } from '../checkpoint/object-store.js';
import { RosterStore, mergeLadder } from '../roster/roster-store.js';
import type { RunConfig } from './config.js';
import type { WaitReporter } from './resolve-character.js';

export interface BuilderDeps {
  clock: Clock;
  ladderSource: LadderSource;
  objectStore: ObjectStore;
  limiter: RateLimiter;
  /** This runner's public IP (discoverPublicIp) — scopes the restored pace
   *  state via limiter.adoptIp; undefined keeps it (conservative). */
  publicIp?: string | undefined;
  log?: (message: string) => void;
}

export type BuildStopReason = 'built' | 'aborted' | 'budget_exhausted';

export interface BuildSummary {
  stopReason: BuildStopReason;
  requests: number;
  /** Roster size after this build (unchanged from before on abort/budget). */
  rosterSize: number;
  /** Characters appended this build (0 unless a full capture merged). */
  rosterAdded: number;
  /** Known characters refreshed this build. */
  rosterRefreshed: number;
}

export class RosterBuilder {
  private readonly limiterState: LimiterPersistence;
  private readonly rosters: RosterStore;

  constructor(
    private readonly config: RunConfig,
    private readonly deps: BuilderDeps,
  ) {
    this.limiterState = new LimiterPersistence(deps.objectStore);
    this.rosters = new RosterStore(deps.objectStore);
  }

  /** Client state under the coordinator slot; pace shared under the runner IP. */
  private get limiterScope(): LimiterScope {
    return { league: this.config.league, slot: COORDINATOR_SLOT, ip: this.deps.publicIp };
  }

  private log(message: string): void {
    this.deps.log?.(message);
  }

  private readonly onWait: WaitReporter = (ms, reason) => {
    if (ms >= 2000) this.log(`rate-limit: waiting ${(ms / 1000).toFixed(1)}s (${reason})`);
  };

  async runOnce(): Promise<BuildSummary> {
    const runStart = this.deps.clock.now();
    if (await this.limiterState.loadInto(this.deps.limiter, this.limiterScope)) {
      this.log('rate-limit: runner IP changed since the checkpoint — pace windows start fresh');
    }

    const captured = await this.captureLadder(runStart);
    if (captured.kind !== 'ok') {
      // Nothing merged this run — report the current (unchanged) roster size.
      // A hard abort exits nonzero (alert); a budget stop is a clean exit and
      // the next fire recaptures from scratch.
      await this.saveLimiter();
      const rosterSize = (await this.rosters.load(this.config.league)).characters.length;
      const stopReason: BuildStopReason =
        captured.kind === 'aborted' ? 'aborted' : 'budget_exhausted';
      return {
        stopReason,
        requests: captured.requests,
        rosterSize,
        rosterAdded: 0,
        rosterRefreshed: 0,
      };
    }

    // Every ladder read appends what it saw to the per-league character
    // database. New entrants grow the roster; the first capture seeds it.
    const nowIso = new Date(this.deps.clock.now()).toISOString();
    const roster = await this.rosters.load(this.config.league);
    const merged = mergeLadder(roster, captured.entries, nowIso);
    await this.rosters.save(merged.roster);
    await this.saveLimiter();
    this.log(
      `roster: ${merged.roster.characters.length} characters ` +
        `(+${merged.added} new, ${merged.refreshed} refreshed)`,
    );

    return {
      stopReason: 'built',
      requests: captured.requests,
      rosterSize: merged.roster.characters.length,
      rosterAdded: merged.added,
      rosterRefreshed: merged.refreshed,
    };
  }

  private async captureLadder(
    runStart: number,
  ): Promise<
    | { kind: 'ok'; entries: LadderEntry[]; requests: number }
    | { kind: 'aborted'; requests: number }
    | { kind: 'budget'; requests: number }
  > {
    const entries: LadderEntry[] = [];
    const { depth, ladderPageSize, league, maxAttempts, maxRunMillis } = this.config;
    let requests = 0;

    for (let offset = 0; offset < depth; offset += ladderPageSize) {
      let page: Extract<LadderResult, { kind: 'ok' }> | undefined;
      let attempts = 0;

      // Retry the page until it succeeds, is exhausted, aborts, or budget ends.
      while (page === undefined) {
        if (this.deps.limiter.isAborted) return { kind: 'aborted', requests };
        if (this.deps.clock.now() - runStart >= maxRunMillis) {
          // Capture is one atomic pass; a partial ladder would skew the roster
          // ranks, so restart cleanly next run.
          return { kind: 'budget', requests };
        }

        const limit = Math.min(ladderPageSize, depth - offset);
        await this.deps.limiter.acquire(this.onWait);
        this.log(`ladder: fetching ${league} offset=${offset} limit=${limit}`);
        const { result, observation } = await this.deps.ladderSource.fetchPage({
          league,
          offset,
          limit,
        });
        this.deps.limiter.observe(observation);
        requests += 1;

        if (result.kind === 'ok') {
          page = result;
          this.log(
            `ladder: got ${result.entries.length} entries at offset=${offset} (total=${result.total})`,
          );
          break;
        }
        this.log(`ladder: offset=${offset} not ok (${result.kind}); retrying`);
        // Ladder is not per-profile: a fatal status is a misconfiguration.
        if (result.kind === 'fatal') return { kind: 'aborted', requests };
        // rate_limited / retryable (incl. malformed ladder): bounded retry. The
        // limiter has already backed off; giving up after maxAttempts stops us
        // hammering a persistently-failing page (hard rules #1/#4).
        attempts += 1;
        if (attempts >= maxAttempts) return { kind: 'aborted', requests };
      }

      entries.push(...page.entries);
      if (page.entries.length === 0 || entries.length >= page.total) break;
    }

    this.log(`ladder: captured ${entries.length} characters for ${league} (depth ${depth})`);
    return { kind: 'ok', entries, requests };
  }

  private async saveLimiter(): Promise<void> {
    await this.limiterState.save(
      this.deps.limiter,
      this.limiterScope,
      new Date(this.deps.clock.now()).toISOString(),
    );
  }
}
