/**
 * ECONOMY POE.NINJA: cache every documented poe1 economy category from
 * https://poe.ninja/docs/api into ONE file per league (economy/<league>.json).
 *
 * poe.ninja's usage guidelines (not GGG's X-Rate-Limit protocol) apply here:
 * responses are HTTP-cached ~5 min and the underlying poe1 data refreshes
 * ~every 15 min, so the workflow polls hourly; requests are sequential with a
 * politeness gap, carry the identifiable User-Agent (same contact rule as GGG
 * traffic — hard rule #1 in spirit), and back off on 429/5xx.
 *
 * A league's file is written ALL-OR-NOTHING: one fetch pass either produces a
 * complete snapshot (every category present, one fetchedAt) or leaves the last
 * good file untouched and fails the run — a partially-overwritten cache would
 * silently drop categories readers already relied on.
 */
import type { EconomyEndpointKey, EconomySnapshotFile } from '@pou/shared';
import { ECONOMY_SCHEMA_VERSION, economyPath } from '@pou/shared';
import type { Clock } from '../rate-limit/clock.js';
import type { HttpClient } from '../sources/types.js';
import { putJson, type ObjectStore } from '../checkpoint/object-store.js';

/**
 * Leagues to cache, hardcoded for the first iteration (a later one derives
 * this from /poe1/api/economy/leagues): the current temporary challenge league
 * plus the permanent Standard league.
 */
export const ECONOMY_LEAGUES: readonly string[] = ['Mirage', 'Standard'];

const DEFAULT_BASE_URL = 'https://poe.ninja';

/**
 * The documented poe1 endpoint groups and their accepted `type` values,
 * transcribed from https://poe.ninja/docs/api (there is no discovery endpoint
 * for the lists — the docs page is the contract).
 */
export const POE1_ECONOMY_ENDPOINTS: readonly {
  key: EconomyEndpointKey;
  path: string;
  types: readonly string[];
}[] = [
  {
    key: 'exchange',
    path: '/poe1/api/economy/exchange/current/overview',
    types: [
      'Currency',
      'Fragment',
      'Runegraft',
      'AllflameEmber',
      'Tattoo',
      'Omen',
      'DjinnCoin',
      'DivinationCard',
      'Artifact',
      'Oil',
      'DeliriumOrb',
      'Scarab',
      'Astrolabe',
      'Fossil',
      'Resonator',
      'Essence',
    ],
  },
  {
    key: 'stashItem',
    path: '/poe1/api/economy/stash/current/item/overview',
    types: [
      'Wombgift',
      'Incubator',
      'UniqueWeapon',
      'UniqueArmour',
      'UniqueAccessory',
      'UniqueFlask',
      'UniqueJewel',
      'ForbiddenJewel',
      'ShrineBelt',
      'UniqueTincture',
      'UniqueRelic',
      'SkillGem',
      'ImbuedGem',
      'ClusterJewel',
      'Map',
      'BlightedMap',
      'BlightRavagedMap',
      'UniqueMap',
      'ValdoMap',
      'Invitation',
      'Memory',
      'IncursionTemple',
      'BaseType',
      'Beast',
      'Vial',
    ],
  },
  {
    key: 'stashCurrency',
    path: '/poe1/api/economy/stash/current/currency/overview',
    types: ['Currency', 'Fragment'],
  },
];

export interface EconomyConfig {
  userAgent: string;
  leagues?: readonly string[];
  /** Defaults to the real host; overridable so tests stay self-descriptive. */
  baseUrl?: string;
  /** Politeness gap between consecutive requests (ms). */
  requestGapMillis?: number;
  /** Attempts per category before it counts as failed (429/5xx only). */
  maxAttempts?: number;
}

export interface EconomyDeps {
  clock: Clock;
  http: HttpClient;
  objectStore: ObjectStore;
  log: (message: string) => void;
}

export interface EconomyCategoryFailure {
  endpoint: EconomyEndpointKey;
  type: string;
  reason: string;
}

export interface EconomyLeagueResult {
  league: string;
  /** Categories fetched successfully (== the full list when written). */
  categoriesFetched: number;
  failures: EconomyCategoryFailure[];
  /** True when the complete snapshot replaced economy/<league>.json. */
  written: boolean;
}

export interface EconomySummary {
  leagues: EconomyLeagueResult[];
  requestCount: number;
}

const DEFAULT_REQUEST_GAP_MILLIS = 300;
const DEFAULT_MAX_ATTEMPTS = 3;
/** Cap a Retry-After wait so one hostile header can't stall the whole run. */
const MAX_RETRY_WAIT_MILLIS = 60_000;

export class EconomyCollector {
  private readonly baseUrl: string;
  private readonly leagues: readonly string[];
  private readonly requestGapMillis: number;
  private readonly maxAttempts: number;
  private requestCount = 0;

  constructor(
    private readonly config: EconomyConfig,
    private readonly deps: EconomyDeps,
  ) {
    this.baseUrl = config.baseUrl ?? DEFAULT_BASE_URL;
    this.leagues = config.leagues ?? ECONOMY_LEAGUES;
    this.requestGapMillis = config.requestGapMillis ?? DEFAULT_REQUEST_GAP_MILLIS;
    this.maxAttempts = config.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
  }

  /** One full pass: every category for every league, one file per league. */
  async runOnce(): Promise<EconomySummary> {
    const leagues: EconomyLeagueResult[] = [];
    for (const league of this.leagues) {
      leagues.push(await this.collectLeague(league));
    }
    return { leagues, requestCount: this.requestCount };
  }

  private async collectLeague(league: string): Promise<EconomyLeagueResult> {
    const categories: EconomySnapshotFile['categories'] = {
      exchange: {},
      stashItem: {},
      stashCurrency: {},
    };
    const failures: EconomyCategoryFailure[] = [];
    let fetched = 0;

    for (const endpoint of POE1_ECONOMY_ENDPOINTS) {
      for (const type of endpoint.types) {
        const url =
          `${this.baseUrl}${endpoint.path}` +
          `?league=${encodeURIComponent(league)}&type=${encodeURIComponent(type)}`;
        const outcome = await this.fetchCategory(url);
        if (outcome.kind === 'ok') {
          categories[endpoint.key][type] = outcome.data;
          fetched += 1;
        } else {
          failures.push({ endpoint: endpoint.key, type, reason: outcome.reason });
          this.deps.log(`${league}: ${endpoint.key}/${type} failed (${outcome.reason})`);
        }
      }
    }

    // All-or-nothing: a partial pass keeps the last good file (see header).
    const written = failures.length === 0;
    if (written) {
      const file: EconomySnapshotFile = {
        schemaVersion: ECONOMY_SCHEMA_VERSION,
        game: 'poe1',
        league,
        fetchedAt: new Date(this.deps.clock.now()).toISOString(),
        categories,
      };
      await putJson(this.deps.objectStore, economyPath(league), file);
      this.deps.log(`${league}: wrote ${economyPath(league)} (${fetched} categories)`);
    }
    return { league, categoriesFetched: fetched, failures, written };
  }

  private async fetchCategory(
    url: string,
  ): Promise<{ kind: 'ok'; data: unknown } | { kind: 'failed'; reason: string }> {
    let reason = 'unreachable';
    for (let attempt = 1; attempt <= this.maxAttempts; attempt += 1) {
      // Politeness gap before every request (including retries): sequential,
      // never bursty — poe.ninja asks for reasonable volume, not a protocol.
      await this.deps.clock.sleep(this.requestGapMillis);
      this.requestCount += 1;
      let res;
      try {
        res = await this.deps.http({
          url,
          headers: { 'user-agent': this.config.userAgent },
        });
      } catch (err) {
        reason = `fetch error: ${err instanceof Error ? err.message : String(err)}`;
        await this.backoff(attempt);
        continue;
      }

      if (res.status === 200) {
        try {
          return { kind: 'ok', data: JSON.parse(res.body) };
        } catch {
          reason = 'invalid JSON';
          await this.backoff(attempt);
          continue;
        }
      }
      if (res.status === 429) {
        // Honor Retry-After when present; a repeat past maxAttempts fails the
        // category (and thereby the run) rather than hammering on.
        const retryAfter = Number.parseFloat(res.headers['retry-after'] ?? '');
        const waitMillis = Number.isFinite(retryAfter)
          ? Math.min(retryAfter * 1000, MAX_RETRY_WAIT_MILLIS)
          : this.backoffMillis(attempt);
        reason = 'throttled (429)';
        await this.deps.clock.sleep(waitMillis);
        continue;
      }
      if (res.status >= 500) {
        reason = `server error (${res.status})`;
        await this.backoff(attempt);
        continue;
      }
      // Any other 4xx is a contract mismatch (unknown type / moved route) —
      // retrying cannot fix it, so fail the category immediately and loudly.
      return { kind: 'failed', reason: `unexpected status ${res.status}` };
    }
    return { kind: 'failed', reason };
  }

  private backoffMillis(attempt: number): number {
    return 2 ** attempt * 1000;
  }

  private backoff(attempt: number): Promise<void> {
    return this.deps.clock.sleep(this.backoffMillis(attempt));
  }
}
