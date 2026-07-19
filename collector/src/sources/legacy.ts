/**
 * Legacy PoE endpoint implementations of the source interfaces:
 *   - ladder:    GET www.pathofexile.com/api/ladders?id=&offset=&limit=
 *   - character: GET /character-window/get-items?accountName=&character=
 *                GET /character-window/get-passive-skills?accountName=&character=
 *
 * These endpoints are undocumented and public-profiles-only (hard rules #2/#7).
 * Edge classification lives in ./classify.ts; this file only builds requests,
 * injects the identifiable User-Agent, and maps categories to result kinds.
 */
import { classifyResponse, observationFor, toCharacterResult } from './classify.js';
import type {
  CharacterCallResult,
  CharacterQuery,
  CharacterSource,
  HttpClient,
  LadderEntry,
  LadderQuery,
  LadderResult,
  LadderSource,
  Observed,
} from './types.js';

export interface LegacySourceConfig {
  userAgent: string;
  /** Defaults to the real hosts; overridable so tests stay self-descriptive. */
  ladderBaseUrl?: string;
  characterBaseUrl?: string;
}

const DEFAULT_LADDER_BASE = 'https://www.pathofexile.com/api/ladders';
const DEFAULT_CHARACTER_BASE = 'https://www.pathofexile.com/character-window';

function encode(v: string): string {
  return encodeURIComponent(v);
}

export class LegacyLadderSource implements LadderSource {
  private readonly base: string;

  constructor(
    private readonly http: HttpClient,
    private readonly config: LegacySourceConfig,
  ) {
    this.base = config.ladderBaseUrl ?? DEFAULT_LADDER_BASE;
  }

  async fetchPage(query: LadderQuery): Promise<Observed<LadderResult>> {
    // The legacy ladder endpoint keys the league on `id`, not `league`; a
    // `league=` query is rejected with 400 `{"error":{"code":2,"message":"Invalid
    // query"}}`, which classifies as a fatal client error and aborts the whole
    // snapshot on ladder page 0 (see the regression test in legacy.test.ts).
    const url = `${this.base}?id=${encode(query.league)}&offset=${query.offset}&limit=${query.limit}`;
    const res = await this.http({ url, headers: { 'user-agent': this.config.userAgent } });
    const category = classifyResponse(res);
    const observation = observationFor(res, category);

    switch (category.tag) {
      case 'json':
        return { result: toLadderResult(category.data), observation };
      case 'rate_limited':
        return { result: { kind: 'rate_limited' }, observation };
      case 'challenge':
        return { result: { kind: 'retryable', reason: 'challenge' }, observation };
      case 'server_error':
        return { result: { kind: 'retryable', reason: 'server' }, observation };
      // Ladder is not per-profile: a 403/404/other here is a misconfiguration,
      // not a skippable profile, so it is fatal to the run.
      default:
        return { result: { kind: 'fatal', status: res.status }, observation };
    }
  }
}

function toLadderResult(data: unknown): LadderResult {
  if (typeof data !== 'object' || data === null || !('entries' in data)) {
    return { kind: 'retryable', reason: 'malformed-ladder' };
  }
  const raw = data as { total?: unknown; entries?: unknown };
  const entriesIn = Array.isArray(raw.entries) ? raw.entries : [];
  const entries = entriesIn.map(toLadderEntry).filter((e): e is LadderEntry => e !== undefined);
  const total = typeof raw.total === 'number' ? raw.total : entries.length;
  return { kind: 'ok', entries, total };
}

function toLadderEntry(entry: unknown): LadderEntry | undefined {
  if (typeof entry !== 'object' || entry === null) return undefined;
  const e = entry as {
    rank?: unknown;
    character?: { name?: unknown; level?: unknown; class?: unknown };
    account?: { name?: unknown };
  };
  const account = e.account?.name;
  const character = e.character?.name;
  if (typeof account !== 'string' || typeof character !== 'string') return undefined;
  return {
    rank: typeof e.rank === 'number' ? e.rank : 0,
    account,
    character,
    class: typeof e.character?.class === 'string' ? e.character.class : 'Unknown',
    level: typeof e.character?.level === 'number' ? e.character.level : 0,
  };
}

export class LegacyCharacterSource implements CharacterSource {
  private readonly base: string;

  constructor(
    private readonly http: HttpClient,
    private readonly config: LegacySourceConfig,
  ) {
    this.base = config.characterBaseUrl ?? DEFAULT_CHARACTER_BASE;
  }

  fetchItems(query: CharacterQuery): Promise<Observed<CharacterCallResult>> {
    return this.call('get-items', query);
  }

  fetchPassives(query: CharacterQuery): Promise<Observed<CharacterCallResult>> {
    return this.call('get-passive-skills', query);
  }

  private async call(
    endpoint: 'get-items' | 'get-passive-skills',
    query: CharacterQuery,
  ): Promise<Observed<CharacterCallResult>> {
    const url = `${this.base}/${endpoint}?accountName=${encode(query.account)}&character=${encode(query.character)}`;
    const res = await this.http({ url, headers: { 'user-agent': this.config.userAgent } });
    const category = classifyResponse(res);
    return { result: toCharacterResult(category), observation: observationFor(res, category) };
  }
}
