/**
 * Mock PoE API: an in-memory `HttpClient` serving recorded-shape fixtures so
 * the whole collector runs with zero network (hard rules #3/#4). It routes by
 * URL (ladders / get-items / get-passive-skills), models per-character
 * behaviors (ok, private, dead, flaky retryable, throttled), and counts calls
 * so tests can assert that resolved characters are never re-fetched.
 */
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { HttpClient, HttpRequest, HttpResponse } from '../src/sources/types.js';

const here = dirname(fileURLToPath(import.meta.url));
const readFixture = (name: string): string => readFileSync(join(here, 'fixtures', name), 'utf8');

export const fixtures = {
  items: readFixture('get-items.json'),
  passives: readFixture('get-passive-skills.json'),
  privateError: readFixture('private-error.json'),
  challenge: readFixture('challenge.html'),
  /** GGG serves a 403 *HTML* page (not JSON) for private profiles. */
  profileForbidden: readFixture('profile-forbidden.html'),
};

/** Rate-limit headers shaped like real GGG responses. */
export const RATE_HEADERS: Record<string, string> = {
  'x-rate-limit-rules': 'Ip',
  'x-rate-limit-ip': '8:10:60,15:60:120',
  'x-rate-limit-ip-state': '1:10:0,1:60:0',
};

export const THROTTLED_HEADERS: Record<string, string> = {
  ...RATE_HEADERS,
  'x-rate-limit-ip-state': '9:10:60,1:60:0',
  'retry-after': '30',
};

export type Behavior =
  | { kind: 'ok' }
  | { kind: 'private' }
  | { kind: 'dead' }
  /** get-items returns 500 for the first `fails` calls, then succeeds. */
  | { kind: 'flaky'; fails: number }
  /** get-items always returns 429 (drives the limiter toward abort). */
  | { kind: 'throttle' }
  /** get-items returns 429 for the first `throttles` calls, then succeeds. */
  | { kind: 'throttleThenOk'; throttles: number }
  /** get-items always returns a Cloudflare HTML challenge (200 body). */
  | { kind: 'challenge' };

export interface MockEntry {
  account: string;
  character: string;
  class: string;
  level: number;
  behavior: Behavior;
}

export interface MockApiOptions {
  league: string;
  entries: MockEntry[];
  /** Fail the very next N ladder-page fetches with 429 before succeeding. */
  ladderThrottleFirst?: number;
}

function key(account: string, character: string): string {
  return `${account}/${character}`;
}

function queryParam(url: string, name: string): string {
  return new URL(url).searchParams.get(name) ?? '';
}

export class MockPoeApi {
  private readonly byKey = new Map<string, MockEntry>();
  readonly itemCalls = new Map<string, number>();
  readonly passiveCalls = new Map<string, number>();
  private ladderThrottleRemaining: number;

  constructor(private readonly options: MockApiOptions) {
    for (const e of options.entries) this.byKey.set(key(e.account, e.character), e);
    this.ladderThrottleRemaining = options.ladderThrottleFirst ?? 0;
  }

  /** The injectable seam handed to the legacy sources. */
  readonly client: HttpClient = (req) => Promise.resolve(this.handle(req));

  private handle(req: HttpRequest): HttpResponse {
    if (req.url.includes('/ladders')) return this.ladder(req);
    if (req.url.includes('get-items')) return this.items(req);
    if (req.url.includes('get-passive-skills')) return this.passives(req);
    return json(404, { error: { code: 1, message: 'not found' } });
  }

  private ladder(req: HttpRequest): HttpResponse {
    if (this.ladderThrottleRemaining > 0) {
      this.ladderThrottleRemaining -= 1;
      return { status: 429, headers: { ...THROTTLED_HEADERS }, body: '' };
    }
    const offset = Number.parseInt(queryParam(req.url, 'offset'), 10) || 0;
    const limit = Number.parseInt(queryParam(req.url, 'limit'), 10) || 200;
    const page = this.options.entries.slice(offset, offset + limit).map((e, i) => ({
      rank: offset + i + 1,
      dead: false,
      online: false,
      character: {
        name: e.character,
        level: e.level,
        class: e.class,
        id: `char-${offset + i}`,
        experience: 0,
      },
      account: { name: e.account, realm: 'pc' },
    }));
    return json(200, { total: this.options.entries.length, cached: false, entries: page });
  }

  private items(req: HttpRequest): HttpResponse {
    const k = key(queryParam(req.url, 'accountName'), queryParam(req.url, 'character'));
    const attempt = (this.itemCalls.get(k) ?? 0) + 1;
    this.itemCalls.set(k, attempt);
    const behavior = this.byKey.get(k)?.behavior ?? { kind: 'ok' };

    switch (behavior.kind) {
      case 'private':
        return { status: 403, headers: { ...RATE_HEADERS }, body: fixtures.privateError };
      case 'dead':
        return json(404, { error: { code: 2, message: 'Character not found' } });
      case 'throttle':
        return { status: 429, headers: { ...THROTTLED_HEADERS }, body: '' };
      case 'throttleThenOk':
        return attempt <= behavior.throttles
          ? { status: 429, headers: { ...THROTTLED_HEADERS }, body: '' }
          : okItems();
      case 'challenge':
        return { status: 200, headers: { ...RATE_HEADERS }, body: fixtures.challenge };
      case 'flaky':
        if (attempt <= behavior.fails) {
          return json(500, { error: { code: 5, message: 'Internal Server Error' } });
        }
        return okItems();
      case 'ok':
        return okItems();
    }
  }

  private passives(req: HttpRequest): HttpResponse {
    const k = key(queryParam(req.url, 'accountName'), queryParam(req.url, 'character'));
    this.passiveCalls.set(k, (this.passiveCalls.get(k) ?? 0) + 1);
    return { status: 200, headers: { ...RATE_HEADERS }, body: fixtures.passives };
  }
}

function okItems(): HttpResponse {
  return { status: 200, headers: { ...RATE_HEADERS }, body: fixtures.items };
}

/** Build a JSON response with the standard rate-limit headers. */
export function json(status: number, body: unknown): HttpResponse {
  return { status, headers: { ...RATE_HEADERS }, body: JSON.stringify(body) };
}

/** Build a deterministic ladder of `depth` entries spanning every behavior. */
export function buildLadder(depth: number): MockEntry[] {
  const classes = ['Juggernaut', 'Necromancer', 'Deadeye', 'Occultist', 'Champion'];
  const entries: MockEntry[] = [];
  for (let i = 0; i < depth; i += 1) {
    let behavior: Behavior = { kind: 'ok' };
    // fails:1 so a flaky character recovers on its next sweep — distinct
    // characters failing back-to-back on the same sweep would otherwise read as
    // an endpoint outage (consecutive-error abort), which the small explicit
    // outcome test exercises deliberately instead.
    if (i > 0 && i % 50 === 0) behavior = { kind: 'private' };
    else if (i > 0 && i % 75 === 0) behavior = { kind: 'dead' };
    else if (i > 0 && i % 37 === 0) behavior = { kind: 'flaky', fails: 1 };
    entries.push({
      account: `acct-${i}`,
      character: `char-${i}`,
      class: classes[i % classes.length] as string,
      level: 100 - (i % 40),
      behavior,
    });
  }
  return entries;
}
