import { describe, expect, it } from 'vitest';
import type { EconomySnapshotFile } from '@pou/shared';
import { ECONOMY_SCHEMA_VERSION, economyPath } from '@pou/shared';
import type { HttpRequest, HttpResponse } from '../sources/types.js';
import { MemoryObjectStore, getJson } from '../checkpoint/object-store.js';
import { FakeClock } from '../rate-limit/clock.js';
import { economyExitCode } from '../run-summary.js';
import { EconomyCollector, POE1_ECONOMY_ENDPOINTS } from './poe-ninja.js';

const BASE = 'https://ninja.test';
const UA = 'poe-ladder-stats/0.1 (+contact@example.invalid)';

const CATEGORY_COUNT = POE1_ECONOMY_ENDPOINTS.reduce((sum, e) => sum + e.types.length, 0);

function okBody(url: string): string {
  return JSON.stringify({ lines: [{ from: url }] });
}

/** Fake poe.ninja: records requests, serves 200 JSON unless a rule matches. */
function makeHttp(rules: (req: HttpRequest) => HttpResponse | undefined = () => undefined) {
  const requests: HttpRequest[] = [];
  const http = (req: HttpRequest): Promise<HttpResponse> => {
    requests.push(req);
    const ruled = rules(req);
    return Promise.resolve(ruled ?? { status: 200, headers: {}, body: okBody(req.url) });
  };
  return { http, requests };
}

function makeCollector(
  http: (req: HttpRequest) => Promise<HttpResponse>,
  store: MemoryObjectStore,
  overrides: { leagues?: string[]; maxAttempts?: number } = {},
) {
  return new EconomyCollector(
    {
      userAgent: UA,
      baseUrl: BASE,
      leagues: overrides.leagues ?? ['Mirage', 'Standard'],
      maxAttempts: overrides.maxAttempts ?? 3,
    },
    {
      clock: new FakeClock(Date.parse('2026-07-21T12:00:00.000Z')),
      http,
      objectStore: store,
      log: () => {},
    },
  );
}

describe('EconomyCollector', () => {
  it('caches every documented category into one file per league', async () => {
    const { http, requests } = makeHttp();
    const store = new MemoryObjectStore();
    const summary = await makeCollector(http, store).runOnce();

    expect(summary.requestCount).toBe(2 * CATEGORY_COUNT);
    expect(summary.leagues.map((l) => l.written)).toEqual([true, true]);
    expect(economyExitCode(summary)).toBe(0);

    for (const league of ['Mirage', 'Standard']) {
      const file = await getJson<EconomySnapshotFile>(store, economyPath(league));
      expect(file).toBeDefined();
      expect(file?.schemaVersion).toBe(ECONOMY_SCHEMA_VERSION);
      expect(file?.game).toBe('poe1');
      expect(file?.league).toBe(league);
      // The fake clock advances by the politeness gaps, so the timestamp sits
      // a few virtual seconds into the pass — same minute, never before it.
      expect(file?.fetchedAt).toMatch(/^2026-07-21T12:00:/);
      for (const endpoint of POE1_ECONOMY_ENDPOINTS) {
        expect(Object.keys(file?.categories[endpoint.key] ?? {})).toEqual([...endpoint.types]);
      }
    }
    // Every request carries the identifiable User-Agent and hits the league +
    // type query documented by poe.ninja.
    for (const req of requests) {
      expect(req.headers['user-agent']).toBe(UA);
      expect(req.url).toMatch(/\?league=(Mirage|Standard)&type=[A-Za-z]+$/);
    }
  });

  it('retries a 429 honoring Retry-After, then succeeds', async () => {
    let throttles = 0;
    const { http } = makeHttp((req) => {
      if (req.url.includes('type=Scarab') && throttles === 0) {
        throttles += 1;
        return { status: 429, headers: { 'retry-after': '2' }, body: '' };
      }
      return undefined;
    });
    const store = new MemoryObjectStore();
    const summary = await makeCollector(http, store, { leagues: ['Mirage'] }).runOnce();

    expect(throttles).toBe(1);
    expect(summary.leagues[0]?.written).toBe(true);
    expect(summary.requestCount).toBe(CATEGORY_COUNT + 1);
  });

  it('keeps the last good file and fails when a category stays down', async () => {
    const store = new MemoryObjectStore();
    const previous = new TextEncoder().encode('{"last":"good"}');
    await store.put(economyPath('Mirage'), previous);

    const { http } = makeHttp((req) =>
      req.url.includes('type=BaseType') ? { status: 500, headers: {}, body: 'boom' } : undefined,
    );
    const summary = await makeCollector(http, store, { leagues: ['Mirage'] }).runOnce();

    const result = summary.leagues[0];
    expect(result?.written).toBe(false);
    expect(result?.failures).toEqual([
      { endpoint: 'stashItem', type: 'BaseType', reason: 'server error (500)' },
    ]);
    expect(result?.categoriesFetched).toBe(CATEGORY_COUNT - 1);
    expect(economyExitCode(summary)).toBe(1);
    // All-or-nothing: the previous complete cache survives the failed pass.
    expect(await store.get(economyPath('Mirage'))).toEqual(previous);
  });

  it('fails an unknown category immediately without retrying (4xx)', async () => {
    let attempts = 0;
    const { http } = makeHttp((req) => {
      if (req.url.includes('type=Vial')) {
        attempts += 1;
        return { status: 404, headers: {}, body: 'nope' };
      }
      return undefined;
    });
    const summary = await makeCollector(http, new MemoryObjectStore(), {
      leagues: ['Mirage'],
    }).runOnce();

    expect(attempts).toBe(1);
    expect(summary.leagues[0]?.failures).toEqual([
      { endpoint: 'stashItem', type: 'Vial', reason: 'unexpected status 404' },
    ]);
  });

  it("one league's failure never blocks the other league's write", async () => {
    const { http } = makeHttp((req) =>
      req.url.includes('league=Mirage') ? { status: 503, headers: {}, body: '' } : undefined,
    );
    const store = new MemoryObjectStore();
    const summary = await makeCollector(http, store).runOnce();

    expect(summary.leagues.map((l) => ({ league: l.league, written: l.written }))).toEqual([
      { league: 'Mirage', written: false },
      { league: 'Standard', written: true },
    ]);
    expect(await store.get(economyPath('Mirage'))).toBeUndefined();
    expect(await getJson(store, economyPath('Standard'))).toBeDefined();
    expect(economyExitCode(summary)).toBe(1);
  });
});
