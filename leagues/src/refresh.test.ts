import { describe, expect, it } from 'vitest';
import {
  fetchLeagues,
  resolveEnv,
  runRefresh,
  userAgent,
  type FetchLike,
  type FetchResponse,
  type RefreshDeps,
  type RefreshEnv,
} from './refresh.js';

const LEAGUES = [
  { id: 'Standard', realm: 'pc', category: { id: 'Standard' } },
  { id: 'Hardcore', realm: 'pc', category: { id: 'Standard' } },
];

function jsonResponse(status: number, body: unknown): FetchResponse {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(typeof body === 'string' ? body : JSON.stringify(body)),
  };
}

type Handler = (url: string, init: Parameters<FetchLike>[1], attempt: number) => FetchResponse;

interface Recorded {
  url: string;
  init: Parameters<FetchLike>[1];
}

function makeFetch(handler: Handler): { fetch: FetchLike; calls: Recorded[] } {
  const calls: Recorded[] = [];
  const fetch: FetchLike = (url, init) => {
    calls.push({ url, init });
    return Promise.resolve(handler(url, init, calls.length));
  };
  return { fetch, calls };
}

function deps(fetch: FetchLike): RefreshDeps {
  return { fetch, sleep: () => Promise.resolve(), log: () => {} };
}

function env(overrides: Partial<RefreshEnv> = {}): RefreshEnv {
  return {
    supabaseUrl: 'https://proj.supabase.co',
    serviceRoleKey: 'svc-key',
    contactEmail: 'ops@example.com',
    ...overrides,
  };
}

describe('runRefresh', () => {
  it('forwards the PoE array verbatim to the upsert_leagues RPC and returns the count', async () => {
    const { fetch, calls } = makeFetch((url, init) => {
      if (url.startsWith('https://api.pathofexile.com')) {
        return jsonResponse(200, LEAGUES);
      }
      if (url.includes('/rest/v1/rpc/upsert_leagues')) {
        const body = JSON.parse(init?.body ?? '{}') as { payload: unknown[] };
        return jsonResponse(200, body.payload.length);
      }
      throw new Error(`unexpected url ${url}`);
    });

    const count = await runRefresh(deps(fetch), env());
    expect(count).toBe(2);

    const poeCall = calls.find((c) => c.url.startsWith('https://api.pathofexile.com'));
    const upsertCall = calls.find((c) => c.url.includes('/rest/v1/rpc/upsert_leagues'));
    expect(poeCall?.init?.headers?.['User-Agent']).toContain('ops@example.com');
    expect(upsertCall?.url).toBe('https://proj.supabase.co/rest/v1/rpc/upsert_leagues');
    expect(upsertCall?.init?.headers?.['Authorization']).toBe('Bearer svc-key');
    expect(upsertCall?.init?.headers?.['apikey']).toBe('svc-key');
    const sent = JSON.parse(upsertCall?.init?.body ?? '{}') as { payload: unknown[] };
    expect(sent.payload).toEqual(LEAGUES);
  });
});

describe('fetchLeagues', () => {
  it('fails fast on a 4xx without retrying', async () => {
    const { fetch, calls } = makeFetch(() => jsonResponse(404, 'not found'));
    await expect(fetchLeagues(deps(fetch), env())).rejects.toThrow(/HTTP 404/);
    expect(calls.length).toBe(1);
  });

  it('retries a transient 5xx and then succeeds', async () => {
    const { fetch, calls } = makeFetch((_url, _init, attempt) =>
      attempt === 1 ? jsonResponse(503, 'busy') : jsonResponse(200, LEAGUES),
    );
    const out = await fetchLeagues(deps(fetch), env());
    expect(out).toEqual(LEAGUES);
    expect(calls.length).toBe(2);
  });

  it('rejects a non-array body as a contract violation', async () => {
    const { fetch, calls } = makeFetch(() => jsonResponse(200, { error: 'nope' }));
    await expect(fetchLeagues(deps(fetch), env())).rejects.toThrow(/non-array/);
    expect(calls.length).toBe(1);
  });
});

describe('resolveEnv', () => {
  it('builds the Supabase URL from the project ref and trims a trailing slash', () => {
    expect(
      resolveEnv({
        SUPABASE_SERVICE_ROLE_KEY: 'k',
        COLLECTOR_CONTACT_EMAIL: 'e@x.io',
        SUPABASE_PROJECT_REF: 'abc',
      }).supabaseUrl,
    ).toBe('https://abc.supabase.co');

    expect(
      resolveEnv({
        SUPABASE_SERVICE_ROLE_KEY: 'k',
        COLLECTOR_CONTACT_EMAIL: 'e@x.io',
        SUPABASE_URL: 'https://custom.example.co/',
      }).supabaseUrl,
    ).toBe('https://custom.example.co');
  });

  it('throws when a required secret is missing', () => {
    expect(() => resolveEnv({ COLLECTOR_CONTACT_EMAIL: 'e@x.io' })).toThrow(
      /SUPABASE_SERVICE_ROLE_KEY/,
    );
  });
});

describe('userAgent', () => {
  it('embeds the contact address', () => {
    expect(userAgent('ops@example.com')).toContain('ops@example.com');
  });
});
