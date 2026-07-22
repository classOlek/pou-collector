import { describe, expect, it } from 'vitest';
import type { HttpRequest, HttpResponse } from '../sources/types.js';
import {
  fetchEconomyLeagues,
  resolveLeaguesEndpoint,
  selectEconomyLeagues,
  type LeagueRow,
} from './leagues-source.js';

const NOW = Date.parse('2026-07-22T12:00:00.000Z');

function row(overrides: Partial<LeagueRow>): LeagueRow {
  return {
    id: 'X',
    category_id: 'X',
    start_at: '2026-07-01T00:00:00+00:00',
    end_at: null,
    ...overrides,
  };
}

describe('resolveLeaguesEndpoint', () => {
  it('prefers LEAGUES_ENDPOINT and trims a trailing slash', () => {
    expect(resolveLeaguesEndpoint({ LEAGUES_ENDPOINT: 'https://x.co/rest/v1/leagues/' })).toBe(
      'https://x.co/rest/v1/leagues',
    );
  });

  it('derives the resource URL from SUPABASE_URL', () => {
    expect(resolveLeaguesEndpoint({ SUPABASE_URL: 'https://abc.supabase.co/' })).toBe(
      'https://abc.supabase.co/rest/v1/leagues',
    );
  });

  it('derives the resource URL from SUPABASE_PROJECT_REF', () => {
    expect(resolveLeaguesEndpoint({ SUPABASE_PROJECT_REF: 'abc' })).toBe(
      'https://abc.supabase.co/rest/v1/leagues',
    );
  });

  it('throws when no target is configured', () => {
    expect(() => resolveLeaguesEndpoint({})).toThrow(/LEAGUES_ENDPOINT/);
  });
});

describe('selectEconomyLeagues', () => {
  it('returns active temporary leagues plus Standard, Standard last', () => {
    const rows = [
      row({ id: 'Standard', category_id: 'Standard', end_at: null }),
      row({ id: 'Hardcore', category_id: 'Standard', end_at: null }),
      row({ id: 'Mirage', category_id: 'Mirage' }),
      row({ id: 'Hardcore Mirage', category_id: 'Mirage' }),
    ];
    expect(selectEconomyLeagues(rows, NOW)).toEqual(['Mirage', 'Hardcore Mirage', 'Standard']);
  });

  it('collapses to just Standard when no challenge league is active', () => {
    const rows = [
      row({ id: 'Standard', category_id: 'Standard' }),
      row({ id: 'Hardcore', category_id: 'Standard' }),
    ];
    expect(selectEconomyLeagues(rows, NOW)).toEqual(['Standard']);
  });

  it('excludes leagues that have not started or have already ended', () => {
    const rows = [
      row({ id: 'Future', category_id: 'Future', start_at: '2026-08-01T00:00:00+00:00' }),
      row({ id: 'Ended', category_id: 'Ended', end_at: '2026-07-01T00:00:00+00:00' }),
      row({ id: 'NoStart', category_id: 'NoStart', start_at: null }),
      row({ id: 'Mirage', category_id: 'Mirage' }),
    ];
    expect(selectEconomyLeagues(rows, NOW)).toEqual(['Mirage', 'Standard']);
  });

  it('keeps a future end_at and dedupes repeated ids', () => {
    const rows = [
      row({ id: 'Mirage', category_id: 'Mirage', end_at: '2026-09-01T00:00:00+00:00' }),
      row({ id: 'Mirage', category_id: 'Mirage' }),
    ];
    expect(selectEconomyLeagues(rows, NOW)).toEqual(['Mirage', 'Standard']);
  });
});

/** Fake endpoint: records the request, serves a canned response. */
function makeHttp(res: HttpResponse) {
  const requests: HttpRequest[] = [];
  const http = (req: HttpRequest): Promise<HttpResponse> => {
    requests.push(req);
    return Promise.resolve(res);
  };
  return { http, requests };
}

describe('fetchEconomyLeagues', () => {
  const env = { SUPABASE_URL: 'https://abc.supabase.co', SUPABASE_SERVICE_ROLE_KEY: 'svc' };

  it('sends the service_role key and returns the selected leagues', async () => {
    const body = JSON.stringify([
      { id: 'Standard', category_id: 'Standard', start_at: null, end_at: null },
      { id: 'Mirage', category_id: 'Mirage', start_at: '2026-07-01T00:00:00+00:00', end_at: null },
    ]);
    const { http, requests } = makeHttp({ status: 200, headers: {}, body });

    const leagues = await fetchEconomyLeagues(env, { http, now: () => NOW });

    expect(leagues).toEqual(['Mirage', 'Standard']);
    expect(requests[0]!.url).toBe(
      'https://abc.supabase.co/rest/v1/leagues?select=id,category_id,start_at,end_at',
    );
    expect(requests[0]!.headers.apikey).toBe('svc');
    expect(requests[0]!.headers.authorization).toBe('Bearer svc');
  });

  it('throws without the service_role key', async () => {
    const { http } = makeHttp({ status: 200, headers: {}, body: '[]' });
    await expect(
      fetchEconomyLeagues({ SUPABASE_URL: 'https://abc.supabase.co' }, { http, now: () => NOW }),
    ).rejects.toThrow(/SUPABASE_SERVICE_ROLE_KEY/);
  });

  it('throws on a non-200 response', async () => {
    const { http } = makeHttp({ status: 401, headers: {}, body: 'permission denied' });
    await expect(fetchEconomyLeagues(env, { http, now: () => NOW })).rejects.toThrow(/HTTP 401/);
  });

  it('throws on a non-array body', async () => {
    const { http } = makeHttp({ status: 200, headers: {}, body: '{"error":"nope"}' });
    await expect(fetchEconomyLeagues(env, { http, now: () => NOW })).rejects.toThrow(/non-array/);
  });

  it('throws on invalid JSON', async () => {
    const { http } = makeHttp({ status: 200, headers: {}, body: 'not json' });
    await expect(fetchEconomyLeagues(env, { http, now: () => NOW })).rejects.toThrow(
      /invalid JSON/,
    );
  });
});
