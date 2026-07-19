import { describe, expect, it } from 'vitest';
import { fixtures, json } from '../../test/mock-api.js';
import { buildUserAgent, CONTACT_PLACEHOLDER } from '../config.js';
import { LegacyCharacterSource, LegacyLadderSource } from './legacy.js';
import type { HttpClient, HttpRequest, HttpResponse } from './types.js';

const UA = buildUserAgent({ env: { COLLECTOR_CONTACT_EMAIL: 'ci@example.test' } });

function scripted(response: HttpResponse): { client: HttpClient; requests: HttpRequest[] } {
  const requests: HttpRequest[] = [];
  const client: HttpClient = (req) => {
    requests.push(req);
    return Promise.resolve(response);
  };
  return { client, requests };
}

describe('buildUserAgent', () => {
  it('injects the contact into an identifiable User-Agent', () => {
    expect(UA).toBe('poe-ladder-stats/0.1 (+ci@example.test)');
  });

  it('throws when the contact is unset (identifiability is required, hard rule #1)', () => {
    expect(() => buildUserAgent({ env: {} })).toThrow(/COLLECTOR_CONTACT_EMAIL/);
  });

  it('falls back to the placeholder only when explicitly allowed', () => {
    expect(buildUserAgent({ env: {}, allowPlaceholder: true })).toContain(CONTACT_PLACEHOLDER);
  });
});

describe('LegacyCharacterSource', () => {
  const query = { account: 'Ziggy D', character: 'Böulder' };

  it('builds the get-items URL with encoded params and the User-Agent', async () => {
    const { client, requests } = scripted({ status: 200, headers: {}, body: fixtures.items });
    await new LegacyCharacterSource(client, { userAgent: UA }).fetchItems(query);
    expect(requests[0]?.url).toBe(
      'https://www.pathofexile.com/character-window/get-items?accountName=Ziggy%20D&character=B%C3%B6ulder',
    );
    expect(requests[0]?.headers['user-agent']).toBe(UA);
  });

  it('maps a JSON body to ok and carries the parsed data', async () => {
    const { client } = scripted({ status: 200, headers: {}, body: fixtures.items });
    const { result } = await new LegacyCharacterSource(client, { userAgent: UA }).fetchItems(query);
    expect(result.kind).toBe('ok');
    if (result.kind === 'ok') {
      expect((result.data as { items: unknown[] }).items).toHaveLength(2);
    }
  });

  it('maps a 429 to rate_limited with a throttled signal (no dead payload)', async () => {
    const { client } = scripted({ status: 429, headers: { 'retry-after': '45' }, body: '' });
    const { result, observation } = await new LegacyCharacterSource(client, {
      userAgent: UA,
    }).fetchItems(query);
    expect(result).toEqual({ kind: 'rate_limited' });
    expect(observation.signal).toBe('throttled');
    // Retry-After now travels on the headers for the limiter to read.
    expect(observation.headers['retry-after']).toBe('45');
  });

  it('maps a 5xx to retryable with an error signal', async () => {
    const { client } = scripted(json(503, { error: { code: 5 } }));
    const { result, observation } = await new LegacyCharacterSource(client, {
      userAgent: UA,
    }).fetchItems(query);
    expect(result.kind).toBe('retryable');
    expect(observation.signal).toBe('error');
  });
});

describe('LegacyLadderSource', () => {
  it('parses ladder entries into the frozen-queue shape', async () => {
    const body = JSON.stringify({
      total: 2,
      entries: [
        {
          rank: 1,
          character: { name: 'A', level: 100, class: 'Juggernaut' },
          account: { name: 'acc1' },
        },
        {
          rank: 2,
          character: { name: 'B', level: 99, class: 'Necromancer' },
          account: { name: 'acc2' },
        },
      ],
    });
    const { client } = scripted({ status: 200, headers: {}, body });
    const { result } = await new LegacyLadderSource(client, { userAgent: UA }).fetchPage({
      league: 'TestLeague',
      offset: 0,
      limit: 200,
    });
    expect(result.kind).toBe('ok');
    if (result.kind === 'ok') {
      expect(result.total).toBe(2);
      expect(result.entries[0]).toEqual({
        rank: 1,
        account: 'acc1',
        character: 'A',
        class: 'Juggernaut',
        level: 100,
      });
    }
  });

  it('keys the league on `id` (not `league`) and sends the User-Agent', async () => {
    const body = JSON.stringify({ total: 0, entries: [] });
    const { client, requests } = scripted({ status: 200, headers: {}, body });
    await new LegacyLadderSource(client, { userAgent: UA }).fetchPage({
      league: 'Standard',
      offset: 0,
      limit: 200,
    });
    expect(requests[0]?.url).toBe(
      'https://www.pathofexile.com/api/ladders?id=Standard&offset=0&limit=200',
    );
    expect(requests[0]?.headers['user-agent']).toBe(UA);
  });

  it('treats a non-429 4xx on the ladder as fatal (misconfiguration, not a profile)', async () => {
    const { client } = scripted(json(404, { error: { code: 1 } }));
    const { result } = await new LegacyLadderSource(client, { userAgent: UA }).fetchPage({
      league: 'Nonexistent',
      offset: 0,
      limit: 200,
    });
    expect(result).toEqual({ kind: 'fatal', status: 404 });
  });
});
