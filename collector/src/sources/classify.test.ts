import { describe, expect, it } from 'vitest';
import { fixtures } from '../../test/mock-api.js';
import { classifyResponse, signalOf, toCharacterResult } from './classify.js';
import type { HttpResponse } from './types.js';

const res = (status: number, body: string, headers: Record<string, string> = {}): HttpResponse => ({
  status,
  headers,
  body,
});

describe('classifyResponse', () => {
  it('classifies a JSON 200 as json/ok', () => {
    const c = classifyResponse(res(200, fixtures.items));
    expect(c.tag).toBe('json');
    expect(signalOf(c)).toBe('ok');
  });

  it('classifies a 403 JSON error body as a private profile (ok signal)', () => {
    const c = classifyResponse(res(403, fixtures.privateError));
    expect(c.tag).toBe('private');
    expect(signalOf(c)).toBe('ok'); // private is a normal outcome, not a danger signal
  });

  it('classifies a 403 HTML profile page as a private profile, not a challenge', () => {
    // GGG serves private profiles as a 403 HTML page (not JSON). Misclassifying
    // it as a challenge tripped the throttle-abort threshold and killed runs.
    const c = classifyResponse(res(403, fixtures.profileForbidden));
    expect(c.tag).toBe('private');
    expect(signalOf(c)).toBe('ok'); // private is a normal outcome, never a danger signal
  });

  it('still treats a genuine Cloudflare challenge on a 403 as a challenge', () => {
    // The "Just a moment" interstitial body — abort-worthy even on a 403.
    expect(classifyResponse(res(403, fixtures.challenge)).tag).toBe('challenge');
    // And a cf-mitigated header marks a challenge regardless of body.
    const c = classifyResponse(
      res(403, fixtures.profileForbidden, { 'cf-mitigated': 'challenge' }),
    );
    expect(c.tag).toBe('challenge');
  });

  it('classifies a 404 as dead', () => {
    expect(classifyResponse(res(404, '{"error":{}}')).tag).toBe('dead');
    expect(classifyResponse(res(404, '')).tag).toBe('dead'); // even with a non-JSON body
    expect(classifyResponse(res(404, fixtures.profileForbidden)).tag).toBe('dead'); // HTML 404 too
  });

  it('classifies a 429 as rate_limited/throttled', () => {
    const c = classifyResponse(res(429, ''));
    expect(c.tag).toBe('rate_limited');
    expect(signalOf(c)).toBe('throttled');
  });

  it('classifies a 5xx as a server_error/error signal', () => {
    expect(signalOf(classifyResponse(res(503, '{"error":{}}')))).toBe('error');
  });

  it('classifies a Cloudflare HTML body as a challenge', () => {
    const c = classifyResponse(res(200, fixtures.challenge));
    expect(c.tag).toBe('challenge');
    expect(signalOf(c)).toBe('challenge');
  });

  it('classifies a non-JSON, non-HTML body as a transient error, not a challenge', () => {
    expect(signalOf(classifyResponse(res(200, 'oops truncated')))).toBe('error');
  });

  it('trusts valid JSON even when a field contains "Cloudflare" (JSON parsed first)', () => {
    // A player/account name mentioning cloudflare must NOT trip the HTML sniff.
    const body = JSON.stringify({ items: [], character: { name: 'Just a moment Cloudflare' } });
    const c = classifyResponse(res(200, body));
    expect(c.tag).toBe('json');
    expect(signalOf(c)).toBe('ok');
  });
});

describe('toCharacterResult', () => {
  it('maps categories to character result kinds', () => {
    expect(toCharacterResult(classifyResponse(res(200, fixtures.items))).kind).toBe('ok');
    expect(toCharacterResult(classifyResponse(res(403, fixtures.privateError))).kind).toBe(
      'private',
    );
    expect(toCharacterResult(classifyResponse(res(404, ''))).kind).toBe('dead');
    expect(toCharacterResult(classifyResponse(res(429, ''))).kind).toBe('rate_limited');
    expect(toCharacterResult(classifyResponse(res(503, '{}'))).kind).toBe('retryable');
    expect(toCharacterResult(classifyResponse(res(200, fixtures.challenge))).kind).toBe(
      'retryable',
    );
  });
});
