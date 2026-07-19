import { describe, expect, it } from 'vitest';
import {
  activeRestrictionSec,
  deriveWindows,
  parseRateLimitRules,
  parseRetryAfterMs,
  parseTuples,
} from './headers.js';

describe('parseTuples', () => {
  it('parses hits:period:penalty tuples', () => {
    expect(parseTuples('5:10:60,15:60:120')).toEqual([
      { hits: 5, periodSec: 10, penaltySec: 60 },
      { hits: 15, periodSec: 60, penaltySec: 120 },
    ]);
  });

  it('drops degenerate tuples (zero/NaN hits or period) so the limiter fails safe', () => {
    // '0:60:300' would otherwise imply rate 0 → infinite-speed acquire.
    expect(parseTuples('0:60:300, x:10:5, 8:10:60, 5:0:0')).toEqual([
      { hits: 8, periodSec: 10, penaltySec: 60 },
    ]);
  });
});

describe('parseRateLimitRules', () => {
  it('associates each named rule with its limit and state headers', () => {
    expect(
      parseRateLimitRules({
        'x-rate-limit-rules': 'Ip,Account',
        'x-rate-limit-ip': '8:10:60',
        'x-rate-limit-ip-state': '1:10:0',
        'x-rate-limit-account': '20:60:120',
        'x-rate-limit-account-state': '3:60:0',
      }),
    ).toEqual([
      {
        name: 'Ip',
        limits: [{ hits: 8, periodSec: 10, penaltySec: 60 }],
        state: [{ hits: 1, periodSec: 10, penaltySec: 0 }],
      },
      {
        name: 'Account',
        limits: [{ hits: 20, periodSec: 60, penaltySec: 120 }],
        state: [{ hits: 3, periodSec: 60, penaltySec: 0 }],
      },
    ]);
  });

  it('returns no rules when the header is absent', () => {
    expect(parseRateLimitRules({})).toEqual([]);
  });
});

describe('deriveWindows', () => {
  it('yields one capped window per limit tuple, preserving each horizon (golden)', () => {
    // The real GGG character-endpoint header: three independent windows.
    const rules = parseRateLimitRules({
      'x-rate-limit-rules': 'Ip',
      'x-rate-limit-ip': '30:60:120,90:1800:600,180:7200:3600',
    });
    expect(deriveWindows(rules, 0.9)).toEqual([
      { cap: 27, periodMs: 60_000 }, // floor(30*0.9)
      { cap: 81, periodMs: 1_800_000 }, // floor(90*0.9)
      { cap: 162, periodMs: 7_200_000 }, // floor(180*0.9) — a quota, not a per-request rate
    ]);
  });

  it('unions windows across every rule and clamps the cap to >= 1', () => {
    const rules = parseRateLimitRules({
      'x-rate-limit-rules': 'Ip,Account',
      'x-rate-limit-ip': '1:60:60', // floor(1*0.9) = 0 → clamped to 1
      'x-rate-limit-account': '20:60:120',
    });
    expect(deriveWindows(rules, 0.9)).toEqual([
      { cap: 1, periodMs: 60_000 },
      { cap: 18, periodMs: 60_000 },
    ]);
  });

  it('yields no windows when nothing is observed', () => {
    expect(deriveWindows([], 0.9)).toEqual([]);
  });
});

describe('activeRestrictionSec', () => {
  it('reports the largest active penalty across rule states, else 0', () => {
    const restricted = parseRateLimitRules({
      'x-rate-limit-rules': 'Ip',
      'x-rate-limit-ip': '30:60:120',
      'x-rate-limit-ip-state': '30:60:45', // being penalized: 45s active
    });
    expect(activeRestrictionSec(restricted)).toBe(45);

    const clear = parseRateLimitRules({
      'x-rate-limit-rules': 'Ip',
      'x-rate-limit-ip': '30:60:120',
      'x-rate-limit-ip-state': '1:60:0',
    });
    expect(activeRestrictionSec(clear)).toBe(0);
  });
});

describe('parseRetryAfterMs', () => {
  it('parses the delta-seconds form', () => {
    expect(parseRetryAfterMs({ 'retry-after': '30' }, 0)).toBe(30_000);
  });

  it('parses the HTTP-date form relative to now (RFC 9110 / Cloudflare)', () => {
    const now = Date.parse('2026-07-17T00:00:00.000Z');
    const future = new Date(now + 45_000).toUTCString();
    expect(parseRetryAfterMs({ 'retry-after': future }, now)).toBe(45_000);
  });

  it('ignores a past date and a missing header', () => {
    const now = Date.parse('2026-07-17T00:00:00.000Z');
    expect(
      parseRetryAfterMs({ 'retry-after': new Date(now - 1000).toUTCString() }, now),
    ).toBeUndefined();
    expect(parseRetryAfterMs({}, now)).toBeUndefined();
  });
});
