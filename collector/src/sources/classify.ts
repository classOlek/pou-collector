/**
 * Response classification for the Cloudflare-fronted PoE edge.
 *
 * This is a property of the *edge*, not of any one endpoint, so it lives apart
 * from the legacy source and is reused verbatim by future sources (the Phase-7
 * OAuth source hits the same Cloudflare front). It owns two mappings:
 *   1. HTTP response → `Category` (what kind of response this is), and
 *   2. `Category` → `CharacterCallResult` kind + `RateSignal`.
 * The run layer owns the remaining policy (kind → CharacterOutcome).
 *
 * Ordering matters: a body that parses as JSON is authoritative and is *never*
 * treated as a Cloudflare challenge, so a player/account name containing
 * "cloudflare" cannot misclassify a good 200 (see classify.test.ts).
 */
import type { CharacterCallResult, HttpResponse, RateObservation, RateSignal } from './types.js';

/** Coarse response category, shared by ladder and character classification. */
export type Category =
  | { tag: 'json'; data: unknown }
  | { tag: 'challenge' } // Cloudflare HTML interstitial (non-JSON 2xx/4xx)
  | { tag: 'private' } // JSON error body on a 403
  | { tag: 'dead' } // 404
  | { tag: 'rate_limited' } // 429
  | { tag: 'server_error' } // 5xx or unexpected non-JSON, non-HTML body
  | { tag: 'client_error'; status: number }; // other JSON 4xx

export function looksLikeHtml(body: string): boolean {
  const head = body.trimStart().slice(0, 400).toLowerCase();
  return (
    head.startsWith('<') ||
    head.includes('<!doctype html') ||
    head.includes('cloudflare') ||
    head.includes('just a moment')
  );
}

/**
 * A genuine Cloudflare *edge* bot-challenge, as opposed to GGG's own HTML error
 * pages (a private profile is served as a 403 HTML page titled "Path of Exile",
 * which is NOT a challenge). Cloudflare stamps `cf-mitigated` on a challenged
 * response and its interstitial body carries the challenge-platform script; we
 * key off those, never off the mere presence of HTML or the word "cloudflare"
 * (every GGG response is Cloudflare-fronted).
 */
export function looksLikeChallenge(res: HttpResponse): boolean {
  if (res.headers['cf-mitigated']) return true;
  const head = res.body.trimStart().slice(0, 600).toLowerCase();
  return (
    head.includes('just a moment') ||
    head.includes('/cdn-cgi/challenge-platform') ||
    head.includes('cf-challenge') ||
    head.includes('__cf_chl')
  );
}

function tryJson(body: string): { ok: true; data: unknown } | { ok: false } {
  try {
    return { ok: true, data: JSON.parse(body) as unknown };
  } catch {
    return { ok: false };
  }
}

export function classifyResponse(res: HttpResponse): Category {
  const { status, body } = res;

  if (status === 429) return { tag: 'rate_limited' };
  if (status >= 500) return { tag: 'server_error' };

  // Valid JSON is authoritative — decide purely on status, never sniff it.
  const json = tryJson(body);
  if (json.ok) {
    if (status === 404) return { tag: 'dead' };
    if (status === 403) return { tag: 'private' }; // JSON error => private profile
    if (status >= 400) return { tag: 'client_error', status };
    return { tag: 'json', data: json.data };
  }

  // Non-JSON body. A genuine Cloudflare edge challenge is authoritative and
  // abort-worthy; check it before status, since it can ride on a 403/503.
  if (looksLikeChallenge(res)) return { tag: 'challenge' };
  // GGG serves error pages as HTML, not JSON: a 404 is a missing/renamed
  // character (dead), and a 403 is a private/forbidden profile (private) — the
  // same meaning as the JSON forms above. Classifying the 403 HTML as a
  // `challenge` (the old fall-through) was fatal: a run of private profiles on
  // the ladder tripped the throttle-abort threshold and killed the snapshot.
  if (status === 404) return { tag: 'dead' };
  if (status === 403) return { tag: 'private' };
  // Any other unexpected HTML/truncated body is a transient edge failure.
  if (looksLikeHtml(body)) return { tag: 'challenge' };
  return { tag: 'server_error' };
}

export function signalOf(category: Category): RateSignal {
  switch (category.tag) {
    case 'rate_limited':
      return 'throttled';
    case 'challenge':
      return 'challenge';
    case 'server_error':
    case 'client_error':
      return 'error';
    default:
      return 'ok';
  }
}

export function observationFor(res: HttpResponse, category: Category): RateObservation {
  return { status: res.status, headers: res.headers, signal: signalOf(category) };
}

/** Map an edge category to the character-source result kind (the swap seam). */
export function toCharacterResult(category: Category): CharacterCallResult {
  switch (category.tag) {
    case 'json':
      return { kind: 'ok', data: category.data };
    case 'private':
      return { kind: 'private' };
    case 'dead':
      return { kind: 'dead' };
    case 'rate_limited':
      return { kind: 'rate_limited' };
    case 'challenge':
      return { kind: 'retryable', reason: 'challenge' };
    case 'server_error':
      return { kind: 'retryable', reason: 'server' };
    case 'client_error':
      return { kind: 'retryable', reason: `http-${category.status}` };
  }
}
