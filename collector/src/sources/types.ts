/**
 * Data-source contracts (hard rule #7: all GGG traffic goes through swappable
 * `LadderSource` / `CharacterSource` interfaces, so the legacy endpoints can be
 * replaced by the official OAuth ladder endpoint without touching callers).
 *
 * Everything below the interfaces speaks HTTP through an injected `HttpClient`
 * seam, never the global `fetch`, so tests never reach the network.
 */

export interface HttpRequest {
  url: string;
  headers: Record<string, string>;
}

export interface HttpResponse {
  status: number;
  /** Header names are lowercased by the client adapter. */
  headers: Record<string, string>;
  body: string;
}

export type HttpClient = (req: HttpRequest) => Promise<HttpResponse>;

/**
 * Body-derived rate signal a source hands to the limiter, which cannot read
 * bodies itself (docs/ARCHITECTURE.md §7). Two distinct danger classes so the
 * limiter can react proportionately:
 *   - `throttled`  = 429 (rate-block danger; hard, low abort threshold)
 *   - `challenge`  = Cloudflare HTML interstitial (same danger class as throttle)
 *   - `error`      = 5xx / unexpected non-JSON (transient-failure danger; gentle
 *                    backoff, high abort threshold — blips must not abort)
 *   - `ok`         = a valid app response, including a private-profile error
 *                    (normal outcome, resets both danger streaks)
 */
export type RateSignal = 'ok' | 'throttled' | 'challenge' | 'error';

/** What the limiter needs from one response to pace and to decide aborts. */
export interface RateObservation {
  status: number;
  headers: Record<string, string>;
  signal: RateSignal;
}

/** One frozen ladder entry (a slot in the work queue). */
export interface LadderEntry {
  rank: number;
  account: string;
  character: string;
  class: string;
  level: number;
}

export type LadderResult =
  | { kind: 'ok'; entries: LadderEntry[]; total: number }
  // Retry-After and any observed limits travel on the observation, not here.
  | { kind: 'rate_limited' }
  | { kind: 'retryable'; reason: string }
  | { kind: 'fatal'; status: number };

/** Outcome of a single character sub-call (get-items or get-passive-skills). */
export type CharacterCallResult =
  | { kind: 'ok'; data: unknown }
  | { kind: 'private' }
  | { kind: 'dead' }
  | { kind: 'rate_limited' }
  | { kind: 'retryable'; reason: string };

/** Sources pair a domain result with the observation fed back to the limiter. */
export interface Observed<T> {
  result: T;
  observation: RateObservation;
}

export interface LadderQuery {
  league: string;
  offset: number;
  limit: number;
}

export interface CharacterQuery {
  account: string;
  character: string;
}

export interface LadderSource {
  fetchPage(query: LadderQuery): Promise<Observed<LadderResult>>;
}

export interface CharacterSource {
  fetchItems(query: CharacterQuery): Promise<Observed<CharacterCallResult>>;
  fetchPassives(query: CharacterQuery): Promise<Observed<CharacterCallResult>>;
}
