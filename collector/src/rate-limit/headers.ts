/**
 * Parsing of GGG's `X-Rate-Limit-*` headers and `Retry-After`.
 *
 * Shape (from observed responses): a `X-Rate-Limit-Rules` header names the
 * active rules, and for each rule `<name>` there is a limit header and a state
 * header, each a comma-separated list of `hits:period:penalty` tuples:
 *
 *   X-Rate-Limit-Rules:        Ip
 *   X-Rate-Limit-Ip:           8:10:60,15:60:120
 *   X-Rate-Limit-Ip-State:     1:10:0,1:60:0
 *
 * A limit tuple = at most `hits` requests per `period` seconds, else a
 * `penalty`-second restriction. A state tuple = current `hits` used in that
 * window and the currently-active restriction seconds (3rd field).
 */
import type { RateLimitRule, RateLimitTuple } from '@classolek/shared';

function toInt(raw: string | undefined): number {
  const n = Number.parseInt((raw ?? '').trim(), 10);
  return Number.isFinite(n) ? n : 0;
}

export function parseTuples(value: string): RateLimitTuple[] {
  return value
    .split(',')
    .map((part) => part.trim())
    .filter((part) => part.length > 0)
    .map((part) => {
      const [hits, periodSec, penaltySec] = part.split(':');
      return { hits: toInt(hits), periodSec: toInt(periodSec), penaltySec: toInt(penaltySec) };
    })
    .filter((t) => t.hits > 0 && t.periodSec > 0); // degenerate tuples fail open — drop them
}

export function parseRateLimitRules(headers: Record<string, string>): RateLimitRule[] {
  const rulesHeader = headers['x-rate-limit-rules'];
  if (!rulesHeader) return [];

  const names = rulesHeader
    .split(',')
    .map((n) => n.trim())
    .filter((n) => n.length > 0);

  const rules: RateLimitRule[] = [];
  for (const name of names) {
    const key = name.toLowerCase();
    const limitHeader = headers[`x-rate-limit-${key}`];
    const stateHeader = headers[`x-rate-limit-${key}-state`];
    if (limitHeader === undefined) continue;
    rules.push({
      name,
      limits: parseTuples(limitHeader),
      state: stateHeader ? parseTuples(stateHeader) : [],
    });
  }
  return rules;
}

/**
 * Honor `Retry-After` in both forms RFC 9110 allows: delta-seconds and an
 * HTTP-date (Cloudflare sends the date form). Returns milliseconds to wait from
 * `nowMs`, or undefined if absent/unparseable/in the past.
 */
export function parseRetryAfterMs(
  headers: Record<string, string>,
  nowMs: number,
): number | undefined {
  const raw = headers['retry-after'];
  if (raw === undefined) return undefined;
  const trimmed = raw.trim();

  if (/^\d+$/.test(trimmed)) {
    return Number.parseInt(trimmed, 10) * 1000;
  }
  const dateMs = Date.parse(trimmed);
  if (Number.isNaN(dateMs)) return undefined;
  const delta = dateMs - nowMs;
  return delta > 0 ? delta : undefined;
}

/**
 * One pacing window: at most `cap` requests may fall within any `periodMs`-long
 * sliding span. GGG advertises several of these per rule (e.g. 30/60s, 90/30m,
 * 180/2h) and enforces them independently, so we model each one independently
 * rather than collapsing them into a single average rate.
 */
export interface PaceWindow {
  cap: number;
  periodMs: number;
}

/**
 * Turn observed rules into pacing windows — one per limit tuple, across every
 * rule (a request must satisfy them all). `cap` is the tuple's hit count minus
 * `headroom` slack, floored, and clamped to >= 1.
 *
 * Unlike the old single-rate budget, this preserves each window's own horizon:
 * a long-horizon *quota* (180 requests / 2 h) stays a quota you may burst
 * against, instead of being flattened into an instantaneous ~1-request-per-44s
 * rate that throttled even a 20-character run to a crawl.
 */
export function deriveWindows(rules: RateLimitRule[], headroom: number): PaceWindow[] {
  const windows: PaceWindow[] = [];
  for (const rule of rules) {
    for (const t of rule.limits) {
      windows.push({
        cap: Math.max(1, Math.floor(t.hits * headroom)),
        periodMs: t.periodSec * 1000,
      });
    }
  }
  return windows;
}

/**
 * Largest currently-active restriction (seconds) any rule's *state* reports —
 * GGG puts the penalty countdown in the third field of the state tuple while
 * you are being limited. 0 when nothing is restricted.
 */
export function activeRestrictionSec(rules: RateLimitRule[]): number {
  let sec = 0;
  for (const rule of rules) {
    for (const s of rule.state) sec = Math.max(sec, s.penaltySec);
  }
  return sec;
}
