/**
 * ECONOMY LEAGUE SELECTION: resolve which leagues the poe.ninja economy cache
 * should cover from the Supabase leagues endpoint, replacing the former
 * hardcoded ['Mirage','Standard'] list (poe-ninja.ts ECONOMY_LEAGUES).
 *
 * Selection rule: every ACTIVE TEMPORARY league — a challenge league, i.e. its
 * category is not "Standard", it has started (start_at ≤ now), and it has not
 * ended (end_at is null or in the future) — plus the permanent "Standard"
 * league, whose economy is always worth caching. Between leagues (no challenge
 * running) this collapses to just ['Standard'].
 *
 * The endpoint is read with the service_role key, which bypasses RLS — so no
 * table GRANT or public-read policy is needed and the anon key stays locked
 * out. The target resource URL is resolved from LEAGUES_ENDPOINT (a full,
 * query-less resource URL, e.g. https://<ref>.supabase.co/rest/v1/leagues) or,
 * failing that, from the SUPABASE_URL / SUPABASE_PROJECT_REF the rest of the
 * repo already uses. The project is never baked into this public repo; a
 * missing target fails loudly.
 */
import type { HttpClient } from '../sources/types.js';

/** The subset of a leagues row this module reads (see shared schema). */
export interface LeagueRow {
  id: string;
  category_id: string | null;
  start_at: string | null;
  end_at: string | null;
}

export type LeaguesEnv = Record<string, string | undefined>;

/**
 * Resolve the bare leagues resource URL (no query string — the caller appends
 * select/filters). Precedence: LEAGUES_ENDPOINT → SUPABASE_URL → project ref.
 */
export function resolveLeaguesEndpoint(env: LeaguesEnv): string {
  const explicit = env.LEAGUES_ENDPOINT?.trim();
  if (explicit) return explicit.replace(/\/+$/, '');
  const base = env.SUPABASE_URL?.trim();
  if (base) return `${base.replace(/\/+$/, '')}/rest/v1/leagues`;
  const ref = env.SUPABASE_PROJECT_REF?.trim();
  if (ref) return `https://${ref}.supabase.co/rest/v1/leagues`;
  throw new Error(
    'Missing leagues endpoint: set LEAGUES_ENDPOINT, or SUPABASE_URL / SUPABASE_PROJECT_REF.',
  );
}

function requiredEnv(env: LeaguesEnv, key: string): string {
  const value = env[key];
  if (value === undefined || value.trim() === '') {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

/**
 * Pick the economy leagues from raw rows: active temporary leagues plus the
 * always-present Standard, deduped, Standard last. `now` is millis-since-epoch.
 */
export function selectEconomyLeagues(rows: LeagueRow[], now: number): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const row of rows) {
    if ((row.category_id ?? '') === 'Standard') continue; // permanent, handled below
    if (row.start_at == null || Date.parse(row.start_at) > now) continue; // not started
    if (row.end_at != null && Date.parse(row.end_at) <= now) continue; // already ended
    if (!seen.has(row.id)) {
      seen.add(row.id);
      out.push(row.id);
    }
  }
  if (!seen.has('Standard')) out.push('Standard');
  return out;
}

export interface FetchLeaguesDeps {
  http: HttpClient;
  /** Millis-since-epoch used to judge start_at/end_at (injected for tests). */
  now: () => number;
}

/**
 * Fetch the leagues from the endpoint and return the economy league list. Any
 * non-200, non-array, or unparseable response throws — the economy run should
 * fail loudly (its alert job fires) rather than silently cache the wrong set.
 */
export async function fetchEconomyLeagues(
  env: LeaguesEnv,
  deps: FetchLeaguesDeps,
): Promise<string[]> {
  const key = requiredEnv(env, 'SUPABASE_SERVICE_ROLE_KEY');
  const endpoint = resolveLeaguesEndpoint(env);
  const url = `${endpoint}?select=id,category_id,start_at,end_at`;

  const res = await deps.http({
    url,
    headers: { apikey: key, authorization: `Bearer ${key}` },
  });
  if (res.status !== 200) {
    throw new Error(`leagues endpoint returned HTTP ${res.status}: ${res.body.slice(0, 200)}`);
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(res.body);
  } catch {
    throw new Error('leagues endpoint returned invalid JSON');
  }
  if (!Array.isArray(parsed)) {
    throw new Error('leagues endpoint returned a non-array body');
  }
  return selectEconomyLeagues(parsed as LeagueRow[], deps.now());
}
