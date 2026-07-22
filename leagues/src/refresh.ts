/**
 * LEAGUES REFRESH: fetch the Path of Exile "main" leagues and upsert them into
 * Supabase.
 *
 * This is the external replacement for the former in-database refresh pipeline
 * that lived in classOlek/olsCloud-supabase (pg_cron + pg_net). The job runs in
 * GitHub Actions (dispatched by the Cloudflare Worker cron in
 * classOlek/olsCloud-scheduler) and its only Supabase contact is a single URL:
 * it POSTs the raw API array to the public.upsert_leagues(payload jsonb) RPC,
 * which still owns the flatten/reshape/upsert transform inside the database.
 *
 * Everything here is pure and network-injected (see RefreshDeps) so the test
 * suite exercises it offline against a fake fetch — no GGG or Supabase traffic
 * in CI, matching the repo's mock-and-fixtures discipline. The real fetch,
 * timers and env are wired only in cli.ts.
 */

/** Minimal structural subset of the global `fetch` needed by this module. */
export type FetchLike = (
  url: string,
  init?: {
    method?: string;
    headers?: Record<string, string>;
    body?: string;
  },
) => Promise<FetchResponse>;

export interface FetchResponse {
  ok: boolean;
  status: number;
  json(): Promise<unknown>;
  text(): Promise<string>;
}

export interface RefreshDeps {
  fetch: FetchLike;
  sleep: (ms: number) => Promise<void>;
  log: (message: string) => void;
}

export interface RefreshEnv {
  /** Base Supabase URL, no trailing slash, e.g. https://<ref>.supabase.co */
  supabaseUrl: string;
  /** service_role key — the only role granted execute on upsert_leagues. */
  serviceRoleKey: string;
  /** Reachable contact address, embedded in the User-Agent (hard rule #1). */
  contactEmail: string;
}

export const POE_LEAGUES_URL = 'https://api.pathofexile.com/leagues?type=main&realm=pc';

const MAX_ATTEMPTS = 4;

/**
 * GGG's API policy requires an identifiable User-Agent naming the app with a
 * reachable contact (the same discipline as hard rule #1 elsewhere in this
 * repo); Cloudflare may 403 generic ones.
 */
export function userAgent(contactEmail: string): string {
  return `olsCloud-leagues-refresh (+https://github.com/classOlek/olsCloud-workers; contact: ${contactEmail})`;
}

/** Resolve and validate the runtime environment. Throws on missing secrets. */
export function resolveEnv(source: Record<string, string | undefined>): RefreshEnv {
  const serviceRoleKey = required(source, 'SUPABASE_SERVICE_ROLE_KEY');
  const contactEmail = required(source, 'COLLECTOR_CONTACT_EMAIL');
  const supabaseUrl = resolveSupabaseUrl(source);
  return { supabaseUrl, serviceRoleKey, contactEmail };
}

/**
 * Resolve the Supabase base URL from configuration only — either SUPABASE_URL
 * (full base URL) or SUPABASE_PROJECT_REF (the project ref). The target project
 * is never baked into this public repo; a missing target fails loudly. The
 * project ref/URL is public (not a secret), but it must come from env/vars so
 * this repo does not point at a specific project on its own.
 */
function resolveSupabaseUrl(source: Record<string, string | undefined>): string {
  const explicit = source.SUPABASE_URL?.trim();
  if (explicit) return explicit.replace(/\/+$/, '');
  const ref = source.SUPABASE_PROJECT_REF?.trim();
  if (ref) return `https://${ref}.supabase.co`;
  throw new Error('Missing Supabase target: set SUPABASE_URL or SUPABASE_PROJECT_REF.');
}

function required(source: Record<string, string | undefined>, key: string): string {
  const value = source[key];
  if (value === undefined || value === '') {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

class HttpError extends Error {
  readonly status: number | undefined;
  readonly fatal: boolean;

  constructor(message: string, status: number | undefined, fatal?: boolean) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
    // 4xx other than 429 will not succeed on retry; fail fast.
    this.fatal = fatal ?? (status !== undefined && status >= 400 && status < 500 && status !== 429);
  }
}

function messageOf(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Run `fn` with exponential backoff (1s, 2s, 4s). Retries transient failures
 * (network errors, 5xx, 429) up to MAX_ATTEMPTS; a fatal HttpError (4xx≠429)
 * stops immediately.
 */
async function withRetries<T>(
  label: string,
  deps: RefreshDeps,
  fn: (attempt: number) => Promise<T>,
): Promise<T> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      return await fn(attempt);
    } catch (err) {
      lastError = err;
      if ((err instanceof HttpError && err.fatal) || attempt === MAX_ATTEMPTS) {
        break;
      }
      const delay = 1000 * 2 ** (attempt - 1);
      deps.log(`${label}: attempt ${attempt} failed (${messageOf(err)}); retrying in ${delay}ms`);
      await deps.sleep(delay);
    }
  }
  throw lastError;
}

/** Fetch the "main" leagues array from the PoE API. */
export async function fetchLeagues(deps: RefreshDeps, env: RefreshEnv): Promise<unknown[]> {
  return withRetries('fetch leagues', deps, async () => {
    const res = await deps.fetch(POE_LEAGUES_URL, {
      headers: { 'User-Agent': userAgent(env.contactEmail), Accept: 'application/json' },
    });
    if (!res.ok) {
      throw new HttpError(`PoE API returned HTTP ${res.status}`, res.status);
    }
    const payload = await res.json();
    if (!Array.isArray(payload)) {
      // A non-array body is a contract violation, not a transient blip.
      throw new HttpError('PoE API returned a non-array body', undefined, true);
    }
    return payload;
  });
}

/** POST the payload to the upsert_leagues RPC; returns the rows-written count. */
export async function upsertLeagues(
  deps: RefreshDeps,
  env: RefreshEnv,
  payload: unknown[],
): Promise<number> {
  return withRetries('upsert leagues', deps, async () => {
    const res = await deps.fetch(`${env.supabaseUrl}/rest/v1/rpc/upsert_leagues`, {
      method: 'POST',
      headers: {
        apikey: env.serviceRoleKey,
        Authorization: `Bearer ${env.serviceRoleKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ payload }),
    });
    if (!res.ok) {
      const detail = await res.text().catch(() => '');
      throw new HttpError(`upsert_leagues RPC returned HTTP ${res.status}: ${detail}`, res.status);
    }
    // upsert_leagues() returns the integer count of rows written.
    const count = await res.json();
    return typeof count === 'number' ? count : Number(count);
  });
}

/** Fetch the leagues and upsert them; returns the rows-written count. */
export async function runRefresh(deps: RefreshDeps, env: RefreshEnv): Promise<number> {
  const payload = await fetchLeagues(deps, env);
  deps.log(`Fetched ${payload.length} leagues from the PoE API.`);
  const upserted = await upsertLeagues(deps, env, payload);
  deps.log(`upsert_leagues wrote ${upserted} rows.`);
  return upserted;
}
