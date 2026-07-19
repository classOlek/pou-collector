/**
 * Collector configuration derived from the environment.
 *
 * Hard rule #1: every request to GGG carries an identifiable User-Agent with a
 * reachable contact address. The contact is read from the environment so a real
 * address is never committed. A missing/blank contact is a hard error in
 * production — silently shipping an unreachable placeholder would violate the
 * identifiability rule — so the placeholder is opt-in for tests/offline use.
 */

export const CONTACT_PLACEHOLDER = 'contact@example.invalid';

export interface CollectorEnv {
  COLLECTOR_CONTACT_EMAIL?: string | undefined;
}

export interface BuildUserAgentOptions {
  env?: CollectorEnv;
  /** Allow falling back to the fake placeholder contact (tests/offline only). */
  allowPlaceholder?: boolean;
}

/**
 * Build the User-Agent string, e.g. `poe-ladder-stats/0.1 (+you@example.com)`.
 *
 * The product token is intentionally a neutral, functional name — it must NOT
 * leak the project/repo name to GGG. Identifiability (hard rule #1) is carried
 * by the reachable contact address, not by the product token.
 */
export function buildUserAgent(options: BuildUserAgentOptions = {}): string {
  const env = options.env ?? process.env;
  const contact = env.COLLECTOR_CONTACT_EMAIL?.trim();
  if (contact) return `poe-ladder-stats/0.1 (+${contact})`;
  if (options.allowPlaceholder) return `poe-ladder-stats/0.1 (+${CONTACT_PLACEHOLDER})`;
  throw new Error(
    'COLLECTOR_CONTACT_EMAIL is required so GGG requests are identifiable (hard rule #1). ' +
      'Set the env var, or pass { allowPlaceholder: true } for offline/test use.',
  );
}
