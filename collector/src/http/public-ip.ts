/**
 * Best-effort discovery of this runner's public IP, used to scope the rate
 * limiter's pace state (RateLimiter.adoptIp): GGG enforces its windows per IP
 * and every GitHub-hosted runner gets a fresh one, so a checkpointed spend from
 * a previous run's IP must not throttle this run.
 *
 * Fail-safe by construction: any error, timeout or implausible body yields
 * `undefined`, which callers treat as "unknown IP — keep the conservative pace
 * state". Discovery must never fail a run or delay it meaningfully (short
 * timeout, two independent echo services).
 */

/** Minimal fetch seam so tests never touch the network. */
export type FetchLike = (
  url: string,
  init: { signal: AbortSignal },
) => Promise<{ ok: boolean; text(): Promise<string> }>;

const IP_ECHO_URLS = ['https://checkip.amazonaws.com/', 'https://api.ipify.org/'];

/** Loose IPv4/IPv6 shape check — guards against an echo service returning an
 *  error page, not against spoofing (a wrong IP only mis-scopes pace state). */
const IP_SHAPE = /^[0-9a-fA-F.:]{3,45}$/;

export async function discoverPublicIp(
  fetchFn: FetchLike = fetch,
  timeoutMs = 5_000,
  urls: string[] = IP_ECHO_URLS,
): Promise<string | undefined> {
  for (const url of urls) {
    try {
      const res = await fetchFn(url, { signal: AbortSignal.timeout(timeoutMs) });
      if (!res.ok) continue;
      const ip = (await res.text()).trim();
      if (IP_SHAPE.test(ip)) return ip;
    } catch {
      // Unreachable service / timeout — try the next echo, then give up.
    }
  }
  return undefined;
}
