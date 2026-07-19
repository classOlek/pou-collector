import { describe, expect, it } from 'vitest';
import { discoverPublicIp, type FetchLike } from './public-ip.js';

const ok = (body: string): ReturnType<FetchLike> =>
  Promise.resolve({ ok: true, text: () => Promise.resolve(body) });

describe('discoverPublicIp (fail-safe echo lookup)', () => {
  it('returns the trimmed IP from the first healthy echo service', async () => {
    const ip = await discoverPublicIp(() => ok('203.0.113.7\n'));
    expect(ip).toBe('203.0.113.7');
  });

  it('falls through errors, non-ok responses and junk bodies to the next echo', async () => {
    const calls: string[] = [];
    const fetchFn: FetchLike = (url) => {
      calls.push(url);
      if (calls.length === 1) return Promise.reject(new Error('unreachable'));
      return ok('2001:db8::1234');
    };
    expect(await discoverPublicIp(fetchFn, 5_000, ['https://a.test/', 'https://b.test/'])).toBe(
      '2001:db8::1234',
    );
    expect(calls).toEqual(['https://a.test/', 'https://b.test/']);

    const notOk: FetchLike = () =>
      Promise.resolve({ ok: false, text: () => Promise.resolve('503') });
    expect(await discoverPublicIp(notOk)).toBeUndefined();

    const junk: FetchLike = () => ok('<html>error page</html>');
    expect(await discoverPublicIp(junk)).toBeUndefined();
  });

  it('never throws — total failure yields undefined (callers keep conservative state)', async () => {
    const boom: FetchLike = () => Promise.reject(new Error('no egress'));
    await expect(discoverPublicIp(boom)).resolves.toBeUndefined();
  });
});
