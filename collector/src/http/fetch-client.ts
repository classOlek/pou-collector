/**
 * Production HttpClient backed by the global `fetch` (Node 22). This is the only
 * place the collector touches the network directly; everything above it speaks
 * through the HttpClient seam, so tests use the MockPoeApi and never hit GGG
 * (hard rules #1/#3). Header names are lowercased to match the seam contract.
 */
import type { HttpClient } from '../sources/types.js';

export function createFetchHttpClient(): HttpClient {
  return async ({ url, headers }) => {
    const res = await fetch(url, { headers, redirect: 'manual' });
    const lower: Record<string, string> = {};
    res.headers.forEach((value, key) => {
      lower[key.toLowerCase()] = value;
    });
    const body = await res.text();
    return { status: res.status, headers: lower, body };
  };
}
