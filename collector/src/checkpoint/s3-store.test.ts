import { describe, expect, it, vi } from 'vitest';
import {
  INDEX_PATH,
  checkpointPath,
  rawChunkShardPath,
  snapshotAggPath,
  snapshotDetailPath,
  snapshotMetaPath,
} from '@pou/shared';
import { contentTypeForKey, isTransientS3Error, retryTransient } from './s3-store.js';

describe('contentTypeForKey', () => {
  it('serves published JSON files as JSON so the web app accepts them', () => {
    // The web repository rejects any data file whose content-type lacks `json`
    // (NotJsonError) — index/meta/agg must all carry a JSON type.
    for (const key of [
      INDEX_PATH,
      snapshotMetaPath('Settlers of Kalguur', 's-1'),
      snapshotAggPath('Settlers of Kalguur', 's-1', 'classes'),
      checkpointPath('Settlers of Kalguur'),
    ]) {
      expect(contentTypeForKey(key)).toContain('json');
    }
  });

  it('types detail Parquet and gzipped raw shards distinctly', () => {
    expect(contentTypeForKey(snapshotDetailPath('L', 's-1', 'characters'))).toBe(
      'application/vnd.apache.parquet',
    );
    expect(contentTypeForKey(rawChunkShardPath('L', 's-1', 3, 0))).toBe('application/gzip');
  });

  it('falls back to octet-stream for unknown keys', () => {
    expect(contentTypeForKey('something/opaque.bin')).toBe('application/octet-stream');
  });
});

/** A no-op sleep so retry tests don't wait on real backoff timers. */
const noSleep = () => Promise.resolve();

describe('isTransientS3Error', () => {
  it('retries R2 5xx / InternalError blips', () => {
    // The exact shape the collect runs failed on: R2 InternalError, HTTP 500.
    expect(isTransientS3Error({ name: 'InternalError', $metadata: { httpStatusCode: 500 } })).toBe(
      true,
    );
    expect(isTransientS3Error({ $metadata: { httpStatusCode: 503 } })).toBe(true);
  });

  it('retries throttling (429 / SlowDown) and dropped connections', () => {
    expect(isTransientS3Error({ $metadata: { httpStatusCode: 429 } })).toBe(true);
    expect(isTransientS3Error({ name: 'SlowDown' })).toBe(true);
    expect(isTransientS3Error({ code: 'ECONNRESET' })).toBe(true);
  });

  it('does not retry deterministic 4xx client errors', () => {
    expect(isTransientS3Error({ name: 'NoSuchKey', $metadata: { httpStatusCode: 404 } })).toBe(
      false,
    );
    expect(isTransientS3Error({ name: 'AccessDenied', $metadata: { httpStatusCode: 403 } })).toBe(
      false,
    );
    expect(isTransientS3Error(null)).toBe(false);
  });
});

describe('retryTransient', () => {
  const transient = () =>
    Object.assign(new Error('boom'), { name: 'InternalError', $metadata: { httpStatusCode: 500 } });

  it('rides out a transient blip and returns the eventual success', async () => {
    const fn = vi
      .fn<() => Promise<string>>()
      .mockRejectedValueOnce(transient())
      .mockRejectedValueOnce(transient())
      .mockResolvedValue('ok');
    const out = await retryTransient(fn, { sleep: noSleep, random: () => 0 });
    expect(out).toBe('ok');
    expect(fn).toHaveBeenCalledTimes(3);
  });

  it('gives up after maxAttempts and propagates the last error', async () => {
    const fn = vi.fn<() => Promise<string>>().mockRejectedValue(transient());
    await expect(
      retryTransient(fn, { maxAttempts: 4, sleep: noSleep, random: () => 0 }),
    ).rejects.toThrow('boom');
    expect(fn).toHaveBeenCalledTimes(4);
  });

  it('does not retry a non-transient (4xx) error', async () => {
    const notFound = Object.assign(new Error('missing'), {
      name: 'NoSuchKey',
      $metadata: { httpStatusCode: 404 },
    });
    const fn = vi.fn<() => Promise<string>>().mockRejectedValue(notFound);
    await expect(retryTransient(fn, { sleep: noSleep })).rejects.toThrow('missing');
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('backs off with exponential ceilings capped at maxDelayMs', async () => {
    const delays: number[] = [];
    const fn = vi.fn<() => Promise<string>>().mockRejectedValue(transient());
    await expect(
      retryTransient(fn, {
        maxAttempts: 5,
        baseDelayMs: 100,
        maxDelayMs: 300,
        random: () => 1, // full-jitter upper bound == the ceiling itself
        sleep: (ms) => {
          delays.push(ms);
          return Promise.resolve();
        },
      }),
    ).rejects.toThrow('boom');
    // Ceilings: 100, 200, 300 (capped), 300 (capped) across the 4 retries.
    expect(delays).toEqual([100, 200, 300, 300]);
  });
});
