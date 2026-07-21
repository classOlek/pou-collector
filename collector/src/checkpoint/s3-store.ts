/**
 * Real ObjectStore backed by the S3-compatible R2 API. This is the *only* file
 * that knows about the AWS SDK — its types never leak past the ObjectStore
 * interface, so the rest of the collector stays swappable and testable. It runs
 * only in GitHub Actions; unit tests use MemoryObjectStore instead.
 */
import {
  DeleteObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import type { ObjectInfo, ObjectStore } from './object-store.js';

/**
 * HTTP content type for a published key, keyed off its extension. R2 serves the
 * stored `ContentType` verbatim, and the web app rejects any data file whose
 * `content-type` doesn't include `json` (NotJsonError) — so an unset type makes
 * a correctly-published index.json unreadable ("Could not load the snapshot
 * index"). Kept a pure function so the mapping is unit-testable without R2.
 */
export function contentTypeForKey(key: string): string {
  if (key.endsWith('.json')) return 'application/json; charset=utf-8';
  if (key.endsWith('.parquet')) return 'application/vnd.apache.parquet';
  if (key.endsWith('.ndjson.gz')) return 'application/gzip';
  return 'application/octet-stream';
}

export interface S3ObjectStoreConfig {
  bucket: string;
  /** R2 S3 endpoint, e.g. https://<account>.r2.cloudflarestorage.com */
  endpoint: string;
  region?: string;
  accessKeyId: string;
  secretAccessKey: string;
  /** Transient-retry tuning; defaults ride out a multi-minute R2 blip. */
  retry?: Partial<RetryOptions>;
}

/** Tuning for {@link retryTransient}. Defaults sized for R2's 5xx/throttle blips. */
export interface RetryOptions {
  /** Total attempts including the first (so `6` == 1 try + 5 retries). */
  maxAttempts: number;
  /** First backoff step in ms; each retry doubles it up to `maxDelayMs`. */
  baseDelayMs: number;
  /** Cap on a single backoff step in ms (before jitter). */
  maxDelayMs: number;
  /** Injectable for tests; resolves after `ms`. */
  sleep: (ms: number) => Promise<void>;
  /** Injectable [0,1) source for full jitter; defaults to Math.random. */
  random: () => number;
}

const DEFAULT_RETRY: RetryOptions = {
  maxAttempts: 6,
  baseDelayMs: 500,
  maxDelayMs: 10_000,
  sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  random: Math.random,
};

export class S3ObjectStore implements ObjectStore {
  private readonly client: S3Client;
  private readonly retry: RetryOptions;

  constructor(private readonly config: S3ObjectStoreConfig) {
    this.retry = { ...DEFAULT_RETRY, ...config.retry };
    this.client = new S3Client({
      region: config.region ?? 'auto',
      endpoint: config.endpoint,
      credentials: {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
      },
      // Defense in depth: the SDK's own retryer also rides out short 5xx/throttle
      // blips; our `retryTransient` wrapper covers the longer R2 outage windows
      // that outlast the SDK's sub-second budget (see s3-store.test.ts).
      maxAttempts: this.retry.maxAttempts,
    });
  }

  async get(key: string): Promise<Uint8Array | undefined> {
    try {
      const out = await retryTransient(
        () => this.client.send(new GetObjectCommand({ Bucket: this.config.bucket, Key: key })),
        this.retry,
      );
      if (!out.Body) return undefined;
      return await out.Body.transformToByteArray();
    } catch (err) {
      if (isNotFound(err)) return undefined;
      throw err;
    }
  }

  async put(key: string, body: Uint8Array): Promise<void> {
    await retryTransient(
      () =>
        this.client.send(
          new PutObjectCommand({
            Bucket: this.config.bucket,
            Key: key,
            Body: body,
            ContentType: contentTypeForKey(key),
          }),
        ),
      this.retry,
    );
  }

  async delete(key: string): Promise<void> {
    await retryTransient(
      () => this.client.send(new DeleteObjectCommand({ Bucket: this.config.bucket, Key: key })),
      this.retry,
    );
  }

  async listDetailed(prefix: string): Promise<ObjectInfo[]> {
    const infos: ObjectInfo[] = [];
    let token: string | undefined;
    do {
      const out = await retryTransient(
        () =>
          this.client.send(
            new ListObjectsV2Command({
              Bucket: this.config.bucket,
              Prefix: prefix,
              ContinuationToken: token,
            }),
          ),
        this.retry,
      );
      for (const obj of out.Contents ?? []) {
        if (obj.Key) infos.push({ key: obj.Key, size: obj.Size ?? 0 });
      }
      token = out.IsTruncated ? out.NextContinuationToken : undefined;
    } while (token);
    return infos;
  }
}

/**
 * Retry `fn` while it throws a transient S3/R2 error (5xx, throttling, or a
 * dropped connection), with exponential backoff + full jitter. Non-transient
 * errors (4xx incl. NotFound, auth) throw immediately — retrying them just burns
 * the request budget. The final attempt's error propagates unchanged.
 *
 * R2 periodically answers with `InternalError` ("We encountered an internal
 * error. Please try again.") for a minute or two; a single unretried hit here
 * fails a whole coordinate/finalize run. Riding it out keeps runs green without
 * leaning on the next scheduled tick to resume.
 */
export async function retryTransient<T>(
  fn: () => Promise<T>,
  options: Partial<RetryOptions> = {},
): Promise<T> {
  const opts = { ...DEFAULT_RETRY, ...options };
  let attempt = 0;
  for (;;) {
    try {
      return await fn();
    } catch (err) {
      attempt += 1;
      if (attempt >= opts.maxAttempts || !isTransientS3Error(err)) throw err;
      const ceiling = Math.min(opts.maxDelayMs, opts.baseDelayMs * 2 ** (attempt - 1));
      await opts.sleep(Math.floor(opts.random() * ceiling));
    }
  }
}

/**
 * True when an S3/R2 error is worth retrying: server-side 5xx, throttling
 * (429 / SlowDown), request timeouts, or a dropped/refused connection. 4xx
 * client errors (NotFound, AccessDenied, malformed request) are excluded — they
 * are deterministic and would fail identically on retry.
 */
export function isTransientS3Error(err: unknown): boolean {
  if (typeof err !== 'object' || err === null) return false;
  const e = err as {
    name?: unknown;
    code?: unknown;
    $metadata?: { httpStatusCode?: unknown };
  };
  const status = e.$metadata?.httpStatusCode;
  if (typeof status === 'number' && (status >= 500 || status === 429)) return true;
  const name = typeof e.name === 'string' ? e.name : '';
  const code = typeof e.code === 'string' ? e.code : '';
  return TRANSIENT_NAMES.has(name) || TRANSIENT_NAMES.has(code);
}

/**
 * Error `name`/`code` values that signal a transient failure even when no HTTP
 * status is attached (e.g. a connection dropped before any response). Covers
 * R2/S3 server errors and Node socket errors.
 */
const TRANSIENT_NAMES = new Set([
  'InternalError',
  'InternalServerError',
  'ServiceUnavailable',
  'SlowDown',
  'ThrottlingException',
  'RequestThrottled',
  'RequestThrottledException',
  'RequestTimeout',
  'RequestTimeoutException',
  'TimeoutError',
  'ECONNRESET',
  'ECONNREFUSED',
  'EPIPE',
  'ETIMEDOUT',
  'EAI_AGAIN',
]);

function isNotFound(err: unknown): boolean {
  if (typeof err !== 'object' || err === null) return false;
  const name = (err as { name?: unknown }).name;
  const status = (err as { $metadata?: { httpStatusCode?: unknown } }).$metadata?.httpStatusCode;
  return name === 'NoSuchKey' || name === 'NotFound' || status === 404;
}
