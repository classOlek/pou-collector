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
}

export class S3ObjectStore implements ObjectStore {
  private readonly client: S3Client;

  constructor(private readonly config: S3ObjectStoreConfig) {
    this.client = new S3Client({
      region: config.region ?? 'auto',
      endpoint: config.endpoint,
      credentials: {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
      },
    });
  }

  async get(key: string): Promise<Uint8Array | undefined> {
    try {
      const out = await this.client.send(
        new GetObjectCommand({ Bucket: this.config.bucket, Key: key }),
      );
      if (!out.Body) return undefined;
      return await out.Body.transformToByteArray();
    } catch (err) {
      if (isNotFound(err)) return undefined;
      throw err;
    }
  }

  async put(key: string, body: Uint8Array): Promise<void> {
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.config.bucket,
        Key: key,
        Body: body,
        ContentType: contentTypeForKey(key),
      }),
    );
  }

  async delete(key: string): Promise<void> {
    await this.client.send(new DeleteObjectCommand({ Bucket: this.config.bucket, Key: key }));
  }

  async listDetailed(prefix: string): Promise<ObjectInfo[]> {
    const infos: ObjectInfo[] = [];
    let token: string | undefined;
    do {
      const out = await this.client.send(
        new ListObjectsV2Command({
          Bucket: this.config.bucket,
          Prefix: prefix,
          ContinuationToken: token,
        }),
      );
      for (const obj of out.Contents ?? []) {
        if (obj.Key) infos.push({ key: obj.Key, size: obj.Size ?? 0 });
      }
      token = out.IsTruncated ? out.NextContinuationToken : undefined;
    } while (token);
    return infos;
  }
}

function isNotFound(err: unknown): boolean {
  if (typeof err !== 'object' || err === null) return false;
  const name = (err as { name?: unknown }).name;
  const status = (err as { $metadata?: { httpStatusCode?: unknown } }).$metadata?.httpStatusCode;
  return name === 'NoSuchKey' || name === 'NotFound' || status === 404;
}
