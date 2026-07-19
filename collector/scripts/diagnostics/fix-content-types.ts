/**
 * One-time ops fix: correct the HTTP `Content-Type` of objects already published
 * to R2 before the writer set it (see `contentTypeForKey`). R2 served those
 * objects as `application/octet-stream`, and the web app rejects any data file
 * whose content-type lacks `json` (NotJsonError → "Could not load the snapshot
 * index"). This rewrites ONLY the metadata (server-side CopyObject with
 * MetadataDirective=REPLACE) — object bodies are never downloaded or changed, so
 * it is safe for large Parquet and respects snapshot immutability (hard rule #5:
 * the data is untouched; only its served content-type is corrected).
 *
 * Prefer running this through the ClaudeDiagnostics workflow (secrets stay in
 * GitHub's encrypted store). Locally, with the same env vars as the snapshot
 * workflow:
 *   R2_ENDPOINT=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_BUCKET=... \
 *     pnpm --filter @pou/collector exec tsx scripts/diagnostics/fix-content-types.ts
 *   ... same env ... tsx scripts/diagnostics/fix-content-types.ts --apply   # execute
 */
import {
  CopyObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from '@aws-sdk/client-s3';
import { contentTypeForKey } from '../../src/checkpoint/s3-store.js';

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var ${name}`);
  return value;
}

async function main(): Promise<void> {
  const apply = process.argv.includes('--apply');
  const bucket = requireEnv('R2_BUCKET');
  const client = new S3Client({
    region: process.env.R2_REGION ?? 'auto',
    endpoint: requireEnv('R2_ENDPOINT'),
    credentials: {
      accessKeyId: requireEnv('R2_ACCESS_KEY_ID'),
      secretAccessKey: requireEnv('R2_SECRET_ACCESS_KEY'),
    },
  });

  let scanned = 0;
  let fixed = 0;
  let token: string | undefined;
  do {
    const page = await client.send(
      new ListObjectsV2Command({ Bucket: bucket, ContinuationToken: token }),
    );
    for (const obj of page.Contents ?? []) {
      const key = obj.Key;
      if (!key) continue;
      scanned += 1;
      const want = contentTypeForKey(key);
      const head = await client.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
      const have = head.ContentType ?? '(none)';
      if (have === want) continue;
      fixed += 1;
      console.log(`${apply ? 'FIX ' : 'WOULD FIX '}${key}: ${have} -> ${want}`);
      if (apply) {
        await client.send(
          new CopyObjectCommand({
            Bucket: bucket,
            Key: key,
            CopySource: `${bucket}/${encodeURIComponent(key)}`,
            MetadataDirective: 'REPLACE',
            ContentType: want,
          }),
        );
      }
    }
    token = page.IsTruncated ? page.NextContinuationToken : undefined;
  } while (token);

  console.log(
    `\n${apply ? 'Applied' : 'Dry run'}: ${fixed} of ${scanned} object(s) ` +
      `${apply ? 'corrected' : 'need correcting'}.` +
      (apply ? '' : ' Re-run with --apply to write the changes.'),
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
