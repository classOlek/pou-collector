import { describe, expect, it } from 'vitest';
import {
  INDEX_PATH,
  checkpointPath,
  rawChunkShardPath,
  snapshotAggPath,
  snapshotDetailPath,
  snapshotMetaPath,
} from '@pou/shared';
import { contentTypeForKey } from './s3-store.js';

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
