import { describe, expect, it } from 'vitest';
import {
  checkpointPath,
  chunkPath,
  chunkPrefix,
  classifyKey,
  economyPath,
  ipPacePath,
  ipPacePrefix,
  parseChunkKey,
  parseDetailKey,
  parseRawKey,
  rawChunkShardPath,
  rawChunkShardPrefix,
  rawShardPrefix,
  rosterPath,
  snapshotAggPath,
  snapshotDetailPath,
  snapshotMetaPath,
  treeCachePath,
  workerStatePath,
} from './r2-paths.js';

describe('r2 paths', () => {
  it('escapes league names with spaces', () => {
    expect(checkpointPath('Settlers of Kalguur')).toBe(
      'state/Settlers%20of%20Kalguur/current.json',
    );
    expect(rosterPath('Settlers of Kalguur')).toBe('state/Settlers%20of%20Kalguur/roster.json');
  });

  it('zero-pads chunk shards under the shard prefix for lexicographic listing', () => {
    expect(rawShardPrefix('Standard', '2026-07-17T00')).toBe('raw/Standard/2026-07-17T00/');
    expect(rawChunkShardPath('Standard', '2026-07-17T00', 7, 2)).toBe(
      'raw/Standard/2026-07-17T00/chunk-00007-002.ndjson.gz',
    );
    expect(rawChunkShardPath('Standard', '2026-07-17T00', 7, 2)).toMatch(
      new RegExp(`^${rawChunkShardPrefix('Standard', '2026-07-17T00', 7)}`),
    );
  });

  it('zero-pads chunk files under the per-snapshot chunk prefix', () => {
    expect(chunkPrefix('Standard', 's1')).toBe('state/Standard/chunks/s1/');
    expect(chunkPath('Standard', 's1', 42)).toBe('state/Standard/chunks/s1/00042.json');
  });

  it('keys worker limiter state by league and runner slot', () => {
    expect(workerStatePath('Standard', 'coordinator')).toBe(
      'state/Standard/workers/coordinator.json',
    );
    expect(workerStatePath('Standard', 'w3')).toBe('state/Standard/workers/w3.json');
  });

  it('keys shared pacing state by league and runner IP under a sweepable prefix', () => {
    expect(ipPacePrefix('Standard')).toBe('state/Standard/ips/');
    expect(ipPacePath('Standard', '203.0.113.7')).toBe('state/Standard/ips/203.0.113.7.json');
    // IPv6 (and any odd characters) are URI-encoded so the key stays flat.
    expect(ipPacePath('Standard', '2001:db8::1')).toBe('state/Standard/ips/2001%3Adb8%3A%3A1.json');
    expect(ipPacePath('Settlers of Kalguur', '203.0.113.7')).toMatch(
      new RegExp(`^${ipPacePrefix('Settlers of Kalguur')}`),
    );
  });

  it('keeps published detail under the public snapshots prefix', () => {
    expect(snapshotDetailPath('Standard', 's1', 'characters')).toMatch(
      /^snapshots\/Standard\/s1\/detail\/characters\.parquet$/,
    );
    expect(snapshotMetaPath('Standard', 's1')).toBe('snapshots/Standard/s1/meta.json');
    expect(snapshotAggPath('Standard', 's1', 'class_distribution')).toBe(
      'snapshots/Standard/s1/agg/class_distribution.json',
    );
  });

  it('pins the cached passive tree per version under the private state prefix', () => {
    expect(treeCachePath('3.25.1')).toBe('state/tree/3.25.1.json');
  });

  it('keys the poe.ninja economy cache flat per league under economy/', () => {
    expect(economyPath('Standard')).toBe('economy/Standard.json');
    expect(economyPath('Settlers of Kalguur')).toBe('economy/Settlers%20of%20Kalguur.json');
  });

  it('classifies every key category (the single source of layout truth)', () => {
    expect(classifyKey('index.json')).toBe('index');
    expect(classifyKey(rawChunkShardPath('Std', 's1', 0, 0))).toBe('raw');
    expect(classifyKey(snapshotDetailPath('Std', 's1', 'characters'))).toBe('detail');
    expect(classifyKey(snapshotAggPath('Std', 's1', 'class_distribution'))).toBe('agg');
    expect(classifyKey(snapshotMetaPath('Std', 's1'))).toBe('meta');
    expect(classifyKey(treeCachePath('3.25'))).toBe('tree');
    expect(classifyKey(checkpointPath('Std'))).toBe('checkpoint');
    expect(classifyKey(rosterPath('Std'))).toBe('roster');
    expect(classifyKey(chunkPath('Std', 's1', 3))).toBe('chunk');
    expect(classifyKey(workerStatePath('Std', 'w0'))).toBe('worker');
    expect(classifyKey(ipPacePath('Std', '203.0.113.7'))).toBe('ip');
    expect(classifyKey(economyPath('Std'))).toBe('economy');
    expect(classifyKey('something/else')).toBe('other');
  });

  it('recovers league + snapshot id from detail, raw and chunk keys (URI-decoded league)', () => {
    expect(parseDetailKey(snapshotDetailPath('Settlers of Kalguur', 's1', 'items'))).toEqual({
      league: 'Settlers of Kalguur',
      snapshotId: 's1',
    });
    expect(parseRawKey(rawChunkShardPath('Settlers of Kalguur', 's1', 3, 0))).toEqual({
      league: 'Settlers of Kalguur',
      snapshotId: 's1',
    });
    expect(parseChunkKey(chunkPath('Settlers of Kalguur', 's1', 3))).toEqual({
      league: 'Settlers of Kalguur',
      snapshotId: 's1',
    });
    expect(parseDetailKey('index.json')).toBeUndefined();
  });
});
