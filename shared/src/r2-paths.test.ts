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
  parseSnapshotStateKey,
  parseWorkerResultKey,
  rawShardPrefix,
  rosterPath,
  snapshotAggPath,
  snapshotDetailPath,
  snapshotMetaPath,
  snapshotStatePath,
  treeCachePath,
  workerResultPath,
  workerResultPrefix,
  workerStatePath,
} from './r2-paths.js';

describe('r2 paths', () => {
  it('escapes league names with spaces', () => {
    expect(checkpointPath('Settlers of Kalguur')).toBe(
      'state/Settlers%20of%20Kalguur/current.json',
    );
    expect(rosterPath('Settlers of Kalguur')).toBe('state/Settlers%20of%20Kalguur/roster.json');
  });

  it('builds the legacy raw-shard prefix for the retention sweep', () => {
    expect(rawShardPrefix('Standard', '2026-07-17T00')).toBe('raw/Standard/2026-07-17T00/');
  });

  it('zero-pads legacy chunk files under the per-snapshot chunk prefix', () => {
    expect(chunkPrefix('Standard', 's1')).toBe('state/Standard/chunks/s1/');
    expect(chunkPath('Standard', 's1', 42)).toBe('state/Standard/chunks/s1/00042.json');
  });

  it('keys the single snapshot state file as gzipped NDJSON under state/', () => {
    expect(snapshotStatePath('Standard', '2026-07-17T00')).toBe(
      'state/Standard/snapshots/2026-07-17T00.ndjson.gz',
    );
    expect(snapshotStatePath('Settlers of Kalguur', 's1')).toBe(
      'state/Settlers%20of%20Kalguur/snapshots/s1.ndjson.gz',
    );
  });

  it('zero-pads per-worker result files under the per-snapshot results prefix', () => {
    expect(workerResultPrefix('Standard', 's1')).toBe('state/Standard/results/s1/');
    expect(workerResultPath('Standard', 's1', 3)).toBe('state/Standard/results/s1/w03.ndjson.gz');
    expect(workerResultPath('Standard', 's1', 12)).toBe('state/Standard/results/s1/w12.ndjson.gz');
    expect(workerResultPath('Standard', 's1', 3)).toMatch(
      new RegExp(`^${workerResultPrefix('Standard', 's1')}`),
    );
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
    expect(classifyKey(`${rawShardPrefix('Std', 's1')}shard-000.ndjson.gz`)).toBe('raw');
    expect(classifyKey(snapshotDetailPath('Std', 's1', 'characters'))).toBe('detail');
    expect(classifyKey(snapshotAggPath('Std', 's1', 'class_distribution'))).toBe('agg');
    expect(classifyKey(snapshotMetaPath('Std', 's1'))).toBe('meta');
    expect(classifyKey(treeCachePath('3.25'))).toBe('tree');
    expect(classifyKey(checkpointPath('Std'))).toBe('checkpoint');
    expect(classifyKey(rosterPath('Std'))).toBe('roster');
    expect(classifyKey(snapshotStatePath('Std', 's1'))).toBe('snapshot-state');
    expect(classifyKey(workerResultPath('Std', 's1', 0))).toBe('worker-result');
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
    expect(
      parseRawKey(`${rawShardPrefix('Settlers of Kalguur', 's1')}shard-000.ndjson.gz`),
    ).toEqual({
      league: 'Settlers of Kalguur',
      snapshotId: 's1',
    });
    expect(parseChunkKey(chunkPath('Settlers of Kalguur', 's1', 3))).toEqual({
      league: 'Settlers of Kalguur',
      snapshotId: 's1',
    });
    expect(parseDetailKey('index.json')).toBeUndefined();
  });

  it('recovers league + snapshot id from state-file and worker-result keys', () => {
    expect(
      parseSnapshotStateKey(snapshotStatePath('Settlers of Kalguur', '2026-07-17T00')),
    ).toEqual({ league: 'Settlers of Kalguur', snapshotId: '2026-07-17T00' });
    expect(parseWorkerResultKey(workerResultPath('Settlers of Kalguur', 's1', 7))).toEqual({
      league: 'Settlers of Kalguur',
      snapshotId: 's1',
    });
    // The state file and its result files are distinct categories under state/.
    expect(parseSnapshotStateKey(workerResultPath('Std', 's1', 0))).toBeUndefined();
    expect(parseWorkerResultKey(snapshotStatePath('Std', 's1'))).toBeUndefined();
    expect(parseSnapshotStateKey(chunkPath('Std', 's1', 0))).toBeUndefined();
  });
});
