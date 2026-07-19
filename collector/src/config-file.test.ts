import { chdir, cwd } from 'node:process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  defaultConfigPath,
  defaultLeaguesPath,
  findWorkspaceRoot,
  loadConfig,
  parseConfig,
  parseLeagues,
  treeVersionFor,
} from './config-file.js';

const base = {
  league: 'Standard',
  depth: 500,
  ladderPageSize: 200,
  maxRunMillis: 1_500_000,
  maxAgeHours: 48,
  maxAttempts: 3,
  chunkSize: 50,
  workerCount: 4,
  snapshotIntervalHours: 12,
  abortCooldownHours: 6,
  maxTransformAttempts: 3,
  treeUrl: 'https://example.test/tree-{version}.json',
  retentionBudgetBytes: 9_000_000_000,
  keepRecentDetail: 6,
};

describe('parseConfig', () => {
  it('accepts a valid config', () => {
    const cfg = parseConfig(base, {});
    expect(cfg.league).toBe('Standard');
    expect(cfg.maxTransformAttempts).toBe(3);
  });

  it('applies league / depth / tree-url env overrides', () => {
    const cfg = parseConfig(base, {
      COLLECTOR_LEAGUE: 'Settlers of Kalguur',
      COLLECTOR_DEPTH: '1000',
      COLLECTOR_TREE_URL: 'https://override.test/t.json',
    });
    expect(cfg.league).toBe('Settlers of Kalguur');
    expect(cfg.depth).toBe(1000);
    expect(cfg.treeUrl).toBe('https://override.test/t.json');
  });

  it('rejects a non-numeric depth override', () => {
    expect(() => parseConfig(base, { COLLECTOR_DEPTH: 'lots' })).toThrow(/COLLECTOR_DEPTH/);
  });

  it('rejects a missing/invalid required number', () => {
    expect(() => parseConfig({ ...base, maxRunMillis: 0 }, {})).toThrow(/maxRunMillis/);
    expect(() => parseConfig({ ...base, maxTransformAttempts: 0 }, {})).toThrow(
      /maxTransformAttempts/,
    );
    const noTreeUrl: Record<string, unknown> = { ...base };
    delete noTreeUrl.treeUrl;
    expect(() => parseConfig(noTreeUrl, {})).toThrow(/treeUrl/);
  });

  it('carries the league → tree-version map through', () => {
    const cfg = parseConfig(base, {}, { Mirage: '3.28' });
    expect(cfg.leagues).toEqual({ Mirage: '3.28' });
  });
});

describe('leagues map (league → tree version)', () => {
  it('accepts a valid map and resolves versions per league', () => {
    const leagues = parseLeagues({ Mirage: '3.28', Standard: '3.28' });
    expect(treeVersionFor(leagues, 'Mirage')).toBe('3.28');
  });

  it('rejects non-object maps and non-string versions', () => {
    expect(() => parseLeagues(['Mirage'])).toThrow(/JSON object/);
    expect(() => parseLeagues({ Mirage: 3.28 })).toThrow(/Mirage/);
    expect(() => parseLeagues({ Mirage: '' })).toThrow(/Mirage/);
  });

  it('fails loudly for an unmapped league (no silent fallback tree)', () => {
    expect(() => treeVersionFor({ Mirage: '3.28' }, 'Necropolis')).toThrow(
      /no tree version mapped for league "Necropolis"/,
    );
  });
});

describe('config path resolution (cwd-independent)', () => {
  const original = cwd();
  afterEach(() => chdir(original));

  it('finds config/collector.json even when cwd is the collector package', () => {
    const collectorDir = dirname(dirname(fileURLToPath(import.meta.url)));
    chdir(collectorDir); // reproduces `pnpm --filter @pou/collector run collect`
    // defaultConfigPath walks up to the workspace root, so this works regardless of cwd.
    expect(defaultConfigPath()).toBe(
      join(findWorkspaceRoot(collectorDir), 'config', 'collector.json'),
    );
    expect(defaultLeaguesPath()).toBe(
      join(findWorkspaceRoot(collectorDir), 'config', 'leagues.json'),
    );
    const cfg = loadConfig(undefined, {});
    expect(cfg.depth).toBeGreaterThanOrEqual(200);
    expect(cfg.depth).toBeLessThanOrEqual(1000);
    expect(cfg.retentionBudgetBytes).toBeLessThan(10_000_000_000); // under the 10 GB ceiling
    expect(cfg.maxTransformAttempts).toBeGreaterThan(0);
    // The checked-in map must cover the checked-in league, or finalize dies.
    expect(treeVersionFor(cfg.leagues, cfg.league)).toMatch(/^\d+\.\d+$/);
    expect(cfg.treeUrl).toContain('{version}');
  });
});
