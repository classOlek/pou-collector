/**
 * Checked-in collector configuration (config/collector.json + config/leagues.json)
 * plus environment overrides. collector.json pins the pilot's league/depth and the
 * operational budgets; leagues.json maps each league to its passive-tree version
 * (resolved per league at transform time — see treeVersionFor). Every key is
 * overridable from the environment as COLLECTOR_<SNAKE_CASE> (see envKeyFor;
 * the leagues map as JSON under COLLECTOR_LEAGUES) so repo Actions variables
 * can retune the collector without a commit; blank/unset falls through to the
 * file, a set-but-invalid value fails loudly. The contact email stays in the
 * environment only (hard rule #1) and is handled separately in config.ts.
 *
 * The config path is resolved by walking up from this module to the workspace
 * root (pnpm-workspace.yaml), so it is found no matter the process cwd — the
 * pnpm `--filter` scripts run with cwd = collector/, not the repo root.
 */
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { RunConfig } from './run/config.js';

/** All config the collector needs — a superset of the orchestrator's RunConfig. */
export interface CollectorConfig extends RunConfig {
  /**
   * League → passive-tree version map (config/leagues.json). The version is
   * resolved per league at transform time via `treeVersionFor` — an unmapped
   * league fails the transform loudly rather than publishing against a wrong
   * tree (the 3.25/Mirage drift bug). Values are the major.minor patch line
   * ("3.28"); the treeUrl template turns that into the skilltree-export tag.
   */
  leagues: Record<string, string>;
  /**
   * Origin URL template for the normalized tree JSON, `{version}` substituted
   * at fetch time (fetched once per version, cached in R2). The checked-in
   * template appends `.0` to match grindinggear/skilltree-export tag naming
   * (3.28 → 3.28.0); point COLLECTOR_TREE_URL elsewhere for odd tags.
   */
  treeUrl: string;
  /** Failed transform attempts before the drained snapshot aborts (anti-wedge). */
  maxTransformAttempts: number;
  /** R2 byte budget before retention trims oldest detail Parquet. */
  retentionBudgetBytes: number;
  /** Newest N snapshots per league whose detail Parquet retention never trims. */
  keepRecentDetail: number;
}

const REQUIRED_NUMBERS: (keyof CollectorConfig)[] = [
  'depth',
  'ladderPageSize',
  'maxRunMillis',
  'maxWaitMillis',
  'maxAgeHours',
  'maxAttempts',
  'chunkSize',
  'workerCount',
  'snapshotIntervalHours',
  'abortCooldownHours',
  'maxTransformAttempts',
  'retentionBudgetBytes',
  'keepRecentDetail',
];

/** String keys overridable from the environment alongside REQUIRED_NUMBERS. */
const OVERRIDABLE_STRINGS: (keyof CollectorConfig)[] = ['league', 'treeUrl'];

export type ConfigEnv = Record<string, string | undefined>;

/**
 * Env var carrying an override for a config key: COLLECTOR_ + SNAKE_CASE, e.g.
 * workerCount → COLLECTOR_WORKER_COUNT (league/depth/treeUrl keep their
 * historical names by construction). Blank/unset falls through to the file.
 */
export function envKeyFor(key: string): string {
  return `COLLECTOR_${key.replace(/([A-Z])/g, '_$1').toUpperCase()}`;
}

/** Parse + validate the league → tree-version map (config/leagues.json). */
export function parseLeagues(raw: unknown): Record<string, string> {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    throw new Error('leagues config must be a JSON object of league → tree version');
  }
  for (const [league, version] of Object.entries(raw)) {
    if (typeof version !== 'string' || version.trim() === '') {
      throw new Error(`leagues["${league}"] must be a non-empty tree version string`);
    }
  }
  return raw as Record<string, string>;
}

/** The tree version mapped for a league; an unmapped league is a hard error. */
export function treeVersionFor(leagues: Record<string, string>, league: string): string {
  const version = leagues[league];
  if (!version) {
    throw new Error(
      `no tree version mapped for league "${league}" — add it to config/leagues.json`,
    );
  }
  return version;
}

/** Parse + validate the config object, applying env overrides. */
export function parseConfig(
  raw: unknown,
  env: ConfigEnv = process.env,
  leaguesRaw: unknown = {},
): CollectorConfig {
  if (typeof raw !== 'object' || raw === null) {
    throw new Error('collector config must be a JSON object');
  }
  const record: Record<string, unknown> = { ...(raw as Record<string, unknown>) };

  // Every config key is overridable from the environment (the workflow feeds
  // repo Actions variables through; precedence: dispatch input → variable →
  // file). A set-but-invalid value is a hard error, never a silent fallback.
  for (const key of OVERRIDABLE_STRINGS) {
    const override = env[envKeyFor(key)]?.trim();
    if (override) record[key] = override;
  }
  for (const key of REQUIRED_NUMBERS) {
    const override = env[envKeyFor(key)]?.trim();
    if (!override) continue;
    const n = Number.parseInt(override, 10);
    if (!Number.isFinite(n) || n <= 0) {
      throw new Error(`invalid ${envKeyFor(key)}: ${override}`);
    }
    record[key] = n;
  }

  const cfg = record as unknown as CollectorConfig;
  cfg.leagues = parseLeagues(leaguesRaw);
  const leaguesOverride = env['COLLECTOR_LEAGUES']?.trim();
  if (leaguesOverride) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(leaguesOverride);
    } catch {
      throw new Error(`invalid COLLECTOR_LEAGUES: not valid JSON`);
    }
    cfg.leagues = parseLeagues(parsed);
  }

  if (typeof cfg.league !== 'string' || cfg.league.trim() === '') {
    throw new Error('config.league is required');
  }
  if (typeof cfg.treeUrl !== 'string' || cfg.treeUrl.trim() === '') {
    throw new Error('config.treeUrl is required');
  }
  for (const key of REQUIRED_NUMBERS) {
    const value = cfg[key];
    if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
      throw new Error(`config.${String(key)} must be a positive number`);
    }
  }
  return cfg;
}

/** Walk up from `start` to the directory containing pnpm-workspace.yaml. */
export function findWorkspaceRoot(start: string): string {
  let dir = start;
  for (;;) {
    if (existsSync(join(dir, 'pnpm-workspace.yaml'))) return dir;
    const parent = dirname(dir);
    if (parent === dir) throw new Error(`pnpm-workspace.yaml not found above ${start}`);
    dir = parent;
  }
}

/** config/collector.json resolved from the workspace root, cwd-independent. */
export function defaultConfigPath(): string {
  const root = findWorkspaceRoot(dirname(fileURLToPath(import.meta.url)));
  return join(root, 'config', 'collector.json');
}

/** config/leagues.json next to collector.json (league → tree version). */
export function defaultLeaguesPath(): string {
  const root = findWorkspaceRoot(dirname(fileURLToPath(import.meta.url)));
  return join(root, 'config', 'leagues.json');
}

export function loadConfig(
  path: string = defaultConfigPath(),
  env: ConfigEnv = process.env,
  leaguesPath: string = defaultLeaguesPath(),
): CollectorConfig {
  return parseConfig(
    JSON.parse(readFileSync(path, 'utf8')),
    env,
    JSON.parse(readFileSync(leaguesPath, 'utf8')),
  );
}
