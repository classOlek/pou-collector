/**
 * Checked-in collector configuration (config/collector.json) plus environment
 * overrides. The file pins the pilot's league/depth and the operational budgets;
 * `COLLECTOR_LEAGUE` / `COLLECTOR_DEPTH` let a workflow_dispatch override league
 * and depth without editing the file (Phase 2). The contact email stays in the
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
  /** Passive-tree version pinned for this league (resolves node hashes). */
  treeVersion: string;
  /** Origin URL for the normalized tree JSON (fetched once, cached in R2). */
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

export interface ConfigEnv {
  COLLECTOR_LEAGUE?: string | undefined;
  COLLECTOR_DEPTH?: string | undefined;
  COLLECTOR_TREE_URL?: string | undefined;
}

/** Parse + validate the config object, applying env overrides. */
export function parseConfig(raw: unknown, env: ConfigEnv = process.env): CollectorConfig {
  if (typeof raw !== 'object' || raw === null) {
    throw new Error('collector config must be a JSON object');
  }
  const cfg = { ...(raw as Record<string, unknown>) } as unknown as CollectorConfig;

  const leagueOverride = env.COLLECTOR_LEAGUE?.trim();
  if (leagueOverride) cfg.league = leagueOverride;
  const depthOverride = env.COLLECTOR_DEPTH?.trim();
  if (depthOverride) {
    const n = Number.parseInt(depthOverride, 10);
    if (!Number.isFinite(n) || n <= 0) throw new Error(`invalid COLLECTOR_DEPTH: ${depthOverride}`);
    cfg.depth = n;
  }
  const treeUrlOverride = env.COLLECTOR_TREE_URL?.trim();
  if (treeUrlOverride) cfg.treeUrl = treeUrlOverride;

  if (typeof cfg.league !== 'string' || cfg.league.trim() === '') {
    throw new Error('config.league is required');
  }
  if (typeof cfg.treeVersion !== 'string' || cfg.treeVersion.trim() === '') {
    throw new Error('config.treeVersion is required');
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

export function loadConfig(
  path: string = defaultConfigPath(),
  env: ConfigEnv = process.env,
): CollectorConfig {
  return parseConfig(JSON.parse(readFileSync(path, 'utf8')), env);
}
