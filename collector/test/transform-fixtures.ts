/**
 * Builders for transform tests: realistic raw shard records (get-items /
 * get-passive-skills shapes), a fixture passive tree, and a drained manifest.
 * Kept beside the collector fixtures so the transform golden tests and the
 * end-to-end state-machine test share one source of truth.
 */
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { SnapshotCharacter, SnapshotManifest } from '@classolek/shared';
import { SCHEMA_VERSION, emptyTally } from '@classolek/shared';
import type { PassiveTree, TreeOrigin } from '../src/transform/tree-source.js';

const here = dirname(fileURLToPath(import.meta.url));

interface RawTreeFixture {
  version: string;
  nodes: { hash: number; name: string; stats: string[]; isKeystone: boolean }[];
}

/** The fixture tree (a dozen nodes incl. keystones) as a normalized PassiveTree. */
export function fixtureTree(version = '3.25-test'): PassiveTree {
  const raw = JSON.parse(
    readFileSync(join(here, 'fixtures', 'tree.json'), 'utf8'),
  ) as RawTreeFixture;
  return { version, nodes: raw.nodes };
}

/** Fake tree origin that counts fetches (to prove the R2 cache is reused). */
export class FakeTreeOrigin implements TreeOrigin {
  fetches = 0;
  constructor(private readonly tree: PassiveTree = fixtureTree()) {}
  fetch(version: string): Promise<PassiveTree> {
    this.fetches += 1;
    return Promise.resolve({ ...this.tree, version });
  }
}

export interface CharSpec {
  rank: number;
  account: string;
  character: string;
  class: string;
  level?: number;
  /** Active gem in the (main) body-armour link group. */
  mainSkill: string;
  /** Support gems linked with the main skill. */
  supports?: string[];
  /** Give the body armour a Unique name (else it is Rare). */
  unique?: string;
  /** Passive node hashes (mix of normal + keystone hashes from the fixture tree). */
  nodes?: number[];
  fetchedAt?: string;
}

/** Build one raw shard record (the exact shape the collector's sink writes). */
export function buildRawRecord(spec: CharSpec): Record<string, unknown> {
  const supports = spec.supports ?? [];
  const bodyArmour = {
    inventoryId: 'BodyArmour',
    name: spec.unique ?? '',
    typeLine: 'Vaal Regalia',
    baseType: 'Vaal Regalia',
    rarity: spec.unique ? 'Unique' : 'Rare',
    ilvl: 86,
    corrupted: false,
    implicitMods: [],
    explicitMods: ['+52 to maximum Life'],
    sockets: Array.from({ length: 1 + supports.length }, () => ({ group: 0, attr: 'S' })),
    socketedItems: [
      {
        socket: 0,
        support: false,
        typeLine: spec.mainSkill,
        properties: [
          { name: 'Level', values: [['20', 0]] },
          { name: 'Quality', values: [['+20%', 1]] },
        ],
      },
      ...supports.map((gem, i) => ({
        socket: i + 1,
        support: true,
        typeLine: gem,
        properties: [{ name: 'Level', values: [['20', 0]] }],
      })),
    ],
  };
  const weapon = {
    inventoryId: 'Weapon',
    name: '',
    typeLine: 'Vaal Axe',
    baseType: 'Vaal Axe',
    rarity: 'Rare',
    ilvl: 84,
    corrupted: false,
    explicitMods: ['Adds 20 to 40 Physical Damage'],
    sockets: [{ group: 0, attr: 'S' }],
    socketedItems: [],
  };
  return {
    rank: spec.rank,
    account: spec.account,
    character: spec.character,
    class: spec.class,
    level: spec.level ?? 100,
    fetchedAt: spec.fetchedAt ?? '2026-07-17T00:00:00.000Z',
    items: {
      character: {
        name: spec.character,
        class: spec.class,
        classId: 1,
        ascendancyClass: 1,
        level: spec.level ?? 100,
        experience: 4250334444,
      },
      items: [bodyArmour, weapon],
    },
    passives: { hashes: spec.nodes ?? [123], hashes_ex: [], items: [], jewel_data: {} },
  };
}

/**
 * One `ok` state-file line for a spec: the queued identity plus the raw
 * payloads inline (`characterData` = the get-items response, `passiveTree` =
 * the get-passives response — the v4 state file IS the raw). This is what the
 * transform streams and emits back to the DuckDB NDJSON. A non-`ok` outcome
 * carries no payloads.
 */
export function buildStateLine(
  spec: CharSpec,
  outcome: SnapshotCharacter['outcome'] = 'ok',
): SnapshotCharacter {
  const raw = buildRawRecord(spec);
  return {
    rank: spec.rank,
    account: spec.account,
    character: spec.character,
    class: spec.class,
    level: spec.level ?? 100,
    outcome,
    attempts: 1,
    ...(outcome === 'ok'
      ? {
          fetchedAt: raw['fetchedAt'] as string,
          characterData: raw['items'],
          passiveTree: raw['passives'],
        }
      : {}),
  };
}

/**
 * A drained manifest (phase transforming) whose outcome rollup matches the ok
 * specs, plus optional private/dead/pending padding. `pending > 0` produces a
 * still-collecting manifest for incremental-publish tests.
 */
export function transformingManifest(
  league: string,
  snapshotId: string,
  specs: CharSpec[],
  extraOutcomes: { private?: number; dead?: number; pending?: number } = {},
): SnapshotManifest {
  const outcomes = {
    ...emptyTally(),
    ok: specs.length,
    private: extraOutcomes.private ?? 0,
    dead: extraOutcomes.dead ?? 0,
    pending: extraOutcomes.pending ?? 0,
  };
  const total = outcomes.ok + outcomes.private + outcomes.dead + outcomes.pending;
  const drained = outcomes.pending === 0;

  return {
    schemaVersion: SCHEMA_VERSION,
    snapshotId,
    league,
    depth: total,
    phase: drained ? 'transforming' : 'collecting',
    ladderCapturedAt: '2026-07-16T22:00:00.000Z',
    ...(drained ? { completedAt: '2026-07-17T00:30:00.000Z' } : {}),
    totalCharacters: total,
    outcomes,
  };
}
