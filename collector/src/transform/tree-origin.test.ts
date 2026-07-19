import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import type { HttpClient, HttpResponse } from '../sources/types.js';
import { HttpTreeOrigin, normalizeTree } from './tree-origin.js';

describe('normalizeTree', () => {
  it('normalizes the array node form', () => {
    const tree = normalizeTree('3.25', {
      nodes: [{ hash: 4271, name: 'Resolute Technique', stats: ['a', 'b'], isKeystone: true }],
    });
    expect(tree.nodes).toEqual([
      { hash: 4271, name: 'Resolute Technique', stats: ['a', 'b'], isKeystone: true },
    ]);
  });

  it('normalizes a hash-keyed object node map (RePoE-style, snake_case keystone)', () => {
    const tree = normalizeTree('3.25', {
      nodes: { '123': { name: 'Strength', stats: ['+10 Str'], is_keystone: false } },
    });
    expect(tree.nodes).toEqual([
      { hash: 123, name: 'Strength', stats: ['+10 Str'], isKeystone: false },
    ]);
  });

  it('throws on missing or empty nodes', () => {
    expect(() => normalizeTree('3.25', {})).toThrow(/nodes/);
    expect(() => normalizeTree('3.25', { nodes: [] })).toThrow(/no nodes/);
  });
});

describe('HttpTreeOrigin', () => {
  const respond = (res: HttpResponse): HttpClient => {
    return (req) => {
      lastUrl = req.url;
      return Promise.resolve(res);
    };
  };
  let lastUrl = '';

  it('substitutes {version} and parses the fetched tree', async () => {
    const body = JSON.stringify({ nodes: [{ hash: 1, name: 'N', stats: [], isKeystone: false }] });
    const origin = new HttpTreeOrigin(respond({ status: 200, headers: {}, body }), {
      treeUrl: 'https://tree.test/tree-{version}.json',
      userAgent: 'ua',
    });
    const tree = await origin.fetch('3.25');
    expect(lastUrl).toBe('https://tree.test/tree-3.25.json');
    expect(tree.version).toBe('3.25');
    expect(tree.nodes).toHaveLength(1);
  });

  it('throws on a non-2xx response (keeps raw, blocks publish upstream)', async () => {
    const origin = new HttpTreeOrigin(respond({ status: 404, headers: {}, body: 'nope' }), {
      treeUrl: 'https://tree.test/tree-{version}.json',
      userAgent: 'ua',
    });
    await expect(origin.fetch('3.25')).rejects.toThrow(/404/);
  });
});

/**
 * Regression lock on the real pinned source. `config.treeUrl` points at the
 * official GGG export (grindinggear/skilltree-export), whose `nodes` is an object
 * keyed by the passive-node id — the same id GGG returns in
 * `get-passive-skills.hashes`. The fixture is a verbatim slice of that export;
 * this guards against a future normalizeTree change silently breaking the source
 * (node ids no longer usable as join keys → every passive resolves to NULL).
 */
describe('normalizeTree — official GGG export shape', () => {
  const sample = JSON.parse(
    readFileSync(
      fileURLToPath(new URL('../../test/fixtures/tree-official-sample.json', import.meta.url)),
      'utf8',
    ),
  );

  it('keys nodes by the passive-node id and drops non-node keys', () => {
    const tree = normalizeTree('3.25', sample);
    // "root" (a non-numeric key with no name/stats) is skipped; the 3 real nodes remain.
    const byHash = new Map(tree.nodes.map((n) => [n.hash, n]));
    expect([...byHash.keys()].sort((a, b) => a - b)).toEqual([89, 94, 31961]);

    // Keystone flag comes from the export's camelCase `isKeystone`; multi-line stat preserved.
    const rt = byHash.get(31961);
    expect(rt).toMatchObject({ name: 'Resolute Technique', isKeystone: true });
    expect(rt?.stats).toEqual(["Your hits can't be Evaded\nNever deal Critical Strikes"]);

    // A mastery node has an empty `stats` array but still resolves to a real name.
    expect(byHash.get(89)).toMatchObject({ name: 'Mine Mastery', stats: [], isKeystone: false });
    // A plain node resolves name + stats; unknown fields (icon/group/orbit/skill) are ignored.
    expect(byHash.get(94)).toEqual({
      hash: 94,
      name: 'Evasion',
      stats: ['14% increased Evasion Rating'],
      isKeystone: false,
    });
  });
});
