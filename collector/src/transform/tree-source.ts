/**
 * Passive-tree resolution (Phase 3, docs/ARCHITECTURE.md §6).
 *
 * The transform turns a character's `hashes` (passive node ids) into readable
 * node names/stats and a keystone flag. Tree data is pinned per league tree
 * *version* and cached in R2: the origin is fetched once, normalized, stored,
 * and reused by every later transform for that version (few writes, no repeat
 * downloads). All tree access goes through the `TreeSource` seam so tests use a
 * small fixture tree and never hit the network (hard rule #3).
 */
import { treeCachePath } from '@classolek/shared';
import type { ObjectStore } from '../checkpoint/object-store.js';

/** One resolved passive node. */
export interface TreeNode {
  hash: number;
  name: string;
  stats: string[];
  isKeystone: boolean;
}

/** Normalized, version-pinned tree: the shape cached in R2 and read by the SQL. */
export interface PassiveTree {
  version: string;
  nodes: TreeNode[];
}

/** Fetches + normalizes the tree for one version from its origin (network/disk). */
export interface TreeOrigin {
  fetch(version: string): Promise<PassiveTree>;
}

export interface TreeSource {
  load(version: string): Promise<PassiveTree>;
}

/**
 * Caching tree source: returns the R2-cached normalized tree when present,
 * otherwise fetches once from the origin, stores it, and returns it. Idempotent
 * and safe to call from every transform run.
 */
export class CachedTreeSource implements TreeSource {
  constructor(
    private readonly store: ObjectStore,
    private readonly origin: TreeOrigin,
  ) {}

  async load(version: string): Promise<PassiveTree> {
    const key = treeCachePath(version);
    const cached = await this.store.get(key);
    if (cached) {
      const parsed = JSON.parse(new TextDecoder().decode(cached)) as PassiveTree;
      if (parsed.version === version && Array.isArray(parsed.nodes)) return parsed;
    }
    const tree = await this.origin.fetch(version);
    await this.store.put(key, new TextEncoder().encode(JSON.stringify(tree)));
    return tree;
  }
}
