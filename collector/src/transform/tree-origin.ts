/**
 * Production TreeOrigin: fetches the pinned, normalized passive-tree JSON for a
 * league version over the HttpClient seam (so it is testable offline and never
 * couples the transform to a specific upstream format). We pin a normalized tree
 * file per version and point `treeUrl` at it; `CachedTreeSource` fetches through
 * this origin once and reuses the R2 cache thereafter.
 *
 * Two node container shapes are accepted so a RePoE-style export can be pointed
 * at directly: a `nodes` array of {hash,name,stats,isKeystone}, or a `nodes`
 * object keyed by hash. Unknown fields are ignored; anything unparseable throws
 * (the transform then keeps raw and does not publish).
 */
import type { HttpClient } from '../sources/types.js';
import type { PassiveTree, TreeNode, TreeOrigin } from './tree-source.js';

export interface HttpTreeOriginConfig {
  /** URL template; `{version}` is replaced with the requested version. */
  treeUrl: string;
  userAgent: string;
}

function toNode(hash: number, raw: unknown): TreeNode | undefined {
  if (typeof raw !== 'object' || raw === null) return undefined;
  const r = raw as { name?: unknown; stats?: unknown; isKeystone?: unknown; is_keystone?: unknown };
  const name = typeof r.name === 'string' ? r.name : '';
  const stats = Array.isArray(r.stats)
    ? r.stats.filter((s): s is string => typeof s === 'string')
    : [];
  const isKeystone = Boolean(r.isKeystone ?? r.is_keystone);
  return { hash, name, stats, isKeystone };
}

export function normalizeTree(version: string, data: unknown): PassiveTree {
  if (typeof data !== 'object' || data === null || !('nodes' in data)) {
    throw new Error('tree JSON missing "nodes"');
  }
  const nodesRaw = (data as { nodes: unknown }).nodes;
  const nodes: TreeNode[] = [];

  if (Array.isArray(nodesRaw)) {
    for (const entry of nodesRaw) {
      const hash = Number((entry as { hash?: unknown })?.hash);
      const node = Number.isFinite(hash) ? toNode(hash, entry) : undefined;
      if (node) nodes.push(node);
    }
  } else if (typeof nodesRaw === 'object' && nodesRaw !== null) {
    for (const [key, value] of Object.entries(nodesRaw as Record<string, unknown>)) {
      const hash = Number(key);
      const node = Number.isFinite(hash) ? toNode(hash, value) : undefined;
      if (node) nodes.push(node);
    }
  } else {
    throw new Error('tree "nodes" must be an array or an object');
  }
  if (nodes.length === 0) throw new Error('tree JSON produced no nodes');
  return { version, nodes };
}

export class HttpTreeOrigin implements TreeOrigin {
  constructor(
    private readonly http: HttpClient,
    private readonly config: HttpTreeOriginConfig,
  ) {}

  async fetch(version: string): Promise<PassiveTree> {
    const url = this.config.treeUrl.replace('{version}', encodeURIComponent(version));
    const res = await this.http({ url, headers: { 'user-agent': this.config.userAgent } });
    if (res.status < 200 || res.status >= 300) {
      throw new Error(`tree fetch failed: ${res.status} ${url}`);
    }
    return normalizeTree(version, JSON.parse(res.body));
  }
}
