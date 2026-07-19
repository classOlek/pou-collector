/**
 * Minimal S3-shaped object surface the collector needs: get / put / delete /
 * listDetailed. Everything durable (checkpoint manifest, raw shards, published
 * files) goes through this seam so tests run against an in-memory fake and never
 * touch R2 (hard rule #4). A single listing method (`listDetailed`) returns keys
 * + sizes; `listKeys` derives a keys-only view so future impls write one listing.
 */
/** A stored object's key and byte size (from a listing, no body fetch). */
export interface ObjectInfo {
  key: string;
  size: number;
}

export interface ObjectStore {
  get(key: string): Promise<Uint8Array | undefined>;
  put(key: string, body: Uint8Array): Promise<void>;
  delete(key: string): Promise<void>;
  /** Keys + sizes under a prefix (listing / usage accounting, no body fetch). */
  listDetailed(prefix: string): Promise<ObjectInfo[]>;
}

/** Keys under a prefix, derived from the single listing method. */
export async function listKeys(store: ObjectStore, prefix: string): Promise<string[]> {
  return (await store.listDetailed(prefix)).map((o) => o.key);
}

/** Read + JSON-parse an object; undefined when absent. Throws on invalid JSON. */
export async function getJson<T>(store: ObjectStore, key: string): Promise<T | undefined> {
  const bytes = await store.get(key);
  if (!bytes) return undefined;
  return JSON.parse(new TextDecoder().decode(bytes)) as T;
}

/** JSON-encode + write an object (pretty-printed for the small public files). */
export function putJson(
  store: ObjectStore,
  key: string,
  value: unknown,
  pretty = false,
): Promise<void> {
  const text = pretty ? JSON.stringify(value, null, 2) : JSON.stringify(value);
  return store.put(key, new TextEncoder().encode(text));
}

/** Working in-memory implementation for tests (prefer fakes over mocks). */
export class MemoryObjectStore implements ObjectStore {
  private readonly objects = new Map<string, Uint8Array>();

  get(key: string): Promise<Uint8Array | undefined> {
    const value = this.objects.get(key);
    return Promise.resolve(value ? value.slice() : undefined);
  }

  put(key: string, body: Uint8Array): Promise<void> {
    this.objects.set(key, body.slice());
    return Promise.resolve();
  }

  delete(key: string): Promise<void> {
    this.objects.delete(key);
    return Promise.resolve();
  }

  listDetailed(prefix: string): Promise<ObjectInfo[]> {
    const infos: ObjectInfo[] = [];
    for (const [key, body] of this.objects) {
      if (key.startsWith(prefix)) infos.push({ key, size: body.byteLength });
    }
    return Promise.resolve(infos);
  }

  /** Test helper: enumerate stored keys. */
  keys(): string[] {
    return [...this.objects.keys()];
  }
}
