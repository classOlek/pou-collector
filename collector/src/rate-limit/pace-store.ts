/**
 * Shared per-IP pacing state at state/<league>/ips/<ip>.json.
 *
 * The pace windows mirror GGG's per-IP request counters, so the recent-acquire
 * spend is keyed by runner IP here rather than by worker slot (limiter-store.ts,
 * which keeps the client-scoped penalty/streaks/rules). Any slot that lands on
 * an IP — across fires, and across the coordinator / worker / build-roster
 * steps — reads the same file, so a reused IP can no longer be double-spent by a
 * different slot that had no memory of the first one's requests.
 *
 * Single writer per object holds as everywhere else: within a fire each matrix
 * job is a distinct runner (distinct IP), and fires are serialized by the shared
 * concurrency group. Absent / corrupt / foreign-schema reads fail open to
 * "fresh pace" (undefined), exactly like a missing slot file.
 */
import type { IpPaceState } from '@classolek/shared';
import { SCHEMA_VERSION, ipPacePath, ipPacePrefix } from '@classolek/shared';
import { getJson, listKeys, putJson, type ObjectStore } from '../checkpoint/object-store.js';

/** One listed IP pace file: its key and (schema-valid) contents. */
export interface IpPaceEntry {
  key: string;
  state: IpPaceState;
}

export class PaceStateStore {
  constructor(private readonly store: ObjectStore) {}

  /** Load an IP's recent-acquire spend; absent/corrupt/foreign → undefined. */
  async load(league: string, ip: string): Promise<number[] | undefined> {
    let state: IpPaceState | undefined;
    try {
      state = await getJson<IpPaceState>(this.store, ipPacePath(league, ip));
    } catch {
      return undefined;
    }
    if (!state || state.schemaVersion !== SCHEMA_VERSION) return undefined;
    return state.recentAcquires;
  }

  async save(league: string, ip: string, recentAcquires: number[], nowIso: string): Promise<void> {
    const state: IpPaceState = {
      schemaVersion: SCHEMA_VERSION,
      ip,
      updatedAt: nowIso,
      recentAcquires,
    };
    await putJson(this.store, ipPacePath(league, ip), state);
  }

  /**
   * Every IP pace file for a league (key + parsed state). Corrupt/foreign files
   * are surfaced with `state: undefined` so the sweep can still reap them.
   */
  async list(league: string): Promise<{ key: string; state: IpPaceState | undefined }[]> {
    const keys = await listKeys(this.store, ipPacePrefix(league));
    const out: { key: string; state: IpPaceState | undefined }[] = [];
    for (const key of keys) {
      let state: IpPaceState | undefined;
      try {
        state = await getJson<IpPaceState>(this.store, key);
      } catch {
        state = undefined;
      }
      out.push({ key, state });
    }
    return out;
  }
}
