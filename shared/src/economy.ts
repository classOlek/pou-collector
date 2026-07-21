/**
 * poe.ninja economy snapshot contract (economy/<league>.json, public).
 *
 * One file per league, holding every documented poe1 economy category from
 * https://poe.ninja/docs/api in a single fetch pass. The file is a CACHE, not
 * an archive: each ECONOMY POE.NINJA workflow fire overwrites it in place
 * (unlike build snapshots, which are immutable once complete — hard rule #4
 * covers snapshots/, not this prefix).
 *
 * Versioned independently of the build-data SCHEMA_VERSION: the economy file
 * carries pass-through poe.ninja responses and has no coupling to the
 * snapshot/roster contracts, so a bump here never forces a build-schema bump
 * (and vice versa).
 */
export const ECONOMY_SCHEMA_VERSION = 1;

/**
 * The three documented poe1 economy endpoint groups (poe.ninja/docs/api):
 *   exchange      — /poe1/api/economy/exchange/current/overview
 *   stashItem     — /poe1/api/economy/stash/current/item/overview
 *   stashCurrency — /poe1/api/economy/stash/current/currency/overview
 */
export type EconomyEndpointKey = 'exchange' | 'stashItem' | 'stashCurrency';

export const ECONOMY_ENDPOINT_KEYS: readonly EconomyEndpointKey[] = [
  'exchange',
  'stashItem',
  'stashCurrency',
];

/** The economy cache file at economy/<league>.json (public). */
export interface EconomySnapshotFile {
  schemaVersion: typeof ECONOMY_SCHEMA_VERSION;
  game: 'poe1';
  league: string;
  /** When this fetch pass ran; every category was fetched within one pass. */
  fetchedAt: string;
  /**
   * Endpoint group → poe.ninja `type` value → that endpoint's raw JSON
   * response, passed through untransformed so readers see exactly what
   * poe.ninja served (the response shapes are theirs to evolve).
   */
  categories: Record<EconomyEndpointKey, Record<string, unknown>>;
}
