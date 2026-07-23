/**
 * The detail-Parquet table schema — the SINGLE OWNER of the per-table column list
 * and DuckDB types (docs/ARCHITECTURE.md §6). The collector's transform emits
 * these columns and a collector test asserts the REAL emitted Parquet matches this
 * descriptor; the web derives its DETAIL_SCHEMA (adds UI notes) and its fixture
 * column specs from it. So the collector, the web reader, and the fixtures can
 * never silently drift — a change here is a schema change (bump SCHEMA_VERSION).
 *
 * Types are DuckDB type strings exactly as `DESCRIBE` reports them for the
 * transform's emitted tables (collector/src/transform/sql.ts).
 */

export interface DetailColumnSchema {
  name: string;
  /** DuckDB type as `DESCRIBE` reports it (VARCHAR, INTEGER, BIGINT, BOOLEAN, VARCHAR[]). */
  type: string;
}

export interface DetailTableSchema {
  name: string;
  columns: DetailColumnSchema[];
}

export const DETAIL_TABLE_SCHEMA: readonly DetailTableSchema[] = [
  {
    name: 'characters',
    columns: [
      { name: 'character_key', type: 'VARCHAR' },
      { name: 'snapshot_id', type: 'VARCHAR' },
      { name: 'rank', type: 'INTEGER' },
      { name: 'account', type: 'VARCHAR' },
      { name: 'character', type: 'VARCHAR' },
      { name: 'class', type: 'VARCHAR' },
      { name: 'ascendancy', type: 'VARCHAR' },
      { name: 'class_id', type: 'INTEGER' },
      { name: 'ascendancy_class', type: 'INTEGER' },
      { name: 'level', type: 'INTEGER' },
      { name: 'experience', type: 'BIGINT' },
      { name: 'fetched_at', type: 'VARCHAR' },
    ],
  },
  {
    name: 'items',
    columns: [
      { name: 'character_key', type: 'VARCHAR' },
      // v6: stable per-item id (`character_key || '#' || <items-array ordinal>`),
      // unique per (character, item). `item_mods.item_key` carries this same value
      // so mods join to the exact item. `slot` (below) stays the `inventoryId`.
      { name: 'item_id', type: 'VARCHAR' },
      { name: 'slot', type: 'VARCHAR' },
      { name: 'name', type: 'VARCHAR' },
      { name: 'base_type', type: 'VARCHAR' },
      { name: 'type_line', type: 'VARCHAR' },
      { name: 'rarity', type: 'VARCHAR' },
      { name: 'ilvl', type: 'INTEGER' },
      { name: 'corrupted', type: 'BOOLEAN' },
      { name: 'influences', type: 'VARCHAR' },
      { name: 'links', type: 'INTEGER' },
      // v5: per-item detail the v4 transform dropped. `sockets` is the raw GGG
      // sockets array as JSON text (colours + link groups, lossless); `quality`
      // and `req_level` are parsed from properties/requirements; the flags are
      // GGG's item booleans (absent => false).
      { name: 'quality', type: 'INTEGER' },
      { name: 'sockets', type: 'VARCHAR' },
      { name: 'req_level', type: 'INTEGER' },
      { name: 'identified', type: 'BOOLEAN' },
      { name: 'fractured', type: 'BOOLEAN' },
      { name: 'synthesised', type: 'BOOLEAN' },
      { name: 'mirrored', type: 'BOOLEAN' },
      { name: 'split', type: 'BOOLEAN' },
    ],
  },
  {
    name: 'item_mods',
    columns: [
      { name: 'character_key', type: 'VARCHAR' },
      // v6: carries the owning item's per-item `item_id` (see items.item_id) — no
      // longer the `inventoryId`. Join `item_mods.item_key = items.item_id` to
      // attribute each mod to the exact item (fixes flask/jewel mod pooling).
      { name: 'item_key', type: 'VARCHAR' },
      { name: 'mod_domain', type: 'VARCHAR' },
      { name: 'mod_text', type: 'VARCHAR' },
    ],
  },
  {
    name: 'skills',
    columns: [
      { name: 'character_key', type: 'VARCHAR' },
      { name: 'slot', type: 'VARCHAR' },
      { name: 'link_group', type: 'INTEGER' },
      { name: 'gem', type: 'VARCHAR' },
      { name: 'level', type: 'INTEGER' },
      { name: 'quality', type: 'INTEGER' },
      { name: 'supports', type: 'VARCHAR[]' },
      { name: 'link_count', type: 'INTEGER' },
      { name: 'is_main', type: 'BOOLEAN' },
    ],
  },
  {
    name: 'passives',
    columns: [
      { name: 'character_key', type: 'VARCHAR' },
      { name: 'node_hash', type: 'BIGINT' },
      { name: 'node_name', type: 'VARCHAR' },
      { name: 'node_stats', type: 'VARCHAR[]' },
      { name: 'is_keystone', type: 'BOOLEAN' },
      // v5: `source` distinguishes base-tree nodes ('tree', from `hashes`) from
      // cluster-jewel / expansion nodes ('cluster', from `hashes_ex`). Cluster
      // node names/stats resolve from each jewel's `jewel_data.subgraph`, not the
      // base tree, so `is_notable` flags cluster notables (false for tree nodes).
      { name: 'source', type: 'VARCHAR' },
      { name: 'is_notable', type: 'BOOLEAN' },
    ],
  },
  {
    // v5: one row per chosen passive mastery (GGG `mastery_effects`: mastery node
    // hash -> chosen effect hash). `effect_stats` resolves from the tree when the
    // effect hash is known, else empty.
    name: 'masteries',
    columns: [
      { name: 'character_key', type: 'VARCHAR' },
      { name: 'node_hash', type: 'BIGINT' },
      { name: 'effect_hash', type: 'BIGINT' },
      { name: 'effect_stats', type: 'VARCHAR[]' },
    ],
  },
  {
    // v5: the verbatim GGG payloads per character (items + passives responses as
    // JSON text) — the never-skip safety net for any field the normalized tables
    // don't yet surface. Its own Parquet file, fetched only on demand (never for
    // aggregate queries), so it doesn't weigh on the analytical tables.
    name: 'raw',
    columns: [
      { name: 'character_key', type: 'VARCHAR' },
      { name: 'items', type: 'VARCHAR' },
      { name: 'passives', type: 'VARCHAR' },
    ],
  },
];

/** Logical table names, in transform order (sql.ts DETAIL_TABLES). */
export const DETAIL_TABLE_NAMES = DETAIL_TABLE_SCHEMA.map((t) => t.name);
