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
      { name: 'slot', type: 'VARCHAR' },
      { name: 'name', type: 'VARCHAR' },
      { name: 'base_type', type: 'VARCHAR' },
      { name: 'type_line', type: 'VARCHAR' },
      { name: 'rarity', type: 'VARCHAR' },
      { name: 'ilvl', type: 'INTEGER' },
      { name: 'corrupted', type: 'BOOLEAN' },
      { name: 'influences', type: 'VARCHAR' },
      { name: 'links', type: 'INTEGER' },
    ],
  },
  {
    name: 'item_mods',
    columns: [
      { name: 'character_key', type: 'VARCHAR' },
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
    ],
  },
];

/** Logical table names, in transform order (sql.ts DETAIL_TABLES). */
export const DETAIL_TABLE_NAMES = DETAIL_TABLE_SCHEMA.map((t) => t.name);
