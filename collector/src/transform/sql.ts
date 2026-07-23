/**
 * DuckDB SQL for the transform (decision #3: SQL does the heavy lifting).
 *
 * The input is one JSON object per line — the `ok` state-file lines transform.ts
 * streams back into the raw get-items / get-passive shape:
 *   { rank, account, character, class, level, fetchedAt,
 *     items:    <get-items response    { character, items[] }>,
 *     passives: <get-passive-skills    { hashes[] }> }
 *
 * `items`/`passives` are read as JSON columns and normalized with DuckDB's JSON
 * operators (`->`, `->>`, `CAST(json AS JSON[])` to unnest arrays). The unnested
 * item rows are materialized ONCE into `item_rows` (the biggest JSON parse) and
 * shared by the items / item_mods / skills tables, then dropped — see transform.ts
 * for the file-backed DB, table drops, and streaming used to bound peak memory.
 *
 * Dedup (defensive): the v4 state file holds exactly one line per (account,
 * character), so a cross-input duplicate can no longer arise the way an orphan
 * chunk shard once could. `chars` still keeps one row per (account, character),
 * the latest by fetchedAt, as a belt-and-suspenders guard that costs nothing
 * when the input is already unique.
 */
import type { AggregateKind } from '@classolek/shared';

/** Column spec for read_json over the streamed state-file NDJSON. */
const RAW_COLUMNS =
  "columns={rank:'INTEGER',account:'VARCHAR','character':'VARCHAR'," +
  "class:'VARCHAR',level:'INTEGER',fetchedAt:'VARCHAR',items:'JSON',passives:'JSON'}, " +
  "format='newline_delimited'";

/** The exact column list `chars` exposes (populated or empty), in order. */
const CHARS_EMPTY =
  'CREATE TABLE chars (character_key VARCHAR, rank INTEGER, account VARCHAR, ' +
  '"character" VARCHAR, class VARCHAR, level INTEGER, fetchedAt VARCHAR, items JSON, passives JSON);';

function sqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function fileListLiteral(paths: string[]): string {
  return `[${paths.map(sqlString).join(', ')}]`;
}

/**
 * Deduped source table, one row per character (latest fetchedAt wins). With no
 * input file, read_json([]) is a DuckDB binder error, so emit a typed empty
 * table instead — the validation gate then blocks publish cleanly.
 */
export function createCharsSql(inputFiles: string[]): string {
  if (inputFiles.length === 0) return CHARS_EMPTY;
  const files = fileListLiteral(inputFiles);
  return `CREATE TABLE chars AS
    WITH raw AS (
      SELECT * FROM read_json(${files}, ${RAW_COLUMNS})
    ),
    ranked AS (
      SELECT *, row_number() OVER (
        PARTITION BY account, character ORDER BY fetchedAt DESC
      ) AS _rn
      FROM raw
    )
    SELECT account || '/' || character AS character_key, * EXCLUDE (_rn)
    FROM ranked WHERE _rn = 1;`;
}

/** Tree nodes table read from the resolved-tree NDJSON (hash → name/stats/keystone). */
export function createTreeNodesSql(treeFile: string): string {
  return `CREATE TABLE tree_nodes AS SELECT * FROM read_json(${sqlString(treeFile)},
    columns={hash:'BIGINT',name:'VARCHAR',stats:'VARCHAR[]',is_keystone:'BOOLEAN'},
    format='newline_delimited');`;
}

/** The unnested-item rows shared by items / item_mods / skills (built once). */
export function createItemRowsSql(): string {
  return `CREATE TABLE item_rows AS
    SELECT character_key, unnest(CAST(items->'$.items' AS JSON[])) AS item
    FROM chars
    WHERE json_array_length(coalesce(items->'$.items', '[]')) > 0;`;
}

export function createCharactersSql(snapshotId: string): string {
  const id = sqlString(snapshotId);
  return `CREATE TABLE characters AS SELECT
      character_key,
      ${id} AS snapshot_id,
      rank,
      account,
      character,
      class,
      items->'$.character'->>'$.class' AS ascendancy,
      TRY_CAST(items->'$.character'->>'$.classId' AS INTEGER) AS class_id,
      TRY_CAST(items->'$.character'->>'$.ascendancyClass' AS INTEGER) AS ascendancy_class,
      level,
      TRY_CAST(items->'$.character'->>'$.experience' AS BIGINT) AS experience,
      fetchedAt AS fetched_at
    FROM chars ORDER BY rank;`;
}

/** One row per equipped item; `links` = size of the item's largest link group. */
export function createItemsSql(): string {
  return `CREATE TABLE items AS
    SELECT
      character_key,
      item->>'$.inventoryId' AS slot,
      item->>'$.name' AS name,
      coalesce(item->>'$.baseType', item->>'$.typeLine') AS base_type,
      item->>'$.typeLine' AS type_line,
      item->>'$.rarity' AS rarity,
      TRY_CAST(item->>'$.ilvl' AS INTEGER) AS ilvl,
      coalesce(TRY_CAST(item->>'$.corrupted' AS BOOLEAN), false) AS corrupted,
      CAST(item->'$.influences' AS VARCHAR) AS influences,
      -- Cast to INTEGER (count(*) is BIGINT) so the emitted column matches the
      -- shared detail-schema descriptor; link counts are tiny (0–6).
      CAST(coalesce((
        SELECT max(cnt) FROM (
          SELECT count(*) AS cnt
          FROM unnest(CAST(coalesce(item->'$.sockets', '[]') AS JSON[])) AS s(sock)
          GROUP BY sock->>'$.group'
        )
      ), 0) AS INTEGER) AS links
    FROM item_rows
    WHERE item->>'$.inventoryId' IS NOT NULL;`;
}

/** One row per mod, tagged by domain (implicit / explicit / crafted / enchant). */
export function createItemModsSql(): string {
  const domain = (field: string, label: string): string =>
    `SELECT character_key, item->>'$.inventoryId' AS item_key, ${sqlString(label)} AS mod_domain,
       unnest(CAST(coalesce(item->'$.${field}', '[]') AS VARCHAR[])) AS mod_text FROM item_rows`;
  return `CREATE TABLE item_mods AS
    ${domain('implicitMods', 'implicit')}
    UNION ALL
    ${domain('explicitMods', 'explicit')}
    UNION ALL
    ${domain('craftedMods', 'crafted')}
    UNION ALL
    ${domain('enchantMods', 'enchant')};`;
}

/**
 * One row per link group holding an active gem. The main-skill heuristic
 * (schema §6): the largest link group on a weapon or body armour, one per
 * character (`is_main`). Ties break deterministically by link size then gem name.
 */
export function createSkillsSql(): string {
  return `CREATE TABLE skills AS
    WITH socketed AS (
      SELECT character_key,
        item->>'$.inventoryId' AS slot,
        CAST(coalesce(item->'$.sockets', '[]') AS JSON[]) AS sockets,
        CAST(coalesce(item->'$.socketedItems', '[]') AS JSON[]) AS gems
      FROM item_rows
      WHERE json_array_length(coalesce(item->'$.socketedItems', '[]')) > 0
    ),
    gems AS (
      SELECT character_key, slot,
        CAST(sockets[CAST(g->>'$.socket' AS INTEGER) + 1]->>'$.group' AS INTEGER) AS link_group,
        (g->>'$.support') = 'true' AS is_support,
        g->>'$.typeLine' AS gem,
        TRY_CAST(regexp_extract((
          SELECT p->'$.values'->0->>0 FROM unnest(CAST(coalesce(g->'$.properties', '[]') AS JSON[])) AS pt(p)
          WHERE p->>'$.name' = 'Level' LIMIT 1), '\\d+') AS INTEGER) AS level,
        TRY_CAST(regexp_extract((
          SELECT p->'$.values'->0->>0 FROM unnest(CAST(coalesce(g->'$.properties', '[]') AS JSON[])) AS pt(p)
          WHERE p->>'$.name' = 'Quality' LIMIT 1), '\\d+') AS INTEGER) AS quality
      FROM socketed, unnest(gems) AS u(g)
    ),
    grouped AS (
      SELECT character_key, slot, link_group,
        max(CASE WHEN NOT is_support THEN gem END) AS gem,
        max(CASE WHEN NOT is_support THEN level END) AS level,
        max(CASE WHEN NOT is_support THEN quality END) AS quality,
        coalesce(list(gem) FILTER (WHERE is_support), []) AS supports,
        -- INTEGER (not the BIGINT count(*)) to match the shared schema descriptor.
        CAST(count(*) AS INTEGER) AS link_count
      FROM gems GROUP BY character_key, slot, link_group
      HAVING max(CASE WHEN NOT is_support THEN gem END) IS NOT NULL
    ),
    ranked AS (
      SELECT *, row_number() OVER (
        PARTITION BY character_key
        ORDER BY (slot IN ('Weapon', 'Weapon2', 'BodyArmour')) DESC, link_count DESC, gem
      ) AS _rn
      FROM grouped
    )
    SELECT character_key, slot, link_group, gem, level, quality, supports, link_count,
      (_rn = 1 AND slot IN ('Weapon', 'Weapon2', 'BodyArmour')) AS is_main
    FROM ranked;`;
}

/** One row per allocated passive, resolved against the tree (LEFT JOIN keeps unknowns). */
export function createPassivesSql(): string {
  return `CREATE TABLE passives AS
    WITH p AS (
      SELECT character_key, unnest(CAST(coalesce(passives->'$.hashes', '[]') AS BIGINT[])) AS node_hash
      FROM chars
    )
    SELECT p.character_key, p.node_hash,
      t.name AS node_name,
      coalesce(t.stats, []) AS node_stats,
      coalesce(t.is_keystone, false) AS is_keystone
    FROM p LEFT JOIN tree_nodes t ON p.node_hash = t.hash;`;
}

/** All normalized detail tables, in dependency order. */
export const DETAIL_TABLES = ['characters', 'items', 'item_mods', 'skills', 'passives'] as const;
export type DetailTable = (typeof DETAIL_TABLES)[number];

export function copyToParquetSql(table: string, path: string): string {
  return `COPY ${table} TO ${sqlString(path)} (FORMAT parquet, CODEC zstd);`;
}

/**
 * name/count aggregate queries (denominator applied in JS as the character
 * count). `satisfies` makes a missing AggregateKind a compile error instead of a
 * multi-hour-collection production crash.
 */
export const AGGREGATE_SQL = {
  class_distribution: `SELECT class AS name, count(*) AS count FROM characters
    WHERE class IS NOT NULL GROUP BY class ORDER BY count DESC, name`,
  skill_popularity: `SELECT gem AS name, count(*) AS count FROM skills
    WHERE is_main AND gem IS NOT NULL GROUP BY gem ORDER BY count DESC, name`,
  unique_usage: `SELECT name, count(DISTINCT character_key) AS count FROM items
    WHERE rarity = 'Unique' AND name IS NOT NULL AND name <> '' GROUP BY name ORDER BY count DESC, name`,
  keystone_usage: `SELECT node_name AS name, count(*) AS count FROM passives
    WHERE is_keystone AND node_name IS NOT NULL GROUP BY node_name ORDER BY count DESC, name`,
} satisfies Record<AggregateKind, string>;
