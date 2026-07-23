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

/**
 * One row per item held in ANY inventory slot — worn gear AND the passive-tree
 * jewels (`inventoryId = 'PassiveJewels'`, incl. cluster jewels): the only filter
 * is a present `inventoryId`, nothing is slot-whitelisted. `links` = size of the
 * item's largest link group. v5 adds the per-item detail the v4 columns dropped:
 * `sockets` (the raw GGG sockets array as JSON text — colours + groups, lossless),
 * `quality`/`req_level` (parsed from properties/requirements) and GGG's item flag
 * booleans (absent => false).
 */
export function createItemsSql(): string {
  const flag = (field: string): string =>
    `coalesce(TRY_CAST(item->>'$.${field}' AS BOOLEAN), false)`;
  // First value string of the first properties/requirements entry named `label`.
  const firstValue = (field: string, label: string): string =>
    `(SELECT e->'$.values'->0->>0
        FROM unnest(CAST(coalesce(item->'$.${field}', '[]') AS JSON[])) AS et(e)
        WHERE e->>'$.name' = ${sqlString(label)} LIMIT 1)`;
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
      ), 0) AS INTEGER) AS links,
      -- v5: quality % and level requirement, parsed from the properties arrays.
      TRY_CAST(regexp_extract(${firstValue('properties', 'Quality')}, '\\d+') AS INTEGER) AS quality,
      -- sockets kept verbatim (JSON text) so colours + link groups survive intact.
      CAST(item->'$.sockets' AS VARCHAR) AS sockets,
      TRY_CAST(regexp_extract(${firstValue('requirements', 'Level')}, '\\d+') AS INTEGER) AS req_level,
      ${flag('identified')} AS identified,
      ${flag('fractured')} AS fractured,
      ${flag('synthesised')} AS synthesised,
      ${flag('mirrored')} AS mirrored,
      ${flag('split')} AS split
    FROM item_rows
    WHERE item->>'$.inventoryId' IS NOT NULL;`;
}

/**
 * One row per mod, tagged by domain. v5 widens coverage past the original four
 * (implicit/explicit/crafted/enchant) to every flat string-array mod list GGG
 * ships: `fractured`, `veiled`, `scourge`, and `utility` (flask action mods,
 * previously dropped entirely). Each list is a VARCHAR[]; a missing list yields
 * no rows. (Nested mod structures like `crucible` stay in the raw safety net.)
 */
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
    ${domain('enchantMods', 'enchant')}
    UNION ALL
    ${domain('fracturedMods', 'fractured')}
    UNION ALL
    ${domain('veiledMods', 'veiled')}
    UNION ALL
    ${domain('scourgeMods', 'scourge')}
    UNION ALL
    ${domain('utilityMods', 'utility')};`;
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

/**
 * One row per allocated passive. v5 unions the base tree (`$.hashes`, source
 * `'tree'`, resolved against `tree_nodes`) with cluster / expansion-jewel nodes
 * (`$.hashes_ex`, source `'cluster'`). Cluster nodes are NOT in the base tree —
 * their names/stats live inside each jewel's `jewel_data[socket].subgraph.nodes`
 * (keyed by node hash) — so `cluster_map` flattens every socket's subgraph and
 * the cluster branch LEFT JOINs onto it. Both LEFT JOINs keep an allocated hash
 * even when unresolved (NULL name), never dropping an allocation. `is_notable`
 * flags cluster notables (always false for tree nodes; keystone-ness is the tree
 * signal there).
 */
export function createPassivesSql(): string {
  return `CREATE TABLE passives AS
    WITH tree_alloc AS (
      SELECT character_key,
        unnest(CAST(coalesce(passives->'$.hashes', '[]') AS BIGINT[])) AS node_hash
      FROM chars
    ),
    cluster_alloc AS (
      SELECT character_key,
        unnest(CAST(coalesce(passives->'$.hashes_ex', '[]') AS BIGINT[])) AS node_hash
      FROM chars
    ),
    -- One row per (character, jewel socket): that socket's subgraph node object.
    jewel_sockets AS (
      SELECT c.character_key,
        json_extract(json_extract(c.passives, '$.jewel_data'),
          '$."' || sk.socket_key || '"')->'$.subgraph'->'$.nodes' AS nodes
      FROM chars c,
        unnest(coalesce(json_keys(json_extract(c.passives, '$.jewel_data')), [])) AS sk(socket_key)
    ),
    -- One row per cluster-generated node, resolved from the subgraph.
    cluster_map AS (
      SELECT js.character_key,
        TRY_CAST(nk.node_key AS BIGINT) AS node_hash,
        json_extract(js.nodes, '$."' || nk.node_key || '"') AS node
      FROM jewel_sockets js,
        unnest(coalesce(json_keys(js.nodes), [])) AS nk(node_key)
      WHERE js.nodes IS NOT NULL
    )
    SELECT ta.character_key, ta.node_hash,
      t.name AS node_name,
      coalesce(t.stats, []) AS node_stats,
      coalesce(t.is_keystone, false) AS is_keystone,
      'tree' AS source,
      false AS is_notable
    FROM tree_alloc ta LEFT JOIN tree_nodes t ON ta.node_hash = t.hash
    UNION ALL
    SELECT ca.character_key, ca.node_hash,
      cm.node->>'$.name' AS node_name,
      CAST(coalesce(cm.node->'$.stats', '[]') AS VARCHAR[]) AS node_stats,
      false AS is_keystone,
      'cluster' AS source,
      coalesce(TRY_CAST(cm.node->>'$.isNotable' AS BOOLEAN), false) AS is_notable
    FROM cluster_alloc ca
      LEFT JOIN cluster_map cm
        ON ca.character_key = cm.character_key AND ca.node_hash = cm.node_hash;`;
}

/**
 * One row per chosen passive mastery. GGG's `mastery_effects` maps a mastery node
 * hash → the chosen effect hash; this flattens that object so the CHOICE is never
 * lost. `effect_stats` is left empty for now — the effect→stat text lives in the
 * tree's mastery-effect table (not `tree_nodes`), so resolving it is a follow-up;
 * the full `mastery_effects` object is preserved verbatim in the `raw` table.
 */
export function createMasteriesSql(): string {
  return `CREATE TABLE masteries AS
    SELECT c.character_key,
      TRY_CAST(mk.node_key AS BIGINT) AS node_hash,
      TRY_CAST(json_extract_string(json_extract(c.passives, '$.mastery_effects'),
        '$."' || mk.node_key || '"') AS BIGINT) AS effect_hash,
      CAST([] AS VARCHAR[]) AS effect_stats
    FROM chars c,
      unnest(coalesce(json_keys(json_extract(c.passives, '$.mastery_effects')), [])) AS mk(node_key);`;
}

/**
 * The verbatim GGG payloads per character (items + passives responses as JSON
 * text). The never-skip safety net: any field the normalized tables don't surface
 * (nested crucible mods, socketed abyss jewels, incubators, full jewel_data …)
 * is always recoverable here. Its own Parquet file, fetched only on demand.
 */
export function createRawSql(): string {
  return `CREATE TABLE raw AS
    SELECT character_key,
      CAST(items AS VARCHAR) AS items,
      CAST(passives AS VARCHAR) AS passives
    FROM chars;`;
}

/** All normalized detail tables, in dependency order (must match DETAIL_TABLE_SCHEMA). */
export const DETAIL_TABLES = [
  'characters',
  'items',
  'item_mods',
  'skills',
  'passives',
  'masteries',
  'raw',
] as const;
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
