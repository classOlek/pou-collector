/**
 * Pins the REAL transform-emitted detail schema to the shared descriptor
 * (@classolek/shared DETAIL_TABLE_SCHEMA). This is the non-circular half of the
 * drift guard: the web derives its read schema and fixtures FROM the descriptor,
 * and this test asserts the transform's actual DuckDB output (column names +
 * types) matches it — so a transform change that alters a column or type fails CI
 * instead of silently breaking the web reader.
 *
 * The tables are built from the real CREATE-TABLE SQL over EMPTY inputs: DuckDB
 * infers column types from the SELECT expressions (not the data), so DESCRIBE
 * reveals exactly what a populated transform would COPY to Parquet.
 */
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { DETAIL_TABLE_SCHEMA } from '@classolek/shared';
import { DuckDb } from './duckdb.js';
import {
  createCharactersSql,
  createCharsSql,
  createItemModsSql,
  createItemRowsSql,
  createItemsSql,
  createMasteriesSql,
  createPassivesSql,
  createRawSql,
  createSkillsSql,
  createTreeNodesSql,
  DETAIL_TABLES,
} from './sql.js';

let db: DuckDb;
let workDir: string;

beforeAll(async () => {
  workDir = await mkdtemp(join(tmpdir(), 'pou-schema-'));
  const treeFile = join(workDir, 'tree.ndjson');
  await writeFile(
    treeFile,
    JSON.stringify({ hash: 1, name: 'x', stats: ['a'], is_keystone: true }) + '\n',
  );
  db = await DuckDb.open();
  await db.run(createCharsSql([])); // empty, typed `chars`
  await db.run(createTreeNodesSql(treeFile));
  await db.run(createItemRowsSql());
  await db.run(createCharactersSql('snapshot'));
  await db.run(createItemsSql());
  await db.run(createItemModsSql());
  await db.run(createSkillsSql());
  await db.run(createPassivesSql());
  await db.run(createMasteriesSql());
  await db.run(createRawSql());
});

afterAll(async () => {
  db?.close();
  if (workDir) await rm(workDir, { recursive: true, force: true });
});

describe('emitted detail schema matches the shared descriptor', () => {
  it('the descriptor covers exactly the transform tables (no added/dropped table)', () => {
    expect(DETAIL_TABLE_SCHEMA.map((t) => t.name)).toEqual([...DETAIL_TABLES]);
  });

  for (const table of DETAIL_TABLE_SCHEMA) {
    it(`${table.name}: columns and DuckDB types match`, async () => {
      // DESCRIBE columns: [column_name, column_type, null, key, default, extra].
      const rows = await db.rows(`DESCRIBE ${table.name}`);
      const actual = rows.map((r) => ({ name: String(r[0]), type: String(r[1]) }));
      expect(actual).toEqual(table.columns.map((c) => ({ name: c.name, type: c.type })));
    });
  }
});
