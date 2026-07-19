/**
 * Environment smoke test: proves the DuckDB engine used by the transform
 * (docs/ARCHITECTURE.md decision #3) actually runs in this container —
 * native binding loads, and the read_json -> Parquet path works end to end.
 */
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { DuckDBInstance } from '@duckdb/node-api';

let dir: string;

beforeAll(async () => {
  dir = await mkdtemp(join(tmpdir(), 'pou-duckdb-'));
});

afterAll(async () => {
  await rm(dir, { recursive: true, force: true });
});

describe('duckdb transform environment', () => {
  it('loads the native binding and evaluates SQL', async () => {
    const instance = await DuckDBInstance.create(':memory:');
    const conn = await instance.connect();
    const reader = await conn.runAndReadAll('select 21 * 2 as answer');
    expect(reader.getRows()).toEqual([[42]]);
    conn.closeSync();
  });

  it('round-trips NDJSON -> Parquet (zstd), the transform hot path', async () => {
    const ndjson = join(dir, 'chars.ndjson');
    const parquet = join(dir, 'chars.parquet');
    await writeFile(
      ndjson,
      ['{"rank":1,"character":"a","level":100}', '{"rank":2,"character":"b","level":98}'].join(
        '\n',
      ),
    );

    const instance = await DuckDBInstance.create(':memory:');
    const conn = await instance.connect();
    await conn.run(
      `copy (select * from read_json('${ndjson}')) to '${parquet}' (format parquet, codec zstd)`,
    );
    const reader = await conn.runAndReadAll(
      `select count(*) as n, max(level) as lvl from read_parquet('${parquet}')`,
    );
    expect(reader.getRows()).toEqual([[2n, 100n]]);
    conn.closeSync();
  });
});
