/**
 * Thin wrapper over @duckdb/node-api so the transform speaks in plain SQL and
 * plain rows, and the native-binding surface stays in one place (mirrors the
 * S3ObjectStore seam). This is the *only* transform file that imports the
 * DuckDB API; everything else composes SQL strings and reads back arrays.
 *
 * DuckDB returns 64-bit integers as JS BigInt (see duckdb-env.test.ts); the
 * `rows()` helper normalizes those to Number so aggregate counts are ordinary
 * numbers by the time they reach the contract types.
 */
import { DuckDBInstance } from '@duckdb/node-api';

type DuckValue = unknown;

/** A DuckDB list value exposes its elements under `.items`. */
function isListValue(value: unknown): value is { items: unknown[] } {
  return (
    typeof value === 'object' &&
    value !== null &&
    Array.isArray((value as { items?: unknown }).items)
  );
}

/** Recursively convert BigInt → Number and DuckDB list values → JS arrays. */
export function normalizeValue(value: DuckValue): unknown {
  if (typeof value === 'bigint') return Number(value);
  if (Array.isArray(value)) return value.map(normalizeValue);
  if (isListValue(value)) return value.items.map(normalizeValue);
  return value;
}

export interface DuckDbOptions {
  /** File-backed DB path (defaults to in-memory). Use a workDir path at scale. */
  path?: string;
  /** Directory DuckDB may spill intermediates to (bounds peak RAM at 15k depth). */
  tempDir?: string;
}

export class DuckDb {
  private constructor(private readonly conn: Awaited<ReturnType<DuckDBInstance['connect']>>) {}

  static async open(options: DuckDbOptions = {}): Promise<DuckDb> {
    const instance = await DuckDBInstance.create(options.path ?? ':memory:');
    const conn = await instance.connect();
    const db = new DuckDb(conn);
    if (options.tempDir) {
      await db.run(`SET temp_directory = '${options.tempDir.replace(/'/g, "''")}'`);
    }
    return db;
  }

  /** DROP a table to release its memory (e.g. after its Parquet is written). */
  async dropTable(table: string): Promise<void> {
    await this.run(`DROP TABLE IF EXISTS ${table}`);
  }

  /** Execute a statement for its side effects (CREATE TABLE, COPY, …). */
  async run(sql: string): Promise<void> {
    await this.conn.run(sql);
  }

  /** Execute a query and return normalized rows (BigInt→Number, lists→arrays). */
  async rows(sql: string): Promise<unknown[][]> {
    const reader = await this.conn.runAndReadAll(sql);
    return reader.getRows().map((row) => row.map(normalizeValue));
  }

  /** Convenience for `select count(*)`-shaped scalars. */
  async count(sql: string): Promise<number> {
    const [[n] = [0]] = await this.rows(sql);
    return Number(n ?? 0);
  }

  close(): void {
    this.conn.closeSync();
  }
}
