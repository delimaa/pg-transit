import type { Options } from 'postgres';
import postgres from 'postgres';
import { pgTransit } from '../src';

export const BENCH_POSTGRES_OPTIONS: Options<{}> = {
  host: 'localhost',
  port: 5429,
  user: 'postgres',
  password: 'postgres',
  database: 'pg_transit_bench',
  onnotice: () => {},
};

export function randomName(prefix: string) {
  return `${prefix}_${Bun.randomUUIDv7('base64url')}`;
}

export async function createBenchContext() {
  const sql = postgres({ ...BENCH_POSTGRES_OPTIONS, database: 'postgres' });
  await sql`
    DROP DATABASE IF EXISTS pg_transit_bench
    WITH
      (FORCE);
  `;
  await sql`CREATE DATABASE pg_transit_bench;`;
  await sql.end();

  const transit = pgTransit({ connection: BENCH_POSTGRES_OPTIONS });

  return {
    transit,
  };
}
