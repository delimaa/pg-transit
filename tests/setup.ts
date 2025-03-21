import postgres from 'postgres';
import { TEST_CONNECTION_OPTIONS } from './utils';

const sql = postgres({ ...TEST_CONNECTION_OPTIONS, database: 'postgres' });

await sql`
  DROP DATABASE IF EXISTS pg_transit_test
  WITH
    (FORCE);
`;
await sql`CREATE DATABASE pg_transit_test;`;

await sql.end();
