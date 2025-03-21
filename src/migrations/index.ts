import type { Sql } from 'postgres';
import { migration1_0_0 } from './1.0.0';

export const migrations: { version: string; sql: string }[] = [
  {
    version: '1.0.0',
    sql: migration1_0_0,
  },
];

export async function runMigrations(sql: Sql) {
  await sql.begin(async (sql) => {
    await sql`
      SELECT
        pg_advisory_xact_lock(829049)
    `; // Randomly choosen lock ID, no special meaning

    await sql`
      CREATE TABLE IF NOT EXISTS pg_transit_migrations (
        id SERIAL PRIMARY KEY,
        version TEXT NOT NULL UNIQUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `;

    const executedMigrations: { version: string }[] = await sql`
      SELECT
        version
      FROM
        pg_transit_migrations
    `;

    const executedVersions = new Set(executedMigrations.map((m) => m.version));

    const notExecutedMigrations = migrations.filter((migration) => !executedVersions.has(migration.version));

    for (const migration of notExecutedMigrations) {
      await sql`${sql.unsafe(migration.sql)}`.simple();

      await sql`
        INSERT INTO
          pg_transit_migrations ${sql({ version: migration.version })}
      `;
    }
  });
}
