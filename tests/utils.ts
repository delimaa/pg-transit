import { afterAll, expect } from 'bun:test';
import type { Options } from 'postgres';
import { PgTransit, pgTransit, type PgTransitOptions } from '../src';

export function randomName(prefix: string) {
  return `${prefix}_${Bun.randomUUIDv7('base64url')}`;
}

export const TEST_CONNECTION_OPTIONS: Options<{}> = {
  host: 'localhost',
  port: 5429,
  user: 'postgres',
  password: 'postgres',
  database: 'pg_transit_test',
  onnotice: () => {},
};

const { host, port, user, password, database } = TEST_CONNECTION_OPTIONS;

export const TEST_CONNECTION_URL = `postgres://${user}:${password}@${host}:${port}/${database}`;

export function createTestContext(options?: PgTransitOptions) {
  const transits: PgTransit[] = [];
  const transit = pgTransit({ connection: TEST_CONNECTION_OPTIONS, ...options });
  transits.push(transit);

  afterAll(async () => {
    await Promise.all(transits.map((transit) => transit.close()));
  });

  return {
    transit,
    newPgTransit(options?: Omit<PgTransitOptions, 'connection'>) {
      const transit = pgTransit({ connection: TEST_CONNECTION_OPTIONS, ...options });
      transits.push(transit);
      return transit;
    },
  };
}

export function expectNumberToBeCloseTo(actual: number, expected: number, margin: number) {
  expect(actual).toBeGreaterThanOrEqual(expected - margin);
  expect(actual).toBeLessThanOrEqual(expected + margin);
}
