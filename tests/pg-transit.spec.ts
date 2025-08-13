import { test } from 'bun:test';
import { pgTransit } from '../src';
import { randomName, TEST_CONNECTION_OPTIONS, TEST_CONNECTION_URL } from './utils';

test('Create PgTransit instance from connection string', async () => {
  const transit = pgTransit({
    connection: {
      url: TEST_CONNECTION_URL,
      onnotice: () => {},
    },
  });

  const topic = transit.topic(randomName('topic'));

  await topic.waitInit();

  await transit.close();
});

test('Create PgTransit instance from connection options', async () => {
  const transit = pgTransit({
    connection: TEST_CONNECTION_OPTIONS,
  });

  const topic = transit.topic(randomName('topic'));

  await topic.waitInit();

  await transit.close();
});
