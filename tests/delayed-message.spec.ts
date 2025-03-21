import { expect, setSystemTime, test } from 'bun:test';
import type { JSONValue } from 'postgres';
import type { SubscriptionMessage } from '../src';
import { createTestContext, expectNumberToBeCloseTo, randomName } from './utils';

const { transit } = createTestContext();

test('Delay a message by a given amount of milliseconds', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const message = await topic.send(
    { foo: 'bar' },
    {
      deliverInMs: 10_000, // 10 seconds
    },
  );

  expectNumberToBeCloseTo(message.deliverAt!.getTime(), Date.now() + 10_000, 1000); // 10 seconds more or less 1 second

  let consumed: SubscriptionMessage<JSONValue> | undefined;
  const consumer = subscription.consume(async (message) => {
    consumed = message;
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed).toBeUndefined();

  setSystemTime(Date.now() + 10_000);

  await consumer.consume();

  expect(consumed).toBeDefined();
  expect(consumed!.id).toBe(message.id);
  expect(consumed!.availableAt).toEqual(message.deliverAt!);

  setSystemTime();
});

test('Delay a message by a given amount of milliseconds with a bulk send', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const messages = await topic.sendBulk([{ foo: 'bar' }, { bar: 'baz' }], {
    deliverInMs: 10_000, // 10 seconds
  });

  expect(messages[0]!.deliverAt).toBeDefined();
  expect(messages[1]!.deliverAt).toBeDefined();

  expectNumberToBeCloseTo(messages[0]!.deliverAt!.getTime(), Date.now() + 10_000, 1000); // 10 seconds more or less 1 second
  expectNumberToBeCloseTo(messages[1]!.deliverAt!.getTime(), Date.now() + 10_000, 1000); // 10 seconds more or less 1 second

  let consumed: SubscriptionMessage<JSONValue>[] = [];
  const consumer = subscription.consume(async (message) => {
    consumed.push(message);
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed).toHaveLength(0);

  setSystemTime(Date.now() + 10_000);

  await consumer.consume();

  expect(consumed).toHaveLength(2);
});

test('When retried, the delay is not applied again', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    maxAttempts: 2,
    retryStrategy: 'linear',
  });

  await subscription.waitInit();

  await topic.send(
    { foo: 'bar' },
    {
      deliverInMs: 10_000, // 10 seconds
    },
  );

  setSystemTime(Date.now() + 10_000);

  const consumed: SubscriptionMessage<JSONValue>[] = [];
  const consumer = subscription.consume(async (message) => {
    consumed.push(message);
    throw new Error('Retry');
  });

  await consumer.waitInit();
  await consumer.consume();

  expect(consumed).toHaveLength(2); // 2 attempts directly. The retry is made without waiting for the delay
});

test('Delay a message at a specific time', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const message = await topic.send(
    { foo: 'bar' },
    {
      deliverAt: new Date(Date.now() + 10_000), // 10 seconds later
    },
  );

  expectNumberToBeCloseTo(message.deliverAt!.getTime(), Date.now() + 10_000, 1000); // 10 seconds more or less 1 second

  let consumed: SubscriptionMessage<JSONValue> | undefined;
  const consumer = subscription.consume(async (message) => {
    consumed = message;
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed).toBeUndefined();

  setSystemTime(Date.now() + 10_000);

  await consumer.consume();

  expect(consumed).toBeDefined();
  expect(consumed!.id).toBe(message.id);
  expect(consumed!.availableAt).toEqual(message.deliverAt!);

  setSystemTime();
});

test('Delay a message at a specific time with a bulk send', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const messages = await topic.sendBulk([{ foo: 'bar' }, { bar: 'baz' }], {
    deliverAt: new Date(Date.now() + 10_000), // 10 seconds later
  });

  expect(messages[0]!.deliverAt).toBeDefined();
  expect(messages[1]!.deliverAt).toBeDefined();

  expectNumberToBeCloseTo(messages[0]!.deliverAt!.getTime(), Date.now() + 10_000, 1000); // 10 seconds more or less 1 second
  expectNumberToBeCloseTo(messages[1]!.deliverAt!.getTime(), Date.now() + 10_000, 1000); // 10 seconds more or less 1 second

  let consumed: SubscriptionMessage<JSONValue>[] = [];
  const consumer = subscription.consume(async (message) => {
    consumed.push(message);
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed).toHaveLength(0);

  setSystemTime(Date.now() + 10_000);

  await consumer.consume();

  expect(consumed).toHaveLength(2);

  setSystemTime();
});
