import { expect, setSystemTime, test } from 'bun:test';
import type { SubscriptionMessage } from '../src';
import { createTestContext, expectNumberToBeCloseTo, randomName } from './utils';
import type { JSONValue } from 'postgres';

const { transit } = createTestContext();

test('No retry by default', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const message = await topic.send({ foo: 'bar' });

  const consumer = subscription.consume(async () => {
    throw new Error('Error');
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  const failedMessages = await subscription.getMessages('failed');

  expect(failedMessages).toHaveLength(1);
  expect(failedMessages[0]!.id).toBe(message.id);
  expect(failedMessages[0]!.attempts).toBe(1);
  expect(failedMessages[0]!.maxAttempts).toBe(1);
});

test('Enable retry by specifying a number of attempts at subscription level', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    maxAttempts: 3,
  });

  await subscription.waitInit();

  const message = await topic.send({ foo: 'bar' });

  let consumed: SubscriptionMessage<JSONValue>[] = [];
  const consumer = subscription.consume(async (message) => {
    consumed.push(message);
    throw new Error('Error');
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed).toHaveLength(3);
  expect(consumed.map((m) => m.id)).toEqual([message.id, message.id, message.id]);
  expect(consumed[0]!.maxAttempts).toBe(3);
});

test('Linear retry backoff', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    maxAttempts: 2,
    retryStrategy: 'linear',
    retryDelayInMs: 10_000, // 10 seconds
  });

  await subscription.waitInit();

  const message = await topic.send({ foo: 'bar' });

  let consumed: SubscriptionMessage<JSONValue>[] = [];

  const consumer = subscription.consume((message) => {
    consumed.push(message);
    throw new Error('Error');
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed).toHaveLength(1);
  expect(consumed[0]!.id).toBe(message.id);
  expect(consumed[0]!.attempts).toBe(1);
  expect(consumed[0]!.maxAttempts).toBe(2);
  expectNumberToBeCloseTo(consumed[0]!.availableAt!.getTime(), Date.now() + 10_000, 100); // 10 seconds more or less 100ms

  setSystemTime(new Date(Date.now() + 10_000));

  await consumer.consume();

  expect(consumed).toHaveLength(2);

  expect(consumed[1]!.id).toBe(message.id);
  expect(consumed[1]!.availableAt).toBeUndefined();

  setSystemTime();
});

test('Exponential retry backoff', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    maxAttempts: 3,
    retryStrategy: 'exponential',
    retryDelayInMs: 10_000, // 10 seconds
  });

  await subscription.waitInit();

  const message = await topic.send({ foo: 'bar' });

  let consumed: SubscriptionMessage<JSONValue>[] = [];

  const consumer = subscription.consume((message) => {
    consumed.push(message);
    throw new Error('Error');
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed).toHaveLength(1);
  expect(consumed[0]!.id).toBe(message.id);
  expect(consumed[0]!.attempts).toBe(1);
  expect(consumed[0]!.maxAttempts).toBe(3);
  expectNumberToBeCloseTo(consumed[0]!.availableAt!.getTime(), Date.now() + 10_000, 100); // 10 seconds more or less 100ms

  setSystemTime(new Date(Date.now() + 10_000));

  await consumer.consume();

  expect(consumed).toHaveLength(2);
  expect(consumed[1]!.id).toBe(message.id);
  expect(consumed[1]!.attempts).toBe(2);
  expect(consumed[1]!.maxAttempts).toBe(3);
  expectNumberToBeCloseTo(consumed[1]!.availableAt!.getTime(), Date.now() + 20_000, 100); // 20 seconds more or less 100ms

  setSystemTime(new Date(Date.now() + 10_000));

  await consumer.consume();

  expect(consumed).toHaveLength(2); // Still 2 because not enough time has passed

  setSystemTime(new Date(Date.now() + 10_000));

  await consumer.consume();

  expect(consumed).toHaveLength(3);
  expect(consumed[2]!.id).toBe(message.id);
  expect(consumed[2]!.attempts).toBe(3);
  expect(consumed[2]!.maxAttempts).toBe(3);
  expect(consumed[2]!.availableAt).toBeUndefined();

  setSystemTime();
});

test('Message to retry is prioritized over new messages', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    maxAttempts: 2,
  });

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });
  await topic.send({ bar: 'baz' });

  let consumed: SubscriptionMessage<JSONValue>[] = [];

  const consumer = subscription.consume((message) => {
    consumed.push(message);
    throw new Error('Error');
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed).toHaveLength(4);
  expect(consumed[0]!.id).toBe(consumed[1]!.id);
  expect(consumed[2]!.id).toBe(consumed[3]!.id);
});
