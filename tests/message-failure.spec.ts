import { expect, test } from 'bun:test';
import type { SubscriptionMessage } from '../src';
import { createTestContext, randomName } from './utils';
import type { JSONValue } from 'postgres';

const { transit } = createTestContext();

test('Mark message as failed', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });

  let consumed: SubscriptionMessage<JSONValue>[] = [];

  const consumer = subscription.consume(async (message) => {
    consumed.push(message);
    throw new Error('Error');
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed).toHaveLength(1);
  expect(consumed[0]!.status).toBe('failed');

  const failedMessages = await subscription.getMessages('failed');
  expect(failedMessages).toHaveLength(1);
});

test('Register error stack trace on failed message', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });

  let consumed: SubscriptionMessage<JSONValue>[] = [];

  const consumer = subscription.consume(async (message) => {
    consumed.push(message);
    throw new Error('Boom');
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed).toHaveLength(1);
  expect(consumed[0]!.errorStack).toBeDefined();
});

test('Message is failed once all attempts are exhausted', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    maxAttempts: 3,
  });

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });

  let consumed: SubscriptionMessage<JSONValue>[] = [];
  const consumer = subscription.consume(async (message) => {
    consumed.push(message);
    throw new Error('Error');
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed).toHaveLength(3);
  expect(consumed[2]!.attempts).toBe(3);
  expect(consumed[2]!.maxAttempts).toBe(3);
  expect(consumed[2]!.status).toBe('failed');

  const failedMessages = await subscription.getMessages('failed');
  expect(failedMessages).toHaveLength(1);
});

test('Manually retry failed message', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });

  let consumed!: SubscriptionMessage<JSONValue>;
  let shouldFail = true;
  const consumer = subscription.consume(async (message) => {
    if (shouldFail) {
      shouldFail = false;
      throw new Error('Error');
    }
    consumed = message;
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  let failedMessages = await subscription.getMessages('failed');

  expect(failedMessages).toHaveLength(1);

  const [message] = failedMessages;

  await message!.retry();

  expect(message!.status).toBe('waiting'); // Pushed back to waiting state
  expect(message!.errorStack).toBeUndefined(); // Error stack is removed

  await consumer.consume();

  expect(consumed.id).toBe(message!.id);
  expect(consumed.status).toBe('completed');
  expect(consumed.attempts).toBe(2);
  expect(consumed.maxAttempts).toBe(1); // Max attempts does not change

  failedMessages = await subscription.getMessages('failed');
  expect(failedMessages).toHaveLength(0);
});
