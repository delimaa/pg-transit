import { expect, test } from 'bun:test';
import type { SubscriptionMessage } from '../src';
import { createTestContext, randomName } from './utils';
import type { JSONValue } from 'postgres';

const { transit } = createTestContext();

test('Add a message with priority', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const message = await topic.send(
    { foo: 'bar' },
    {
      priority: 1,
    },
  );

  expect(message.priority).toBe(1);

  let consumed!: SubscriptionMessage<JSONValue>;
  const consumer = subscription.consume(async (message) => {
    consumed = message;
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed.priority).toBe(1);
});

test('Message with lower priority number is processed before message with higher priority number', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const message1 = await topic.send(
    { foo: 'bar' },
    {
      priority: 2,
    },
  );

  const message2 = await topic.send(
    { bar: 'baz' },
    {
      priority: 1,
    },
  );

  let consumed: SubscriptionMessage<JSONValue>[] = [];
  const consumer = subscription.consume(async (message) => {
    consumed.push(message);
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed).toHaveLength(2);
  expect(consumed[0]!.id).toBe(message2.id);
  expect(consumed[1]!.id).toBe(message1.id);
});

test('Message with priority is processed before messages without priority', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const message1 = await topic.send({ foo: 'bar' });

  const message2 = await topic.send(
    { bar: 'baz' },
    {
      priority: 1,
    },
  );

  let consumed: SubscriptionMessage<JSONValue>[] = [];
  const consumer = subscription.consume(async (message) => {
    consumed.push(message);
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed).toHaveLength(2);
  expect(consumed[0]!.id).toBe(message2.id);
  expect(consumed[1]!.id).toBe(message1.id);
});
