import { expect, test } from 'bun:test';
import type { SubscriptionMessage } from '../src';
import { createTestContext, randomName } from './utils';
import type { JSONValue } from 'postgres';

const { transit } = createTestContext();

test('Consume messages from the beginning of the topic', async () => {
  const topic = transit.topic(randomName('topic'));

  const message1 = await topic.send({ foo: 'bar' });
  const message2 = await topic.send({ bar: 'baz' });

  const subscription = topic.subscribe(randomName('subscription'), {
    startPosition: 'earliest',
  });

  let consumedMessages: SubscriptionMessage<JSONValue>[] = [];

  const consumer = subscription.consume((message) => {
    consumedMessages.push(message);
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumedMessages.length).toBe(2);
  const [consumed1, consumed2] = consumedMessages;
  expect(consumed1!.id).toBe(message1.id);
  expect(consumed1!.data).toEqual({ foo: 'bar' });
  expect(consumed2!.id).toBe(message2.id);
  expect(consumed2!.data).toEqual({ bar: 'baz' });
});

test('Consume messages from the latest position of the topic by default', async () => {
  const topic = transit.topic(randomName('topic'));

  await topic.send({ foo: 'bar' });

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const message2 = await topic.send({ bar: 'baz' });

  let consumedMessages: SubscriptionMessage<JSONValue>[] = [];

  const consumer = subscription.consume((message) => {
    consumedMessages.push(message);
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumedMessages.length).toBe(1);
  const [consumed] = consumedMessages;
  expect(consumed!.id).toBe(message2.id);
  expect(consumed!.data).toEqual({ bar: 'baz' });
});
