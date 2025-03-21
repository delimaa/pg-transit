import { expect, test } from 'bun:test';
import type { SubscriptionMessage } from '../src';
import { createTestContext, randomName } from './utils';
import type { JSONValue } from 'postgres';

const { transit } = createTestContext();

test('Send message', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const message = await topic.send({ foo: 'bar' });

  expect(message!.data).toEqual({ foo: 'bar' });
});

test('Send message in bulk preserves order', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const [message1, message2, message3] = await topic.sendBulk([{ foo: 'bar' }, { bar: 'baz' }, { baz: 'qux' }]);

  expect(message1!.data).toEqual({ foo: 'bar' });
  expect(message2!.data).toEqual({ bar: 'baz' });
  expect(message3!.data).toEqual({ baz: 'qux' });

  let consumedMessages: SubscriptionMessage<JSONValue>[] = [];

  const consumer = subscription.consume((message) => {
    consumedMessages.push(message);
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumedMessages.length).toBe(3);
  const [consumed1, consumed2, consumed3] = consumedMessages;
  expect(consumed1!.id).toEqual(message1!.id);
  expect(consumed2!.id).toEqual(message2!.id);
  expect(consumed3!.id).toEqual(message3!.id);
  expect(consumed1!.data).toEqual({ foo: 'bar' });
  expect(consumed2!.data).toEqual({ bar: 'baz' });
  expect(consumed3!.data).toEqual({ baz: 'qux' });
});
