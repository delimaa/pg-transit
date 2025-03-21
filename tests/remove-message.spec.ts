import { expect, test } from 'bun:test';
import { createTestContext, randomName } from './utils';
import type { SubscriptionMessage } from '../src';
import type { JSONValue } from 'postgres';

const { transit } = createTestContext();

test('Remove message from topic', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const message = await topic.send({ foo: 'bar' });

  await message.remove();

  const messages = await topic.getMessages();
  expect(messages.length).toBe(0);

  const subscriptionMessages = await subscription.getMessages();
  expect(subscriptionMessages.length).toBe(0);
});

test('When removing a message and the message is processed by a subscription, the message should be removed from the subscription without error', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const message = await topic.send({ foo: 'bar' });

  let consumed!: SubscriptionMessage<JSONValue>;
  const consumer = subscription.consume(async (message) => {
    await Bun.sleep(10);
    consumed = message;
  });

  await consumer.waitInit();

  await Promise.all([consumer.waitIdle(), message.remove()]);

  expect(consumed).toBeDefined();
  expect(consumed.id).toBe(message.id);

  const messages = await topic.getMessages();
  expect(messages.length).toBe(0);

  const subscriptionMessages = await subscription.getMessages();
  expect(subscriptionMessages.length).toBe(0);
});
