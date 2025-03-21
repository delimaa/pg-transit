import { expect, test } from 'bun:test';
import type { JSONValue } from 'postgres';
import type { SubscriptionMessage } from '../src';
import { createTestContext, randomName } from './utils';

const { transit, newTransit } = createTestContext();

test('By default, retention policy does not keep any acknowledged message', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const message = await topic.send({ foo: 'bar' });

  let consumedMessage!: SubscriptionMessage<JSONValue>;

  const consumer = subscription.consume((message) => {
    consumedMessage = message;
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumedMessage!.id).toBe(message.id);

  const trimmedCount = await transit.trimTopics();

  expect(trimmedCount).toBe(1);

  const messages = await topic.getMessages();
  expect(messages.length).toBe(0);

  const subscriptionMessages = await subscription.getMessages();
  expect(subscriptionMessages.length).toBe(0);
});

test('Messages are considered acknowledged and as such are trimmed when no subscription is attached to the topic', async () => {
  const topic = transit.topic(randomName('topic'));

  await topic.send({ foo: 'bar' });

  const trimmedCount = await transit.trimTopics();

  expect(trimmedCount).toBe(1);

  const messages = await topic.getMessages();
  expect(messages.length).toBe(0);
});

test('Do not trim unacknowledged messages', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });

  const trimmedCount = await transit.trimTopics();

  expect(trimmedCount).toBe(0);

  const messages = await topic.getMessages();
  expect(messages.length).toBe(1);

  const subscriptionMessages = await subscription.getMessages();
  expect(subscriptionMessages.length).toBe(1);
});

test('Keep a fixed number of acknowledged messages in the topic', async () => {
  const topic = transit.topic(randomName('topic'), {
    maxMessagesRetention: 1,
  });

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  await topic.send({ foo: 'bar' }); // First acknowledged message, trimmed
  const message2 = await topic.send({ bar: 'baz' }); // Second acknowledged message, not trimmed

  const consumer = subscription.consume(() => {
    // no-op
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  const message3 = await topic.send({ baz: 'qux' }); // Unacknowledged message, not trimmed

  const trimmedCount = await transit.trimTopics();

  expect(trimmedCount).toBe(1);

  const messages = await topic.getMessages();
  expect(messages.length).toBe(2);
  expect(messages[0]!.id).toEqual(message2.id); // Keep the last message acknowledged message
  expect(messages[1]!.id).toEqual(message3.id); // Keep the unacknowledged message

  const subscriptionMessages = await subscription.getMessages();
  expect(subscriptionMessages.length).toBe(2);
});

test('Infinite retention of acknowledged messages', async () => {
  const topic = transit.topic(randomName('topic'), {
    maxMessagesRetention: Infinity,
  });

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });

  const trimmedCount = await transit.trimTopics();

  expect(trimmedCount).toBe(0);

  const messages = await topic.getMessages();
  expect(messages.length).toBe(1);

  const subscriptionMessages = await subscription.getMessages();
  expect(subscriptionMessages.length).toBe(1);
});

test('Auto trim topics each 1 minutes by default', async () => {
  expect(transit.trimTopicsIntervalInMs).toBe(60_000);
});

test('Configure custom auto trim interval', async () => {
  const transit = newTransit({
    trimTopicsIntervalInMs: 10_000,
  });

  expect(transit.trimTopicsIntervalInMs).toBe(10_000);
});
