import { expect, test } from 'bun:test';
import type { JSONValue } from 'postgres';
import type { SubscriptionMessage } from '../src';
import { createTestContext, randomName } from './utils';

const { transit } = createTestContext();

test('Using concurrency options, a consumer can consume multiple messages at once on a parallel subscription', async () => {
  const topic = transit.topic('topic');

  const subscription = topic.subscribe(randomName('subscription'), {
    consumptionMode: 'parallel',
  });

  await subscription.waitInit();

  await topic.send('foo');
  await topic.send('bar');

  let consumingMessages: SubscriptionMessage<JSONValue>[] = [];
  const consumingMessagesLengthHistory: number[] = [0];

  const consumer = subscription.consume(
    async (message) => {
      consumingMessages.push(message);
      consumingMessagesLengthHistory.push(consumingMessages.length);
      await Bun.sleep(5);
      consumingMessages = consumingMessages.filter((m) => m.id !== message.id);
      consumingMessagesLengthHistory.push(consumingMessages.length);
    },
    {
      concurrency: 2,
    },
  );

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumingMessagesLengthHistory).toEqual([0, 1, 2, 1, 0]);
});

test('Concurrency is ignored when consuming messages from a sequential subscription', async () => {
  const topic = transit.topic('topic');

  const subscription = topic.subscribe(randomName('subscription'), {
    consumptionMode: 'sequential',
  });

  await subscription.waitInit();

  await topic.send('foo');
  await topic.send('bar');

  let consumingMessages: SubscriptionMessage<JSONValue>[] = [];
  const consumingMessagesLengthHistory: number[] = [0];

  const consumer = subscription.consume(
    async (message) => {
      consumingMessages.push(message);
      consumingMessagesLengthHistory.push(consumingMessages.length);
      await Bun.sleep(5);
      consumingMessages = consumingMessages.filter((m) => m.id !== message.id);
      consumingMessagesLengthHistory.push(consumingMessages.length);
    },
    {
      concurrency: 2,
    },
  );

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumingMessagesLengthHistory).toEqual([0, 1, 0, 1, 0]);
});

test('Concurrency defaults to 1', async () => {
  const topic = transit.topic('topic');

  const subscription = topic.subscribe(randomName('subscription'), {
    consumptionMode: 'parallel',
  });

  const consumer = subscription.consume(async () => {});

  await consumer.waitInit();

  expect(consumer.concurrency).toBe(1);
});

test('Autostart consumer', async () => {
  const topic = transit.topic('topic');

  const subscription = topic.subscribe(randomName('subscription'));

  const consumer = subscription.consume(async () => {}, {
    autostart: true,
  });

  await consumer.waitInit();

  expect(consumer.isStarted).toBe(true);
  expect(consumer.isIdle).toBe(false);
});

test('Do not autostart consumer', async () => {
  const topic = transit.topic('topic');

  const subscription = topic.subscribe(randomName('subscription'));

  const consumer = subscription.consume(async () => {}, {
    autostart: false,
  });

  await consumer.waitInit();

  expect(consumer.isStarted).toBe(false);
  expect(consumer.isIdle).toBe(true);
});

test('Autostart consumer by default', async () => {
  const topic = transit.topic('topic');

  const subscription = topic.subscribe(randomName('subscription'));

  const consumer = subscription.consume(async () => {});

  await consumer.waitInit();

  expect(consumer.isStarted).toBe(true);
  expect(consumer.isIdle).toBe(false);
});

test('Default polling interval is 1 second', async () => {
  const topic = transit.topic('topic');

  const subscription = topic.subscribe(randomName('subscription'));

  const consumer = subscription.consume(async () => {});

  expect(consumer.pollingIntervalInMs).toBe(1000);
});

test('Set custom polling interval', async () => {
  const topic = transit.topic('topic');

  const subscription = topic.subscribe(randomName('subscription'));

  const consumer = subscription.consume(async () => {}, {
    pollingIntervalInMs: 500,
  });

  expect(consumer.pollingIntervalInMs).toBe(500);
});
