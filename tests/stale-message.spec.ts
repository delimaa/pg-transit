import { expect, setSystemTime, test } from 'bun:test';
import type { JSONValue } from 'postgres';
import type { SubscriptionMessage } from '../src';
import { createTestContext, randomName } from './utils';

const { transit, newTransit } = createTestContext();

test('Reset stale messages', async () => {
  transit.staleMessageTimeoutInMs = 1;

  let onceStaleMessageId!: string;
  let onceStaleSubscriptionId!: string;
  transit.once('stale', ({ messageId, subscriptionId }) => {
    onceStaleMessageId = messageId;
    onceStaleSubscriptionId = subscriptionId;
  });

  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    consumptionMode: 'parallel',
  });

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });

  const consumed: SubscriptionMessage<JSONValue>[] = [];
  const consumer1 = subscription.consume(async (message) => {
    consumed.push(message);
    await Bun.sleep(50);
  });
  const consumer2 = subscription.consume(async (message) => {
    consumed.push(message);
    await Bun.sleep(50);
  });

  await Promise.all([consumer1.waitInit(), consumer2.waitInit()]);

  await Bun.sleep(10); // Be sure the message is picked up by the first consumer

  const resetCount = await transit.resetStaleMessages();

  expect(resetCount).toBe(1);

  await Promise.all([consumer1.consume(), consumer2.consume()]);

  expect(consumed).toHaveLength(2);
  expect(consumed[0]!.id).toBe(consumed[1]!.id);
  expect(consumed[0]!.staleCount).toBe(0);
  expect(consumed[1]!.staleCount).toBe(1);

  expect(onceStaleMessageId).toBe(consumed[0]!.id);
  expect(onceStaleSubscriptionId).toBe(subscription.id);
});

test('Do not reset not stale messages', async () => {
  transit.staleMessageTimeoutInMs = 1_000;

  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    consumptionMode: 'parallel',
  });

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });

  const consumed: SubscriptionMessage<JSONValue>[] = [];
  const consumer1 = subscription.consume(async (message) => {
    consumed.push(message);
    await Bun.sleep(50);
  });
  const consumer2 = subscription.consume(async (message) => {
    consumed.push(message);
    await Bun.sleep(50);
  });

  await Promise.all([consumer1.waitInit(), consumer2.waitInit()]);

  await Bun.sleep(5); // Be sure the message is picked up by the first consumer

  const resetCount = await transit.resetStaleMessages();

  expect(resetCount).toBe(0);

  await Promise.all([consumer1.consume(), consumer2.consume()]);

  expect(consumed).toHaveLength(1);

  setSystemTime();
});

test('Send heartbeat at regular interval to keep message alive', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });

  let consumed!: SubscriptionMessage<JSONValue>;
  const consumer = subscription.consume(
    async (message) => {
      consumed = message;
      await Bun.sleep(50);
    },
    {
      heartbeatIntervalInMs: 5,
    },
  );

  await consumer.waitInit();
  await Bun.sleep(10); // Be sure the message is picked up by the consumer

  const lastHeartbeatAt1 = consumed.lastHeartbeatAt!;

  await Bun.sleep(10); // Wait for new heartbeat to be sent

  const lastHeartbeatAt2 = consumed.lastHeartbeatAt!;

  await Bun.sleep(10); // Wait for new heartbeat to be sent

  const lastHeartbeatAt3 = consumed.lastHeartbeatAt!;

  expect(lastHeartbeatAt2.getTime()).toBeGreaterThan(lastHeartbeatAt1.getTime());
  expect(lastHeartbeatAt3.getTime()).toBeGreaterThan(lastHeartbeatAt2.getTime());
});

test('Configure reset stale message interval', async () => {
  const transit = newTransit({
    resetStaleMessagesIntervalInMs: 10_000,
  });

  expect(transit.resetStaleMessagesIntervalInMs).toBe(10_000);
});

test('Stale message on sequential subscription does not lock subscription', async () => {
  transit.staleMessageTimeoutInMs = 1;

  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    consumptionMode: 'sequential',
  });

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });
  await topic.send({ bar: 'baz' });

  const consumed: SubscriptionMessage<JSONValue>[] = [];
  const handler = async (message: SubscriptionMessage<JSONValue>) => {
    consumed.push(message);

    // Stale on first message
    if (consumed.length === 1) {
      await Bun.sleep(100);
      return;
    }
  };
  const consumer1 = subscription.consume(handler);
  const consumer2 = subscription.consume(handler, { autostart: false });

  await Promise.all([consumer1.waitInit(), consumer2.waitInit()]);
  await Bun.sleep(20);

  const resetCount = await transit.resetStaleMessages();

  expect(resetCount).toBe(1);

  await consumer2.consume(); // Repick the stale message & process the next one

  expect(consumed).toHaveLength(3);
  expect(consumed[0]!.data).toEqual({ foo: 'bar' });
  expect(consumed[1]!.data).toEqual({ foo: 'bar' });
  expect(consumed[2]!.data).toEqual({ bar: 'baz' });
});

test('A message can only stale and be reset once. On second stale it fails', async () => {
  transit.staleMessageTimeoutInMs = 1;

  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    consumptionMode: 'parallel',
  });

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });

  const consumed: SubscriptionMessage<JSONValue>[] = [];
  const consumer1 = subscription.consume(
    async (message) => {
      consumed.push(message);
      await Bun.sleep(100);
    },
    {
      autostart: false,
    },
  );
  const consumer2 = subscription.consume(
    async (message) => {
      consumed.push(message);
      await Bun.sleep(100);
    },
    { autostart: false },
  );
  const consumer3 = subscription.consume(
    async (message) => {
      consumed.push(message);
      await Bun.sleep(100);
    },
    { autostart: false },
  );

  await Promise.all([consumer1.waitInit(), consumer2.waitInit()]);

  consumer1.consume(); // Pick up the message

  await Bun.sleep(10); // Wait for the message to be picked up

  expect(consumed).toHaveLength(1);

  const resetCount = await transit.resetStaleMessages();
  expect(resetCount).toBe(1);

  consumer2.consume(); // Repick the stale message

  await Bun.sleep(10); // Wait for the message to be picked up again

  expect(consumed).toHaveLength(2);

  const resetCount2 = await transit.resetStaleMessages();
  expect(resetCount2).toBe(1); // Second reset

  const failedMessages = await subscription.getMessages('failed');
  expect(failedMessages).toHaveLength(1); // Second reset marked the message as failed

  await consumer3.consume(); // No more message to pick up

  expect(consumed).toHaveLength(2);
  expect(consumed[0]!.staleCount).toBe(0);
  expect(consumed[1]!.staleCount).toBe(1);
});
