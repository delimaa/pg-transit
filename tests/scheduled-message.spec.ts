import { expect, setSystemTime, test } from 'bun:test';
import type { SubscriptionMessage } from '../src';
import { createTestContext, randomName } from './utils';
import type { JSONValue } from 'postgres';

const { transit, newPgTransit } = createTestContext();

test('Schedule a message using CRON', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const scheduledMessage = await topic.schedule(
    'my-cron-job',
    {
      cron: '0 0 * * *', // Every day at midnight
    },
    {
      foo: 'bar',
    },
  );

  expect(scheduledMessage.name).toBe('my-cron-job');
  expect(scheduledMessage.cron).toBe('0 0 * * *');
  expect(scheduledMessage.data).toEqual({ foo: 'bar' });

  const nextMidnight = new Date();
  nextMidnight.setHours(0, 0, 0, 0);
  nextMidnight.setDate(nextMidnight.getDate() + 1); // Next midnight
  expect(scheduledMessage.nextOccurrenceAt).toEqual(nextMidnight);

  setSystemTime(nextMidnight);

  const messages = await transit.processScheduledMessages();
  expect(messages).toHaveLength(1);

  const subscriptionMessages = await subscription.getMessages();
  expect(subscriptionMessages).toHaveLength(1);

  let consumed!: SubscriptionMessage<JSONValue>;
  const consumer = subscription.consume((message) => {
    consumed = message;
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumed.data).toEqual({ foo: 'bar' });

  setSystemTime();
});

test('Schedule a message with options', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const in1Hour = new Date(Date.now() + 1_000 * 60 * 60); // In one hour

  const scheduledMessage = await topic.schedule(
    'my-cron-job',
    {
      cron: '0 0 * * *', // Every day at midnight
    },
    {
      foo: 'bar',
    },
    {
      deliverAt: in1Hour,
      priority: 3,
    },
  );

  expect(scheduledMessage.deliverAt).toEqual(in1Hour);
  expect(scheduledMessage.priority).toEqual(3);

  const nextMidnight = new Date();
  nextMidnight.setHours(0, 0, 0, 0);
  nextMidnight.setDate(nextMidnight.getDate() + 1); // Next midnight
  setSystemTime(nextMidnight);

  const messages = await transit.processScheduledMessages();

  expect(messages).toHaveLength(1);
  expect(messages[0]!.deliverAt).toEqual(in1Hour);
  expect(messages[0]!.priority).toEqual(3);

  setSystemTime();
});

test('Remove a scheduled message', async () => {
  const topic = transit.topic(randomName('topic'));

  const scheduledMessage = await topic.schedule(
    'my-cron-job',
    {
      cron: '0 0 * * *', // Every day at midnight
    },
    {
      foo: 'bar',
    },
  );

  await scheduledMessage.remove();

  const scheduledMessages = await topic.getScheduledMessages();
  expect(scheduledMessages).toHaveLength(0);
});

test('Specify a number of repetitions', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const scheduledMessage = await topic.schedule(
    'my-cron-job',
    {
      cron: '0 0 * * *', // Every day at midnight
      repeats: 1,
    },
    {
      foo: 'bar',
    },
  );

  expect(scheduledMessage.repeats).toEqual(1);

  const nextMidnight = new Date();
  nextMidnight.setHours(0, 0, 0, 0);
  nextMidnight.setDate(nextMidnight.getDate() + 1); // Next midnight
  setSystemTime(nextMidnight);

  const messages = await transit.processScheduledMessages();

  expect(messages.filter((m) => m.topicId === topic.id)).toHaveLength(1);

  nextMidnight.setDate(nextMidnight.getDate() + 1); // Next midnight again
  setSystemTime(nextMidnight);

  const messages2 = await transit.processScheduledMessages();

  expect(messages2.filter((m) => m.topicId === topic.id)).toHaveLength(0);

  setSystemTime();
});

test('Configure scheduled messages processing interval', async () => {
  const transit = newPgTransit({
    scheduledMessagesProcessingIntervalInMs: 1_000,
  });

  expect(transit.scheduledMessagesProcessingIntervalInMs).toEqual(1_000);
});
