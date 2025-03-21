import { expect, test } from 'bun:test';
import type { JSONValue } from 'postgres';
import { createTestContext, randomName } from './utils';

const { transit } = createTestContext();

test('Report message progress', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });

  const consumer = subscription.consume(async (message) => {
    await message.updateProgress(40);
    await message.updateProgress(80);
  });

  const progresses: JSONValue[] = [];
  consumer.on('progress', (message) => {
    progresses.push(message.progress!);
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(progresses.length).toBe(2);
  expect(progresses).toEqual([40, 80]);
});

test('Report message progress passing object', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });

  const consumer = subscription.consume(async (message) => {
    await message.updateProgress({ page1: 'processing', page2: 'waiting' });
    await message.updateProgress({ page1: 'done', page2: 'processing' });
  });

  const progresses: JSONValue[] = [];
  consumer.on('progress', (message) => {
    progresses.push(message.progress!);
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(progresses.length).toBe(2);
  expect(progresses).toEqual([
    { page1: 'processing', page2: 'waiting' },
    { page1: 'done', page2: 'processing' },
  ]);
});

test('Reset progress on retry', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    maxAttempts: 2,
  });

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });

  let firstAttempt = true;
  let progress!: JSONValue;
  const consumer = subscription.consume(async (message) => {
    if (firstAttempt) {
      firstAttempt = false;
      await message.updateProgress(40);
      throw new Error('Test error');
    }

    progress = message.progress!;
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(progress).toBeUndefined();
});
