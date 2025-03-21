import { expect, test } from 'bun:test';
import type { SubscriptionMessage } from '../src';
import { createTestContext, randomName } from './utils';
import type { JSONValue } from 'postgres';

const { transit } = createTestContext();

test('Consume a message in a topic from a subscription', async () => {
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

  expect(message.id).toBe(consumedMessage!.id);
  expect(consumedMessage!.data).toEqual({ foo: 'bar' });
});

test('Consume each message once in order when consuming consecutives messages in a topic from a subscription', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const message1 = await topic.send({ foo: 'bar' });
  const message2 = await topic.send({ bar: 'baz' });

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

test('Each subscription consume the message when consuming a message in a topic from different subscriptions', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription1 = topic.subscribe(randomName('subscription'));
  const subscription2 = topic.subscribe(randomName('subscription'));

  await Promise.all([subscription1.waitInit(), subscription2.waitInit()]);

  const message1 = await topic.send({ foo: 'bar' });

  let consumedMessage1!: SubscriptionMessage<JSONValue>;
  let consumedMessage2!: SubscriptionMessage<JSONValue>;

  const consumer1 = subscription1.consume((message) => {
    consumedMessage1 = message;
  });

  const consumer2 = subscription2.consume((message) => {
    consumedMessage2 = message;
  });

  await Promise.all([consumer1.waitInit(), consumer2.waitInit()]);
  await Promise.all([consumer1.waitIdle(), consumer2.waitIdle()]);

  expect(message1.id).toBe(consumedMessage1!.id);
  expect(consumedMessage1!.data).toEqual({ foo: 'bar' });
  expect(message1.id).toBe(consumedMessage2!.id);
  expect(consumedMessage2!.data).toEqual({ foo: 'bar' });
});

test('Consume each message once when concurrent consumers on same subscription', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  const message1 = await topic.send({ foo: 'bar' });
  const message2 = await topic.send({ bar: 'baz' });

  let consumedMessages: SubscriptionMessage<JSONValue>[] = [];

  const consumer1 = subscription.consume((message) => {
    consumedMessages.push(message);
  });

  const consumer2 = subscription.consume((message) => {
    consumedMessages.push(message);
  });

  await Promise.all([consumer1.waitInit(), consumer2.waitInit()]);
  await Promise.all([consumer1.waitIdle(), consumer2.waitIdle()]);

  expect(consumedMessages.length).toBe(2);
  expect(consumedMessages.map((m) => m.id).sort()).toEqual([message1.id, message2.id].sort());
});

test('Create one subscription only when same subscription is created multiple times in parallel', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscriptionName = randomName('subscription');
  const subscription1 = topic.subscribe(subscriptionName);
  const subscription2 = topic.subscribe(subscriptionName);
  const subscription3 = topic.subscribe(subscriptionName);

  await Promise.all([subscription1.waitInit(), subscription2.waitInit(), subscription3.waitInit()]);

  expect(subscription1.id).toBe(subscription2.id);
  expect(subscription1.id).toBe(subscription3.id);
});

test('Consume messages one by one, meaning only one consumer at time consume a message from the subscription, even if multiple consumers are consuming the subscription', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });
  await topic.send({ bar: 'baz' });

  const actions: string[] = [];

  const consumer1 = subscription.consume(async () => {
    actions.push('start');
    await Bun.sleep(5);
    actions.push('end');
  });

  const consumer2 = subscription.consume(async () => {
    actions.push('start');
    await Bun.sleep(5);
    actions.push('end');
  });

  await Promise.all([consumer1.waitInit(), consumer2.waitInit()]);
  await Promise.all([consumer1.waitIdle(), consumer2.waitIdle()]);

  expect(actions).toEqual(['start', 'end', 'start', 'end']);
});

test('Consume messages in parallel, meaning multiple consumers at time consume a message from the subscription', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    consumptionMode: 'parallel',
  });

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });
  await topic.send({ bar: 'baz' });

  const actions: string[] = [];

  const consumer1 = subscription.consume(async () => {
    actions.push('start');
    await Bun.sleep(5);
    actions.push('end');
  });

  const consumer2 = subscription.consume(async () => {
    actions.push('start');
    await Bun.sleep(5);
    actions.push('end');
  });

  await Promise.all([consumer1.waitInit(), consumer2.waitInit()]);
  await Promise.all([consumer1.waitIdle(), consumer2.waitIdle()]);

  expect(actions).toEqual(['start', 'start', 'end', 'end']);
});

test('Parallel consumption with a consumer consuming significantly faster than another consumer', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    consumptionMode: 'parallel',
  });

  await subscription.waitInit();

  await topic.send({ foo: 'bar' });
  await topic.send({ bar: 'baz' });
  await topic.send({ baz: 'qux' });

  const consumedMessages: SubscriptionMessage<JSONValue>[] = [];

  const consumer1 = subscription.consume(async (message) => {
    consumedMessages.push(message);
  });

  const consumer2 = subscription.consume(async (message) => {
    await Bun.sleep(10);
    consumedMessages.push(message);
  });

  await Promise.all([consumer1.waitInit(), consumer2.waitInit()]);
  await Promise.all([consumer1.waitIdle(), consumer2.waitIdle()]);

  expect(consumedMessages.length).toBe(3);
});

test("When a new message is sent and an earliest subscription is created at the same time, ensure that we don't miss the message", async () => {
  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'), {
    startPosition: 'earliest',
  });

  topic.send({ foo: 'bar' });

  let consumedMessage!: SubscriptionMessage<JSONValue>;

  const consumer = subscription.consume((message) => {
    consumedMessage = message;
  });

  await consumer.waitInit();
  await consumer.waitIdle();

  expect(consumedMessage).toBeDefined();
});

test('Fail when a subscription is created with different options than the one already created', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscriptionName = randomName('subscription');
  const subscription1 = topic.subscribe(subscriptionName, {
    consumptionMode: 'sequential',
    maxAttempts: 1,
    retryStrategy: 'linear',
    retryDelayInMs: 0,
    startPosition: 'earliest',
  });
  await subscription1.waitInit();

  const subscription2 = topic.subscribe(subscriptionName, {
    consumptionMode: 'parallel',
    maxAttempts: 2,
    retryStrategy: 'exponential',
    retryDelayInMs: 1000,
    startPosition: 'latest',
  });

  let error: Error | undefined;
  subscription2.on('error', (err) => {
    error = err;
  });

  await subscription2.waitInit();

  expect(error).toBeDefined();
});

test('Remove a subscription and the messages in it', async () => {
  const topic = transit.topic(randomName('topic'));

  const subscriptionName = randomName('subscription');
  const subscription1 = topic.subscribe(subscriptionName);

  await subscription1.waitInit();

  await topic.send({ foo: 'bar' });

  await subscription1.remove();

  const subscription2 = topic.subscribe(subscriptionName);

  await subscription2.waitInit();

  const messages = await subscription2.getMessages();
  expect(messages.length).toBe(0); // no more messages in the subscription
});
