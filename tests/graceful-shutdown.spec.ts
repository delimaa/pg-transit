import { test } from 'bun:test';
import { createTestContext, randomName } from './utils';

const { newPgTransit } = createTestContext();

test('Graceful shutdown when resetting stale jobs', async () => {
  const transit = newPgTransit({
    staleMessageTimeoutInMs: 0,
  });

  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  await topic.sendBulk(Array.from({ length: 100 }, (_, i) => ({ foo: `bar${i}` })));

  transit.resetStaleMessages();

  await transit.close();

  await Bun.sleep(10);
});

test('Graceful shutdown when trimming topics', async () => {
  const transit = newPgTransit();

  const topic = transit.topic(randomName('topic'), {
    maxMessagesRetention: 10,
  });

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  await topic.sendBulk(Array.from({ length: 100 }, (_, i) => ({ foo: `bar${i}` })));

  void transit.trimTopics();

  await transit.close();

  await Bun.sleep(10);
});

test('Graceful shutdown when processing scheduled messages', async () => {
  const transit = newPgTransit();

  const topic = transit.topic(randomName('topic'));

  const subscription = topic.subscribe(randomName('subscription'));

  await subscription.waitInit();

  await topic.schedule('my-message', { cron: '*/1 * * * *' }, 'foo');

  transit.processScheduledMessages();

  await transit.close();

  await Bun.sleep(10);
});

test('Graceful shutdown when initializing topics, subscriptions and consumers', async () => {
  const transit = newPgTransit();

  const topic1 = transit.topic(randomName('topic'));
  const topic2 = transit.topic(randomName('topic'));

  const subscription1 = topic1.subscribe(randomName('subscription'));
  const subscription2 = topic2.subscribe(randomName('subscription'));
  const subscription3 = topic1.subscribe(randomName('subscription'));
  const subscription4 = topic2.subscribe(randomName('subscription'));

  subscription1.consume(() => {});
  subscription2.consume(() => {});
  subscription3.consume(() => {});
  subscription4.consume(() => {});
  subscription1.consume(() => {});
  subscription2.consume(() => {});
  subscription3.consume(() => {});
  subscription4.consume(() => {});

  await transit.close();

  await Bun.sleep(10);
});
