import { expect, test } from 'bun:test';
import { createTestContext, randomName } from './utils';

const { transit } = createTestContext();

test('Clear all messages and scheduled messages in a topic', async () => {
  const topic = transit.topic(randomName('topic'));

  await topic.send({ foo: 'bar' });
  await topic.schedule('my-cron-job', { cron: '0 0 * * *' }, { foo: 'bar' });

  await topic.clear();

  const messages = await topic.getMessages();
  expect(messages).toHaveLength(0);

  const scheduledMessages = await topic.getScheduledMessages();
  expect(scheduledMessages).toHaveLength(0);
});
