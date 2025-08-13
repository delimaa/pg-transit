import { expect, test } from 'bun:test';
import type { SubscriptionMessage } from '../src';
import { createTestContext, randomName } from './utils';

const { newPgTransit } = createTestContext();

type Email = {
  from: string;
  to: string;
  subject: string;
  body: string;
};

test('Two app instances, one email topic, two subscriptions: one job queue style sending emails, one sequential log style logging emails', async () => {
  const transit1 = newPgTransit({ staleMessageTimeoutInMs: 200, resetStaleMessagesIntervalInMs: 100 });
  const transit2 = newPgTransit({ staleMessageTimeoutInMs: 200, resetStaleMessagesIntervalInMs: 100 });

  const topicName = randomName('email_topic');
  const app1Topic = transit1.topic<Email>(topicName);
  const app2Topic = transit2.topic<Email>(topicName);

  const sendEmailSubscriptionName = randomName('send_email_subscription');
  const app1SendEmailSubscription = app1Topic.subscribe(sendEmailSubscriptionName, {
    consumptionMode: 'parallel',
    maxAttempts: 2,
  });
  const app2SendEmailSubscription = app2Topic.subscribe(sendEmailSubscriptionName, {
    consumptionMode: 'parallel',
    maxAttempts: 2,
  });

  const logEmailSubscriptionName = randomName('log_email_subscription');
  const app1LogEmailSubscription = app1Topic.subscribe(logEmailSubscriptionName, {
    consumptionMode: 'sequential',
  });
  const app2LogEmailSubscription = app2Topic.subscribe(logEmailSubscriptionName, {
    consumptionMode: 'sequential',
  });

  const sendEmailMessages: SubscriptionMessage<Email>[] = [];

  const sendEmailHandler = async (message: SubscriptionMessage<Email>) => {
    sendEmailMessages.push(message);

    if (message.data.subject === 'Hello 1') {
      return; // success
    }

    if (message.data.subject === 'Hello 2') {
      // fail once, success on retry
      if (sendEmailMessages.filter((m) => m.data.subject === 'Hello 2').length === 1) {
        throw new Error('Fail');
      } else {
        return; // success
      }
    }

    if (message.data.subject === 'Hello 3') {
      // always fail
      throw new Error('Fail');
    }

    if (message.data.subject === 'Hello 4') {
      // stale on first, success on retry
      if (sendEmailMessages.filter((m) => m.data.subject === 'Hello 4').length === 1) {
        await Bun.sleep(1_000);
        return;
      } else {
        return; // success
      }
    }
  };

  const app1SendEmailConsumer = app1SendEmailSubscription.consume(sendEmailHandler, {
    concurrency: 2,
    pollingIntervalInMs: 80,
  });
  const app2SendEmailConsumer = app2SendEmailSubscription.consume(sendEmailHandler, {
    pollingIntervalInMs: 100,
  });

  const logEmailMessages: SubscriptionMessage<Email>[] = [];

  const logEmailHandler = async (message: SubscriptionMessage<Email>) => {
    logEmailMessages.push(message);

    // fail on last, success on first two
    if (message.data.subject === 'Hello 3') {
      throw new Error('Fail');
    }

    if (message.data.subject === 'Hello 4') {
      // stale and fail
      if (logEmailMessages.filter((m) => m.data.subject === 'Hello 4').length === 1) {
        await Bun.sleep(1_000);
        return;
      } else {
        throw new Error('Fail');
      }
    }
  };

  const app1LogEmailConsumer = app1LogEmailSubscription.consume(logEmailHandler, {
    pollingIntervalInMs: 50,
  });
  const app2LogEmailConsumer = app2LogEmailSubscription.consume(logEmailHandler, {
    pollingIntervalInMs: 100,
  });

  await Promise.all([
    app1SendEmailConsumer.waitInit(),
    app2SendEmailConsumer.waitInit(),
    app1LogEmailConsumer.waitInit(),
    app2LogEmailConsumer.waitInit(),
  ]);

  await Promise.all([
    app1Topic.send(
      {
        from: 'john.doe@mail.com',
        to: 'jane.doe@mail.com',
        subject: 'Hello 3',
        body: "I'm delayed",
      },
      {
        deliverInMs: 500,
      },
    ),
    app1Topic.send({
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 2',
      body: 'Just simple',
    }),
    app2Topic.send(
      {
        from: 'john.doe@mail.com',
        to: 'jane.doe@mail.com',
        subject: 'Hello 1',
        body: "I've got priority",
      },
      {
        priority: 1,
      },
    ),
    app2Topic.send({
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 4',
      body: "I'm gonna stale",
    }),
  ]);

  await Bun.sleep(1_000);

  expect(sendEmailMessages.sort((a, b) => a.data.subject.localeCompare(b.data.subject)).map((m) => m.data)).toEqual([
    {
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 1',
      body: "I've got priority",
    },
    {
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 2',
      body: 'Just simple',
    },
    {
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 2',
      body: 'Just simple',
    },
    {
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 3',
      body: "I'm delayed",
    },
    {
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 3',
      body: "I'm delayed",
    },
    {
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 4',
      body: "I'm gonna stale",
    },
    {
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 4',
      body: "I'm gonna stale",
    },
  ]);

  expect(logEmailMessages.map((m) => m.data)).toEqual([
    {
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 1',
      body: "I've got priority",
    },
    {
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 2',
      body: 'Just simple',
    },
    {
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 4',
      body: "I'm gonna stale",
    },
    {
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 4',
      body: "I'm gonna stale",
    },
    {
      from: 'john.doe@mail.com',
      to: 'jane.doe@mail.com',
      subject: 'Hello 3',
      body: "I'm delayed",
    },
  ]);
});
