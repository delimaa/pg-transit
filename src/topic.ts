import CronExpressionParser from 'cron-parser';
import EventEmitter from 'node:events';
import type { JSONValue, Sql } from 'postgres';
import { v7 } from 'uuid';
import { insertMessages, Message, type MessageOptions, type MessageRow } from './message';
import { ScheduledMessage, type ScheduledMessageConfig, type ScheduledMessageRow } from './scheduled-message';
import { Subscription, type SubscriptionOptions } from './subscription';
import { patchAsyncMethods } from './utils';

export type TopicOptions = {
  /**
   * The maximum number of acknowledged messages to keep in the topic.
   * Acknowledged messages are those that have been consumed by all the subscriptions attached to the topic.
   *
   * Must be a number between 0 and `Infinity`.
   * - Set `0` to remove all acknowledged messages.
   * - Set `1000` to keep `1000` acknowledged messages maximum.
   * - Set `Infinity` to keep all acknowledged messages indefinitely.
   *
   * @default 0
   */
  maxMessagesRetention?: number;
};

export class Topic<T extends JSONValue> extends EventEmitter<{
  send: [message: Message<T>];
  schedule: [message: ScheduledMessage<T>];
  trim: [count: number];
}> {
  private readonly sql: Sql;

  private readonly init: Promise<void>;

  private readonly subscriptions: Subscription<T>[] = [];

  readonly name: string;

  readonly id!: string;

  readonly maxMessagesRetention: number;

  constructor(props: { sql: Sql; pgTransitInit: Promise<void>; name: string }, options?: TopicOptions) {
    super();

    this.sql = props.sql;

    this.name = props.name;

    this.maxMessagesRetention = options?.maxMessagesRetention ?? 0;

    this.init = (async () => {
      await props.pgTransitInit;

      let [row] = await props.sql`
        INSERT INTO
          pg_transit_topics ${props.sql({ id: v7(), name: props.name })}
        ON CONFLICT (name) DO NOTHING
        RETURNING
          id
      `;

      if (!row) {
        [row] = await props.sql`
          SELECT
            id
          FROM
            pg_transit_topics
          WHERE
            name = ${props.name}
        `;
      }

      this.id = row!['id'];
    })();
  }

  async waitInit(): Promise<void> {
    await this.init;
  }

  subscribe(subscriptionName: string, options?: SubscriptionOptions): Subscription<T> {
    const subscription = new Subscription({ sql: this.sql, topic: this, name: subscriptionName }, options);

    this.subscriptions.push(subscription);

    return subscription;
  }

  async stopConsumers(): Promise<void> {
    await Promise.all(this.subscriptions.map((subscription) => subscription.stopConsumers()));
  }

  async send(data: T, options?: MessageOptions): Promise<Message<T>> {
    const [message] = await insertMessages(this.sql, this.id, [data], options);

    this.emit('send', message!);

    return message!;
  }

  async sendBulk(data: T[], options?: MessageOptions): Promise<Message<T>[]> {
    const messages = await insertMessages(this.sql, this.id, data, options);

    for (const message of messages) {
      this.emit('send', message);
    }

    return messages;
  }

  async schedule(
    scheduledMessageName: string,
    config: ScheduledMessageConfig,
    data: T,
    options?: MessageOptions,
  ): Promise<ScheduledMessage<T>> {
    const now = new Date();
    const nextOccurrenceAt = CronExpressionParser.parse(config.cron).next().toDate();

    const row: ScheduledMessageRow<T> = {
      topic_id: this.id,
      name: scheduledMessageName,
      data,
      created_at: now,
      updated_at: null,
      cron: config.cron,
      next_occurrence_at: nextOccurrenceAt,
      deliver_at: options?.deliverAt ?? null,
      deliver_in_ms: options?.deliverInMs ?? null,
      priority: options?.priority ?? null,
      repeats: config.repeats ?? null,
      repeats_made: 0,
    };

    const updatedAt = new Date();

    const [{ inserted }]: [{ inserted: boolean }] = await this.sql`
      INSERT INTO
        pg_transit_scheduled_messages ${this.sql(row)}
      ON CONFLICT (topic_id, name) DO UPDATE
      SET
        data = EXCLUDED.data,
        cron = EXCLUDED.cron,
        next_occurrence_at = EXCLUDED.next_occurrence_at,
        deliver_in_ms = EXCLUDED.deliver_in_ms,
        deliver_at = EXCLUDED.deliver_at,
        priority = EXCLUDED.priority,
        repeats = EXCLUDED.repeats,
        updated_at = ${updatedAt}
      RETURNING
        updated_at IS NULL AS inserted
    `;

    if (inserted) {
      row.updated_at = updatedAt;
    }

    const message = new ScheduledMessage(this.sql, row);

    if (inserted) {
      this.emit('schedule', message);
    }

    return message;
  }

  async getMessages(): Promise<Message<T>[]> {
    const rows: MessageRow<T>[] = await this.sql`
      SELECT
        *
      FROM
        pg_transit_messages
      WHERE
        topic_id = ${this.id}
      ORDER BY
        id
    `;

    return rows.map((row) => new Message(this.sql, row));
  }

  async getScheduledMessages(): Promise<ScheduledMessage<T>[]> {
    const rows: ScheduledMessageRow<T>[] = await this.sql`
      SELECT
        *
      FROM
        pg_transit_scheduled_messages
      WHERE
        topic_id = ${this.id}
    `;

    return rows.map((row) => new ScheduledMessage(this.sql, row));
  }

  async trim(): Promise<number> {
    if (this.maxMessagesRetention === Infinity) {
      return 0;
    }

    const count = await this.sql.begin(async (sql) => {
      const subscriptions: { id: string }[] = await sql`
        SELECT
          id
        FROM
          pg_transit_subscriptions
        WHERE
          topic_id = ${this.id}
      `;

      const subscriptionIds = subscriptions.map((s) => s.id);

      let earliestUnprocessedMessage: { id: string } | undefined;

      if (subscriptions.length > 0) {
        [earliestUnprocessedMessage] = await sql`
          SELECT
            message_id AS id
          FROM
            pg_transit_subscription_messages
          WHERE
            subscription_id = ANY (${sql.array(subscriptionIds)}::uuid[])
            AND status <> 'completed'
          ORDER BY
            message_id
          LIMIT
            1
        `;
      }

      const [latestMessageToRemove]: [{ id: string }?] = await sql`
        SELECT
          id
        FROM
          pg_transit_messages
        WHERE
          topic_id = ${this.id} ${earliestUnprocessedMessage ? sql`AND id < ${earliestUnprocessedMessage.id}` : sql``}
        ORDER BY
          id DESC
        LIMIT
          1
        OFFSET
          ${this.maxMessagesRetention}
      `;

      if (!latestMessageToRemove) {
        return 0;
      }

      const { count } = await sql`
        DELETE FROM pg_transit_messages
        WHERE
          topic_id = ${this.id}
          AND id <= ${latestMessageToRemove.id}
      `;

      return count;
    });

    if (count > 0) {
      this.emit('trim', count);
    }

    return count;
  }

  async clear(): Promise<void> {
    await this.sql.begin(async (sql) => {
      await sql`
        DELETE FROM pg_transit_messages
        WHERE
          topic_id = ${this.id}
      `;

      await sql`
        DELETE FROM pg_transit_scheduled_messages
        WHERE
          topic_id = ${this.id}
      `;
    });
  }
}

patchAsyncMethods(Topic.prototype, { doBefore: (instance) => (instance as any).init });
