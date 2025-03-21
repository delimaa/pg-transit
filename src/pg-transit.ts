import CronExpressionParser from 'cron-parser';
import { EventEmitter } from 'node:events';
import type { JSONValue, Options, PostgresType, Sql } from 'postgres';
import postgres from 'postgres';
import { insertMessages, Message } from './message';
import { runMigrations } from './migrations';
import type { ScheduledMessageRow } from './scheduled-message';
import { Topic, type TopicOptions } from './topic';
import { loop, patchAsyncMethods } from './utils';

export type PgTransitOptions = {
  connection: Options<Record<string, PostgresType>> & { url?: string };

  /**
   * The interval in milliseconds to automatically trim topics (enforce retention policy).
   *
   * @default 60_000 // 1 minute
   */
  trimTopicsIntervalInMs?: number;

  /**
   * The timeout in milliseconds to consider a message stale after not receiving a heartbeat.
   *
   * @default 60_000 // 1 minute
   */
  staleMessageTimeoutInMs?: number;

  /**
   * The interval in milliseconds to check for stale messages and reset them for new processing.
   *
   * @default 60_000 // 1 minute
   */
  resetStaleMessagesIntervalInMs?: number;

  /**
   * Interval in milliseconds to check for scheduled messages to process.
   *
   * @default 5_000 // 5 seconds
   */
  scheduledMessagesProcessingIntervalInMs?: number;
};

export function pgTransit(options: PgTransitOptions): PgTransit {
  const sql =
    typeof options.connection.url === 'string'
      ? postgres(options.connection.url, options.connection)
      : postgres(options.connection);

  return new PgTransit(sql, options);
}

export class PgTransit extends EventEmitter<{
  stale: [{ messageId: string; subscriptionId: string }];
}> {
  private readonly init: Promise<void>;

  private readonly topics: Topic<any>[] = [];

  readonly trimTopicsIntervalInMs: number;
  private readonly stopTrimTopicsLoop: () => void;

  staleMessageTimeoutInMs: number;
  readonly resetStaleMessagesIntervalInMs: number;
  private readonly stopResetStaleMessagesLoop: () => void;

  readonly scheduledMessagesProcessingIntervalInMs: number;
  private readonly stopScheduledMessagesProcessingLoop: () => void;

  private readonly pendingPromises = new Set<Promise<any>>();

  constructor(
    private readonly sql: Sql,
    options?: PgTransitOptions,
  ) {
    super();

    this.trimTopicsIntervalInMs = options?.trimTopicsIntervalInMs ?? 60_000;
    this.staleMessageTimeoutInMs = options?.staleMessageTimeoutInMs ?? 60_000;
    this.resetStaleMessagesIntervalInMs = options?.resetStaleMessagesIntervalInMs ?? 60_000;
    this.scheduledMessagesProcessingIntervalInMs = options?.scheduledMessagesProcessingIntervalInMs ?? 5_000;

    this.init = (async () => {
      await runMigrations(sql);
    })();

    this.stopTrimTopicsLoop = loop(async () => {
      await this.trimTopics();
    }, this.trimTopicsIntervalInMs);

    this.stopResetStaleMessagesLoop = loop(async () => {
      await this.resetStaleMessages();
    }, this.resetStaleMessagesIntervalInMs);

    this.stopScheduledMessagesProcessingLoop = loop(async () => {
      await this.processScheduledMessages();
    }, this.scheduledMessagesProcessingIntervalInMs);
  }

  async close(): Promise<void> {
    this.stopTrimTopicsLoop();
    this.stopResetStaleMessagesLoop();
    this.stopScheduledMessagesProcessingLoop();

    await Promise.all([...[...this.pendingPromises], ...this.topics.map((topic) => topic.stopConsumers())]);

    await this.sql.end();
  }

  topic<T extends JSONValue>(name: string, options?: TopicOptions): Topic<T> {
    const topic = new Topic<T>(
      {
        sql: this.sql,
        pgTransitInit: this.init,
        name,
      },
      options,
    );

    this.topics.push(topic);

    return topic;
  }

  async trimTopics(): Promise<number> {
    const promise = (async () => {
      const counts = await Promise.all(this.topics.map((topic) => topic.trim()));

      return counts.reduce((acc, count) => acc + count, 0);
    })();

    this.pendingPromises.add(promise);

    try {
      return await promise;
    } finally {
      this.pendingPromises.delete(promise);
    }
  }

  async resetStaleMessages(): Promise<number> {
    const promise = (async () => {
      const staleAt = new Date(Date.now() - this.staleMessageTimeoutInMs);

      return await this.sql.begin(async (sql) => {
        const rows: { message_id: string; subscription_id: string }[] = await sql`
          UPDATE pg_transit_subscription_messages
          SET
            status = (
              CASE
                WHEN stale_count = 0 THEN 'waiting'
                ELSE 'failed'
              END
            )::pg_transit_message_status,
            stale_count = stale_count + 1,
            last_heartbeat_at = NULL
          WHERE
            status = 'processing'
            AND last_heartbeat_at <= ${staleAt}
          RETURNING
            message_id,
            subscription_id
        `;

        if (rows.length === 0) {
          return 0;
        }

        rows.forEach((row) => {
          this.emit('stale', {
            messageId: row.message_id,
            subscriptionId: row.subscription_id,
          });
        });

        // SET processing = FALSE to subscriptions where stale messages were found
        const subscriptionIds = [...new Set(rows.map((row) => row.subscription_id))];
        await sql`
          UPDATE pg_transit_subscriptions
          SET
            processing = FALSE
          WHERE
            id = ANY (${sql.array(subscriptionIds)}::uuid[])
        `;

        return rows.length;
      });
    })();

    this.pendingPromises.add(promise);

    try {
      return await promise;
    } finally {
      this.pendingPromises.delete(promise);
    }
  }

  async processScheduledMessages(): Promise<Message<JSONValue>[]> {
    const promise = (async () => {
      const messages: Message<JSONValue>[] = [];

      await this.sql.begin(async (sql) => {
        const rows: ScheduledMessageRow<JSONValue>[] = await sql`
          SELECT
            *
          FROM
            pg_transit_scheduled_messages
          WHERE
            next_occurrence_at <= ${new Date()}
            AND (
              repeats IS NULL
              OR repeats_made < repeats
            )
          FOR UPDATE
            SKIP LOCKED
        `;

        for (const row of rows) {
          const [message] = await insertMessages(this.sql, row.topic_id, [row.data], {
            deliverAt: row.deliver_at ?? undefined,
            deliverInMs: row.deliver_in_ms ?? undefined,
            priority: row.priority ?? undefined,
          });

          messages.push(message!);

          await sql`
            UPDATE pg_transit_scheduled_messages
            SET
              next_occurrence_at = ${CronExpressionParser.parse(row.cron).next().toDate()},
              repeats_made = repeats_made + 1
            WHERE
              topic_id = ${row.topic_id}
              AND name = ${row.name}
          `;
        }
      });

      return messages;
    })();

    this.pendingPromises.add(promise);

    try {
      return await promise;
    } finally {
      this.pendingPromises.delete(promise);
    }
  }
}

patchAsyncMethods(PgTransit.prototype, { doBefore: (instance) => (instance as any).init });
