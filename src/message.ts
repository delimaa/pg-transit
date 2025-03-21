import type { JSONValue, Sql } from 'postgres';
import { v7 } from 'uuid';

export async function insertMessages<T extends JSONValue>(
  sql: Sql,
  topicId: string,
  data: T[],
  options?: MessageOptions,
): Promise<Message<T>[]> {
  const createdAt = new Date();

  let deliverAt: Date | undefined;
  if (options?.deliverInMs) {
    deliverAt = new Date(Date.now() + options.deliverInMs);
  }
  if (options?.deliverAt) {
    deliverAt = options.deliverAt;
  }

  const priority = options?.priority;

  const rows: MessageRow<T>[] = data.map((data) => ({
    id: v7(),
    topic_id: topicId,
    data,
    created_at: createdAt,
    deliver_at: deliverAt ?? null,
    priority: priority ?? null,
  }));

  const ids = rows.map((row) => row.id);

  await sql.begin(async (sql) => {
    await sql`
      INSERT INTO
        pg_transit_messages ${sql(rows)}
    `;

    await sql`
      INSERT INTO
        pg_transit_subscription_messages (subscription_id, message_id, available_at)
      SELECT
        pg_transit_subscriptions.id,
        pg_transit_messages.id,
        ${deliverAt ?? null}
      FROM
        pg_transit_subscriptions
        JOIN pg_transit_messages ON pg_transit_messages.topic_id = ${topicId}
      WHERE
        pg_transit_subscriptions.topic_id = ${topicId}
        AND pg_transit_messages.id = ANY (${sql.array(ids)}::uuid[])
    `;
  });

  return rows.map((row) => new Message(sql, row));
}

export type MessageOptions = {
  /**
   * Delay the message by a given amount of milliseconds before it is delivered to the subscriptions.
   *
   * @default 0 // No delay
   */
  deliverInMs?: number;

  /**
   * The date to deliver the message to the subscriptions.
   *
   * @default new Date() // Instant delivery
   */
  deliverAt?: Date;

  /**
   * Message priority.
   * Accepts positive or negative integers.
   * A lower number means higher priority.
   * Messages with the same priority are processed in the order they were sent.
   * Messages with no priority are processed after messages with priority (no priority is the lowest priority).
   */
  priority?: number;
};

export type MessageRow<T extends JSONValue> = {
  id: string;
  topic_id: string;
  data: T;
  created_at: Date;
  deliver_at: Date | null;
  priority: number | null;
};

export class Message<T extends JSONValue> {
  get id(): string {
    return this.row.id;
  }

  get topicId(): string {
    return this.row.topic_id;
  }

  get data(): T {
    return this.row.data;
  }

  get createdAt(): Date {
    return this.row.created_at;
  }

  get deliverAt(): Date | undefined {
    return this.row.deliver_at ?? undefined;
  }

  get priority(): number | undefined {
    return this.row.priority ?? undefined;
  }

  constructor(
    private readonly sql: Sql,
    private readonly row: MessageRow<T>,
  ) {}

  async remove(): Promise<void> {
    await this.sql`
      DELETE FROM pg_transit_messages
      WHERE
        topic_id = ${this.topicId}
        AND id = ${this.id}
    `;
  }
}
