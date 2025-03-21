import EventEmitter from 'node:events';
import type { JSONValue, Sql } from 'postgres';
import { v7 } from 'uuid';
import { Consumer, type ConsumerOptions } from './consumer';
import { SubscriptionMessage, type MessageStatus } from './subscription-message';
import type { Topic } from './topic';
import { patchAsyncMethods } from './utils';

export type SubscriptionOptions = {
  /**
   * In `sequential` mode, messages are consumed one at a time.
   * This guarantees strict ordering of messages processing.
   * More consumers do not increase throughput, but provides more resilience.
   *
   * In `parallel` mode, messages are consumed in parallel.
   * Messages are still processed in order.
   * However strict ordering is not guaranteed because a given consumer may process message many times faster than another consumer.
   * More consumers means more throughput.
   * This is the mode to use for job queue style processing.
   *
   * @default 'sequential'
   */
  consumptionMode?: 'sequential' | 'parallel';

  /**
   * The position to start consuming messages from on initial subscription creation.
   *
   * `earliest` means to consume all messages from the beginning of the topic.
   * `latest` means only new messages will be consumed.
   *
   * @default 'latest'
   */
  startPosition?: 'earliest' | 'latest';

  /**
   * The number of processing attempts for the message before it is considered failed.
   *
   * Must be greater than or equal to `1`.
   *
   * @default 1
   */
  maxAttempts?: number;

  /**
   * - `linear` to retry the message after a fixed delay.
   * - `exponential` to retry the message after an exponentially increasing delay computed as `delay * 2 ^ (attempts - 1)`.
   *
   * @default 'linear'
   */
  retryStrategy?: 'exponential' | 'linear';

  /**
   * The number of milliseconds to wait before retrying a failed message.
   *
   * @default 0 // No delay
   */
  retryDelayInMs?: number;
};

export type SubscriptionRow = {
  id: string;
  topic_id: string;
  name: string;
  created_at: Date;
  processing: boolean;
  consumption_mode: 'sequential' | 'parallel';
  start_position: 'earliest' | 'latest';
  max_attempts: number;
  retry_strategy: 'exponential' | 'linear';
  retry_delay: number;
};

export class Subscription<T extends JSONValue> extends EventEmitter<{
  error: [error: Error];
}> {
  private readonly sql: Sql;

  private readonly init: Promise<void>;

  private readonly consumers: Consumer<T>[] = [];

  readonly name: string;

  readonly id!: string;

  readonly consumptionMode!: 'sequential' | 'parallel';

  readonly startPosition!: 'earliest' | 'latest';

  readonly maxAttempts!: number;
  readonly retryStrategy!: 'exponential' | 'linear';
  readonly retryDelay!: number;

  constructor(props: { sql: Sql; topic: Topic<T>; name: string }, options?: SubscriptionOptions) {
    super();

    this.sql = props.sql;
    this.name = props.name;
    const consumptionMode = options?.consumptionMode ?? 'sequential'; // default to 'sequential'
    const startPosition = options?.startPosition ?? 'latest'; // default to 'latest'
    const maxAttempts = options?.maxAttempts ?? 1; // default to 1
    const retryStrategy = options?.retryStrategy ?? 'linear'; // default to 'linear'
    const retryDelay = options?.retryDelayInMs ?? 0; // default to 0

    this.init = (async () => {
      await (props.topic as any).init;

      const [newSub]: [{ id: string }?] = await props.sql`
        INSERT INTO
          pg_transit_subscriptions ${props.sql({
          id: v7(),
          topic_id: props.topic.id,
          name: props.name,
          consumption_mode: consumptionMode,
          start_position: startPosition,
          max_attempts: maxAttempts,
          retry_strategy: retryStrategy,
          retry_delay: retryDelay,
        })}
        ON CONFLICT (topic_id, name) DO NOTHING
        RETURNING
          id
      `;

      if (newSub) {
        this.id = newSub.id;
        this.consumptionMode = consumptionMode;
        this.startPosition = startPosition;
        this.maxAttempts = maxAttempts;
        this.retryStrategy = retryStrategy;
        this.retryDelay = retryDelay;

        if (startPosition === 'earliest') {
          await props.sql`
            INSERT INTO
              pg_transit_subscription_messages (subscription_id, message_id)
            SELECT
              ${this.id},
              id
            FROM
              pg_transit_messages
            WHERE
              topic_id = ${props.topic.id}
          `;
        }
      } else {
        const [existingSub]: [SubscriptionRow] = await props.sql`
          SELECT
            *
          FROM
            pg_transit_subscriptions
          WHERE
            name = ${props.name}
        `;

        const mismatchedOptions = [
          { option: 'consumptionMode', value: consumptionMode, existingValue: existingSub.consumption_mode },
          { option: 'startPosition', value: startPosition, existingValue: existingSub.start_position },
          { option: 'maxAttempts', value: maxAttempts, existingValue: existingSub.max_attempts },
          { option: 'retryStrategy', value: retryStrategy, existingValue: existingSub.retry_strategy },
          { option: 'retryDelay', value: retryDelay, existingValue: existingSub.retry_delay },
        ].filter(({ value, existingValue }) => value !== existingValue);

        if (mismatchedOptions.length > 0) {
          const mismatchedDescriptions = mismatchedOptions
            .map(({ option, existingValue }) => `${option}: ${existingValue}`)
            .join(', ');

          const error = new Error(
            `Subscription "${props.name}" on topic ${props.topic.name} already exists with different options (${mismatchedDescriptions}). If you want to change the options, please remove the subscription first.`,
          );

          this.emit('error', error);
        }

        this.id = existingSub.id;
        this.consumptionMode = existingSub.consumption_mode;
        this.startPosition = existingSub.start_position;
        this.maxAttempts = existingSub.max_attempts;
        this.retryStrategy = existingSub.retry_strategy;
        this.retryDelay = existingSub.retry_delay;
      }
    })();
  }

  async waitInit(): Promise<void> {
    await this.init;
  }

  async stopConsumers(): Promise<void> {
    await Promise.all(this.consumers.map((consumer) => consumer.stop()));
  }

  consume(handler: (message: SubscriptionMessage<T>) => void | Promise<void>, options?: ConsumerOptions): Consumer<T> {
    const consumer = new Consumer({ subscription: this, handler, options });

    this.consumers.push(consumer);

    return consumer;
  }

  async getNextMessages(count: number): Promise<SubscriptionMessage<T>[]> {
    if (count <= 0) {
      throw new Error('Count must be greater than 0');
    }

    return await this.sql.begin(async (sql) => {
      if (this.consumptionMode === 'sequential') {
        const [subscription]: [{ processing: boolean }] = await sql`
          SELECT
            processing
          FROM
            pg_transit_subscriptions
          WHERE
            id = ${this.id}
          FOR UPDATE
        `;

        if (subscription.processing) {
          return [];
        }
      }

      const messages: {
        id: string;
        data: T;
        created_at: Date;
        attempts: number;
        available_at: Date | null;
        error_stack: string | null;
        priority: number | null;
        stale_count: number;
      }[] = await sql`
        SELECT
          pg_transit_messages.id,
          pg_transit_messages.data,
          pg_transit_messages.created_at,
          pg_transit_subscription_messages.attempts,
          pg_transit_subscription_messages.available_at,
          pg_transit_subscription_messages.error_stack,
          pg_transit_messages.priority,
          pg_transit_subscription_messages.stale_count
        FROM
          pg_transit_subscription_messages
          LEFT JOIN pg_transit_messages ON pg_transit_subscription_messages.message_id = pg_transit_messages.id
        WHERE
          subscription_id = ${this.id}
          AND status = 'waiting'
          AND (
            available_at IS NULL
            OR available_at <= ${new Date()}
          )
        ORDER BY
          pg_transit_messages.priority ASC NULLS LAST,
          pg_transit_messages.id
        FOR UPDATE OF
          pg_transit_subscription_messages SKIP LOCKED
        LIMIT
          ${this.consumptionMode === 'sequential' ? 1 : count}
      `;

      if (messages.length === 0) {
        return [];
      }

      if (this.consumptionMode === 'sequential') {
        await sql`
          UPDATE pg_transit_subscriptions
          SET
            processing = TRUE
          WHERE
            id = ${this.id}
        `;
      }

      const messageIds = messages.map((message) => message.id);

      const lastHeartbeatAt = new Date();

      await sql`
        UPDATE pg_transit_subscription_messages
        SET
          status = 'processing',
          attempts = attempts + 1,
          last_heartbeat_at = ${lastHeartbeatAt},
          progress = NULL
        WHERE
          subscription_id = ${this.id}
          AND message_id = ANY (${sql.array(messageIds)}::uuid[])
      `;

      return messages.map(
        (message) =>
          new SubscriptionMessage({
            sql: this.sql,
            subscription: this,
            id: message.id,
            data: message.data,
            createdAt: message.created_at,
            status: 'processing',
            attempts: message.attempts + 1,
            availableAt: message.available_at,
            errorStack: message.error_stack,
            lastHeartbeatAt,
            priority: message.priority,
            progress: null,
            staleCount: message.stale_count,
          }),
      );
    });
  }

  async getMessages(...statuses: MessageStatus[]): Promise<SubscriptionMessage<T>[]> {
    const messages: {
      id: string;
      data: T;
      created_at: Date;
      status: MessageStatus;
      attempts: number;
      available_at: Date | null;
      error_stack: string | null;
      last_heartbeat_at: Date | null;
      priority: number | null;
      progress: JSONValue | null;
      stale_count: number;
    }[] = await this.sql`
      SELECT
        pg_transit_messages.id,
        pg_transit_messages.data,
        pg_transit_messages.created_at,
        pg_transit_subscription_messages.status,
        pg_transit_subscription_messages.attempts,
        pg_transit_subscription_messages.available_at,
        pg_transit_subscription_messages.error_stack,
        pg_transit_subscription_messages.last_heartbeat_at,
        pg_transit_messages.priority,
        pg_transit_subscription_messages.progress,
        pg_transit_subscription_messages.stale_count
      FROM
        pg_transit_subscription_messages
        LEFT JOIN pg_transit_messages ON pg_transit_subscription_messages.message_id = pg_transit_messages.id
      WHERE
        subscription_id = ${this.id} ${statuses.length > 0
        ? this.sql`
            AND status = ANY (
              ${this.sql.array(statuses)}::pg_transit_message_status[]
            )
          `
        : this.sql``}
      ORDER BY
        pg_transit_messages.id
    `;

    return messages.map(
      (message) =>
        new SubscriptionMessage({
          sql: this.sql,
          subscription: this,
          id: message.id,
          data: message.data,
          createdAt: message.created_at,
          status: message.status,
          attempts: message.attempts,
          availableAt: message.available_at,
          errorStack: message.error_stack,
          lastHeartbeatAt: message.last_heartbeat_at,
          priority: message.priority,
          progress: message.progress,
          staleCount: message.stale_count,
        }),
    );
  }

  async remove(): Promise<void> {
    await this.sql`
      DELETE FROM pg_transit_subscriptions
      WHERE
        id = ${this.id}
    `;
  }
}

patchAsyncMethods(Subscription.prototype, { doBefore: (instance) => (instance as any).init });
