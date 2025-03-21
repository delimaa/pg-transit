import { EventEmitter } from 'node:events';
import type { JSONValue, Sql } from 'postgres';
import type { Subscription } from './subscription';

export type MessageStatus = 'waiting' | 'processing' | 'completed' | 'failed';

export class SubscriptionMessage<T extends JSONValue> extends EventEmitter<{
  progress: [message: SubscriptionMessage<T>];
}> {
  private readonly sql: Sql;
  private readonly subscription: Subscription<T>;

  readonly id: string;
  readonly data: T;
  readonly createdAt: Date;

  private _status: MessageStatus;
  get status(): MessageStatus {
    return this._status;
  }

  private _attempts: number;
  get attempts(): number {
    return this._attempts;
  }

  private _availableAt?: Date;
  get availableAt(): Date | undefined {
    return this._availableAt;
  }

  get maxAttempts(): number {
    return this.subscription.maxAttempts;
  }

  private _errorStack?: string;
  get errorStack(): string | undefined {
    return this._errorStack;
  }

  private _lastHeartbeatAt?: Date;
  get lastHeartbeatAt(): Date | undefined {
    return this._lastHeartbeatAt;
  }

  readonly priority?: number;

  private _progress?: JSONValue;
  get progress(): JSONValue | undefined {
    return this._progress;
  }

  readonly staleCount: number;

  constructor(props: {
    sql: Sql;
    subscription: Subscription<T>;
    id: string;
    data: T;
    createdAt: Date;
    status: MessageStatus;
    attempts: number;
    availableAt: Date | null;
    errorStack: string | null;
    lastHeartbeatAt: Date | null;
    priority: number | null;
    progress: JSONValue | null;
    staleCount: number;
  }) {
    super();

    this.sql = props.sql;
    this.subscription = props.subscription;
    this.id = props.id;
    this.data = props.data;
    this.createdAt = props.createdAt;
    this._status = props.status;
    this._attempts = props.attempts;
    this._availableAt = props.availableAt ?? undefined;
    this._errorStack = props.errorStack ?? undefined;
    this._lastHeartbeatAt = props.lastHeartbeatAt ?? undefined;
    this.priority = props.priority ?? undefined;
    this._progress = props.progress ?? undefined;
    this.staleCount = props.staleCount;
  }

  private async completeMessage(sql: Sql): Promise<void> {
    await sql`
      UPDATE pg_transit_subscription_messages
      SET
        status = 'completed'
      WHERE
        subscription_id = ${this.subscription.id}
        AND message_id = ${this.id}
    `;

    this._status = 'completed';
  }

  async complete(): Promise<void> {
    if (this.subscription.consumptionMode === 'sequential') {
      await this.sql.begin(async (sql) => {
        await this.completeMessage(sql);

        await sql`
          UPDATE pg_transit_subscriptions
          SET
            processing = FALSE
          WHERE
            id = ${this.subscription.id}
        `;
      });
    } else {
      await this.completeMessage(this.sql);
    }
  }

  private async failMessage(sql: Sql, error: Error): Promise<void> {
    const status = this.attempts >= this.maxAttempts ? 'failed' : 'waiting';

    let availableAt: Date | null = null;
    if (this.attempts < this.maxAttempts) {
      if (this.subscription.retryStrategy === 'linear') {
        availableAt = new Date(Date.now() + this.subscription.retryDelay);
      }
      if (this.subscription.retryStrategy === 'exponential') {
        availableAt = new Date(Date.now() + this.subscription.retryDelay * Math.pow(2, this.attempts - 1));
      }
    }

    await sql`
      UPDATE pg_transit_subscription_messages
      SET
        ${sql({ status, available_at: availableAt, error_stack: error.stack ?? null })}
      WHERE
        subscription_id = ${this.subscription.id}
        AND message_id = ${this.id}
    `;

    this._status = status;
    this._availableAt = availableAt ?? undefined;
    this._errorStack = error.stack ?? undefined;
  }

  async fail(error: Error) {
    if (this.subscription.consumptionMode === 'sequential') {
      await this.sql.begin(async (sql) => {
        await this.failMessage(sql, error);

        await sql`
          UPDATE pg_transit_subscriptions
          SET
            processing = FALSE
          WHERE
            id = ${this.subscription.id}
        `;
      });
    } else {
      await this.failMessage(this.sql, error);
    }
  }

  async retry(): Promise<void> {
    await this.sql`
      UPDATE pg_transit_subscription_messages
      SET
        ${this.sql({
        status: 'waiting',
        available_at: null,
        error_stack: null,
      })}
      WHERE
        subscription_id = ${this.subscription.id}
        AND message_id = ${this.id}
    `;

    this._status = 'waiting';
    this._availableAt = undefined;
    this._errorStack = undefined;
  }

  async heartbeat(): Promise<void> {
    const now = new Date();

    await this.sql`
      UPDATE pg_transit_subscription_messages
      SET
        last_heartbeat_at = ${now}
      WHERE
        subscription_id = ${this.subscription.id}
        AND message_id = ${this.id}
    `;

    this._lastHeartbeatAt = now;
  }

  async updateProgress(value: JSONValue): Promise<void> {
    await this.sql`
      UPDATE pg_transit_subscription_messages
      SET
        progress = ${this.sql.json(value)}
      WHERE
        subscription_id = ${this.subscription.id}
        AND message_id = ${this.id}
    `;

    this._progress = value;

    this.emit('progress', this);
  }
}
