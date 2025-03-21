import type { JSONValue, Sql } from 'postgres';

export type ScheduledMessageConfig = {
  /**
   * CRON expression to schedule the message.
   *
   * Uses `cron-parser` library under the hood to parse the CRON expression.
   */
  cron: string;

  /**
   * Number of repetitions for the scheduled message.
   *
   * Repeat indefinitely by default.
   */
  repeats?: number;
};

export type ScheduledMessageRow<T extends JSONValue> = {
  topic_id: string;
  name: string;
  data: T;
  created_at: Date;
  updated_at: Date | null;
  next_occurrence_at: Date;
  cron: string;
  deliver_in_ms: number | null;
  deliver_at: Date | null;
  priority: number | null;
  repeats: number | null;
  repeats_made: number;
};

export class ScheduledMessage<T extends JSONValue> {
  get topicId(): string {
    return this.row.topic_id;
  }

  get name(): string {
    return this.row.name;
  }

  get data(): JSONValue {
    return this.row.data;
  }

  get createdAt(): Date {
    return this.row.created_at;
  }

  get updatedAt(): Date | undefined {
    return this.row.updated_at ?? undefined;
  }

  get nextOccurrenceAt(): Date {
    return this.row.next_occurrence_at;
  }

  get cron(): string {
    return this.row.cron;
  }

  get deliverInMs(): number | undefined {
    return this.row.deliver_in_ms ?? undefined;
  }

  get deliverAt(): Date | undefined {
    return this.row.deliver_at ?? undefined;
  }

  get priority(): number | undefined {
    return this.row.priority ?? undefined;
  }

  get repeats(): number | undefined {
    return this.row.repeats ?? undefined;
  }

  get repeatsMade(): number {
    return this.row.repeats_made;
  }

  constructor(
    private readonly sql: Sql,
    private readonly row: ScheduledMessageRow<T>,
  ) {}

  async remove(): Promise<void> {
    await this.sql`
      DELETE FROM pg_transit_scheduled_messages
      WHERE
        topic_id = ${this.topicId}
        AND name = ${this.name}
    `;
  }
}
