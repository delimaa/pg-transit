import { EventEmitter } from 'node:events';
import type { SubscriptionMessage } from './subscription-message';
import type { Subscription } from './subscription';
import { loop, patchAsyncMethods } from './utils';
import type { JSONValue } from 'postgres';

export type ConsumerOptions = {
  /**
   * The maximum number of messages to consume concurrently.
   *
   * Only applies to consumers attached to parallel subscriptions.
   *
   * @default 1
   */
  concurrency?: number;

  /**
   * Whether to start consuming messages immediately after creating the consumer.
   *
   * If `false`, the consumer will not start consuming messages until `start()` is called.
   *
   * @default true
   */
  autostart?: boolean;

  /**
   * The interval in milliseconds to poll for new messages.
   *
   * @default 1000 // 1 second
   */
  pollingIntervalInMs?: number;

  /**
   * The interval in milliseconds to send heartbeats to keep the message alive.
   *
   * @default 10_000 // 10 seconds
   */
  heartbeatIntervalInMs?: number;
};

export class Consumer<T extends JSONValue> extends EventEmitter<{
  completed: [message: SubscriptionMessage<T>];
  failed: [message: SubscriptionMessage<T>, error: Error];
  process: [message: SubscriptionMessage<T>];
  progress: [message: SubscriptionMessage<T>];
  idle: [];
  consume: [];
}> {
  private readonly init: Promise<void>;

  private readonly subscription: Subscription<T>;

  private readonly handler: (message: SubscriptionMessage<T>) => void | Promise<void>;

  private _isStarted = false;
  get isStarted() {
    return this._isStarted;
  }

  private stopConsumeLoop?: () => void;

  /** A promise that resolves when the consumer stops processing (becomes idle) */
  private waitIdlePromise?: Promise<void>;

  get isIdle() {
    return !this.waitIdlePromise;
  }

  private consumingMessageCount = 0;

  private _concurrency!: number;
  get concurrency() {
    return this._concurrency;
  }
  set concurrency(value: number) {
    if (value < 1) {
      throw new Error('Concurrency must be greater than 0');
    }

    if (this.subscription.consumptionMode === 'sequential') {
      this._concurrency = 1; // Ignore concurrency on sequential subscriptions
    } else {
      this._concurrency = value;
    }
  }

  readonly pollingIntervalInMs: number;

  readonly heartbeatIntervalInMs: number;

  constructor(props: {
    subscription: Subscription<T>;
    handler: (message: SubscriptionMessage<T>) => void | Promise<void>;
    options?: ConsumerOptions;
  }) {
    super();

    this.subscription = props.subscription;
    this.handler = props.handler;

    this.pollingIntervalInMs = props.options?.pollingIntervalInMs ?? 1_000; // Default to 1 second
    this.heartbeatIntervalInMs = props.options?.heartbeatIntervalInMs ?? 10_000; // Default to 10 seconds

    this.init = (async () => {
      await (props.subscription as any).init;

      // Set consumer concurrency after subscription is initialized to have access to consumption mode in the `set concurrency` setter
      this.concurrency = props.options?.concurrency ?? 1; // Default to 1

      const autostart = props.options?.autostart ?? true;
      if (autostart) {
        this.start();
      }
    })();
  }

  async waitInit(): Promise<void> {
    await this.init;
  }

  start() {
    if (this.isStarted) {
      return;
    }

    this._isStarted = true;

    this.stopConsumeLoop = loop(
      async () => {
        await this.consume();
      },
      this.pollingIntervalInMs,
      { immediate: true },
    );
  }

  async stop() {
    if (!this.isStarted) {
      return;
    }

    this._isStarted = false;

    this.stopConsumeLoop?.();

    await this.waitIdle();
  }

  async waitIdle() {
    await this.waitIdlePromise;
  }

  async consume() {
    this.waitIdlePromise ??= new Promise((resolve) => {
      this.emit('consume');

      // This counter is used to track the number of consumeNextMessages calls in the event loop to be able to pass the consumer as idle only on last call
      let consumeNextMessagesCallCount = 0;

      const consumeNextMessages = async () => {
        const free = this.concurrency - this.consumingMessageCount;

        if (free <= 0) {
          return;
        }

        consumeNextMessagesCallCount++;

        try {
          const messages = await this.subscription.getNextMessages(free);

          if (messages.length === 0 && this.consumingMessageCount === 0 && consumeNextMessagesCallCount === 1) {
            this.waitIdlePromise = undefined;

            resolve();

            this.emit('idle');

            return;
          }

          // Process all messages in the background. Try to pick up next messages as soon as a consumer concurrency slot is free
          messages.forEach(async (message) => {
            await this.processMessage(message);

            await consumeNextMessages();
          });
        } finally {
          consumeNextMessagesCallCount--;
        }
      };

      consumeNextMessages();
    });

    await this.waitIdlePromise;
  }

  private async processMessage(message: SubscriptionMessage<T>) {
    this.consumingMessageCount++;

    const stopHeartbeatLoop = loop(async () => {
      await message.heartbeat();
    }, this.heartbeatIntervalInMs);

    let error: Error | undefined;

    this.emit('process', message);

    const progressListener = (message: SubscriptionMessage<T>) => {
      this.emit('progress', message);
    };
    message.on('progress', progressListener);

    try {
      try {
        await this.handler(message);
      } catch (e) {
        error = e as Error;
      }

      if (error) {
        await message.fail(error);
      } else {
        await message.complete();
      }
    } finally {
      this.consumingMessageCount--;

      stopHeartbeatLoop();

      message.off('progress', progressListener);
    }

    if (error) {
      this.emit('failed', message, error);
    } else {
      this.emit('completed', message);
    }
  }
}

patchAsyncMethods(Consumer.prototype, { doBefore: (instance) => (instance as any).init });
