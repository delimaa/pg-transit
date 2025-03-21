# Getting Started

This guide will walk you through the key steps to get PgTransit up and running in your Node.js project. By the end of this page, you’ll be ready to define topics, create subscriptions, and start sending and consuming messages.

## 1. Installation

::: code-group

```sh [npm]
npm install pg-transit
```

```sh [yarn]
yarn add pg-transit
```

```sh [pnpm]
pnpm add pg-transit
```

```sh [bun]
bun add pg-transit
```

:::

## 2. Setting Up PostgreSQL

PgTransit stores all its data in your PostgreSQL database. You can use an existing instance or set up a new one locally or in the cloud.

## 3. Creating a PgTransit Instance

Initialize PgTransit by connecting it to your PostgreSQL database. This instance will manage topics, subscriptions and sending and consuming messages.

```ts [transit.ts]
import { pgTransit } from 'pg-transit';

export const transit = pgTransit({
  connection: {
    // use a connection string
    url: 'postgres://user:password@localhost:5432/mydb',

    // or pass connection parameters one by one
    host: 'localhost',
    port: 5432,
    user: 'user',
    password: 'password',
    database: 'mydb',
  }
});
```

Because PgTransit uses [postgres package](https://www.npmjs.com/package/postgres) under the hood, the `connection` property above accepts complete Postgres.js library options object. You can [refer to Postgres.js documentation](https://github.com/porsager/postgres) for more details on connection options.

## 4. Defining a Topic

A topic is a channel through which messages are sent and distributed to subscriptions.

```ts [order-placed-topic.ts]
export const orderPlacedTopic = transit.topic('order-placed');
```

## 5. Creating Subscriptions

Subscriptions receive messages from a topic. You can think of them as a group of consumers that process messages from a topic.

Subscriptions enable job queues style parallel consumption, event logs style sequential consumption or even pub/sub style fan-out delivery.

```ts [log-order-subscription.ts]
export const logOrderSubscription = orderPlacedTopic.subscribe('log-order', {
  consumptionMode: 'sequential' // Consume messages one by one
});
```

```ts [ship-order-subscription.ts]
export const shipOrderSubscription = orderPlacedTopic.subscribe('ship-order', {
  consumptionMode: 'parallel' // Consume messages in parallel
  maxAttempts: 3 // Retry up to 2 times on failure
});
```

## 6. Consuming Messages

Attach consumers to your subscription to start processing messages.

```ts [log-order-consumer.ts]
export const logOrderConsumer = logOrderSubscription.consume((message) => {
  // Your logging logic here
  logger.info(`Order placed: ${message.data.orderId}`);
});
```

```ts [ship-order-consumer.ts]
const shipOrderConsumer = shipOrderSubscription.consume(async (message) => {
  // Your shipping logic here
  await shippingService.shipOrder({
    amount: message.data.amount,
    address: message.data.address,
    // ...
  });
}, {
  concurrency: 5, // Process up to 5 messages in parallel for this consumer
});
```

## 7. Sending Messages

Send messages to a topic to trigger consumers. PgTransit handles delivery, retries, and delays automatically.

```ts [pay-order.ts]
await orderPlacedTopic.send({
  orderId: '12345',
  amount: 100,
  address: '123 Main St'
});
```

## 8. Next Steps

You’re now ready to build more advanced features with PgTransit! Here are some topics you might want to explore next:

- Retry policies
- Delayed messages
- Stale message detection
- And more!

Explore the rest of the documentation to dive deeper.
