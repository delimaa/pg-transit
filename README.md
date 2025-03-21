# PgTransit

**Reliable Messaging, Queues, and Event Logs — All in PostgreSQL.**

PgTransit is a developer-friendly messaging library for Node.js that unifies queues, event logs, and pub/sub into a single, expressive API — built entirely on PostgreSQL.

## 🚀 Features

### 1. **Unified Messaging Model**

- **One abstraction for all use cases**: sequential event logs, parallel job queues, and pub/sub messaging.
- Design your messaging architecture using a single API with flexible consumption modes.

### 2. **Built on PostgreSQL**

- **No new infrastructure**: use your existing Postgres database.
- Leverages proven SQL patterns (`SKIP LOCKED`, transactions, etc.) for reliable and concurrent message processing.

### 3. **Developer-Friendly by Design**

- **Zero-ceremony API** with sensible defaults and batteries included.
- Built-in support for:
  - Retries
  - Delayed delivery
  - Fan-out messaging
  - Concurrency control
  - Stale message handling
- Fully typed, testable, and ergonomic for real-world applications.

---

## 📦 Installation

```sh
npm install pg-transit
```

## ⚡️ Usage

### 1. Create a PgTransit Instance

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

Because PgTransit uses [postgres package](https://www.npmjs.com/package/postgres) under the hood, the `connection` property above accepts complete Postgres.js library options object. You can [refer to Postgres.js documentation](https://github.com/porsager/postgres?tab=readme-ov-file#connection) for more details on connection options.

### 2. Define a Topic

A topic is a channel through which messages are sent and distributed to subscriptions.

```ts [order-placed-topic.ts]
export const orderPlacedTopic = transit.topic('order-placed');
```

### 3. Create Subscriptions

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

### 4. Consume Messages

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

### 5. Send Messages

Send messages to a topic to trigger consumers. PgTransit handles delivery, retries, and delays automatically.

```ts [pay-order.ts]
await orderPlacedTopic.send({
  orderId: '12345',
  amount: 100,
  address: '123 Main St'
});
```

## ❤️ Why PgTransit?

Modern applications rely heavily on messaging — whether it’s background job queues, audit logs, or cross-service communication. Instead of introducing new infrastructure, PgTransit lets you:

- Use PostgreSQL as a reliable message store
- Model queues, logs, and pub/sub with one consistent interface
- Ship faster with fewer moving parts

## 🛠 Requirements

- Node.js >= 16
- PostgreSQL >= 12

## 📄 License

PgTransit is licensed under the [MIT License](https://opensource.org/licenses/MIT).
