# Why PgTransit?

PgTransit is a messaging and job processing library designed for developers who want powerful, flexible communication between services — without introducing new infrastructure. If you're building modern Node.js applications and already use PostgreSQL, PgTransit gives you the tools to implement queues, event logs, and pub/sub patterns natively.

## 🧠 One Model

PgTransit unifies three common messaging paradigms in a single consistent API:

- **Job Queues** — Distribute work across workers with automatic retry, concurrency limits, and parallel consumption.
- **Event Logs** — Achieve append-only sequential consumption for event sourcing and audit trails.
- **Pub/Sub** — Decouple services with fan-out delivery to multiple independent subscribers.

No need to choose a different tool or mental model for each messaging style — PgTransit adapts to your architecture.

## 🐘 Powered by PostgreSQL

Why operate an entirely separate messaging stack when your database already does 90% of the work?

- **No extra infrastructure** - Run messaging within your existing PostgreSQL instance.

- **Built-in durability and consistency** - Use the reliability of ACID-compliant transactions to guarantee message safety.

- **Battle-tested SQL patterns** - Under the hood, PgTransit uses proven approaches like FOR UPDATE SKIP LOCKED, leveraging PostgreSQL’s locking and indexing mechanisms to safely distribute work across consumers.

## 💡 Zero-Ceremony Developer Experience

PgTransit was designed from the start with DX and simplicity in mind:

- Expressive API with full TypeScript support
- Built-in support for retries, delays, stale message detection, and more
- Subscription-level control over concurrency and ordering
- Clear event-driven programming model with testability in mind

## 🛠️ Use Cases

PgTransit is an ideal fit for:

- Microservices coordination without a dedicated message broker
- Event-driven backend systems with PostgreSQL at the core
- Background job queues (email, billing, webhooks)
- Internal pub/sub communication across services
- Lightweight event sourcing setups

## 🔄 Reliable, Observable, Extensible

- Message attempts and delivery history tracking
- Heartbeat-based stale detection
- Optional retry and delay policies
- Easily introspect and debug from within Postgres

## ✅ Choose PgTransit If You Want

- Messaging built into your database, not bolted on
- A unified API for multiple messaging styles
- Simplicity, observability, and reliability without extra infrastructure
- Seamless integration in PostgreSQL-first Node.js environments

## 🚀 Ready to get started?

**Turn your Postgres into a powerful messaging hub**

Set up your topics, define subscriptions, and start processing messages.

[Get started ->](/getting-started)
