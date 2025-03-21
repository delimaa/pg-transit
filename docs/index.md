---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: 'PgTransit'
  text: 'Reliable Messaging, Queues, and Event Logs - All in PostgreSQL'
  tagline: 'A lightweight, developer-friendly Node.js library for event-driven systems, job queues, and pub/sub built on Postgres.'
  image:
    src: /home-image.png
    alt: VitePress
  actions:
    - theme: brand
      text: Why PgTransit?
      link: /why
    - theme: alt
      text: GitHub
      link: https://github.com/delimaa/pg-transit
    - theme: alt
      text: NPM
      link: https://github.com/delimaa/pg-transit

features:
  - title: 'Unified Messaging Model'
    details: '<b>Event logs, job queues, and pub/sub — all in one consistent abstraction.</b><br /><br />Design your system with a single messaging API that adapts to your needs: sequential event consumption, parallel job processing, or fan-out messaging between services.'
  - title: 'Built on PostgreSQL'
    details: '<b>No extra infrastructure. No new ops burden.</b><br /><br />Leverage your existing Postgres database for durable, transactional messaging. PgTransit uses battle-tested SQL patterns like SKIP LOCKED to ensure reliable, concurrent message handling at scale.'
  - title: 'Developer-Friendly by Design'
    details: '<b>Zero-ceremony API, batteries-included features.</b><br /><br />Get retries, delayed delivery, fan-out subscriptions, concurrency control, and stale message handling — all through a simple, expressive Node.js interface built with testability and DX in mind.'
---
