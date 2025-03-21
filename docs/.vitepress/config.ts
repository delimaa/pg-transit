import { defineConfig } from 'vitepress';
import { groupIconMdPlugin, groupIconVitePlugin } from 'vitepress-plugin-group-icons';

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: 'PgTransit - Reliable Messaging, Queues, and Event Logs - All in PostgreSQL',
  description:
    'A lightweight, developer-friendly Node.js library for event-driven systems, job queues, and pub/sub built on Postgres.',
  head: [['link', { rel: 'icon', href: '/favicon.png' }]],
  themeConfig: {
    siteTitle: false,
    logo: {
      src: './logo-full.png',
      alt: 'PgTransit Logo',
    },
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Examples', link: '/' },
    ],

    sidebar: [
      {
        text: 'Introduction',
        items: [
          { text: 'Why PgTransit?', link: '/why' },
          { text: 'Getting Started', link: '/getting-started' },
        ],
      },
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/delimaa/pg-transit' },
      { icon: 'npm', link: 'https://github.com/delimaa/pg-transit' },
    ],
  },
  markdown: {
    config(md) {
      md.use(groupIconMdPlugin);
    },
  },
  vite: {
    plugins: [groupIconVitePlugin()],
  },
});
