export const migration1_0_0 = `
-- Topics
CREATE TABLE IF NOT EXISTS pg_transit_topics (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Messages
CREATE TABLE IF NOT EXISTS pg_transit_messages (
  id UUID PRIMARY KEY,
  topic_id UUID NOT NULL REFERENCES pg_transit_topics(id) ON DELETE CASCADE,
  data JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deliver_at TIMESTAMPTZ,
  priority INTEGER
);

-- Scheduled messages
CREATE TABLE IF NOT EXISTS pg_transit_scheduled_messages (
  topic_id UUID NOT NULL REFERENCES pg_transit_topics(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  data JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ,
  next_occurrence_at TIMESTAMPTZ NOT NULL,
  cron TEXT NOT NULL,
  deliver_in_ms INTEGER,
  deliver_at TIMESTAMPTZ,
  priority INTEGER,
  repeats INTEGER,
  repeats_made INTEGER DEFAULT 0,
  PRIMARY KEY (topic_id, name)
);

CREATE INDEX IF NOT EXISTS pg_transit_scheduled_messages_topic_id_idx ON pg_transit_scheduled_messages (topic_id);
CREATE INDEX IF NOT EXISTS pg_transit_scheduled_messages_next_occurrence_at_idx ON pg_transit_scheduled_messages (next_occurrence_at);

-- Subscriptions
CREATE TABLE IF NOT EXISTS pg_transit_subscriptions (
  id UUID PRIMARY KEY,
  topic_id UUID NOT NULL REFERENCES pg_transit_topics(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processing BOOLEAN NOT NULL DEFAULT false,
  consumption_mode TEXT NOT NULL DEFAULT 'sequential',
  start_position TEXT NOT NULL DEFAULT 'earliest',
  max_attempts INTEGER NOT NULL DEFAULT 1,
  retry_strategy TEXT NOT NULL DEFAULT 'linear',
  retry_delay INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS pg_transit_subscriptions_topic_id_idx ON pg_transit_subscriptions (topic_id);
CREATE UNIQUE INDEX IF NOT EXISTS pg_transit_subscriptions_topic_id_name_idx ON pg_transit_subscriptions (topic_id, name);

-- Subscription messages
DO $$ BEGIN
  CREATE TYPE pg_transit_message_status AS ENUM ('waiting', 'processing', 'completed', 'failed');
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS pg_transit_subscription_messages (
  subscription_id UUID NOT NULL REFERENCES pg_transit_subscriptions(id) ON DELETE CASCADE,
  message_id UUID NOT NULL REFERENCES pg_transit_messages(id) ON DELETE CASCADE,
  status pg_transit_message_status NOT NULL DEFAULT 'waiting',
  attempts INTEGER NOT NULL DEFAULT 0,
  available_at TIMESTAMPTZ,
  error_stack TEXT,
  last_heartbeat_at TIMESTAMPTZ,
  progress JSONB,
  stale_count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (subscription_id, message_id)
);

CREATE INDEX IF NOT EXISTS pg_transit_subscription_messages_subscription_id_idx ON pg_transit_subscription_messages (subscription_id);
CREATE INDEX IF NOT EXISTS pg_transit_subscription_messages_message_id_idx ON pg_transit_subscription_messages (message_id);
CREATE INDEX IF NOT EXISTS pg_transit_subscription_messages_status_idx ON pg_transit_subscription_messages (status);
CREATE INDEX IF NOT EXISTS pg_transit_subscription_messages_available_at_idx ON pg_transit_subscription_messages (available_at);
CREATE INDEX IF NOT EXISTS pg_transit_subscription_messages_last_heartbeat_at_idx ON pg_transit_subscription_messages (last_heartbeat_at);
`;
