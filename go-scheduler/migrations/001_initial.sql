CREATE TABLE IF NOT EXISTS {{SCHEMA}}.schedules (
 schedule_key text PRIMARY KEY,
 revision bigint NOT NULL CHECK (revision >= 1),
 enabled boolean NOT NULL,
 calendar jsonb NOT NULL,
 target_alias text NOT NULL,
 operation text NOT NULL,
 payload jsonb NOT NULL,
 next_due_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS schedules_due_idx ON {{SCHEMA}}.schedules (next_due_at, schedule_key) WHERE enabled AND next_due_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.occurrences (
 occurrence_id uuid PRIMARY KEY,
 schedule_key text NOT NULL,
 schedule_revision bigint NOT NULL,
 scheduled_for timestamptz NOT NULL,
 target_alias text NOT NULL,
 operation text NOT NULL,
 payload jsonb NOT NULL,
 status text NOT NULL CHECK (status IN ('pending','leased','blocked','accepted')),
 attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
 available_at timestamptz NOT NULL,
 lease_token uuid,
 lease_until timestamptz,
 job_id uuid,
 last_error_code text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS occurrences_dispatch_idx ON {{SCHEMA}}.occurrences (available_at, created_at) WHERE status IN ('pending','leased');
CREATE INDEX IF NOT EXISTS occurrences_schedule_idx ON {{SCHEMA}}.occurrences (schedule_key, created_at, occurrence_id);
CREATE UNIQUE INDEX IF NOT EXISTS occurrences_one_unaccepted_per_schedule_idx ON {{SCHEMA}}.occurrences (schedule_key) WHERE status <> 'accepted';

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.control (
 id smallint PRIMARY KEY CHECK (id = 1),
 revision bigint NOT NULL CHECK (revision >= 0),
 paused boolean NOT NULL,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO {{SCHEMA}}.control (id, revision, paused) VALUES (1, 0, {{START_PAUSED}}) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.service_state (
 id smallint PRIMARY KEY CHECK (id = 1),
 last_tick_at timestamptz,
 last_dispatch_at timestamptz
);
INSERT INTO {{SCHEMA}}.service_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
