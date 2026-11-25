CREATE INDEX IF NOT EXISTS idx_article_processing_items_topic_ready
    ON __SCHEMA_Q__.article_processing_items ((updated_at AT TIME ZONE 'UTC'), article_id, claim_token)
    WHERE state = 'completed';

CREATE OR REPLACE VIEW __SCHEMA_Q__.topic_shadow_inputs AS
SELECT r.article_id, r.claim_token,
       a.source_id, a.title, a.summary, a.fetched_at AT TIME ZONE 'UTC' AS fetched_at,
       a.published_at AT TIME ZONE 'UTC' AS published_at,
       r.updated_at AT TIME ZONE 'UTC' AS ready_at,
       l.title AS serbian_title, l.summary AS serbian_summary
FROM __SCHEMA_Q__.article_processing_items r
JOIN __SCHEMA_Q__.articles a
  ON a.id = r.article_id AND a.completed_processing_claim = r.claim_token
LEFT JOIN __SCHEMA_Q__.article_localizations l
  ON l.article_id = a.id AND l.locale = 'sr-Latn'
 AND l.status = 'completed' AND l.updated_at <= r.updated_at
 AND nullif(btrim(l.title), '') IS NOT NULL AND nullif(btrim(l.summary), '') IS NOT NULL
WHERE r.state = 'completed' AND a.processing_state = 'completed'
  AND a.summary_processed IS TRUE AND a.is_advertisement IS NOT TRUE
  AND nullif(btrim(a.summary), '') IS NOT NULL;

CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.topic_shadow_checkpoints (
    policy_version text PRIMARY KEY CHECK (length(policy_version) BETWEEN 1 AND 80),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    scan_after timestamptz NOT NULL,
    scan_article_id integer NOT NULL DEFAULT 0,
    scan_claim uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    covered_through timestamptz,
    lease_owner uuid,
    lease_until timestamptz,
    api_day date,
    api_requests integer NOT NULL DEFAULT 0 CHECK (api_requests >= 0),
    CHECK ((lease_owner IS NULL) = (lease_until IS NULL))
);

ALTER TABLE __SCHEMA_Q__.topic_shadow_checkpoints
    ADD COLUMN IF NOT EXISTS replay_in_progress_at timestamptz;

CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.topic_shadow_topics (
    policy_version text NOT NULL REFERENCES __SCHEMA_Q__.topic_shadow_checkpoints(policy_version),
    topic_id uuid NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    state text NOT NULL CHECK (state IN ('candidate', 'active', 'dormant')),
    card jsonb NOT NULL CHECK (jsonb_typeof(card) = 'object'),
    first_source_at timestamptz NOT NULL,
    first_available_at timestamptz NOT NULL,
    material_count integer NOT NULL CHECK (material_count >= 0),
    opened_at timestamptz,
    last_material_at timestamptz,
    quieted_at timestamptz,
    PRIMARY KEY (policy_version, topic_id),
    CHECK ((state = 'candidate') = (opened_at IS NULL)),
    CHECK ((state = 'dormant') = (quieted_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_topic_shadow_topics_activity
    ON __SCHEMA_Q__.topic_shadow_topics (policy_version, last_material_at DESC, topic_id);

CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.topic_shadow_decisions (
    policy_version text NOT NULL REFERENCES __SCHEMA_Q__.topic_shadow_checkpoints(policy_version),
    article_id integer NOT NULL CHECK (article_id > 0),
    claim_token uuid NOT NULL,
    ready_at timestamptz NOT NULL,
    input jsonb NOT NULL CHECK (jsonb_typeof(input) = 'object'),
    state text NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'retry', 'accepted', 'review', 'superseded')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz,
    topic_id uuid,
    decided_at timestamptz,
    decision jsonb CHECK (jsonb_typeof(decision) = 'object'),
    PRIMARY KEY (policy_version, article_id, claim_token),
    FOREIGN KEY (policy_version, topic_id)
        REFERENCES __SCHEMA_Q__.topic_shadow_topics(policy_version, topic_id),
    CHECK ((state = 'retry') = (next_attempt_at IS NOT NULL)),
    CHECK ((state IN ('accepted', 'review', 'superseded')) = (decided_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_topic_shadow_decisions_pending
    ON __SCHEMA_Q__.topic_shadow_decisions (policy_version, ready_at, article_id, claim_token)
    WHERE state IN ('queued', 'retry');

CREATE INDEX IF NOT EXISTS idx_topic_shadow_decisions_topic
    ON __SCHEMA_Q__.topic_shadow_decisions (policy_version, topic_id, ready_at DESC, article_id DESC, claim_token)
    WHERE state = 'accepted' AND topic_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.topic_shadow_events (
    policy_version text NOT NULL,
    topic_id uuid NOT NULL,
    topic_revision bigint NOT NULL CHECK (topic_revision > 0),
    event_key text NOT NULL CHECK (length(event_key) BETWEEN 1 AND 160),
    kind text NOT NULL CHECK (kind IN ('opened', 'updated', 'quieted', 'returned')),
    event_at timestamptz,
    available_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL,
    article_id integer,
    claim_token uuid,
    evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence) = 'object'),
    PRIMARY KEY (policy_version, topic_id, event_key),
    UNIQUE (policy_version, topic_id, topic_revision, kind),
    FOREIGN KEY (policy_version, topic_id)
        REFERENCES __SCHEMA_Q__.topic_shadow_topics(policy_version, topic_id),
    FOREIGN KEY (policy_version, article_id, claim_token)
        REFERENCES __SCHEMA_Q__.topic_shadow_decisions(policy_version, article_id, claim_token),
    CHECK ((article_id IS NULL) = (claim_token IS NULL))
);

CREATE INDEX IF NOT EXISTS idx_topic_shadow_events_cutoff
    ON __SCHEMA_Q__.topic_shadow_events (policy_version, available_at, recorded_at);

CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.topic_shadow_editions (
    policy_version text NOT NULL REFERENCES __SCHEMA_Q__.topic_shadow_checkpoints(policy_version),
    cutoff_at timestamptz NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    created_at timestamptz NOT NULL,
    complete boolean NOT NULL,
    snapshot jsonb NOT NULL CHECK (jsonb_typeof(snapshot) = 'object'),
    PRIMARY KEY (policy_version, cutoff_at, revision)
);

REVOKE ALL ON __SCHEMA_Q__.topic_shadow_inputs, __SCHEMA_Q__.topic_shadow_checkpoints,
    __SCHEMA_Q__.topic_shadow_topics, __SCHEMA_Q__.topic_shadow_decisions,
    __SCHEMA_Q__.topic_shadow_events, __SCHEMA_Q__.topic_shadow_editions FROM PUBLIC;

DO $topic_shadow_grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'news_domain_runtime') THEN
        GRANT SELECT ON __SCHEMA_Q__.topic_shadow_inputs TO news_domain_runtime;
        GRANT SELECT, INSERT, UPDATE ON __SCHEMA_Q__.topic_shadow_checkpoints,
            __SCHEMA_Q__.topic_shadow_topics, __SCHEMA_Q__.topic_shadow_decisions TO news_domain_runtime;
        GRANT SELECT, INSERT ON __SCHEMA_Q__.topic_shadow_events,
            __SCHEMA_Q__.topic_shadow_editions TO news_domain_runtime;
    END IF;
END
$topic_shadow_grants$;
