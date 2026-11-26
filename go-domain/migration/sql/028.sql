CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.topic_shadow_semantic_index (
    policy_version text PRIMARY KEY,
    collection text NOT NULL UNIQUE,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    upper_fetched_at timestamptz NOT NULL,
    upper_article_id bigint NOT NULL,
    cursor_fetched_at timestamptz NOT NULL,
    cursor_article_id bigint NOT NULL DEFAULT 0,
    scanned_articles bigint NOT NULL DEFAULT 0 CHECK (scanned_articles >= 0),
    completed_at timestamptz,
    prune_cursor text,
    lease_owner uuid,
    lease_until timestamptz,
    embedding_day date,
    embedding_requests integer NOT NULL DEFAULT 0 CHECK (embedding_requests >= 0),
    embedding_tokens bigint NOT NULL DEFAULT 0 CHECK (embedding_tokens >= 0),
    CHECK ((lease_owner IS NULL) = (lease_until IS NULL))
);

REVOKE ALL ON __SCHEMA_Q__.topic_shadow_semantic_index FROM PUBLIC;
DO $semantic_grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'news_domain_runtime') THEN
        GRANT SELECT, INSERT, UPDATE ON __SCHEMA_Q__.topic_shadow_semantic_index TO news_domain_runtime;
    END IF;
END
$semantic_grants$;

ALTER TABLE __SCHEMA_Q__.integration_credentials DROP CONSTRAINT IF EXISTS integration_credentials_provider_check;
ALTER TABLE __SCHEMA_Q__.integration_credentials ADD CONSTRAINT integration_credentials_provider_check CHECK (provider IN ('gemini','openai','telegram','viber','apns','jev'));
