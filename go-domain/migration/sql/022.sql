ALTER TABLE __SCHEMA_Q__.articles
    ADD COLUMN source_title TEXT CHECK (char_length(source_title) <= 500),
    ADD COLUMN completed_processing_claim UUID;

CREATE TABLE __SCHEMA_Q__.article_dedup_observations (
    article_id INTEGER NOT NULL REFERENCES __SCHEMA_Q__.articles(id) ON DELETE CASCADE,
    version SMALLINT NOT NULL CHECK (version > 0),
    article_attempt INTEGER NOT NULL CHECK (article_attempt > 0),
    claim_token UUID NOT NULL,
    exact_hash BYTEA CHECK (exact_hash IS NULL OR octet_length(exact_hash) = 32),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (article_id, version)
);

CREATE INDEX idx_article_dedup_exact ON __SCHEMA_Q__.article_dedup_observations
    (version, exact_hash, registered_at, article_id) WHERE exact_hash IS NOT NULL;

CREATE INDEX idx_article_dedup_recent ON __SCHEMA_Q__.articles (fetched_at DESC, id DESC)
    WHERE processing_state = 'completed' AND processed IS TRUE
      AND is_advertisement IS NOT TRUE AND summary_processed IS TRUE;
