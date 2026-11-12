ALTER TABLE __SCHEMA_Q__.integration_credentials DROP CONSTRAINT IF EXISTS integration_credentials_provider_check;
ALTER TABLE __SCHEMA_Q__.integration_credentials ADD CONSTRAINT integration_credentials_provider_check CHECK (provider IN ('gemini','openai','telegram','viber','apns'));

CREATE TABLE __SCHEMA_Q__.push_installations (
 installation_id UUID PRIMARY KEY,
 capability_hash BYTEA NOT NULL CHECK (octet_length(capability_hash)=32),
 token_hash BYTEA NOT NULL CHECK (octet_length(token_hash)=32),
 token_envelope_version INTEGER,
 token_key_id TEXT,
 token_nonce BYTEA,
 token_ciphertext BYTEA,
 locale TEXT NOT NULL CHECK(locale IN ('ru','sr-Latn')),
 environment TEXT NOT NULL CHECK(environment IN ('sandbox','production')),
 state TEXT NOT NULL CHECK(state IN ('pending','active','disabled','invalid')),
 subscription_version BIGINT NOT NULL CHECK(subscription_version>0),
 challenge_hash BYTEA,
 challenge_expires_at TIMESTAMPTZ,
 challenge_attempts SMALLINT NOT NULL DEFAULT 0 CHECK(challenge_attempts BETWEEN 0 AND 8),
 activated_at TIMESTAMPTZ,
 version_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CHECK ((token_envelope_version IS NULL)=(token_key_id IS NULL) AND (token_key_id IS NULL)=(token_nonce IS NULL) AND (token_nonce IS NULL)=(token_ciphertext IS NULL)),
 CHECK ((challenge_hash IS NULL)=(challenge_expires_at IS NULL))
);
CREATE INDEX idx_push_installations_active ON __SCHEMA_Q__.push_installations(locale,environment) WHERE state='active';
CREATE TABLE __SCHEMA_Q__.push_readiness_events (
 id BIGSERIAL PRIMARY KEY,
 edition_id UUID NOT NULL REFERENCES __SCHEMA_Q__.editorial_digest_editions(id),
 edition_date DATE NOT NULL,
 locale TEXT NOT NULL CHECK(locale IN ('ru','sr-Latn')),
 ready_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 fanout_completed_at TIMESTAMPTZ,
 fanout_cursor UUID,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(edition_date,locale), UNIQUE(edition_id)
);
CREATE TABLE __SCHEMA_Q__.push_challenges (
 installation_id UUID NOT NULL REFERENCES __SCHEMA_Q__.push_installations(installation_id),
 subscription_version BIGINT NOT NULL,
 nonce_hash BYTEA CHECK(nonce_hash IS NULL OR octet_length(nonce_hash)=32),
 nonce_envelope_version INTEGER,
 nonce_key_id TEXT,
 nonce_nonce BYTEA,
 nonce_ciphertext BYTEA,
 environment TEXT NOT NULL CHECK(environment IN ('sandbox','production')),
 state TEXT NOT NULL CHECK(state IN ('pending','sending','retry_wait','accepted','confirmed','expired','cancelled','unknown')),
 apns_id UUID NOT NULL,
 expires_at TIMESTAMPTZ NOT NULL,
 attempts SMALLINT NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 5),
 next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 lease_id UUID,
 lease_until TIMESTAMPTZ,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(installation_id,subscription_version)
);
CREATE TABLE __SCHEMA_Q__.push_deliveries (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 event_id BIGINT REFERENCES __SCHEMA_Q__.push_readiness_events(id),
 installation_id UUID NOT NULL REFERENCES __SCHEMA_Q__.push_installations(installation_id),
 subscription_version BIGINT NOT NULL,
 edition_id UUID NOT NULL REFERENCES __SCHEMA_Q__.editorial_digest_editions(id),
 edition_date DATE NOT NULL,
 locale TEXT NOT NULL CHECK(locale IN ('ru','sr-Latn')),
 token_hash BYTEA NOT NULL CHECK(octet_length(token_hash)=32),
 environment TEXT NOT NULL CHECK(environment IN ('sandbox','production')),
 kind TEXT NOT NULL DEFAULT 'digest' CHECK(kind IN ('challenge','digest','canary')),
 state TEXT NOT NULL CHECK(state IN ('pending','sending','retry_wait','accepted','rejected','unknown','cancelled','expired')),
 expires_at TIMESTAMPTZ NOT NULL DEFAULT (now()+interval '24 hours'),
 attempts SMALLINT NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 5),
 next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 lease_id UUID,
 lease_until TIMESTAMPTZ,
 apns_id UUID NOT NULL,
 status_code SMALLINT NOT NULL DEFAULT 0,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(event_id,installation_id)
);
CREATE UNIQUE INDEX uq_push_digest_installation_day_locale ON __SCHEMA_Q__.push_deliveries(installation_id,edition_date,locale) WHERE kind='digest';
CREATE UNIQUE INDEX uq_push_digest_token_day_locale ON __SCHEMA_Q__.push_deliveries(token_hash,environment,edition_date,locale) WHERE kind='digest';
ALTER TABLE __SCHEMA_Q__.push_deliveries ADD COLUMN idempotency_key UUID;
CREATE UNIQUE INDEX uq_push_canary_idempotency ON __SCHEMA_Q__.push_deliveries(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_push_deliveries_due ON __SCHEMA_Q__.push_deliveries(next_attempt_at,created_at) WHERE state IN ('pending','retry_wait');
CREATE INDEX idx_push_deliveries_lease ON __SCHEMA_Q__.push_deliveries(lease_until) WHERE state='sending';
CREATE TABLE __SCHEMA_Q__.push_rate_buckets (
 bucket_key BYTEA PRIMARY KEY CHECK(octet_length(bucket_key)=32),
 window_started_at TIMESTAMPTZ NOT NULL,
 hits INTEGER NOT NULL CHECK(hits BETWEEN 0 AND 10000),
 expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_push_rate_buckets_expiry ON __SCHEMA_Q__.push_rate_buckets(expires_at);
CREATE TABLE __SCHEMA_Q__.push_cutover (id SMALLINT PRIMARY KEY CHECK(id=1), enabled BOOLEAN NOT NULL DEFAULT FALSE, sender_enabled BOOLEAN NOT NULL DEFAULT FALSE, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
INSERT INTO __SCHEMA_Q__.push_cutover(id,enabled) VALUES(1,FALSE);
