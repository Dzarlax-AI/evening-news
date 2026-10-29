CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.installation_state (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    installation_id TEXT NOT NULL UNIQUE,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    provisioning_complete BOOLEAN NOT NULL DEFAULT false,
    maintenance_mode BOOLEAN NOT NULL DEFAULT false,
    onboarding_step TEXT NOT NULL DEFAULT 'storage',
    onboarding_completed BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.managed_admin (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.setup_tokens (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    token_hash BYTEA NOT NULL CHECK (octet_length(token_hash) = 32),
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.integration_credentials (
    provider TEXT PRIMARY KEY CHECK (provider IN ('gemini','openai','telegram')),
    revision BIGINT NOT NULL CHECK (revision > 0),
    envelope_version INTEGER,
    key_id TEXT,
    nonce BYTEA,
    ciphertext BYTEA,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((envelope_version IS NULL AND key_id IS NULL AND nonce IS NULL AND ciphertext IS NULL)
        OR (envelope_version IS NOT NULL AND key_id IS NOT NULL AND nonce IS NOT NULL AND ciphertext IS NOT NULL))
);
