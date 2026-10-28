CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.editorial_digest_editions (
                id UUID PRIMARY KEY,
                edition_date DATE NOT NULL,
                locale VARCHAR(16) NOT NULL CHECK (locale='ru'),
                policy_version VARCHAR(32) NOT NULL
                    CHECK (policy_version ~ '^[a-z0-9-]{1,32}$'),
                snapshot_hash VARCHAR(64) NOT NULL CHECK (snapshot_hash ~ '^[0-9a-f]{64}$'),
                snapshot JSONB NOT NULL CHECK (jsonb_typeof(snapshot)='object'
                    AND jsonb_typeof(snapshot->'articles')='array'
                    AND jsonb_array_length(snapshot->'articles') BETWEEN 1 AND 500),
                edition JSONB NOT NULL CHECK (jsonb_typeof(edition)='object'
                    AND edition->>'version'='1' AND edition->>'locale'='ru'
                    AND edition->>'source_snapshot_hash'=snapshot_hash
                    AND jsonb_typeof(edition->'stories')='array'),
                rendered JSONB NOT NULL CHECK (jsonb_typeof(rendered)='object'
                    AND jsonb_typeof(rendered->'rich_html')='string'
                    AND jsonb_typeof(rendered->'plain_parts')='array'
                    AND jsonb_typeof(rendered->'preview_text')='string'),
                provider_usage JSONB NOT NULL DEFAULT '{}'::jsonb
                    CHECK (jsonb_typeof(provider_usage)='object'),
                job_id UUID,
                attempt_generation INTEGER CHECK (attempt_generation IS NULL OR attempt_generation > 0),
                created_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
                CHECK ((job_id IS NULL) = (attempt_generation IS NULL))
            );

CREATE UNIQUE INDEX IF NOT EXISTS uq_editorial_digest_job_attempt_locale
            ON __SCHEMA_Q__.editorial_digest_editions(job_id, attempt_generation, locale);

CREATE INDEX IF NOT EXISTS idx_editorial_digest_date_locale_created
            ON __SCHEMA_Q__.editorial_digest_editions(edition_date, locale, created_at DESC);

CREATE OR REPLACE FUNCTION __SCHEMA_Q__.reject_editorial_digest_mutation()
            RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog, __SCHEMA_Q__
            AS $function$
            BEGIN
                RAISE EXCEPTION 'editorial_digest_editions are immutable' USING ERRCODE='55000';
            END
            $function$;

DROP TRIGGER IF EXISTS trg_editorial_digest_immutable
            ON __SCHEMA_Q__.editorial_digest_editions;

CREATE TRIGGER trg_editorial_digest_immutable
            BEFORE UPDATE OR DELETE ON __SCHEMA_Q__.editorial_digest_editions
            FOR EACH ROW EXECUTE FUNCTION __SCHEMA_Q__.reject_editorial_digest_mutation();
