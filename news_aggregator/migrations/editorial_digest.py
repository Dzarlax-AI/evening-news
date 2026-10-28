"""Add immutable, complete editorial edition records for RU digests."""

from typing import Any, Dict

from sqlalchemy import text

from .base_migration import BaseMigration

REQUIRED_MIGRATION_ID = "014_editorial_digest"


def ensure_editorial_digest_migration(results: Dict[str, Any]) -> None:
    errors = [str(error) for error in results.get("errors", [])]
    failures = [error for error in errors if REQUIRED_MIGRATION_ID in error
                or error.startswith("Migration system error:")]
    if failures:
        raise RuntimeError("Required editorial digest migration failed: " + "; ".join(failures))


class EditorialDigestMigration(BaseMigration):
    def __init__(self):
        super().__init__(REQUIRED_MIGRATION_ID, "Persist immutable Russian editorial editions", "1.0.0")

    async def check_needed(self, db):
        table = await db.scalar(text("""
            SELECT EXISTS (SELECT 1 FROM information_schema.tables
              WHERE table_schema=current_schema() AND table_name='editorial_digest_editions')
        """))
        index = await db.scalar(text("""
            SELECT EXISTS (SELECT 1 FROM pg_indexes
              WHERE schemaname=current_schema() AND indexname='uq_editorial_digest_job_attempt_locale')
        """))
        date_index = await db.scalar(text("""
            SELECT EXISTS (SELECT 1 FROM pg_indexes
              WHERE schemaname=current_schema() AND indexname='idx_editorial_digest_date_locale_created')
        """))
        trigger = await db.scalar(text("""
            SELECT EXISTS (
              SELECT 1 FROM pg_trigger t
              JOIN pg_class c ON c.oid=t.tgrelid
              JOIN pg_namespace n ON n.oid=c.relnamespace
              WHERE n.nspname=current_schema() AND c.relname='editorial_digest_editions'
                AND t.tgname='trg_editorial_digest_immutable' AND NOT t.tgisinternal
            )
        """)) if table else False
        return not table or not index or not date_index or not trigger

    async def execute(self, db):
        schema = await db.scalar(text("SELECT current_schema()"))
        if not isinstance(schema, str) or not schema:
            raise RuntimeError("Could not resolve the active database schema")
        quote_ident = db.get_bind().dialect.identifier_preparer.quote
        schema_q = quote_ident(schema)
        await db.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema_q}.editorial_digest_editions (
                id UUID PRIMARY KEY,
                edition_date DATE NOT NULL,
                locale VARCHAR(16) NOT NULL CHECK (locale='ru'),
                policy_version VARCHAR(32) NOT NULL
                    CHECK (policy_version ~ '^[a-z0-9-]{{1,32}}$'),
                snapshot_hash VARCHAR(64) NOT NULL CHECK (snapshot_hash ~ '^[0-9a-f]{{64}}$'),
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
                provider_usage JSONB NOT NULL DEFAULT '{{}}'::jsonb
                    CHECK (jsonb_typeof(provider_usage)='object'),
                job_id UUID,
                attempt_generation INTEGER CHECK (attempt_generation IS NULL OR attempt_generation > 0),
                created_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
                CHECK ((job_id IS NULL) = (attempt_generation IS NULL))
            )
        """))
        await db.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_editorial_digest_job_attempt_locale
            ON {schema_q}.editorial_digest_editions(job_id, attempt_generation, locale)
        """))
        await db.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_editorial_digest_date_locale_created
            ON {schema_q}.editorial_digest_editions(edition_date, locale, created_at DESC)
        """))
        await db.execute(text(f"""
            CREATE OR REPLACE FUNCTION {schema_q}.reject_editorial_digest_mutation()
            RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog, {schema_q}
            AS $function$
            BEGIN
                RAISE EXCEPTION 'editorial_digest_editions are immutable' USING ERRCODE='55000';
            END
            $function$
        """))
        await db.execute(text(f"""
            DROP TRIGGER IF EXISTS trg_editorial_digest_immutable
            ON {schema_q}.editorial_digest_editions
        """))
        await db.execute(text(f"""
            CREATE TRIGGER trg_editorial_digest_immutable
            BEFORE UPDATE OR DELETE ON {schema_q}.editorial_digest_editions
            FOR EACH ROW EXECUTE FUNCTION {schema_q}.reject_editorial_digest_mutation()
        """))
        await db.commit()
        return {"tables_created_or_confirmed": ["editorial_digest_editions"],
                "indexes_created_or_confirmed": [
                    "uq_editorial_digest_job_attempt_locale",
                    "idx_editorial_digest_date_locale_created",
                ]}

    async def rollback(self, db):
        raise NotImplementedError("Editorial edition history is immutable and needs an explicit retention decision")
