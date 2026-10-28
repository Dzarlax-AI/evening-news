"""Add durable delivery intent and per-part receipts without retaining message text."""

from typing import Any, Dict

from sqlalchemy import text

from .base_migration import BaseMigration

REQUIRED_MIGRATION_ID = "015_digest_delivery"


def ensure_digest_delivery_migration(results: Dict[str, Any]) -> None:
    errors = [str(error) for error in results.get("errors", [])]
    failures = [error for error in errors if REQUIRED_MIGRATION_ID in error
                or error.startswith("Migration system error:")]
    if failures:
        raise RuntimeError("Required digest delivery migration failed: " + "; ".join(failures))


class DigestDeliveryMigration(BaseMigration):
    def __init__(self):
        super().__init__(REQUIRED_MIGRATION_ID, "Persist digest delivery receipts", "1.0.0")

    async def check_needed(self, db):
        return not await db.scalar(text("""
            SELECT to_regclass(current_schema() || '.digest_delivery_intents') IS NOT NULL
               AND to_regclass(current_schema() || '.digest_delivery_parts') IS NOT NULL
        """))

    async def execute(self, db):
        schema = await db.scalar(text("SELECT current_schema()"))
        if not isinstance(schema, str) or not schema:
            raise RuntimeError("Could not resolve the active database schema")
        schema_q = db.get_bind().dialect.identifier_preparer.quote(schema)
        await db.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema_q}.digest_delivery_intents (
                id UUID PRIMARY KEY,
                edition_id UUID NOT NULL REFERENCES {schema_q}.editorial_digest_editions(id),
                destination_hash CHAR(64) NOT NULL CHECK (destination_hash ~ '^[0-9a-f]{{64}}$'),
                purpose VARCHAR(32) NOT NULL CHECK (purpose = 'ru_digest'),
                render_hash CHAR(64) NOT NULL CHECK (render_hash ~ '^[0-9a-f]{{64}}$'),
                job_id UUID NOT NULL,
                attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
                domain_instance_id UUID NOT NULL,
                state VARCHAR(16) NOT NULL CHECK (state IN ('active','completed')),
                parts_sent INTEGER CHECK (parts_sent > 0),
                rich_message BOOLEAN,
                CHECK ((state = 'active' AND parts_sent IS NULL AND rich_message IS NULL)
                    OR (state = 'completed' AND parts_sent IS NOT NULL AND rich_message IS NOT NULL)),
                created_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
                UNIQUE (edition_id, destination_hash, purpose)
            )
        """))
        await db.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema_q}.digest_delivery_parts (
                intent_id UUID NOT NULL REFERENCES {schema_q}.digest_delivery_intents(id),
                method VARCHAR(16) NOT NULL CHECK (method IN ('sendMessage', 'sendRichMessage')),
                part_index INTEGER NOT NULL CHECK (part_index >= 0),
                payload_hash CHAR(64) NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{{64}}$'),
                job_id UUID NOT NULL,
                attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
                domain_instance_id UUID NOT NULL,
                state VARCHAR(16) NOT NULL CHECK (state IN ('sending', 'sent', 'rejected', 'unknown')),
                message_id BIGINT,
                status_code INTEGER CHECK (status_code BETWEEN 100 AND 599),
                retry_after_seconds DOUBLE PRECISION CHECK (retry_after_seconds >= 0),
                created_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
                updated_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
                PRIMARY KEY (intent_id, method, part_index),
                CHECK (state = 'sent' OR message_id IS NULL)
            )
        """))
        await db.commit()
        return {"tables_created_or_confirmed": ["digest_delivery_intents", "digest_delivery_parts"]}

    async def rollback(self, db):
        raise NotImplementedError("Delivery receipts protect against duplicate sends and cannot be discarded")
