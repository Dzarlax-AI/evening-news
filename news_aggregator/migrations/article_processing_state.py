"""Add resumable article processing without requeuing completed history."""

from sqlalchemy import text

from .base_migration import BaseMigration

REQUIRED_MIGRATION_ID = "008_article_processing_state"


def ensure_article_processing_migration(results):
    errors = [str(error) for error in results.get("errors", [])]
    failures = [error for error in errors if REQUIRED_MIGRATION_ID in error
                or error.startswith("Migration system error:")]
    if failures:
        raise RuntimeError("Required article processing migration failed: " + "; ".join(failures))


class ArticleProcessingStateMigration(BaseMigration):
    columns = {
        "processing_state": "VARCHAR(20)",
        "processing_attempts": "INTEGER NOT NULL DEFAULT 0",
        "next_attempt_at": "TIMESTAMP",
        "last_processing_error": "TEXT",
    }

    def __init__(self):
        super().__init__(REQUIRED_MIGRATION_ID,
                         "Persist bounded article processing outcomes and retries", "2.2.0")

    async def check_needed(self, db):
        result = await db.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = 'articles'
              AND column_name IN ('processing_state', 'processing_attempts',
                                   'next_attempt_at', 'last_processing_error')
        """))
        if set(result.scalars().all()) != set(self.columns):
            return True
        result = await db.execute(text("SELECT EXISTS (SELECT 1 FROM articles WHERE processing_state IS NULL)"))
        return bool(result.scalar())

    async def execute(self, db):
        try:
            for name, sql_type in self.columns.items():
                await db.execute(text(f"ALTER TABLE articles ADD COLUMN IF NOT EXISTS {name} {sql_type}"))
            # Only unclassified legacy rows are backfilled. Reruns preserve retry budgets.
            await db.execute(text("""
                UPDATE articles SET processing_state = CASE
                    WHEN processed IS TRUE AND summary_processed IS TRUE
                         AND category_processed IS TRUE AND ad_processed IS TRUE
                    THEN 'completed' ELSE 'pending' END
                WHERE processing_state IS NULL
            """))
            await db.execute(text("ALTER TABLE articles ALTER COLUMN processing_state SET DEFAULT 'pending'"))
            await db.execute(text("ALTER TABLE articles ALTER COLUMN processing_state SET NOT NULL"))
            await db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_articles_processing_due
                ON articles (processing_state, next_attempt_at, id)
                WHERE processing_state IN ('pending', 'retry_wait')
            """))
            await db.commit()
            return {"columns_added_or_confirmed": list(self.columns)}
        except Exception:
            await db.rollback()
            raise
