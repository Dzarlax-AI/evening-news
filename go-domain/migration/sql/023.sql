ALTER TABLE __SCHEMA_Q__.articles
    ADD COLUMN telegram_context JSONB,
    ADD CONSTRAINT ck_articles_telegram_context_object
        CHECK (telegram_context IS NULL OR jsonb_typeof(telegram_context) = 'object');

DO $telegram_context_grants$
DECLARE
    schema_name TEXT := current_schema();
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'news_ingestion_runtime') THEN
        EXECUTE format('GRANT SELECT (telegram_context) ON %I.articles TO news_ingestion_runtime', schema_name);
        EXECUTE format('GRANT INSERT (telegram_context) ON %I.articles TO news_ingestion_runtime', schema_name);
        EXECUTE format('GRANT UPDATE (telegram_context) ON %I.articles TO news_ingestion_runtime', schema_name);
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'news_processing_runtime') THEN
        EXECUTE format('GRANT SELECT (telegram_context) ON %I.articles TO news_processing_runtime', schema_name);
    END IF;
END
$telegram_context_grants$;
