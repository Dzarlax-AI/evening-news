-- Operator-run once after migration 012_article_ingestion has succeeded.
-- Connect as the News database administrator to the News database. The
-- default schema is "news"; pass -v news_schema=... to select another schema.
-- This file intentionally contains no passwords or automatic app startup hook.
-- Review the target database, schema, role names, and grants before execution.
\set ON_ERROR_STOP on
\if :{?news_schema}
\else
\set news_schema news
\endif

CREATE ROLE news_ingestion_fence_owner NOLOGIN NOINHERIT NOBYPASSRLS
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE news_ingestion_runtime LOGIN NOINHERIT NOBYPASSRLS
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION CONNECTION LIMIT 5;
\password news_ingestion_runtime

GRANT USAGE ON SCHEMA :"news_schema" TO news_ingestion_runtime, news_ingestion_fence_owner;

-- The fence owner can inspect and lock queue ownership, attempt, and receipt
-- rows only while executing the SECURITY DEFINER fence function. UPDATE on a
-- harmless key column is needed by PostgreSQL for SELECT ... FOR UPDATE; the
-- function itself performs no queue, owner, or execution-receipt updates.
GRANT SELECT ON :"news_schema".background_worker_status,
                :"news_schema".background_jobs,
                :"news_schema".background_job_executions
TO news_ingestion_fence_owner;
GRANT UPDATE (id) ON :"news_schema".background_worker_status TO news_ingestion_fence_owner;
GRANT UPDATE (id) ON :"news_schema".background_jobs TO news_ingestion_fence_owner;
GRANT UPDATE (job_id) ON :"news_schema".background_job_executions TO news_ingestion_fence_owner;

GRANT SELECT, INSERT ON :"news_schema".article_ingestion_runs TO news_ingestion_fence_owner;
GRANT UPDATE (id) ON :"news_schema".article_ingestion_runs TO news_ingestion_fence_owner;

-- The function has a fixed search_path in migration 012. Transfer it to the
-- non-login owner so the ingestion login cannot use the function's authority.
GRANT CREATE ON SCHEMA :"news_schema" TO news_ingestion_fence_owner;
ALTER FUNCTION :"news_schema".fence_article_ingestion_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) OWNER TO news_ingestion_fence_owner;
REVOKE CREATE ON SCHEMA :"news_schema" FROM news_ingestion_fence_owner;
REVOKE ALL ON FUNCTION :"news_schema".fence_article_ingestion_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION :"news_schema".fence_article_ingestion_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) TO news_ingestion_runtime;

-- Runtime reads enabled source configuration and updates its fetch timestamp.
GRANT SELECT (id, name, source_type, url, enabled, config, last_fetch)
ON :"news_schema".sources TO news_ingestion_runtime;
GRANT UPDATE (last_fetch) ON :"news_schema".sources TO news_ingestion_runtime;

-- The service reads bounded run/source receipts, writes its progress, and
-- updates terminal run state and result fields.
GRANT SELECT ON :"news_schema".article_ingestion_runs TO news_ingestion_runtime;
GRANT UPDATE (state, result, error_code, updated_at, finished_at)
ON :"news_schema".article_ingestion_runs TO news_ingestion_runtime;
GRANT SELECT, INSERT, UPDATE ON :"news_schema".article_ingestion_source_runs TO news_ingestion_runtime;
GRANT SELECT, INSERT, UPDATE ON :"news_schema".source_collection_state TO news_ingestion_runtime;

GRANT SELECT (id, source_id, title, url, content, summary, image_url,
              published_at, fetched_at, processed)
ON :"news_schema".articles TO news_ingestion_runtime;
GRANT INSERT (source_id, title, url, content, summary, image_url, media_files,
              published_at, fetched_at, processed, summary_processed,
              category_processed, processing_state, processing_attempts,
              next_attempt_at, last_processing_error, hash_content,
              is_advertisement, ad_confidence, ad_type, ad_reasoning,
              ad_markers, ad_processed)
ON :"news_schema".articles TO news_ingestion_runtime;
GRANT UPDATE (title, content, url, image_url, published_at, hash_content)
ON :"news_schema".articles TO news_ingestion_runtime;
GRANT USAGE, SELECT ON SEQUENCE :"news_schema".articles_id_seq TO news_ingestion_runtime;
