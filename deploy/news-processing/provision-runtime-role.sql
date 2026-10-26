-- Operator-run after migration 013_article_processing succeeds.
-- Connect as the News database administrator. No password is stored here.
\set ON_ERROR_STOP on
\if :{?news_schema}
\else
\set news_schema news
\endif
\if :{?news_callback_role}
\else
\echo 'Required psql variable news_callback_role is missing'
SELECT 1 / 0;
\quit
\endif

-- Validate the callback role before creating or altering any roles.
SELECT 1 / CASE
    WHEN :'news_callback_role' <> ''
     AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'news_callback_role')
    THEN 1 ELSE 0
END;

CREATE ROLE news_processing_fence_owner NOLOGIN NOINHERIT NOBYPASSRLS
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE ROLE news_processing_runtime LOGIN NOINHERIT NOBYPASSRLS
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION CONNECTION LIMIT 6;
\password news_processing_runtime

GRANT USAGE ON SCHEMA :"news_schema" TO news_processing_runtime, news_processing_fence_owner;
GRANT SELECT ON :"news_schema".background_worker_status,
                :"news_schema".background_jobs,
                :"news_schema".background_job_executions
TO news_processing_fence_owner;
GRANT UPDATE (id) ON :"news_schema".background_worker_status TO news_processing_fence_owner;
GRANT UPDATE (id) ON :"news_schema".background_jobs TO news_processing_fence_owner;
GRANT UPDATE (job_id) ON :"news_schema".background_job_executions TO news_processing_fence_owner;
GRANT SELECT, INSERT ON :"news_schema".article_processing_runs TO news_processing_fence_owner;
GRANT UPDATE (id) ON :"news_schema".article_processing_runs TO news_processing_fence_owner;
GRANT CREATE ON SCHEMA :"news_schema" TO news_processing_fence_owner;
ALTER FUNCTION :"news_schema".fence_article_processing_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) OWNER TO news_processing_fence_owner;
REVOKE CREATE ON SCHEMA :"news_schema" FROM news_processing_fence_owner;
REVOKE ALL ON FUNCTION :"news_schema".fence_article_processing_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION :"news_schema".fence_article_processing_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) TO news_processing_runtime;
GRANT EXECUTE ON FUNCTION :"news_schema".fence_article_processing_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) TO :"news_callback_role";

GRANT SELECT (id, kind, payload, state, attempt_generation)
ON :"news_schema".background_jobs TO news_processing_runtime;

GRANT SELECT ON :"news_schema".article_processing_runs,
                :"news_schema".article_processing_items
TO news_processing_runtime;
GRANT UPDATE (state, result, error_code, updated_at, finished_at)
ON :"news_schema".article_processing_runs TO news_processing_runtime;
GRANT INSERT, UPDATE ON :"news_schema".article_processing_items TO news_processing_runtime;
GRANT SELECT (id, title, url, content, summary, source_id,
              published_at, summary_processed, category_processed, ad_processed,
              is_advertisement, processing_attempts, processing_state, next_attempt_at,
              last_processing_error, fetched_at, image_url, media_files, hash_content, ad_confidence, ad_type,
              ad_reasoning, processed)
ON :"news_schema".articles TO news_processing_runtime;
GRANT UPDATE (title, content, summary, summary_processed, category_processed, ad_processed,
              is_advertisement, processing_attempts, processing_state, next_attempt_at,
              last_processing_error, image_url, media_files, ad_confidence, ad_type,
              ad_reasoning, published_at, processed)
ON :"news_schema".articles TO news_processing_runtime;
GRANT SELECT ON :"news_schema".sources, :"news_schema".categories, :"news_schema".category_mapping
TO news_processing_runtime;
GRANT UPDATE (usage_count, last_used) ON :"news_schema".category_mapping TO news_processing_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON :"news_schema".article_categories TO news_processing_runtime;
GRANT USAGE, SELECT ON SEQUENCE :"news_schema".article_categories_id_seq TO news_processing_runtime;
