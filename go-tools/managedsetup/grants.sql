-- Managed News role policy. Applied after schema migrations by maintenance only.
-- Roles are pre-created without superuser, inheritance, database/role creation,
-- or bypass-RLS privileges. Runtime services never receive the database owner.
GRANT USAGE ON SCHEMA news TO news_web_runtime, news_domain_runtime,
 news_worker_runtime, news_execution_runtime, news_ingestion_runtime,
 news_processing_runtime, news_ingestion_fence_owner, news_processing_fence_owner;
REVOKE ALL ON news.integration_credentials FROM PUBLIC, news_web_runtime,
 news_worker_runtime, news_execution_runtime, news_ingestion_runtime, news_processing_runtime;
REVOKE ALL ON news.managed_admin, news.setup_tokens FROM PUBLIC, news_domain_runtime,
 news_worker_runtime, news_execution_runtime, news_ingestion_runtime, news_processing_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON news.managed_admin, news.setup_tokens TO news_web_runtime;
GRANT SELECT ON news.installation_state TO news_web_runtime;
GRANT UPDATE (onboarding_step, onboarding_completed, revision, updated_at) ON news.installation_state TO news_web_runtime;
GRANT SELECT ON news.installation_state TO news_domain_runtime;
GRANT SELECT, INSERT, UPDATE ON news.integration_credentials TO news_domain_runtime;

-- Public readers and explicit administrator management operations.
GRANT SELECT ON news.articles, news.sources, news.categories, news.article_categories,
 news.category_mapping, news.daily_summaries, news.editorial_digest_editions,
 news.ai_usage_tracking, news.extraction_attempts, news.processing_stats,
 news.background_jobs, news.background_worker_status,
 news.scheduler_occurrence_receipts TO news_web_runtime;
GRANT INSERT, UPDATE, DELETE ON news.sources TO news_web_runtime;
GRANT INSERT, UPDATE ON news.categories TO news_web_runtime;
GRANT INSERT, UPDATE, DELETE ON news.category_mapping TO news_web_runtime;
GRANT DELETE ON news.articles TO news_web_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON news.settings TO news_web_runtime;
GRANT INSERT ON news.background_jobs, news.background_job_idempotency_keys,
 news.scheduler_occurrence_receipts TO news_web_runtime;
GRANT SELECT ON news.background_job_idempotency_keys TO news_web_runtime;
-- Durable acceptance locks queue and alias rows before checking duplicates.
-- PostgreSQL requires UPDATE on a column even when the lock changes no data.
GRANT UPDATE (id) ON news.background_jobs TO news_web_runtime;
GRANT UPDATE (key) ON news.background_job_idempotency_keys TO news_web_runtime;
GRANT UPDATE (occurrence_id) ON news.scheduler_occurrence_receipts TO news_web_runtime;
GRANT USAGE, SELECT ON SEQUENCE news.sources_id_seq, news.categories_id_seq,
 news.category_mapping_id_seq, news.background_jobs_queue_position_seq
 TO news_web_runtime;

-- The private domain owns AI, editorial, learning, delivery, and operational
-- settings; it cannot take over the worker or manufacture execution receipts.
GRANT SELECT ON news.articles, news.article_categories, news.categories,
 news.category_mapping, news.sources, news.article_ingestion_runs,
 news.article_ingestion_source_runs, news.article_processing_runs,
 news.article_processing_items, news.background_jobs, news.background_worker_status,
 news.background_job_executions, news.daily_summaries, news.domain_stability,
 news.schedule_settings, news.scheduler_occurrence_receipts, news.background_job_idempotency_keys,
 news.source_collection_state TO news_domain_runtime;
GRANT UPDATE (id, media_files, image_url) ON news.articles TO news_domain_runtime;
GRANT UPDATE (id) ON news.background_worker_status, news.background_jobs TO news_domain_runtime;
GRANT UPDATE (article_id) ON news.article_processing_items TO news_domain_runtime;
GRANT UPDATE (job_id, legacy_alert_state) ON news.background_job_executions TO news_domain_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON news.settings TO news_domain_runtime;
GRANT SELECT, INSERT, UPDATE ON news.extraction_patterns, news.processing_stats,
 news.digest_delivery_intents, news.digest_delivery_parts TO news_domain_runtime;
GRANT SELECT, INSERT ON news.extraction_attempts, news.ai_usage_tracking,
 news.editorial_digest_editions TO news_domain_runtime;
GRANT USAGE, SELECT ON SEQUENCE news.extraction_patterns_id_seq,
 news.extraction_attempts_id_seq, news.ai_usage_tracking_id_seq, news.processing_stats_id_seq
 TO news_domain_runtime;

-- Queue ownership belongs to the worker; the execution service locks the same
-- lease row but may update only its harmless identifier for FOR UPDATE checks.
GRANT SELECT, UPDATE ON news.background_worker_status TO news_worker_runtime;
GRANT SELECT, UPDATE, DELETE ON news.background_jobs TO news_worker_runtime;
GRANT SELECT, INSERT ON news.background_job_executions TO news_worker_runtime;
GRANT SELECT ON news.background_worker_status, news.background_jobs,
 news.schedule_settings, news.articles TO news_execution_runtime;
GRANT UPDATE (id) ON news.background_worker_status TO news_execution_runtime;
GRANT UPDATE (id) ON news.background_jobs TO news_execution_runtime;
GRANT SELECT, INSERT, UPDATE ON news.background_job_executions TO news_execution_runtime;
GRANT UPDATE (is_running, last_run, last_finished_at, last_success_at, last_status,
 last_error, last_duration_seconds, next_run) ON news.schedule_settings TO news_execution_runtime;
GRANT UPDATE (processing_state, processing_attempts, next_attempt_at, last_processing_error, processed, summary_processed, category_processed, ad_processed) ON news.articles TO news_execution_runtime;

-- Existing audited news-ingestion policy, with pre-created roles.
GRANT USAGE ON SCHEMA news TO news_ingestion_runtime, news_ingestion_fence_owner;

-- The fence owner can inspect and lock queue ownership, attempt, and receipt
-- rows only while executing the SECURITY DEFINER fence function. UPDATE on a
-- harmless key column is needed by PostgreSQL for SELECT ... FOR UPDATE; the
-- function itself performs no queue, owner, or execution-receipt updates.
GRANT SELECT ON news.background_worker_status,
                news.background_jobs,
                news.background_job_executions
TO news_ingestion_fence_owner;
GRANT UPDATE (id) ON news.background_worker_status TO news_ingestion_fence_owner;
GRANT UPDATE (id) ON news.background_jobs TO news_ingestion_fence_owner;
GRANT UPDATE (job_id) ON news.background_job_executions TO news_ingestion_fence_owner;

GRANT SELECT, INSERT ON news.article_ingestion_runs TO news_ingestion_fence_owner;
GRANT UPDATE (id) ON news.article_ingestion_runs TO news_ingestion_fence_owner;

-- The function has a fixed search_path in migration 012. Transfer it to the
-- non-login owner so the ingestion login cannot use the function's authority.
GRANT CREATE ON SCHEMA news TO news_ingestion_fence_owner;
ALTER FUNCTION news.fence_article_ingestion_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) OWNER TO news_ingestion_fence_owner;
REVOKE CREATE ON SCHEMA news FROM news_ingestion_fence_owner;
REVOKE ALL ON FUNCTION news.fence_article_ingestion_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION news.fence_article_ingestion_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) TO news_ingestion_runtime;

-- Runtime reads enabled source configuration and updates its fetch timestamp.
GRANT SELECT (id, name, source_type, url, enabled, config, last_fetch)
ON news.sources TO news_ingestion_runtime;
GRANT UPDATE (last_fetch) ON news.sources TO news_ingestion_runtime;

-- The service reads bounded run/source receipts, writes its progress, and
-- updates terminal run state and result fields.
GRANT SELECT ON news.article_ingestion_runs TO news_ingestion_runtime;
GRANT UPDATE (state, result, error_code, updated_at, finished_at)
ON news.article_ingestion_runs TO news_ingestion_runtime;
GRANT SELECT, INSERT, UPDATE ON news.article_ingestion_source_runs TO news_ingestion_runtime;
GRANT SELECT, INSERT, UPDATE ON news.source_collection_state TO news_ingestion_runtime;

GRANT SELECT (id, source_id, title, url, content, summary, image_url,
              published_at, fetched_at, processed, hash_content)
ON news.articles TO news_ingestion_runtime;
GRANT INSERT (source_id, title, url, content, summary, image_url, media_files,
              published_at, fetched_at, processed, summary_processed,
              category_processed, processing_state, processing_attempts,
              next_attempt_at, last_processing_error, hash_content,
              is_advertisement, ad_confidence, ad_type, ad_reasoning,
              ad_markers, ad_processed)
ON news.articles TO news_ingestion_runtime;
GRANT UPDATE (title, content, url, image_url, published_at, hash_content)
ON news.articles TO news_ingestion_runtime;
GRANT USAGE, SELECT ON SEQUENCE news.articles_id_seq TO news_ingestion_runtime;

-- Existing audited news-processing policy, with pre-created roles.
GRANT USAGE ON SCHEMA news TO news_processing_runtime, news_processing_fence_owner;
GRANT SELECT ON news.background_worker_status,
                news.background_jobs,
                news.background_job_executions
TO news_processing_fence_owner;
GRANT UPDATE (id) ON news.background_worker_status TO news_processing_fence_owner;
GRANT UPDATE (id) ON news.background_jobs TO news_processing_fence_owner;
GRANT UPDATE (job_id) ON news.background_job_executions TO news_processing_fence_owner;
GRANT SELECT, INSERT ON news.article_processing_runs TO news_processing_fence_owner;
GRANT UPDATE (id) ON news.article_processing_runs TO news_processing_fence_owner;
GRANT CREATE ON SCHEMA news TO news_processing_fence_owner;
ALTER FUNCTION news.fence_article_processing_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) OWNER TO news_processing_fence_owner;
REVOKE CREATE ON SCHEMA news FROM news_processing_fence_owner;
REVOKE ALL ON FUNCTION news.fence_article_processing_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION news.fence_article_processing_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) TO news_processing_runtime;
GRANT EXECUTE ON FUNCTION news.fence_article_processing_attempt(
    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP
) TO news_domain_runtime;

GRANT SELECT (id, kind, payload, state, attempt_generation)
ON news.background_jobs TO news_processing_runtime;

GRANT SELECT ON news.article_processing_runs,
                news.article_processing_items
TO news_processing_runtime;
GRANT UPDATE (state, result, error_code, updated_at, finished_at)
ON news.article_processing_runs TO news_processing_runtime;
GRANT INSERT, UPDATE ON news.article_processing_items TO news_processing_runtime;
GRANT SELECT (id, title, url, content, summary, source_id,
              published_at, summary_processed, category_processed, ad_processed,
              is_advertisement, processing_attempts, processing_state, next_attempt_at,
              last_processing_error, fetched_at, image_url, media_files, hash_content, ad_confidence, ad_type,
              ad_reasoning, processed)
ON news.articles TO news_processing_runtime;
GRANT UPDATE (title, content, summary, summary_processed, category_processed, ad_processed,
              is_advertisement, processing_attempts, processing_state, next_attempt_at,
              last_processing_error, image_url, media_files, ad_confidence, ad_type,
              ad_reasoning, published_at, processed)
ON news.articles TO news_processing_runtime;
GRANT SELECT ON news.sources, news.categories, news.category_mapping
TO news_processing_runtime;
GRANT UPDATE (usage_count, last_used) ON news.category_mapping TO news_processing_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON news.article_categories TO news_processing_runtime;
GRANT USAGE, SELECT ON SEQUENCE news.article_categories_id_seq TO news_processing_runtime;

-- Read sequence state for the managed application's database-only backup;
-- USAGE remains limited to sequences the domain actually inserts into.
GRANT SELECT ON SEQUENCE news.sources_id_seq, news.categories_id_seq,
 news.category_mapping_id_seq, news.daily_summaries_id_seq, news.domain_stability_id_seq,
 news.schedule_settings_id_seq, news.articles_id_seq, news.article_categories_id_seq,
 news.background_jobs_queue_position_seq TO news_domain_runtime;

-- Locale data uses the same ownership boundary as articles and categories.
GRANT SELECT ON news.article_localizations, news.category_localizations TO news_web_runtime, news_domain_runtime;
GRANT SELECT, INSERT, UPDATE ON news.article_localizations TO news_processing_runtime;
GRANT SELECT ON news.category_localizations TO news_processing_runtime;
GRANT INSERT, UPDATE, DELETE ON news.category_localizations TO news_web_runtime;

GRANT SELECT, INSERT, UPDATE ON news.editorial_locale_inputs TO news_domain_runtime;
GRANT SELECT, INSERT ON news.editorial_serbian_retry TO news_domain_runtime;
GRANT UPDATE (publication_pinned, publication_attempted, delivery_enabled, destination) ON news.editorial_serbian_retry TO news_domain_runtime;
GRANT EXECUTE ON FUNCTION news.enqueue_serbian_digest_retry() TO news_worker_runtime;

-- Domain owns Viber receipts; only the worker can invoke admission.
GRANT SELECT, INSERT, UPDATE ON news.viber_publication_requests, news.viber_test_receipts, news.viber_digest_deliveries, news.viber_digest_parts, news.viber_channel_leases TO news_domain_runtime;
GRANT EXECUTE ON FUNCTION news.enqueue_viber_digest_delivery() TO news_worker_runtime;
