-- Run only against a NEW, migrated news_restore_* database, with no runtimes.
-- settings.json must be authenticated, validated and <=16 MiB in the private cwd.
-- psql -Xq -v target_database=... -v installation_id=... -f import-settings.sql
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL search_path = pg_catalog, news;
SET LOCAL statement_timeout = '30s';
CREATE TEMP TABLE recovery_guard AS SELECT :'target_database'::text AS db, :'installation_id'::text AS installation;
CREATE TEMP TABLE recovery_input(payload jsonb NOT NULL);
CREATE TEMP TABLE recovery_runtime(payload jsonb NOT NULL);
\copy recovery_input FROM 'settings.json' WITH (FORMAT csv, DELIMITER E'\x1f', QUOTE E'\x02', ESCAPE E'\x02')
\copy recovery_runtime FROM 'background-input.json' WITH (FORMAT csv, DELIMITER E'\x1f', QUOTE E'\x02', ESCAPE E'\x02')
DO $guard$
DECLARE d jsonb; k text; occupied boolean; viber jsonb; viber_count integer;
BEGIN
 IF current_database() !~ '^news_restore_[a-z0-9_]+$' OR current_database() <> (SELECT db FROM recovery_guard) THEN RAISE EXCEPTION 'recovery target rejected'; END IF;
 IF (SELECT count(*) FROM recovery_runtime)<>1 OR (SELECT payload->>'service' FROM recovery_runtime) IS DISTINCT FROM 'background' OR (SELECT payload->>'version' FROM recovery_runtime) IS DISTINCT FROM '1' OR (SELECT payload->>'installation_id' FROM recovery_runtime) IS DISTINCT FROM (SELECT installation FROM recovery_guard) THEN RAISE EXCEPTION 'recovery runtime manifest rejected'; END IF;
 IF (SELECT count(*) FROM recovery_input) <> 1 THEN RAISE EXCEPTION 'recovery document rejected'; END IF;
 SELECT payload INTO d FROM recovery_input;
 IF d->>'format' IS DISTINCT FROM 'news-settings-v1' OR COALESCE(d->>'schema_version','') NOT IN ('16','18','20') OR d#>>'{identity,installation_id}' IS DISTINCT FROM (SELECT installation FROM recovery_guard) OR (d#>>'{identity,installation_id}') !~ '^[a-f0-9]{32}$' THEN RAISE EXCEPTION 'recovery identity/version rejected'; END IF;
 IF EXISTS(SELECT 1 FROM news.installation_state) OR EXISTS(SELECT 1 FROM news.sources) OR EXISTS(SELECT 1 FROM news.articles) OR EXISTS(SELECT 1 FROM news.background_jobs) OR EXISTS(SELECT 1 FROM news.editorial_digest_editions) OR EXISTS(SELECT 1 FROM news.integration_credentials) OR EXISTS(SELECT 1 FROM news.managed_admin) OR EXISTS(SELECT 1 FROM news.setup_tokens) OR EXISTS(SELECT 1 FROM news.source_collection_state) THEN RAISE EXCEPTION 'recovery requires empty target'; END IF;
 -- All non-configuration histories must also be empty, even if their parent
 -- articles/jobs have already been removed from an old target.
 FOREACH k IN ARRAY ARRAY['daily_summaries','processing_stats','extraction_attempts','ai_usage_tracking','domain_stability','background_job_idempotency_keys','background_job_executions','scheduler_occurrence_receipts','article_ingestion_runs','article_ingestion_source_runs','article_processing_runs','article_processing_items','digest_delivery_intents','digest_delivery_parts','viber_digest_deliveries','viber_digest_parts','viber_channel_leases','viber_publication_requests','viber_test_receipts'] LOOP
  EXECUTE format('SELECT EXISTS(SELECT 1 FROM news.%I)',k) INTO occupied;
  IF occupied THEN RAISE EXCEPTION 'recovery requires empty history'; END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()) THEN RAISE EXCEPTION 'recovery target has connected clients'; END IF;
 FOREACH k IN ARRAY ARRAY['sources','categories','category_mapping','extraction_patterns','schedule_settings','settings','managed_admin','integration_credentials'] LOOP IF jsonb_typeof(d->k) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'recovery section rejected'; END IF; END LOOP;
 IF EXISTS(SELECT 1 FROM jsonb_object_keys(d) AS sections(section_name) WHERE section_name NOT IN ('format','schema_version','identity','sources','categories','category_localizations','category_mapping','extraction_patterns','schedule_settings','settings','managed_admin','integration_credentials')) THEN RAISE EXCEPTION 'unknown recovery section'; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(d->'settings') x WHERE x->>'key' NOT IN ('ai_settings','ai_settings_revision','ai_model_prices','telegram_chat_id','telegram_service_chat_id','telegram_settings_revision','telegram_serbian_chat_id','telegram_serbian_delivery_enabled','viber_settings','operational_settings_revision','operational_limits_enabled','operational_max_articles','operational_per_source','operational_days','operational_newest_date','operational_processing_enabled','operational_delivery_enabled','operational_serbian_enabled')) THEN RAISE EXCEPTION 'unknown setting'; END IF;
 SELECT count(*) INTO viber_count FROM jsonb_array_elements(d->'settings') x WHERE x->>'key'='viber_settings';
 IF viber_count > 1 THEN RAISE EXCEPTION 'Viber settings rejected'; END IF;
 IF viber_count = 1 THEN
  SELECT x->'value' INTO viber FROM jsonb_array_elements(d->'settings') x WHERE x->>'key'='viber_settings';
  IF jsonb_typeof(viber) IS DISTINCT FROM 'object'
   OR (SELECT count(*) FROM jsonb_object_keys(CASE WHEN jsonb_typeof(viber)='object' THEN viber ELSE '{}'::jsonb END)) <> 10
   OR EXISTS(SELECT 1 FROM jsonb_object_keys(CASE WHEN jsonb_typeof(viber)='object' THEN viber ELSE '{}'::jsonb END) AS keys(key) WHERE key NOT IN ('revision','delivery_enabled','channel_id','channel_name','sender_id','credential_revision','verified_credential_revision','webhook_ready','verified_at','enabled_from'))
   OR jsonb_typeof(viber->'revision') IS DISTINCT FROM 'number'
   OR jsonb_typeof(viber->'delivery_enabled') IS DISTINCT FROM 'boolean'
   OR jsonb_typeof(viber->'channel_id') IS DISTINCT FROM 'string'
   OR jsonb_typeof(viber->'channel_name') IS DISTINCT FROM 'string'
   OR jsonb_typeof(viber->'sender_id') IS DISTINCT FROM 'string'
   OR jsonb_typeof(viber->'credential_revision') IS DISTINCT FROM 'number'
   OR jsonb_typeof(viber->'verified_credential_revision') IS DISTINCT FROM 'number'
   OR jsonb_typeof(viber->'webhook_ready') IS DISTINCT FROM 'boolean'
   OR jsonb_typeof(viber->'verified_at') NOT IN ('string','null')
   OR jsonb_typeof(viber->'enabled_from') NOT IN ('string','null') THEN
   RAISE EXCEPTION 'Viber settings rejected';
  END IF;
  IF (viber->>'revision')::numeric < 0 OR (viber->>'revision')::numeric >= 9223372036854775807 OR trunc((viber->>'revision')::numeric) <> (viber->>'revision')::numeric
   OR (viber->>'credential_revision')::numeric < 0 OR (viber->>'credential_revision')::numeric > 9223372036854775807 OR trunc((viber->>'credential_revision')::numeric) <> (viber->>'credential_revision')::numeric
   OR (viber->>'verified_credential_revision')::numeric < 0 OR (viber->>'verified_credential_revision')::numeric > 9223372036854775807 OR trunc((viber->>'verified_credential_revision')::numeric) <> (viber->>'verified_credential_revision')::numeric
   OR octet_length(viber->>'channel_id') > 128 OR (viber->>'channel_id') ~ '[[:space:][:cntrl:]]'
   OR octet_length(viber->>'channel_name') > 256 OR (viber->>'channel_name') ~ '^[[:space:][:cntrl:]]|[[:space:][:cntrl:]]$'
   OR octet_length(viber->>'sender_id') > 128 OR (viber->>'sender_id') ~ '[[:space:][:cntrl:]]' THEN
   RAISE EXCEPTION 'Viber settings rejected';
  END IF;
  IF (viber->>'verified_at' IS NOT NULL AND (viber->>'verified_at') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$')
   OR (viber->>'enabled_from' IS NOT NULL AND (viber->>'enabled_from') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$') THEN
   RAISE EXCEPTION 'Viber settings rejected';
  END IF;
  BEGIN
   IF viber->>'verified_at' IS NOT NULL THEN PERFORM (viber->>'verified_at')::timestamptz; END IF;
   IF viber->>'enabled_from' IS NOT NULL THEN PERFORM (viber->>'enabled_from')::timestamptz; END IF;
  EXCEPTION WHEN OTHERS THEN RAISE EXCEPTION 'Viber settings rejected';
  END;
 END IF;
 IF d ? 'category_localizations' AND jsonb_typeof(d->'category_localizations') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'localized categories rejected'; END IF;
 IF jsonb_array_length(d->'managed_admin')<>1 OR EXISTS(SELECT 1 FROM jsonb_array_elements(d->'sources') x WHERE x->>'source_type' NOT IN ('rss','telegram','custom')) THEN RAISE EXCEPTION 'recovery configuration rejected'; END IF;
END $guard$;
-- These configuration rows are bootstrap seeds in a newly migrated target.
-- Never use TRUNCATE CASCADE or touch another schema.
DELETE FROM news.category_mapping;
DELETE FROM news.categories;
DELETE FROM news.extraction_patterns;
DELETE FROM news.schedule_settings;
DELETE FROM news.settings;
INSERT INTO news.sources(id,name,source_type,url,enabled,fetch_interval,config) SELECT id,name,source_type,url,enabled,fetch_interval,config FROM jsonb_populate_recordset(NULL::news.sources,(SELECT payload->'sources' FROM recovery_input));
INSERT INTO news.categories(id,name,display_name,description,color,is_local) SELECT id,name,display_name,description,color,is_local FROM jsonb_populate_recordset(NULL::news.categories,(SELECT payload->'categories' FROM recovery_input));
INSERT INTO news.category_localizations(category_id,locale,display_name) SELECT category_id,locale,display_name FROM jsonb_populate_recordset(NULL::news.category_localizations,(SELECT COALESCE(payload->'category_localizations','[]'::jsonb) FROM recovery_input));
INSERT INTO news.category_mapping(id,ai_category,fixed_category,confidence_threshold,description,created_by,is_active) SELECT id,ai_category,fixed_category,confidence_threshold,description,created_by,is_active FROM jsonb_populate_recordset(NULL::news.category_mapping,(SELECT payload->'category_mapping' FROM recovery_input));
INSERT INTO news.extraction_patterns(id,domain,selector_pattern,extraction_strategy,discovered_by,is_stable,success_count,failure_count) SELECT id,domain,selector_pattern,extraction_strategy,discovered_by,is_stable,success_count,failure_count FROM jsonb_populate_recordset(NULL::news.extraction_patterns,(SELECT payload->'extraction_patterns' FROM recovery_input));
INSERT INTO news.schedule_settings(id,task_name,enabled,schedule_type,hour,minute,weekdays,timezone,task_config) SELECT id,task_name,enabled,schedule_type,hour,minute,weekdays,timezone,task_config FROM jsonb_populate_recordset(NULL::news.schedule_settings,(SELECT payload->'schedule_settings' FROM recovery_input));
INSERT INTO news.settings(key,value) SELECT key,value FROM jsonb_populate_recordset(NULL::news.settings,(SELECT payload->'settings' FROM recovery_input));
INSERT INTO news.managed_admin(id,username,password_hash,revision) SELECT id,username,password_hash,revision FROM jsonb_populate_recordset(NULL::news.managed_admin,(SELECT payload->'managed_admin' FROM recovery_input));
INSERT INTO news.integration_credentials(provider,revision,envelope_version,key_id,nonce,ciphertext) SELECT provider,revision,envelope_version,key_id,nonce,ciphertext FROM jsonb_populate_recordset(NULL::news.integration_credentials,(SELECT payload->'integration_credentials' FROM recovery_input));
-- A restored channel needs a fresh verify/connect operation before delivery.
UPDATE news.settings SET value=(value::jsonb || jsonb_build_object(
 'revision',(value->>'revision')::bigint+1,
 'delivery_enabled',false,
 'verified_credential_revision',0,
 'webhook_ready',false,
 'verified_at',NULL,
 'enabled_from',NULL
))::json WHERE key='viber_settings';
INSERT INTO news.installation_state(id,installation_id,revision,provisioning_complete,maintenance_mode,onboarding_step,onboarding_completed)
 SELECT 1,payload#>>'{identity,installation_id}',(payload#>>'{identity,revision}')::bigint,true,true,'finish',true FROM recovery_input;
-- Match appsettings defaults and adoption's legacy role-manifest precedence.
-- Retain saved fields, fill missing fields from the same checkpoint's background
-- manifest, then validate the complete typed set before publishing a revision.
INSERT INTO news.settings(key,value)
SELECT key,value FROM recovery_runtime CROSS JOIN LATERAL (VALUES
 ('operational_limits_enabled',to_jsonb(COALESCE(NULLIF(trim(payload#>>'{values,NEWS_LIMIT_ENABLED}'),''),'false')::boolean)),
 ('operational_max_articles',to_jsonb(COALESCE(NULLIF(trim(payload#>>'{values,NEWS_LIMIT_MAX_ARTICLES}'),''),'50')::integer)),
 ('operational_per_source',to_jsonb(COALESCE(NULLIF(trim(payload#>>'{values,NEWS_LIMIT_PER_SOURCE}'),''),'50')::integer)),
 ('operational_days',to_jsonb(COALESCE(NULLIF(trim(payload#>>'{values,NEWS_LIMIT_DAYS}'),''),'1')::integer)),
 ('operational_newest_date',to_jsonb(COALESCE(trim(payload#>>'{values,NEWS_LIMIT_NEWEST_DATE}'),''))),
 ('operational_processing_enabled','true'::jsonb),
 ('operational_delivery_enabled','false'::jsonb),
 ('operational_serbian_enabled','false'::jsonb)
) AS effective(key,value) ON CONFLICT(key) DO NOTHING;
DO $operational$
DECLARE maximum integer; per_source integer; days integer; newest text; b text;
BEGIN
 SELECT (value#>>'{}')::integer INTO maximum FROM news.settings WHERE key='operational_max_articles';
 SELECT (value#>>'{}')::integer INTO per_source FROM news.settings WHERE key='operational_per_source';
 SELECT (value#>>'{}')::integer INTO days FROM news.settings WHERE key='operational_days';
 SELECT value#>>'{}' INTO newest FROM news.settings WHERE key='operational_newest_date';
 IF maximum IS NULL OR per_source IS NULL OR days IS NULL OR newest IS NULL OR maximum NOT BETWEEN 1 AND 100000 OR per_source NOT BETWEEN 1 AND 100000 OR days NOT BETWEEN 0 AND 36500 THEN RAISE EXCEPTION 'invalid recovered operational limits'; END IF;
 IF newest<>'' AND (newest !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' OR (newest::date)::text<>newest) THEN RAISE EXCEPTION 'invalid recovered operational date'; END IF;
 FOR b IN SELECT value#>>'{}' FROM news.settings WHERE key IN ('operational_limits_enabled','operational_processing_enabled','operational_delivery_enabled','operational_serbian_enabled','telegram_serbian_delivery_enabled') LOOP
  IF b IS NULL OR b NOT IN ('true','false','True','False','TRUE','FALSE','1','0','t','f','T','F') THEN RAISE EXCEPTION 'invalid recovered operational switch'; END IF;
 END LOOP;
END $operational$;
INSERT INTO news.settings(key,value) VALUES('operational_delivery_enabled','false'::jsonb),('telegram_serbian_delivery_enabled','false'::jsonb) ON CONFLICT(key) DO UPDATE SET value=excluded.value;
INSERT INTO news.settings(key,value) VALUES('operational_settings_revision','1'::jsonb) ON CONFLICT(key) DO UPDATE SET value=to_jsonb((news.settings.value#>>'{}')::bigint+1);
UPDATE news.sources SET last_fetch=NULL,last_success=NULL,last_error=NULL,error_count=0;
UPDATE news.schedule_settings SET next_run=NULL,is_running=false,last_run=NULL,last_finished_at=NULL,last_success_at=NULL,last_status=NULL,last_error=NULL,last_duration_seconds=NULL;
DO $sequences$ BEGIN
 PERFORM setval(pg_get_serial_sequence('news.sources','id'),COALESCE((SELECT max(id) FROM news.sources),1),EXISTS(SELECT 1 FROM news.sources));
 PERFORM setval(pg_get_serial_sequence('news.categories','id'),COALESCE((SELECT max(id) FROM news.categories),1),EXISTS(SELECT 1 FROM news.categories));
 PERFORM setval(pg_get_serial_sequence('news.category_mapping','id'),COALESCE((SELECT max(id) FROM news.category_mapping),1),EXISTS(SELECT 1 FROM news.category_mapping));
 PERFORM setval(pg_get_serial_sequence('news.extraction_patterns','id'),COALESCE((SELECT max(id) FROM news.extraction_patterns),1),EXISTS(SELECT 1 FROM news.extraction_patterns));
 PERFORM setval(pg_get_serial_sequence('news.schedule_settings','id'),COALESCE((SELECT max(id) FROM news.schedule_settings),1),EXISTS(SELECT 1 FROM news.schedule_settings));
END $sequences$;
COMMIT;
