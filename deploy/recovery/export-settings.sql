-- Operator recipe. Run psql -XqAt with stdout redirected into a private file.
-- Source runtimes must be quiesced; this query itself changes no durable data.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '30s';
SELECT jsonb_build_object(
  'format','news-settings-v1',
  'schema_version',16,
  'identity',(SELECT jsonb_build_object('installation_id',installation_id,'revision',revision) FROM news.installation_state WHERE id=1),
  'sources',COALESCE((SELECT jsonb_agg(jsonb_build_object('id',id,'name',name,'source_type',source_type,'url',url,'enabled',enabled,'fetch_interval',fetch_interval,'config',COALESCE((SELECT jsonb_object_agg(key,value) FROM jsonb_each(COALESCE(config::jsonb,'{}')) WHERE key IN ('limit','max_articles_per_check','min_title_length','use_browser','wait_for_js','wait_timeout_ms','enable_ai_analysis','article_selectors','title_selectors','date_selectors','link_selectors','learned_container_selectors','learned_title_selectors','learned_link_selectors','learned_date_selectors','learned_general_selectors')),'{}'::jsonb)) ORDER BY id) FROM news.sources),'[]'::jsonb),
  'categories',COALESCE((SELECT jsonb_agg(jsonb_build_object('id',id,'name',name,'display_name',display_name,'description',description,'color',color,'is_local',is_local) ORDER BY id) FROM news.categories),'[]'::jsonb),
  'category_mapping',COALESCE((SELECT jsonb_agg(jsonb_build_object('id',id,'ai_category',ai_category,'fixed_category',fixed_category,'confidence_threshold',confidence_threshold,'description',description,'created_by',created_by,'is_active',is_active) ORDER BY id) FROM news.category_mapping),'[]'::jsonb),
  'extraction_patterns',COALESCE((SELECT jsonb_agg(jsonb_build_object('id',id,'domain',domain,'selector_pattern',selector_pattern,'extraction_strategy',extraction_strategy,'discovered_by',discovered_by,'is_stable',is_stable) ORDER BY id) FROM news.extraction_patterns WHERE discovered_by IN ('manual','user')),'[]'::jsonb),
  'schedule_settings',COALESCE((SELECT jsonb_agg(jsonb_build_object('id',id,'task_name',task_name,'enabled',enabled,'schedule_type',schedule_type,'hour',hour,'minute',minute,'weekdays',weekdays,'timezone',timezone,'task_config',task_config) ORDER BY id) FROM news.schedule_settings),'[]'::jsonb),
  'settings',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',key,'value',value) ORDER BY key) FROM news.settings WHERE key IN ('ai_settings','ai_settings_revision','ai_model_prices','telegram_chat_id','telegram_service_chat_id','telegram_settings_revision','operational_settings_revision','operational_limits_enabled','operational_max_articles','operational_per_source','operational_days','operational_newest_date','operational_processing_enabled','operational_delivery_enabled')),'[]'::jsonb),
  'managed_admin',COALESCE((SELECT jsonb_agg(jsonb_build_object('id',id,'username',username,'password_hash',password_hash,'revision',revision) ORDER BY id) FROM news.managed_admin),'[]'::jsonb),
  'integration_credentials',COALESCE((SELECT jsonb_agg(jsonb_build_object('provider',provider,'revision',revision,'envelope_version',envelope_version,'key_id',key_id,'nonce',nonce,'ciphertext',ciphertext) ORDER BY provider) FROM news.integration_credentials),'[]'::jsonb));
COMMIT;
