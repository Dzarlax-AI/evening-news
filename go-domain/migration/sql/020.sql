ALTER TABLE __SCHEMA_Q__.integration_credentials DROP CONSTRAINT IF EXISTS integration_credentials_provider_check;
ALTER TABLE __SCHEMA_Q__.integration_credentials ADD CONSTRAINT integration_credentials_provider_check CHECK (provider IN ('gemini','openai','telegram','viber'));
ALTER TABLE __SCHEMA_Q__.background_jobs DROP CONSTRAINT IF EXISTS ck_background_jobs_kind;
ALTER TABLE __SCHEMA_Q__.background_jobs ADD CONSTRAINT ck_background_jobs_kind CHECK (kind IN ('news_processing','news_digest','telegram_digest','backup','reprocess_failed','article_reprocess','summaries_generate','source_test','serbian_digest_retry','viber_digest_delivery'));
CREATE TABLE __SCHEMA_Q__.viber_publication_requests (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), russian_edition_id UUID NOT NULL REFERENCES __SCHEMA_Q__.editorial_digest_editions(id),
 channel_id TEXT NOT NULL, purpose TEXT NOT NULL DEFAULT 'digest' CHECK (purpose='digest'), sender_id TEXT NOT NULL,
 settings_revision BIGINT NOT NULL, credential_revision BIGINT NOT NULL CHECK (credential_revision>0), enabled_from TIMESTAMPTZ NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','materialized','cancelled')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(russian_edition_id,channel_id,purpose)
);
-- Only the domain producer and database owner may create or mutate durable
-- publication and operational snapshots; web retains ordinary settings DML.
CREATE OR REPLACE FUNCTION __SCHEMA_Q__.protect_managed_publication_settings() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $settings_guard$
DECLARE protected boolean := false;
BEGIN
 IF TG_OP <> 'INSERT' THEN
  protected := left(OLD.key, length('managed_digest_publication:'))='managed_digest_publication:' OR left(OLD.key, length('managed_job_config:'))='managed_job_config:';
 END IF;
 IF TG_OP <> 'DELETE' THEN
  protected := protected OR left(NEW.key, length('managed_digest_publication:'))='managed_digest_publication:' OR left(NEW.key, length('managed_job_config:'))='managed_job_config:';
 END IF;
 IF protected AND NOT has_table_privilege(current_user, format('%I.%I',TG_TABLE_SCHEMA,'viber_publication_requests'), 'INSERT') THEN
  RAISE EXCEPTION 'managed publication settings require producer privilege' USING ERRCODE='42501';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $settings_guard$;
CREATE TRIGGER trg_protect_managed_publication_settings BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA_Q__.settings
FOR EACH ROW EXECUTE FUNCTION __SCHEMA_Q__.protect_managed_publication_settings();
CREATE OR REPLACE FUNCTION __SCHEMA_Q__.protect_viber_publication_request() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,__SCHEMA_Q__ AS $request_guard$
BEGIN
 IF (NEW.russian_edition_id,NEW.channel_id,NEW.purpose,NEW.sender_id,NEW.settings_revision,NEW.credential_revision,NEW.enabled_from,NEW.created_at) IS DISTINCT FROM (OLD.russian_edition_id,OLD.channel_id,OLD.purpose,OLD.sender_id,OLD.settings_revision,OLD.credential_revision,OLD.enabled_from,OLD.created_at) OR (OLD.state <> 'pending' AND NEW.state IS DISTINCT FROM OLD.state) THEN RAISE EXCEPTION 'immutable viber request' USING ERRCODE='55000'; END IF;
 RETURN NEW;
END $request_guard$;
CREATE TRIGGER trg_viber_request_immutable BEFORE UPDATE ON __SCHEMA_Q__.viber_publication_requests FOR EACH ROW EXECUTE FUNCTION __SCHEMA_Q__.protect_viber_publication_request();
CREATE TABLE __SCHEMA_Q__.viber_digest_deliveries (
 id UUID PRIMARY KEY,
 russian_edition_id UUID NOT NULL REFERENCES __SCHEMA_Q__.editorial_digest_editions(id),
 serbian_edition_id UUID REFERENCES __SCHEMA_Q__.editorial_digest_editions(id),
 channel_id TEXT NOT NULL, purpose TEXT NOT NULL DEFAULT 'digest' CHECK (purpose='digest'),
 sender_id TEXT NOT NULL, snapshot_hash TEXT, content_hash TEXT, render_version TEXT, render_hash TEXT, job_id UUID, attempt_generation INTEGER, domain_instance_id UUID, settings_revision BIGINT NOT NULL, credential_revision BIGINT NOT NULL CHECK (credential_revision>0),
 enabled_from TIMESTAMPTZ NOT NULL,
 state TEXT NOT NULL CHECK (state IN ('waiting_edition','ready','sending','retry_wait','completed','rejected','delivery_unknown','exhausted','configuration_changed','cancelled')),
 dispatches INTEGER NOT NULL DEFAULT 0 CHECK (dispatches BETWEEN 0 AND 6),
 next_dispatch_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_job_id UUID,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(russian_edition_id,channel_id,purpose)
);
CREATE TABLE __SCHEMA_Q__.viber_digest_parts (
 delivery_id UUID NOT NULL REFERENCES __SCHEMA_Q__.viber_digest_deliveries(id),
 part_index INTEGER NOT NULL CHECK (part_index>=0), text TEXT NOT NULL,
 job_id UUID, attempt_generation INTEGER, domain_instance_id UUID,
 payload_hash CHAR(64) NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
 state TEXT NOT NULL CHECK (state IN ('pending','sending','sent','rejected','unknown')),
 attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3), message_token TEXT,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(delivery_id,part_index),
 CHECK ((state='sent') = (message_token IS NOT NULL))
);
CREATE TABLE __SCHEMA_Q__.viber_test_receipts (
 request_id UUID PRIMARY KEY, channel_id TEXT NOT NULL, sender_id TEXT NOT NULL,
 settings_revision BIGINT NOT NULL, credential_revision BIGINT NOT NULL,
 state TEXT NOT NULL CHECK (state IN ('sending','sent','rejected','unknown')),
 message_token TEXT, status_code INTEGER NOT NULL DEFAULT 0, api_status INTEGER NOT NULL DEFAULT 0,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CHECK ((state='sent')=(message_token IS NOT NULL))
);
CREATE TABLE __SCHEMA_Q__.viber_channel_leases (
 channel_id TEXT PRIMARY KEY, delivery_id UUID REFERENCES __SCHEMA_Q__.viber_digest_deliveries(id),
 test_request_id UUID REFERENCES __SCHEMA_Q__.viber_test_receipts(request_id),
 lease_until TIMESTAMPTZ, next_post_at TIMESTAMPTZ NOT NULL DEFAULT now(), cooldown_until TIMESTAMPTZ NOT NULL DEFAULT now(), CHECK (delivery_id IS NULL OR test_request_id IS NULL)
);
CREATE OR REPLACE FUNCTION __SCHEMA_Q__.protect_viber_binding() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,__SCHEMA_Q__ AS $guard$
BEGIN
 IF (NEW.russian_edition_id,NEW.channel_id,NEW.purpose,NEW.sender_id,NEW.settings_revision,NEW.credential_revision,NEW.enabled_from) IS DISTINCT FROM (OLD.russian_edition_id,OLD.channel_id,OLD.purpose,OLD.sender_id,OLD.settings_revision,OLD.credential_revision,OLD.enabled_from)
 OR (OLD.serbian_edition_id IS NOT NULL AND (NEW.serbian_edition_id,NEW.snapshot_hash,NEW.content_hash) IS DISTINCT FROM (OLD.serbian_edition_id,OLD.snapshot_hash,OLD.content_hash))
 OR (OLD.render_hash IS NOT NULL AND (NEW.render_hash,NEW.render_version) IS DISTINCT FROM (OLD.render_hash,OLD.render_version)) THEN
 RAISE EXCEPTION 'immutable viber binding' USING ERRCODE='55000'; END IF;
 IF NEW.serbian_edition_id IS NOT NULL AND NOT EXISTS (
  SELECT 1 FROM __SCHEMA_Q__.editorial_digest_editions ru JOIN __SCHEMA_Q__.editorial_digest_editions sr ON sr.job_id=ru.job_id AND sr.attempt_generation=ru.attempt_generation
  WHERE ru.id=NEW.russian_edition_id AND ru.locale='ru' AND sr.id=NEW.serbian_edition_id AND sr.locale='sr-Latn'
 ) THEN RAISE EXCEPTION 'viber edition mismatch' USING ERRCODE='55000'; END IF;
 RETURN NEW;
END $guard$;
CREATE TRIGGER trg_viber_binding_immutable BEFORE UPDATE ON __SCHEMA_Q__.viber_digest_deliveries FOR EACH ROW EXECUTE FUNCTION __SCHEMA_Q__.protect_viber_binding();
CREATE OR REPLACE FUNCTION __SCHEMA_Q__.protect_viber_part() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,__SCHEMA_Q__ AS $guard$
BEGIN
 IF TG_OP='INSERT' THEN
  IF EXISTS(SELECT 1 FROM __SCHEMA_Q__.viber_digest_deliveries WHERE id=NEW.delivery_id AND render_hash IS NOT NULL) THEN RAISE EXCEPTION 'viber parts already frozen' USING ERRCODE='55000'; END IF;
  RETURN NEW;
 END IF;
 IF TG_OP='DELETE' OR (NEW.delivery_id,NEW.part_index,NEW.text,NEW.payload_hash) IS DISTINCT FROM (OLD.delivery_id,OLD.part_index,OLD.text,OLD.payload_hash) OR (OLD.state IN ('sent','unknown') AND NEW.state IS DISTINCT FROM OLD.state) OR NEW.attempts < OLD.attempts THEN RAISE EXCEPTION 'immutable viber part' USING ERRCODE='55000'; END IF;
 RETURN NEW;
END $guard$;
CREATE TRIGGER trg_viber_part_immutable BEFORE INSERT OR UPDATE OR DELETE ON __SCHEMA_Q__.viber_digest_parts FOR EACH ROW EXECUTE FUNCTION __SCHEMA_Q__.protect_viber_part();
-- Persisted selections are application-generated but may be stale or malformed.
-- Invalid values must not block admission for other publications.
CREATE OR REPLACE FUNCTION __SCHEMA_Q__.viber_parse_time(v text) RETURNS timestamptz LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $parse$
BEGIN RETURN v::timestamptz; EXCEPTION WHEN data_exception THEN RETURN NULL; END $parse$;
CREATE OR REPLACE FUNCTION __SCHEMA_Q__.viber_parse_bigint(v text) RETURNS bigint LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $parse$
BEGIN RETURN v::bigint; EXCEPTION WHEN data_exception THEN RETURN NULL; END $parse$;
REVOKE ALL ON FUNCTION __SCHEMA_Q__.viber_parse_time(text), __SCHEMA_Q__.viber_parse_bigint(text) FROM PUBLIC;
CREATE OR REPLACE FUNCTION __SCHEMA_Q__.enqueue_viber_digest_delivery()
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,__SCHEMA_Q__ AS $enqueue$
DECLARE delivery_row __SCHEMA_Q__.viber_digest_deliveries%ROWTYPE; new_id UUID;
BEGIN
 PERFORM pg_advisory_xact_lock_shared(1649258121,728440), pg_advisory_xact_lock_shared(1649258121,728441);
 -- Reconstruct only from new, fenced shared publication selections plus the
 -- exact immutable job snapshot; never search existing editions by date.
 INSERT INTO __SCHEMA_Q__.viber_publication_requests(russian_edition_id,channel_id,sender_id,settings_revision,credential_revision,enabled_from,created_at)
 SELECT ru.id, cfg.value->'viber'->>'channel_id', cfg.value->'viber'->>'sender_id',
 __SCHEMA_Q__.viber_parse_bigint(cfg.value->'viber'->>'revision'), __SCHEMA_Q__.viber_parse_bigint(cfg.value->'viber'->>'credential_revision'),
 __SCHEMA_Q__.viber_parse_time(cfg.value->'viber'->>'enabled_from'), __SCHEMA_Q__.viber_parse_time(sel.value->>'publication_at')
 FROM __SCHEMA_Q__.settings sel
 JOIN __SCHEMA_Q__.background_jobs j ON j.id::text=sel.value->>'job_id'
 JOIN __SCHEMA_Q__.settings cfg ON cfg.key='managed_job_config:'||j.id::text||':'||(sel.value->>'attempt_generation')
 JOIN __SCHEMA_Q__.editorial_digest_editions ru ON ru.id::text=sel.value->>'russian_edition_id' AND ru.locale='ru' AND ru.job_id IS NOT NULL
 WHERE sel.key='managed_digest_publication:'||j.id::text||':'||(sel.value->>'attempt_generation')
 AND (sel.value->>'attempt_generation') ~ '^[1-9][0-9]{0,9}$'
 AND cfg.value->>'job_id'=j.id::text AND cfg.value->>'attempt_generation'=sel.value->>'attempt_generation'
 AND j.kind IN ('news_digest','telegram_digest') AND (j.kind<>'news_digest' OR j.payload->'send_telegram' IS DISTINCT FROM 'false'::jsonb)
 AND (cfg.value::jsonb)->'settings'->'delivery_enabled'='true'::jsonb AND (cfg.value::jsonb)->'settings'->'serbian_enabled'='true'::jsonb
 AND (cfg.value::jsonb)->'viber'->'delivery_enabled'='true'::jsonb AND (cfg.value::jsonb)->'viber'->'webhook_ready'='true'::jsonb
 AND length(cfg.value->'viber'->>'channel_id') BETWEEN 1 AND 128 AND length(cfg.value->'viber'->>'sender_id') BETWEEN 1 AND 128
 AND __SCHEMA_Q__.viber_parse_bigint(cfg.value->'viber'->>'revision')>=0
 AND __SCHEMA_Q__.viber_parse_bigint(cfg.value->'viber'->>'credential_revision')>0
 AND __SCHEMA_Q__.viber_parse_bigint(cfg.value->'viber'->>'credential_revision')=__SCHEMA_Q__.viber_parse_bigint(cfg.value->'viber'->>'verified_credential_revision')
 AND __SCHEMA_Q__.viber_parse_time(sel.value->>'publication_at')>clock_timestamp()-interval '24 hours'
 AND __SCHEMA_Q__.viber_parse_time(sel.value->>'publication_at')<=clock_timestamp()
 AND __SCHEMA_Q__.viber_parse_time(cfg.value->'viber'->>'enabled_from')<=__SCHEMA_Q__.viber_parse_time(sel.value->>'publication_at')
 AND (ru.created_at AT TIME ZONE 'UTC')>=__SCHEMA_Q__.viber_parse_time(cfg.value->'viber'->>'enabled_from')
 ON CONFLICT (russian_edition_id,channel_id,purpose) DO NOTHING;
 UPDATE __SCHEMA_Q__.viber_publication_requests r SET state='cancelled'
 WHERE r.state='pending' AND (r.created_at<=clock_timestamp()-interval '24 hours' OR NOT (
  COALESCE((SELECT (value #>> '{}')::boolean FROM __SCHEMA_Q__.settings WHERE key='operational_delivery_enabled'),true)
  AND COALESCE((SELECT (value->>'delivery_enabled')::boolean AND (value->>'enabled_from')::timestamptz=r.enabled_from
   AND value->>'channel_id'=r.channel_id AND value->>'sender_id'=r.sender_id
   AND (value->>'credential_revision')::bigint=r.credential_revision AND (value->>'verified_credential_revision')::bigint=r.credential_revision
   AND (value->>'webhook_ready')::boolean FROM __SCHEMA_Q__.settings WHERE key='viber_settings'),false)
  AND COALESCE((SELECT revision=r.credential_revision AND ciphertext IS NOT NULL FROM __SCHEMA_Q__.integration_credentials WHERE provider='viber'),false)
 ));
 INSERT INTO __SCHEMA_Q__.viber_digest_deliveries(id,russian_edition_id,serbian_edition_id,channel_id,purpose,sender_id,settings_revision,credential_revision,enabled_from,state,snapshot_hash,content_hash,created_at)
 SELECT gen_random_uuid(),ru.id,sr.id,r.channel_id,r.purpose,r.sender_id,r.settings_revision,r.credential_revision,r.enabled_from,
 CASE WHEN sr.id IS NULL THEN 'waiting_edition' ELSE 'ready' END,sr.snapshot_hash,CASE WHEN sr.id IS NULL THEN NULL ELSE encode(sha256(convert_to(sr.edition::text,'UTF8')),'hex') END,r.created_at
 FROM __SCHEMA_Q__.viber_publication_requests r JOIN __SCHEMA_Q__.editorial_digest_editions ru ON ru.id=r.russian_edition_id AND ru.locale='ru' AND ru.job_id IS NOT NULL AND (ru.created_at AT TIME ZONE 'UTC')>=r.enabled_from
 LEFT JOIN __SCHEMA_Q__.editorial_digest_editions sr ON sr.job_id=ru.job_id AND sr.attempt_generation=ru.attempt_generation AND sr.locale='sr-Latn'
 WHERE r.state='pending' ON CONFLICT (russian_edition_id,channel_id,purpose) DO NOTHING;
 UPDATE __SCHEMA_Q__.viber_publication_requests r SET state='materialized' WHERE r.state='pending' AND EXISTS
 (SELECT 1 FROM __SCHEMA_Q__.viber_digest_deliveries d WHERE d.russian_edition_id=r.russian_edition_id AND d.channel_id=r.channel_id AND d.purpose=r.purpose AND d.enabled_from=r.enabled_from AND d.sender_id=r.sender_id AND d.credential_revision=r.credential_revision AND d.settings_revision=r.settings_revision);
 UPDATE __SCHEMA_Q__.viber_digest_deliveries x SET state='delivery_unknown',updated_at=now()
 WHERE x.state IN ('ready','retry_wait','sending')
 AND (x.state='sending' OR EXISTS(SELECT 1 FROM __SCHEMA_Q__.viber_digest_parts p WHERE p.delivery_id=x.id AND p.state IN ('sending','unknown')))
 AND EXISTS(SELECT 1 FROM __SCHEMA_Q__.background_jobs j WHERE j.id=x.last_job_id AND j.state IN ('failed','succeeded','delivery_unknown','interrupted','queued'));
 UPDATE __SCHEMA_Q__.viber_digest_deliveries x SET state='exhausted',updated_at=now()
 WHERE x.state IN ('waiting_edition','ready','retry_wait') AND (x.created_at<=now()-interval '24 hours' OR x.dispatches>=6) AND (x.last_job_id IS NULL OR EXISTS (SELECT 1 FROM __SCHEMA_Q__.background_jobs j WHERE j.id=x.last_job_id AND j.state IN ('succeeded','failed','delivery_unknown')));
 UPDATE __SCHEMA_Q__.viber_digest_deliveries x SET serbian_edition_id=sr.id,snapshot_hash=sr.snapshot_hash,content_hash=encode(sha256(convert_to(sr.edition::text,'UTF8')),'hex'),state='ready',updated_at=now()
 FROM __SCHEMA_Q__.editorial_digest_editions ru JOIN __SCHEMA_Q__.editorial_digest_editions sr ON sr.job_id=ru.job_id AND sr.attempt_generation=ru.attempt_generation AND sr.locale='sr-Latn'
 WHERE x.state='waiting_edition' AND x.russian_edition_id=ru.id AND ru.locale='ru';
 SELECT x.* INTO delivery_row FROM __SCHEMA_Q__.viber_digest_deliveries x
 LEFT JOIN __SCHEMA_Q__.background_jobs j ON j.id=x.last_job_id
 WHERE x.state IN ('ready','retry_wait') AND x.serbian_edition_id IS NOT NULL AND x.dispatches<6
 AND x.next_dispatch_at<=now() AND x.created_at>now()-interval '24 hours'
 AND (x.last_job_id IS NULL OR j.id IS NULL OR j.state IN ('succeeded','failed','delivery_unknown'))
 AND NOT EXISTS(SELECT 1 FROM __SCHEMA_Q__.viber_digest_parts p WHERE p.delivery_id=x.id AND (p.state IN ('sending','unknown') OR (p.state='rejected' AND x.state<>'retry_wait')))
 ORDER BY x.next_dispatch_at,x.id FOR UPDATE OF x SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN false; END IF;
 new_id:=gen_random_uuid();
 INSERT INTO __SCHEMA_Q__.background_jobs(id,kind,payload,idempotency_key,state)
 VALUES(new_id,'viber_digest_delivery',jsonb_build_object('delivery_id',delivery_row.id),
 'viber-delivery:'||delivery_row.id::text||':'||(delivery_row.dispatches+1)::text,'queued');
 UPDATE __SCHEMA_Q__.viber_digest_deliveries SET dispatches=dispatches+1,last_job_id=new_id,updated_at=now() WHERE id=delivery_row.id;
 RETURN true;
END $enqueue$;
REVOKE ALL ON FUNCTION __SCHEMA_Q__.enqueue_viber_digest_delivery() FROM PUBLIC;
