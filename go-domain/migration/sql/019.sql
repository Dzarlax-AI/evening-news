CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.editorial_serbian_retry (
 russian_edition_id UUID PRIMARY KEY,
 destination TEXT,
 delivery_enabled BOOLEAN NOT NULL DEFAULT false,
 publication_pinned BOOLEAN NOT NULL DEFAULT false,
 publication_attempted BOOLEAN NOT NULL DEFAULT false,
 next_dispatch_at TIMESTAMPTZ NOT NULL DEFAULT now()+interval '5 minutes',
 dispatches INTEGER NOT NULL DEFAULT 0 CHECK (dispatches BETWEEN 0 AND 12),
 last_job_id UUID,
 CHECK (NOT delivery_enabled OR (destination IS NOT NULL AND length(destination)>0))
);
CREATE OR REPLACE FUNCTION __SCHEMA_Q__.protect_serbian_retry_policy()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,__SCHEMA_Q__ AS $policy$
BEGIN
 IF NEW.russian_edition_id IS DISTINCT FROM OLD.russian_edition_id OR (OLD.publication_attempted AND NOT NEW.publication_attempted) OR (OLD.publication_pinned AND (NEW.publication_pinned IS DISTINCT FROM OLD.publication_pinned OR NEW.destination IS DISTINCT FROM OLD.destination OR NEW.delivery_enabled IS DISTINCT FROM OLD.delivery_enabled)) OR (NOT OLD.publication_pinned AND NOT NEW.publication_pinned AND (NEW.destination IS DISTINCT FROM OLD.destination OR NEW.delivery_enabled IS DISTINCT FROM OLD.delivery_enabled)) THEN
  RAISE EXCEPTION 'serbian retry policy is immutable' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $policy$;
CREATE TRIGGER trg_serbian_retry_policy BEFORE UPDATE ON __SCHEMA_Q__.editorial_serbian_retry FOR EACH ROW EXECUTE FUNCTION __SCHEMA_Q__.protect_serbian_retry_policy();
ALTER TABLE __SCHEMA_Q__.background_jobs DROP CONSTRAINT IF EXISTS ck_background_jobs_kind;
ALTER TABLE __SCHEMA_Q__.background_jobs DROP CONSTRAINT IF EXISTS background_jobs_kind_check;
ALTER TABLE __SCHEMA_Q__.background_jobs ADD CONSTRAINT ck_background_jobs_kind CHECK (kind IN ('news_processing','news_digest','telegram_digest','backup','reprocess_failed','article_reprocess','summaries_generate','source_test','serbian_digest_retry'));

CREATE OR REPLACE FUNCTION __SCHEMA_Q__.enqueue_serbian_digest_retry()
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,__SCHEMA_Q__ AS $retry$
DECLARE r __SCHEMA_Q__.editorial_serbian_retry%ROWTYPE; new_id uuid;
BEGIN
 SELECT x.* INTO r FROM __SCHEMA_Q__.editorial_serbian_retry x
 JOIN __SCHEMA_Q__.editorial_locale_inputs i ON i.russian_edition_id=x.russian_edition_id
 JOIN __SCHEMA_Q__.editorial_digest_editions e ON e.id=x.russian_edition_id AND e.locale='ru'
 LEFT JOIN __SCHEMA_Q__.background_jobs j ON j.id=x.last_job_id
 WHERE x.next_dispatch_at<=now() AND x.dispatches<12
 AND ((i.attempts<3 AND i.last_error IS NOT NULL) OR (x.publication_pinned AND x.publication_attempted AND x.delivery_enabled AND EXISTS (
  SELECT 1 FROM __SCHEMA_Q__.editorial_digest_editions sr
  WHERE sr.locale='sr-Latn' AND sr.job_id=e.job_id AND sr.attempt_generation=e.attempt_generation
  AND NOT EXISTS (SELECT 1 FROM __SCHEMA_Q__.digest_delivery_intents d WHERE d.edition_id=sr.id
   AND (d.state='completed' OR EXISTS (SELECT 1 FROM __SCHEMA_Q__.digest_delivery_parts p WHERE p.intent_id=d.id)))
 )))
 AND (x.last_job_id IS NULL OR j.id IS NULL OR j.state IN ('succeeded','failed'))
 ORDER BY x.next_dispatch_at,x.russian_edition_id FOR UPDATE OF x SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN false; END IF;
 new_id:=gen_random_uuid();
 INSERT INTO __SCHEMA_Q__.background_jobs(id,kind,payload,idempotency_key,state)
 VALUES(new_id,'serbian_digest_retry',jsonb_build_object('russian_edition_id',r.russian_edition_id),
        'serbian-retry:'||r.russian_edition_id::text||':'||(r.dispatches+1)::text,'queued');
 UPDATE __SCHEMA_Q__.editorial_serbian_retry SET dispatches=dispatches+1,last_job_id=new_id,
  next_dispatch_at=now()+interval '30 minutes' WHERE russian_edition_id=r.russian_edition_id;
 RETURN true;
END $retry$;
REVOKE ALL ON FUNCTION __SCHEMA_Q__.enqueue_serbian_digest_retry() FROM PUBLIC;
