ALTER TABLE __SCHEMA_Q__.editorial_digest_editions DROP CONSTRAINT IF EXISTS editorial_digest_editions_locale_check;
ALTER TABLE __SCHEMA_Q__.editorial_digest_editions ADD CONSTRAINT editorial_digest_editions_locale_check CHECK (locale IN ('ru','sr-Latn'));
-- PostgreSQL assigns a table-level name when the original inline CHECK also
-- references snapshot_hash. Find that check by its definition, not its name.
DO $localization_constraints$
DECLARE constraint_name text;
BEGIN
 FOR constraint_name IN SELECT c.conname FROM pg_constraint c
  WHERE c.conrelid=to_regclass(__SCHEMA_LITERAL__ || '.editorial_digest_editions') AND c.contype='c'
  AND pg_get_constraintdef(c.oid) LIKE '%edition%' AND pg_get_constraintdef(c.oid) LIKE '%locale%'
 LOOP
  EXECUTE format('ALTER TABLE %I.editorial_digest_editions DROP CONSTRAINT %I',__SCHEMA_LITERAL__,constraint_name);
 END LOOP;
END $localization_constraints$;
ALTER TABLE __SCHEMA_Q__.editorial_digest_editions ADD CONSTRAINT editorial_digest_editions_edition_check CHECK (
 jsonb_typeof(edition)='object' AND edition->>'version'='1'
 AND edition->>'locale'=locale AND edition->>'source_snapshot_hash'=snapshot_hash
 AND jsonb_typeof(edition->'stories')='array');
CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.editorial_locale_inputs (
 russian_edition_id UUID PRIMARY KEY,
 russian_snapshot JSONB NOT NULL,
 serbian_snapshot JSONB,
 attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3),
 next_attempt_at TIMESTAMPTZ,
 last_error TEXT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO __SCHEMA_Q__.category_localizations(category_id,locale,display_name)
SELECT id,'sr-Latn',CASE lower(name)
 WHEN 'serbia' THEN 'Srbija' WHEN 'tech' THEN 'Tehnologija' WHEN 'technology' THEN 'Tehnologija'
 WHEN 'ai' THEN 'Veštačka inteligencija' WHEN 'politics' THEN 'Politika' WHEN 'business' THEN 'Poslovanje'
 WHEN 'media' THEN 'Mediji' WHEN 'science' THEN 'Nauka' WHEN 'health' THEN 'Zdravlje'
 WHEN 'world' THEN 'Svet' WHEN 'culture' THEN 'Kultura' WHEN 'other' THEN 'Ostalo'
 WHEN 'marketing' THEN 'Marketing' END
FROM __SCHEMA_Q__.categories WHERE lower(name) IN ('serbia','tech','technology','ai','politics','business','media','science','health','world','culture','other','marketing')
ON CONFLICT(category_id,locale) DO NOTHING;

CREATE OR REPLACE FUNCTION __SCHEMA_Q__.reject_editorial_locale_input_mutation()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog, __SCHEMA_Q__ AS $frozen_inputs$
BEGIN
 IF NEW.russian_edition_id IS DISTINCT FROM OLD.russian_edition_id
 OR NEW.russian_snapshot IS DISTINCT FROM OLD.russian_snapshot
 OR (OLD.serbian_snapshot IS NOT NULL AND NEW.serbian_snapshot IS DISTINCT FROM OLD.serbian_snapshot) THEN
  RAISE EXCEPTION 'editorial locale inputs are immutable' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $frozen_inputs$;
DROP TRIGGER IF EXISTS trg_editorial_locale_inputs_immutable ON __SCHEMA_Q__.editorial_locale_inputs;
CREATE TRIGGER trg_editorial_locale_inputs_immutable BEFORE UPDATE ON __SCHEMA_Q__.editorial_locale_inputs
FOR EACH ROW EXECUTE FUNCTION __SCHEMA_Q__.reject_editorial_locale_input_mutation();

-- A source or Russian analysis revision invalidates only an existing dependent
-- Serbian result. It does not create backfill work for untranslated history.
CREATE OR REPLACE FUNCTION __SCHEMA_Q__.invalidate_article_serbian_localization()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog, __SCHEMA_Q__ AS $invalidate_locale$
BEGIN
 UPDATE __SCHEMA_Q__.article_localizations
 SET status='retry_wait',attempts=0,next_retry_at=(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')+interval '30 minutes',
     last_error='source_changed',updated_at=CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
 WHERE article_id=NEW.id AND locale='sr-Latn';
 RETURN NEW;
END $invalidate_locale$;
REVOKE ALL ON FUNCTION __SCHEMA_Q__.invalidate_article_serbian_localization() FROM PUBLIC;
DROP TRIGGER IF EXISTS trg_article_serbian_invalidation ON __SCHEMA_Q__.articles;
CREATE TRIGGER trg_article_serbian_invalidation AFTER UPDATE OF title,content,summary,url ON __SCHEMA_Q__.articles
FOR EACH ROW WHEN (OLD.title IS DISTINCT FROM NEW.title OR OLD.content IS DISTINCT FROM NEW.content
 OR OLD.summary IS DISTINCT FROM NEW.summary OR OLD.url IS DISTINCT FROM NEW.url)
EXECUTE FUNCTION __SCHEMA_Q__.invalidate_article_serbian_localization();
