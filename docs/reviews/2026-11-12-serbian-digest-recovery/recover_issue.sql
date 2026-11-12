-- One-shot recovery for the approved 2026-11-11 Serbian digest only.
-- Run with psql -X -v ON_ERROR_STOP=1 against the News database.
-- The transaction refuses changed identity, policy, history, or delivery state.
BEGIN;
SET LOCAL search_path = news, pg_catalog;
SET LOCAL statement_timeout = '15s';
SELECT pg_advisory_xact_lock(hashtextextended('serbian-digest-recovery:6c215a61-23ad-58af-bab2-2db2df086401', 0));
-- Serialize with operational/category writers that use this shared lock.
SELECT pg_advisory_xact_lock(1649258121, 728440);

DO $recovery$
DECLARE
  ru_id constant uuid := '6c215a61-23ad-58af-bab2-2db2df086401';
  ru_job uuid;
  ru_attempt integer;
  retry_row news.editorial_serbian_retry%ROWTYPE;
  old_job_state text;
  recovery_job uuid;
  mapped_count integer;
  category_count integer;
BEGIN
  SELECT e.job_id, e.attempt_generation INTO STRICT ru_job, ru_attempt
  FROM news.editorial_digest_editions e
  WHERE e.id = ru_id AND e.locale = 'ru' AND e.edition_date = DATE '2026-11-11'
  FOR SHARE;

  SELECT r.* INTO STRICT retry_row
  FROM news.editorial_serbian_retry r
  WHERE r.russian_edition_id = ru_id
  FOR UPDATE;

  PERFORM 1 FROM news.categories FOR UPDATE;
  PERFORM 1 FROM news.category_localizations WHERE locale = 'sr-Latn' FOR UPDATE;

  IF retry_row.publication_pinned IS DISTINCT FROM true
     OR retry_row.publication_attempted IS DISTINCT FROM true
     OR retry_row.delivery_enabled IS DISTINCT FROM true
     OR retry_row.destination IS NULL OR retry_row.destination = ''
     OR retry_row.dispatches <> 12 THEN
    RAISE EXCEPTION 'recovery guard failed: pinned policy or exhausted history changed';
  END IF;

  SELECT j.id INTO recovery_job FROM news.background_jobs j
  WHERE j.idempotency_key = 'serbian-recovery:6c215a61-23ad-58af-bab2-2db2df086401:2026-11-12-stage-a';
  IF recovery_job IS NOT NULL THEN
    IF retry_row.last_job_id IS DISTINCT FROM recovery_job
       OR NOT EXISTS (SELECT 1 FROM news.background_jobs j
                      WHERE j.id = recovery_job AND j.kind = 'serbian_digest_retry'
                        AND j.payload = jsonb_build_object('russian_edition_id', ru_id)) THEN
      RAISE EXCEPTION 'recovery guard failed: idempotency key is bound to different work';
    END IF;
    -- A previous invocation already admitted the one stable job. Never requeue it.
    RETURN;
  END IF;

  IF retry_row.last_job_id IS NOT NULL THEN
    SELECT j.state INTO old_job_state FROM news.background_jobs j WHERE j.id = retry_row.last_job_id;
    IF old_job_state IS DISTINCT FROM 'succeeded' AND old_job_state IS DISTINCT FROM 'failed' THEN
      RAISE EXCEPTION 'recovery guard failed: last retry is not terminal';
    END IF;
  END IF;

  IF EXISTS (SELECT 1 FROM news.editorial_locale_inputs i
             WHERE i.russian_edition_id = ru_id AND (i.serbian_snapshot IS NOT NULL OR i.attempts <> 0)) THEN
    RAISE EXCEPTION 'recovery guard failed: Serbian input already exists or has attempts';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM news.editorial_locale_inputs i WHERE i.russian_edition_id = ru_id) THEN
    RAISE EXCEPTION 'recovery guard failed: frozen Russian input is missing';
  END IF;
  IF EXISTS (SELECT 1 FROM news.editorial_locale_inputs i
             JOIN news.editorial_digest_editions e ON e.id = i.russian_edition_id
             WHERE i.russian_edition_id = ru_id AND i.russian_snapshot IS DISTINCT FROM e.snapshot) THEN
    RAISE EXCEPTION 'recovery guard failed: frozen Russian input differs from the edition';
  END IF;

  IF EXISTS (SELECT 1 FROM news.editorial_digest_editions sr
             WHERE sr.locale = 'sr-Latn' AND sr.job_id = ru_job AND sr.attempt_generation = ru_attempt) THEN
    RAISE EXCEPTION 'recovery guard failed: paired Serbian edition exists';
  END IF;
  IF EXISTS (SELECT 1 FROM news.background_jobs j
             WHERE j.kind = 'serbian_digest_retry' AND j.payload->>'russian_edition_id' = ru_id::text
               AND j.state IN ('queued','running','interrupted','delivery_unknown')) THEN
    RAISE EXCEPTION 'recovery guard failed: retry job is active or ambiguous';
  END IF;
  IF EXISTS (SELECT 1 FROM news.digest_delivery_intents d
             JOIN news.editorial_digest_editions sr ON sr.id = d.edition_id
             WHERE sr.locale = 'sr-Latn' AND sr.job_id = ru_job AND sr.attempt_generation = ru_attempt
               AND (d.state IN ('sending','unknown','completed') OR EXISTS
                    (SELECT 1 FROM news.digest_delivery_parts p WHERE p.intent_id = d.id))) THEN
    RAISE EXCEPTION 'recovery guard failed: paired Serbian delivery has a receipt or part';
  END IF;

  -- Repair only absent labels; existing category translations remain byte-for-byte unchanged.
  INSERT INTO news.category_localizations(category_id, locale, display_name)
  SELECT c.id, 'sr-Latn', m.display_name
  FROM news.categories c
  JOIN (VALUES
    ('business','Poslovanje'), ('tech','Tehnologija'), ('technology','Tehnologija'),
    ('science','Nauka'), ('nature','Priroda i ekologija'), ('serbia','Srbija'),
    ('marketing','Marketing'), ('media','Mediji'), ('other','Ostalo'),
    ('ai','Veštačka inteligencija'), ('politics','Politika'), ('history','Istorija')
  ) AS m(category_name, display_name) ON lower(c.name) = m.category_name
  ON CONFLICT (category_id, locale) DO UPDATE
  SET display_name = EXCLUDED.display_name
  WHERE btrim(news.category_localizations.display_name) = '';

  SELECT count(*) INTO category_count FROM news.categories;
  SELECT count(*) INTO mapped_count
  FROM news.categories c
  WHERE NOT EXISTS (SELECT 1 FROM news.category_localizations cl
                    WHERE cl.category_id = c.id AND cl.locale = 'sr-Latn' AND btrim(cl.display_name) <> '');
  IF category_count <> 11 OR mapped_count <> 0 THEN
    RAISE EXCEPTION 'recovery guard failed: category inventory is unknown or a Serbian label is missing';
  END IF;

  -- Fixed idempotency key makes later invocations observe the same job; dispatch history is untouched.
  recovery_job := gen_random_uuid();
  INSERT INTO news.background_jobs(id, kind, payload, idempotency_key, state)
  VALUES (recovery_job, 'serbian_digest_retry', jsonb_build_object('russian_edition_id', ru_id),
          'serbian-recovery:6c215a61-23ad-58af-bab2-2db2df086401:2026-11-12-stage-a', 'queued');

  UPDATE news.editorial_serbian_retry
  SET last_job_id = recovery_job
  WHERE russian_edition_id = ru_id;
END
$recovery$;

COMMIT;
