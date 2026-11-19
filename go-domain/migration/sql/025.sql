CREATE VIEW __SCHEMA_Q__.article_feed_duplicate_predecessors
WITH (security_barrier = true) AS
SELECT subject.id AS article_id, predecessor.id AS predecessor_id
FROM __SCHEMA_Q__.articles subject
JOIN __SCHEMA_Q__.article_dedup_observations current_observation
  ON current_observation.article_id = subject.id
 AND current_observation.version = 1
 AND current_observation.claim_token = subject.completed_processing_claim
JOIN __SCHEMA_Q__.article_dedup_observations prior_observation
  ON prior_observation.version = current_observation.version
 AND prior_observation.exact_hash = current_observation.exact_hash
JOIN __SCHEMA_Q__.articles predecessor
  ON predecessor.id = prior_observation.article_id
 AND predecessor.completed_processing_claim = prior_observation.claim_token
 AND predecessor.source_id = subject.source_id
 AND predecessor.published_at = subject.published_at
WHERE subject.processing_state = 'completed'
  AND predecessor.processing_state = 'completed'
  AND current_observation.exact_hash IS NOT NULL
  AND (subject.source_id IS NOT NULL AND subject.published_at IS NOT NULL)
  AND (COALESCE(predecessor.fetched_at, 'infinity'), predecessor.id)
    < (COALESCE(subject.fetched_at, 'infinity'), subject.id);

REVOKE ALL ON __SCHEMA_Q__.article_feed_duplicate_predecessors FROM PUBLIC;

DO $feed_duplicate_grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'news_web_runtime') THEN
        EXECUTE format('GRANT SELECT ON %I.article_feed_duplicate_predecessors TO news_web_runtime', __SCHEMA_LITERAL__);
    END IF;
END
$feed_duplicate_grants$;
