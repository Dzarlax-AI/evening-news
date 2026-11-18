ALTER TABLE __SCHEMA_Q__.articles
    ADD COLUMN media_evidence JSONB,
    ADD CONSTRAINT ck_articles_media_evidence_object
        CHECK (media_evidence IS NULL OR (
            jsonb_typeof(media_evidence) = 'object' AND octet_length(media_evidence::text) <= 8192
        ));

DO $media_evidence_grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'news_processing_runtime') THEN
        EXECUTE format('GRANT UPDATE (media_evidence) ON %I.articles TO news_processing_runtime', __SCHEMA_LITERAL__);
    END IF;
END
$media_evidence_grants$;
