CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.article_localizations (
 article_id INTEGER NOT NULL REFERENCES __SCHEMA_Q__.articles(id) ON DELETE CASCADE,
 locale TEXT NOT NULL CHECK (locale IN ('ru','sr-Latn')),
 title TEXT, summary TEXT,
 input_fingerprint TEXT NOT NULL CHECK (length(input_fingerprint)=64),
 prompt_version TEXT NOT NULL, model TEXT NOT NULL,
 status TEXT NOT NULL CHECK (status IN ('completed','retry_wait','exhausted')),
 attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
 next_retry_at TIMESTAMP, last_error TEXT,
 updated_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
 PRIMARY KEY(article_id, locale)
);
CREATE INDEX IF NOT EXISTS idx_article_localizations_due ON __SCHEMA_Q__.article_localizations(next_retry_at,article_id) WHERE status='retry_wait';
CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.category_localizations (
 category_id INTEGER NOT NULL REFERENCES __SCHEMA_Q__.categories(id) ON DELETE CASCADE,
 locale TEXT NOT NULL CHECK (locale IN ('ru','sr-Latn')),
 display_name TEXT NOT NULL CHECK (length(trim(display_name))>0),
 PRIMARY KEY(category_id, locale)
);
