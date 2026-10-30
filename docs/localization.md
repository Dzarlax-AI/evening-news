# Localization contracts

The supported locales are `ru` and Serbian Latin `sr-Latn`. Russian public URLs keep their existing meaning. An explicit `locale` query selects Serbian content; unknown locales fail closed. Saved interface preference is carried onto subsequent links and never changes the meaning of an old Russian article or edition URL.

## Articles

Opt-in Serbian generation is pinned in the operational settings for each new job. The main analysis requests both localized titles/summaries from one source input. Categorization and advertisement classification remain common. The version 2 callback result preserves the Russian projection atomically with the localized rows. An invalid shared result cannot be persisted as partial success.

`article_localizations` records each locale's output, input fingerprint, prompt/model metadata, status, attempts, and retry deadline. A correct Russian result survives a failed Serbian result. Only explicit failed Serbian rows are eligible for the bounded locale retry; it translates saved Russian analysis without extraction or incrementing the article processing attempt. Claim, worker-generation, input-fingerprint and source-snapshot checks reject stale results. No job automatically translates historical articles.

`category_localizations` holds localized display names for common category IDs. The admin category editor preserves an omitted Serbian display name. Script checks reject predominantly Cyrillic Serbian prose while preserving quoted source text; these checks do not prove translation quality.

## Editorial editions and delivery

The bilingual main generation uses one provider request with saved summaries and verifies identical story order, membership, primary articles, and exclusions. Locale evidence refers to exact spans of the corresponding saved summaries. A malformed Serbian result does not invalidate a completed Russian edition.

`editorial_locale_inputs` freezes the Russian snapshot and, once available, the Serbian snapshot. Serbian prose retries use that same Russian composition. Retry attempts are bounded and delayed; changing live summaries cannot rewrite frozen inputs. A fresh explicit generation job creates a new edition. Serbian-only retry targets an exact existing bilingual Russian edition, rather than a latest edition for the date or a historical Russian edition.

Russian edition IDs, hashes, rendered messages and source snapshots remain compatible. Serbian editions have their own IDs and delivery receipts, and public links include their locale. The language selector on an immutable edition resolves only its paired locale by the common job/attempt identity. Missing or ambiguous pairs remain explicitly unavailable.

One bot token serves distinct Russian, service and optional Serbian channels. Channel IDs and switches are pinned with each job's settings. Publication attempts the ready Russian edition before waiting for optional Serbian retry work. Each locale retains independent delivery intents and part receipts. An unknown outcome does not authorize an automatic resend. Disabling Serbian delivery does not redirect it to another channel.

## Interface and recovery

Public, admin, login and setup templates share a source-copy catalog; localized source is compiled before user data is inserted. Static and inline client text uses the same catalog. Catalog inventory, placeholders, rendered script syntax and locale behavior are checked in tests. Unknown server errors use localized presentation copy and retain stable machine codes; user-authored data is not translated.

Public feeds and search include only ready Serbian articles in Serbian mode. Original article bodies are explicitly labeled as original and do not inherit a Serbian language assertion. Locale-specific HTML, links and ETags remain separate. Locale preference does not alter sessions, CSRF validation, provider choice, generation or publication settings. Switching with unsaved edits requires confirmation, including controls outside HTML forms.

Migrations 017–019 add storage and expand edition constraints without rewriting Russian history. Managed provisioning/recovery reapplies the checked-in restricted grant policy. Existing external database installations require their reviewed operator upgrade and grants procedure. In particular, the worker needs EXECUTE on `enqueue_serbian_digest_retry()`; it does not need direct access to retry-policy or article tables. Settings exports include localized category labels and Serbian controls; importing them restores both delivery switches disabled. Settings export schema 18 describes the configuration payload; migration 019 adds runtime retry metadata only and does not change that format. Recovery retains support for version 16 settings documents.

Real provider quality and real channel delivery require separate authorization. Offline providers, isolated PostgreSQL and local UI fixtures validate contracts without those operations.
