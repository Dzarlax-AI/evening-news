# Article processing protocol v1

Private Go processor owns article orchestration, extraction, retry and final article/category writes. Python retains provider compute, prompts, selector learning and digest generation. No production rollout is included.

## Runs

Use the existing ingestion run envelope and fencing conventions: version=1, job_id, attempt_generation, owner_id, generation, backend_pid, executor_instance_id, deadline_at. POST /internal/v1/processing/runs starts or replays; GET /internal/v1/processing/runs/{id} polls; POST /internal/v1/processing/runs/{id}/cancel cancels. GET /healthz is observational. ARTICLE_PROCESSING_URL and ARTICLE_PROCESSING_TOKEN configure the executor client.

Derive run ID as ingestion does, with namespace evening-news-processing-v1. Canonical job payload in PostgreSQL determines selection, force, dry_run and limits. Support news_processing, news_digest with run_processing true, article_reprocess and reprocess_failed. Exact envelope replay returns the same receipt; conflicts return 409. A dedicated fenced SQL guard validates these job kinds, active worker and executor receipt. Do not weaken existing ingestion guards.

## Python callback

POST /internal/v1/article-ai uses separate ARTICLE_AI_TOKEN and fixed configured executor origin (never request-provided URLs). Body includes version, full run envelope, run_id, article_id, article_attempt, input_fingerprint, operation and input. Operations: analyze, extraction_context, extraction_feedback. Response echoes run_id, article_id, article_attempt, input_fingerprint and operation, plus result. Reject stale or mismatched active executor/job/item claims before work; validate again before learning writes. Bound bodies at 5 MiB and text by configured character limit. No arbitrary methods, SQL or callbacks.

analyze calls existing AIClient.analyze_article_complete on prepared text, retaining model/prompts/parser. It cannot call get_article_summary_with_metadata or write Article/ArticleCategory. Input title/content/url and optional original_context. Result retains existing normalized AI result shape. extraction_context obtains existing learned selector/method; extraction_feedback records existing extraction memory and stability metadata only. Authentication/stale failures are fatal; ordinary learning failures are bounded best effort. HTTP 429 busy is reserved for admission before provider compute, never provider failures.

### Fixed callback inputs

- `analyze`: exactly `title`, `content`, `url`, with optional `original_context`. Fingerprint is SHA-256 of UTF-8 canonical JSON sorted by key, compact separators, unescaped Unicode and HTML characters, with U+2028/U+2029 escaped as lowercase JSON sequences. Response identity must match every request identity field.
- `extraction_context`: exactly `url`; result carries learned `selector` and `method`. It uses the already persisted item fingerprint and cannot replace the input text.
- `extraction_feedback`: required `url`, `method`, `success`, `observations`; optional `selector_used`, `content_length`, `extraction_time_ms`, `quality_score`, `error_code`. The `observations` array is bounded (at most 64 exact `{selector,method,success}` objects). It uses the existing item fingerprint. Do not send article text or arbitrary extraction response objects. URL must match the claimed article.

A run receipt contains `id`, `job_id`, `attempt_generation`, `state`, `result`, and optional `error_code`. The result preserves the public job-kind-specific status and bounded article outcomes; a completed service run can still report article-level retry or rejection. These must remain visible through durable job status and the admin poller.

## Persistence

Short claim transaction, network and AI outside transactions, short fenced commit. Add article_processing_runs/items. Item claim includes attempt and input fingerprint; final writes and category replacement are atomic and compare claim and article attempt. No stale result can delete categories. Preserve existing partial stages, publication state, busy attempt refund, genuine failure budget and due dates. Unknown provider response delivery does not promise exactly-once billing. Restricted database role cannot write queue ownership, digest data or delete articles. Reprocess initialization remains canonical existing job claim behavior.

## Verification

Synthetic PostgreSQL, fake collector/AI only. Golden fixtures must freeze existing Python behavior before legacy removal. Exercise partial stages, Unicode limits, busy/refund, exhausted, rejected/ad, challenge, forced extraction improvement rules, category mapping, stale fences/cancel/replay, all four job kinds. Build Linux amd64/arm64 and test Mac ARM64. No production, browser, Readeck, real providers or public sites.
