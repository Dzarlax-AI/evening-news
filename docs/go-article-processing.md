# Go article processing

> Historical migration context: the Python executor and Unix child described below predate the current Go domain candidate. For its runtime and unfinished acceptance, see [Private Go domain runtime](go-domain-runtime.md).

The processing module and private HTTP contract remain in place. The current
local Compose architecture runs it as a component of the single Go
`background` runtime, not as a separate service container. See [Core image
build and release](core-image-build-and-release.md) for the packaging and
release gate. This source consolidation has not been published or rolled out.

This migration introduced a private Go processing component for article selection, extraction decisions, bounded processing, retry state, and atomic article/category persistence. The Python executor retains job composition, AI compute and prompts, selector learning, daily summaries, and delivery. The component was deployed separately in production on 26 October 2026 from PR #28; the release record is historical and is not evidence that this background consolidation was deployed. The first scheduled collection covered all eight sources; after correcting the Python callback fence grant, both newly collected articles completed summarization and categorization through Go. Existing worker heartbeat timeouts remain a separate unresolved issue; the earlier rollout did not claim to fix them.

## Request flow

The Go job worker owns the durable job and worker lease. It sends the fixed job attempt to the Python executor. After Go ingestion completes, the executor starts a processing run and polls its receipt. The processor reads canonical job options from PostgreSQL, claims due articles in bounded batches, calls the existing Go extraction service and narrow Python AI bridge, then commits results under the current worker/executor/item fence. AI and extraction never run inside a database transaction.

Supported jobs are `news_processing`, `news_digest` with processing enabled, `article_reprocess`, and `reprocess_failed`. Existing article retry and publication states remain visible to the web/API. Normal cycles retain aggregate success/warning behavior; selected reprocessing retains per-article states. Normal run receipts keep at most 20 outcome samples with accurate total/omitted counts, and legacy error text is projected to finite reason codes without changing the stored article error. Busy admission refunds its attempt; genuine failures retain the finite retry budget. Successful partial stages survive a retry, including reclaimed forced reprocessing. Existing inline HTML media is parsed only after the source text passes its resource bound; stored media and already-completed summaries are preserved. Dry-run candidate inspection does not recover or modify article/claim state. Replaying a run returns its existing receipt, while a conflicting or stale fence is rejected.

See the [versioned internal contract](contracts/article-processing-v1.md) for envelopes and callback identity. AI provider delivery remains potentially ambiguous after a connection loss; durable fencing prevents stale article/category writes, but cannot guarantee exactly-once provider billing.

## Local setup

The [example Compose stack](../docker-compose.example.yml) includes the processing listener on `background:8094`. Supply distinct `ARTICLE_PROCESSING_TOKEN` and `ARTICLE_AI_TOKEN` secrets. Python needs `ARTICLE_PROCESSING_URL=http://background:8094` and the processing token. The background component needs its restricted `PROCESSING_DATABASE_URL`, fixed `ARTICLE_AI_URL` for the Python executor, the AI token, and existing extraction service URL/token. Never give it the general job executor token or AI provider credentials. The one-shot migration completes before processing readiness is required; background waits for executor and extractor health without creating an executor/background dependency cycle.

Processing concurrency, batch size, retry policy, date filters and source/cycle limits must match the reviewed deployment configuration. Go owns these settings after cutover; setting them only on the Python executor is insufficient. Text limits count Unicode characters rather than UTF-8 bytes. The private service does not connect to CDP or manage a browser.

## Database and deployment

The additive migration installs `article_processing_runs`, `article_processing_items`, and a dedicated SQL fence. Preserve receipt history. Pass the actual schema and the existing Python application database role explicitly when running [the reviewed SQL template](../deploy/news-processing/provision-runtime-role.sql), for example `psql -v news_schema=<schema> -v news_callback_role=<python-role> -f deploy/news-processing/provision-runtime-role.sql`. `news_callback_role` is required and must name an existing role; the template checks it before changing roles and grants that role only `EXECUTE` on the fence function. Use a matching `search_path` in the service database URL. The Go runtime role must not own queue tables or have broad article deletion privileges. The [deployment template](../deploy/news-processing/compose.processing.yml) exposes no host ports or mounts and uses a read-only filesystem. Its memory budget requires synthetic acceptance evidence before a separate production approval.

The Python AI callback authorizes the active executor job and article claim and validates the bounded input fingerprint. It exposes fixed analyze/context/feedback operations only. Provider prompts and parsing remain in Python. Learning feedback may update extraction metadata, never Article or ArticleCategory.

## Verification and rollback

The [frozen synthetic corpus](../tests/fixtures/article-processing-v1/README.md) records previous Python outcomes. Run Python client/callback/migration tests, Go unit/race/PostgreSQL checks, and Linux amd64/arm64 builds. The isolated ingestion fixture gate now includes the real Go processor and fake AI endpoints; it checks processing receipts as well as ingestion receipts, resource use, and restart counters. No real sites, providers, messaging, or production database belong in these gates.

A later rollout must separately approve migration, restricted-role provisioning, the exact reviewed image digest, and application configuration. Before rollback to the previous Python runtime, stop or fence Go processing writers and active attempts. Keep article data and receipts; do not reset retries or restore the database. Browser and other shared clients are outside this migration.
