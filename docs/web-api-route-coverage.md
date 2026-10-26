# Go web API route coverage

This ledger records the HTTP surface wired by the Go web process. Durable jobs own asynchronous operations; the private backend owns provider, backup-file, and process-monitor operations. Routes fail closed when their required dependency is unavailable.

## Management routes

All `/api/v1` management routes below require an API admin principal. Mutations also use the API middleware's cross-site origin check for cookie-authenticated requests.

| Route group | Methods and behavior | Persistence/commands | Verification |
|---|---|---|---|
| Sources | `GET/POST /api/v1/sources`, `PUT /{sourceID}`, `PATCH /{sourceID}/toggle`, `DELETE /{sourceID}`, `POST /{sourceID}/test` | Parameterized source CRUD; source deletion refuses attached articles unless `delete_articles=true`; test submits durable `source_test` job | `repositories.TestPostgresManagementSourceCategoryArticleAndSummaryContracts`; HTTP admin gate test |
| Categories | `GET /api/v1/categories/`, `POST /api/v1/categories[/]`, `PUT /{categoryID}` | Category CRUD and local-category constraint in transaction | PostgreSQL management contract test |
| Category mappings | Canonical `/api/v1/categories/mappings` CRUD/toggle/fixed/unmapped routes, plus legacy `/api/v1/category-mappings` aliases | Mapping transaction plus injected cache-change notifier; mapping writes report adapter failure if notifier is unavailable | PostgreSQL management contract test |
| Articles | `GET /api/v1/articles/{articleID}`, `POST /api/v1/articles/{articleID}/reprocess` | Article detail query; reprocess enqueues durable `article_reprocess` job | PostgreSQL article-detail assertion; shared queue tests |
| Processing | `POST /api/v1/process/run`, `GET /api/v1/process/status`, `POST /api/v1/process/retry` | Durable queue submissions and worker status query | Shared queue tests; PostgreSQL worker-status assertion |
| Summaries | `GET /api/v1/summaries/daily`, `/categories`, `/{summaryID}`, `POST /generate` | Read summaries and enqueue durable generation command | PostgreSQL summary read assertions; shared queue tests |
| Jobs | `GET /api/v1/jobs/{jobID}` | Read durable job receipt and worker availability | Shared queue tests |
| Dashboard and statistics | `GET /api/v1/stats/dashboard?days=`, `/queue`, `/ai-usage?days=`, `/processing?days=`, `/extractor` | PostgreSQL aggregates over articles, processing stats, AI usage, extraction attempts/domain stability, worker status and durable jobs | `repositories.TestPostgresManagementSourceCategoryArticleAndSummaryContracts`; malformed-query auth-order test |
| Telegram settings and digest | `GET/POST /api/v1/telegram/settings`, `POST /api/v1/telegram/send-digest` | Settings live in `settings`; response metadata comes from a secret-free private backend operation; a successful settings write triggers one cache-refresh request and reports a warning if that refresh fails; digest submits `telegram_digest` job | Backend client tests; queue tests; HTTP auth gate |
| Category approval | `POST /api/v1/categories/ai-category-mapping/approve` | Upsert/reactivate durable category mapping, count impacted articles, then refresh private category cache | PostgreSQL management contract test; backend client invalidation test |
| System | `GET /api/v1/system/health/db`, `GET /api/v1/system/info` | Live pool query/metrics and Go runtime/build/disk data | PostgreSQL management contract test |
| Backup creation | `POST /api/v1/backup`, `POST /api/v1/backup/sync` | Both submit durable `backup` jobs in worker mode | Shared queue tests |
| API documentation | `GET/HEAD /openapi.json`, `/docs`, `/docs/oauth2-redirect`, `/redoc` | Serves the checked-in OpenAPI contract and local docs assets | `httpui` docs-handler tests |

The private backend registration owns `POST /api/v1/categories/ai-category-analysis`, `POST /api/v1/telegram/test`, backup listing/download/upload/restore/cleanup/schedule, system process-monitor/cleanup, and migrations. These handlers use the bounded private backend client with a separate service token.

The Go web process requires `DATABASE_URL`, `ADMIN_PASSWORD`, `WEB_BACKEND_URL`, and the separate `WEB_BACKEND_TOKEN`. It reads scheduler receiver settings from `SCHEDULED_JOBS_TOKEN`, `SCHEDULED_JOBS_RECEIPT_LIMIT`, `BACKGROUND_EXECUTION_MODE=worker`, and `SCHEDULE_DISPATCH_MODE=service`. `WEB_BACKEND_URL` is the executor origin only (for example `http://executor:8100`); the typed client adds its fixed private route prefix. It never reads Telegram provider tokens. `ALLOWED_ORIGINS`, `TRUSTED_HOSTS`, `SITE_BASE_URL`, and `TRUSTED_PROXY_CIDRS` configure the outer HTTP trust boundary; trusted proxy CIDRs are required when `TRUST_FORWARD_AUTH` is enabled.

## Separate command endpoints

| Route | Access and contract | Verification |
|---|---|---|
| `POST /api/public/article/{articleID}/reprocess` | Uses the injected public-admin predicate; queues durable reprocess command | Covered by the public API lane |
| `POST /api/v1/internal/scheduled-jobs` | Exact bearer token, JSON envelope no larger than 8 KiB, unknown/duplicate keys rejected; returns 202 for a new receipt and 200 for a duplicate, 409 for conflicting occurrence content, 422 for invalid envelopes, and 503/`Retry-After` for worker or queue backpressure | `TestScheduledJobsReceiverRejectsBadEnvelopeBeforeService`; shared scheduled job tests |

All routes listed here are behind the API-admin middleware except the internal receiver, which uses its service bearer token, and the public-admin article reprocess route. The HTML admin pages continue to use Basic or trusted ForwardAuth according to the Python baseline; an API cookie does not grant HTML admin access.

## Local verification

Run unit and HTTP tests with the repository's Go module:

```sh
cd go-web
go test ./internal/management ./internal/repositories ./internal/httpui
```

The repository integration test skips unless `TEST_DATABASE_URL` points at a loopback PostgreSQL database. It creates and drops a unique schema and tests source deletion side effects, category local state, mapping upsert/toggle, article detail, summaries, stats aggregates, worker status, and scheduler latest/running/last-success metadata. It must not be pointed at production or shared databases.
