# Go web route coverage

This ledger describes the HTTP surface of the local Go web service by responsibility. It does not describe production deployment state. The checked-in OpenAPI document is `go-web/contracts/openapi.json`; it is served at `/openapi.json`, with offline documentation pages at `/docs` and `/redoc`.

## Public reader and status

| Route group | Methods and behavior | Data boundary |
|---|---|---|
| Reader pages | `GET /`, `/list`, `/search`, `/digest/{date}`; legacy `/news`, `/feed`, `/cards` redirect to `/` | Public templates and read-only repository operations |
| Public API | `GET /api/public/feed`, `/article/{articleID}`, `/categories/config`, `/categories`, `/sources`, `/search` | Public read repository; missing articles return 404 and unavailable database reads return 503 |
| Versioned public feed | `GET /api/v1/`, `/api/v1/feed`, `/api/v1/feed.rss` | Public read repository and RSS formatter |
| Health | `GET /health`, `/health/app`, `/api/v1/system/health/db` | Process status and a bounded database health query |
| Static and docs | `GET /static/*`, `/openapi.json`, `/docs`, `/docs/oauth2-redirect`, `/redoc` | Files constrained to the configured static root; checked-in OpenAPI and local documentation rendering |

## Authentication and admin pages

Auth and small public status routes are registered by the auth component: `POST /auth/login`, `GET /auth/me`, `POST /auth/refresh`, `GET /auth/security-status`, `POST /auth/logout`, `GET /auth-status`, and `GET /api/public/admin-check`.

Admin HTML pages are served under `/admin`, including dashboard, sources, summaries, stats, backup, categories, and settings. The managed `/admin/backup` page contains operator guidance and no upload/restore/autobackup controls; manual profiles retain the existing page. The backup API remains a partial application-data export, not the operator settings bundle. HTML access accepts configured Basic authentication or trusted ForwardAuth according to the local web configuration. API auth accepts configured Bearer, admin-cookie, or Basic credentials, plus trusted identity where enabled. Cookie-authenticated mutations use the HTTP middleware's origin checks. See the auth package tests for exact challenge, role, cookie, lockout, and cross-site behavior.

## Management routes

Management APIs require an API admin principal. CRUD operations write through parameterized repositories; asynchronous commands create durable News jobs rather than performing long-running work inside the HTTP handler.

| Route group | Methods and behavior | Persistence or command boundary |
|---|---|---|
| Sources | `GET/POST /api/v1/sources`, `PUT /{sourceID}`, `PATCH /{sourceID}/toggle`, `DELETE /{sourceID}`, `POST /{sourceID}/test` | Source CRUD; deletion refuses attached articles unless explicitly requested; source test queues a durable job |
| Categories and mappings | `GET/POST /api/v1/categories[/]`, `PUT /{categoryID}`, mapping CRUD/toggle/fixed/unmapped routes under `/api/v1/categories/mappings`, plus `/api/v1/category-mappings` compatibility aliases | Category and mapping transactions; mapping writes call the private cache invalidation operation after commit |
| Articles | `GET /api/v1/articles/{articleID}`, `POST /api/v1/articles/{articleID}/reprocess` | Article read and durable `article_reprocess` job |
| Processing | `POST /api/v1/process/run`, `GET /api/v1/process/status`, `POST /api/v1/process/retry` | Durable processing jobs and worker state |
| Summaries | `GET /api/v1/summaries/daily`, `/categories`, `/{summaryID}`, `POST /api/v1/summaries/generate` | Summary reads and durable generation job |
| Jobs | `GET /api/v1/jobs/{jobID}` | Durable job receipt and worker availability |
| Statistics | `GET /api/v1/stats/dashboard`, `/queue`, `/ai-usage`, `/processing`, `/extractor` | PostgreSQL aggregates and worker/job status |
| Telegram settings and digest | `GET/POST /api/v1/telegram/settings`, `POST /api/v1/telegram/send-digest` | Persist sanitized settings metadata; notify executor after commit; queue `telegram_digest` job |
| Category approval | `POST /api/v1/categories/ai-category-mapping/approve` | Upsert/reactivate mapping and refresh executor cache |
| Schedule facade | `GET/POST /api/v1/schedule/settings`, `PUT/DELETE /api/v1/schedule/settings/{task_name}`, `GET /api/v1/schedule/status` | Authenticated facade to the scheduler service |
| System | `GET /api/v1/system/info` | Go runtime/build/disk information |
| Backup creation | `POST /api/v1/backup`, `/api/v1/backup/sync` | Both queue a durable `backup` job |

## Private-backend operations exposed through the web API

These public management routes require API admin authorization and call an explicit typed operation on the private backend client. The client has a fixed executor origin, a separate service bearer token, bounded request/response sizes, and no automatic retries for mutation operations.

| Public route | Private operation | Behavior |
|---|---|---|
| `POST /api/v1/categories/ai-category-analysis` | Category analysis | Worker mode returns the executor's 409 rather than calling AI from the web process |
| `POST /api/v1/telegram/test` | Telegram diagnostic | Calls the executor-owned provider integration |
| `GET /api/v1/backup`, `/api/v1/backups` | Backup list | Returns filename, exact byte size, admin display fields, and timestamp |
| `GET /api/v1/backup/download/{filename}` | Backup download | Streams a bounded known-length file; unsafe names are rejected before backend access |
| `POST /api/v1/backup/upload` | Backup upload | Multipart is size-bounded and streamed; disabled with 409 in worker mode |
| `POST /api/v1/backup/restore` | Backup restore | Disabled with 409 in worker mode; executor does not invent archive extraction behavior |
| `POST /api/v1/backup/cleanup` | Backup cleanup | Disabled with 409 in worker mode |
| `GET/POST /api/v1/backup/schedule` | Backup schedule | Reads or updates the existing schedule response contract |
| `GET /api/v1/system/process-monitor` | Process status | Returns process-monitor/worker status from the executor boundary |
| `POST /api/v1/system/process-monitor/cleanup` | Process cleanup | Returns 409 because browser process cleanup belongs to the Go collector |
| `POST /api/v1/system/migrations/run` | Migration operation | Delegates through the restricted private operation |

The private Python executor uses `/internal/web/v1/*` routes and `WEB_BACKEND_TOKEN`. These are service-to-service operations, not public routes and not a generic reverse proxy. The executor also retains fixed job operations for AI and delivery. No Gemini or Telegram provider credential is required by the Go web process.

## Separate command endpoints

| Route | Access and contract | Responsibility |
|---|---|---|
| `POST /api/public/article/{articleID}/reprocess` | Public-admin predicate; queues reprocess command | Public reprocess entry point |
| `POST /api/v1/internal/scheduled-jobs` | Dedicated bearer token; bounded strict JSON envelope; returns durable occurrence receipt | Scheduler-to-web dispatch receiver |

The scheduled-job receiver returns 202 for a new receipt, 200 for an exact duplicate, 409 for conflicting occurrence content, 422 for invalid envelopes, and 503 with `Retry-After` for worker/queue backpressure. It is not protected by the browser/API admin dependency; it uses its own service token.

## Local configuration and verification

`DATABASE_URL`, `ADMIN_PASSWORD`, `WEB_BACKEND_URL`, and `WEB_BACKEND_TOKEN` are required for the Go web. The private backend URL is an origin only, such as `http://executor:8100`; the client appends fixed operation paths. Other settings cover admin identity/JWT, scheduler client and receiver tokens, allowed origins, trusted hosts, site URL, and trusted proxy CIDRs. Provider credentials are configured only for provider-owning services.

The one-shot `migrate` service runs `/usr/local/bin/news-migrate` and must complete successfully before database consumers start. The Go web verifies the expected schema at startup and performs no DDL.

Run web tests from the repository root:

```sh
cd go-web
go test ./internal/backend ./internal/auth ./internal/httpui ./internal/jobs ./internal/management ./internal/repositories ./internal/schedules
```

`httpui.TestFrozenRouteInventoryMatchesAssembledMux` sends requests through the assembled handlers and compares matched `Request.Pattern` values with the retained Python registration inventory. It normalizes route parameter names and checks method handling; it is not a static list comparison. PostgreSQL repository integration tests skip without `TEST_DATABASE_URL` and refuse non-loopback hosts. They create disposable schemas and must not use a production or shared database.
