# Evening News web/API baseline

This document freezes the Python web behavior observed on 2026-10-26 for the component migration. It describes the running FastAPI registration and current behaviors; it does not endorse every behavior as desirable. The machine-readable route and OpenAPI snapshot is `tests/fixtures/web-contract-v1/registered-routes-openapi.json`.

## Reproduce the registration snapshot

Use the repository Python environment. The capture script injects synthetic import-time credentials only when the caller has not provided them. It imports the app but never enters its lifespan, initializes a database, runs migrations, starts the queue, or calls external services:

```sh
/private/tmp/news-ingestion-venv-20261024/bin/python scripts/capture_web_contract.py
/private/tmp/news-ingestion-venv-20261024/bin/python -m pytest tests/test_web_contract_baseline.py -q
```

The snapshot records 95 path records / 110 method entries with normalized uppercase HTTP methods, OpenAPI path/query/body/response schemas and separately registered framework endpoints (`/docs`, `/redoc`, `/openapi.json`, `/static`). It records `redirect_slashes=False`; slash behavior must be preserved (for example, `/health/` returns 404 while `/health` succeeds). Routes declared inside included routers are flattened in OpenAPI, so the capture merges both inventories. Do not regenerate the fixture simply to accept a changed contract; review the resulting diff and behavior first.

## HTTP response fixtures and live baseline

`registered-routes-openapi.json` is the frozen schema contract. The pytest baseline also exercises actual ASGI responses without lifespan: `GET /health` returns `200 {"status":"healthy"}`, and an anonymous `GET /api/v1/sources` returns `401 {"detail":"Invalid or missing credentials"}` with `WWW-Authenticate: Bearer`. Route-specific API response bodies, validation errors and HTML/redirect output are exposed in the OpenAPI schema where documented; undocumented/error-only behavior must be sampled by the owned endpoint fixture tests before migrating that endpoint.

Current public `/api/public/feed` and `/api/public/article/{article_id}` have known legacy fallback behavior: database read failures and certain absent IDs can be represented as successful mock-shaped responses. Preserve these as observations during differential characterization, but flag them as correctness defects for an explicit product decision; do not silently treat a fallback payload as proof of data existence. Login and other endpoint outcomes are similarly frozen by live capture before implementation, rather than inferred from declarations.

## Route families and ownership

The 116 method entries group as follows; concrete paths and methods remain authoritative in the snapshot.

| Route prefix | Current module | Operation class | Typical persistence/backend boundary |
|---|---|---|---|
| `/`, `/list`, `/search`, `/digest/*`, `/news`, `/feed`, `/cards` | `main.py`, `public.py` | public reads / redirects | templates and DB-backed digest reads; redirects have no write |
| `/api/public/*`, `/api/v1/` | `public.py`, `api/feed_router.py` | public reads; one public reprocess command | DB queue/data service; reprocess delegates to protected article handler |
| `/api/v1/categories*`, `/sources*` | `api/categories_router.py`, `sources_router.py`, `api/__init__.py` | admin reads and CRUD commands | DB/session/service; source test invokes adapter and requires fake network |
| `/api/v1/process/*`, `/summaries/*`, `/articles/*`, `/jobs/*` | corresponding `api/*_router.py` | commands and status/read operations | processing/summary service and durable background jobs |
| `/api/v1/schedule/*`, `/internal/scheduled-jobs` | `api/scheduler_router.py`, `api/scheduled_jobs_router.py` | schedule commands, status/read; trusted ingress | scheduler service API and durable receipt/job persistence |
| `/api/v1/telegram/*` | `api/telegram_router.py` | settings, test and delivery commands | settings DB; Telegram adapter for test/send |
| `/api/v1/backup*`, `/api/v1/backups` | `api/backup_router.py`, alias in `api/__init__.py` | backup reads and filesystem commands | backup service and local files; isolate all mutations in temp dirs |
| `/api/v1/stats/*`, `/system/*` | `api/stats_router.py`, `system_router.py` | monitoring reads and maintenance commands | DB/process monitor; migrations/cleanup are side effects |
| `/auth/*`, `/admin*` | `auth_api.py`, `admin.py` | auth and HTML UI | JWT/cookie plus in-memory rate/lockout state; admin pages render templates |
| `/health*`, `/auth-status`, `/api/public/admin-check` | `main.py` | read-only status | in-memory/config/header/cookie only |
| `/docs*`, `/redoc`, `/openapi.json`, `/static` | FastAPI / StaticFiles | framework UI and asset reads | no application DB |


The route snapshot is the complete method/path inventory. The public surface includes HTML root/list/search/digest pages, redirects, public feed/category/source/search/article APIs, health and auth-status. `/api/v1` includes public feeds plus protected category/source/stats/process/telegram/backup/article/system/scheduler/summary/job APIs and category mappings. Authentication endpoints are `/auth/login`, `/auth/me`, `/auth/refresh`, `/auth/security-status`, `/auth/logout`. Admin pages live under `/admin`; static and generated API documentation are framework routes. `/api/v1/internal/scheduled-jobs` uses a separate scheduled-jobs bearer token, not the ordinary admin dependency. `/internal/v1/*` belongs to the separate executor app and is not mounted by `news_aggregator.main:app`; capture it independently when defining the executor boundary.

## Authentication behavior

- Admin HTML `news_aggregator.auth.get_current_admin_user` accepts trusted `X-authentik-username` only when `TRUST_FORWARD_AUTH=true`, otherwise HTTP Basic credentials; this dependency does not accept the JWT cookie.
- API `news_aggregator.security.get_current_user` checks trusted `X-authentik-username`, JWT Bearer, `admin_token` cookie, then valid HTTP Basic. `require_admin` requires `level == "admin"`; missing/invalid credentials return 401 and a non-admin JWT returns 403.
- `/api/v1` router applies `require_admin` to most management families. Feed endpoints and public APIs have distinct public behavior. Scheduled job ingress validates its dedicated token.
- Anonymous `POST /auth/login` returns 401 `{"detail":"Not authenticated"}`; valid HTTP Basic currently returns 500 `{"detail":"Internal Server Error"}` because `authenticate_admin_basic` returns a username string while `login` dereferences `.username`. It sets no cookie. Treat this as a known defect; do not copy the defect into the new implementation. `/auth/me` accepts valid admin Bearer JWT or `admin_token` cookie and returns username/level/expiry; expired JWT and anonymous calls return 401 `{"detail":"Invalid or missing credentials"}` with Bearer challenge. The legacy Basic fallback authenticates but then returns 500 because the fallback payload has no `exp`, which `/auth/me` dereferences. A valid non-admin JWT can authenticate but is rejected by protected management routes with 403 `{"detail":"Admin access required"}`. `POST /auth/logout` is unauthenticated and returns a success message. The admin HTML dashboard separately sets an `admin_token` cookie after successful HTTP Basic authentication (HttpOnly, SameSite=Lax, Path=/; Secure unless development). Refresh and invalid-token behavior plus CSRF characteristics should be captured before auth replacement. Keep the current distinctions; do not infer cookie mutation safety from OpenAPI.
- Known areas to review separately: `TRUST_FORWARD_AUTH` identity trust, cookie mutation/CSRF, API login response typing, and public mock fallback responses. These are findings, not permission to weaken or silently change the baseline.

## Templates, scripts, and error shapes

HTML templates are rooted at `web/templates`; `main.py` renders `public/list.html` for `/`, and public pages use the public template family. Admin endpoints render templates under `web/templates/admin/`; page navigation is in `admin_base.html`. Static assets are mounted at `/static`. Record the template name, status, `Location`, content type and stable response body for each HTML or redirect route when adding endpoint-level fixtures. FastAPI validation uses its standard 422 JSON body (`detail` entries with `loc`, `msg`, `type`); HTTPException uses `{"detail": ...}`; authentication errors also carry `WWW-Authenticate`. Preserve actual endpoint-specific variants where present.

### Rendered page/template and JavaScript map

| HTTP view | Template / dependencies | Inline behavior or shared assets |
|---|---|---|
| `/`, `/list` | `public/list.html` extends `base.html`; includes `_unified_sidebar.html`, `_filters_toolbar.html`, `_article_modal.html`, `_feed_core.html` | `/static/js/main.js`; inline feed/filter/modal handlers in included templates |
| `/search` | `public/search.html` extends `base.html`; includes article modal and feed core | `/static/js/main.js`; inline search state and request handlers |
| `/digest/{date}` | `public/digest.html` extends `base.html` | `/static/js/main.js` and base theme/navigation behavior |
| `/admin/*` pages | dashboard, sources, summaries, stats, backup, categories, settings, telegram, schedule templates extend `admin/admin_base.html` | `/static/js/main.js`; each page includes inline page-specific requests/actions; stats also uses `/static/js/chart.min.js` |
| all public/admin templates | base/admin base templates | dark mode/navigation and shared styling from `/static/css/main.css`; public articles and admin forms send credentialed requests where their inline script specifies it |

The table identifies concrete source dependencies; it does not claim complete browser acceptance. Current inspection found inline JavaScript rather than separate purpose-specific public/admin bundles (apart from shared `main.js` and Chart.js). CSS, event handlers, referenced API endpoints and failure displays should be included in later UI parity checks.

## Database side effects to preserve and classify

Classify every mutation by the existing handler/service it calls; do not run these operations against real state in baseline tests.

| Operation family | Expected persistence boundary | External side effects / caution |
|---|---|---|
| Public feed, article, category, source/search and digest reads | SQL SELECTs through database queue/service; feed response may use process-local cache | No writes intended; current DB-error fallback can still return 200 |
| Source/category/category-mapping CRUD and toggles | DB insert/update/delete through service/session or queue | Source test may perform outbound fetch; use fake source adapter only |
| Article reprocess, processing run/retry, summary generation | Durable queue/job or processing state mutations; inspect exact handler before parity | AI/extractor work is outside write locks and must be replaced with fakes |
| Telegram settings, test and send-digest | Settings-row write/read; delivery operation | Telegram delivery is external; tests must stub it |
| Backup list/download/upload/sync/restore/cleanup/schedule | Filesystem and/or DB metadata/state | Restore/cleanup are destructive; use temp fixture directories only |
| Schedule settings and scheduled-job ingress | Schedule row or durable occurrence/receipt/job records | Scheduled ingress has dedicated bearer token; no scheduler network calls |
| Admin login/refresh/logout | JWT/cookie state, in-memory lockout counters; typically no SQL write | Cookie flags and response body are part of contract |

The table is a boundary map, not a claim that every listed endpoint currently follows an ideal transactional design. For exact SQL side effects, map each HTTP handler through its service/repository call and record statements/transaction boundaries in endpoint fixtures. Do not execute mutating routes merely to discover them; use fake repositories or isolated synthetic DB schemas.

## Plan step 4 cutover discovery (2026-10-26; read-only)

The checked-in Go OpenAPI document currently has the same 102 path/method operation pairs as the frozen Python OpenAPI schema. That is a useful route inventory comparison, not evidence that all Python regression tests and Docker acceptance probes have moved. The Python registered-route snapshot also records framework/static routes and methods separately; keep `tests/fixtures/web-contract-v1/registered-routes-openapi.json` as historical input after the Python app is removed rather than making the Go server import the legacy app to regenerate it.

### Runtime ownership and removal boundary

| Python module or path | Cutover disposition | Concrete dependency / acceptance condition |
|---|---|---|
| `news_aggregator/main.py`, `public.py`, `admin.py`, `auth_api.py`, `auth.py`, `security.py` | Web runtime candidates for deletion after the Go image and route gate pass | `main.py` currently composes all public/admin/API routes and owns startup migrations, DB initialization, queue/cache warmup, cleanup task and engine shutdown. The API/auth modules are imported by the tests below; no deletion before their behavior has Go evidence. |
| `news_aggregator/api/*_router.py` | Web-only routers may be removed after endpoint parity; preserve shared logic explicitly | `data_service.py` lazily imports `api.feed_router.extract_domain`, `clean_html_entities`, and `clean_summary_text`; move these formatters to a neutral service/module before removing `feed_router.py`. `public.py` imports `api.articles_router.ReprocessRequest` and `reprocess_article`; the Go public reprocess route already exists, so Python wrapper and API dependency can go together after HTTP tests cover its options/auth/202 receipt. `sources_router.py` is not a worker runtime dependency: `source_test` is dispatched by `services/background_execution.py` and `executor.py`; preserve that executor implementation. |
| `news_aggregator/executor.py`, `services/web_backend.py`, models, DB, migrations, job/processing services, extraction/source adapters, `cli.py` | Keep | `executor.py` is a separate FastAPI app; it imports `services.web_backend` and serves authenticated private AI, Telegram configuration/test, backup-file and cache operations. Job execution still needs Python domain/provider implementations. The CLI and migration command are separate entry points. Do not delete the executor because the public web moved to Go. |
| `news_aggregator/migrate.py` and `news_aggregator/migrations/*` | Keep as a one-shot migration image/command | `migrate.py` duplicates the migration registrations and `ensure_*` checks currently run in `main.py` lifespan, and closes the DB engine on exit. Local Compose should run it as a one-shot Python service; Go web and every DB-dependent worker/service should wait for successful completion. Executor must no longer wait for Go web health, and Go web must not run a second migration owner. |
| `web/templates`, `web/static` | Go web owns the checked-in runtime assets; old Python copies are removable only after template/asset parity checks | Go Dockerfile copies Go templates and `web/static`; compare all asset/template references from the Go admin/public templates before removing the legacy tree. Local dev overlay currently bind-mounts `./web` into both `web` and `executor`; that mount should be removed from executor when it no longer serves templates. |

Python requirements are shared between executor, CLI and tests, so deleting the web app does **not** justify removing the framework stack wholesale: `executor.py` imports FastAPI/Starlette request/response/upload APIs and runs under Uvicorn; multipart upload uses `python-multipart`. `Jinja2` is only referenced by the Python web composition/admin templates in current source search and is a candidate after legacy web templates disappear. `PyJWT` and `slowapi` are only used by legacy auth/security paths in current source search and are candidates after callers/tests move. Keep `click`, `rich`, SQLAlchemy/asyncpg, Pydantic, aiohttp, BeautifulSoup, psutil and domain dependencies according to CLI/executor callers. Re-run an import/dependency audit after actual deletion; this discovery did not change `requirements.txt`.

### Existing Python test migration map

| Existing test file / behavior | Go evidence already present | Required before removing Python web imports |
|---|---|---|
| `test_web_contract_baseline.py` | `httpui/docs_test.go` compares frozen route inventory to assembled Go mux; Go HTTP/auth tests cover representative responses | Keep the frozen fixture. Replace its import of `news_aggregator.main`/Python live baseline with fixture-only assertions or a documented historical capture; do not make it fail merely because Python web was intentionally deleted. Preserve differential assertions for health, HTML login, API role/auth/error, slash redirects and response body shapes in Go tests. |
| `test_article_processing_routing.py` | Go jobs validation/acceptance tests, management HTTP command tests, PostgreSQL queue tests | Port all three command cases: processing enqueue kind/payload, single article reprocess `force`/`reextract_media`, and selected retry preserving completed-stage semantics/IDs. |
| `test_background_execution_api.py` | Go management routes/jobs/auth plus backend transport tests | Add explicit checks for worker source-create skips network, source-test missing ID and queue-full statuses, disabled source-test is non-mutating, AI analysis returns 409 before private call, Telegram diagnostic delegates once, public reprocess passes options/auth and returns 202, process cleanup conflict without calling backend, deterministic idempotency-key prefix. These cases currently assert handler/domain details that broad route inventory tests do not prove. |
| `test_background_jobs_postgres.py` | `repositories/jobs_test.go`, `jobs/jobs_test.go`, executor Postgres tests | Keep tests for Python executor/worker invariants (lease/fence, attempt ownership, delivery uncertainty, job SQL). Replace only Python web enqueue/receipt API assertions with Go service/API tests. Do not drop cross-language concurrent producer, pending cap, canonical hash, coalescing and schedule transaction tests. |
| `test_operational_reliability.py` | Go queue route plus private backend adapter tests; Go has no inline Telegram delivery command | Keep operational alert/service tests. The three manual digest tests exercise old embedded-mode immediate send/reservation, while Go web intentionally accepts a durable `telegram_digest` job. Replace with the approved worker-mode HTTP receipt/backpressure contract; do not imply immediate Telegram delivery. |
| `test_public_read_golden.py`, `test_publication_feeds.py` | Go public reader golden and PostgreSQL read integration | Keep synthetic golden data and publication/ad-visibility cases as the cross-language oracle. Move feed formatter assertions away from `api.feed_router`; retain DB error, missing article and unpublished article distinctions, including explicitly approved corrections to mock-200 behavior. |
| `test_scheduled_jobs_api.py` | Go `jobs` unit/PG tests and receiver HTTP tests | Preserve envelope validation, wrong token before DB, dedupe/conflict/replay receipt behavior, worker unavailable with no receipt, API-admin dependency exclusion, strict malformed/non-ASCII token and dispatcher-mode rules. Use Python tests only for executor/DB semantics still owned there. |
| `test_scheduler_service_facade.py` | Go schedules client/root/HTTP tests and management route registration | Port legacy facade owner filtering, status merge (older running vs newer queued), revision conflict, service unavailable/malformed timestamp, create/update semantics independent of follow-up status fetch, timezone and unrelated-consumer isolation. Then remove Python schedule router/facade only if no CLI/service caller remains. |
| `test_web_backend_api.py` | Go backend fixed-path/auth/size/sanitization tests | Keep Python tests because `web_backend.py` is executor-owned; run them under the private executor image. They are not web-app retention evidence. |
| `test_migration_bootstrap.py` | Migration command tests | Keep; Compose one-shot migration should invoke the same bootstrap implementation. Add a Compose graph/startup test asserting migration-success dependency and no web↔executor cycle. |

### Acceptance scripts and local Compose blockers

Current scripts that launch a Python web command are concrete cutover blockers:

| Script | Current Python-web coupling | Needed migration |
|---|---|---|
| `scripts/run_background_worker_fixture_gate.py` | Starts `uvicorn news_aggregator.main:app`; checks Python routes/OpenAPI, hashes Python `news_aggregator` + `web` source from runtime image | Use the candidate Go web image for public/admin/health/API checks; retain a separate Python executor image and its private endpoints. Replace source-hash checks with pinned image/build identity and relevant Go gate checks. |
| `scripts/run_go_job_worker_fixture_gate.py` | Starts Python web; `go_worker_probe.py` is executed inside that container and imports Python DB/worker code for queue receipts, source/job enqueue, dashboard/status and OpenAPI checks | Keep Python probes only in a dedicated fixture helper/executor or replace them with HTTP + `psql` fixture assertions. It must not require Python or repository modules inside the Go image. Preserve its Python lease-contention probe as an explicit cross-language worker compatibility check. |
| `scripts/run_go_ingestion_fixture_gate.py` | Starts Python web and runs `gate_seed.py` / processing probes through it; validates source hash inventory against Python web tree | Run Go web and seed/assert using external fixture Python helper or SQL, leaving ingestion/executor/processing services as their actual owners. Remove source-hash assumption. |
| `scripts/run_go_scheduler_fixture_gate.py` | Starts `uvicorn news_aggregator.main:app`; uses Python `/api/v1/schedule/*` facade and web auth | Replace web image and keep scheduler scenario, auth/API calls and receipt checks against Go scheduler facade. Update fixtures and env for Go web required backend/admin/trust config. |
| `scripts/run_go_web_fixture_gate.py` | Already targets the Go web image and exercises Go HTTP + worker/private executor; subclasses Go ingestion/job gates and uses their helper probes | Natural full acceptance entry point, but inherited helper methods must be disentangled from Python-web assumptions listed above. Avoid maintaining two independent full-pipeline gates. |

CI references these scripts in `.github/workflows/docker-build-push.yml`, `go-scheduler.yml`, `go-article-ingestion.yml`, and the new `web-api.yml`; update path filters, image build dependencies, and gate invocations together with script changes. The release workflow currently treats the Python app image as both `web` and `executor`; split the Go web image from the retained Python executor image explicitly.

`docker-compose.example.yml` currently defines `web` and `executor` from the same Python Dockerfile and shares a large `app-environment` anchor containing required Gemini and Telegram provider keys. Executor waits for `web: service_healthy` because web startup owns migration. The local cutover needs a Go `web` service with minimal web-only configuration (database, admin/JWT, required private-backend URL/token, scheduler receiver/trust settings), and a one-shot Python `migrate` service with only migration DB configuration. Make Go web, executor, worker, ingestion, processing, and any other direct schema consumer wait for `migrate: service_completed_successfully`; remove executor's web-health dependency. Do not pass Gemini/Telegram credentials to Go web; keep provider keys only in executor and other owning services. The dev overlay must drop Python source/template mounts on Go web and should retain them only for Python executor files if live-reload is intentional.

Major blockers before step 4 can be approved as complete: (1) rerun Go full image/API/DB/job/worker/executor fixture gates on the candidate, (2) migrate the five Python web-starting gates and their Python-inside-web probes, (3) keep the listed Python tests' semantic coverage with replacement Go tests while retaining executor/service-level tests, (4) detach formatters and public reprocess types from deleted routers, (5) prove the one-shot migration/Compose dependency graph, (6) verify no provider secret is required by or passed to Go web, and (7) only then remove the Python web modules/templates/dependencies and run the full Python suite for remaining CLI/executor/domain callers.
