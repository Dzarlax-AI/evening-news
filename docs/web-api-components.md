# Web and API components

This document describes the repository's local development architecture after the Python web application was removed. It groups the system by responsibility; implementation language and current process placement are annotated as implementation details. It is not a production deployment record.

## Responsibilities

| Responsibility | Current implementation | Owns |
|---|---|---|
| Public reader and web API | Go web process | Public HTML pages and data APIs, admin pages, authentication, management routes, API documentation, HTTP trust middleware, and calls to the private executor for the narrow operations it owns |
| Management and durable commands | Go web packages and News PostgreSQL | Source/category CRUD, summaries and statistics reads, settings, durable job submission and receipts |
| Durable job execution | Go worker process | Queue claims, lease/recovery handling, and dispatch of fixed authenticated job operations to the private executor |
| Python domain operations | Private Python executor process | Existing Python job/domain functions, AI and prompts, summary generation, Telegram delivery and diagnostics, and bounded private backup/cache operations |
| Collection and ingestion | Go collection/ingestion process | RSS/Atom, public Telegram and Page Monitor capture, source progress, and fenced persistence of fetched articles |
| Article processing | Go processing process | Article selection, processing states, retries, and article/category persistence; delegates AI analysis through the private executor callback |
| Extraction | Go extractor process | Bounded HTTP extraction and the optional native browser-rendering profile |
| Schedule dispatch | Go scheduler process and its own PostgreSQL database | Calendar schedule state and delivery of versioned commands to the Go web receiver |
| Schema bootstrap | One-shot Python migration command | Existing database initialization, versioned migrations, and required schema checks before schema consumers start |

Current implementation details in parentheses are not service consolidation requirements. The local Compose project runs separate services for the web process, migration command, executor, worker, ingestion, processing, extractor, scheduler, and the News and scheduler databases.

## Request and data flow

```mermaid
flowchart LR
    User[Reader or administrator] --> Web[Reader, admin, and API<br/>(Go web)]
    Web <--> DB[(News PostgreSQL)]
    Web --> Jobs[Durable News jobs]
    Worker[Job execution<br/>(Go worker)] --> Jobs
    Worker -->|fixed authenticated operation| Executor[Private domain operations<br/>(Python executor)]
    Web -->|typed private operation| Executor
    Executor -->|configured-source collection| Ingestion[Collection and ingestion<br/>(Go)]
    Ingestion <--> DB
    Ingestion --> Sources[Source adapters<br/>(Go)]
    Sources --> Extract[Article extraction<br/>(Go)]
    Extract --> Webs[External web origins]
    Processing[Article processing<br/>(Go)] <--> DB
    Processing -->|private AI callback| Executor
    Executor --> AI[AI provider]
    Executor --> Telegram[Telegram delivery]
    Executor <--> DB
    Scheduler[Schedule dispatch<br/>(Go scheduler)] -->|scheduled command| Web
    Migration[One-shot migration command] -->|must succeed before startup| DB
```

The Go web is the only web UI/API implementation in the local example. It owns `/`, public feed pages and APIs, `/admin`, authenticated management endpoints, health endpoints, and the documented route contract. The private executor is a service boundary rather than a second web app: it exposes fixed, authenticated internal operations used by Go components and is not mounted into the public Go mux.

The Go job worker claims durable jobs from News PostgreSQL and sends fixed job names and validated payloads to the executor. The executor uses Python domain code for the operations that remain there. Collection, ingestion, article processing, and extraction are separate responsibilities and may call each other through their service interfaces; this diagram does not require that they share a process.

## Database lifecycle

`migrate` is a one-shot Compose service that runs:

```sh
python -m news_aggregator.migrate
```

It initializes and upgrades the existing News schema, performs required schema checks, and exits. Schema-dependent services wait for successful completion. The Go web checks that the required schema is present at startup and does not apply DDL. The scheduler owns a separate database and schema.

Integration tests use isolated schemas on a dedicated loopback PostgreSQL instance. The repository's migration and readiness checks must not be pointed at production or a shared database during local test runs.

## Private executor connection and secrets

The Go web configuration requires:

| Variable | Purpose |
|---|---|
| `WEB_BACKEND_URL` | Fixed private executor origin, for example `http://executor:8100`; the client constructs only its typed internal paths |
| `WEB_BACKEND_TOKEN` | Separate bearer credential for Go web to private executor calls |

The URL is an origin, not a reverse-proxy target. The Go client does not forward arbitrary user paths or provider credentials. `GEMINI_API_KEY` and `TELEGRAM_TOKEN` are configured only for services that call those providers, principally the private Python executor; they are not required by the Go web service and must not be passed to it.

Other Go web settings include `DATABASE_URL`, `ADMIN_USERNAME`, `ADMIN_PASSWORD`, `JWT_SECRET`, scheduler API and receiver credentials, `ALLOWED_ORIGINS`, `TRUSTED_HOSTS`, `SITE_BASE_URL`, and proxy trust configuration. The complete values and service dependencies are defined by `docker-compose.example.yml`; keep secrets in an ignored local `.env` file or shell environment.

## HTTP ownership

The Go web process composes public pages/data, authentication, admin rendering, management handlers, scheduler facade and receiver, typed private-backend handlers, and local API documentation. The route ledger in [web API route coverage](web-api-route-coverage.md) lists these groups and their service boundaries. `go-web/contracts/openapi.json` is the checked-in API document served by `/openapi.json`; `/docs` and `/redoc` use local rendering and assets.

The private executor listens only on its service interface. Its `/internal/web/v1/*` operations require `WEB_BACKEND_TOKEN` and are not part of the public web route inventory. Its domain operations include AI category analysis, Telegram configuration/test and service-cache refresh, backup-file operations, and process monitoring. Worker-mode backup upload, cleanup, and restore are rejected with 409; durable backup creation remains a Go job.

## Local checks

From the repository root:

```sh
python -m pytest tests -q
cd go-web
go test -race -count=1 ./...
go vet ./...
go mod verify
```

PostgreSQL integration checks require `TEST_DATABASE_URL` to point to a loopback-only disposable test database. Docker acceptance checks use local fixture credentials and disposable data; they do not change production services.

The historical Python route and OpenAPI capture is retained under `tests/fixtures/web-contract-v1/` as migration input. Go route tests compare the assembled mux with that frozen inventory and compare API docs with the checked-in Go OpenAPI contract; they do not import the deleted Python web application.
