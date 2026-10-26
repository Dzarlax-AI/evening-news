# Evening News

Evening News collects stories from RSS/Atom feeds, public Telegram channels, and monitored web pages. It extracts article text, filters low-quality content, generates AI summaries and categories, and publishes daily digests to Telegram and the public reader.

**Live site:** [news.dzarlax.dev](https://news.dzarlax.dev)

## What it does

1. Collects articles from configured feeds, Telegram channels, and custom pages.
2. Extracts full text from linked articles when a source provides only a snippet.
3. Filters duplicate, short, and boilerplate content before article analysis.
4. Summarizes and categorizes articles, then creates daily summaries by category.
5. Publishes digests to Telegram and links to the public site.

## Architecture

The local development stack is organized around product responsibilities. Go and Python are implementation details shown in parentheses; they are not the architectural boundaries. The diagram describes logical responsibilities and data flow, not a prescription to combine services or containers.

```mermaid
flowchart LR
    Reader[Public readers and administrators]
    Web[Web reader, admin UI, and API<br/>(Go web process)]
    NewsDB[(News PostgreSQL)]
    Jobs[Durable job queue<br/>(in News PostgreSQL)]
    Worker[Job execution<br/>(Go worker process)]
    Domain[Private News operations<br/>(Python executor process)]
    AI[AI analysis and prompts<br/>(private executor domain code)]
    Delivery[Daily summaries and Telegram delivery<br/>(private executor domain code)]
    Ingestion[Configured-source collection and persistence<br/>(Go ingestion process)]
    Sources[RSS, Telegram, and Page Monitor adapters<br/>(Go collection code)]
    Processing[Article processing and state transitions<br/>(Go processing process)]
    Extract[Article extraction<br/>(Go extraction process)]
    Scheduler[Schedule management and dispatch<br/>(Go scheduler process + scheduler database)]
    Migrate[One-shot schema bootstrap<br/>(Python migration command)]
    Internet[Public source websites]
    Telegram[Telegram channel]

    Reader --> Web
    Web <--> NewsDB
    Web --> Jobs
    Worker --> Jobs
    Worker -->|authenticated fixed job operations| Domain
    Domain -->|source and article commands| Ingestion
    Ingestion --> Sources
    Ingestion <--> NewsDB
    Sources --> Extract
    Extract --> Internet
    Processing <--> NewsDB
    Processing --> Extract
    Processing -->|private AI callback| AI
    Domain --> AI
    Domain --> Delivery
    Delivery --> Telegram
    Delivery <--> NewsDB
    Web -->|typed private operations| Domain
    Web <--> Scheduler
    Scheduler -->|scheduled command| Web
    Migrate -->|successful completion required| NewsDB
```

The Go web service replaces the former Python web application. It serves the public reader, admin pages, health endpoints, and public and management APIs. The private Python executor remains responsible for fixed job operations that use Python domain code, AI prompts and provider calls, summaries, and Telegram delivery. Go owns durable queue execution, collection and ingestion, article processing, extraction, and schedule dispatch in this local stack.

The one-shot `migrate` service runs `python -m news_aggregator.migrate` before schema-dependent services start. The Go web service does not apply migrations. `WEB_BACKEND_URL` names only the private executor origin, such as `http://executor:8100`; the Go client appends its fixed operation paths. `WEB_BACKEND_TOKEN` is a separate service credential shared only by the Go web process and private executor. Provider credentials such as `GEMINI_API_KEY` and `TELEGRAM_TOKEN` belong to the executor and are not required by the Go web service.

## Admin panel

The admin UI is served at `/admin`. It provides source and category management, summaries and statistics, Telegram settings, schedule settings, and backup operations. Commands that run asynchronously are submitted to the durable job queue and report a job receipt. In worker mode, offline backup mutations such as upload, cleanup, and restore are disabled by the private executor.

## Public reader and API

The public reader is available at `/` and `/list`, with search at `/search` and daily digests at `/digest/YYYY-MM-DD`. It includes filtering by category and source, dark mode, and a responsive article view.

Public data endpoints live under `/api/public/`; the authenticated management API lives under `/api/v1/`. The route ledger is in [Go web route coverage](docs/web-api-route-coverage.md), and the frozen historical Python route/schema snapshot remains under `tests/fixtures/web-contract-v1/` for migration tests. The public OpenAPI contract is `go-web/contracts/openapi.json` and is served at `/openapi.json` with local documentation at `/docs` and `/redoc`.

## Local development

The example Compose stack starts the Go web service, a one-shot Python schema migration command, the private Python executor, the Go job worker, the Go ingestion and processing services, the extractor, the scheduler, and separate PostgreSQL instances for News and scheduler data. These are distinct Compose services today; their deployment boundaries are not implied by the logical diagram above. The minimal profile uses HTTP-only extraction and does not include browser rendering.

Create an ignored `.env` file or export the required local credentials before starting Compose. Use development-only values:

```dotenv
DB_PASSWORD=local-news-db-password
SCHEDULER_DB_PASSWORD=local-scheduler-db-password
ADMIN_PASSWORD=local-admin-password
JWT_SECRET=local-jwt-signing-secret
WEB_BACKEND_TOKEN=local-web-executor-token
JOB_EXECUTOR_TOKEN=local-job-executor-token
ARTICLE_AI_TOKEN=local-article-ai-token
ARTICLE_PROCESSING_TOKEN=local-processing-token
ARTICLE_INGESTION_TOKEN=local-ingestion-token
EXTRACTION_SERVICE_TOKEN=local-extraction-token
SCHEDULED_JOBS_TOKEN=local-scheduler-receiver-token
SCHEDULER_API_TOKEN=local-scheduler-api-token
GEMINI_API_KEY=your-development-provider-key
TELEGRAM_TOKEN=your-development-bot-token
TELEGRAM_CHAT_ID=your-development-channel-id
```

Provider keys are passed to the Python executor because it owns provider operations. The Go web service receives the database URL, admin/JWT settings, `WEB_BACKEND_URL` and `WEB_BACKEND_TOKEN`, scheduler client settings, and HTTP trust configuration; it does not receive AI or Telegram provider keys.

Start the stack:

```sh
docker compose -f docker-compose.example.yml up --build -d
```

The app is available at `http://localhost:8000`. The `migrate` service exits after its one-shot bootstrap; inspect its logs if schema-dependent services do not start. The dev overlay is optional:

```sh
docker compose -f docker-compose.example.yml -f docker-compose.dev.yml up --build -d
```

## Tests

Run the Python domain/executor suite and Go checks from the repository root:

```sh
TEST_DATABASE_URL=postgresql+asyncpg://fixture_admin:fixture-only@127.0.0.1:5432/webtests python -m pytest tests -q
cd go-web
TEST_DATABASE_URL=postgres://fixture_admin:fixture-only@127.0.0.1:5432/webtests go test -race -count=1 ./...
go vet ./...
go mod verify
```

PostgreSQL integration tests require `TEST_DATABASE_URL` to point to a dedicated loopback test database. Do not use a production or shared database. Docker acceptance scripts exercise the local multi-service path and use disposable fixtures.

## Configuration and documentation

The Compose example is the source of truth for the complete local service configuration. The Go web environment includes `DATABASE_URL`, `ADMIN_USERNAME`, `ADMIN_PASSWORD`, `JWT_SECRET`, `WEB_BACKEND_URL`, `WEB_BACKEND_TOKEN`, scheduler receiver settings, and HTTP trust settings. `WEB_BACKEND_URL` must be a fixed executor origin; the client does not accept arbitrary backend paths or URLs.

English technical guides:

- [Web/API components](docs/web-api-components.md)
- [Go web route coverage](docs/web-api-route-coverage.md)
- [Article ingestion](docs/go-article-ingestion.md)
- [Article processing](docs/go-article-processing.md)
- [Go scheduler](docs/go-scheduler.md)
- [Source collection](docs/go-source-manager.md)
- [Telegram collection](docs/go-telegram-source.md)
- [Page Monitor](docs/go-page-monitor.md)
- [Native browser worker](docs/go-cloak-native-worker.md)

This documentation describes the repository's local development stack. It does not claim that production has been upgraded or switched to this topology.
