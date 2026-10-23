# Evening News

A self-hosted news digest service that collects articles from RSS feeds, Telegram channels, and custom sources — then uses AI to summarize, categorize, and deliver a daily digest to your Telegram channel.

**Live demo:** [news.dzarlax.dev](https://news.dzarlax.dev)

---

## What it does

1. **Collects** news from your configured sources throughout the day — RSS feeds, public Telegram channels, and arbitrary web pages with no RSS.
2. **Extracts** full article text when a source only provides a snippet, following links and parsing the destination page.
3. **Filters** extracted content — rejects spam, navigation boilerplate, and duplicates before sending anything to AI.
4. **Summarizes** each article with AI (Google Gemini), extracting the key points in a consistent format.
5. **Categorizes** articles automatically. The more you correct the AI via the admin panel, the better it gets over time — corrections accumulate as examples and are injected into future prompts.
6. **Generates** a per-category daily summary across all collected articles.
7. **Publishes** a digest to your Telegram channel as a Rich Message, with a first-party full version available on the public site.

Everything runs on a schedule you control, or you can trigger any step manually from the admin panel.

---

## Architecture

The deployed application uses the standalone Go scheduler for calendar timing and command dispatch. Python retains source configuration, News PostgreSQL persistence, selector memory, durable job execution, AI processing, and digest delivery. A separate private Go collection service handles SourceManager dispatch, RSS/Atom collection, Telegram channel capture/parsing, Page Monitor capture/parsing, and article extraction. Python calls its authenticated API and saves the results; the collection service does not own the database or send digests.

The Python web app and [separate background worker](docs/background-worker.md) retain News PostgreSQL persistence, selector memory, durable job execution, AI processing, and digest delivery. Scheduled commands are accepted by the app into its durable job queue and executed by the worker. `BACKGROUND_EXECUTION_MODE=worker` is required for scheduled work; `embedded` remains available for manual execution paths.

The [standalone Go scheduler](docs/go-scheduler.md) owns calendar timing and dispatch, with its own PostgreSQL schema and versioned HTTP command/receipt contract. It is deployed separately from the News application. The News API exposes schedule configuration through a thin authenticated Go-service facade, and the app accepts dispatched commands into its durable queue. The scheduler is independently packaged and does not depend on article, AI, or delivery models.

The Go collection service owns RSS/Atom, Telegram, Page Monitor capture and parsing, and article extraction. The app retains source CRUD/configuration and source-specific durable state. Browser rendering is available only in the native Go deployment profile; the minimal local Compose example intentionally uses HTTP-only extraction and Page Monitor capture.

```mermaid
flowchart TD
    subgraph sources["Sources"]
        RSS[RSS / Atom feeds]
        TGS[Public Telegram channels]
        WEB[Custom pages]
    end

    subgraph app["Python web + execution worker"]
        Adapter[Scheduled command adapter]
        Jobs[Durable News jobs]
        SourceClient[Source configuration + Go API client]
        Memory[Page Monitor state + selector learning]
        DB[(News PostgreSQL schema)]
        Proc[Quality checks + article processing]
        AI[Google Gemini]
        Digest[Daily summaries + digest delivery]
        UI[FastAPI admin + public site]
    end

    subgraph scheduling["Standalone Go scheduler — production service"]
        GoScheduler[Calendar + command dispatch]
        ScheduleDB[(Scheduler-owned PostgreSQL schema)]
        GoScheduler <--> ScheduleDB
    end

    subgraph go["Private Go collection service — no database"]
        GoCollect[SourceManager<br/>up to 5 source jobs]
        GoRSS[Guarded RSS / Atom collection]
        GoTelegram[Telegram HTTP + parsing]
        GoPage[Page Monitor capture + parsing]
        GoArticle[Article extraction]
        Admission[Native render admission<br/>production: 2 active, up to 8 waiting]
    end

    subgraph browser["Shared browser infrastructure"]
        Cloak[One Cloak browser<br/>owned context + target per render]
        Egress[Private HTTP/S egress gateway]
    end

    Channel[Your Telegram channel]
    Origins[Public websites]

    UI -->|schedule CRUD / status| GoScheduler
    UI -->|manual commands| Jobs
    GoScheduler -->|private authenticated command + receipt| Adapter
    Adapter --> Jobs
    Jobs --> SourceClient
    SourceClient <--> Memory
    SourceClient <-->|private authenticated API| GoCollect
    RSS --> GoRSS
    TGS --> GoTelegram
    WEB --> GoPage
    GoCollect --> GoRSS
    GoCollect --> GoTelegram
    GoCollect --> GoPage
    GoRSS -->|guarded HTTP| Origins
    GoTelegram -->|guarded HTTP first| Origins
    GoPage -->|guarded HTTP when configured| Origins
    SourceClient -->|save collected articles| DB
    DB --> Proc
    Proc <-->|private authenticated API| GoArticle
    GoArticle -->|guarded HTTP| Origins
    GoArticle -->|when rendering is needed| Admission
    GoTelegram -->|eligible browser fallback| Admission
    GoPage -->|when rendering is configured| Admission
    Admission -->|CDP control| Cloak
    Cloak -->|browser HTTP/S| Egress
    Egress --> Origins
    Proc <--> AI
    Proc -->|save processed articles| DB
    Jobs --> Digest
    DB --> Digest
    Digest <--> AI
    Digest -->|save summaries| DB
    Digest --> Channel
    DB --> UI
```

This diagram shows the runtime ownership boundary: Go owns collection, extraction, scheduling, and dispatch; Python owns News persistence, durable work execution, article processing, and delivery. See the [scheduler release record](deploy/news-scheduler/RELEASE_2026-10-23.md) for deployment verification.

See the [source collection guide](docs/go-source-manager.md), [Telegram guide](docs/go-telegram-source.md), [Page Monitor guide](docs/go-page-monitor.md), [native worker guide](docs/go-cloak-native-worker.md), and [deployment handoff](deploy/news-extractor/HANDOFF.md) for contracts, limits, and deployment boundaries.

---

## Admin panel

A web UI at `/admin` lets you manage everything without touching config files:

- **Dashboard** — today's articles, processing status, quick stats
- **Sources** — add/edit/remove RSS feeds, Telegram channels, and custom pages
- **Schedule** — configure when fetching, processing, and digest sending happen; trigger any step manually
- **Summaries** — browse AI-generated daily summaries by category and date
- **Categories** — manage categories; correct AI mistakes — corrections become training examples for future runs
- **Telegram** — configure delivery channels without restarting the app; send a test message
- **Backup** — create and restore database backups
- **Stats** — article counts, source activity, AI token usage and cost per run

---

## Public feed

The app serves a public news reader at `/` (no login required):

- **List view** (`/`) — the only view, with sidebar filtering by category and source, sticky toolbar with search and time period filters
- **Search** (`/search`) — full-text search across collected articles
- **Daily digest** (`/digest/YYYY-MM-DD`) — a stable full-version page linked from Telegram digests
- **Article modal** — click any article to read its AI summary and full text in a modal

Design follows the [dzarlax.dev](https://dzarlax.dev) brand system: warm ivory background, Georgia serif headings, dark graphite accents, full dark mode support. Mobile-optimized with compact cards and horizontal category chips.

---

## Sources

**RSS / Atom** — feed URLs are collected by the guarded Go collector. When the feed provides only a snippet, the full article is fetched automatically using the Go extraction service.

**Telegram** — public channels via the Go collector. It extracts messages and follows external links to fetch full article text. Explicit media refresh for a historical post retains a bounded Python compatibility helper.

**Custom (Page Monitor)** — for sites without RSS. Point it at a URL and it finds new articles using CSS selectors. The Go service captures and parses the page while Python retains selector memory, cursor/snapshot state, and optional AI learning. You can provide your own selectors as a fixed override. The minimal local Compose example supports HTTP pages; use the [native deployment guide](docs/go-cloak-native-worker.md) for JavaScript-rendered pages.

---

## Article extraction

When a source yields a link rather than full text, the Python app sends it to the private Go extraction service. There is no Python extraction fallback. The HTTP-only local profile handles ordinary HTTP pages; JavaScript rendering requires the separately configured native Go browser deployment.

---

## Tests

Run the Python suite from the repository root and the Go checks from `go/`:

```bash
python -m pytest tests -q
cd go
go test -race -count=1 ./...
go vet ./...
go mod verify
```

CI runs the Python suite and race-enabled Go tests, vet, and module verification. Some integration tests need their configured PostgreSQL or browser test environment.

---

## Reliability

**Smart Filter** — before any article reaches the AI, it goes through a content quality check: too-short or boilerplate-only content is rejected, and MD5-based duplicate detection (24-hour window) ensures the same article isn't processed twice even if multiple sources pick it up.

**Ad detection** — as part of AI processing, each article is classified as news or advertisement (`is_advertisement`, `ad_type`, confidence score). Detected ads are excluded from the digest and filtered out in the public feed. The classifier distinguishes promotional content from legitimate product announcements published by news sources.

**Dead Letter Queue** — articles that fail during AI processing are written to a persistent queue and retried automatically with exponential backoff (up to 3 attempts). Permanently failed articles are archived separately for inspection.

**Circuit Breaker** — AI and HTTP calls are wrapped in a circuit breaker. If a service starts failing repeatedly, the breaker opens and stops sending requests until the service recovers, preventing cascading failures from taking down the whole pipeline.

---

## Cost control

AI calls are the main operating cost. Several mechanisms keep them in check:

- **Article limiter** — `NEWS_LIMIT_MAX_ARTICLES` and `NEWS_LIMIT_PER_SOURCE` cap how many articles are processed per day.
- **Deduplication** — articles already summarized are never sent to AI again.
- **Token tracking** — input, output, and cached tokens are recorded per run. Set `AI_INPUT_COST_PER_1M` / `AI_OUTPUT_COST_PER_1M` to see cost estimates in the Stats panel.

---

## Getting started

### With Docker (recommended)

This standalone example is a minimal HTTP-only local stack. It starts the Go collector and scheduler, the News web app and worker, and separate PostgreSQL instances for News and scheduler data. It does not start a browser or render JavaScript pages.

Set `DB_PASSWORD`, `SCHEDULER_DB_PASSWORD`, `GEMINI_API_KEY`, `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`, `ADMIN_PASSWORD`, `JWT_SECRET`, `SCHEDULER_API_TOKEN`, `SCHEDULED_JOBS_TOKEN`, and `EXTRACTION_SERVICE_TOKEN` in the shell or an ignored `.env` file before starting it. Use fixture credentials for local development. Set Page Monitor sources to HTTP mode (`use_browser=false`) in this profile. For JavaScript-rendered sources, follow the [native deployment guide](docs/go-cloak-native-worker.md).

```bash
docker compose -f docker-compose.example.yml up --build -d
```

The app will be available at `http://localhost:8000`, with the admin panel at `/admin`.

### For local development

This adds read-only source mounts and development settings to the complete example stack; it is an overlay and must be used with `docker-compose.example.yml`:

```bash
docker compose -f docker-compose.example.yml -f docker-compose.dev.yml up --build -d
```

### Production topology

Production runs the Python News app and durable worker with the private Go collector and standalone Go scheduler. Native browser rendering is an optional Go deployment profile that uses the approved browser and egress infrastructure. [`compose.native.yml`](deploy/news-extractor/compose.native.yml) and [`compose.shared-browser.yml`](deploy/news-extractor/compose.shared-browser.yml) are deployment templates, not standalone Compose projects.

---

## Configuration

The core app settings are:

```bash
# Database (PostgreSQL)
DATABASE_URL=postgresql+asyncpg://newsuser:pass@postgres:5432/newsdb

# Google Gemini (for summarization and categorization)
GEMINI_API_KEY=your_gemini_api_key

# Telegram (where digests are published)
TELEGRAM_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_channel_id
TELEGRAM_RICH_MESSAGES_ENABLED=true
SITE_BASE_URL=https://news.example.com

# Admin panel
ADMIN_USERNAME=admin
ADMIN_PASSWORD=your_secure_password
JWT_SECRET=your_jwt_secret
```

The Go collector and scheduler are also mandatory runtime services. Configure
`EXTRACTION_SERVICE_URL` and `EXTRACTION_SERVICE_TOKEN`,
`SCHEDULER_API_URL` and `SCHEDULER_API_TOKEN`, plus `SCHEDULED_JOBS_TOKEN` for
the private News job receiver. The Compose example wires these service URLs
and credentials; see `docker-compose.example.yml` for the complete reference.

Go is the only collection, scheduling, and extraction runtime. Startup requires authenticated Go service configuration. Deprecated Python/shadow/fallback mode settings are rejected when they request removed behavior; inert compatibility values do not restore legacy runtimes. See the [source collection guide](docs/go-source-manager.md), [scheduler guide](docs/go-scheduler.md), and [native worker guide](docs/go-cloak-native-worker.md).

---

## API

The authenticated application API is under `/api/v1/`; public feed data endpoints are under `/api/public/`. Full public API documentation: [`docs/PUBLIC_API_DOCS_EN.md`](docs/PUBLIC_API_DOCS_EN.md).

Health endpoints include `GET /health`, `GET /health/app`, `GET /api/v1/system/health/db`, and `GET /api/v1/system/process-monitor`.

---

## Known limitations

- Prometheus metrics are partially wired up but not complete
