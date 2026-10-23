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

An optional [separate Python background worker](docs/background-worker.md) executes durable work outside the web process. With `BACKGROUND_EXECUTION_MODE=worker`, web submits PostgreSQL jobs and reports their status; `python -m news_aggregator.worker` executes them under a singleton database lease. The default remains `embedded`; the production deployment uses separate web and worker processes.

The [standalone Go scheduler](docs/go-scheduler.md) provides reusable scheduling with its own PostgreSQL schema and a versioned HTTP command/receipt contract. It is deployed separately from the News application. In `SCHEDULE_DISPATCH_MODE=service`, the News adapter accepts scheduled commands into the existing durable queue; Python remains the execution worker. The scheduler is independently packaged and does not depend on article, AI, or delivery models. Repository routing defaults remain Python.

Source collection runs up to five jobs concurrently. Telegram HTTP fetches do not reserve a browser slot. Article rendering, Telegram browser fallback, and Page Monitor rendering share one bounded native admission gate: **two active renders in the current production deployment**, with at most eight waiting operations. Each render owns a disposable browser context and target. Waiting respects the caller's deadline; admission-time expiry remains retryable busy rather than consuming an article attempt.

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

This diagram describes the deployed Go routes as of October 23, 2026. The standalone Go scheduler owns calendar timing and command dispatch; News persistence, job execution, article processing, and delivery remain in Python. Repository routing defaults remain Python, and native render concurrency defaults to one; the deployed configuration explicitly selects all four Go collection modes and `EXTRACTOR_BROWSER_CONCURRENT=2`. These are configuration choices, not changed defaults for a new installation. See the [scheduler release record](deploy/news-scheduler/RELEASE_2026-10-23.md) for deployment verification.

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

**RSS / Atom** — feed URLs collected by the guarded Go collector in production with `SOURCE_MANAGER_MODE=go`; the repository default remains Python. When the feed provides only a snippet, the full article is fetched automatically using the article extraction pipeline below.

**Telegram** — public channels via web preview. The parser strips UI chrome (forwarded headers, reactions, buttons) and follows external links to fetch full article text. The production profile routes collection and parsing through Go; Python remains the default mode.

**Custom (Page Monitor)** — for sites without RSS. Point it at a URL and it finds new articles using CSS selectors. If selectors need learning, Python analyzes the captured page and saves learned selectors for the next fetch. The production profile sends page capture and parsing to Go while Python retains selector memory and optional AI learning. You can also provide your own selectors as a fixed override.

---

## Article extraction

When a source yields a link rather than full text, the Python app sends it to the private Go extractor when `EXTRACTION_SERVICE_MODE=go`. The service applies its guarded extraction pipeline and, in the production profile, can use the shared Cloak browser for eligible pages. `EXTRACTION_SERVICE_PYTHON_FALLBACK` defaults to false; enabling a Python fallback is an explicit configuration choice. In Python mode, the existing Python extraction path remains in use.

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

This standalone example is for local use. It starts the app with its own PostgreSQL and browser containers; it is separate from the production shared-browser topology.

Configure the example's credentials before starting it.

```bash
docker compose -f docker-compose.example.yml up -d
```

The app will be available at `http://localhost:8000`, with the admin panel at `/admin`.

### For local development

```bash
docker compose -f docker-compose.dev.yml up -d
```

This mounts local application code into the container so changes take effect without rebuilding. It expects a configured PostgreSQL endpoint; it does not start the production Go extractor or shared Cloak browser.

### Production topology

Production runs the Python app, private Go extractor service, and egress gateway alongside the externally managed shared Cloak browser. The deployed News Compose configuration uses matching immutable app/worker images, all four Go routing modes, and two native render slots. [`compose.native.yml`](deploy/news-extractor/compose.native.yml) and [`compose.shared-browser.yml`](deploy/news-extractor/compose.shared-browser.yml) are deployment templates; their conservative defaults do not fully describe the current deployed configuration. Follow the [deployment handoff](deploy/news-extractor/HANDOFF.md) for merge order and runtime checks; these overlay files are not standalone Compose projects.

---

## Configuration

The only things you need to get started:

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

Everything else has sensible defaults. Full reference is in `docker-compose.example.yml`.

Routing defaults remain Python. The deployed configuration explicitly sets `SOURCE_MANAGER_MODE=go`, `EXTRACTION_SERVICE_MODE=go`, `TELEGRAM_SOURCE_MODE=go`, and `PAGE_MONITOR_SOURCE_MODE=go`, with the private service URL and bearer token configured. Go SourceManager requires both source modes to be Go. Article-extraction Python fallback is disabled, and Go source clients do not silently fall back to Python. `EXTRACTOR_BROWSER_CONCURRENT` supports one or two native renders: its default is one, while the deployed value is two. See the [source collection guide](docs/go-source-manager.md) and [native worker guide](docs/go-cloak-native-worker.md).

---

## API

The authenticated application API is under `/api/v1/`; public feed data endpoints are under `/api/public/`. Full public API documentation: [`docs/PUBLIC_API_DOCS_EN.md`](docs/PUBLIC_API_DOCS_EN.md).

Health endpoints include `GET /health`, `GET /health/app`, `GET /api/v1/system/health/db`, and `GET /api/v1/system/process-monitor`.

---

## Known limitations

- Prometheus metrics are partially wired up but not complete
