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

Python remains the application and orchestration layer: it owns RSS collection, scheduling, AI processing, selector memory, database writes, and digest delivery. The private Go extractor service handles article extraction and can also collect Telegram and Page Monitor sources. These routes are selected independently; each Python mode remains the default, and Go routes require the private service URL and bearer token. A Python fallback for article extraction is available only when explicitly enabled.

The production Compose profile selects Go for article extraction, Telegram, and Page Monitor. Go browser requests use the shared Cloak browser through the private control network and egress gateway; Readeck also uses that browser. The Python app retains its browser pool for paths that remain in Python mode. See the [Page Monitor guide](docs/go-page-monitor.md), [Telegram guide](docs/go-telegram-source.md), and [production deployment handoff](deploy/news-extractor/HANDOFF.md).

```mermaid
flowchart TD
    subgraph sources["Sources"]
        RSS[RSS / Atom feeds]
        TGS[Telegram channels]
        WEB[Custom pages<br/>Page Monitor]
    end

    subgraph app["Python application"]
        Scheduler[Scheduler]
        SourceManager[Python source manager]
        Memory[Page Monitor selector memory]
        DB[(PostgreSQL)]
        Filter[Content quality and deduplication]
        Proc[Article processor]
        AI1[Google Gemini<br/>summarize + categorize]
        DGen[Daily summary generator]
        AI2[Google Gemini<br/>per-category summary]
        UI[FastAPI admin and public site]
        TGBot[Telegram digest publisher]
    end

    subgraph go["Private Go extractor service"]
        GoArticle[Article extraction]
        GoTelegram[Telegram source route]
        GoPage[Page Monitor source route]
    end

    subgraph browser["Shared browser infrastructure"]
        Cloak[Cloak browser]
        Readeck[Readeck]
        Egress[HTTP/S egress gateway]
    end

    Channel[Your Telegram channel]
    WebOrigins[Public websites]

    RSS --> SourceManager
    TGS --> SourceManager
    WEB --> SourceManager
    Scheduler --> SourceManager
    SourceManager <--> Memory
    SourceManager <-->|private API when Go mode is enabled| GoTelegram
    SourceManager <-->|private API when Go mode is enabled| GoPage
    Proc <-->|article extraction when Go mode is enabled| GoArticle
    GoArticle -->|CDP control| Cloak
    GoTelegram -->|CDP control when needed| Cloak
    GoPage -->|CDP control when needed| Cloak
    GoArticle -->|guarded HTTP| WebOrigins
    GoTelegram -->|guarded HTTP| WebOrigins
    GoPage -->|guarded HTTP| WebOrigins
    Cloak -->|browser HTTP/S| Egress
    Egress --> WebOrigins
    Readeck -->|CDP control| Cloak
    SourceManager --> DB
    GoTelegram -->|posts| SourceManager
    GoPage -->|items| SourceManager
    DB --> Filter
    Filter -->|passes| Proc
    Proc <--> AI1
    Proc --> DB
    DB --> DGen
    DGen <--> AI2
    DGen --> DB
    TGBot --> Channel
    DB --> UI
    UI --> TGBot
```

The diagram shows the production route choices. Settings default to Python: `EXTRACTION_SERVICE_MODE=python`, `TELEGRAM_SOURCE_MODE=python`, and `PAGE_MONITOR_SOURCE_MODE=python`. The production overlays are merged with the existing infrastructure and News Compose projects, not run as a standalone stack; see the [deployment handoff](deploy/news-extractor/HANDOFF.md).

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

**RSS / Atom** — any standard feed URL. When the feed provides only a snippet, the full article is fetched automatically using the article extraction pipeline below.

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

Production combines the existing News Compose project with [`compose.native.yml`](deploy/news-extractor/compose.native.yml) and the infrastructure overlay [`compose.shared-browser.yml`](deploy/news-extractor/compose.shared-browser.yml). The News overlay configures the private Go service, selects Go for article extraction, Telegram, and Page Monitor, and connects the app and worker to the shared Cloak browser through private control and egress networks. Readeck uses that shared browser as well. Follow the [deployment handoff](deploy/news-extractor/HANDOFF.md) for the actual merge order and runtime checks; these overlay files are not standalone Compose projects.

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

The source routing settings default to Python. Set `EXTRACTION_SERVICE_MODE=go`, `TELEGRAM_SOURCE_MODE=go`, or `PAGE_MONITOR_SOURCE_MODE=go` only when the private Go service URL and bearer token are configured. The production Compose overlay sets all three to Go and disables article-extraction fallback. The Go Telegram and Page Monitor clients do not silently retry through Python.

---

## API

The authenticated application API is under `/api/v1/`; public feed data endpoints are under `/api/public/`. Full public API documentation: [`docs/PUBLIC_API_DOCS_EN.md`](docs/PUBLIC_API_DOCS_EN.md).

Health endpoints include `GET /health`, `GET /health/app`, `GET /api/v1/system/health/db`, and `GET /api/v1/system/process-monitor`.

---

## Known limitations

- Prometheus metrics are partially wired up but not complete
