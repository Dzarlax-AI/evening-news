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

```mermaid
flowchart TD
    subgraph sources["Sources"]
        RSS[RSS / Atom feeds]
        TGS[Telegram channels]
        WEB[Custom pages<br/>Page Monitor]
    end

    subgraph fetch["1 · Fetch"]
        Fetcher[Source fetcher<br/>feedparser · nodriver · HTTP]
    end

    subgraph extract["2 · Extract"]
        Ext[Content extractor]
        Ext1[Learned patterns]
        Ext2[Enhanced selectors]
        Ext3[Readability]
        Ext4[Browser / nodriver CDP]
        Ext1 --> Ext2 --> Ext3 --> Ext4
    end

    subgraph db["Storage"]
        DB[(PostgreSQL)]
    end

    subgraph process["3 · Process"]
        Filter[Smart Filter<br/>dedup · quality check]
        Proc[Article processor]
        AI1[Google Gemini<br/>summarize + categorize]
        DLQ[Dead Letter Queue<br/>failed → retry]
    end

    subgraph digest["4 · Digest"]
        DGen[Daily summary<br/>generator]
        AI2[Google Gemini<br/>per-category summary]
    end

    subgraph delivery["5 · Deliver"]
        Site[Public site<br/>daily digest page]
        TGBot[Telegram Bot<br/>rich digest message]
        Channel[Your Telegram<br/>channel]
    end

    subgraph admin["Admin panel  /admin"]
        UI[FastAPI web UI]
        Scheduler[Scheduler]
    end

    RSS --> Fetcher
    TGS --> Fetcher
    WEB --> Fetcher
    Fetcher --> DB
    DB --> Ext
    Ext --> Ext1
    Ext --> DB
    DB --> Filter
    Filter -->|passes| Proc
    Filter -->|duplicate / junk| DLQ
    Proc <--> AI1
    Proc --> DB
    Proc -->|on failure| DLQ
    DLQ -->|retry| Proc
    DB --> DGen
    DGen <--> AI2
    DGen --> DB
    DB --> Site
    Site --> TGBot
    TGBot --> Channel
    UI --> DB
    Scheduler -->|triggers| Fetcher
    Scheduler -->|triggers| Proc
    Scheduler -->|triggers| DGen
```

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

**Telegram** — public channels via web preview. The parser strips UI chrome (forwarded headers, reactions, buttons) and follows external links to fetch full article text.

**Custom (Page Monitor)** — for sites without RSS. Point it at a URL and it finds new articles using a cascade of CSS selectors. If the built-in selectors don't match the page, the browser renders the page and AI analyzes its structure to generate working selectors. Learned selectors are saved and reused on the next fetch. You can also provide your own selectors as a fixed override.

---

## Article extraction

When a source yields a link rather than full text, the app extracts article content using a cascade of strategies — moving to the next only if the previous one produces low-quality output:

1. **Learned patterns** — CSS selectors that worked before on the same domain, stored in the database with usage counts. Fastest path; gets better the more articles you process.
2. **Enhanced selectors** — a set of common structural CSS selectors (`article`, `[class*=content]`, `[class*=article-body]`, etc.) tried heuristically.
3. **Readability** — Mozilla's Readability algorithm, which strips navigation and boilerplate and extracts the main content block.
4. **Browser rendering** — Alpine Chrome via CDP (nodriver) for JS-heavy pages that don't render without JavaScript. Runs in a separate Docker container.
5. **Fallback** — generic text extraction from the raw HTML as a last resort.

Successful extractions are recorded per domain. The method that worked best gets promoted and tried first on the next request to the same domain. Domains with repeated failures get exponential backoff — delays double up to 6 hours before retrying.

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

```bash
cp docker-compose.example.yml docker-compose.yml
# Edit docker-compose.yml — fill in your API keys and passwords
docker-compose up -d
```

The app will be available at `http://localhost:8000`. The admin panel is at `/admin`.

### For local development

```bash
docker-compose -f docker-compose.dev.yml up -d
```

This mounts your local code into the container so changes take effect without rebuilding.

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

---

## API

The app exposes a REST API at `/api/v1/`. Full documentation: [`docs/PUBLIC_API_DOCS_EN.md`](docs/PUBLIC_API_DOCS_EN.md).

Health endpoints: `GET /health/db` — database pool status, `GET /process-monitor` — background process status.

---

## Known limitations

- No automated tests
- Prometheus metrics are partially wired up but not complete
