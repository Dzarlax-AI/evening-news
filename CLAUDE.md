# Evening News project notes

Read [README.md](README.md) and [docs/web-api-components.md](docs/web-api-components.md) for the current local architecture. Component responsibilities are independent of implementation language and container placement. Local changes do not imply a production rollout.

## Runtime ownership

- `go-web/`: HTTP/UI, authentication, job acceptance, schedule management, admin commands, and PostgreSQL adapters in separate packages.
- `go-worker/`: durable queue ownership, lease, claims, heartbeat, and recovery.
- `go-ingestion/`: configured-source collection and article persistence.
- `go/`: RSS/Telegram/Page Monitor capture and parsing, article extraction, native browser client.
- `go-processing/`: article processing state transitions and persistence.
- `go-scheduler/`: schedule storage and dispatch, with its separate scheduler database.
- `news_aggregator/executor.py`: private authenticated domain executor; AI callbacks, prompts, daily summaries, and delivery remain Python.
- `news_aggregator/migrate.py`: one-shot schema bootstrap; public web does not migrate.

The former Python public/admin/API/auth modules and `web/templates/` have been removed. Use `go-web/templates/`; shared CSS and JavaScript remain under `web/static/`. The private executor still needs FastAPI, Uvicorn, and multipart parsing.

## Local commands

```bash
python -m pytest tests -q
(cd go-web && go test -race ./... && go vet ./... && go mod verify)
docker compose -f docker-compose.example.yml -f docker-compose.dev.yml up --build
```

PostgreSQL integration tests require an isolated loopback `TEST_DATABASE_URL`; Python SQLAlchemy tests use `postgresql+asyncpg://`, while Go tests use `postgres://`. Never point tests at production. Fixture providers and sources must remain synthetic.

## Data and private operations

The database is PostgreSQL. Use the actual model field names: `DailySummary.summary_text`, `DailySummary.articles_count`, and `Article.fetched_at`.

Python `DatabaseQueueManager` reads use `execute_read`, writes use `execute_write`. Keep AI calls outside write locks; the manager commits writes automatically. Explicit sessions outside it still need a commit.

The web receives only its database, auth, scheduler, and private backend credentials. `WEB_BACKEND_URL` is the executor origin, for example `http://executor:8100`, without a route prefix. `WEB_BACKEND_TOKEN` is separate from job/AI tokens. Never pass Gemini or Telegram provider credentials to web.

`get_telegram_service()` is a singleton; settings updates must invalidate it through the existing reset/cache contract. Main digest and service-alert chats are separate settings. Worker-mode backup upload, cleanup, and restore return 409; do not enable online restore as part of web work.

## UI and compatibility

Preserve the existing Book styling, dark mode, public/admin paths, auth modes, queue idempotency, and persisted job receipts. Go templates use `html/template` escaping. Admin pages extend `go-web/templates/admin/admin_base.html`; public pages use `go-web/templates/base.html`.

The historical API/schema oracle lives in `tests/fixtures/web-contract-v1/`. Read [docs/web-api-legacy-removal-map.md](docs/web-api-legacy-removal-map.md) for the retired Python tests and their retained or Go replacements. Do not restore a Python web fallback to make a test pass.
