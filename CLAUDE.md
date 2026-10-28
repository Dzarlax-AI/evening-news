# Evening News project notes

Read [README.md](README.md) and [docs/web-api-components.md](docs/web-api-components.md) for the current local architecture. Component responsibilities are independent of implementation language and container placement. Local changes do not imply a production rollout.

## Runtime ownership

- `go-web/`: HTTP/UI, authentication, job acceptance, schedule management, admin commands and PostgreSQL adapters.
- `go-worker/`: durable queue ownership, lease, claims, heartbeat and recovery.
- `go-ingestion/`: configured-source collection and article persistence.
- `go/`: RSS/Telegram/Page Monitor capture and parsing, article extraction and native browser client.
- `go-processing/`: article processing state transitions and persistence.
- `go-scheduler/`: schedule storage and dispatch, with its separate scheduler database.
- `go-domain/`: private authenticated domain operations, AI callbacks, prompts, summaries and delivery, plus the explicit one-shot migration.

Use `go-web/templates/`; shared CSS and JavaScript remain under `web/static/`. The shared browser and isolated browser-testing infrastructure remain separate from the Go application runtime.

## Local commands

```bash
make help
(cd go-web && go test -race ./... && go vet ./... && go mod verify)
docker compose -f docker-compose.example.yml -f docker-compose.dev.yml up --build
```

PostgreSQL integration tests require an isolated loopback `TEST_DATABASE_URL`; scheduler tests use `SCHEDULER_TEST_DATABASE_URL`. Never point tests at production. Fixture providers and sources must remain synthetic.

## Data and private operations

The database is PostgreSQL. Use the actual model field names: `DailySummary.summary_text`, `DailySummary.articles_count` and `Article.fetched_at`. Keep AI and other external calls outside database transactions and preserve job, attempt and domain fences. Do not automatically replay uncertain delivery.

The web receives only its database, auth, scheduler and private backend credentials. `WEB_BACKEND_URL` is the domain origin, for example `http://executor:8100`, without a route prefix. `WEB_BACKEND_TOKEN` is separate from job/AI tokens. Never pass Gemini or Telegram provider credentials to web.

Main digest and service-alert chats are separate settings. Worker-mode backup upload, cleanup and restore return 409; do not enable online restore as part of web work.

## UI and compatibility

Preserve the existing Book styling, dark mode, public/admin paths, auth modes, queue idempotency and persisted job receipts. Go templates use `html/template` escaping. Admin pages extend `go-web/templates/admin/admin_base.html`; public pages use `go-web/templates/base.html`.

The historical API/schema oracle lives in `tests/fixtures/web-contract-v1/`. Read [docs/web-api-legacy-removal-map.md](docs/web-api-legacy-removal-map.md) for the retired Python tests and their retained or Go replacements. Do not restore a Python web fallback to make a test pass.
