# Go source collection

`SOURCE_MANAGER_MODE=go` moves runtime source dispatch and RSS/Atom fetching into the existing private Go extractor service. Python still reads source configuration, manages Page Monitor state and optional selector learning, enriches Telegram links, deduplicates articles, and writes PostgreSQL. The scheduler, AI processing, admin CRUD, and digest delivery stay in Python.

This mode is opt-in and has not been deployed by this change. Existing deployment overlays do not enable it.

## Configuration

```dotenv
SOURCE_MANAGER_MODE=go
TELEGRAM_SOURCE_MODE=go
PAGE_MONITOR_SOURCE_MODE=go
EXTRACTION_SERVICE_URL=http://news-extractor:8080
EXTRACTION_SERVICE_TOKEN=<private-service-token>
```

The default is `SOURCE_MANAGER_MODE=python`. Go mode rejects missing credentials and mixed Python Telegram/Page Monitor modes at startup. Use matching app and worker versions. Collection errors never trigger an automatic whole-batch fallback to Python.

## Contract and limits

`POST /v1/sources/collect` uses the existing private bearer authentication and contract version `1`. Each request includes a request ID, a batch budget, and up to eight descriptors with unique positive numeric `source_id` values. Results retain every input ID in input order, including failures and deferrals. Equal source names remain distinct through persistence.

The manager runs up to five source operations concurrently and serializes browser-capable Telegram/Page Monitor jobs. Queued jobs do not spend their per-source operation budget before dispatch. A batch takes one shared admission slot; typed runners execute directly without recursively acquiring that slot. Cancellation and drain propagate to active work.

Request and response bodies are limited to 8 MiB. RSS wire and decoded bodies are limited to 5 MiB; guarded transport enforces public destinations, redirect validation, pinned DNS resolution, and a maximum 60-second RSS transport timeout. A feed without a caller limit may return at most 1,000 entries; exceeding that cap produces an explicit resource error.

A batch budget is at most 120 seconds, including queue time and serialization reserve. Python sends bounded windows and retries deferred sources within a bounded pass deadline, capped at 600 seconds. Oversized individual source results fail explicitly. When whole results do not fit together, remaining sources are deferred rather than silently truncating article lists. Page Monitor pagination comes from its existing typed runner.

Outcomes are `success`, `partial`, `deferred`, `failed`, and `rejected`, with finite error codes and retry metadata. Python checks IDs, types, nested Telegram/Page Monitor payloads, and RSS fields before normalization. Existing collection return values retain an empty list for a source that could not deliver articles.

## State and compatibility

RSS parity is characterized against Python feedparser using the committed synthetic RSS/Atom corpus. It covers content versus summary, dates, author/GUID/tags, image priorities, relative links, HTML sanitization, missing fields, malformed input, cancellation, and size bounds. This is corpus-level evidence, not a claim of universal feedparser equivalence. Private feeds, unusual authentication, and real-world feed compatibility require a separately approved acceptance run. One intentional security difference is URL admission: relative article/media links resolve against the final feed URL or Atom XML base, and only bounded absolute HTTP(S) URLs without credentials, control characters, or attribute-injection characters are returned. Invalid article links are skipped individually; invalid images are dropped while preserving the article. Python independently validates these URL fields. The recorded feedparser baseline remains unchanged; the relative-image deviation is asserted explicitly.

Page Monitor keeps Python snapshots, acknowledged hashes, partial offsets, and optional selector learning. Reparse uses the already captured HTML. Telegram linked-article enrichment happens after batch admission is released. Neither source collection nor AI runs inside the database write lock.

## Local verification

Run Python tests from the repository root and Go checks from `go/`:

```bash
.venv/bin/python -m pytest tests/test_source_manager_go_collection.py tests/test_source_manager_service_client.py tests/test_processing_postgres.py -q
cd go
go test -race -count=1 ./...
go vet ./...
go mod verify
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/extract-service-linux-amd64 ./cmd/extract-service
```

PostgreSQL tests require an isolated loopback-only `TEST_DATABASE_URL`. They must never point at production.

From the repository root, the Docker fixture gate runs the actual Linux/amd64 binary against owned synthetic RSS, Telegram, and Page Monitor origins:

```bash
.venv/bin/python scripts/run_source_manager_fixture_gate.py \
  --binary /tmp/extract-service-linux-amd64 \
  --report /tmp/source-manager-fixture-report.json
```

The gate uses an internal Docker network, a fixture-only TLS certificate, a loopback API port, and a 256 MiB worker limit. It checks the direct client plus six collection/SQLite persistence cycles, including duplicate source names and repeated delivery. It records binary/source hashes, image digests, timing, worker memory and idle state, and verifies cleanup of its own labeled resources. ARM64 Docker hosts run the Linux/amd64 worker under emulation; these timings are not native Linux performance measurements. The gate fetches container images but never real source websites or AI services.

## Rollback boundary

Before any future production enablement, validate approved real sources, matching immutable app/worker images, and normal collection/persistence behavior. Switching collection back to `SOURCE_MANAGER_MODE=python` is a configuration rollback with an app restart. Preserve saved articles and deduplication state. This feature does not change the shared browser or its other clients.
