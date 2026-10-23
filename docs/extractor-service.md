# Private extraction service

The Go service is an independent release unit and the only collection and
extraction runtime. Python retains source configuration, durable Page Monitor
state, News persistence, AI, and delivery. Go owns RSS/Atom, Telegram,
Page Monitor capture/parsing, and article extraction. It has no database or
delivery credentials. The app requires authenticated Go service URLs and tokens
at application startup; service failures do not fall back to Python implementations.

## Production preparation status

Production uses pinned CloakBrowser 0.5.2 (`cloakserve`), not the ordinary Chrome
used in the initial local tests. The `fulfill-experimental` backend fetches
responses through Go and fulfills them over CDP; it does **not** preserve native
browser TLS/HTTP fingerprinting. The separate opt-in `native` backend uses
browser-owned traffic through the authenticated CONNECT gateway, with an
independently verified network boundary. Controlled same-image fixtures do not
prove publisher compatibility or production lifecycle acceptance. The initial pilot and remaining gates are
in the [production readiness plan](ai-plans/2026-10-16-extractor-production-readiness.html).

HTTP-only is the default service profile. The optional native Go browser profile
is selected explicitly in deployments with the approved browser and egress
infrastructure. Eligible HTTP results may request a bounded native-browser retry
within the original caller deadline. Authentication, invalid payload, resource,
cancellation, rate-limit, and transport failures do not start another strategy.
Upstream 429 preserves its bounded `retry_after_seconds` for durable retry.

Go checks public DNS before browser-required work. Connection-time DNS pinning
applies to Go HTTP requests. The native path uses its own explicit isolation
assertion; Compose-managed browser bootstrap still requires separate Linux
lifecycle acceptance.

Reviewable standby/canary/rollback overlays and release instructions are in
[`deploy/news-extractor/`](../deploy/news-extractor/README.md). They have not been
applied. The CI publication job requires a manual default-branch run after
verification; preparing that workflow does not publish an image.

After the isolated API smoke and comparison pass, CI also uploads the exact
Compose-built image as the short-lived `extractor-candidate-image` artifact.
Its metadata records the archive SHA-256, Docker image ID and tested source SHA.
This two-day artifact is for isolated test runs only; it is not a registry
release or a production image reference. The separate manual default-branch
publication gate remains unchanged.

## API compatibility

`POST /v1/extract` requires `Authorization: Bearer <service token>`. The request
contains `request_id`, `url`, optional `content`, `html`, `title`, `source_type`,
`strategy`, `selectors`, `learned_selector`, `learned_method`, `use_browser`,
`budget_ms`, `max_bytes` and `max_chars`. Limits only tighten server limits.
Replay-only injection fields are not part of this API. Source collection and
article extraction are separate operations: an article found through a custom
source is submitted as an article, not a custom listing.

Responses carry `contract_version`, `build_version`, `request_id`, `outcome`,
`original_url`, `final_url`, `result`, selector `observations`,
`needs_selector_study`, optional bounded `study_html`, and an optional error with
`code`, `retryable`, and bounded optional `upstream_status`,
`retry_after_seconds`, `reason`. The result
preserves extracted text, metadata, media, method/provenance and truncation.
Consumers ignore new optional response fields. Required fields and their meaning
are stable within v1. A breaking change needs v2 and an explicit migration.

HTTP errors distinguish invalid input (400), authentication (401), body limits
(413), overload (429) and unavailable/draining service (503). A completed
extraction with no suitable content or a destination failure is a typed outcome,
not an implicit success. Python alone controls durable retries; method fallbacks
inside Go consume one request budget. Request IDs correlate diagnostics and are
not persistent deduplication keys.

The Python article, Telegram, Page Monitor and SourceManager clients share one
event-loop-local permit per configured service base URL. One request may be in
flight and at most eight may wait; admission ends after 120 seconds or the
caller's outer deadline, whichever comes first. The permit is acquired before
JSON encoding and HTTP session creation, then released after the response and
session close. Normalization, selector learning, AI work and persistence remain
outside the permit. Queue saturation is reported as retryable `busy` without
spending an article attempt; its next durable retry is scheduled after 60
seconds. Callers pass their remaining outer pass budget separately from each
HTTP request's configured timeout. Queue time consumes the outer budget; the
HTTP timeout starts after admission and is bounded by the caller's remaining
time. If the outer deadline expires in the queue, no HTTP request is started
and the result is local `busy`. Only an explicit HTTP 429 with a usable
`Retry-After` may be retried, at most eight times within that deadline; the
permit is released while waiting. Transport errors are never replayed.

`/health/live`, `/health/ready`, `/version` and `/stats` are private operational endpoints.
Chrome availability is a capability, not a requirement for HTTP-only service.
Never publish the extraction or CDP ports on a public interface.

## Ownership of the compatibility ledger

| IDs | Initial runtime owner | Requirement |
|---|---|---|
| E01–E15 | Go article pipeline | URL admission, article strategies, ordering and alternate-page recovery |
| M01–M04 | Go article and Telegram routes | Metadata, attachments, provenance and caller semantics |
| R01–R02 | Go RSS/Atom collector | Feed acquisition and article normalization |
| T01–T05 | Go Telegram collector | HTTP acquisition, message identity/media and linked expansion |
| C01–C05 | Go Page Monitor route; Python state adapter | Listing capture/parsing, selector learning, snapshots and deduplication |
| X01 | Both, at their network boundary | Bounded wire/decoded content, DOM and text; no unbounded cross-service payload |

This table assigns responsibility; it does not claim every live path is verified.
The [method ledger](extraction-methods.md) remains authoritative. Go replay does
not verify live transport, persistent learned state, or durable job behavior.

## Build and isolated container

The build context is **`go/`**, not the repository root. This keeps application
environment files, database backups and publisher captures outside the context.

```sh
docker build -f docker/Dockerfile.extractor --build-arg BUILD_VERSION=<commit> -t evening-news-extractor:<commit> go
```

The final image is a non-root static executable plus CA certificates. It includes
no shell, database client or Python runtime. Release promotion uses an immutable
image digest; do not deploy `latest` or overwrite a promoted tag.

For isolated tests, provide a new test token and run Compose with an explicitly
empty env file. Compose otherwise loads the repository `.env` implicitly.

```sh
export EXTRACTOR_TEST_TOKEN=<dedicated-test-token>
export EXTRACTOR_TEST_VERSION=<commit>
docker compose --env-file /dev/null -f docker-compose.extractor-test.yml up -d --build --wait
python scripts/smoke_extractor_service.py --endpoint http://127.0.0.1:18080 --expected-build "$EXTRACTOR_TEST_VERSION"
docker compose --env-file /dev/null -f docker-compose.extractor-test.yml down
```

On Windows create an empty file under ignored `artifacts/` and pass that path
instead of `/dev/null`. The test Compose has no application, database, scheduler
or sender, and uses a separate network. Its resource limits are test settings,
not validated production sizing. Browser capability is disabled in this minimal
HTTP test stand; dedicated browser tests must use a separate Chrome and verify
request ownership/cleanup. Do not attach the test stand to production networks.

## Independent update and rollback

1. Build/test the new digest with v1 contract fixtures and the Python client adapter.
2. Start the candidate in the private network with its own resource limits,
   `EXTRACTOR_HTTP_ONLY=true` and no Chrome endpoint.
3. Check readiness, build version, authentication and HTTP-only smoke.
4. Switch the Go service endpoint, then drain the previous service. A bounded
   interruption is acceptable during the pilot; zero downtime is not assumed.
5. Roll back by selecting the previous compatible Go service and app digests.
   There is no Python-only route. No database restore or migration is involved.

SIGTERM must stop admission before draining requests. Shutdown deadlines include
cleanup of owned browser resources. Do not kill shared Chrome to clear one job.
Retain the previous image and its API fixture snapshot before any promotion.
There is no previously released service v1 yet: first-version contract tests do
not prove compatibility with a historical binary. Future releases must test the
retained prior client/server artifacts in both directions.

## Acceptance and measurement

Run Go unit/race/vet checks, Python client and processing regressions, strict
source-golden replay comparisons, a real client/server smoke, overload/cancel/drain
tests and dedicated CDP checks. Measure the Go service and its browser profile
with the same work, limits and concurrency. Record whole-system memory and peak,
Go heap/goroutines, browser targets, latency and CPU; do not infer savings from
Go heap alone.

The code inventory corrected an assumption in the initial plan: article extraction
has no active AI selector generation contract. Page Monitor selector learning
belongs to a separate bounded app path. A `needs_selector_study` response does
not start an LLM workflow in article extraction.

Python adapter settings: `EXTRACTION_SERVICE_URL`, `EXTRACTION_SERVICE_TOKEN`,
`EXTRACTION_SERVICE_TIMEOUT_SECONDS` (default/maximum 30),
`EXTRACTION_SERVICE_TOTAL_BUDGET_SECONDS` (default 120, maximum 600). The token
must match the Go server's `EXTRACTOR_SERVICE_TOKEN` (minimum 16 bytes).
Deprecated mode/fallback variables are inert with Go-only values; explicit
requests for removed Python/shadow/fallback behavior fail startup. Go accepts `LISTEN_ADDR`,
`EXTRACTOR_HTTP_ONLY` (default true), `CHROME_ENDPOINT`,
`EXTRACTOR_MAX_CONCURRENT` and `EXTRACTOR_BROWSER_CONCURRENT`.

Production rollout remains separate from local implementation approval.


### Captured study input and response size

The production article runner omits the optional `study_html` response field.
It preserves `outcome=needs_selector_study`, the study flag and selector
observations. Captured HTML remains available internally to the Go pipeline for
alternative-URL processing, but the Python adapter does not consume it.
Returning a near-limit captured page would inflate the JSON envelope and could
hide the intended outcome behind `response_too_large`. No byte or memory limits
are increased.

Article and web result metadata is bounded on success and failure: title and
author each use at most 1 KiB, description 4 KiB, and publication date 128 bytes,
on UTF-8 boundaries. Image URLs over 8 KiB are omitted whole. The trace includes
`metadata_truncated` when these limits change output. Article identity is not
truncated.

Article media retains first-seen order and URL deduplication, with at most 256
items, 8 KiB per raw/resolved URL, 1 KiB per alt text and 256 KiB for the serialized
media array. Oversize URLs are skipped whole; `media_truncated` records omitted or
shortened output. Embedded-video matching scans incrementally so duplicate
matches do not consume slots. Alternate-page discovery retains at most two
candidates during collection and skips raw/resolved URLs over 8 KiB, preventing
long base URLs from multiplying across an unbounded candidate list.
