# Private extraction service

The Go service is an independent release unit. Python continues to own scheduling,
source collection, AI, learned-selector storage, article transactions and delivery.
The service owns article extraction and its temporary HTTP/browser resources.
It has no database or delivery credentials. Routing defaults to Python until the
isolated checks and rollout decision are complete.

## Production preparation status

Production uses pinned CloakBrowser 0.5.2 (`cloakserve`), not the ordinary Chrome
used in the initial local tests. The `fulfill-experimental` backend fetches
responses through Go and fulfills them over CDP; it does **not** preserve native
browser TLS/HTTP fingerprinting. The separate opt-in `native` backend uses
browser-owned traffic through the authenticated CONNECT gateway, with an
independently verified network boundary. Controlled same-image fixtures do not
prove publisher compatibility or production lifecycle acceptance. The initial pilot and remaining gates are
in the [production readiness plan](ai-plans/2026-10-16-extractor-production-readiness.html).

`shadow` currently runs inline within its caller's deadline. Its diagnostic logs
are observational, but the extra runtime can affect authoritative processing.
Do not enable it on production. Initial comparisons must run outside the
authoritative processing pipeline. A lifecycle-owned background observer remains
separate work. Native egress and shared-browser lifecycle preparation are covered
by the approved [isolation plan](ai-plans/2026-10-22-native-production-isolation.html).

The pilot enforces `EXTRACTOR_HTTP_ONLY=true` by default on Go and
`use_browser=false` in its first Python request. In the opt-in native-browser
mode, eligible HTTPS URLs on the default or port 443 first try Go HTTP and make
at most one `use_browser=true` retry within the remaining total budget, and only
for a valid extraction response classified as `browser_required`, `no_content`,
`needs_selector_study`, `upstream_forbidden`, `challenge`, or `invalid_content`.
A successful HTTP response returns without a browser request. Service HTTP
authentication/admission errors, invalid payloads, resource limits,
cancellation, service/upstream rate limits and transport failures are not
retried through the browser. Explicit callers of the service API that set
`use_browser=true` still request the direct native path. Upstream 429 preserves
its bounded `retry_after_seconds` for Python's durable retry. Learning belongs
to the final authoritative extraction attempt and is not recorded twice when
the browser retry runs.

Go checks public DNS before handing browser-required work to Python. Its
connection-time DNS pinning applies to Go HTTP requests. The existing native
Python browser transport retains its own redirect/subrequest policy; the handoff
check does not extend Go's connection-time guarantees to that second runtime.
The opt-in native path uses its own explicit isolation assertion; the new
Compose-managed browser bootstrap still requires separate Linux lifecycle acceptance.

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

`/health/live`, `/health/ready`, `/version` and `/stats` are private operational endpoints.
Chrome availability is a capability, not a requirement for HTTP-only service.
Never publish the extraction or CDP ports on a public interface.

## Ownership of the compatibility ledger

| IDs | Initial runtime owner | Requirement |
|---|---|---|
| E01–E15 | Go article pipeline; Python rollback | URL admission, all article strategies, ordering and alternate-page recovery |
| M01–M04 | Go article extraction; Python retains incompatible media-only route | Metadata and attachments retain their provenance and caller semantics |
| R01–R02 | Python source collector | Existing RSS/Atom collection remains; Go parser replay is separate evidence |
| T01–T05 | Python Telegram collector | Browser/HTTP source acquisition, message identity/media and expansion remain |
| C01–C05 | Python custom collector | Learned listing structure, AI generation, snapshots and dedup remain |
| X01 | Both, at their network boundary | Bounded wire/decoded content, DOM and text; no unbounded cross-service payload |

This table assigns responsibility; it does not claim every live path is verified.
The [method ledger](extraction-methods.md) remains authoritative. In particular,
forced replay does not verify a live fallback graph, persistent learned state or
source collection. The authenticated Telegram media endpoint parses message markup
and must remain on Python until its operation has a separate compatible contract.

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

1. Build/test the new digest with v1 contract fixtures and the Python client.
2. Start the candidate in the private network with its own resource limits,
   `EXTRACTOR_HTTP_ONLY=true` and no Chrome endpoint.
3. Check readiness, build version, authentication and HTTP-only smoke.
4. Switch the Python endpoint, then drain the previous service. A bounded
   interruption is acceptable during the pilot; zero downtime is not assumed.
5. Roll back by selecting the previous digest/endpoint. Python-only routing is
   the other rollback route. No database restore or migration is involved.

SIGTERM must stop admission before draining requests. Shutdown deadlines include
cleanup of owned browser resources. Do not kill shared Chrome to clear one job.
Retain the previous image and its API fixture snapshot before any promotion.
There is no previously released service v1 yet: first-version contract tests do
not prove compatibility with a historical binary. Future releases must test the
retained prior client/server artifacts in both directions.

## Acceptance and measurement

Run Go unit/race/vet checks, Python client and processing regressions, strict
source-golden replay comparisons, a real client/server smoke, overload/cancel/drain
tests and dedicated CDP checks. Before activation, compare Python-only against
Python + Go + Chrome under the same work, limits, concurrency and fallback counts.
Record whole-system container memory and peak, Go heap/goroutines, Chrome targets,
latency, CPU and fallback reasons. Do not infer savings from Go heap alone.

The shadow path must be observational: it may not mutate learned state, outcomes,
attempts or delivery, and must not request AI study. The authoritative Python path
still performs its ordinary writes. If Go requests selector study or fails, any
pilot fallback must be sequential and consume the remaining caller budget.

The code inventory corrected an assumption in the initial plan: article extraction
has no active AI selector generation contract. Active C04 structure study belongs
to the Python custom collector and remains there. A `needs_selector_study` response
does not start a new LLM workflow: it fails closed unless bounded Python fallback
is explicitly enabled. Reserved wire fields are not proof of an implemented AI
round trip.

Python settings: `EXTRACTION_SERVICE_MODE=python|shadow|go` (default `python`),
`EXTRACTION_SERVICE_URL`, `EXTRACTION_SERVICE_TOKEN`,
`EXTRACTION_SERVICE_TIMEOUT_SECONDS` (default/maximum 30),
`EXTRACTION_SERVICE_TOTAL_BUDGET_SECONDS` (default 120, maximum 600), and
`EXTRACTION_SERVICE_PYTHON_FALLBACK` (default false). The token must match the Go
server's `EXTRACTOR_SERVICE_TOKEN` (minimum 16 bytes). Go also accepts `LISTEN_ADDR`,
`EXTRACTOR_HTTP_ONLY` (default true), `CHROME_ENDPOINT`,
`EXTRACTOR_MAX_CONCURRENT` and `EXTRACTOR_BROWSER_CONCURRENT`.

Production rollout remains separate from local implementation approval.


### Captured study input and response size

The production article runner omits the optional `study_html` response field.
It preserves `outcome=needs_selector_study`, the study flag and selector
observations. Captured HTML remains available internally to the Go pipeline for
alternative-URL processing, but the current Python client does not consume it.
The configured Python fallback obtains its own input; returning a near-limit
page would only inflate the JSON envelope and could hide the intended outcome
behind `response_too_large`. No byte or memory limits are increased.

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
