# Telegram channel extraction in Go

The private extractor service exposes `POST /v1/telegram/fetch`. It fetches a
public Telegram preview over guarded HTTP and uses at most one native Cloak
render when HTTP fails in an eligible way or contains no posts. It uses the
existing browser and disposable owned contexts. Auth, policy, resource-limit,
rate-limit, cancellation and deadline failures do not trigger blind retries.

Go owns Telegram collection, parsing, linked expansion, and article persistence
through the Go ingestion service. Python retains source configuration and
observational source probes. Explicit admin media refresh is a narrow compatibility
exception: Python fetches the exact historical post over bounded HTTPS and uses
the retained media helper. It does not run source collection, article extraction,
a browser, or a fallback. The Go channel API currently has no post-specific
request; changing that API is outside this cleanup. Missing/failed captures
preserve stored media. Telegram HTTP fetches overlap within the source
worker pool and reserve no native browser slot. Only eligible browser fallback
enters the render gate shared with article extraction and Page Monitor. See the
[source collection guide](go-source-manager.md).

The application requires `ARTICLE_INGESTION_URL` and
`ARTICLE_INGESTION_TOKEN` at startup and has no Python collection fallback.
AI, summaries, quality checks and delivery remain in Python; scheduling is owned
by the Go scheduler service.

## Private wire contract

Use the existing service Bearer token. A request contains only:

```json
{"contract_version":"1","request_id":"unique-id","channel_url":"https://t.me/s/example","source_name":"Example","limit":100,"budget_ms":30000}
```

The body is limited to 16 KiB. Limits are 1–100 accepted posts and 1–30,000 ms.
Only HTTPS `t.me` and `telegram.me` channel paths are accepted, without arbitrary
query parameters, credentials or nonstandard ports. The service canonicalizes
the channel to `https://t.me/s/<lowercase-name>`; public URL/DNS/dial guards also
apply to redirects and rendering. Callers cannot inject HTML, network options
or browser selectors.

The response has `contract_version`, `build_version`, `request_id`,
`original_url`, `final_url`, `outcome`, `fetch_method`, `articles` and an optional
`error` with finite `code` and `retryable`. `outcome` is `success`, `partial`,
`failed` or `rejected`; `fetch_method` is `http` or `native_browser` when a page
was obtained. The response is bounded to 5 MiB. Go ingestion validates the receipt
and persists Telegram article fields, source IDs, and run metadata under the
active executor fence.

A later post or response limit preserves the accepted prefix as `partial` with an
explicit error. Go ingestion records per-source success or failure and keeps
the accepted prefix durable. No articles are marked processed by collection;
later scheduled fetches remain possible.

## Parser contract and checks

`extractor.ParseTelegramChannel(html, pageURL, sourceName, limit, fallbackTime)`
is the pure parser beneath this endpoint. It performs no network, browser,
database, AI or delivery work. The DOM is parsed once. The input limit is 5 MiB
and each post is limited to 100,000 characters. A zero fallback time is invalid;
the parser never reads the wall clock. Valid prefixes accompany later explicit
parser-limit errors. Missing messages return `ErrTelegramEmpty`.

The immutable synthetic JSON snapshot lives in `tests/fixtures/telegram/` and is
checked by the Go extractor tests. It records reviewed parser output; the
removed Python parser is not regenerated at test time. Go checks actual URLs,
IDs, timestamps and metadata, with only documented runtime-dependent fragments
normalized.

```sh
cd go
go test -race ./extractor ./service ./pipeline ./cmd/extract-service
go vet ./...
```

The production switch requires reviewed source, Linux CI, real-channel HTTP
acceptance, an isolated native fallback check and post-deployment acceptance.
Implementing the endpoint alone is not evidence that production uses it.
