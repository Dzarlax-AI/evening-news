# Article analysis component

Article analysis owns the Gemini request, article prompt and response schema,
provider response parsing, and normalization of titles, summaries, categories,
advertisement fields, and publication-date metadata. It is separate from article
extraction, durable processing, editorial edition generation, and delivery.

The implementation lives in `go-domain/analysis` and the bounded
`news-article-analysis` command. The command runs inside the existing private
domain runtime. It adds no container, registry repository, public endpoint, or
database role. Moving the implementation does not move the Gemini credential to
the background runtime.

## Boundary and compatibility

The authenticated `/internal/v1/article-ai` callback and its operation envelope
remain version 1. Run identity, article attempt, input fingerprint, claim checks,
and checks before and after computation remain at that boundary. The
`extraction_context` and `extraction_feedback` operations retain their existing
selector-learning behavior.

The Python adapter temporarily owns metadata reads, the shared provider admission
state, and usage persistence. It passes bounded structured category metadata to
the native component and records returned token usage through the existing
accounting path. No provider request runs inside a database write transaction.
This is a migration step, not complete removal of the Python domain runtime.

The callback consumes the same normalized analysis fields as before. Dynamic
category labels, local-category rules, mapping examples, scientific title-only
fallback, original source context, Unicode truncation, partial results and
confidence defaults remain part of compatibility. The current language policy
is Russian. Serbian content, website localization and a separate channel belong
to a later stage.

## Command protocol

The command reads one bounded JSON request on stdin and writes one versioned JSON
response on stdout. Input contains the article fields, category metadata,
local-category settings, model, configured provider endpoint, and configured
text limit. The endpoint and model come from trusted runtime configuration, not
from the article URL. The article URL is prompt context; the command does not
fetch that page.

Successful responses contain `version`, `ok`, `result`, and optional `usage`.
Usage includes the model and prompt, completion, cached and total token counts.
An explicit zero total is preserved; a missing or null total uses the existing
fallback sum. Provider error responses contain finite error codes, optional HTTP
status and Retry-After, and whether a pre-response transport failure permits the
existing immediate transport retry.

The API key is supplied through the narrow child environment. It is never a CLI
argument or a JSON response field. The adapter bounds stdin, stdout, stderr and
process lifetime, and kills and reaps the child on cancellation or timeout.
Untrusted provider bodies and article contents must not appear in diagnostics.

## Retry ownership

The existing shared rate limiter and circuit breaker remain outside the
one-shot process. A new process must not reset admission or breaker state for
every article. The actual previous HTTP client policy allows up to three
attempts for transport errors or timeouts before response headers. Its backoff
and rate admission apply to each such attempt.

Provider status errors, body-read or decoding failures, and an unknown child
outcome do not acquire an additional immediate retry. HTTP status and Retry-After
reach the existing durable article-processing policy. A provider failure is not
converted to a successful fabricated summary. Usage is recorded once, not once
per wrapper retry.

## Build and validation

The core Docker build adds the command only to the private `executor` target.
The web and background inventories reject that payload. Core toolchains remain
Go 1.25.6-compatible; the independent extractor has its own toolchain and release.

```sh
go -C go-domain test -race ./...
go -C go-domain vet ./...
go -C go-domain build -o /tmp/news-article-analysis ./cmd/news-article-analysis
```

Python integration tests use `NEWS_ARTICLE_ANALYSIS_BINARY` to select the local
executable. CI sets `NEWS_ARTICLE_ANALYSIS_INTEGRATION_REQUIRED=1` so missing
integration tooling cannot silently skip acceptance. Tests use a loopback fake
Gemini and synthetic fixtures, including frozen legacy prompt, schema and
normalization results under `tests/fixtures/article-analysis-v1/`.

The complete release gate still checks role-specific image contents and the
consolidated synthetic application path. A real-provider trial or a production
rollout requires its own approved scope; local parity is not evidence of a
production deployment.
