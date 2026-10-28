# Article analysis boundary

Article analysis owns the Gemini request, article prompt and response schema,
provider response parsing, and normalization of titles, summaries, categories,
advertisement fields, and publication-date metadata. It remains separate from
article extraction, durable processing, editorial edition generation, and
delivery.

The implementation lives in `go-domain/analysis` and `go-domain/airuntime`. The private Go domain process serves the versioned callback and calls its analysis component directly. Durable article processing remains in the Go background runtime; no new container, child process, or public endpoint is required.

## Compatibility

The authenticated `/internal/v1/article-ai` callback and operation envelope
remain version 1. Run identity, article attempt, input fingerprint, claim checks,
and checks before and after computation remain at that boundary.
`extraction_context` and `extraction_feedback` retain their selector-learning
behavior.

The runtime loads category names/display names ordered by name, mapping examples
ordered by fixed category and descending usage count, and the first local
category. It caches the values for five minutes. Missing nullable `is_local`
values are false. A failed metadata query uses the legacy static categories.
The runtime also owns Gemini transport, a shared sliding-window permit gate,
the AI breaker, and best-effort `ai_usage_tracking` inserts. Telegram POST
attempts use that same gate.

The callback consumes the same normalized analysis fields as before. Dynamic
category labels, local-category rules, mapping examples, scientific title-only
fallback, original source context, Unicode truncation, partial results, and
confidence defaults remain part of compatibility. The current language policy
is Russian. Serbian content, website localization, and a separate channel are
later work.

## Retry and errors

The runtime preserves the previous shared provider admission and breaker state
across article requests. Provider requests retry up to three times only for
pre-response transport errors or timeouts, with four-second waits; each actual
attempt obtains one permit. Provider statuses, response-body/decode failures,
and unknown outcomes do not receive a runtime transport retry. Known provider status and Retry-After information remains available to callers. Unknown outcomes are not resent automatically.

Provider calls run outside database transactions. Usage records
are queued best effort after a completed provider result. An ambiguous database
commit is not retried, and tracking failure does not replay the provider call or
change its result. An explicit zero token total stays zero in the provider
response; a missing or null total normalizes to prompt plus completion plus
cached tokens. If the normalized total is zero, accounting uses prompt plus
completion while the response retains zero.

## Runtime and validation

The process uses a shared bounded PostgreSQL pool, including its dedicated singleton guard, and reserves one connection in the total budget for `pg_dump`. Shutdown cancels and drains active work before releasing ownership. The runtime does not migrate the database at startup.

The Go core matrix runs against PostgreSQL 16 and 17 with fake providers. The integrated image gate remains incomplete; current evidence and gaps are recorded in [Go tooling coverage](../go-tools/COVERAGE.md). No production deployment is implied.
