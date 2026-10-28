# Article analysis boundary

Article analysis owns the Gemini request, article prompt and response schema,
provider response parsing, and normalization of titles, summaries, categories,
advertisement fields, and publication-date metadata. It remains separate from
article extraction, durable processing, editorial edition generation, and
delivery.

The implementation lives in `go-domain/analysis`. The long-lived
`news-ai-runtime` child runs inside the existing private Python domain container
and communicates over its authenticated Unix socket. Python retains the
versioned article callback, media and delivery prompts, selector callbacks,
and PID 1. Durable article processing remains in Go. There is no new container,
port, database role, or public endpoint. The standalone `news-article-analysis` command remains a
pure analysis diagnostic path; production article requests use the runtime
socket and do not spawn that command per article.

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
The runtime also owns Gemini transport for the remaining `AIClient` raw and
structured prompts, a shared sliding-window permit gate for legacy HTTP POSTs,
the AI breaker, and best-effort `ai_usage_tracking` inserts. Native provider
calls and Python `AsyncHTTPClient.post` attempts share one gate; native calls do
not acquire an extra permit. Telegram POST attempts use that same gate.

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
and unknown outcomes do not receive a runtime transport retry. Existing
task-specific Python retries still see known provider statuses and Retry-After.
Local RPC errors, invalid envelopes, and child disconnects remain finite and
status-less, so they cannot be mistaken for retryable provider failures or a
terminal provider 400. Unknown local outcomes are never resent automatically.

Provider calls run outside Python and Go database transactions. Usage records
are queued best effort after a completed provider result. An ambiguous database
commit is not retried, and tracking failure does not replay the provider call or
change its result. An explicit zero token total stays zero in the provider
response; a missing or null total normalizes to prompt plus completion plus
cached tokens. If the normalized total is zero, accounting uses prompt plus
completion while the response retains zero.

## Runtime and validation

Python starts one runtime child before the executor serves requests. It creates
an owner-only temporary directory and socket, passes a fresh bearer token only
through the child environment, and never restarts a child that exits. Child
exit revokes active work and makes the executor unavailable. Shutdown cancels
active operations, stops the runtime and drains its bounded usage queue, then
releases the Python singleton database guard and pool.

The native database pool is fixed at two connections and is reserved from the
existing `DB_POOL_SIZE + DB_MAX_OVERFLOW` budget. Python retains at least two
connections for the singleton guard and normal operations; startup rejects a
total budget below four rather than increasing it. Offline Python database
commands use the same conservative split and leave the native reservation
unused.

`docs/ai-runtime.md` defines the exact Unix-socket JSON protocol and process
configuration. CI builds `news-ai-runtime` and requires its fake-provider,
isolated-PostgreSQL integration tests with `NEWS_AI_RUNTIME_INTEGRATION_REQUIRED=1`.
The integration uses a loopback `TEST_DATABASE_URL` and a temporary schema.
Local fixture tests do not contact Gemini or Telegram. Go module tests use Go
1.25.6; the independent extractor toolchain is unchanged.
