# Private native runtime protocols

> Historical migration context: the Python executor and Unix child described below predate the current Go domain candidate. For its runtime and unfinished acceptance, see [Private Go domain runtime](go-domain-runtime.md).

The AI runtime is one long-lived Go child process inside the private Python domain container. Python owns callback v1 validation, media and delivery prompts, and PID 1. Go owns editorial snapshot and generation, immutable edition storage and Telegram delivery, as well as article analysis, category metadata caching, the legacy Gemini raw and structured transport, one shared sliding-window admission gate, one AI circuit breaker for former `AIClient` operations, and best-effort `ai_usage_tracking` writes. See [Go digest delivery](go-digest-delivery.md) for the workflow and receipt contract.

The process listens only on an owner-controlled Unix domain socket. It does not expose TCP, a public endpoint, or an arbitrary HTTP proxy. Each boot receives a fresh bearer token from its parent. Every request, including status, requires `Authorization: Bearer <token>`. The token, provider key, provider endpoint credentials, DSN, request content, prompt, provider response body, and article data never appear in logs or error responses.

## Common request and response

Every POST request JSON object has `version: 1`, a bounded `request_id` string (1–64 bytes), and `deadline_at` as an RFC3339 timestamp in the future and no more than five minutes ahead. `GET /v1/status` has no body. Request objects reject unknown fields, malformed JSON, trailing JSON, and bodies over 5 MiB. Cancellation and deadline expiry cancel limiter waits and active provider requests. The Python client does not retry local RPC POSTs; a lost digest accept acknowledgement is reconciled by reading the same operation receipt.

Success responses use `{ "version": 1, "ok": true, "result": ... }`. Failures use `{ "version": 1, "ok": false, "error": { "code": ..., "status_code": ..., "retry_after_seconds": ..., "retryable_transport": ... } }`. Error codes are finite safe identifiers; optional fields appear only when known. Operation failures use local HTTP 502, while provider status and Retry-After appear only in the envelope; this preserves statuses such as provider 204 and 304 whose HTTP responses suppress bodies. Python maps only known provider error codes with a provider status to its legacy provider-error type. Local RPC HTTP status never supplies provider status; local validation, auth, protocol, timeout, and disconnect errors remain status-less and are not automatically retried.

## Routes

### `GET /v1/status`

Requires the common bearer authentication. Returns `result: { "ready": true, "protocol_version": 1 }` when the database pool and provider configuration passed startup checks and the listener is active. It has no request body.

### `POST /v1/analyze`

Request fields: common fields, `input`, and `model`.

`input` is exactly `{ "title": string, "content": string, "url": string, "original_context": string|null, "max_text_chars": integer }`. It does not accept categories, API keys, endpoints, or provider configuration. `model` is a bounded value from the authenticated Python caller's trusted settings; an omitted model uses `SUMMARIZATION_MODEL`. The backend loads category names/display names ordered by name, mapping examples ordered by fixed category and descending usage count (up to five per category), and the first local category from PostgreSQL. Metadata is cached for 300 seconds; a failed metadata query uses the legacy static five-category fallback.

The result value is `{ "result": object, "usage": usage|null }`, preserving the existing article-analysis result, error codes, and usage semantics. Explicit Gemini total-token zero remains zero in the returned article usage. Missing total-token accounting uses the legacy prompt + completion + cached fallback.

### `POST /v1/generate`

Request fields: common fields, `mode` (`text` or `structured`), `prompt`, `model`, optional `schema`, `options`, `analysis_type`, and `domain`. Model is a bounded value from the authenticated Python caller's trusted settings; endpoint and key come only from process configuration. Text mode options are `{ "temperature": number, "max_tokens": integer }`. Structured mode requires `schema`, and preserves the legacy fixed temperature `0.1` and token limit `2000`; its options are ignored. Analysis type and domain are bounded strings used only for usage accounting. Domain metadata may be up to 8192 UTF-8 bytes to accommodate legacy callers that pass full article URLs; accounting remains best effort and does not block provider calls.

Text mode returns `result: { "choices": [{ "message": { "content": string } }], "usage": { "prompt_tokens": integer, "completion_tokens": integer, "cached_tokens": integer, "total_tokens": integer } }`. It selects only the first candidate's first text part, trims it, and accepts a candidate with no text part as an empty string. Structured mode returns `result: { "result": JSON value, "usage": ... }`; it selects the first text part, parses JSON, accepts top-level JSON arrays as the legacy Python parser does, and retains fenced/object extraction recovery. Missing provider total-token counts normalize to prompt + completion + cached tokens in the response and usage insert. If the normalized total is zero, usage accounting falls back to prompt + completion, excluding cached tokens, while the response retains zero.

Raw text requests retain the existing payload and 30-second provider timeout; structured requests retain their payload and 40-second timeout. Both preserve the legacy requirement for HTTP 200. Only pre-response transport and timeout failures retry, at most three attempts with four-second waits between attempts. Each actual provider attempt obtains one shared RPS permit. HTTP status, response decoding, JSON decoding, cancellation, and uncertain outcomes are not replayed. Both modes pass through the shared AI breaker.

### `POST /v1/permit`

Accepts common fields only and acquires exactly one shared sliding-window permit for one legacy Python HTTP POST attempt. It does not call the AI breaker. Native provider calls acquire the same limiter directly, so the Python caller must not acquire another permit around `/v1/generate` or `/v1/analyze`.

### `POST /v1/categories/invalidate`

Accepts common fields only, expires the category metadata cache, and returns `result: { "invalidated": true }`.

### `POST /v1/telegram/send`

Accepts common fields and a logical `channel` (`main` or `service`), `method`, `text`, and optional `keyboard`. The child selects the destination from configured defaults and database overrides; callers cannot supply a bot URL or destination. It sends once through the shared admission gate and returns the finite delivery state and available status metadata. A settings lookup failure before a non-digest alert uses configured defaults. No provider response body is returned.

### `POST` and `GET /v2/digest/operations`

The POST accepts common RPC fields and a bounded digest-operation input carrying the job, attempt, operation ID, domain instance, execution envelope, input fingerprint and execution context. It returns an in-process operation receipt promptly. The GET reads that same key and domain instance; changed input conflicts. The RPC request remains bounded by the common deadline while the accepted operation runs within the original job deadline. Native PostgreSQL fencing checks the worker and execution authorities independently before work and fenced writes. See [Go digest delivery](go-digest-delivery.md) for delivery receipts and ambiguous outcomes.

## Configuration and lifecycle

The process requires `NEWS_AI_SOCKET`, a per-boot `NEWS_AI_TOKEN` of 32–256 bytes, `DATABASE_URL`, `GEMINI_API_KEY`, `GEMINI_API_ENDPOINT`, and `SUMMARIZATION_MODEL`; `postgresql+asyncpg://` DSNs are normalized for pgx. `RPS` configures the shared one-second sliding window. `LOCAL_CATEGORY` configures the legacy local-category rule. `AI_INPUT_COST_PER_1M`, `AI_OUTPUT_COST_PER_1M`, and `AI_CACHED_INPUT_COST_PER_1M` configure usage-cost accounting. Digest operations also use `DIGEST_MODEL` and `SITE_BASE_URL`. `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID`, `TELEGRAM_SERVICE_CHAT_ID`, and `TELEGRAM_RICH_MESSAGES_ENABLED` are passed explicitly to the child when configured; Telegram configuration remains optional until a send is requested. The bot token remains in the private domain and is not passed to web or background. `NEWS_AI_DB_MAX_CONNS` is bounded to 1–2 and defaults to two; Python integration reserves these connections from its existing fixed pool budget.

Startup requires the socket parent directory to exist with mode 0700, refuses any pre-existing socket path, creates the Unix socket with mode 0600, and never removes a path it did not create. There is no hidden child restart or replay after child exit. On SIGTERM the runtime closes admission, cancels active requests, drains the bounded usage queue for up to five seconds, closes the pool, and removes its socket. Usage insertion is best effort; it does not retry an ambiguous COMMIT and does not change an already completed provider response.

The process reads category metadata and editorial snapshots, writes immutable editions and delivery receipts under the execution fence, and performs best-effort `ai_usage_tracking` INSERTs. It creates no tables, applies no migrations, and changes no database grants. Usage accounting preserves the caller's domain metadata without truncation, including full URLs. If a value exceeds the existing database column limit, that best-effort INSERT may fail and increments `usage_failed`; the completed AI response remains successful and is never replayed. PostgreSQL `varchar(255)` counts characters, so valid multibyte values may exceed 255 UTF-8 bytes. Python reserves the native pool's two connections from the existing `DB_POOL_SIZE + DB_MAX_OVERFLOW` budget, requires at least two remaining for the Python guard and normal operations, and fails startup if the total is below four. This split also applies to offline Python database tools; their unused native reservation is not reassigned.

The standalone article-analysis CLI remains available for diagnostics, but production callback requests use the long-lived child and do not spawn the CLI per article. The executor starts one child before becoming ready. Child exit makes it unavailable and stops the executor; shutdown cancels active operations before asking the runtime to stop and drain usage, then releases the Python database guard.
