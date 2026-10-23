# Go scheduler HTTP contract v1

This document freezes the standalone scheduler's public-to-internal wire format. The scheduler owns only its configured PostgreSQL schema. A target is a server-side alias; schedule clients cannot provide a URL, credentials, headers, or shell command.

Machine-readable OpenAPI 3.1 description: [`go-scheduler-openapi.yaml`](go-scheduler-openapi.yaml). This contract remains the behavioral reference for operational semantics and limits.

## Conventions

- Base path: `/v1`; all API and consumer requests and responses use UTF-8 JSON.
- Timestamps are UTC RFC3339 with an explicit `Z` suffix. Scheduler timestamps use RFC3339Nano with at most six fractional digits, preserving PostgreSQL microseconds. Trailing fractional zeroes may be omitted. `scheduled_for` never truncates microseconds.
- UUIDs use canonical lowercase hyphenated form. Schedule keys match `[a-z0-9][a-z0-9._:-]{0,127}`.
- JSON request bodies are limited to 16 KiB, except `POST /v1/import`, which is limited to 512 KiB. Each `payload` is an object whose compact UTF-8 JSON encoding is at most 4096 bytes. Unknown fields are rejected.
- API authentication is `Authorization: Bearer <SCHEDULER_API_TOKEN>`. Missing/invalid credentials return 401. The token is accepted only for the scheduler API and is never logged.
- Scheduler API errors are always `application/json` with shape `{"error":{"code":"...","message":"..."}}`; messages contain no credentials or payload values. Stable codes are listed below.

## Schedule resource

```json
{
  "schedule_key": "news:42",
  "revision": 3,
  "enabled": true,
  "calendar": {
    "type": "daily",
    "timezone": "Europe/Belgrade",
    "hour": 18,
    "minute": 30,
    "weekdays": [1, 2, 3, 4, 5]
  },
  "target_alias": "news-worker",
  "operation": "news_digest",
  "payload": {"run_processing": true, "send_telegram": false},
  "next_due_at": "2026-10-23T16:30:00Z",
  "created_at": "2026-10-23T10:00:00.123456Z",
  "updated_at": "2026-10-23T10:00:00.123456Z"
}
```

`revision` starts at 1 after creation and increases by exactly 1 for every accepted configuration change, including enable/disable. `created_at` is immutable; `updated_at` changes only on a successful configuration mutation. `next_due_at` is null when disabled and otherwise the next unclaimed due time in UTC. Dispatch/health state is not stored in this resource.

Calendar `type` is one of `daily`, `weekly`, `hourly`, or `interval`; `timezone` must be a valid IANA TZ database name. `weekdays` uses ISO values 1=Monday through 7=Sunday, is optional for `daily` and `hourly`, required and non-empty for `weekly`, and forbidden for `interval`. `daily` requires `hour` (0–23) and `minute` (0–59); `weekly` requires the same; `hourly` requires `minute` (0–59); `interval` requires `interval_minutes` (1–1440). Fields not applicable to the chosen type are rejected. Weekday arrays must contain unique integers in ascending order.

`operation` is an opaque consumer-defined string matching `[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`. Scheduler validates structure and limits only; it does not whitelist domain operations or interpret payload fields. `target_alias` must exist in the service's startup allowlist.

For the existing News consumer, the operation names are `news_processing`, `news_digest`, `telegram_digest`, `backup`, and `reprocess_failed`. These are examples of opaque values, not a scheduler allowlist. Payload fields for that consumer follow its own validation; examples include `news_digest` with `run_processing` and `send_telegram` booleans, or `reprocess_failed` with `limit` (1–100) and `dry_run` boolean. The scheduler does not validate those fields.

## CRUD endpoints

All endpoints below require the scheduler API bearer token.

### `GET /v1/schedules`

Returns `200`:

```json
{"schedules":[<Schedule>, ...],"next_cursor":"<opaque or null>"}
```

Items are ordered by `schedule_key` ascending. Disabled schedules are included. The default page size is 100. Query parameters are `limit` (1–100) and opaque `cursor`. Responses use `{"schedules":[...],"next_cursor":"<opaque or null>"`. Clients page through the entire list as needed. Cursors are opaque and stable only for the current dataset snapshot.

### `GET /v1/schedules/{schedule_key}`

Returns `200` with the `Schedule` resource itself, or `404` `schedule_not_found`.

### `PUT /v1/schedules/{schedule_key}`

Idempotent upsert addressed by the stable key. The body is:

```json
{
  "expected_revision": 0,
  "enabled": true,
  "calendar": {"type":"daily","timezone":"Europe/Belgrade","hour":18,"minute":30},
  "target_alias": "news-worker",
  "operation": "news_digest",
  "payload": {}
}
```

For creation, `expected_revision` must be `0`; success returns `201` and the created `Schedule` with `revision: 1`. For update, it must exactly match the current revision; success returns `200` with revision incremented by one. A stale or incorrect expected revision returns `409` `revision_conflict` and `{"error":{"code":"revision_conflict","message":"schedule revision does not match"},"current_revision":<integer>}`. An existing key with `expected_revision: 0` also conflicts. Retrying a timed-out PUT with the same expected revision returns conflict if the first write committed; clients GET the resource to resolve that outcome.

### `DELETE /v1/schedules/{schedule_key}`

Body: `{"expected_revision":<current integer>}`. Success returns `204` with no body. Stale revision returns `409` `revision_conflict`; an unknown key returns `404` `schedule_not_found`. Delete is explicit and does not delete already-created occurrences/outbox records.

### Enable and pause

Per-schedule enable/disable uses the same `PUT` with a full replacement resource and expected revision; setting `enabled:false` clears `next_due_at`, and setting it true calculates the next future due time. Existing pending outbox work is retained.

### `PUT /v1/control`

Emergency pause state, body `{"expected_revision":<integer>,"paused":true}`. Initial control revision is 0. A successful change returns `200` with `{"revision":<integer>,"paused":<boolean>,"updated_at":"<RFC3339Nano UTC>"}`. Stale revision returns `409` `revision_conflict`. The successful control-row commit is the pause linearization point: after it commits, no new occurrence generation or dispatch claim may begin. Requests already claimed before that point may complete and be acknowledged; accepted work cannot be retracted. Due/outbox work remains for resumption.

### `GET /v1/status`

Returns `200` with `{"paused":<boolean>,"control_revision":<integer>,"last_tick_at":"<RFC3339Nano UTC or null>","last_dispatch_at":"<RFC3339Nano UTC or null>","pending_outbox":<non-negative integer>}`. `pending_outbox` counts every non-accepted row, including pending, leased, and blocked. This is an authenticated operational status endpoint. It reports scheduler acceptance/dispatch state, never consumer execution success.

### Health

`GET /healthz` is process liveness and returns `200` `{"status":"ok"}` while the process can serve requests. `GET /readyz` verifies PostgreSQL connectivity, completed schema migrations, and that the scheduler loop has ticked within three configured poll intervals; it returns `200` `{"status":"ready"}` or `503` `{"status":"not_ready"}`. Health endpoints do not disclose connection strings or credentials.

### `GET /version`

Unauthenticated build metadata returns `200` `{"version":"1","build_id":"<id>"}`. `build_id` is an opaque non-secret build identifier embedded at build time; it never contains runtime configuration or paths. Docker builds accept `SCHEDULER_BUILD_ID` and embed that value in the binary. Local builds use `development` unless set by ldflags.

## Schedule and control errors

- `400 invalid_json`: malformed JSON, unknown fields, or body over limit.
- `401 unauthorized`: absent or invalid bearer token.
- `404 schedule_not_found`: key does not exist.
- `409 revision_conflict`: expected revision does not match current state.
- `409 import_requires_pause`: import attempted while scheduling is not paused.
- `409 import_conflict`: one or more imported keys conflict with existing configuration.
- `409 occurrence_not_retryable`: accepted occurrence cannot be explicitly replayed.
- `404 occurrence_not_found`: occurrence does not exist.
- `405 method_not_allowed`: method is unsupported for the route.
- `404 not_found`: route does not exist.
- `422 invalid_schedule`: invalid key, calendar, timezone, payload, operation, or unavailable target alias.
- `500 internal_error`: unexpected server failure, without internal details.
- `503 unavailable`: database or service is temporarily unavailable; includes `Retry-After` when known.

## Delivery envelope v1

Scheduler POSTs to the statically configured URL for the selected target alias. It uses `Content-Type: application/json`, `Authorization: Bearer <target-scoped token>`, and `Idempotency-Key: <occurrence_id>`. No client-supplied URL or header is forwarded. Envelope body:

```json
{
  "version": 1,
  "occurrence_id": "79c2d7b2-96ee-4eb3-ad6d-81969a0e348e",
  "schedule_key": "news:42",
  "schedule_revision": 3,
  "scheduled_for": "2026-10-23T16:30:00Z",
  "operation": "news_digest",
  "payload": {"run_processing":true,"send_telegram":false}
}
```

The body is at most 8192 bytes. `occurrence_id` remains identical across every delivery retry. The outbox row captures the schedule revision, target alias, operation, payload, and scheduled instant at creation; later schedule edits do not change an existing envelope.

The receiver accepts only `200` or `202`, `Content-Type: application/json`, and this exact JSON object with no unknown fields:

```json
{
  "version": 1,
  "occurrence_id": "79c2d7b2-96ee-4eb3-ad6d-81969a0e348e",
  "job_id": "d35ec3f9-4196-4bf8-840a-61e7972442f9",
  "status": "accepted"
}
```

All values must be correctly typed; both UUIDs must be canonical; returned `occurrence_id` must equal the request; `status` must equal `accepted`. A malformed, mismatched, oversized, non-JSON, or non-200/202 response is not an acknowledgement and remains pending/retryable. `job_id` is the receiver's durable job identity; a duplicate idempotency key with identical content returns the same job receipt. A receiver that uses the same key with different content returns `409` and must not enqueue another job.

The receiver's idempotency receipt is durable across deletion/retention of the corresponding execution job, so a delayed duplicate still returns the original `job_id` and cannot create a second job. If durable receipt storage is at capacity, the receiver returns `503` and creates no job; it must not accept a command unless both the job and its deduplication receipt can commit durably.

## Delivery state and retry behavior

Occurrence creation, outbox insertion, and schedule `next_due_at` advancement commit in one PostgreSQL transaction. All HTTP happens after commit. A schedule has at most one non-accepted occurrence (pending, leased, or blocked) at a time; while one exists, its due timestamp remains unchanged even if its calendar becomes due again. Once accepted, one occurrence may be created on a later tick and the schedule advances to the first calendar instant after that tick's captured time. This permits at most one overdue occurrence per schedule after downtime instead of a catch-up burst. Accepted consumer jobs may still queue up to the receiver's capacity; the scheduler does not observe execution completion. Dispatcher ownership uses a bounded lease and fencing token; only the currently valid, unexpired lease can update delivery state. No transaction is held over network I/O. The scheduler caps non-accepted outbox rows; when the cap is full, due schedules stay unchanged and are considered on later ticks. Pending and blocked rows are never dropped to make room.

`200`/`202` plus a valid receipt marks the outbox item accepted. `429`, `503`, network errors, timeouts, and malformed receipts retry with bounded exponential backoff and jitter; valid integer-seconds or HTTP-date `Retry-After` is honored up to the configured maximum delay. `401` and `422` are terminal visible blocked states until operator/configuration action; other non-retryable 4xx responses are also blocked. Pending/blocked work is never deleted by age. Only accepted outbox rows are eligible for configured retention. A paused scheduler generates no new occurrences and makes no dispatch requests.

## Service configuration boundary

At startup the service requires `SCHEDULER_DATABASE_URL` using PostgreSQL `postgresql://` or `postgres://` syntax, `SCHEDULER_DB_SCHEMA`, `SCHEDULER_API_TOKEN`, and `SCHEDULER_TARGET_ALIASES`. `LISTEN_ADDR` defaults to `:8080`. `SCHEDULER_START_PAUSED` sets the pause state only when the control row is first initialized; after that, persisted control state wins across restarts. Set it to `true` for the first initialization/import. The schema value must match `[a-z_][a-z0-9_]{0,62}` and is quoted as an identifier in SQL. `SCHEDULER_DB_MAX_CONNS` bounds the pool (default 8, allowed 1–64); `SCHEDULER_MAX_INFLIGHT` bounds concurrent HTTP dispatches (default 4, allowed 1–32). `SCHEDULER_POLL_INTERVAL` defaults to `1s` and is bounded to `100ms`–`5m`. `SCHEDULER_HTTP_TIMEOUT` defaults to `10s` and is bounded to `100ms`–`2m`. Lease duration is at least 30 seconds and at least twice the HTTP timeout.

`SCHEDULER_TARGET_ALIASES` is a comma-separated, unique list of aliases matching `[a-z0-9][a-z0-9-]{0,62}`. For each alias, the service reads `SCHEDULER_TARGET_<ALIAS_WITH_HYPHENS_REPLACED_BY_UNDERSCORES>_URL` and `_TOKEN` from process environment. URLs must be absolute URLs without userinfo or fragments. HTTPS is required by default. Static HTTP targets are allowed only when `SCHEDULER_ALLOW_HTTP_TARGETS=true`, for private fixture/deployment networks; user-supplied URLs remain forbidden. Redirect following is always disabled. The target token is sent only to that alias's configured origin. These values are never part of schedule resources or logs. The service does not resolve user-controlled hosts, execute commands, or make requests to URLs from payloads.

`SCHEDULER_MAX_OUTBOX` bounds all non-accepted occurrences (pending, leased, and blocked), defaults to 10000, and accepts 100–100000. When full, occurrence creation stops without advancing due schedules; dispatch of existing work may continue to free capacity. A persisted invalid calendar causes that generation transaction to fail closed, without creating an occurrence or advancing any schedule; readiness remains false until the data is corrected.

The database login is provisioned externally with privileges only on the scheduler schema. Startup migrations create/use tables only in that schema and fail on an invalid identifier or insufficient privileges. Credentials are read from environment, excluded from errors/logs, and never returned by status or health endpoints.

The container health command is `scheduler healthcheck`; it requests the local `/readyz` endpoint and exits nonzero unless it returns ready.

Integration tests use only `SCHEDULER_TEST_DATABASE_URL`, separate from runtime `SCHEDULER_DATABASE_URL`. Tests accept TCP hosts `127.0.0.1` or `localhost` only, create one random schema with the `scheduler_test_` prefix, and clean up only that schema. The CI fixture value is `postgres://fixture_admin:fixture-only@127.0.0.1:55448/scheduler_tests`. If the test DSN is absent, PostgreSQL integration tests report skipped; unit tests remain runnable.

## Import and occurrence inventory

### `POST /v1/import`

Imports one bounded batch of legacy schedules. The request is accepted only while globally paused, and only when `expected_control_revision` equals the current control revision. `items` must contain 1–100 unique keys. Each item has exactly the fields `schedule_key`, `enabled`, `calendar`, `target_alias`, `operation`, `payload`, `next_due_at`; `next_due_at` is an exact UTC timestamp or null. Enabled imports require a non-null `next_due_at`; disabled imports require null. No client-supplied `created_at` is accepted; imported resources receive service-generated creation/update timestamps. Example:

```json
{
  "expected_control_revision": 2,
  "items": [{
    "schedule_key": "news:42",
    "enabled": true,
    "calendar": {"type":"daily","timezone":"Europe/Belgrade","hour":18,"minute":30},
    "target_alias": "news-worker",
    "operation": "news_digest",
    "payload": {"run_processing":true,"send_telegram":false},
    "next_due_at": "2026-10-23T17:30:19.728869Z"
  }]
}
```

The request is atomic: if any item conflicts or is invalid, no item is imported. Missing keys are created at revision 1 with the exact imported `next_due_at`. An existing key is a no-op only if enabled state, calendar, target alias, operation, payload, and `next_due_at` all match exactly; otherwise the whole batch returns `409` `import_conflict`. Repeating an identical successful import is idempotent. Success returns `200` `{"control_revision":<integer>,"created":["<key>",...],"unchanged":["<key>",...]}` with both key lists sorted. If not paused, return `409` `import_requires_pause`; if control revision differs, return `409` `revision_conflict`.

### `GET /v1/occurrences`

Authenticated, bounded inventory for rollback and operations. Query parameters: optional `schedule_key`, `limit` 1–100 (default 100), and opaque `cursor`. If `schedule_key` is supplied, it must refer to a schedule that currently exists; otherwise the endpoint returns `404` `schedule_not_found`. A valid schedule with no occurrences returns an empty array. Without the filter, inventory includes orphaned occurrences retained after their schedule is deleted. Returns `200` `{"occurrences":[...],"next_cursor":"<opaque or null>"}` ordered by creation time then occurrence UUID. Each item is exactly:

```json
{"occurrence_id":"79c2d7b2-96ee-4eb3-ad6d-81969a0e348e","schedule_key":"news:42","schedule_revision":3,"scheduled_for":"2026-10-23T17:30:19.728869Z","target_alias":"news-worker","status":"leased","lease_expires_at":"2026-10-23T17:30:49.728869Z","lease_active":true,"attempts":1,"created_at":"2026-10-23T17:30:19.728869Z","updated_at":"2026-10-23T17:30:20.000001Z","job_id":null}
```

`status` is one of `pending`, `leased`, `blocked`, or `accepted`; `job_id` is null until a valid receipt is stored. `target_alias` is captured immutably on the occurrence even if its schedule is later deleted. `lease_expires_at` is the stored lease deadline or null when no lease is held. `lease_active` is calculated by PostgreSQL at inventory-query time as `status='leased' AND lease_until > clock_timestamp()`. An expired lease remains `status='leased'` and has `lease_active:false` until reclaimed. The response never includes lease tokens, target URLs, tokens, raw payload, or log text. `404 schedule_not_found` is returned for an unknown schedule filter.

### `POST /v1/occurrences/{occurrence_id}/retry`

Operator action for a blocked delivery after its target configuration or receiver has been corrected. Requires the scheduler API bearer token and an empty JSON object body `{}`. For `blocked`, transitions the same occurrence and unchanged envelope to `pending` with `available_at` set to now; attempt history is retained. For `pending` or `leased`, returns `200` `{"occurrence_id":"<uuid>","status":"<current status>"}` without changing state. For `accepted`, returns `409` `occurrence_not_retryable`; accepted work is never replayed. Unknown occurrence returns `404` `occurrence_not_found`.

## Calendar compatibility

Daily, hourly, and interval calculations preserve the behavior of `news_aggregator/services/scheduler.py::calculate_next_run` for imported legacy schedules: daily chooses the configured wall-clock time strictly after `now`, then skips to a selected weekday if needed; hourly chooses the configured minute in the next local hour and applies the existing selected-weekday adjustment; interval clamps to 1–1440 minutes and adds the interval to `now`. The legacy function uses `pytz`-aware `datetime.replace` and timedelta arithmetic, which retains the offset attached to the current local time across a DST transition. Golden fixtures record that behavior, including Belgrade's spring and autumn transitions; Go must match fixture outputs for imported supported types. Generic `weekly` is a new type defined as the configured local hour/minute on the next matching ISO weekday strictly after `now`; it is not claimed as legacy parity.

When a due schedule is observed, one occurrence is created for the stored `next_due_at`; the scheduler advances `next_due_at` in the same transaction to the first calculated time strictly after the transaction's captured current time. This emits at most one overdue occurrence after downtime and prevents catch-up storms. Schedule calculation uses an injected clock in tests; the service clock is UTC and converts through the configured IANA timezone.
