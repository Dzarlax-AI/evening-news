# Evening News execution and orchestration contract

Status: **migration contract**. Sections headed “Current” characterize the pre-migration Python baseline at commit `1443a32aeb53d73a3d27c69bae9c1a9c41369746`; they do not describe the new runtime. The settled integration boundary below describes the replacement. Pure synthetic snapshots characterize serialization and projection, while separate PostgreSQL and composed-runtime tests exercise fencing and execution.

## Current ownership and network boundary

- The Go background worker owns the durable queue lease and claims jobs. Python `news_aggregator/executor.py` still owns acceptance/idempotency of execution attempts, Python execution receipts, singleton guard, callbacks, dispatch, timeout enforcement, result projection, legacy completion application, and recovery notifications.
- `news_aggregator/services/background_execution.py` dispatches all eight kinds. The old worker class remains used for timeout policy and other code; this contract does not imply the module can be removed wholesale.
- The existing private executor protocol is under `/internal/v1/`. Its worker auth compares `Authorization` to exactly `Bearer <JOB_EXECUTOR_TOKEN>`. Missing configured token is 503; a missing, malformed, wrong-case, or wrong token is 401. There are no docs/openapi routes.
- Current routes:

| Method and path | Behavior / success | Notable failures |
|---|---|---|
| `GET /internal/v1/status` | `{executor_instance_id, active_executions, ready}` | 503 when guard, runtime services, or DB readiness fails |
| `GET /internal/v1/job-executions/{job_id}/policy` | `{timeout_seconds}` for persisted job | 404 unknown job; authenticated, guarded |
| `POST /internal/v1/job-executions` | 202. New: `{job_id, attempt_generation, executor_instance_id, state, accepted_at, duplicate:false}`. Existing identical digest: same receipt projection and `duplicate:true` | 409 changed envelope or stale worker/job attempt; 422 invalid envelope/deadline/stored payload; 503 executor busy |
| `GET /internal/v1/job-executions/{job_id}/{attempt_generation}` | Complete receipt | 404 missing/invalid attempt |
| `POST /internal/v1/job-executions/{job_id}/{attempt_generation}/events` | Accepts only `{"event":"finalized"}` and returns legacy completion/alert state | 409 stale lease or nonterminal/noncurrent attempt; 422 invalid/unknown event |
| `POST /internal/v1/worker-events` | Accepts only `{"event":"recovered","owner_id":"<canonical UUID>","generation":N}`; returns `{event:"recovered", schedules_reconciled:N}` | 409 stale worker lease; 422 malformed event |
| `POST /internal/v1/job-executions/{job_id}/{attempt_generation}/cancel` | Complete receipt plus `execution_stopped` and `current_executor_instance_id` | 404 missing receipt |

All these endpoints authenticate and require the Python executor DB singleton guard. Auth check precedes route body processing. Executor `HTTPException` errors use FastAPI's `{detail: ...}` JSON shape. The Go client rejects non-2xx, bounds response bodies to 8192 bytes, rejects unknown response fields, and treats receipt result raw JSON over 4096 bytes as invalid.

## Current main execution envelope

The current worker → Python acceptance body is exactly these six fields; no `backend_pid` is sent by the current Go client:

```json
{
  "version": 1,
  "job_id": "canonical-lowercase-hyphenated-uuid",
  "owner_id": "canonical-lowercase-hyphenated-uuid",
  "generation": 7,
  "attempt_generation": 3,
  "deadline_at": "2026-10-27T10:15:30Z"
}
```

The parser rejects duplicate JSON object keys at any nesting level, non-finite JSON constants, invalid UTF-8/JSON, non-object input, any missing or extra field, and bodies larger than 8192 bytes. `version` must be exactly integer 1; `generation` and `attempt_generation` must be exact integers in `1..2^31-1` (booleans are rejected despite Python's `bool` subclassing `int`). UUID values must parse and round-trip to the exact canonical string. `deadline_at` must be a string parseable as ISO timestamp with an explicit offset; `Z` is accepted. The parser converts the instant to UTC for comparison but retains the original string in the canonical digest input. The digest is SHA-256 over UTF-8 compact JSON with sorted keys for the six canonical fields, retaining the input `deadline_at` spelling. Therefore distinct offset spellings of the same instant produce distinct hashes. The immutable fixture fixes one digest example.

At admission, deadline must be strictly in the future and no more than 86400 seconds ahead, checked against application time and again against DB clock. Admission validates current worker owner/generation/heartbeat and the persisted job state/attempt under locks. Existing `(job_id, attempt_generation)` with same digest is a duplicate; a different digest is 409. The current worker lease row supplies `backend_pid`; the database advisory lock is rechecked by the fence before receipt writes. Python's executor instance UUID is process-scoped.

Receipt fields are `job_id`, `attempt_generation`, nullable `owner_id`, `generation`, nullable `deadline_at`, `executor_instance_id`, `state`, `accepted_at`, `started_at`, `finished_at`, `result`, and nullable `error_code`. States emitted/accepted by the current Go client: `accepted`, `running`, `succeeded`, `failed`, `interrupted`, `delivery_unknown`. Persisted timestamps may be null; receipt serialization uses ISO form and UTC `Z` for deadline. Error codes observed in execution: `worker_error`, `worker_interrupted`, `external_outcome_unknown`; `error_code` is a safe code, not an exception message.

## Current payload contract: eight job kinds

Payload JSON is canonicalized by sorted keys and compact separators for its 4096-byte cap. Unknown fields are rejected. Schedule identity fields are optional but must appear together; `schedule_id` is a positive exact integer and `scheduled_for` must be a string whose `datetime.fromisoformat(...).isoformat()` is byte-for-byte equal (thus canonical, and may be naive). `timeout_seconds` where allowed is an exact integer `1..86400`.

| Kind | Accepted payload fields and defaults | Execution behavior |
|---|---|---|
| `news_processing` | `schedule_id`, `scheduled_for`, `timeout_seconds` | `run_full_cycle`; fatal error raises. Does not send a digest. |
| `news_digest` | schedule fields; `run_processing=true`, `send_telegram=true`; optional `timeout_seconds` | Processing step if enabled, then digest step if enabled. Delivery is considered possible unless `send_telegram` is explicitly false. |
| `telegram_digest` | schedule fields; optional `timeout_seconds` | Digest step only; delivery possible. |
| `backup` | schedule fields; optional `timeout_seconds` | Creates backup and returns byte count. |
| `reprocess_failed` | schedule fields; `limit=20` at executor dispatch (validator only validates supplied limit `1..100`); `dry_run=false`; optional `timeout_seconds` | Calls due retry processing with limit/dry-run. |
| `article_reprocess` | `article_ids` required, 1..100 distinct positive exact integers; sorted on normalization; `force=true`; `reextract_media=false` | Optionally refreshes each selected article's media (per-item exceptions logged and ignored except stale fence), then selected article processing with `reset=false`, preserve completed stages. |
| `summaries_generate` | no fields | Generates today's per-category daily summaries. |
| `source_test` | `source_id` required positive exact integer | Reads source snapshot, probes without persisting enabled-state mutation. Probe failure becomes receipt `failed/worker_error`, with `connection_ok:false` and warning result. |

`schedule_id`/`scheduled_for` are not admitted for `article_reprocess` or `summaries_generate`; payload-specific flags are only for the kinds in the table.

## Current timeout precedence

Default seconds by kind: `telegram_digest=600`; `news_processing=3600`; `news_digest=3600`; `backup=1800`; `reprocess_failed=1800`; `article_reprocess=3600`; `summaries_generate=3600`; `source_test=300`.

For schedule-key jobs only, a valid payload `timeout_seconds` takes precedence and is capped at 86400. Otherwise, if payload has `schedule_id`, the persisted schedule `task_config.timeout_seconds` is coerced with `int`; positive values override default and are capped at 86400; missing, invalid, zero, or negative values leave the default. The job runner applies the immutable deadline as a second cap: actual wait is `min(configured_timeout, deadline-now)` and nonpositive remainder times out immediately. At present the executor asks this Python method for the configured policy before running.

## Current result projection and completion behavior

The durable result is a content-free projection, JSON encoded compactly and capped at 4096 bytes. `_bounded_result` projects recognized scalar metrics and statuses, then `_safe_result` rejects unknown keys and enforces bounds. It does not persist arbitrary errors, source URLs, article text, prompts, or exception strings.

- Accepted statuses: `success`, `warning`, `deferred`, `dry_run`, `completed`, `rejected`, `exhausted`, `retry_wait`, `incomplete`, `settled`, `busy`.
- Recognized boolean fields: `success`, `settled`, `retryable`, `dry_run`, `connection_ok`.
- Recognized nonnegative finite metrics, clamped to 1,000,000,000: `articles_fetched`, `articles_processed`, `articles_summarized`, `articles_categorized`, `articles_requeued`, `articles_rejected`, `articles_retrying`, `articles_exhausted`, `total_candidates`, `found_candidates`, `processed`, `improved`, `summaries_generated`, `parts_sent`, `errors_count`, `duration_seconds`, `backup_bytes`, `requeued_count`.
- `errors` list is reduced to `errors_count`; count is capped at 1,000,000,000. Errors or `error`/`fatal_error` turn absent/`success` status into `warning`; an explicit non-success recognized status is preserved. NaN/infinity/bool metrics are omitted.
- Source test derives `connection_ok` from truthiness of `success`; false forces warning and the executor stores a failed receipt.
- For `reprocess_failed`, `dry_run` output reflects whether input status was exactly `dry_run`.
- Selected article outcomes allow states `completed`, `rejected`, `exhausted`, `pending`, `retry_wait`, `processing`; safe reasons are `advertisement`, `resource_limit`, `category_mapping_failed`, `missing_summary`, `smart_filter_rejected`, `extraction_challenge`, `provider_timeout`, otherwise reason is `other`. Projection keeps at most the first 20 valid outcomes when more than one exists; result includes `article_outcomes_total` (clamped to at least valid count and at most 100 when supplied) and `article_outcomes_omitted`. Any non-completed valid selected outcome upgrades `success` to `warning`, but an explicit recognized status such as `incomplete` remains. `_safe_result` permits up to 100 outcomes. `article_id` validation here uses `isinstance(int)` and consequently technically accepts bool, though projection also requires state and this edge is not a desired Go behavior to preserve unless parity evidence requires it.
- Execution exceptions and timeout are reduced to `{status:"warning"}`. Delivery-capable job timeout/cancellation/error becomes receipt `delivery_unknown` / `external_outcome_unknown`; other timeout/error becomes `failed` / `worker_error`, while cancellation for a non-delivery job becomes `interrupted` / `worker_interrupted`. Stale lease loses authority to write a terminal receipt.

Legacy `finalized` is a separate idempotent side effect. The first application synchronizes terminal job state/result to a missing or still-active receipt if needed, marks `legacy_completion_applied_at`, updates a still-running schedule unless it was edited after job creation, and maps `succeeded` + ordinary result to schedule success (except warning/deferred); delivery unknown stays unknown. Failed/deferred schedules get retry times (10/5 minutes); disabled schedules clear next run. It runs source-alert policy and operational warning/recovery behavior keyed by schedule identity. Alert state is `not_needed`, `attempting`, `delivered`, `unknown`; if a process restarts after `attempting`, it becomes `unknown`, and notification is not blindly repeated. `recovered` reconciles running schedules that lack active queued/running/interrupted jobs; digest delivery possibility yields `delivery_unknown` and no automatic digest resend.

## Current orchestration and digest behavior

`NewsOrchestrator._run_full_cycle()` currently does Go article ingestion+save, then Go article processing, combines aggregate counts/errors, persists processing stats, and returns. Selected processing receipt fields (`success`, `settled`, `retryable`, `article_outcomes`, totals/omitted) are deliberately stripped from full-cycle output. A fatal/unconfirmed ingestion result stops before AI/article processing. Full-cycle processing does not send Telegram.

Digest work is separate. `news_digest` can run processing then `send_telegram_digest`; `telegram_digest` only calls the digest method. `send_telegram_digest()` reloads Telegram settings from DB, checks whether any `DailySummary` exists for the current UTC date, and if the count is zero invokes `_generate_daily_summaries(timeout=300)` itself. It then builds digest content, may prepare rich Telegram content, sends either one rich message or fallback HTML, or multiple split parts; the first split part receives the full-site link. Thus orchestration in Go must explicitly preserve **check → generate if absent → prepare content → send**, while accounting for AI summary generation inside the current send method. If no summaries can be built, it returns a failed result. Partial split sends can occur before overall failure; after ambiguous delivery, do not resend automatically.

## Proposed Go/domain contract (new; not current behavior)

The new private Python leaf API should expose a finite, versioned set of typed operations (for example summary existence/generation, digest preparation, digest delivery, backup, and explicitly retained operational policy). It must not accept arbitrary Python method names, SQL, or URLs. Each operation request carries a version, operation discriminator, typed bounded input, input fingerprint, and the expanded execution fence:

```json
{
  "version": 1,
  "job_id": "canonical UUID",
  "attempt_generation": 3,
  "owner_id": "canonical UUID",
  "generation": 7,
  "backend_pid": 12345,
  "executor_instance_id": "canonical UUID",
  "deadline_at": "RFC3339 timestamp",
  "operation": "typed-operation-name",
  "input_fingerprint": "64 lowercase hex chars",
  "input": {}
}
```

The example above illustrates fence membership; the settled wire format below nests that eight-field fence under `envelope` and additionally pins the domain process. It is not the existing six-field worker acceptance envelope. Go obtains backend PID from the locked worker status, owns execution instance and receipt state, and supplies all fence members to Python leaf calls. Python validates operation-specific fields, exact current execution identity and deadline, then rechecks the fence transactionally before writes or publication. Input fingerprint binds the idempotent leaf request. Network/AI work must occur outside DB write locks.

Go owns lifecycle, timeout selection, attempt receipt/idempotency, result projection parity, schedule completion application, and full step ordering. Python may retain finite domain operations and operational alert policy behind an explicit leaf call, but cannot resume ownership of execution lifecycle. For any leaf call with ambiguous outcome, first reconcile an operation receipt/outcome where available; otherwise mark delivery outcome unknown and never automatically resend. A cache local to one domain process is not delivery idempotency. Preserve external delivery's possibility of partial completion. No user content or secrets go into receipts or diagnostics.

## Synthetic snapshot scope and unverified behavior

`tests/fixtures/execution-contract-v1/` freezes a synthetic envelope/digest, normalized payload examples for all eight kinds, and bounded result projection examples. `tests/test_execution_contract_snapshot.py` characterizes these pure Python functions. It has no DB fixture and invokes no real database, site, Telegram, AI, selector provider, or network. It does not verify PostgreSQL lock/fence/recovery behavior, endpoint authentication over ASGI, real provider behavior, or Go parity. PostgreSQL-specific statements above are based on code inspection and existing PostgreSQL tests; do not label them locally verified by these fixtures.

## Settled integration boundary for implementation

The existing six-field worker API and the eight-field ingestion/processing envelope stay unchanged. New Go execution adds authenticated `GET /internal/v1/job-executions/{job_id}/{attempt}/context` while an attempt is active. Its response is `{version:1,envelope:<eight-field fence>,executor_backend_pid:<execution guard physical PID>,kind,payload}`. The extra PID is outside the envelope and its digest. Python checks this authority before work and uses both worker and execution advisory-lock/PID checks in its transactional fence; losing either authority rejects late writes. Go and Python singleton advisory XOR keys must differ.

A fenced transaction that locks and validates `background_worker_status` linearizes before a replacement worker can update that row. After the replacement generation commits, stale-generation transactions fail validation. The advisory-lock check does not retroactively abort a transaction that already passed validation and holds the worker row lock when the Go advisory session is released. Cancellation prevents new steps; in-flight external requests can still have an ambiguous outcome and must not be resent automatically.

The new domain service status contains its process UUID (`domain_instance_id`), readiness and active operation count; it does not advertise job-executor readiness. The worker probes Go execution status instead. Domain startup does not wait for an active Go attempt. Regular leaf operations use `POST /internal/v1/domain-operations` and a bounded read-only operation-status endpoint. The request shape is `{version:1,operation,operation_id,domain_instance_id,envelope:<eight-field fence>,input_fingerprint,input}`. Operation IDs distinguish repeated per-article operations. The fingerprint is SHA-256 of compact sorted JSON input. Operation kind and inputs must be allowed by the persisted job kind/payload, not only by possession of the bearer token. Requests are pinned to the observed domain process UUID; after domain restart, missing local operation state must not justify resending publication.

The deterministic operation ID uses SHA-256 of the UTF-8 namespace `evening-news-domain-operation-v1\x00` followed by the compact JSON array `[job_id, attempt_as_string, operation, logical_id]`. Its first 16 bytes receive UUID version-5 and RFC variant bits. This is a project-specific algorithm, not the standard UUIDv5 hash algorithm. It deliberately excludes the input fingerprint so that changed input under the same operation identity conflicts. A shared test vector fixes this encoding. Expanded child envelopes use the persisted deadline in UTC at PostgreSQL microsecond precision; only the original six-field admission digest retains the caller's deadline spelling.

Finite operations: `summary_status` (read count), `summaries_generate` (existing AI summary transaction), `digest_prepare` (existing formatter, no AI or sending), `digest_publish` (one prepared digest, no generation), `media_refresh` (selected article only), `source_test` (job source only), `backup` (existing backup operation), and `stats_publish` (safe aggregate stats). Preparation can return an opaque bounded token owned by that domain instance; publishing binds the token to the same job/attempt. Cached operation results are bounded and attempt/deadline scoped. A duplicate with different input is a conflict; if a delivery POST might have arrived, its status must be reconciled or classified unknown, never blindly replayed. Domain calls occur outside database transactions.

Full-cycle stats round `duration_seconds` to millisecond precision before hashing and sending. This matches receipt precision and avoids different Go/Python exponent spellings for tiny fractions in the input fingerprint. Counts are bounded integers. The client validates every receipt identity and fingerprint after both an ordinary POST response and a lost-acknowledgement GET reconciliation.

Go finalization applies legacy schedule fields and `legacy_completion_applied_at` under the current worker lease. For a scheduled job whose alert was not decided, it sets the existing `legacy_alert_state` to `pending` atomically; unscheduled jobs use `not_needed`. Python retains operational source-alert classification/delivery as a separate terminal leaf, not the execution lifecycle. `POST /internal/v1/domain-events/finalized` accepts version, job_id, attempt_generation (including legacy zero), current worker owner_id/generation/backend_pid, current executor_instance_id and executor_backend_pid. It validates both advisory authorities and the terminal job/receipt under locks. A pending alert can be claimed once; attempting from a lost process becomes unknown. This terminal leaf does not reset schedule fields or change the execution state. It deliberately accepts an expired execution deadline only for terminal notification, never ordinary writes/AI callbacks. Go's finalized event returns the existing worker-compatible response. Recovery of orphan schedules belongs to Go; source-alert/provider implementation remains Python. Existing schema already admits pending alert state, so this split needs no new table/column.
