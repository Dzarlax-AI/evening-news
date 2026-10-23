# Background worker

`BACKGROUND_EXECUTION_MODE=worker` separates the Python web process from
background execution. The `embedded` mode remains available for manual
operations. Scheduled commands require worker mode because calendar timing and
dispatch belong to the standalone Go scheduler; the Python schedule loop has
been removed.

## Responsibilities

- Web: authenticated administration, source CRUD, public reads, durable job
  submission, and job status.
- Python worker: accepted scheduled jobs, collection through Go, article processing,
  Gemini calls, summary generation, and the existing digest delivery.
- Go service: source dispatch, RSS/Atom, Telegram and Page Monitor capture/parsing,
  and article extraction. Its native render limit is configured independently.

Both Python processes share PostgreSQL and use the same application image with
different entrypoints. This is an execution boundary with a shared schema;
there is no new message broker or public worker execution port.

## Configuration and startup

Use matching application versions and the same database for web and worker.
Select `BACKGROUND_EXECUTION_MODE=worker` explicitly in both processes. The
required authenticated Go service URLs and tokens are validated at application
startup.

`SCHEDULER_ENABLED` is a deprecated inert compatibility setting; it does not
enable or disable Go scheduling. `WORKER_POLL_INTERVAL_SECONDS` defaults to 1,
`WORKER_SHUTDOWN_DRAIN_SECONDS` to 20, and `WORKER_JOB_RETENTION_DAYS` to 7.
The worker refuses to start unless worker mode is explicitly selected.

`WORKER_MAX_JOB_ATTEMPTS` defaults to 3 (allowed range 1–10). After repeated
interrupted execution reaches this limit, recovery marks that job `failed` with
`worker_recovery_exhausted` so later queued work can proceed. The job remains
inspectable; articles and saved stages are retained. This job recovery limit is
separate from article retry counters and does not change browser-busy refunds.

The web process applies the application's additive migrations at startup. The
worker requires a compatible schema; it does not apply migrations itself. The
new migration creates the durable jobs, idempotency keys, and worker ownership
metadata. Queue acceptance must fail closed if the required schema is missing.

Entrypoints:

```bash
# Web, with background execution disabled in this process by worker mode
uvicorn news_aggregator.main:app --host 0.0.0.0 --port 8000

# Background execution, with the same code/image and explicit worker mode
python -m news_aggregator.worker
```

Override the application image's HTTP healthcheck for the worker container:

```yaml
healthcheck:
  test: ["CMD", "python", "-m", "news_aggregator.worker", "--healthcheck"]
  interval: 10s
  timeout: 6s
  start_period: 30s
  retries: 3
```

This command reads the persisted heartbeat with a bounded database timeout. It
does not start a worker, open a browser, or expose a new network port.

These commands require configured environment variables. They are not an
instruction to switch production. Deploy and test the pair together in an
isolated environment before proposing a production release.

## Durable jobs

Manual and scheduled operations use the same PostgreSQL queue. The initial
worker executes one pipeline job at a time; this does not serialize individual
Go HTTP source jobs or change the Go browser render limit. At most 32 jobs may
wait in `queued` or `interrupted` state.

Job payloads contain validated IDs and flags, not article bodies, credentials,
or arbitrary source URLs. Responses expose bounded status and aggregate results.
Supported operations cover processing, summaries, article reprocessing, source
connection tests, backup creation, and digest delivery.

Results fit within 4 KiB. Selected-article jobs retain up to 20 finite article
outcomes with total and omitted counts; warning classification covers the full
selection. A forced selected-article job initializes its retry state once in the
claim transaction. Recovery preserves subsequent attempts and completed stages.

An idempotency key identifies a particular request. Reusing it with different
parameters is a conflict. Identical active manual work can share one job; aliases
remain associated with that job after completion. A scheduled occurrence uses
its schedule ID and due timestamp, so scheduler retries do not enqueue that
occurrence twice.

States are `queued`, `running`, `succeeded`, `failed`, `interrupted`, and
`delivery_unknown`. Queue acceptance is not processing success. Interrupted
processing work remains durable and resumes using the existing article stages
and attempt counters; existing native-browser busy handling still applies.

## API and administration

In worker mode, long operations return HTTP 202 with a job ID and status URL.
The administrator can read `GET /api/v1/jobs/{job_id}` using the existing admin
API authentication. First-party pages follow the job to its terminal outcome;
they must not display successful completion merely because it was queued.

The affected operations include:

- `POST /api/v1/process/run`
- `POST /api/v1/articles/{article_id}/reprocess`
- `POST /api/v1/summaries/generate`
- `POST /api/v1/telegram/send-digest`
- `POST /api/v1/sources/{source_id}/test`

Short database actions such as selected article retry retain their existing
response and coordinate with the active worker through PostgreSQL. Worker mode
has no silent fallback to execution inside web when the worker is unavailable.
Embedded mode retains the existing execution behavior.

The explicit Telegram connection diagnostic remains available. The legacy AI
category-mapping analysis endpoint returns HTTP 409 in worker mode; migrating
its interactive suggestions is outside the initial job contract. Manual category
mapping CRUD remains available.

## Ownership and recovery

A dedicated PostgreSQL connection holds the singleton worker advisory lock.
An additional fencing generation identifies the worker, and each claimed job
has its own attempt generation. Result writes must validate ownership while the
transaction remains locked through commit. A stale attempt cannot publish a
late result after another attempt takes over.

The worker maintains a persisted heartbeat independently of individual jobs.
Loss of ownership stops claims and cancels active work. A second worker cannot
start another scheduler or execute the same pipeline in parallel. Web restarts
do not cancel accepted work.

Worker startup recovers interrupted durable jobs under its database lease,
preserving accepted job identity and completed stages. Legacy numeric
`schedule_id` payloads remain recoverable, and completing or recovering those
legacy jobs may update `ScheduleSettings.next_run`. The worker does not calculate
or mutate Go-owned calendar due times.

Digest jobs can have an uncertain external outcome after a crash. Such jobs
become `delivery_unknown` and are not resent automatically. An explicit user
action is required to retry delivery. This boundary does not promise exactly-once
Telegram sends or exactly-once external AI billing.

## Data, resource limits, and rollback

Only the worker writes processing caches and domain statistics. Web status reads
use persisted metadata rather than constructing an orchestrator. Backup downloads
need access to the worker's backup files. Mount `/app/data` writable only in the
worker and share a persistent `/app/backups` volume between web and worker so
both processes see the same archives. Worker-mode upload, cleanup, and restore
API requests remain disabled; those maintenance actions require a stopped
worker and an offline procedure. Keep process log directories separate. The
worker's backup command uses its installed PostgreSQL client and does not require
a Docker socket. Keep both application images at the same revision. Allow a
container stop grace period longer than the configured shutdown drain.

Backup creation is queued and backup downloads remain available. In worker mode,
the web API rejects backup upload, cleanup, and restore with HTTP 409. A live
restore must not race accepted jobs or replace their ownership metadata.

Measure web and worker idle/active memory separately and together. Separating
Python processes adds independent interpreter and connection-pool overhead; it
does not establish a memory reduction or higher throughput. Go/browser limits
remain unchanged.

Before changing execution mode, stop/drain the worker and inventory queued or
interrupted jobs. Disabling worker mode does not migrate or delete accepted
jobs. Retain the additive tables and saved articles. Manual embedded operations
must not substitute for the Go scheduler and worker.

## Validation status

At commit `27d05e5`, the local regression suite passed with 1,206 tests and 10 skips, including real
PostgreSQL ownership, migration, stale-attempt fencing, recovery, subprocess
cleanup, and JavaScript behavior checks. Independent code review found no
remaining actionable findings after the fixes. The Docker acceptance gate passed
all 23 checks on macOS ARM64 using PostgreSQL 16 and separate web/worker
containers. It exercised source collection, extraction, and Gemini through HTTP
fixtures, a web restart during processing, singleton ownership, worker recovery,
and ambiguous Telegram delivery without automatic replay. Recovery explicitly
advanced only the synthetic article's retry deadline while its worker was stopped;
attempts and saved stages remained unchanged.

The ARM64 image matched all 132 application/UI files at commit `27d05e5`. Web
and Python worker together used about 177–184 MiB after startup in this short fixture run
(startup snapshot: 231 MiB),
excluding PostgreSQL and fake service containers. This is not production sizing,
a real Go/browser benchmark, or a throughput claim. Fixture resources were removed.
Review follow-ups also preserve resumable processing-only digests (`send_telegram=false`)
and best-effort optional media refresh. Enabled delivery retains the conservative
no-replay rule. A fresh focused PostgreSQL/runtime/UI run passed 53 tests after
these final corrections. The final Linux fixture gate rebuilds the current PR head.
Final head checks are tracked in [PR #18](https://github.com/Dzarlax-AI/evening-news/pull/18);
production rollout is outside this change.

To repeat the acceptance gate without publishing an image:

```bash
docker build -f docker/Dockerfile -t evening-news-background-worker:fixture .
python scripts/run_background_worker_fixture_gate.py \
  --image evening-news-background-worker:fixture \
  --output-dir artifacts/background-worker-fixture
```

The harness creates uniquely labeled resources on an internal-only Docker network,
mounts only read-only test fixtures, uses dummy credentials, and removes only its
own containers and network. It never accepts a production endpoint.

The gate uses the real `/admin/` Basic-auth page to obtain the JWT cookie and
checks the public article-reprocess endpoint with it. A separate diagnostic
found a pre-existing HTTP 500 in the auxiliary `/auth/login` endpoint (its
dependency returns a string, while the handler reads `.username`). That endpoint
is unchanged by this PR; the tested administrator cookie path succeeds.
