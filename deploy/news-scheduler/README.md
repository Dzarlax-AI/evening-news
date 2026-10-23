# Standalone scheduler operations

The standalone Go scheduler is deployed with the News app and Python worker.
It owns calendar timing and command-delivery state in a dedicated PostgreSQL
database/schema. The News adapter accepts commands into the existing durable
job queue; Python continues to own job execution, persistence, article
processing, and delivery. See the [October 23, 2026 release record](RELEASE_2026-10-23.md)
for the verified rollout evidence.

## Topology and guardrails

- The scheduler has no published port, Traefik labels, or public route.
- `news-scheduler-control` is an internal Docker network shared only by `app`
  and `scheduler`. The app keeps its existing `infra` and `private-control`
  attachments; the Python `worker` does not join `news-scheduler-control`.
- The scheduler also joins `infra` to reach `infra-postgres-1:5432`.
  `LISTEN_ADDR=:8080` binds on both interfaces, so services on `infra` can
  reach its API; every API request still requires the scheduler bearer token.
- The scheduler's only target is the statically configured `news-worker` alias
  at `http://app:8000/api/v1/internal/scheduled-jobs`. HTTPS is the default;
  the explicit HTTP opt-in is limited to this configured private-network target.
  Do not add arbitrary endpoint configuration or user-supplied URLs.
- The Go container runs as UID/GID 65532 with a read-only root filesystem,
  dropped capabilities, no-new-privileges, PID limit 64, memory limit 64 MiB,
  and CPU limit 0.25. These are configured limits, not observed memory use.
- No Gemini, Telegram, browser, or other provider credentials belong in these
  files. Existing app/worker environment remains managed by the canonical News
  stack.

## Private configuration

Copy `scheduler.env.example` to `scheduler.env` and
`scheduler-client.env.example` to `scheduler-client.env` on the deployment
host. Set both files to mode `0600`, owned by the Compose operator. The local
`.gitignore` excludes these files and snapshot artifacts. Do not edit or reuse
the existing News `.env`.

Generate independent random credentials for the scheduler API and News
receiver. Put the API token in both files. Put the receiver token in
`scheduler-client.env` as `SCHEDULED_JOBS_TOKEN` and in `scheduler.env` as
`SCHEDULER_TARGET_NEWS_WORKER_TOKEN`. The receiver token is accepted only by
the News command-ingress endpoint; the API token is accepted only by the
scheduler API. Never use an administrator or database password for either.

Set `SCHEDULER_DATABASE_URL` to the dedicated runtime login and database; URL
encode password characters. Set `SCHEDULER_IMAGE` to the immutable published
image digest and `NEWS_SCHEDULER_DIR` to this private config directory in the
Compose operator environment. Do not put either in the existing `.env` or
print the fully rendered Compose configuration.

`scheduler-client.env.example` preserves the deployed host allowlist and adds
`app`, the internal target hostname needed by News host validation. Preserve
any other custom hosts in the existing allowlist when creating the private
file. Never use a wildcard. The original News `.env` remains unchanged.

## PostgreSQL 17 provisioning

The shared PostgreSQL 17 server hosts a separate `evening_news_scheduler`
database. The infrastructure administrator remains the database owner; the
scheduler runtime role owns only schema `scheduler_service`. The News role has
no access to this database, and the scheduler role cannot access News tables.
Provision with an administrator session without restarting PostgreSQL. Keep
the password entry interactive so it does not appear in shell history or SQL
logs:

```sql
CREATE ROLE scheduler_runtime LOGIN;
\password scheduler_runtime
CREATE DATABASE evening_news_scheduler;
REVOKE ALL ON DATABASE evening_news_scheduler FROM PUBLIC;
GRANT CONNECT ON DATABASE evening_news_scheduler TO scheduler_runtime;
\connect evening_news_scheduler
REVOKE ALL ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA scheduler_service AUTHORIZATION scheduler_runtime;
```

The runtime role is not a superuser, database owner, or database creator. Its
migrations use the pre-created schema it owns. Do not grant the News runtime
role membership in `scheduler_runtime` or connect rights to this database.
Verify the role boundary without changing grants on the shared News database;
PostgreSQL system-catalog visibility is expected.

## Compose and service operation

The checked-in `compose.scheduler.yml` is a staged template and keeps the
`standalone-scheduler` profile for preparation workflows. The final production
Compose render removes that preparation-only profile so the scheduler remains
part of ordinary future Compose operations. Any Compose model that includes
`compose.service-mode.yml` must include the scheduler service so its healthy
dependency is defined.

The deployed modes are `BACKGROUND_EXECUTION_MODE=worker`,
`SCHEDULE_DISPATCH_MODE=service`, and `SCHEDULER_ENABLED=false` on the News
processes. The last setting is inert compatibility configuration; no Python
calendar loop remains. The standalone Go control owns schedule CRUD/status and
dispatch, and the Python worker consumes its accepted durable jobs.

Pause schedule creation and dispatch through the authenticated Go control API.
`SCHEDULER_ENABLED=false` is not a pause switch, and no Compose overlay changes
Go scheduler state. Wait for active dispatch leases to settle before inventory
or rollback work; pausing does not cancel a request already in flight.

Render Compose with the scheduler image digest and private config directory
set in the operator environment. Inspect only a sanitized projection of image
references, service names, environment variable names, network membership,
ports, labels, health checks, and limits. Confirm that the scheduler has only
the `infra` and `scheduler-control` networks, no host port, and no public route.
Services on `infra` can reach the bearer-protected API.

Check container health and `/readyz` from the private control network; compare
`/version` build ID with the release record. `/healthz` checks liveness, while
`/readyz` also checks database/schema readiness and scheduler-loop freshness.
When verifying an operation, check scheduler occurrence inventory, News
receipt/job state, and pending/blocked outbox counts together. A receipt
confirms durable acceptance, not execution completion. Do not delete pending,
blocked, accepted, or leased occurrences or News jobs as cleanup.

The initial migration imported two enabled schedules with their exact verified
due timestamps. Production access checks and the first natural scheduled
execution are documented in the [release record](RELEASE_2026-10-23.md).

## Rollback

Rollback preserves every accepted, pending, blocked, and leased occurrence and
every News job/receipt. Do not restore an old snapshot over a live database,
delete queue data, or replay an accepted occurrence.

1. Pause Go scheduling and dispatch through the authenticated control API with
   the current revision. Wait for active dispatch leases to settle; pause does
   not cancel a request already in flight.
2. Take a fresh consistent News PostgreSQL snapshot and keep it in the
   protected on-host backup location. Restore it into a loopback-only isolated
   PostgreSQL 17 instance. Run
   `scripts/scheduler_bridge.py rollback-inventory` against the paused
   scheduler and that snapshot from a bridge container without production DB
   credentials or egress. The read-only Python bridge container needs a bounded
   writable temporary mount, for example `--tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m`,
   for import-time caches. Keep the resulting protected artifact on the host.
   It contains current calendars and due times, News job history, orphan
   schedule keys, and pending/blocked/accepted occurrence inventory. Expired
   leases remain visible. Retry only after an active lease or state change
   settles.
3. Review and reconcile the inventory, including orphan keys and accepted
   occurrence IDs, with the change owner. Restore Python `ScheduleSettings`
   only through a separately reviewed transactional procedure that maps
   current scheduler state and exact due instants. The inventory command is
   read-only; it does not write settings back into News.
4. Keep Go paused until reconciliation is verified against the isolated
   snapshot and News execution/receipt inventory. If the scheduler release
   must be rolled back, restore a previously approved Go scheduler/app
   release and resume only after the occurrence and receipt inventory is
   reconciled. This repository no longer contains a Python calendar runtime;
   do not switch modes or resume dispatch against an unreconciled inventory.
   Keep the Go database and inventory for review; do not drop them as part of
   rollback.

## Daily DST policy

Daily schedules use local civil dates. An autumn fold uses its first instant;
a spring gap shifts by its actual offset change. Fully skipped dates and a gap
shift that crosses local midnight are skipped. Hourly, weekly, and interval
behavior is unchanged. Initial import preserves the exact existing due time;
the daily civil-time policy applies when calculating the next due time after
an occurrence. Do not rewrite imported due times during migration.
