# Standalone scheduler deployment artifacts

These files describe a private deployment overlay for the approved standalone
Go scheduler and the existing News app/worker. They do not replace the News
image, change the canonical Compose project, publish an image, or deploy
anything. Keep the existing Python scheduler as the source of truth until the
cutover checklist below is executed separately.

## Topology and guardrails

- The scheduler has no published port, Traefik labels, or public route.
- `news-scheduler-control` is an internal Docker network shared only by `app`
  and `scheduler`. The News app keeps its existing `infra` and
  `private-control` attachments; this overlay only adds the scheduler control
  link. The Python `worker` does not join `news-scheduler-control`.
- The scheduler also joins the existing `infra` network to reach
  `infra-postgres-1:5432`. `LISTEN_ADDR=:8080` binds the API on both of its
  interfaces, so the API is also reachable by services on `infra` (including
  the worker); every API request still requires the scheduler bearer token.
  Keep that token strong and scoped. The service has no published host port or
  public route. The scheduler uses a dedicated PostgreSQL database and login;
  the News database role receives no access to that database.
- The scheduler's only target is the statically configured
  `news-worker` alias at `http://app:8000/api/v1/internal/scheduled-jobs`.
  Explicit HTTP opt-ins are set on both sides because this is a private,
  internal-only network. Do not add arbitrary endpoint configuration.
- The Go container runs as UID/GID 65532 with a read-only root filesystem,
  dropped capabilities, no-new-privileges, PID limit 64, memory limit 64 MiB,
  and CPU limit 0.25. These are conservative starting limits, not production
  sizing evidence; verify health and restart/OOM counters after rollout.
- No real Gemini, Telegram, browser, or Readeck credentials belong in these
  files. The app/worker's pre-existing environment remains managed by the
  canonical News stack.

## Prepare private configuration

Copy `scheduler.env.example` to `scheduler.env` and
`scheduler-client.env.example` to `scheduler-client.env` in this directory on
the deployment host. Set both files to mode `0600`, owned by the Compose
operator. The local `.gitignore` excludes those files and snapshot artifacts.
Do not edit or reuse the existing News `.env`.

Generate two independent random credentials: a scheduler API bearer token and
a News receiver token. Put the API token in both files. Put the receiver token
in `scheduler-client.env` as `SCHEDULED_JOBS_TOKEN` and in `scheduler.env` as
`SCHEDULER_TARGET_NEWS_WORKER_TOKEN`. The receiver token is accepted only by
the News command-ingress endpoint; the API token is accepted only by the
scheduler API. Never use the administrator password or either database
password for these purposes.

Set `SCHEDULER_DATABASE_URL` to the dedicated runtime login and database. URL
encode characters in the password. Use the immutable published scheduler image
reference (a registry digest, `@sha256:...`) through the `SCHEDULER_IMAGE`
process variable when rendering Compose. Do not put it in the existing `.env`;
use the deployment environment or a separate protected release file. Set
`NEWS_SCHEDULER_DIR` to the absolute path of this private config directory in
the Compose operator environment; this locates the env files without reading
or changing the existing `.env`.

## PostgreSQL 17 provisioning

Provision the database on the existing `infra-postgres-1` PostgreSQL 17
instance with an administrator session, without restarting PostgreSQL. Keep
the password entry interactive so it does not appear in shell history or SQL
logs. The existing infrastructure administrator remains the database owner;
the scheduler runtime login owns only its service schema. Adapt only the role
password to the value stored in `scheduler.env`:

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

The runtime login is not a PostgreSQL superuser, database owner, or database
creator. Its migration can use the pre-created schema it owns. Revoking `PUBLIC`
access applies only inside the new dedicated database; do not alter grants on
the shared News database. Do not grant the News runtime role membership in
`scheduler_runtime` or connect rights to `evening_news_scheduler`. Before
starting the service, verify that the News runtime role cannot connect to
`evening_news_scheduler`; verify as `scheduler_runtime` that it has neither
`CREATE` on the News schema nor `SELECT` on representative private News tables.
Do not revoke shared `PUBLIC` privileges on existing databases to satisfy this
check. PostgreSQL system-catalog visibility is expected; the boundary is private
application data and schema modification.

## Compose validation and startup

Run Compose from the canonical News project directory and include the
canonical News files, the currently approved overlays that define the
`infra`/`private-control` networks, and `compose.scheduler.yml` in that order.
Activate `--profile standalone-scheduler` for every invocation that includes
`compose.service-mode.yml`, including operations targeting the app or worker;
its required healthy scheduler dependency must be present in the Compose model.
During paused preparation, explicitly targeting the scheduler also activates it. Render
with the immutable `SCHEDULER_IMAGE` value set in the operator environment, but
do not print or attach the fully rendered configuration: it contains
credentials from the existing application environment. Inspect a sanitized
projection for image references, service names, environment variable names,
network memberships, ports, labels, health checks, and resource caps. Confirm
the scheduler has exactly the expected `infra` and `scheduler-control`
attachments; remember that services on `infra` can reach its bearer-protected
API even though no host port is published.

The base overlay defaults both News processes to `SCHEDULE_DISPATCH_MODE=python`
and the scheduler starts paused. Start only the `scheduler` service with the
`standalone-scheduler` profile. Confirm its container is healthy and `/readyz`
is ready from a container on `news-scheduler-control`; check `/version` and
compare its build ID with the published artifact metadata. Confirm the
scheduler has no host port, public router, or unrelated network attachment.
The scheduler must stay paused until schedules have been imported and checked.

## Cutover checklist

The following steps are an operational checklist, not authorization to run
them. Production changes need their separate rollout window and operator
approval. Preserve the pre-cutover News database snapshot and the generated
inventory artifacts in the approved protected backup location.

1. **Preflight.** Record the current app and worker image digests, deployment
   Compose file order, scheduler image digest/build ID, PostgreSQL version,
   aggregate worker health/lease state, and the paused scheduler control
   revision. Keep exact News schedule rows only inside the protected on-VPS
   snapshot artifact. Do not copy production schedule data or derived metadata
   to a workstation or CI artifact, and do not record credential values.
2. **Disable only the Python schedule loops.** Apply
   `compose.cutover-drain.yml` temporarily while retaining
   `SCHEDULE_DISPATCH_MODE=python`. Recreate the app and worker through their
   normal Compose lifecycle so the existing worker can shut down gracefully.
   Keep the worker running in `BACKGROUND_EXECUTION_MODE=worker`; do not stop
   queue consumption. Wait until all scheduled News jobs are terminal and the
   worker reports a fresh healthy lease. Manual News jobs remain available.
3. **Take and inspect an offline snapshot.** Take a consistent PostgreSQL
   snapshot of the News database after the drain. Keep the snapshot and its
   derived schedule artifact on the VPS in a private, access-controlled
   location. Restore it into an isolated temporary PostgreSQL 17 instance
   reachable only through loopback from the bridge process (for example, by
   sharing the temporary database container's network namespace). Run
   `scripts/scheduler_bridge.py export` with
   `NEWS_SNAPSHOT_DATABASE_URL` pointing only to that loopback snapshot, in a
   container with no production database credentials and no egress. The bridge
   rejects non-loopback hosts and active scheduled News jobs, writes a new
   mode-0600 artifact without overwriting an earlier one, and includes the
   legacy due times in UTC with microseconds. Never point it at the production
   News DSN or copy either artifact off the VPS. Validate that each enabled
   schedule has its original non-null
   `next_due_at`, each disabled schedule has a null due time, keys are stable
   `news:<legacy-id>`, and the artifact's due timestamps exactly match the
   preserved `next_run` values after UTC normalization.
4. **Import while paused.** Read the scheduler's current
   `control_revision`; do not guess it. From the app's private network, run the
   reviewed bridge import with the offline snapshot artifact, the private
   `http://scheduler:8080` API URL, the scheduler API token, and that exact
   expected control revision. Explicit private-HTTP opt-in is required; the
   app's `SCHEDULER_API_ALLOW_HTTP=true` and the bridge's explicit private HTTP
   option must remain scoped to this internal endpoint. The scheduler contract
   requires pause and preserves exact imported due instants. Do not skip
   conflicts or retry a mutation blindly. A failed or ambiguous import leaves
   the Python loop disabled and the scheduler paused until its outcome is
   inspected.
5. **Verify before enabling.** Compare every imported key, operation, target
   alias, enabled flag, calendar, payload, and `next_due_at` against the
   snapshot; compare timestamps byte-for-byte in canonical UTC form, including
   fractional microseconds. Confirm the control is still paused and no imported
   row has been changed. Review the News facade's settings/status reads without
   creating a job. Record the import response and control revision without
   recording tokens or payloads.
6. **Switch the app and worker, then resume.** Remove
   `compose.cutover-drain.yml` from the active Compose file list, activate
   `--profile standalone-scheduler`, and apply `compose.service-mode.yml` while
   the Go scheduler remains paused. Confirm
   the app is healthy, service-mode schedule reads/status use the Go facade,
   the worker still owns its fresh lease, and adapter authentication is
   configured with the receiver token. Then, as a separate final cutover
   action, unpause the scheduler using the authenticated API and current
   expected control revision. Observe one due occurrence through scheduler
   inventory, one durable News receipt,
   and the corresponding News job completion. Do not use actual AI-provider,
   Telegram, browser, or Readeck credentials for smoke tests.
7. **Post-cutover observation.** Verify scheduler and News health, pending and
   blocked outbox counts, worker lease freshness, job/receipt counts, container
   restarts and OOM counters, and that no duplicate scheduled occurrence was
   accepted. Keep the drain overlay file, snapshot, import response, and inventory
   through the agreed observation window. Do not include the drain overlay in
   the active Compose file list after switching to service mode; do not delete outbox rows,
   occurrences, receipts, or News jobs as cleanup.

## Rollback checklist

Rollback must preserve every accepted, pending, blocked, and leased occurrence
and every News job/receipt. Do not restore the original snapshot over a live
database, delete queue data, or replay an accepted occurrence.

1. Pause Go scheduling and dispatch through the authenticated control API using
   the current revision. Wait until there is no active dispatch lease; pause
   does not cancel a request already in flight.
2. Take a fresh consistent News PostgreSQL snapshot and keep it on the VPS in
   the protected backup location. Restore it to a loopback-only isolated
   PostgreSQL 17 instance. Run
   `scripts/scheduler_bridge.py rollback-inventory` from the app's private
   network against the paused scheduler and that snapshot, using a bridge
   container with no production database credentials or egress. Retain the
   resulting protected artifact on the VPS. It contains current News-owned
   calendars with exact
   `next_run`, News job history, orphan schedule keys, and pending/blocked/
   accepted occurrence inventory. If an active lease or state change is
   detected, keep the service paused and retry inventory after it settles.
3. Review and reconcile the inventory, including any orphaned keys and accepted
   occurrence IDs, with the change owner. Restore Python `ScheduleSettings`
   only from a separately reviewed, transactional restore procedure that maps
   current scheduler state and exact due instants. The inventory command is
   read-only; the current bridge does not write schedule settings back into
   News. Do not use a stale pre-cutover configuration snapshot as the rollback
   state.
4. Keep the Go scheduler paused until that reconciliation is verified against
   the isolated snapshot and the News execution/receipt inventory. With
   separate rollback authorization, switch the app and worker back to Python
   mode, enable the Python scheduler, and only then resume it. Do not unpause
   both schedulers at once. Keep the Go database and its inventory intact for
   review; do not drop its database or schema as part of rollback.

## Daily DST policy

The approved correction computes daily schedules by local civil date. An autumn
fold uses its first occurrence only. A spring gap shifts by its actual offset
change; fully skipped dates and shifts leaving the configured local date are
skipped. Hourly, weekly and interval behavior remains unchanged. Initial import
preserves the exact existing due timestamp; subsequent daily calculation uses
the corrected policy. Verify this policy and sequential transition regressions
before cutover; do not rewrite imported due times during migration.
