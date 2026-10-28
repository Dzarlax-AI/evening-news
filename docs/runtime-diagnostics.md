# Runtime incident collector

`go-tools/cmd/collect-runtime-incident` builds a small local incident bundle from JSONL fixtures or, after an explicit `--live`, from one configured SSH operator host. It does not run as a service, daemon, or scheduled task. With no mode selected it stays offline and requires `--fixture`.

## Offline replay

Fixture input is JSON Lines. Each line may be a `snapshot`, a structured `event`, or a `log` line containing a structured diagnostic event:

```json
{"kind":"snapshot","snapshot":{"captured_at":"2026-10-27T10:00:00Z","worker":{"generation":7,"pid":123,"heartbeat_at":"2026-10-27T10:00:00Z"}}}
{"kind":"event","event":{"event":"worker_heartbeat_failure","event_utc":"2026-10-27T10:00:01Z","stage":"commit","reason":"timeout","generation":7,"backend_pid":123}}
```

Run a replay with:

```sh
cd go-tools
go run ./cmd/collect-runtime-incident --fixture /path/to/fixture.jsonl --output /path/to/local-output
```

The fixture must be a regular file; FIFOs and device files are rejected. `--duration-seconds` applies to offline replay as well as live collection. Replay stops reading as soon as the duration, bundle count, or output limit is reached, including while skipping an oversized line. The collector creates a unique `incident-*` directory and writes up to three `bundle-*.json` files. It retains at most 30 snapshots in memory for the pre-event ring. A bundle contains a sanitized trigger, pre-event snapshots, the latest snapshot, an evidence timestamp, and an explicit current-snapshot status. Missing database or host sections are listed under `missing`; snapshots more than one second from the event are marked late.

## Explicit operator-run live collection

Live collection requires an SSH config host alias and these local operator settings:

| Environment variable | Value |
|---|---|
| `EVENING_NEWS_SSH_HOST` | Configured SSH host alias, not a raw connection string |
| `EVENING_NEWS_BACKGROUND_CONTAINER` | Background worker container name |
| `EVENING_NEWS_DOMAIN_CONTAINER` | Domain container providing the native log filter |
| `EVENING_NEWS_EXTRACTOR_CONTAINER` | Optional extractor/Page Monitor container name |
| `EVENING_NEWS_DB_CONTAINER` | PostgreSQL container name (production is `infra-postgres-1`) |
| `EVENING_NEWS_DB_USER` | PostgreSQL operator/read-only user |
| `EVENING_NEWS_DB_NAME` | PostgreSQL database name (production is `aistack`) |
| `EVENING_NEWS_DB_SCHEMA` | Worker table schema (production is `news`) |

Then invoke it deliberately:

```sh
cd go-tools
go run ./cmd/collect-runtime-incident --live --output /path/to/local-output
```

The database name and schema can also be supplied as `--db-name` and `--db-schema`; both CLI values and environment values are validated as PostgreSQL identifiers. Live mode needs every required setting and has no automatic startup path. The collector issues one SSH poll per second at most and stops at 30 minutes, three bundles, or 5 MiB written for that run. It keeps a 30-snapshot pre-event ring in memory and deduplicates recently observed event fingerprints.

Each database observation runs one fixed `BEGIN READ ONLY` transaction with `statement_timeout='500ms'` through `docker exec psql`. It reads `news.background_worker_status`, bounded `pg_stat_activity` fields and blocker PIDs, and numeric WAL/checkpoint counters. Checkpoint counters are extracted from row JSON so PostgreSQL 16/17 view differences produce missing metrics rather than failing the worker/activity snapshot. No schema changes, grants, data writes, or database settings are made.

The remote script reads host memory, swap and memory PSI. For the background, optional extractor, and PostgreSQL containers it inspects container ID/start/restart/OOM metadata, then reads cgroup v2 files from the host using the inspected PID; it does not run a shell inside application containers. It records current working set as `memory.current - inactive_file`, and records file cache separately, plus memory OOM counters and CPU throttle deltas. A changed container ID/start time or reset counter has no delta. Missing samples are explicit and do not establish a historical cause.

The collector reads stderr and stdout from the background and optional extractor logs. The native domain executable filters at most 64 lines and 262144 bytes before output crosses SSH; a line over 8192 bytes also fails the read. An incomplete, oversized or timed-out read records a finite collection error and does not advance that container's cursor. Each poll seeds its initial cursor at a validated UTC timestamp before metadata collection; after that, each container keeps its own last successful read boundary. This retains events across slow polls and transient log-read failures; overlapping lines are deduplicated in memory. The allowlist filter accepts both Go `slog` JSON records and Page Monitor text records, and emits only sanitized heartbeat and Page Monitor fields before any log output crosses SSH. Raw log lines are not transferred or saved, and PostgreSQL statement logs are not collected. URLs, domains, source names, selectors, HTML, JavaScript, exception text, CDP payloads, client addresses, query text, arbitrary app names and raw errors are discarded. Invalid or unknown event stages/reasons are normalized to finite values; malformed timestamps are rejected.

The remote commands do not modify files or services. Bundle directories are mode `0700` and files are mode `0600`, created exclusively and never overwritten or deleted. Repeated runs create new uniquely named incident directories; the collector does not prune old output. If an output bound is reached, collection stops.

The operator must configure SSH access, container names, database name/schema, and a database user with only the needed read access; the tool does not provision access. The SSH host must have Bash, Docker CLI access, PostgreSQL `psql`, and cgroup v2 host files; the domain container must have the native filter command. Review and protect the local bundle as operational data. A backend that disappeared before observation cannot establish whether an unacknowledged `COMMIT` became durable; the collector records observed state and does not infer commit outcome. Collection reports observed samples and finite collection errors, never a verified historical cause.
