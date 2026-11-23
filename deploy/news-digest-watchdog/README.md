# Independent digest watchdog

This one-shot operator command runs separately from News. The example timer checks every five minutes. Installation and activation are operator steps. Production installation, notification ownership changes and real Telegram sends require separate authorization.

Build from `go-tools` with `go build -o news-digest-watchdog ./cmd/digest-watchdog`. Put the binary at `/usr/local/bin/news-digest-watchdog` and create a dedicated unprivileged `news-watchdog` system user before installing the example service and timer. Place `/etc/news-digest-watchdog/watchdog.env` in a root-owned directory with restrictive permissions. Use systemd EnvironmentFile syntax. Provide these values privately:

- `WATCHDOG_SCHEDULER_DATABASE_URL`: read-only scheduler PostgreSQL connection URI.
- `WATCHDOG_NEWS_DATABASE_URL`: read-only News PostgreSQL connection URI with the News schema in its search path.
- `WATCHDOG_SCHEDULER_SCHEMA`: scheduler schema name, default `public`.
- `WATCHDOG_TELEGRAM_TOKEN`, `WATCHDOG_TELEGRAM_CHAT_ID`: a service bot and service destination. These are separate private operator credentials and are never retrieved from News credential storage.
- `WATCHDOG_MONITOR_FROM`: required RFC3339 timestamp; only occurrences scheduled at or after this instant are monitored. Choose the activation boundary explicitly to avoid alerts for old backlog.
- `WATCHDOG_ARCHIVE_URL`: optional HTTPS archive URL included in alerts.

Do not paste the private environment file into tickets or logs. Both databases require dedicated read-only credentials. Scheduler access requires SELECT on `schedules`, `occurrences`, and `control`. News access requires SELECT on `background_jobs`, `background_job_executions`, `settings`, `editorial_digest_editions`, `digest_delivery_intents`, `digest_delivery_parts`, and `viber_digest_deliveries`. Grant only the needed tables to the new roles, review whether `settings` access meets local policy, and restrict network reachability. The database must include the digest retry migration before enabling this tool. The command does not write to PostgreSQL.

The state directory is private to the service account. A file lock prevents overlapping runs, and a synchronously persisted reservation prevents repeated alerts after restart. This gives at-most-once notification attempts: a crash after reservation, a Telegram rejection, or an ambiguous network response can result in no notification, and the timer deliberately will not retry it. Monitor service failures separately. Preserve state on upgrades; deleting it can repeat notifications.

The command caches expected dates while the scheduler is reachable, checks current paused/enabled schedules, waits on an eligible retry, reports confirmed unknown delivery immediately, and reports missing/unconfirmed outcomes after 30 minutes. Delivery evidence uses job settings snapshots and per-destination receipts. On database failure it reports that verification is unavailable using the last cached expected occurrence. A host outage cannot be detected until the host returns. Changes to pause or delivery settings cannot be observed during a database outage. First startup during an outage cannot infer a schedule it never observed.

Before activation, validate the SQL and grants against isolated copies of the scheduler and News schemas. Test with fake notifications and paused, disabled, partial, unknown and retry cases. After separately authorized activation, verify timer health and a real scheduled user path. Set `DIGEST_WATCHDOG_OWNS_ALERTS=true` in the domain runtime only after verifying the independent sender. To return ownership to the domain, restore that setting to `false`, stop and disable the timer, and preserve the state directory and delivery receipts. Keep ambiguous deliveries in their existing state.
