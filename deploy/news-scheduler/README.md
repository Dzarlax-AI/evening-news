# Optional standalone scheduler profile

The standard News installation runs the scheduler inside the Go background process. These Compose overlays are retained for operators who deliberately deploy it as a separate service; they are not additional steps for `make managed-up` and must not be layered onto a managed role-file installation without a reviewed configuration change.

## Configuration reference

Use the root [complete environment inventory](../../deploy/config/full.env.example) and [configuration catalog](../../go-tools/configcatalog/catalog.json). Separate checked-in env examples have been consolidated into that reference. Create only the private files required by this optional overlay, owned by the Compose operator with mode `0600`:

| File | Required configuration |
| --- | --- |
| `scheduler.env` | `SCHEDULER_DATABASE_URL`, `SCHEDULER_DB_SCHEMA`, `SCHEDULER_API_TOKEN`, `SCHEDULER_TARGET_ALIASES`, `SCHEDULER_TARGET_NEWS_WORKER_URL`, `SCHEDULER_TARGET_NEWS_WORKER_TOKEN` |
| `scheduler-client.env` | `SCHEDULER_API_TOKEN`, `SCHEDULED_JOBS_TOKEN`, `TRUSTED_HOSTS` |

The scheduler API token must match in both files. The receiver token is shared between `SCHEDULED_JOBS_TOKEN` and `SCHEDULER_TARGET_NEWS_WORKER_TOKEN`; it is distinct from the API token and administrator credentials. Preserve the installation's existing host allowlist and include the private target hostname. Do not use a wildcard.

Select an immutable `SCHEDULER_IMAGE` and set `NEWS_SCHEDULER_DIR` to the private directory containing those files. Do not copy every assignment from the full reference or print a rendered configuration containing secrets. Provider/Telegram keys do not belong in scheduler configuration.

## Boundaries and operation

The scheduler stores schedules and dispatch/outbox state in its separate PostgreSQL database and `scheduler_service` schema. Its role must not access News tables. The News receiver accepts fixed commands into the durable job queue; the Go worker claims jobs and the private Go domain handles the corresponding domain operations.

`compose.scheduler.yml` assumes a manually configured base with `app`, `worker` and the required existing networks. It adds a private scheduler service with an explicit preparation profile and starts it paused. `compose.service-mode.yml` is a separate activation overlay that requires that service to exist. They are operator templates, not a self-contained installation or the current production topology. Inspect and validate the complete Compose model before applying either.

The scheduler has no public route or published port. Private-network HTTP requires explicit opt-in, fixed target aliases and separate bearer tokens. Pause/resume uses the authenticated scheduler control API with its expected revision; changing an obsolete application scheduler flag does not pause durable dispatch. Pausing does not cancel already dispatched jobs. Wait for active leases and outbox state before maintenance; preserve schedule revisions and due instants.

Provisioning shared PostgreSQL roles or changing an installation's deployment mode requires explicit approval. Do not restart the database, change ownership, or broaden grants as part of a configuration cleanup. The default managed installer handles fresh local database/role provisioning automatically; external database setup follows the [configuration guide](../../docs/configuration.md).

The [dated release record](RELEASE_2026-10-23.md) is historical rollout evidence, not current installation instructions.
