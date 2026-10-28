# News production environment preparation

The production target is one root-owned `/root/news/.env` file with mode
`0600`. Each Compose service receives only the environment keys it already
used. Never attach the whole file to every container with `env_file`.

## Prepare a private candidate

The Go helper is a development candidate. A synthetic Docker Compose round-trip
passed locally; the final migration and image acceptance gates are still open.

Run the helper on the host that has the current production Compose file and
its environment files. Choose a new output directory; the helper refuses to
overwrite one:

```sh
go -C go-tools run ./cmd/prepare-runtime-env \
  --compose /root/news/docker-compose.yml \
  --output-dir /root/news/env-candidate-2026-10-26 \
  --project news
```

The helper loads `/root/news/.env` for Compose interpolation when that file
exists beside the Compose file. For a differently named interpolation file,
pass `--source-env /absolute/path/to/source.env`. An explicitly supplied path
must exist. Host shell variables are excluded from interpolation, so they
cannot shadow values from the selected file. The audit records the selected
interpolation file's absolute path (or `null` when there is none), never its
contents.

The helper reads `docker compose config --format json` into memory and writes
three private files: `compose.candidate.json`, `.env`, and `audit.json`. The
source Compose file, interpolation file, and service environment files are
read-only inputs. The
candidate directory is mode `0700`; every output file is mode `0600`. The audit
contains absolute environment-file references, service/key mappings, and
counts. It contains no values or secret hashes.

Rendered environment values use Compose's `$$` escape for a literal dollar.
The helper decodes that escape once when writing dotenv environment values.
It copies every non-environment Compose field unchanged, preserving escaped
dollars in healthchecks, commands, and labels. Empty values remain present and
are allowed; a missing key fails Compose's required-variable check. Values
that cannot be represented without changing bytes, including an odd trailing
backslash or an odd run of backslashes immediately before an apostrophe, fail
closed.

Before applying a candidate, render it with its generated environment file and
compare it with the current production render. Compare effective environment
strings after decoding the rendered `$$` escape; all non-environment fields
must match exactly. Review the audit to confirm all service mappings and source
references. Apply only after all five production roles match: `app`,
`background`, `executor`, `extractor`, and `egress`.

## Key mapping

The app and executor used the same container variable name for different
database roles. Keep their host keys separate and map them back to the
application's existing `DATABASE_URL` name:

| Production role | Container environment key | Canonical key in `.env` |
|---|---|---|
| `app` | `DATABASE_URL` | `WEB_DATABASE_URL` |
| `executor` (and `migrate`) | `DATABASE_URL` | `EXECUTOR_DATABASE_URL` |
| `background` | `WORKER_DATABASE_URL` | `WORKER_DATABASE_URL` |
| `background` | `EXECUTION_DATABASE_URL` | `EXECUTION_DATABASE_URL` |
| `background` | `INGESTION_DATABASE_URL` | `INGESTION_DATABASE_URL` |
| `background` | `PROCESSING_DATABASE_URL` | `PROCESSING_DATABASE_URL` |
| `background` | `SCHEDULER_DATABASE_URL` | `SCHEDULER_DATABASE_URL` |

Other keys retain their existing names. A key shared across services is
written once only when its effective value is the same. A conflicting value
for the same canonical key fails closed. Different names remain separate even
when their current values happen to match; this preserves token aliases and
service boundaries.

The checked-in [`.env.example`](../../.env.example) is for local Compose. It
contains placeholders only and documents separate web, executor, scheduler,
worker, ingestion, and processing DSNs. Production DSNs and credentials stay
on the server.

## Fresh local Compose database setup

The local PostgreSQL image starts with database `newsdb`, owner/login
`newsuser`, and schema `public`. The migration command creates the application
tables in that schema. The ingestion and processing runtime users do not exist
in a fresh database; create them after migration and before starting the
background service. Their role templates default to schema `news`, so pass
`news_schema=public` for this local profile.

Start only the two local databases and run the one-shot migration:

```sh
cp .env.example .env
# Replace the development placeholders in .env first.
docker compose --env-file .env -f docker-compose.example.yml up -d postgres scheduler-postgres
docker compose --env-file .env -f docker-compose.example.yml run --rm migrate
```

Copy and run the reviewed, least-privilege local role templates. `\password`
prompts for each runtime password without placing it in SQL or shell history.
For the processing callback, `newsuser` is the existing local application
database role and receives only the template's fence-function `EXECUTE` grant.

```sh
docker compose --env-file .env -f docker-compose.example.yml cp \
  deploy/news-ingestion/provision-runtime-role.sql postgres:/tmp/news-ingestion-role.sql
docker compose --env-file .env -f docker-compose.example.yml exec -it postgres \
  psql -U newsuser -d newsdb -v news_schema=public -f /tmp/news-ingestion-role.sql

docker compose --env-file .env -f docker-compose.example.yml cp \
  deploy/news-processing/provision-runtime-role.sql postgres:/tmp/news-processing-role.sql
docker compose --env-file .env -f docker-compose.example.yml exec -it postgres \
  psql -U newsuser -d newsdb -v news_schema=public \
  -v news_callback_role=newsuser -f /tmp/news-processing-role.sql
```

Set the passwords entered at the prompts in `INGESTION_DATABASE_URL` and
`PROCESSING_DATABASE_URL` in `.env`. Use the exact template role names
`news_ingestion_runtime` and `news_processing_runtime`, and keep
`search_path=public` in both URLs. `WORKER_DATABASE_URL` uses the already
created `newsuser` role for local development. The disposable example also
sets `EXECUTION_DATABASE_URL` explicitly to that owner role for its separate
execution pool; neither owner connection may be copied to production. A future
production rollout must provision and review a separate execution role before
starting the new background version; it must not reuse the worker's credentials.
This code migration does not apply those production grants. The separate
scheduler database already initializes its `scheduler` login. Then start the
remaining services:

```sh
docker compose --env-file .env -f docker-compose.example.yml up --build -d
```

This sequence keeps the background service stopped until migration and role
provisioning finish. It uses the checked-in grant templates without widening
their production grants or changing production databases.

## Applying without unrelated restarts

Changing environment-file references can change Compose service configuration
hashes even when every effective environment value is identical. A hash change
alone is not a reason to recreate protected services. Validate the candidate
first and install its agreed Compose/environment pair without an unscoped
`docker compose up`. A separately approved executor release uses
`docker compose up -d --no-deps executor` after draining work; unchanged roles
keep their running containers. Do not recreate services merely to align stored
configuration-hash labels.
