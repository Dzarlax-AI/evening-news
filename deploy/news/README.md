# News production environment preparation

The production target is one root-owned `/root/news/.env` file with mode
`0600`. Each Compose service receives only the environment keys it already
used. Never attach the whole file to every container with `env_file`.

## Prepare a private candidate

Run the helper on the host that has the current production Compose file and
its environment files. Choose a new output directory; the helper refuses to
overwrite one:

```sh
python3 scripts/prepare_runtime_env.py \
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

## Applying without unrelated restarts

Changing environment-file references can change Compose service configuration
hashes even when every effective environment value is identical. A hash change
alone is not a reason to recreate protected services. Validate the candidate
first and install its agreed Compose/environment pair without an unscoped
`docker compose up`. A separately approved executor release uses
`docker compose up -d --no-deps executor` after draining work; unchanged roles
keep their running containers. Do not recreate services merely to align stored
configuration-hash labels.
