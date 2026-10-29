# Configuration reference

This guide describes the configuration in this source checkout. It does not establish release or deployment readiness; use the instructions that match the version actually running. Fresh local installs use the managed profile and [minimal `.env.example`](../.env.example). The older multi-DSN Compose profile uses [`.env.legacy.example`](../.env.legacy.example) and the [legacy deployment guide](../deploy/news/README.md).

## Complete environment catalog

The machine-readable [configuration catalog](../go-tools/configcatalog/catalog.json) is the source for generators and audits. Each entry records:

| Field | Meaning |
| --- | --- |
| `key` | Environment name or documented dynamic-name pattern. |
| `owner` | Service, tool, or profile that consumes or supplies the value. |
| `type`, `default` | Expected value shape and observed default. A required value has no runtime default; a Compose default may differ by profile. |
| `secret` | Whether the value is credential material. Database URLs are marked secret because they contain credentials. |
| `store`, `reload` | Where the value is held and how a change becomes active. |
| `migration`, `precedence` | Compatibility notes and which source wins when multiple sources exist. |
| `sources` | Code, Compose, or deployment-template locations used to verify the entry. |
| `class`, `managed_roles` | Runtime/deployment classification and role manifests that allow the key. |

The advanced [`.env.full.example`](../.env.full.example) lists deployable environment names with type and default metadata. All assignments are commented and blank; they are not recommendations or usable credentials. Do not enable the whole file. Test/fixture switches, operator/CI-only process inputs, and Compose-generated container outputs are excluded because they are not installation inputs; the catalog retains them with a non-deployment classification for audits. In managed mode, secret material and most service settings live in private per-role runtime manifests rather than `.env`.

## Fresh managed installation

The managed profile needs no hand-written database URLs, passwords, signing keys, provider keys, or service tokens. Its only optional environment input is `SITE_BASE_URL`. The default local origin is `http://localhost:8000`; the profile binds the published port to `127.0.0.1`. The installer generates installation state and private role files in persistent volumes, then provisions separate News and scheduler databases and runtime roles on one PostgreSQL 17 server.

```sh
cp .env.example .env
make managed-up
make managed-token
```

Open `/setup` on the local site and use the printed one-time token to create the first administrator. It expires after 15 minutes and is consumed on claim. Configure the AI provider, Telegram, sources, categories, and schedule in the UI. AI settings are saved without making provider requests; provider calls happen when an article or digest job is run. The admin settings form does not return saved secret values.

For a hosted site, set `SITE_BASE_URL` to its HTTPS origin and configure the web trust settings for the actual reverse proxy and host. The setup command accepts a separate protected options file for trusted proxy CIDRs; it never enables forwarded administrator identity automatically. For example, create a JSON file such as `{"trusted_proxy_cidrs":["172.20.0.2/32"]}` with mode `0600`, replacing the example address with the exact proxy peer, then pass it to the one-shot installer:

```sh
docker compose -f docker-compose.managed.yml --profile setup run --rm \
  -v "$PWD/options.json:/input/options.json:ro" setup prepare \
  --options /input/options.json
docker compose -f docker-compose.managed.yml up --build -d
```

Public installations require HTTPS. The local HTTP exception requires a loopback site origin, and the managed Compose profile publishes port 8000 on host loopback.

The fresh local profile uses the HTTP-only extractor and does not connect to a shared browser. Browser-backed deployments retain their existing deployment profile; no browser lifecycle or network rules are changed by this installer.

The installer has explicit prepare, provisioning, and setup-token commands. Recovery commands operate on the same installation state and preserve encrypted credentials only when the corresponding private key material is available. A database dump by itself cannot restore encrypted provider credentials. See the [managed CLI source](../go-domain/cmd/newsctl/main.go) and [recovery implementation](../go-domain/cmd/newsctl/recovery.go) for current command behavior.

### Pre-provisioned external database

An advanced fresh-install path accepts pre-provisioned PostgreSQL databases and roles. It is separate from the default managed profile. Prepare a protected JSON input file (mode `0600`, at most 16 KiB) containing `version: 1`, a distinct `bootstrap_dsn`, and `runtime_dsns` entries for `web`, `domain`, `worker`, `execution`, `ingestion`, `processing`, and `scheduler`. All News runtime DSNs must point to the same database with `search_path=news`; the scheduler DSN uses a separately pre-created `scheduler_service` schema. The bootstrap DSN targets the same News database and schema but uses a different identity from all runtime roles. It needs CONNECT and schema USAGE plus SELECT/INSERT/UPDATE on the installer state, admin, and setup-token tables; it does not create schema objects or grant roles.

The News schema must already be fully migrated, and the tables checked by the installer must be empty. A separate scheduler database must already exist on the same PostgreSQL endpoint with an empty `scheduler_service` schema; the scheduler role needs schema USAGE and CREATE for its own objects, and must have no News database access. Runtime roles must be distinct, non-owner, non-superuser, non-`BYPASSRLS`, non-`CREATEDB`, and non-`CREATEROLE`, with the required table grants. The installer checks those boundaries, writes the installation identity and role manifests, and creates no database, schema, role, grant, or migration. Existing populated installations belong to the adoption path below.

The protected input is passed to `newsctl prepare --external-db /input/external-db.json`; optional proxy settings can be passed with `--options /input/options.json`. Use the installer-only protected bootstrap input for `setup-token` and `reset-admin`. Provisioning and the current backup, restore, and resume commands do not support external database installations. Native PostgreSQL 16 and 17 installer fixtures pass for fresh provisioning, external role/grant preflight and state verification, adoption parity/import, and recovery fault paths. This verifies those fixture paths; it does not establish production acceptance or a full external CLI/Compose startup. External database backup and restore remain unsupported. See [external install validation](../go-domain/cmd/newsctl/external.go) for the implemented preflight.

### Legacy configuration adoption

`newsctl adopt` accepts only a protected regular file containing canonical JSON from Docker Compose v2 (`docker compose config --format json`). It does not parse dotenv files, YAML, or interpolation. The JSON must use object-valued service environments containing string values; its maximum size is 1 MiB. Supported runtime groups are web, executor, background, and extractor. Keep this generated configuration in a private directory with mode `0600` or stricter; it contains secrets.

The default is a read-only dry run:

```sh
newsctl adopt --config /input/compose.json --dry-run
```

Dry-run is the default. It prints only service groups, key names, unsupported runtime keys, and that role privilege/schema-parity checks are deferred until `--apply`; it does not print configuration values, open network connections, or change files or database rows. Unsupported runtime keys keep the operation in dry-run mode until the configuration is understood.

`--apply` additionally requires a protected JSON bootstrap file (mode `0600`, at most 8 KiB) with `version` set to `1` and a `bootstrap_dsn` for the same News database/schema, plus a separate output path for the generated Compose override:

```sh
newsctl adopt --config /input/compose.json --apply \
  --bootstrap-input /input/bootstrap.json \
  --compose-output /output/adopted.override.yml
```

Before applying, stop the web, background, executor, and scheduler services. Apply checks for active sessions from all seven runtime database identities and aborts before migration/import or activation if any remain. The seven roles must be reachable non-superuser roles and must not have `BYPASSRLS`, `CREATEDB`, `CREATEROLE`, or ownership of the target database/schema/objects; the CLI does not grant roles. The separate bootstrap role needs the reviewed migration and import permissions. Apply runs the additive migration and supported imports against the existing News database in place, checks parity for existing settings, sources, categories, category mappings and schedules, and emits role files plus a Compose override for the existing PostgreSQL service. It does not copy the database or replace its volume. The adapter imports the supported administrator hash and provider credentials, and seeds the seven operational settings from legacy `NEWS_LIMIT_*` values only when there is no existing operational revision. Unsupported runtime keys fail closed. Native PostgreSQL 16 and 17 fixtures pass for import/parity, offline-session refusal, and role/grant checks. The fixture suite does not establish production acceptance or validate a full external CLI/Compose startup, nor approve applying any particular installation.

## Managed role files and precedence

The managed profile mounts one private runtime manifest per role: web, domain, background, extractor, or egress. A role reader accepts only its allowlisted keys from the catalog. Runtime services do not share a general `.env` file. The web and background manifests contain service credentials and their own database URLs; provider credentials and the domain encryption key stay in private domain state. The `managed_roles` field is the allowlist for each role reader.

When `NEWS_RUNTIME_FILE` selects a managed manifest, supported services load only that manifest and do not fall back to process environment values for missing fields or load errors. Legacy standalone services read their process environment at startup. Editing a legacy env file does not change a running service; recreate the affected service to apply it. `NEWS_RUNTIME_FILE` is a bootstrap selector, not a shared secret.

Persisted AI provider/model settings override the domain's initial provider/model defaults. In the managed profile, provider credentials are stored encrypted in PostgreSQL; deleting a stored credential does not revive an environment fallback. Telegram destinations can be stored in the settings table, with service-channel fallback to the main destination. Saving AI settings does not call a provider.

Managed operational controls are typed PostgreSQL settings, separate from environment configuration. They include the article limits (`limits_enabled`, `max_articles`, `per_source`, `days`, `newest_date`) and the `processing_enabled` and `delivery_enabled` switches. Defaults are limits off, 50 articles overall, 50 per source, one day, no upper date, and processing and delivery enabled. Each job attempt pins one settings snapshot before ingestion; processing reads that snapshot for candidate selection and delivery. In legacy mode, `NEWS_LIMIT_*` environment values retain their existing behavior without this database snapshot.

## Legacy Compose profile

The legacy [Compose example](../docker-compose.example.yml) and `deploy/` templates are separate configuration profiles. Their inputs remain documented in [`.env.legacy.example`](../.env.legacy.example) and the [legacy deployment guide](../deploy/news/README.md). A Compose input may map to a differently named environment variable inside a container. In particular, each database URL and service token belongs to a specific role or boundary; preserve those distinctions and grants when migrating.

The scheduler builds `SCHEDULER_TARGET_<ALIAS>_URL` and `SCHEDULER_TARGET_<ALIAS>_TOKEN` from `SCHEDULER_TARGET_ALIASES`: aliases are uppercased and hyphens become underscores. The `news-worker` alias uses `SCHEDULER_TARGET_NEWS_WORKER_URL` and `SCHEDULER_TARGET_NEWS_WORKER_TOKEN`. The token is secret. Targets require HTTPS unless an explicit HTTP override is enabled.

Some similarly named settings apply to different processes. Standalone scheduler and extractor use `LISTEN_ADDR`; the combined background scheduler uses `SCHEDULER_LISTEN_ADDR`. The egress service uses `EGRESS_LISTEN_ADDR`. Extractor concurrency defaults differ between standalone code and the legacy Compose profile; see the specific catalog entry and source location.

## Catalog maintenance

From `go-tools`, run the catalog drift check:

```sh
GOCACHE=/tmp/news-configcatalog-cache go test -count=1 ./configcatalog
```

To print current AST-discovered Go keys and source locations:

```sh
GOCACHE=/tmp/news-configcatalog-cache CONFIG_CATALOG_DISCOVER=1 go test -count=1 ./configcatalog -run TestPrintDiscovery -v
```

The scanner checks Go environment reads and helper calls, table-driven configuration, role allowlists, dynamic scheduler targets, Compose interpolation, container environment outputs, and checked-in deployment examples. It is bounded rather than a whole-program data-flow analysis: new helpers, scripts, or table layouts require updating discovery rules. The test verifies coverage and source references, not whether every default, secret classification, or precedence description is semantically correct; review those values against the source and profile when configuration changes.
