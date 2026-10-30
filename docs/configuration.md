# Configuration reference

This guide describes the configuration in this source checkout. It does not establish release or deployment readiness; use the instructions that match the version actually running. Fresh local installs use the managed profile and [minimal `.env.example`](../.env.example). The older multi-DSN Compose profile uses [`deploy/config/development.env.example`](../deploy/config/development.env.example) and the [legacy deployment guide](../deploy/news/README.md).

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

The advanced [`deploy/config/full.env.example`](../deploy/config/full.env.example) is the central reference for deployable environment names, including optional standalone component profiles, with type and default metadata. Separate per-component env examples have been consolidated into this file. All assignments are commented and blank; they are not recommendations or usable credentials. Do not enable the whole file. Test/fixture switches, operator/CI-only process inputs, and Compose-generated container outputs are excluded because they are not installation inputs; the catalog retains them with a non-deployment classification for audits. In managed mode, secret material and most service settings live in private per-role runtime manifests rather than `.env`.

## Fresh managed installation

The managed profile needs no hand-written database URLs, passwords, signing keys, provider keys, or service tokens. Its only optional environment input is `SITE_BASE_URL`. The default local origin is `http://localhost:8000`; the profile binds the published port to `127.0.0.1`. The installer generates installation state and private role files in persistent volumes, then provisions separate News and scheduler databases and runtime roles on one PostgreSQL 17 server. Fresh HTTP-only setup remains the zero-configuration default. A fresh install can instead attach to an existing native Cloak browser/egress stack using the protected configuration flow below; it creates no Chrome or Readeck service.

The following shortcuts use the default local loopback HTTP profile. For any HTTPS origin, run the one-shot prepare command with protected proxy options shown below before starting the profile.

```sh
cp .env.example .env
make managed-up
make managed-token
```

Open `/setup` on the local site and use the printed one-time token to create the first administrator. It expires after 15 minutes and is consumed on claim. Configure the AI provider, Telegram, sources, categories, and schedule in the UI. AI settings are saved without making provider requests; provider calls happen when an article or digest job is run. The admin settings form does not return saved secret values.

For any HTTPS site origin, including one on loopback, set `SITE_BASE_URL` to that origin and configure trust for the actual reverse proxy and host. Prepare requires a protected options file with explicit, specific `trusted_proxy_cidrs`; invalid and all-address (`/0`) ranges are rejected; use only the actual proxy peer or network. For example, create a JSON file such as `{"trusted_proxy_cidrs":["172.20.0.2/32"]}` with mode `0600`, replacing the example address with the exact proxy peer, then pass it to the one-shot installer. The local HTTP exception requires `LOCAL_HTTP_SETUP=true`, a loopback site origin using `localhost`, `127.0.0.1`, or `::1`, and every published web port bound explicitly to loopback; host-network mode is not allowed.

```sh
docker compose -f docker-compose.managed.yml --profile setup run --rm \
  -v "$PWD/options.json:/input/options.json:ro" setup prepare \
  --options /input/options.json
docker compose -f docker-compose.managed.yml up --build -d
```

Public installations require HTTPS. The local HTTP exception requires a loopback site origin, and the managed Compose profile publishes port 8000 on host loopback.

### Attach an existing native browser stack on a fresh install

Use this optional path when a fresh managed install should use the existing native Cloak browser and egress stack. Render that stack's complete current Compose configuration to canonical JSON using the same Compose files and environment options normally used to start it. Run the command from the Cloak project directory; keep the JSON private because it contains credentials. The JSON must describe the current executor, background, extractor, and egress services and their native browser, proxy, controller, and network settings. Do not hand-edit or substitute a dotenv file.

Create a private host directory for the rendered input and generated override. From the directory where the existing Cloak stack is normally invoked, render the current configuration using the same Compose files and environment options you use to run it. For example, if the stack is defined by the default Compose file in that directory:

```sh
umask 077
NEWS_PRIVATE_DIR="$HOME/.local/share/evening-news"
mkdir -p "$NEWS_PRIVATE_DIR"
chmod 700 "$NEWS_PRIVATE_DIR"
docker compose config --format json > "$NEWS_PRIVATE_DIR/native-compose.json"
chmod 600 "$NEWS_PRIVATE_DIR/native-compose.json"
```

If the stack uses explicit Compose files or an env file, include those same options before `config`. Then, from the Evening News project directory, prepare the managed install with the protected input mounted read-only and the private output directory mounted for the generated override:

```sh
NEWS_PRIVATE_DIR="$HOME/.local/share/evening-news"
docker compose -f docker-compose.managed.yml --profile setup run --rm \
  -v "$NEWS_PRIVATE_DIR/native-compose.json:/run/news/private/native-compose.json:ro" \
  -v "$NEWS_PRIVATE_DIR:/run/news/output" setup prepare \
  --browser-config /run/news/private/native-compose.json \
  --compose-output /run/news/output/news-native.override.yml
```

The command above is for the default local loopback setup. For a hosted native-browser install, set `SITE_BASE_URL` in `.env` to the HTTPS origin and create `$NEWS_PRIVATE_DIR/options.json` with exact, specific trusted proxy CIDRs (mode `0600`). Use this command instead; missing, invalid, or all-address (`/0`) proxy ranges fail closed:

```sh
NEWS_PRIVATE_DIR="$HOME/.local/share/evening-news"
docker compose -f docker-compose.managed.yml --profile setup run --rm \
  -v "$NEWS_PRIVATE_DIR/native-compose.json:/run/news/private/native-compose.json:ro" \
  -v "$NEWS_PRIVATE_DIR/options.json:/run/news/private/options.json:ro" \
  -v "$NEWS_PRIVATE_DIR:/run/news/output" setup prepare \
  --browser-config /run/news/private/native-compose.json \
  --options /run/news/private/options.json \
  --compose-output /run/news/output/news-native.override.yml
```

The setup container creates the override as root with mode `0600`. To make only the generated file readable by the current non-root host user, change that file's owner through the one-shot setup image. This preserves its private mode and does not change ownership of the input JSON, directory, or state volumes:

```sh
NEWS_PRIVATE_DIR="$HOME/.local/share/evening-news"
docker compose -f docker-compose.managed.yml --profile setup run --rm --no-deps \
  --user 0:0 --entrypoint /bin/chown \
  -v "$NEWS_PRIVATE_DIR:/run/news/output" setup \
  "$(id -u):$(id -g)" /run/news/output/news-native.override.yml
```

Run all subsequent Compose commands with both the managed base file and the generated override so the same installation volumes and native network attachments are selected. For example:

```sh
NEWS_PRIVATE_DIR="$HOME/.local/share/evening-news"
docker compose -f docker-compose.managed.yml -f "$NEWS_PRIVATE_DIR/news-native.override.yml" up --build -d
docker compose -f docker-compose.managed.yml -f "$NEWS_PRIVATE_DIR/news-native.override.yml" --profile setup run --rm setup setup-token
docker compose -f docker-compose.managed.yml -f "$NEWS_PRIVATE_DIR/news-native.override.yml" logs -f web background executor extractor egress
docker compose -f docker-compose.managed.yml -f "$NEWS_PRIVATE_DIR/news-native.override.yml" down
```

The command imports settings from the rendered configuration; it does not create a browser or egress service. The generated override is private and is written without replacing a different existing file. It preserves the exact external control and outbound network names and adds an internal extraction-call network. Executor and background retain their default network plus extraction-call access; extractor and egress have no default route. The HTTP-only profile remains the default and continues to use `make managed-prepare`, `make managed-up`, and the other `make managed-*` shortcuts without an override.

Focused CLI and Compose checks do not establish full-stack acceptance. The managed native-browser gate, including PostgreSQL 16/17 and Linux two-tab/Page Monitor coverage, must pass separately before describing this profile as accepted.

Native-browser managed installations currently fail closed for encrypted local `backup`, `restore` (including staged restore), and `resume`: the archive format does not yet contain the egress manifest and generated Compose override. HTTP-only fresh-install recovery is unchanged. The generic CLI recovery for adoption/external-database installs remains unsupported; the separate [operator settings-only recovery procedure](managed-settings-recovery.md) covers configuration recovery into a clean target without article history.

The installer has explicit prepare, provisioning, and setup-token commands. For HTTP-only local installs, recovery commands operate on the same installation state and preserve encrypted credentials only when the corresponding private key material is available. Native-browser and adopted-profile limitations are described above. A database dump by itself cannot restore encrypted provider credentials. See the [managed CLI source](../go-domain/cmd/newsctl/main.go) and [recovery implementation](../go-domain/cmd/newsctl/recovery.go) for current command behavior.

### Pre-provisioned external database

An advanced fresh-install path accepts pre-provisioned PostgreSQL databases and roles. It is separate from the default managed profile. Prepare a protected JSON input file (mode `0600`, at most 16 KiB) containing `version: 1`, a distinct `bootstrap_dsn`, and `runtime_dsns` entries for `web`, `domain`, `worker`, `execution`, `ingestion`, `processing`, and `scheduler`. All News runtime DSNs must point to the same database with `search_path=news`; the scheduler DSN uses a separately pre-created `scheduler_service` schema. The bootstrap DSN targets the same News database and schema but uses a different identity from all runtime roles. It needs CONNECT and schema USAGE plus SELECT/INSERT/UPDATE on the installer state, admin, and setup-token tables; it does not create schema objects or grant roles.

The News schema must already be fully migrated, and the tables checked by the installer must be empty. A separate scheduler database must already exist on the same PostgreSQL endpoint with an empty `scheduler_service` schema; the scheduler role needs schema USAGE and CREATE for its own objects, and must have no News database access. Runtime roles must be distinct, non-owner, non-superuser, non-`BYPASSRLS`, non-`CREATEDB`, and non-`CREATEROLE`, with the required table grants. The installer checks those boundaries, writes the installation identity and role manifests, and creates no database, schema, role, grant, or migration. Existing populated installations belong to the adoption path below.

The protected input is passed to `newsctl prepare --external-db /input/external-db.json`; optional proxy settings can be passed with `--options /input/options.json`. Use the installer-only protected bootstrap input for `setup-token` and `reset-admin`. Provisioning and the current backup, restore, and resume commands do not support external database installations. Native PostgreSQL 16 and 17 installer fixtures pass for fresh provisioning, external role/grant preflight and state verification, adoption parity/import, and recovery fault paths. This verifies those fixture paths; it does not establish production acceptance or a full external CLI/Compose startup. External database backup and restore remain unsupported. See [external install validation](../go-domain/cmd/newsctl/external.go) for the implemented preflight.

### Existing-installation adoption

`newsctl adopt` accepts only a protected regular file containing canonical JSON from Docker Compose v2 (`docker compose config --format json`). It does not parse dotenv files, YAML, or interpolation. Keep this generated configuration in a private directory with mode `0600` or stricter; it contains secrets. Adoption imports the seven runtime DSNs from the canonical per-service configuration (including the current application alias and `web`), and preserves the current extractor, egress, and private-control/outbound topology. It also carries `GOMAXPROCS` and `GOMEMLIMIT` from the background service into its generated Compose process environment; these Go runtime settings are not application manifest values. Explicitly retired legacy executor settings are omitted, while any unknown runtime key remains an adoption blocker. An optional egress manifest is imported when present. Missing or non-lossless role settings fail closed; DSNs are not expected to be typed in by hand. The installer preflights reachability through all seven DSNs and stops with an actionable error if a database is unreachable.

The default is a read-only dry run. Mount the rendered JSON read-only into the one-shot setup container:

```sh
NEWS_PRIVATE_DIR="$HOME/.local/share/evening-news"
docker compose -f docker-compose.managed.yml --profile setup run --rm \
  -v "$NEWS_PRIVATE_DIR/compose.json:/run/news/private/compose.json:ro" setup adopt \
  --config /run/news/private/compose.json --dry-run
```

Dry-run is the default. It prints only service groups, key names, unsupported runtime keys, and that role privilege/schema-parity checks are deferred until `--apply`; it does not print configuration values, open network connections, or change files or database rows. Unsupported runtime keys keep the operation in dry-run mode until the configuration is understood.

`--apply` additionally requires a protected JSON bootstrap file (mode `0600`, at most 8 KiB) containing `version: 1` and a `bootstrap_dsn` for the same News database/schema, plus a separate output path for the generated Compose override. This one-time installer input is for the additive migration only; it does not grant roles or privileges. Bind the rendered Compose JSON and bootstrap JSON read-only, and mount a private output directory for the override. Apply must run through the existing `executor` service on the original Compose network so its database hostnames resolve. Do not add `docker-compose.managed.yml` to the existing project. Select the existing Compose project name from its current canonical Compose export (the `.name` field), or use the exact `-p` value from its current startup command, and use that same name for rendering, applying, and subsequent startup. Set `NEWS_COMPOSE_ARGS` to the exact `-f` and `--env-file` arguments already used for this installation; if the existing command uses `-p`, put that value in `NEWS_PROJECT` and omit it from this array. The example paths and project name are placeholders and must be replaced before running.

Before adoption, the canonical web service must have an explicit `SITE_BASE_URL`. Any HTTPS origin, including one on loopback, requires valid, specific `TRUSTED_PROXY_CIDRS`; invalid and all-address (`/0`) ranges are rejected; use only the actual proxy peer or network. Loopback HTTP is accepted only for `localhost`, `127.0.0.1`, or `::1`, with `LOCAL_HTTP_SETUP=true` and every published web port explicitly bound to loopback, with no host-network mode. The CLI refuses missing or invalid settings. These are prerequisites, not a statement about any current production configuration.

```sh
NEWS_PRIVATE_DIR="$HOME/.local/share/evening-news"
# Replace these paths with the complete Compose file and env-file arguments
# already used for this installation. Never render or print the resulting JSON.
NEWS_COMPOSE_ARGS=( -f "/path/to/current/compose.yml" -f "/path/to/native/overlay.yml" --env-file "/path/to/current/.env" )
# Set from the existing canonical export .name or exact current -p value.
NEWS_PROJECT="existing-compose-project-name"
NEWS_COMPOSE=(docker compose -p "$NEWS_PROJECT" "${NEWS_COMPOSE_ARGS[@]}")
umask 077
mkdir -p "$NEWS_PRIVATE_DIR"
chmod 700 "$NEWS_PRIVATE_DIR"
"${NEWS_COMPOSE[@]}" config --format json > "$NEWS_PRIVATE_DIR/compose.json"
chmod 600 "$NEWS_PRIVATE_DIR/compose.json"
test "$(jq -er '.name' "$NEWS_PRIVATE_DIR/compose.json")" = "$NEWS_PROJECT"
NEWS_WEB_SERVICE=$(jq -er 'if ((.services.app.environment // {}) | length) > 0 then "app" elif ((.services.web.environment // {}) | length) > 0 then "web" else error("supported News web service not found") end' "$NEWS_PRIVATE_DIR/compose.json")
NEWS_RUNTIME_SERVICES=("$NEWS_WEB_SERVICE" executor background extractor)
for service in executor background extractor; do
  jq -e --arg service "$service" '.services[$service] != null' "$NEWS_PRIVATE_DIR/compose.json" >/dev/null
done
if jq -e '.services.egress != null' "$NEWS_PRIVATE_DIR/compose.json" >/dev/null; then
  NEWS_RUNTIME_SERVICES+=(egress)
fi
# Stop only News runtimes. If a separate scheduler service uses these DBs,
# verify its exact name and stop it explicitly; the managed background embeds it.
"${NEWS_COMPOSE[@]}" stop "${NEWS_RUNTIME_SERVICES[@]}"
"${NEWS_COMPOSE[@]}" run --rm --no-deps \
  --user 0:0 --entrypoint /usr/local/bin/newsctl \
  -v "$NEWS_PRIVATE_DIR/compose.json:/run/news/private/compose.json:ro" \
  -v "$NEWS_PRIVATE_DIR/bootstrap.json:/run/news/private/bootstrap.json:ro" \
  -v "$NEWS_PRIVATE_DIR:/run/news/output" \
  -v "${NEWS_PROJECT}_managed_setup:/state/setup" \
  -v "${NEWS_PROJECT}_managed_web:/state/web" \
  -v "${NEWS_PROJECT}_managed_domain:/state/domain" \
  -v "${NEWS_PROJECT}_managed_background:/state/background" \
  -v "${NEWS_PROJECT}_managed_extractor:/state/extractor" \
  -v "${NEWS_PROJECT}_managed_egress:/state/egress" \
  executor adopt \
  --config /run/news/private/compose.json --apply \
  --bootstrap-input /run/news/private/bootstrap.json \
  --compose-output /run/news/output/adopted.override.yml
```

Apply checks for active sessions from all seven runtime database identities and aborts before migration/import or activation if any remain. The seven roles must be reachable non-superuser roles and must not have `BYPASSRLS`, `CREATEDB`, `CREATEROLE`, or ownership of the target database/schema/objects; the CLI does not grant roles. The bootstrap role needs only the reviewed additive-migration and import permissions. Apply runs migration 016 and supported imports against the existing News database in place, verifies runtime grants before activation, checks parity for existing settings, sources, categories, category mappings and schedules, and emits role files plus a Compose override. It does not copy the database, replace its volume, or grant privileges. The adapter imports the supported administrator hash and provider credentials, and seeds the seven operational settings from legacy `NEWS_LIMIT_*` values only when there is no existing operational revision. Unsupported runtime keys fail closed with scoped operator guidance. The generated override deprecates environment configuration only for the News runtime services it changes. Do not delete a shared env file if another service still uses it. Continue using the same Compose arguments and `-p "$NEWS_PROJECT"` for config-only checks and subsequent News-runtime commands, appending only the generated adoption override. A config-only check verifies that Compose resolves each generated logical role volume to the same `${NEWS_PROJECT}_managed_*` name used for the importer state mounts:

The apply container runs as root and creates the generated override with mode `0600`. For a non-root host user, change the owner of only that file through a one-shot run of the original executor image; this preserves mode `0600` and does not touch the secret inputs or managed state volumes:

```sh
"${NEWS_COMPOSE[@]}" run --rm --no-deps --user 0:0 \
  --entrypoint /bin/chown \
  -v "$NEWS_PRIVATE_DIR:/run/news/output" executor \
  "$(id -u):$(id -g)" /run/news/output/adopted.override.yml
```

A root-owned production runner can omit this ownership handoff.

```sh
for role in web domain background extractor; do
  actual=$("${NEWS_COMPOSE[@]}" -f "$NEWS_PRIVATE_DIR/adopted.override.yml" config --format json \
    | jq -er --arg volume "managed_$role" '.volumes[$volume].name')
  test "$actual" = "${NEWS_PROJECT}_managed_$role"
done
if jq -e '.services.egress' "$NEWS_PRIVATE_DIR/compose.json" >/dev/null; then
  actual=$("${NEWS_COMPOSE[@]}" -f "$NEWS_PRIVATE_DIR/adopted.override.yml" config --format json \
    | jq -er '.volumes.managed_egress.name')
  test "$actual" = "${NEWS_PROJECT}_managed_egress"
fi
```

The volume-name checks read Compose configuration and do not start containers. After verifying them, use the same Compose arguments and project name to start, inspect, and stop only the adopted News services:

```sh
"${NEWS_COMPOSE[@]}" -f "$NEWS_PRIVATE_DIR/adopted.override.yml" up --no-deps -d "${NEWS_RUNTIME_SERVICES[@]}"
"${NEWS_COMPOSE[@]}" -f "$NEWS_PRIVATE_DIR/adopted.override.yml" ps "${NEWS_RUNTIME_SERVICES[@]}"
"${NEWS_COMPOSE[@]}" -f "$NEWS_PRIVATE_DIR/adopted.override.yml" stop "${NEWS_RUNTIME_SERVICES[@]}"
```

These runtime commands use the prebuilt News images, target only the derived service array, and do not stop or remove the rest of the project. Do not use project-wide `down` for adoption. Adoption recovery remains unsupported. Focused fixture and Compose checks do not establish production acceptance or current production reachability.

## Managed role files and precedence

The managed profile mounts one private runtime manifest per role: web, domain, background, extractor, or egress. A role reader accepts only its allowlisted keys from the catalog. Runtime services do not share a general `.env` file. The web and background manifests contain service credentials and their own database URLs; provider credentials and the domain encryption key stay in private domain state. The `managed_roles` field is the allowlist for each role reader.

When `NEWS_RUNTIME_FILE` selects a managed manifest, supported services load only that manifest and do not fall back to process environment values for missing fields or load errors. Legacy standalone services read their process environment at startup. Editing a legacy env file does not change a running service; recreate the affected service to apply it. `NEWS_RUNTIME_FILE` is a bootstrap selector, not a shared secret.

Persisted AI provider/model settings override the domain's initial provider/model defaults. In the managed profile, provider credentials are stored encrypted in PostgreSQL; deleting a stored credential does not revive an environment fallback. Telegram destinations can be stored in the settings table, with service-channel fallback to the main destination. Saving AI settings does not call a provider.

Managed operational controls are typed PostgreSQL settings, separate from environment configuration. They include the article limits (`limits_enabled`, `max_articles`, `per_source`, `days`, `newest_date`) and the `processing_enabled` and `delivery_enabled` switches. Defaults are limits off, 50 articles overall, 50 per source, one day, no upper date, and processing and delivery enabled. Each job attempt pins one settings snapshot before ingestion; processing reads that snapshot for candidate selection and delivery. In legacy mode, `NEWS_LIMIT_*` environment values retain their existing behavior without this database snapshot.

## Legacy Compose profile

The legacy [Compose example](../deploy/compose/development.yml) and `deploy/` templates are separate configuration profiles. Their inputs remain documented in [`deploy/config/development.env.example`](../deploy/config/development.env.example) and the [legacy deployment guide](../deploy/news/README.md). A Compose input may map to a differently named environment variable inside a container. In particular, each database URL and service token belongs to a specific role or boundary; preserve those distinctions and grants when migrating.

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
