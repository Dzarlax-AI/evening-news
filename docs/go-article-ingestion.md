# Go article ingestion

## Ownership and status

The private Go article ingestion service is a new database-aware application layer. It is distinct from the existing Go collector, which remains database-free and owns source collection/parsing and article extraction. The ingestion service reads source configuration and Page Monitor progress from the News database, calls the collector API, then persists articles and collection receipts under a live executor fence.

The Python executor still owns the durable job execution boundary and the rest of the News operation. After ingestion succeeds, Python continues article quality checks, AI summarization and categorization, daily summaries, and digest delivery. Python also retains source CRUD and selector learning. The standalone Go scheduler and Go queue worker retain their current responsibilities.

The repository default is `ARTICLE_INGESTION_MODE=python`. Go mode is available for an isolated deployment when the private service and its separately provisioned database role are ready. A Go-mode error fails the active job; it does not silently switch to Python. This implementation is not deployed by this change, and no production mode switch is implied.

## Private topology

The service listens on `ARTICLE_INGESTION_LISTEN` (default `:8093`). It needs private network reachability to the News PostgreSQL database, existing Go collector, Python executor selector callback, and the Python executor that submits and polls runs. It has no published host port, public route, Traefik labels, browser/CDP connection, or shared-browser mount.

The Python executor submits an immutable attempt envelope with the active job, worker lease, executor receipt, backend PID, and deadline. The Go service validates this fence through `fence_article_ingestion_attempt` before starting or committing work. Run IDs are deterministic per job attempt, so a repeated request resolves the same receipt. Source writes and Page Monitor cursor acknowledgement are committed transactionally. Selector-study callbacks return only bounded learned selectors to the Python executor.

The service uses the existing collector API at `/v1/sources/collect`; it does not connect to source websites itself. It uses separate bearer credentials for the run API, collector API, and selector callback. Use independent values for each boundary. The service does not need Gemini, Telegram, browser, or administrator credentials.

## Configuration

| Variable | Consumer | Notes |
| --- | --- | --- |
| `ARTICLE_INGESTION_MODE` | Python app/executor | `python` by default; `go` is an explicit deployment setting |
| `ARTICLE_INGESTION_URL` | Python app/executor | Private base URL for the service |
| `ARTICLE_INGESTION_TOKEN` | Python app/executor and Go service | Run API bearer token |
| `ARTICLE_INGESTION_LISTEN` | Go service | Defaults to `:8093` |
| `ARTICLE_INGESTION_DB_MAX_CONNS` | Go service | Defaults to 5; accepted range 1–20 |
| `DATABASE_URL` | Go service | Must use the separately provisioned least-privilege News role |
| `EXTRACTION_SERVICE_URL` | Go service | Existing private Go collector base URL |
| `EXTRACTION_SERVICE_TOKEN` | Go service and collector | Collector API bearer token |
| `EXTRACTION_SERVICE_NATIVE_BROWSER` | Go service | Existing collector browser-eligibility setting; defaults to `false` and should match the collector's operator configuration |
| `JOB_EXECUTOR_URL` | Go service | Private Python executor callback base URL |
| `JOB_EXECUTOR_TOKEN` | Go service and Python executor | Selector callback bearer token |

The endpoint must be an internal service address. Do not put secrets in the committed example files, Compose output, CI artifacts, or diagnostic reports. See `deploy/news-ingestion/` for the unconfigured template.

## Database provisioning

First deploy the Python application migration `012_article_ingestion` through the normal application migration process. The migration adds `article_ingestion_runs`, `article_ingestion_source_runs`, `source_collection_state`, and the fence function. It does not create a role or grant application runtime privileges.

After migration success, an authorized database operator can use `deploy/news-ingestion/provision-runtime-role.sql` as a starting point. It creates a non-login function owner and a separate ingestion login for the `news` schema by default; pass the psql variable `news_schema` if the deployment uses another schema. The role password is entered interactively with `\password`; the file contains no password. Review database/schema names and run the script manually as a database administrator. Do not run it automatically from application startup or the migration.

The runtime grants are limited to reading source configuration, updating source fetch time and ingestion-owned progress, and inserting/updating the article fields required by collection. The separate fence function owner can read and lock the active job and worker rows while validating a request. It needs column-level update privileges to perform `SELECT ... FOR UPDATE`, but receives no permission to change worker ownership, job state, job payload, or executor state. The function revokes `PUBLIC` execution in the migration. Verify the negative privilege cases in PostgreSQL tests before using a deployment-specific grant script.

The migration currently owns the fence function as the role that applies migrations. Where database policy requires a dedicated non-login function owner, the operator provisioning script transfers ownership to its isolated non-login role after the migration has created the function. That role must not be granted to the application or ingestion login.

## Compose template

`deploy/news-ingestion/compose.ingestion.yml` has no published ports, public labels, browser mounts, or provider credentials. It attaches the service to an operator-supplied private network shared with the News database, collector, and Python executor. The template applies a read-only root filesystem, drops Linux capabilities, disables privilege escalation, and sets bounded container resources. The template is not a complete production deployment and does not select an image digest or enable Go mode in the application.

Create separate random service tokens on the deployment host, store them in a mode-0600 ignored environment file, and pass the same token only to the two services on each authenticated boundary. Use a database URL for the dedicated runtime role; do not reuse the app administrator or migration credentials.

## Build and checks

From the repository root:

```sh
docker build -f go-ingestion/Dockerfile -t evening-news-ingestion:local .
```

The Go module uses Go 1.25.6:

```sh
cd go-ingestion
go test -race ./...
go vet ./...
go mod verify
```

CI runs these checks with PostgreSQL 16 and 17 migration/store coverage and builds a local Linux/amd64 test image. The PR workflow does not publish a container image.

## Isolated acceptance

Run the real ingestion image, existing Go collector, Python app/executor, Go job worker, and PostgreSQL on a uniquely labeled internal Docker network. Synthetic RSS, Telegram, and Page Monitor origins are the only source endpoints. Python selector and AI calls point to fake fixture services. The gate rejects non-fixture source origins and does not use production network attachments, credentials, external providers, real messaging, or a shared browser.

Acceptance checks should confirm that the actual Python client submits a current executor fence, the Go service invokes the real collector contract, all three fixture source types are persisted, Page Monitor progress is acknowledged with the corresponding article transaction, the run/source receipts reach their expected terminal states, and subsequent Python processing reaches the fake AI service. Save a sanitized report that records fixture resource IDs, checks, and that no external service was used; remove only resources carrying the gate's unique labels.

The fixture gate command is:

```sh
docker pull postgres:17-alpine
docker pull python:3.12-alpine
docker build -f docker/Dockerfile -t evening-news-ingestion-app:fixture .
docker build -f docker/Dockerfile.extractor -t evening-news-collector:fixture go
docker build -f go-worker/Dockerfile -t evening-news-job-worker:fixture .
docker build --platform linux/amd64 -f go-ingestion/Dockerfile -t evening-news-ingestion:fixture .
python scripts/run_go_ingestion_fixture_gate.py \
  --app-image evening-news-ingestion-app:fixture \
  --worker-image evening-news-job-worker:fixture \
  --collector-image evening-news-collector:fixture \
  --ingestion-image evening-news-ingestion:fixture \
  --output-dir artifacts/go-ingestion-fixture
```

Pull the PostgreSQL and Python runtime images before running the gate. The gate creates fixture containers with `--pull=never`; it never accesses an image registry from inside its isolated network. CI performs these pulls in a preflight step before it builds the fixture images and starts the gate. The gate consumes the four local fixture images and starts its own PostgreSQL container on the internal fixture network. It is not a production smoke test. Production schema provisioning, image publication, deployment, and switching `ARTICLE_INGESTION_MODE` remain separate actions.

## Rollback boundary

The initial deployment and mode switch require a separate review. Keep the Python mode as the rollback path until the Go deployment and isolated acceptance have been reviewed. Do not drop ingestion receipts or Page Monitor cursor state as part of rollback; retention and cleanup require a separate decision.
