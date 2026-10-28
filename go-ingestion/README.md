# Go article ingestion service

This module provides the private, database-aware article ingestion component in the Go background process. It reads enabled source configuration from the News PostgreSQL schema, asks the Go collector to collect articles, and persists bounded article and source progress under the active execution fence. The collector remains a separate database-free service. Go web owns source administration, the background process owns orchestration, and the private Go domain owns callbacks, summaries and delivery.

The Go execution pipeline calls this component for configured-source collection. The private service URL and bearer token are required at application startup. A service failure fails the active job; there is no Python fallback.

## Configuration

The service requires:

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | PostgreSQL connection using the separately provisioned ingestion runtime role when run standalone; the background process maps `INGESTION_DATABASE_URL` to this component |
| `ARTICLE_INGESTION_TOKEN` | Bearer token accepted by the private run API |
| `EXTRACTION_SERVICE_URL` | Existing Go collector endpoint (`/v1/sources/collect`) |
| `EXTRACTION_SERVICE_TOKEN` | Bearer token for the collector API |
| `EXTRACTION_SERVICE_NATIVE_BROWSER` | Existing collector browser-eligibility setting; defaults to `false` and must match the operator's collector configuration |
| `SELECTOR_SERVICE_URL` | Private Go domain selector callback endpoint; falls back to `JOB_EXECUTOR_URL` when unset |
| `JOB_EXECUTOR_TOKEN` | Bearer token for selector callbacks |
| `ARTICLE_INGESTION_LISTEN` | Listen address; defaults to `:8093` |
| `ARTICLE_INGESTION_DB_MAX_CONNS` | PostgreSQL pool limit; defaults to 5 and is bounded to 1–5 |

The service uses up to five pooled PostgreSQL connections plus one dedicated connection for the fenced run lock. The runtime role limit is six connections to cover that maximum combined budget.

Run submission, status, cancellation, and selector callback endpoints are private service APIs. `/healthz` is an unauthenticated liveness probe intended for the container health check and private orchestration only. Do not publish the service port or add a public route.

The explicit `news-migrate` command must first apply additive migration `012_article_ingestion`. That migration creates ingestion records and the narrow fence function; it does not create a login role or grant production privileges. An operator must provision the runtime role separately after reviewing [the ingestion guide](../docs/go-article-ingestion.md).

## Build

Build from the repository root so the module files are in the Docker build context:

```sh
docker build -f go-ingestion/Dockerfile -t evening-news-ingestion:local .
```

The Dockerfile cross-compiles for the selected target architecture and runs as UID/GID 65532. On Apple Silicon, a local Docker build uses the local ARM64 target; CI also builds and exercises the Linux/amd64 image. The runtime image has a read-only-compatible filesystem and a `/healthz` health check.

Run module checks from this directory with Go 1.25.6:

```sh
go test -race ./...
go vet ./...
go mod verify
```

Store tests that require PostgreSQL accept `TEST_DATABASE_URL` only when it points to a loopback address. The isolated Docker fixture gate exercises the actual component and collector with synthetic origins; see [the ingestion guide](../docs/go-article-ingestion.md).

## Deployment status

This service has been used by production since PR #22. This repository cleanup removes the dormant Python collection and persistence fallback; it does not deploy a new image or change production configuration. The Compose files under `deploy/news-ingestion/` are private deployment templates, not an instruction to deploy them.
