# Standalone Go scheduler

This module is an optional generic scheduler service. It owns only the PostgreSQL schema named by `SCHEDULER_DB_SCHEMA`; it does not read News tables or execute consumer work. The [HTTP contract](../docs/go-scheduler-contract.md) is the behavioral reference, with a machine-readable [OpenAPI 3.1 description](../docs/go-scheduler-openapi.yaml).

Build and test from this directory:

```sh
go test -race ./...
go vet ./...
GOOS=linux GOARCH=amd64 go build ./cmd/scheduler
GOOS=darwin GOARCH=arm64 go build ./cmd/scheduler
docker build -t rss-go-scheduler:local .
```

Runtime requires `SCHEDULER_DATABASE_URL`, `SCHEDULER_DB_SCHEMA`, `SCHEDULER_API_TOKEN`, and `SCHEDULER_TARGET_ALIASES`. Each configured target alias has its own `SCHEDULER_TARGET_<ALIAS>_URL` and `_TOKEN` environment variables. See the contract for bounds, optional settings, and exact request/response formats. Set `SCHEDULER_START_PAUSED=true` for first-time initialization before importing schedules; subsequent restarts use persisted control state.

The container includes the IANA timezone database. Its health check runs `scheduler healthcheck`, which checks readiness through the local HTTP endpoint.
