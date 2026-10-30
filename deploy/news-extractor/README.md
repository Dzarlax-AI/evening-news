# Go extraction service

The Go extraction service is the only article extraction runtime. Python callers use its authenticated HTTP API; there is no Python implementation, shadow path, or fallback. The News app requires valid service URLs and tokens at application startup, and service failures remain visible to the caller.

## Profiles

`compose.canary.yml` is the HTTP-only profile. It provides guarded HTTP extraction without JavaScript rendering. `compose.native-canary.yml` and `compose.native.yml` select the native Go browser profile for deployments that have the approved browser and egress infrastructure. The local `deploy/compose/development.yml` also uses HTTP-only mode and supports ordinary HTTP sources; it does not render JavaScript pages or provision Cloak/browser infrastructure.

Use the [native worker guide](../../docs/go-cloak-native-worker.md) for browser-enabled deployment details. Keep the extractor private: it has no public port or public route and requires its bearer token. Configure its endpoint and token in the News app and its service token in the extractor. Never place credentials in rendered Compose output or reports.

## Local checks

Build the image from the repository root using `docker/Dockerfile.extractor`, with `go/` as the build context. The Dockerfile performs module verification and builds the Go service. The image health check uses the service's built-in health command. For local Compose, set the required News, scheduler, and extractor credentials in the shell or an ignored env file, then run:

```bash
docker compose --project-directory . -f deploy/compose/development.yml config --quiet
docker compose --project-directory . -f deploy/compose/development.yml up --build
```

The example stack includes separate PostgreSQL instances for News and the scheduler, plus the Go collector, scheduler, web app, and durable worker. Scheduled work requires `BACKGROUND_EXECUTION_MODE=worker`; the worker container is included. The development overlay can be added with `-f deploy/compose/development.override.yml`.
