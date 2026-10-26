# Go scheduler Docker fixture

The fixture gate runs supplied local News app, Go job worker, Go article processor, and standalone Go scheduler images on a newly created internal-only Docker network. A private Python executor runs from the same app image and receives the processor's AI callback. It uses a disposable PostgreSQL 16 database with separate `news` and `scheduler` schema owners, synthetic source/AI/Telegram receivers, and a second generic consumer. It never publishes a port or mounts the repository, `.env`, credentials, or application data. The app and executor containers mount only this fixture directory read-only; Go services have no host filesystem mount.

Build the app, Go worker, Go processor, and scheduler images from the current checkout first. Build Go images with the repository root as context (`docker build -f go-worker/Dockerfile ... .` and `docker build -f go-processing/Dockerfile ... .`). For the scheduler image, set `SCHEDULER_BUILD_ID` to the gate's deterministic SHA-256 over sorted relative file paths and contents from `go.mod`, optional `go.sum`, recursive `*.go`, and `migrations/**`. File framing is `relative/path`, NUL, bytes, NUL. The gate compares that value to the running scheduler's `/version` response and compares the running app's Python/web source files against the checkout.

Run the gate with explicit local image references and a report directory:

```sh
python3 scripts/run_go_scheduler_fixture_gate.py \
  --app-image evening-news:go-scheduler-fixture \
  --scheduler-image rss-go-scheduler:fixture \
  --worker-image evening-news-job-worker:fixture \
  --processing-image evening-news-article-processing:fixture \
  --output-dir /tmp/go-scheduler-fixture-gate
```

The script resolves each image reference to an immutable local Docker image ID, uses `--pull=never`, and records the IDs in its JSON report. It also requires a locally available `postgres:16-alpine` image and pins its resolved ID for this run. Container resources have explicit memory, CPU, and process caps. Cleanup discovers only containers and the network carrying this invocation's unique ownership label; it never prunes Docker resources.

The gate covers scheduler API auth/revision/CRUD, News facade access, paused import with microsecond preservation, isolated database roles, worker-down and full-queue 503 retry, malformed receipts, invalid target credentials, a generic opaque operation, a deliberately delayed generic target while a second News alias still dispatches, real Go article processing through the Python executor AI callback, provider routing to fixture Go/Gemini/Telegram endpoints, restart persistence, and occurrence inventory for rollback. It does not test production routing, real providers, public Cloudflare behavior, or cross-process database races covered by unit/integration suites.

For full-queue coverage, the fixture holds one real synthetic article-reprocess job in the fake Gemini endpoint, then submits 32 different synthetic article-reprocess requests through the authenticated News API. It records the exact accepted job IDs, kinds, and queued states before checking that the scheduler receives a retryable 503 while the worker heartbeat remains fresh. Cleanup removes only those job aliases and the 32 uniquely prefixed synthetic articles. The JSON report includes idle, slow-target-active, full-queue-active, and final per-container resource snapshots with restart/OOM state; these are fixture observations, not performance claims.
