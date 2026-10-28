# Core image build and release

This guide documents local build commands, release workflow, and the deployed
PR33 production layout. It does not authorize a new image publication.

## Production status

PR33's production consolidation was deployed on 2026-10-26. News owns five
production containers: the Go web app, one Go background runtime, the Python
executor, the extractor, and protective egress. External PostgreSQL and browser
infrastructure are outside that count. The single-env preparation process and
role mapping are documented in [the News deployment guide](../deploy/news/README.md).

## Runtime images

`docker/Dockerfile` defines three independent final targets in one Buildx Bake
graph:

| Bake target | Local fixture tag | Registry repository | Runtime contents |
|---|---|---|---|
| `web` | `evening-news-web:consolidation-fixture` | `ghcr.io/dzarlax-ai/rss-web` | Go web binary, templates, API contracts, web static files, CA certificates |
| `background` | `evening-news-background:consolidation-fixture` | `ghcr.io/dzarlax-ai/rss-background` | One Go background process hosting scheduler, worker, ingestion, article processing, and execution components; CA certificates and timezone data |
| `executor` | `evening-news-executor:consolidation-fixture` | `ghcr.io/dzarlax-ai/rss-summarizer` | Python domain runtime, long-lived native AI runtime and editorial CLI, migrations, runtime dependencies, and PostgreSQL backup client |

Each image has its own runtime boundary and registry digest. They share a build
graph, not a common final image. The background build embeds the same `REVISION`
in its runtime and scheduler version response. The final image OCI revision
label carries that same source revision.

The `web` and `background` images do not install Python, provider libraries,
Go compilers, or PostgreSQL backup tools. The executor target now includes the
bounded `/usr/local/bin/news-editorial` CLI for digest generation and rendering
and `/usr/local/bin/news-ai-runtime` for private article and legacy Gemini
transport;
this candidate change does not by itself confirm a production rollout. It does
not include web assets, development scripts, or Go build tools. `requirements.txt` is for
local tests; runtime builds install only `requirements-runtime.txt`.

## Build and inspect locally

The Bake default group loads all three targets for the local ARM64 platform.
Set a revision when you want to inspect source metadata:

```sh
REVISION="$(git rev-parse HEAD)" PLATFORM=linux/arm64 \
  docker buildx bake -f docker-bake.hcl --set '*.platform=linux/arm64' --load
python3 scripts/check_core_image_inventory.py
```

The inventory check runs without network access and with a read-only root
filesystem. It inspects the locally loaded target contents and required
executor runtime utilities. It does not assert image sizes as disk savings or
make performance claims.

The combined synthetic gate uses the three local targets and a separately
built extractor fixture. Preload its versioned PostgreSQL and Python infrastructure
images first; the gate intentionally uses `--pull=never` so its acceptance
environment cannot silently change during a run:

```sh
docker pull postgres:17-alpine
docker pull python:3.12-alpine
docker build --platform linux/arm64 -f docker/Dockerfile.extractor \
  -t evening-news-collector:consolidation-fixture go
python scripts/run_go_background_fixture_gate.py \
  --web-image evening-news-web:consolidation-fixture \
  --background-image evening-news-background:consolidation-fixture \
  --executor-image evening-news-executor:consolidation-fixture \
  --collector-image evening-news-collector:consolidation-fixture \
  --output-dir artifacts/go-background-fixture
```

The gate uses its isolated PostgreSQL and synthetic services. It must not be
given real provider, messaging, source-site, production network, or shared
browser credentials. The extractor and browser build/release workflows remain
independent from the core image release.

## Local Compose

The example stack uses the `web`, `background`, and `executor` targets. The
one-shot `migrate` command uses the executor target. The background service
waits for migration, PostgreSQL, the executor, the extractor, and the scheduler
database; the executor does not wait on the background service. Web waits for
the background and executor health checks. This avoids an executor/background
startup cycle while allowing the scheduler's persisted outbox to retry dispatch
until web is ready.

The private scheduler, ingestion, processing, health, and execution listeners use ports
`8080`, `8093`, `8094`, `8095`, and `8101` on the background service. Compose does not
publish these ports on the host. The worker drain defaults to 20 seconds, the
background shutdown bound to 60 seconds, and Compose allows 75 seconds before
forcing the background container to stop.

Set `WORKER_DATABASE_URL`, `EXECUTION_DATABASE_URL`, `INGESTION_DATABASE_URL`, and
`PROCESSING_DATABASE_URL` to connection strings for the exact provisioned
least-privilege News roles. The execution pool is separate from the worker and
defaults to two pool connections plus its dedicated advisory-guard session. The
Compose example neither creates these roles nor falls back to one shared
`DATABASE_URL`. `SCHEDULER_DATABASE_URL` must use the
existing scheduler database role. The Python executor retains its existing
News `DATABASE_URL`.

`JOB_EXECUTOR_URL` points the worker to the embedded Go execution API on
`http://background:8101`. `DOMAIN_SERVICE_URL` points Go orchestration to the
authenticated Python domain API on `http://executor:8100`. Ingestion uses the
separate `SELECTOR_SERVICE_URL` for selector-learning callbacks. The compatible
`ARTICLE_AI_URL` callback stays on the executor and invokes its native analysis component; these service URLs do not move provider keys
into the Go background process.

The local example is intentionally HTTP-only and does not include production
egress or a shared browser service. It has the core app roles, extractor, and
two local database containers, plus a temporary migration command. Production
uses external PostgreSQL placement and browser integration together with the
separate egress container; its five News-owned containers include one
background process hosting scheduler, worker, ingestion, article processing,
and execution.

## CI and explicit publication

The canonical `Core images and full-stack acceptance` workflow runs the Python
suite on PostgreSQL 16, all core Go module race tests and `go vet` against
PostgreSQL 16 and 17, then builds the Bake targets and runs image inventory and
consolidated synthetic acceptance on Linux amd64. Pull requests and ordinary
pushes to `main` do not publish core images.

Core module workflows retain their module, migration, and contract checks. They
do not independently assemble or publish core runtime images. Extractor and
shared-browser workflows retain their independent builds and publication
paths.

Publication is available only through `workflow_dispatch` on reviewed `main`
with `publish=true`. That job repeats the Python/Go prerequisite jobs, then
builds and accepts the exact local images it publishes. Before the first push,
the workflow checks that all three immutable `sha-<40-character-commit>` tags
are absent:

- `ghcr.io/dzarlax-ai/rss-web`
- `ghcr.io/dzarlax-ai/rss-background`
- `ghcr.io/dzarlax-ai/rss-summarizer`

The publisher requires one successful gate report and matches each target's
accepted image ID, fixture tag, and revision against local Docker inspection.
It also verifies each image's platform and OCI revision label before any push,
then tags the captured image IDs directly. It does not retag mutable local
names or run another build after the acceptance gate. Only after all three pushes
succeed does it write `release-images.json` with target, repository, revision,
platform, image ID, and registry digest. This manifest is the release's exact
artifact inventory; consumers must not infer that targets share a digest.

Registry writes across three repositories cannot be atomic. If a push succeeds
for one target and a later push fails, the workflow produces no complete
manifest and does not overwrite existing immutable SHA tags on retry. Treat
that run as a partial release and inspect registry state before any further
release action. This workflow has not been run for publication here.
