# Core images and release

The development candidate builds three core targets from `docker/Dockerfile`:

| Target | Local fixture image | Release repository | Runtime responsibility |
| --- | --- | --- | --- |
| `web` | `evening-news-web:consolidation-fixture` | `ghcr.io/dzarlax-ai/rss-web` | Public reader, admin UI and API |
| `background` | `evening-news-background:consolidation-fixture` | `ghcr.io/dzarlax-ai/rss-background` | Scheduler, durable worker, ingestion, processing and execution |
| `executor` | `evening-news-executor:consolidation-fixture` | `ghcr.io/dzarlax-ai/rss-summarizer` | Private Go domain operations, callbacks, analysis and delivery; explicit one-shot migration |

The web and background builds use Go 1.25.6. The private domain build uses Go
1.26.10 and links the editorial and extractor libraries into the domain
binary. The executor runs as UID/GID 10001 and contains PostgreSQL 16 and 17
client tools; it selects the dump client for the database server major. The
other core targets run as UID/GID 65532. The separate extractor and shared
browser builds retain their own responsibilities and release paths.

## Current status

This is an incomplete migration candidate. Linux CI has built the three core
images, but the integrated vertical pipeline has not passed, and the complete
fault, recovery, PostgreSQL 16/17 backup and restore, and image acceptance
gates remain outstanding. The legacy Python fixture gate and associated CI
paths remain until equivalent behavior is proven. The PR-only development
gate does not produce a release acceptance receipt and intentionally fails
until all required phases are implemented and validated. See the
[tooling coverage map](../go-tools/COVERAGE.md) for current evidence and gaps.

No image may be released based on a build alone. A release workflow must verify
all required tests and the full isolated fixture against the exact built image
IDs, then recheck those IDs and their shared revision before publishing the
same image contents without rebuilding. It must confirm all three immutable SHA
tags are absent before the first push, and refuse publication if any check is
uncertain. Publication is limited to the explicitly invoked reviewed-main
release workflow; pull request jobs do not publish. Review and production
rollout require their own authorization.

## Local build

When resources permit, the core images can be built as one graph:

```sh
REVISION="$(git rev-parse HEAD)" PLATFORM=linux/amd64 \
  docker buildx bake -f docker-bake.hcl --set '*.platform=linux/amd64' --load
```

Building does not run acceptance or publish an image. The Compose example uses
an explicit `news-migrate` one-shot service before the runtime starts; it does
not perform automatic schema migration from web or domain startup. Database
roles and their secrets stay separate. See the [News deployment guide](../deploy/news/README.md)
for local database preparation and runtime environment mapping.
