# Core images and release

The three core targets build from `docker/Dockerfile`:

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

## Historical migration acceptance

A previous Go-only migration candidate passed its PostgreSQL 16 and 17 image
gates, including pipeline, delivery faults, shutdown, strict backup/restore and
final web checks. That historical gate wrote a receipt only after every phase,
image identity check and cleanup succeeded. Subsequent changes require fresh
CI evidence for the exact source revision. This record describes the earlier
pipeline; the current selective catalog and release process is documented
below. A passing synthetic fixture does not establish production acceptance.

## Selective CI workflow

`.github/workflows/docker-build-push.yml` runs on pull requests and pushes to
`main`, including documentation-only changes. The `impact` job checks out the
exact head SHA with complete Git history, resolves the baseline, and invokes
`scripts/ci-core.py plan`. The planner writes a summary and outputs the selected
modules, core targets, extractor fixture target, integration requirement and
whether an exact candidate can be reused. Unknown or incomplete inputs fail
closed. With no explicit full-integration request, an empty selection means no
module tests or image build run; the stable `Core checks` job confirms that
empty result.

Manual dispatch accepts `publish`, `full_integration` and
`bootstrap_fixture`. `publish` is effective only for exact `main`;
`full_integration` explicitly requests the reusable gate, and
`bootstrap_fixture` explicitly allows one build when the fingerprinted
extractor fixture is missing.

Selected Go modules run race tests, vet and dependency verification against
PostgreSQL 16 and 17. The web module also builds TypeScript assets required by
its tests. Selected core image targets receive ARM64 cross-build checks.
Secondary workflows retain specialized replay, browser, API, callback, native
browser, and standalone architecture checks; they skip package-level race/vet
work when the root matrix already covers that module.

For a pull request that requires full integration, the candidate job builds
all three core images at the pull request head and the extractor fixture. The
full mixed-version gate needs a concrete complete core image set and does not
use a publication baseline for an untrusted PR. PR candidate artifacts can be
read only by that isolated gate; they are never eligible for publication.

On `main`, the plan compares the head with the latest trusted published
catalog. It builds only core targets whose source fingerprints changed and
reuses unchanged targets by their existing immutable digest. Each target keeps
its own source revision, image ID, digest and fingerprint. A catalog revision
identifies the reviewed component set; it does not claim that reused images
were rebuilt at that revision. When the extractor fingerprint changed, its
fixture is built with the candidate. Otherwise the full gate reuses a fixture
artifact keyed by the exact source fingerprint. Missing fixture state blocks
the gate unless `bootstrap_fixture` was explicitly requested.

The reviewed-main exact-SHA candidate contains the impact plan, raw and
normalized baseline, catalog, metadata and saved image archive. A PR candidate
uses the three locally built core images and fixture in a legacy gate bundle;
it is not a publication catalog and has no trusted baseline. Candidate lookup
verifies artifact provenance and reuses a main candidate only when the plan
and baseline still match. The root workflow uses an exact-SHA concurrency
group to prevent duplicate candidate lifecycles for the same head.

## Catalog and acceptance receipt

The release catalog is schema version 2. It contains all three targets on
`linux/amd64`, with per-image repository, revision, image ID, registry digest,
source fingerprint and origin (`built` or `reused`). Built targets identify
the candidate SHA and have no registry digest before publication. Reused
targets retain their prior revision, digest and image identity. The catalog
also records its baseline revision.

The main full acceptance gate consumes that exact candidate catalog and key.
For a pull request, it consumes the isolated legacy candidate bundle bound to
the head SHA and all three built image identities. The gate runs the vertical
image fixture on PostgreSQL 16 and 17, managed-adoption acceptance on
PostgreSQL 16 and 17, and managed settings-recovery fixtures on
PostgreSQL 16 and 17. The vertical fixture captures every component identity
before and after the test. The final receipt is accepted only after all gate
and recovery jobs pass and both vertical receipts agree. For main, the receipt
key binds the catalog, which binds the complete tested image combination,
including reused images. The isolated PR receipt binds its head SHA and the
three locally built image identities without creating a publication catalog.

`.github/workflows/core-integration.yml` is reusable and manually callable.
Workflow calls provide `head_sha`, `candidate_run_id`, `candidate_artifact`,
`catalog_key`, `is_pr` and optional `bootstrap_fixture`. Manual dispatch on
`main` requires the SHA, candidate run and artifact, and catalog key. It
publishes an accepted gate artifact only after the full acceptance and
recovery jobs pass. Successful gates are reusable for the same catalog key.

The extractor fixture is reused by exact fingerprint. Its archive records the
fingerprint and image ID and is checked before use. The first fixture for a
fingerprint requires a candidate that already contains it or an explicit
bootstrap request; the gate does not silently rebuild a missing unchanged
fixture.

## Artifact trust and baseline resolution

`go-tools/cmd/ci-artifacts` provides `baseline`, `find` and `download`. It
verifies repository identity, workflow path, event, run status, head SHA,
artifact metadata digest and commit ancestry. It rejects expired, incomplete,
truncated or ambiguous artifact history and safely extracts downloaded ZIPs.
Only a successful reviewed-main publication workflow can provide the trusted
baseline. The narrow in-progress PR candidate exception is limited to reading
that candidate for its own isolated gate.

The CLI takes explicit repository and checkout identities:

```sh
ci-artifacts baseline --repository OWNER/REPO --root PATH --head SHA [--output FILE]
ci-artifacts find --repository OWNER/REPO --root PATH --head SHA --kind KIND [--key SHA256] [--current-run-id ID] [--output FILE]
ci-artifacts download --repository OWNER/REPO --root PATH --head SHA --kind KIND --run-id ID --name NAME --output PATH [--current-run-id ID] [--allow-current-pr-only-for-gate]
```

`KIND` is `candidate`, `gate` or `fixture`.

`scripts/ci-core.py` orchestrates the workflow operations:

| Subcommand | Operation |
| --- | --- |
| `plan` | Resolve the PR merge-base or trusted main baseline, run the impact planner and select reuse/build work |
| `candidate` | Build selected images or restore a matching exact-SHA candidate, then save its catalog and metadata |
| `lookup-gate` | Find a successful gate for the exact catalog key |
| `gate` | Restore the candidate, verify identities, obtain the fingerprinted extractor fixture, and run full acceptance |
| `publish` | Restore the accepted candidate and gate, then invoke the selective publisher without rebuilding |
| `baseline` | Resolve the current trusted publication catalog; called again while publication holds the global lock |

The Go planner is available as `go-tools/cmd/ci-impact`; it requires full
`base` and `head` commit IDs and emits modules, targets, fixture targets,
checks, fingerprints, reasons and fallback status. The catalog tool
`go-tools/cmd/core-image-catalog` has `prepare` and `normalize-v1` commands.
The latter accepts a legacy v1 manifest only after validating its three
registry objects and source fingerprints. Unknown catalog versions fail
closed.

## Publication

Core publication is available only through `workflow_dispatch` on exact
`refs/heads/main` with `publish=true`. It reuses a trusted exact-SHA candidate
when its impact plan and baseline match and requires a successful gate for its
catalog key. Publication does not repeat the module tests, full gate or image
build when those accepted artifacts are available. Only targets marked
`built` are published; reused digests and revisions remain unchanged.

The publisher has `packages: write` permission only in its own job. A global
`core-image-publication` concurrency group serializes publication across
different SHAs. While holding that lock, the publisher resolves the latest
baseline again before the first push and before writing the final catalog. If
the baseline moved, publication fails and needs a fresh candidate and gate;
stale or ambiguous state blocks publication. Selected immutable SHA
tags are checked before mutation. Existing tags are accepted only when their
registry content can be reconciled to the exact accepted image identity; they
are never overwritten. The full catalog manifest is emitted only after every
selected push succeeds.

The extractor and shared-browser publishers remain separate workflows. On
ordinary PR/push runs, each builds its image only when its own image inputs
change. An explicit standalone manual publish on `main` is a deliberate
rebuild exception. The core publisher does not publish either standalone
image.

## Local build

When resources permit, build the complete local core graph with:

```sh
REVISION="$(git rev-parse HEAD)" PLATFORM=linux/amd64 \
  docker buildx bake -f docker-bake.hcl --set '*.platform=linux/amd64' --load
```

Building does not run acceptance or publish an image. The Compose example uses
an explicit `news-migrate` one-shot service before the runtime starts; it does
not perform automatic schema migration from web or domain startup. Database
roles and their secrets stay separate. See the [News deployment guide](../deploy/news/README.md)
for local database preparation and runtime environment mapping.

## Verification status

The workflows, selector and artifact checks have local implementation checks,
including workflow parsing/linting and focused unit tests. No GitHub Actions
run, core image publication or production rollout is claimed by this document.
Local CI validation does not replace the separate GitHub run and user-path
acceptance required for release.
