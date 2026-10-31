# Go tooling and runtime coverage

The News application, operator tools and CI fixtures use Go. The separate browser stack retains four Python launcher/policy/relay files. The archived application Python has been removed; language-independent golden fixtures remain regression inputs.

This document maps implemented checks to behavior. It is not a claim that every failure mode is covered or that every listed test has been rerun on the current documentation revision. Run the relevant checks for each change.

## Verified release baseline

The managed-installation release was merged at `7f36cbebc62f78fafb85f84798e144c05744e1bb` in [PR48](https://github.com/Dzarlax-AI/evening-news/pull/48). Its reviewed head passed seven CI workflows, including PostgreSQL 16/17 managed installation/adoption and Linux native-browser acceptance. Published core and extractor images were deployed with the existing native browser topology. The first normal scheduled job succeeded for all eight sources. These are dated release checks, not pending migration gates or a long-term stability guarantee.

A subsequent production proxy-only fix was verified on 2026-10-30 in an existing Cloudflare Access browser session: dashboard and operational settings loaded successfully. It changed no Go authentication policy. An automated end-to-end test through that actual Access middleware remains a separate gap.

## Tooling checks

Paths below are relative to `go-tools/` unless a repository-root path is explicit.

| Behavior | Implementation and checks |
| --- | --- |
| Manual runtime env generation | `cmd/prepare-runtime-env`, `runtimeenv`: original render goldens, quoting, private output, non-overwrite and secret-safe audit |
| Immutable registry tag guard | `cmd/check-ghcr-tag-absent`, `registryguard`: repository scopes, authenticated absence, redirects and fail-closed errors |
| Selective core image publication | `cmd/core-image-release`, `corerelease`: schema-v2 full catalog, per-image revisions and fingerprints, exact mixed-image receipt, selected-target publishing, fresh baseline resolution and registry reconciliation |
| Core runtime inventory | `cmd/check-core-image-inventory`: rejects Python/pip and retired helpers in application images |
| Extraction replay parity | `cmd/compare-extraction-replays`, `replaycheck`: frozen reports, changed fingerprints, missing cycles and content/media mismatch |
| Extractor comparison and smoke | `extractorcompare`, `extractorsmoke`: bounded requests, authentication, private URL denial, anchors and safe reports |
| Incident bundles | `incidentbundle`, `incidentfilter`: bounded/redacted logs, cursor handling and command construction; collecting evidence is not automatic remediation |
| Schedule export/import | `schedulerbridge`: revision drift, active leases, private output and rollback inventory |
| Synthetic HTTP/provider services | `cmd/fixture-services`, `cmd/fixture-origin`: RSS encodings, Telegram, CSS Page Monitor, delivery failures, shutdown and counters |
| Fixture source seeding | `cmd/seed-fixture-sources`: explicit fixture opt-in, isolated DB and repeatable eight-source inventory |
| Image identity and acceptance receipts | `fixturegate`: exact image ID/platform/revision checks against either a complete mixed-revision catalog or the legacy shared-revision receipt |
| Change impact planning | `ciimpact`, `cmd/ci-impact`: tracked base/head snapshots, selected module/image/check outputs, source fingerprints, conservative fallback and failure on incomplete history or unresolved imports |
| CI artifact trust | `ciartifact`, `cmd/ci-artifacts`: trusted run/repository/workflow/event/SHA checks, ancestry, archive digest verification, safe extraction, baseline lookup and narrowly scoped current-PR gate access |
| CI orchestration | repository `scripts/ci-core.py`, `scripts/test_ci_core.py`, `scripts/test_ci_impact.py`: baseline planning, candidate build/reuse, gate lookup, full acceptance, selective publication and event-to-SHA routing |
| Browser contracts and isolation | `browsercontract`, `cloakfixture`: browser image/Compose contracts and isolated native Cloak checks; fixture acceptance does not authorize changing a shared live browser |
| Configuration inventory | `configcatalog`: code-discovered keys, valid source locations, deployment inputs, full example and managed-role boundaries |
| Managed setup/adoption | `managedsetup`, `cmd/managed-adoption-gate`, repository-root `go-domain/cmd/newsctl`: installation, native attachment, role boundaries, preservation and supported recovery profiles |

## Runtime behavior checks

Paths in this table are relative to the repository root.

| Behavior | Test locations and scope |
| --- | --- |
| Callback authentication and ownership fences | `go-domain/callbacks`, `go-domain/authority`, `go-domain/cmd/news-domain` |
| Selector analysis and learning cache | `go-domain/selectors`, `go-domain/learning` |
| Telegram parsing and media | `go-domain/media`, `go/extractor`, `go/service` |
| AI providers, usage and validation | `go-domain/airuntime`, `go-domain/callbacks`; the external go-ai module owns its provider adapter tests |
| Editorial validation and delivery | `go-editorial/editorial`, `go-domain/digestworkflow`, `go-domain/telegramdelivery`: edition validation, persistence, unknown/partial outcomes and retry behavior |
| Alerts and terminal events | `go-domain/terminalevents`: terminal/source event contracts and deduplication |
| Database backups and HTTP restrictions | `go-domain/backup`, `go-domain/webbackend`: streaming, cancellation, worker-mode restrictions and role boundaries |
| Schema bootstrap | `go-domain/migration`, `go-domain/cmd/news-migrate`: one-shot migration and PostgreSQL parity |
| Worker heartbeat and lifecycle | `go-worker/internal/worker`, `go-background/internal/runtime`, `go-execution`: leases, generations, shutdown and durable execution |
| Collection and persistence | `go-ingestion/internal/ingest`, `go-ingestion/internal/store`, `go/service`, `go/extractor`: eight-source fixture pipeline and source receipts |
| Native browser concurrency | `go/transport/native_browser_test.go`, `go-tools/cloakfixture`: independent targets and isolated real-browser checks |
| Schedule persistence and dispatch | `go-scheduler/internal/store`, `go-scheduler/internal/dispatch`, `go-web/internal/schedules`: durable outbox, fencing, retries, pause and fixed target aliases |
| Web/API and authentication | `go-web/internal/httpui`, `go-web/internal/auth`, `go-web/internal/transport`, `go-web/internal/repositories`, `go-domain/webbackend` |

## Running checks

Run `make test` and `make vet` for the News modules, or use focused `go test ./...`, `go test -race ./...`, `go vet ./...` and `go mod verify` inside a module. PostgreSQL integration tests require an explicitly isolated loopback test database. Image and browser fixture workflows have their own isolated infrastructure; unit tests alone do not establish full-stack acceptance.

## Selective CI routing and release artifacts

The root `.github/workflows/docker-build-push.yml` workflow runs on every pull
request and every push to `main`, including docs-only changes. Its `impact`
job checks out the full head SHA with complete history and invokes
`scripts/ci-core.py plan`. Pull requests use their merge base; main runs resolve
the latest trusted published catalog through `ci-artifacts baseline`. The plan
records selected modules, core targets, extractor fixture target, fingerprints
and full-integration requirement. Unknown inputs, missing history or
unavailable/untrusted baselines fail closed. An empty selection is explicit,
and the stable `Core checks` job validates that no selected job was
unexpectedly skipped.

Selected Go modules run race tests, vet and `go mod verify` on PostgreSQL 16
and 17; selected image targets are cross-built for ARM64. The web module also
builds its TypeScript assets. The CI tooling tests run when `go-tools` is
selected. Secondary workflows retain offline extraction replay, browser
contracts, API/frontend behavior, callback and migration checks, scheduler
registry guard and platform compilation, extractor smoke, and native Cloak
concurrency. They skip module-wide or focused Go race/vet work when the root
selected-module matrix already covers that module. See
[`core-image-build-and-release.md`](../docs/core-image-build-and-release.md)
for candidate, gate and publication details.

The root workflow publishes no image on PR or push. Main candidate images and
catalogs are saved as exact-SHA artifacts. `ci-artifacts` verifies the
originating run, repository, workflow path, event, SHA, artifact archive digest
and ancestry before lookup or extraction. The in-progress PR exception is
limited to reading a candidate for its own isolated gate; it cannot be used
for publication. `ci-core.py` coordinates candidate reuse only when both the
impact plan and baseline match.

The separate `core-integration.yml` workflow tests the exact mixed core image
set on PostgreSQL 16 and 17, runs managed adoption on both versions, and runs
managed recovery on both versions. It reuses extractor fixture artifacts by
source fingerprint. A missing fixture requires the explicit bootstrap input.
The final accepted gate artifact is emitted only after the acceptance and
recovery jobs pass; gates are reusable for the same catalog key.

Manual core publication requires `workflow_dispatch` on exact `main` with
`publish=true` and a successful gate for the candidate catalog. It publishes
only newly built targets, preserves reused image revisions and digests, and
does not rebuild accepted images. The publisher runs under a global
cross-SHA lock, re-resolves the baseline before pushing and before writing the
final catalog, and has package-write permission only in its job. Extractor
and browser builds have their own input fingerprints and separate manual
publishers; an explicit manual publish on `main` is a rebuild exception for
that standalone image.

Local parser, lint and unit-test results are not GitHub Actions evidence. No
live CI run or image publication is claimed here.

## Explicit limits

- Native-browser, adopted and external-database installations still do not use the generic `newsctl` local backup/restore path. The separate [operator settings recovery](../docs/managed-settings-recovery.md) recipe restores configuration and matching keys into clean targets; it intentionally excludes article/delivery history. Its PostgreSQL 16/17 fixtures are wired into core CI.
- Real-provider editorial quality requires source review; synthetic provider tests do not establish factual quality or authorize paid calls.
- The production Cloudflare Access admin/API chain needs its own automated regression coverage. A passing native auth endpoint does not exercise that chain.
- Historical PostgreSQL COMMIT delays remain an unresolved diagnostic question; passing heartbeat tests or one scheduled run does not establish their cause.
- Standalone component deployment profiles and manual development configuration remain separate from the managed default; changing their topology requires its own review.

The [retirement ledger](RETIREMENT.md) records the historical porting decisions. It is retained as evidence rather than a list of still-pending implementation tasks.

## Russian and Serbian localization

- Article partial-result validation, atomic Russian projection, malformed shared responses and preservation of completed stages: processing/analysis localization tests.
- Bounded Serbian-only retries, claim/source fencing and unchanged extraction/article attempts: isolated PostgreSQL processing tests; race checks cover processing/store.
- Shared bilingual Gemini/OpenAI editorial transport, immutable composition, locale evidence and Serbian-only retries: editorial localization/provider tests.
- Additive migrations, restricted grants, frozen snapshots, source revision invalidation and paired editions: migration and digestworkflow PostgreSQL tests.
- Pinned Telegram destinations, unknown-delivery receipt protection and disabled Serbian delivery after synthetic settings recovery: appsettings, delivery and recovery tests.
- Full routed UI catalog inventory, placeholders, rendered JS syntax, safe error presentation, query/cookie priorities, dirty controls, unavailable/paired editions and locale-aware feed/search/API/RSS: go-web tests plus the Node locale behavior fixture.
- Local Playwright smoke acceptance exercised public read paths, managed login/setup, seven admin pages, unsaved changes, mobile/dark mode and keyboard focus using synthetic data.

These tests do not establish live Serbian generation quality, real provider token cost, production proxy/Access behavior or actual Telegram delivery. Those operations require separate authorization.
