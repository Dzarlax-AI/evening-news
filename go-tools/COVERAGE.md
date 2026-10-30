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
| Core image publication | `cmd/core-image-release`, `corerelease`: all images inspected before publishing; mismatched revisions rejected |
| Core runtime inventory | `cmd/check-core-image-inventory`: rejects Python/pip and retired helpers in application images |
| Extraction replay parity | `cmd/compare-extraction-replays`, `replaycheck`: frozen reports, changed fingerprints, missing cycles and content/media mismatch |
| Extractor comparison and smoke | `extractorcompare`, `extractorsmoke`: bounded requests, authentication, private URL denial, anchors and safe reports |
| Incident bundles | `incidentbundle`, `incidentfilter`: bounded/redacted logs, cursor handling and command construction; collecting evidence is not automatic remediation |
| Schedule export/import | `schedulerbridge`: revision drift, active leases, private output and rollback inventory |
| Synthetic HTTP/provider services | `cmd/fixture-services`, `cmd/fixture-origin`: RSS encodings, Telegram, CSS Page Monitor, delivery failures, shutdown and counters |
| Fixture source seeding | `cmd/seed-fixture-sources`: explicit fixture opt-in, isolated DB and repeatable eight-source inventory |
| Image revision identity | `fixturegate`: separate image IDs, shared revision and replacement detection |
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

## Explicit limits

- Native-browser, adopted and external-database installations still do not use the generic `newsctl` local backup/restore path. The separate [operator settings recovery](../docs/managed-settings-recovery.md) recipe restores configuration and matching keys into clean targets; it intentionally excludes article/delivery history. Its PostgreSQL 16/17 fixtures are wired into core CI.
- Real-provider editorial quality requires source review; synthetic provider tests do not establish factual quality or authorize paid calls.
- The production Cloudflare Access admin/API chain needs its own automated regression coverage. A passing native auth endpoint does not exercise that chain.
- Historical PostgreSQL COMMIT delays remain an unresolved diagnostic question; passing heartbeat tests or one scheduled run does not establish their cause.
- Standalone component deployment profiles and manual development configuration remain separate from the managed default; changing their topology requires its own review.

The [retirement ledger](RETIREMENT.md) records the historical porting decisions. It is retained as evidence rather than a list of still-pending implementation tasks.
