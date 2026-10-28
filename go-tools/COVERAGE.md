# Go tooling migration coverage — work in progress

This is an inventory of ported behavior, not an acceptance certificate. The
original caller and check inventory is retained in the task evidence as
`original-contract-inventory.json` and `original-image-gate-checks.json`.
No Python gate, fixture or workflow is retired by this table alone.

The four historical `evening_news*.yml` and `summarization.yml` workflows
invoked `src/evening_news.py`, `src/evening_news_Constructor_KM.py` or
`src/summarization.py`, with `src/requirements*.txt`. The `src` tree is absent
from the current repository; only archive documentation mentions those old
entrypoints. These workflows could not run the current application or exercise
its behavior and have been retired. Current core workflows remain separately
tracked below; this does not replace their coverage.

The unreferenced `check_articles.py`, `check_attempts.py` and
`check_hype_source.py` were one-off source/database inspections; current web
admin and API views provide the supported inspection path. The unreferenced
`inspect_scheduler.py` and `reset_scheduler_tasks.py` operated on the retired
legacy scheduler state; resetting `is_running` cannot recover a durable Go job.
The unreferenced `production_migration.sh` was a one-time PostgreSQL multi-category procedure
for an obsolete application and manual schema. These helpers were retired;
current migrations and recovery still require the tests and gates below.

| Existing caller or behavior | Go candidate | Evidence and remaining gap |
| --- | --- | --- |
| `scripts/prepare_runtime_env.py` | `cmd/prepare-runtime-env`, `runtimeenv` | Independent original-Python three-case and seven-quote golden; CLI file mode, non-overwrite and audit-secret checks. A local synthetic Compose round-trip matched the original render; broader edge-case and Linux CI coverage remains. |
| `scripts/check_ghcr_tag_absent.py` | `cmd/check-ghcr-tag-absent`, `registryguard` | Fake transport covers all allowed repository scopes, authenticated absence, unauthenticated 404 recheck, redirects and fail-closed errors. Workflow not switched. |
| `scripts/core_image_release.py` | `cmd/core-image-release`, `corerelease` | Fake runner proves all three images inspected before the first tag or push and rejects a mismatched third image. Immutable tag guard remains a separate mandatory CI step. Workflow not switched. |
| `scripts/check_core_image_inventory.py` | `cmd/check-core-image-inventory` | Forbids Python, pip and retired helpers; passed on all three candidate images in the PR-only PG16 and PG17 jobs at `cce1736`. The release workflow is not switched. |
| `scripts/compare_extraction_replays.py` | `cmd/compare-extraction-replays`, `replaycheck` | The Go and original Python reports matched on all 72 frozen cases over six cycles; validation tests cover changed fingerprints, missing cycles and content/media mismatches. The converted workflow awaits Linux CI. |
| `scripts/compare_extractor_service.py` | `cmd/compare-extractor-service`, `extractorcompare` | Go race tests cover the original matching goldens, request validation, bounded HTTP, failed outcomes and redacted reports. The converted image workflow awaits Linux CI. |
| `scripts/collect_runtime_incident.py` | `cmd/collect-runtime-incident`, `incidentbundle`, `incidentfilter`, domain subcommand | Offline and fake-runner tests cover redacted bundles, bounded log filtering, cursor failure and live command construction. Live SSH was not invoked; an overlong line now fails the read without advancing its cursor. Image integration awaits CI. |
| `scripts/smoke_extractor_service.py` | `cmd/smoke-extractor-service`, `extractorsmoke` | Fake transport covers readiness, version, authentication, injection rejection, extracted anchors and private URL denial. Real service smoke remains. |
| Consolidated gate image identity checks | `fixturegate` | Snapshots three distinct IDs at one revision and detects tag replacement; no acceptance receipt is issued by this helper. |
| `tests/fixtures/go-scheduler/fake_services.py` | `cmd/fixture-services` | Used by the candidate image gate for synthetic provider, unknown outcome, 429 and partial delivery scenarios. Coverage of all existing scenarios must be audited. |
| `tests/fixtures/go-ingestion/gate_origin.py` and `go-source-manager/fixture_origin.py` | `cmd/fixture-origin` | Synthetic UTF-8 and Windows-1251 RSS, Telegram, selector-study Page Monitor, fresh SIGTERM page and request counter. The eight-source job passed on PG16/17 candidate images at `cce1736`; full acceptance remains open. |
| `tests/fixtures/go-web/seed_sources.py` | `cmd/seed-fixture-sources` | Real fresh PG16 migrations followed by two seed runs returned stable IDs and 4 RSS, 1 Telegram, 3 custom sources. Their ingestion receipts passed on PG16/17 candidate images at `cce1736`. Requires explicit fixture flag and `gate_db`. |

The consolidated image gate and its probe, SQL and orchestration dependencies,
other active workflows, behavior mapping of original Python tests and
cross-platform release acceptance remain outstanding. At `cce1736`, the PG17
candidate image jobs on PG16 and PG17 reached all implemented phases, including strict restore;
it intentionally failed because remaining acceptance phases are unfinished.
No old coverage may be retired until
an equivalent behavior check or evidence-backed obsolete-caller rationale exists.

## Runtime behavior map — incomplete audit

This map groups behavior rather than equating individual test counts. The
original inventory identifies 784 Python test functions across application and
fixture files. The following candidates must be reviewed against that
inventory before any test or caller is retired.

| Behavior | Candidate Go evidence | Remaining review |
| --- | --- | --- |
| Private callback authentication, admission, stale claims and migration 013 fence | `go-domain/callbacks/*test.go`, `go-domain/authority/*test.go`, `go-domain/cmd/news-domain/main_test.go` | Full execution-to-callback image path and role boundaries |
| Selector analysis, source fingerprint changes and cache publication | `go-domain/selectors/*test.go`, `go-domain/learning/*test.go` | Integrated Page Monitor learning passed on PG16/17 candidate images at `cce1736`; full acceptance remains open |
| Telegram media and source parsing | `go-domain/media/*test.go`, `go/extractor/telegram_*test.go`, `go/service/telegram_test.go` | Original test-by-behavior retirement audit |
| AI analysis and prompts | `go-domain/airuntime/*test.go`, `go-domain/callbacks/compute_test.go` | Provider-independent full-pipeline acceptance and all original contract cases |
| Digest generation, persistence and delivery safety | `go-domain/digestworkflow/*test.go`, `go-domain/telegramdelivery/*test.go`, `go-editorial/editorial/*test.go` | Actual image unknown, partial and 429 cases; editorial source review is separate |
| Terminal and source alerts | `go-domain/terminalevents/*test.go` | Full runtime event, interruption and network fault paths |
| Backup, streaming and worker-mode HTTP restrictions | `go-domain/backup/*test.go`, `go-domain/webbackend/backups_test.go`, `go-domain/webbackend/boundary_test.go` | PG16/17 candidate image dump and strict restore passed at `cce1736`; interrupted streams need confirmation |
| One-shot schema bootstrap and migrations | `go-domain/migration/*test.go`, `go-domain/cmd/news-migrate` | Fresh and existing PostgreSQL 16/17 parity and privileges |
| Job execution, worker leasing and background lifecycle | `go-execution/*test.go`, `go-worker/internal/worker/*test.go`, `go-background/internal/runtime/*test.go` | Full image lease loss, shutdown and recovery |
| Ingestion and source receipts | `go-ingestion/internal/ingest/*test.go`, `go-ingestion/internal/store/*test.go`, `go/service/*test.go`, `go/extractor/*test.go` | Eight synthetic sources passed the real image pipeline on PG16/17 at `cce1736`; isolated browser coverage remains separate |
| Schedule dispatch, pause, import and replay | `go-scheduler/internal/*/*test.go`, `go-web/internal/schedules/*test.go` | End-to-end image outbox retention and due dispatch |
| Public and admin routes and authentication | `go-web/internal/httpui/*test.go`, `go-web/internal/auth/*test.go`, `go-web/internal/repositories/*test.go`, `go-domain/webbackend/*test.go` | Payload and role compatibility in the integrated image gate |

The protected shared-browser Python entrypoint, policy, Dockerfile, workflow,
associated tests and `scripts/shadow_cdp_relay.py` are not candidates for
retirement. Archives and historical documents are not rewritten to change
language statistics. Browser-only tools need an explicit caller and coverage
review before any retirement decision.
