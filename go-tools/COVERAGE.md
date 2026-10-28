# Go tooling migration coverage — work in progress

This is an inventory of ported behavior, not an acceptance certificate. The
original caller and check inventory is retained in the task evidence as
`original-contract-inventory.json` and `original-image-gate-checks.json`.
No Python gate, fixture or workflow is retired by this table alone.

| Existing caller or behavior | Go candidate | Evidence and remaining gap |
| --- | --- | --- |
| `scripts/prepare_runtime_env.py` | `cmd/prepare-runtime-env`, `runtimeenv` | Independent original-Python three-case and seven-quote golden; CLI file mode, non-overwrite and audit-secret checks. A local synthetic Compose round-trip matched the original render; broader edge-case and Linux CI coverage remains. |
| `scripts/check_ghcr_tag_absent.py` | `cmd/check-ghcr-tag-absent`, `registryguard` | Fake transport covers all allowed repository scopes, authenticated absence, unauthenticated 404 recheck, redirects and fail-closed errors. Workflow not switched. |
| `scripts/core_image_release.py` | `cmd/core-image-release`, `corerelease` | Fake runner proves all three images inspected before the first tag or push and rejects a mismatched third image. Immutable tag guard remains a separate mandatory CI step. Workflow not switched. |
| `scripts/check_core_image_inventory.py` | `cmd/check-core-image-inventory` | Targets the proposed native executor and forbids Python, pip and retired helpers. Not tested on built images; workflow not switched. |
| `scripts/smoke_extractor_service.py` | `cmd/smoke-extractor-service`, `extractorsmoke` | Fake transport covers readiness, version, authentication, injection rejection, extracted anchors and private URL denial. Real service smoke remains. |
| Consolidated gate image identity checks | `fixturegate` | Snapshots three distinct IDs at one revision and detects tag replacement; no acceptance receipt is issued by this helper. |
| `tests/fixtures/go-scheduler/fake_services.py` | `cmd/fixture-services` | Synthetic provider, consumer, idempotency, 429 and hold/release tests; not yet connected to image acceptance. Coverage of all existing scenarios must be audited. |
| `tests/fixtures/go-ingestion/gate_origin.py` and `go-source-manager/fixture_origin.py` | `cmd/fixture-origin` | Synthetic UTF-8 and Windows-1251 RSS, Telegram, selector-study Page Monitor, fresh SIGTERM page and request counter; no image acceptance yet. |
| `tests/fixtures/go-web/seed_sources.py` | `cmd/seed-fixture-sources` | Real fresh PG16 migrations followed by two seed runs returned stable IDs and 4 RSS, 1 Telegram, 3 custom sources. Actual ingestion receipts remain unverified. Requires explicit fixture flag and `gate_db`. |

The consolidated image gate and its probe, SQL and orchestration dependencies,
other active workflows, behavior mapping of original Python tests, real
provider-independent pipeline, image inventory, PG17, backup/restore and
cross-platform builds remain outstanding. No old coverage may be retired until
an equivalent behavior check or evidence-backed obsolete-caller rationale exists.

## Runtime behavior map — incomplete audit

This map groups behavior rather than equating individual test counts. The
original inventory identifies 784 Python test functions across application and
fixture files. The following candidates must be reviewed against that
inventory before any test or caller is retired.

| Behavior | Candidate Go evidence | Remaining review |
| --- | --- | --- |
| Private callback authentication, admission, stale claims and migration 013 fence | `go-domain/callbacks/*test.go`, `go-domain/authority/*test.go`, `go-domain/cmd/news-domain/main_test.go` | Full execution-to-callback image path and role boundaries |
| Selector analysis, source fingerprint changes and cache publication | `go-domain/selectors/*test.go`, `go-domain/learning/*test.go` | Integrated Page Monitor learning in the image gate |
| Telegram media and source parsing | `go-domain/media/*test.go`, `go/extractor/telegram_*test.go`, `go/service/telegram_test.go` | Original test-by-behavior retirement audit |
| AI analysis and prompts | `go-domain/airuntime/*test.go`, `go-domain/callbacks/compute_test.go` | Provider-independent full-pipeline acceptance and all original contract cases |
| Digest generation, persistence and delivery safety | `go-domain/digestworkflow/*test.go`, `go-domain/telegramdelivery/*test.go`, `go-editorial/editorial/*test.go` | Actual image unknown, partial and 429 cases; editorial source review is separate |
| Terminal and source alerts | `go-domain/terminalevents/*test.go` | Full runtime event, interruption and network fault paths |
| Backup, streaming and worker-mode HTTP restrictions | `go-domain/backup/*test.go`, `go-domain/webbackend/backups_test.go`, `go-domain/webbackend/boundary_test.go` | Actual PG16 and PG17 dump and strict restore; interrupted streams |
| One-shot schema bootstrap and migrations | `go-domain/migration/*test.go`, `go-domain/cmd/news-migrate` | Fresh and existing PostgreSQL 16/17 parity and privileges |
| Job execution, worker leasing and background lifecycle | `go-execution/*test.go`, `go-worker/internal/worker/*test.go`, `go-background/internal/runtime/*test.go` | Full image lease loss, shutdown and recovery |
| Ingestion and source receipts | `go-ingestion/internal/ingest/*test.go`, `go-ingestion/internal/store/*test.go`, `go/service/*test.go`, `go/extractor/*test.go` | Real eight-source image pipeline and isolated browser coverage |
| Schedule dispatch, pause, import and replay | `go-scheduler/internal/*/*test.go`, `go-web/internal/schedules/*test.go` | End-to-end image outbox retention and due dispatch |
| Public and admin routes and authentication | `go-web/internal/httpui/*test.go`, `go-web/internal/auth/*test.go`, `go-web/internal/repositories/*test.go`, `go-domain/webbackend/*test.go` | Payload and role compatibility in the integrated image gate |

The protected shared-browser Python entrypoint, policy, Dockerfile, workflow,
associated tests and `scripts/shadow_cdp_relay.py` are not candidates for
retirement. Archives and historical documents are not rewritten to change
language statistics. Browser-only tools need an explicit caller and coverage
review before any retirement decision.
