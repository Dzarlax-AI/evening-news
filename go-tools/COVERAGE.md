# Go tooling migration coverage — work in progress

This is an inventory of ported behavior, not an acceptance certificate. The
original caller and check inventory is retained in the task evidence as
`original-contract-inventory.json` and `original-image-gate-checks.json`.
No Python gate, fixture or workflow is retired by this table alone.

| Existing caller or behavior | Go candidate | Evidence and remaining gap |
| --- | --- | --- |
| `scripts/prepare_runtime_env.py` | `cmd/prepare-runtime-env`, `runtimeenv` | Independent original-Python three-case and seven-quote golden; CLI file mode, non-overwrite and audit-secret checks. Compose round-trip with actual Docker remains. |
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
