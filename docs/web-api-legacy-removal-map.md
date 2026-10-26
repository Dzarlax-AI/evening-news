# Python web retirement mapping

This patch removes the Python FastAPI presentation layer after Go web acceptance. It keeps frozen historical contracts in `tests/fixtures/web-contract-v1/`; Go tests compare the Go mux and public response shape to those files. The Go image owns `go-web/templates/` and continues to share `web/static/`. The separate `web/templates/` tree is only used by the retiring Python app.

## Removed Python test functions and replacement evidence

| Retired Python test | Go or retained replacement |
|---|---|
| `test_registered_routes_and_schemas_match_frozen_baseline` | `httpui.TestFrozenRouteInventoryMatchesAssembledMux`; `TestCheckedInOpenAPIIsAnUnmodifiedBaselineCopy` |
| `test_live_http_health_auth_admin_cookie_and_redirect_contracts` | `httpui.TestPublicPagesRedirectsAndHealth`, auth package HTTP tests, `TestAdminRoutesRenderExistingPagesAndSetCookie`, `TestAdminRedirectsAndUnknownPaths` |
| `test_public_data_service_success_responses_match_golden` | `httpui.TestPublicResponsesMatchPythonDataServiceGolden`; retain `public-read-golden.json` as offline golden, not a live Python oracle |
| `test_processing_run_endpoint_enqueues_worker_job` | `httpui.TestLegacyWorkerModeCommandsReturnReceiptsAndPersistPythonPayloads` (`POST /api/v1/process/run`) |
| `test_article_reprocess_enqueues_identity_and_force_options` | Same HTTP+Postgres test; asserts defaults, explicit flags and receipt persistence |
| `test_selected_retry_preserves_completed_stages_and_queues_ids` | Same HTTP+Postgres test; asserts selected article IDs and flags |
| Scheduled receipt replay/conflict/retention and worker-down tests | `httpui.TestLegacyScheduledReceiverHTTPEnvelopeAndReceiptContract`; `jobs.TestPostgresJobAcceptanceAndOccurrenceReceipts` |
| Malformed scheduler envelope and scoped bearer tests | `httpui.TestScheduledJobsReceiverRejectsBadEnvelopeBeforeService` plus receiver HTTP auth/replay matrix |
| Scheduler facade settings/CRUD/timestamp/ownership cases | `schedules.TestScheduleRootFiltersOwnershipMergesJobsAndPreservesDSTTimestamp`, revision/ownership/error tests, HTTP facade tests, and passing Go scheduler gate (57 checks) |
| Scheduler service client bounded pagination/occurrence metadata cases | `schedules.TestSchedulerClientPaginationAndResponseValidation`, `TestSchedulerClientTimeoutAndResponseSizeAreBounded` |
| `test_worker_mode_source_creation_skips_network_connection_test` | Go management source-create/Postgres integration in the v8 pipeline gate (the Go path performs the DB write without a network probe) |
| `test_worker_mode_source_test_preserves_not_found_http_status` | Go source-test command HTTP/Postgres integration checks 404/no receipt; queue-full counterpart is `TestLegacySourceTestQueueFullReturns503WithoutCreatingReceipt` |
| `test_worker_mode_source_test_preserves_queue_http_errors` | `httpui.TestLegacySourceTestQueueFullReturns503WithoutCreatingReceipt` |
| `test_worker_source_test_probes_disabled_source_without_mutating_enabled_state` | Retained as `tests/test_background_execution_domain.py`; this is executor behavior, not web behavior |
| `test_worker_mode_ai_category_analysis_is_rejected_before_db_or_ai` | `httpui.TestWorkerModeBackendConflictsPreserveAnalysisAndCleanupContracts` and `backend.TestWorkerModeCategoryAnalysisAndProcessCleanupPreserveConflictOnce` |
| `test_worker_mode_keeps_explicit_telegram_connection_diagnostic` | `httpui.TestTelegramDiagnosticDelegatesOnceAndPreservesBackendStatus` and backend fixed-endpoint/one-call transport tests |
| `test_public_reprocess_wrapper_delegates_types_and_accepted_response` | Go worker-recovery gate public reprocess delivery plus `httpui.TestPublicReprocessRejectsCrossOriginCookieRequestsBeforeCommand` |
| `test_worker_mode_process_cleanup_returns_conflict_before_monitor_creation` | `httpui.TestWorkerModeBackendConflictsPreserveAnalysisAndCleanupContracts` and backend conflict/one-call test |
| `test_generated_idempotency_key_uses_alphanumeric_prefix` | Go public reprocess accepted request in worker-recovery gate; durable receipt and generated-key validation exercised by the Go flow |
| Embedded Python send-digest immediately invoked Telegram delivery tests | Go job-command test/gate verifies `POST /api/v1/telegram/send-digest` creates durable `telegram_digest` work; the private worker owns delivery |

## Tests that remain domain-owned

`tests/test_background_jobs_postgres.py` retains lease claim/recovery, generation fencing, delivery-unknown, schedule transaction and queue validation coverage. Its old Python HTTP `get_job` assertion is replaced by `httpui.TestLegacySelectedArticleOutcomeIsReturnedByPersistedJobStatus`, which queues reprocessing, persists a `retry_wait`/`missing_summary` selected-article outcome, and verifies the actual `GET /api/v1/jobs/{id}` response.

`tests/test_publication_feeds.py` retains DataService feed/count and article visibility domain tests; Python route tests are replaced by `repositories.TestPostgresPublicReaderMultiCategoryAndReadPaths` and `httpui.TestLegacyPublicationVisibilityAcrossPublicHTTPAndFeeds`. `tests/test_publication_digest.py` must retain orchestrator publication grouping while removing old router calls.

The disabled-source worker test in `tests/test_background_execution_api.py` is not a Python web test and must move to a domain-focused test file. Source creation/not-found/queue-full, admin backend conflict, Telegram delegation, public reprocess and idempotency checks are represented in Go HTTP/backend gates. `tests/test_operational_reliability.py` retains its first four domain alert/monitor tests; embedded Python immediate-digest tests are retired because Go durably enqueues digest work.

`tests/test_public_feed_template_security.py` and `tests/test_background_job_ui.py` must read `go-web/templates/public/fragments/feed_core.html` and `article_modal.html` respectively before `web/templates/` is removed. Do not remove `web/static/`.

Keep `tests/fixtures/web-contract-v1/*.json` as historical contract inputs. Private executor tests (`test_web_backend_api.py`), migrations/bootstrap tests, source/page-monitor clients, orchestration, CLI, article-processing, and durable worker lease/claim tests remain. Do not remove FastAPI/Uvicorn/python-multipart: the private executor still uses them. This patch does not replace private backend operations with placeholders.
