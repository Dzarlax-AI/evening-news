"""Fast contract guards for the consolidated background Docker acceptance gate."""

import importlib.util
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/run_go_background_fixture_gate.py"


def load_gate_module():
    scripts_dir = str(ROOT / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location("run_go_background_fixture_gate", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_cli_has_four_explicit_images_and_output_dir():
    module = load_gate_module()
    args = module.build_parser().parse_args([
        "--web-image", "web:test", "--background-image", "background:test",
        "--executor-image", "executor:test", "--collector-image", "collector:test",
        "--output-dir", "/tmp/report",
    ])
    assert args.web_image == "web:test"
    assert args.background_image == "background:test"
    assert args.executor_image == "executor:test"
    assert args.collector_image == "collector:test"


def test_paused_import_timestamp_matches_db_microprecision_without_tolerance():
    module = load_gate_module()
    expected = "2030-01-02T03:04:05.123450Z"
    assert module.same_db_instant(expected, "2030-01-02T03:04:05.12345Z")
    assert module.same_db_instant(expected, "2030-01-02T05:04:05.123450+02:00")
    assert module.safe_utc_timestamp_diagnostic("2030-01-02T03:04:05.12345Z") == expected
    for different in (
        "2030-01-02T03:04:05.123451Z",
        "2030-01-02T03:04:05.1234501Z",
        "2030-01-02T03:04:05.12345",
        "not-a-timestamp",
        "2030-02-30T03:04:05Z",
        None,
    ):
        assert not module.same_db_instant(expected, different)
    for invalid in (
        "2030-01-02T03:04:05.1234501Z",
        "2030-01-02T03:04:05.12345",
        "not-a-timestamp",
        "2030-02-30T03:04:05Z",
        None,
    ):
        assert module.safe_utc_timestamp_diagnostic(invalid) is None


def test_paused_import_timestamp_diagnostics_are_safe_and_exact():
    module = load_gate_module()
    diagnostic = module.paused_import_timestamp_diagnostic(
        "2030-01-02T03:04:05.123450Z", "2030-01-02T03:04:05.123451Z",
    )
    assert diagnostic == {
        "expected_utc": "2030-01-02T03:04:05.123450Z",
        "actual_utc": "2030-01-02T03:04:05.123451Z",
        "expected_valid": True, "actual_valid": True,
        "same_db_microsecond_instant": False,
    }


def test_fixture_environment_uses_only_scoped_database_urls_and_background_endpoints(tmp_path):
    module = load_gate_module()
    images = dict(module.DEFAULT_IMAGES)
    gate = module.ConsolidatedBackgroundGate(images, tmp_path)
    env = gate.background_environment()
    assert set(("WORKER_DATABASE_URL", "EXECUTION_DATABASE_URL", "SCHEDULER_DATABASE_URL", "INGESTION_DATABASE_URL",
                "PROCESSING_DATABASE_URL")).issubset(env)
    assert "DATABASE_URL" not in env
    assert env["JOB_EXECUTOR_URL"] == "http://background:8101"
    assert env["DOMAIN_SERVICE_URL"] == "http://executor:8100"
    assert env["EXECUTION_MAX_CONNECTIONS"] == "2"
    assert env["SELECTOR_SERVICE_URL"] == "http://executor:8100"
    assert env["EXECUTION_DATABASE_URL"].startswith("postgres://fixture_execution_runtime:")
    assert env["SCHEDULER_LISTEN_ADDR"] == ":8080"
    assert env["ARTICLE_INGESTION_LISTEN"] == ":8093"
    assert env["ARTICLE_PROCESSING_LISTEN"] == ":8094"
    assert env["BACKGROUND_HEALTH_LISTEN"] == ":8095"
    assert env["SCHEDULER_TARGET_NEWS_WORKER_URL"].startswith("http://web:")
    assert "fixture-only" in env["JOB_EXECUTOR_TOKEN"]
    executor_env = gate.app_environment("executor")
    assert executor_env["ARTICLE_INGESTION_URL"] == "http://background:8093"
    assert executor_env["ARTICLE_PROCESSING_URL"] == "http://background:8094"
    assert executor_env["ARTICLE_PROCESSING_TOKEN"] == env["ARTICLE_PROCESSING_TOKEN"]
    assert executor_env["ARTICLE_AI_TOKEN"] == env["ARTICLE_AI_TOKEN"]
    assert executor_env["EXECUTION_SERVICE_URL"] == "http://background:8101"


def test_gate_checks_authenticated_go_execution_status():
    source = SCRIPT.read_text()
    assert '"http://background:8101/internal/v1/status"' in source
    assert "missing_token_status == 401" in source
    assert '"executor_instance_id", "active_executions", "ready"' in source
    assert "component_codes == [200, 200, 200, 200, 200]" in source


def test_default_news_digest_gate_uses_python_queue_producer_and_precedes_ambiguous_send():
    source = SCRIPT.read_text()
    fixture_producer = (ROOT / "tests/fixtures/go-background/enqueue_news_digest.py").read_text()
    run_body = source[source.index("    def run(self):"):]
    assert "enqueue_job_in_session(session, \"news_digest\", payload, key" in fixture_producer
    assert '"{}", idempotency_key' in source
    assert '"news_digest_omitted_flags_authority_and_delivery"' in source
    assert run_body.index("self.verify_default_news_digest_authority()") < run_body.index("self.verify_ambiguous_delivery_recovery()")
    assert 'result->>\'parts_sent\'' in source


def test_blocked_ai_timeout_captures_queued_job_and_worker_dispatch_diagnostics(monkeypatch, tmp_path):
    module = load_gate_module()
    gate = module.ConsolidatedBackgroundGate(dict(module.DEFAULT_IMAGES), tmp_path)
    gate.access_token = "fixture-token"
    monkeypatch.setattr(gate, "fake_counters_http", lambda: {"providers": {"gemini": 15}})
    monkeypatch.setattr(gate, "request", lambda *args, **kwargs: (
        200, {"status": "queued", "error_code": None, "error_type": None}
    ))
    article_queries = []

    def pg_value(query):
        article_queries.append(query)
        if "processing_state" in query:
            return "pending|false|false|false|false"
        if "background_worker_status" in query:
            return "8|running|true|0.2"
        if "background_job_executions WHERE state IN" in query:
            return "4|1|0"
        return ""

    monkeypatch.setattr(gate, "pg_value", pg_value)
    monkeypatch.setattr(gate, "capture_execution_failure_diagnostics", lambda job_id: {
        "job": {"state": "queued", "attempt_generation": 0},
        "execution_attempts": [],
        "execution_log_phases": [],
    })
    try:
        gate.wait_gemini_call(15, timeout=0, job_id="11111111-1111-4111-8111-111111111111",
                              phase="worker_reprocess", article_id="5")
    except module.GateFailure as exc:
        assert str(exc) == "fixture_job_did_not_reach_blocked_fake_ai"
    else:
        raise AssertionError("blocked AI wait unexpectedly succeeded")

    diagnostic = gate.report["blocked_ai_wait_diagnostics"]["worker_reprocess"]
    assert diagnostic["article_id"] == 5
    assert diagnostic["article_stage_state"] == "pending|false|false|false|false"
    assert diagnostic["job_execution_diagnostics"]["job"]["state"] == "queued"
    assert diagnostic["job_execution_diagnostics"]["execution_attempts"] == []
    assert diagnostic["worker_lease"] == {
        "generation": 8, "status": "running", "owner_present": True, "heartbeat_age_seconds": 0.2,
    }
    assert diagnostic["worker_dispatch"] == {
        "queued_jobs": 4, "running_jobs": 1, "active_execution_attempts": 0,
        "active_jobs": None,
    }
    assert len(article_queries) == 5


def test_worker_dispatch_snapshot_limits_active_job_fields_to_safe_metadata(monkeypatch, tmp_path):
    module = load_gate_module()
    gate = module.ConsolidatedBackgroundGate(dict(module.DEFAULT_IMAGES), tmp_path)
    monkeypatch.setattr(gate, "pg_value", lambda _query: "2|1|1")
    monkeypatch.setattr(gate, "_json_probe", lambda _query: [
        {"kind": "news_processing", "state": "running", "scheduled": True,
         "payload": {"secret": "must not be copied"}},
        {"kind": "unexpected", "state": "running", "scheduled": False},
    ])

    snapshot = gate.worker_dispatch_snapshot()
    assert snapshot == {
        "queued_jobs": 2, "running_jobs": 1, "active_execution_attempts": 1,
        "active_jobs": [{"kind": "news_processing", "state": "running", "scheduled": True}],
    }
    assert "secret" not in repr(snapshot)


def test_worker_queue_idle_rejects_active_or_unknown_work(monkeypatch, tmp_path):
    module = load_gate_module()
    gate = module.ConsolidatedBackgroundGate(dict(module.DEFAULT_IMAGES), tmp_path)
    monkeypatch.setattr(gate, "worker_lease_snapshot", lambda: {"generation": 2})
    for counts in ((1, 0, 0), (0, 1, 0), (0, 0, 1), (None, None, None)):
        snapshot = dict(zip(("queued_jobs", "running_jobs", "active_execution_attempts"), counts))
        monkeypatch.setattr(gate, "worker_dispatch_snapshot", lambda: snapshot)
        try:
            gate.wait_worker_queue_idle(timeout=0)
        except module.GateFailure as exc:
            assert str(exc) == "fixture_worker_queue_did_not_drain"
        else:
            raise AssertionError("active or unavailable queue was treated as idle")
        assert gate.report["worker_queue_quiescence"]["idle"] is False


def test_worker_queue_idle_accepts_only_drained_work(monkeypatch, tmp_path):
    module = load_gate_module()
    gate = module.ConsolidatedBackgroundGate(dict(module.DEFAULT_IMAGES), tmp_path)
    monkeypatch.setattr(gate, "worker_lease_snapshot", lambda: {"generation": 2})
    monkeypatch.setattr(gate, "worker_dispatch_snapshot", lambda: {
        "queued_jobs": 0, "running_jobs": 0, "active_execution_attempts": 0,
    })
    gate.wait_worker_queue_idle(timeout=0)
    assert gate.report["worker_queue_quiescence"]["idle"] is True


def test_scheduler_probe_command_is_valid_python_and_targets_internal_runtime(monkeypatch, tmp_path):
    module = load_gate_module()
    gate = module.ConsolidatedBackgroundGate(dict(module.DEFAULT_IMAGES), tmp_path)
    captured = []

    class Result:
        returncode = 0
        stdout = '{"status":200,"body":{"ready":true}}\n'

    def fake_docker(*args, **kwargs):
        captured.extend(args)
        return Result()

    monkeypatch.setattr(gate, "docker", fake_docker)
    status, body = gate.scheduler_request("GET", "/v1/status")
    code = captured[captured.index("-c") + 1]
    compile(code, "fixture_scheduler_probe", "exec")
    assert status == 200 and body == {"ready": True}
    assert "http://background:8080/v1/status" in code


def test_gate_is_internal_and_never_builds_or_prunes_images():
    source = SCRIPT.read_text()
    assert '"--internal"' in (ROOT / "scripts/run_go_ingestion_fixture_gate.py").read_text()
    assert '"build"' not in source
    assert '"prune"' not in source
    assert "fixture.invalid" not in source or "https://fixture.invalid" not in source


def test_ambiguous_delivery_uses_private_held_fake_provider_and_worker_lease_loss():
    gate_source = SCRIPT.read_text()
    fake_source = (ROOT / "tests/fixtures/go-scheduler/fake_services.py").read_text()
    assert "verify_ambiguous_delivery_recovery" in gate_source
    assert "telegram_mode=\"accept_then_hold\"" in gate_source
    assert "lock_worker_status.py" in gate_source
    assert "delivery_unknown" in gate_source
    assert "TELEGRAM_RELEASE.wait" in fake_source
    assert "COUNTERS[\"telegram\"] += 1" in fake_source
    assert '"executor"' in (ROOT / "tests/fixtures/background-worker/sitecustomize.py").read_text()
    assert '"telegram_endpoints"' in fake_source
    assert '"ambiguous_delivery_wait_diagnostics"' in gate_source


def test_docker_top_pid_args_parser_handles_headers_and_process_rows():
    module = load_gate_module()
    assert module.parse_docker_top_pid_args(
        "PID ARGS\n123 /news-background\n124 helper --flag\n"
    ) == [(123, "/news-background"), (124, "helper --flag")]


def test_consolidated_web_container_gets_scheduler_target_alias(monkeypatch, tmp_path):
    module = load_gate_module()
    gate = module.ConsolidatedBackgroundGate(dict(module.DEFAULT_IMAGES), tmp_path)
    captured = {}

    def parent_run_container(_self, name, **kwargs):
        captured["name"] = name
        captured.update(kwargs)

    monkeypatch.setattr(module.GoWebGate, "run_container", parent_run_container)
    gate.run_container(gate.prefix + "-web", command=[], image="web:test",
                       env={"TRUSTED_HOSTS": "127.0.0.1,localhost"})
    assert captured["alias"] == "web"
    assert captured["env"]["TRUSTED_HOSTS"] == "127.0.0.1,localhost,web"


def test_worker_fault_diagnostics_only_keep_sanitized_json_fields(monkeypatch, tmp_path):
    module = load_gate_module()
    gate = module.ConsolidatedBackgroundGate(dict(module.DEFAULT_IMAGES), tmp_path)

    class Result:
        returncode = 0
        stdout = (
            '{"msg":"worker heartbeat failed; lease released","error":"worker lease is no longer current '
            '(heartbeat stage=update reason=timeout mutex_wait_ms=0 begin_ms=0 update_ms=1000 commit_ms=0 '
            'sqlstate=57014 secret=must-not-appear)"}\n'
        )

    monkeypatch.setattr(gate, "docker", lambda *args, **kwargs: Result())
    phases = gate.sanitized_worker_json_phases()
    assert phases == [{
        "msg": "worker heartbeat failed; lease released",
        "reason": "timeout",
        "timings_ms": {"mutex_wait": 0, "begin": 0, "update": 1000, "commit": 0},
    }]
    assert "secret" not in repr(phases)
    assert SCRIPT.read_text().count('/consolidated-fixtures/lock_worker_status.py", "22"') == 2


def test_migration_failure_diagnostics_are_precleanup_and_sanitized(monkeypatch, tmp_path):
    module = load_gate_module()
    gate = module.ConsolidatedBackgroundGate(dict(module.DEFAULT_IMAGES), tmp_path)

    class Result:
        def __init__(self, stdout="", returncode=0):
            self.stdout = stdout
            self.returncode = returncode
            self.stderr = ""

    def fake_docker(*args, **kwargs):
        if args[:2] == ("inspect", "--format"):
            return Result('{"Status":"exited","ExitCode":4,"OOMKilled":false,"Error":""}')
        if args[0] == "logs":
            return Result("migration failed for postgresql://gate_user:fixture-only-password@postgres/gate_db\n")
        if args[0] == "exec" and args[2] == "pg_isready":
            return Result(returncode=0)
        raise AssertionError(args)

    monkeypatch.setattr(gate, "docker", fake_docker)
    gate.capture_migration_bootstrap_diagnostics()
    diagnostic = gate.report["migration_bootstrap_diagnostics"]
    assert diagnostic["container_state"]["exit_code"] == 4
    assert diagnostic["database_scope"] == "disposable synthetic gate_db only"
    assert diagnostic["postgres_ready_at_failure_capture"] is True
    assert "fixture-only-password" not in repr(diagnostic)
    assert "postgresql://[redacted]" in diagnostic["log_excerpt"][0]


def test_failed_execution_diagnostics_keep_only_safe_job_and_run_metadata(monkeypatch, tmp_path):
    module = load_gate_module()
    gate = module.ConsolidatedBackgroundGate(dict(module.DEFAULT_IMAGES), tmp_path)
    job_id = "11111111-2222-4333-8444-555555555555"
    queries = []

    def fake_pg_value(sql):
        queries.append(sql)
        if "FROM background_jobs" in sql:
            return '{"kind":"article_reprocess","state":"failed","owner_generation":5,"attempt_generation":2,"error_code":"worker_error"}'
        if "FROM background_job_executions" in sql:
            return '[{"attempt_generation":1,"worker_generation":4,"state":"interrupted","error_code":"worker_interrupted","started":true,"finished":true,"duration_seconds":8.5}]'
        if "FROM article_ingestion_runs" in sql:
            return "[]"
        if "FROM article_processing_runs" in sql:
            return '[{"attempt_generation":2,"worker_generation":5,"state":"succeeded","error_code":null,"started":true,"finished":true,"duration_seconds":0.25}]'
        raise AssertionError(sql)

    class Result:
        returncode = 0
        stdout = (
            '2026-10-27 WARN execution operation failed phase=result_projection '
            'reason_code=result_rejected sqlstate=22023 private=payload-secret SQL=SELECT-secret\n'
            '2026-10-27 WARN worker heartbeat failed error=database-url-secret\n'
        )
        stderr = ""

    monkeypatch.setattr(gate, "pg_value", fake_pg_value)
    monkeypatch.setattr(gate, "docker", lambda *args, **kwargs: Result())
    diagnostic = gate.capture_execution_failure_diagnostics(job_id)
    assert diagnostic["job"] == {
        "kind": "article_reprocess", "state": "failed", "owner_generation": 5,
        "attempt_generation": 2, "error_code": "worker_error",
    }
    assert diagnostic["execution_attempts"] == [{
        "attempt_generation": 1, "worker_generation": 4, "state": "interrupted",
        "error_code": "worker_interrupted", "started": True, "finished": True,
        "duration_seconds": 8.5,
    }]
    assert diagnostic["ingestion_runs"] == []
    assert diagnostic["processing_runs"][0]["state"] == "succeeded"
    assert diagnostic["execution_log_phases"] == [{
        "phase": "result_projection", "reason_code": "result_rejected", "sqlstate": "22023",
    }]
    encoded = repr(diagnostic)
    for secret in ("payload-secret", "SELECT-secret", "database-url-secret", "fixture-only"):
        assert secret not in encoded
    assert all("SELECT" in sql and "FROM " in sql for sql in queries)


def test_execution_failure_diagnostics_reject_invalid_ids_and_untrusted_codes(monkeypatch, tmp_path):
    module = load_gate_module()
    gate = module.ConsolidatedBackgroundGate(dict(module.DEFAULT_IMAGES), tmp_path)
    monkeypatch.setattr(gate, "pg_value", lambda _sql: '{"kind":"article_reprocess","state":"failed","owner_generation":-1,"attempt_generation":2,"error_code":"secret from database"}')

    class Result:
        returncode = 0
        stdout = 'execution operation failed phase=bad phase reason_code=secret value\n'
        stderr = ""

    monkeypatch.setattr(gate, "docker", lambda *args, **kwargs: Result())
    invalid_id = gate.capture_execution_failure_diagnostics("not-a-uuid")
    assert invalid_id == {"available": False}
    safe_id = gate.capture_execution_failure_diagnostics("11111111-2222-4333-8444-555555555555")
    assert safe_id["job"] == {
        "kind": "article_reprocess", "state": "failed", "owner_generation": None,
        "attempt_generation": 2, "error_code": None,
    }
    assert safe_id["execution_log_phases"] == []
    assert "secret from database" not in repr(safe_id)


def test_terminal_failed_job_attaches_safe_failure_metadata(monkeypatch, tmp_path):
    module = load_gate_module()
    gate = module.ConsolidatedBackgroundGate(dict(module.DEFAULT_IMAGES), tmp_path)
    monkeypatch.setattr(module.GoWebGate, "wait_job_http", lambda *_args, **_kwargs: {
        "status": "failed", "error_code": "worker_error", "error_type": "database-url-secret",
    })
    monkeypatch.setattr(gate, "capture_execution_failure_diagnostics", lambda _job_id: {
        "execution_log_phases": [{"phase": "pipeline_execute", "reason_code": "handler_failed"}],
    })
    result = gate.wait_job_http("11111111-2222-4333-8444-555555555555")
    assert result["status"] == "failed"
    job_diagnostic = gate.report["job_diagnostics"]["11111111-2222-4333-8444-555555555555"]
    assert job_diagnostic["error_code"] == "worker_error"
    assert job_diagnostic["error_type"] is None
    assert job_diagnostic["failure_diagnostics"]["execution_log_phases"][0]["reason_code"] == "handler_failed"
    assert "database-url-secret" not in repr(gate.report)


def test_active_sigterm_article_is_fresh_synthetic_and_requires_ai(monkeypatch, tmp_path):
    module = load_gate_module()
    gate = module.ConsolidatedBackgroundGate(dict(module.DEFAULT_IMAGES), tmp_path)
    statements = []

    def fake_pg_value(sql):
        statements.append(sql)
        if sql.startswith("SELECT id FROM sources"):
            return "7"
        if sql.startswith("INSERT INTO articles"):
            return "42"
        return "pending|false|false|false|false"

    monkeypatch.setattr(gate, "pg_value", fake_pg_value)
    article_id = gate.create_fresh_sigterm_article()
    insert = next(sql for sql in statements if sql.startswith("INSERT INTO articles"))
    assert article_id == 42
    assert "Synthetic active SIGTERM fixture" in insert
    assert "rss.fixture.test/active-sigterm/" in insert
    assert "'pending',0," in insert
    assert gate.report["checks"]["active_sigterm_fixture_article_requires_ai"] is True
    assert gate.report["active_sigterm_fixture_article"]["stage_state"] == "pending|false|false|false|false"
    assert "force=False" in SCRIPT.read_text()
    assert '"article_last_processing_error": article_error_code' in SCRIPT.read_text()


def test_lease_recovery_article_is_fresh_synthetic_and_requires_ai(monkeypatch, tmp_path):
    module = load_gate_module()
    gate = module.ConsolidatedBackgroundGate(dict(module.DEFAULT_IMAGES), tmp_path)
    gate.run_id = "abcdef123456"
    statements = []

    def fake_pg_value(sql):
        statements.append(sql)
        if sql.startswith("SELECT id FROM sources"):
            return "7"
        if sql.startswith("INSERT INTO articles"):
            return "43"
        return "pending|false|false|false|false"

    monkeypatch.setattr(gate, "pg_value", fake_pg_value)
    article_id = gate.create_fresh_fixture_article("worker_lease_recovery")
    insert = next(sql for sql in statements if sql.startswith("INSERT INTO articles"))
    assert article_id == 43
    assert "Synthetic active SIGTERM fixture" in insert
    assert "rss.fixture.test/active-sigterm/" in insert
    assert "abcdef123456" not in insert
    assert "'pending',0," in insert
    assert gate.report["checks"]["lease_recovery_fixture_article_requires_ai"] is True
    assert gate.report["lease_recovery_fixture_article"]["stage_state"] == "pending|false|false|false|false"
    assert "force=False" in SCRIPT.read_text()


def test_active_sigterm_origin_serves_only_valid_synthetic_markers():
    origin_path = ROOT / "tests/fixtures/go-source-manager/fixture_origin.py"
    spec = importlib.util.spec_from_file_location("fixture_origin", origin_path)
    origin = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(origin)
    marker = "abcdef123456"
    document = origin.active_sigterm_document(marker)
    assert document is not None
    assert document.count(marker.encode()) >= 2
    assert b"Synthetic active SIGTERM fixture" in document
    assert len(document) > 1000
    assert origin.active_sigterm_document("INVALID") is None
