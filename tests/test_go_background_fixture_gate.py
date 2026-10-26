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


def test_fixture_environment_uses_only_scoped_database_urls_and_background_endpoints(tmp_path):
    module = load_gate_module()
    images = dict(module.DEFAULT_IMAGES)
    gate = module.ConsolidatedBackgroundGate(images, tmp_path)
    env = gate.background_environment()
    assert set(("WORKER_DATABASE_URL", "SCHEDULER_DATABASE_URL", "INGESTION_DATABASE_URL",
                "PROCESSING_DATABASE_URL")).issubset(env)
    assert "DATABASE_URL" not in env
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
