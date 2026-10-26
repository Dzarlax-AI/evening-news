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
