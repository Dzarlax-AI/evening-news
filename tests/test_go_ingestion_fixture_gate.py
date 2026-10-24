"""Fast guards for the private deployment template and real fixture gate."""

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from run_go_ingestion_fixture_gate import has_host_port_binding, sanitize_fixture_log


def test_runtime_role_sql_is_manual_and_separates_function_owner():
    sql = (ROOT / "deploy/news-ingestion/provision-runtime-role.sql").read_text()
    assert "Operator-run once after migration" in sql
    assert "\\set news_schema news" in sql
    assert 'ON :"news_schema".sources' in sql
    assert "CREATE ROLE news_ingestion_fence_owner NOLOGIN NOINHERIT NOBYPASSRLS" in sql
    assert "CREATE ROLE news_ingestion_runtime LOGIN NOINHERIT NOBYPASSRLS" in sql
    assert "CONNECTION LIMIT 6" in sql
    assert "NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION" in sql
    assert "\\password news_ingestion_runtime" in sql
    assert re.search(r"(?<!NO)SUPERUSER", sql, re.IGNORECASE) is None
    assert "GRANT ALL" not in sql.upper()
    assert re.search(r"GRANT SELECT\s*\([^;]*hash_content\)[^;]*ON :\"news_schema\"\.articles TO news_ingestion_runtime", sql, re.IGNORECASE)
    assert "FROM PUBLIC" in sql
    migration = (ROOT / "news_aggregator/migrations/article_ingestion.py").read_text()
    assert "CREATE ROLE" not in migration.upper()


def test_ingestion_pool_bound_fits_one_dedicated_run_lock_connection():
    main = (ROOT / "go-ingestion/cmd/news-ingestion/main.go").read_text()
    readme = (ROOT / "go-ingestion/README.md").read_text()
    guide = (ROOT / "docs/go-article-ingestion.md").read_text()
    assert 'envInt("ARTICLE_INGESTION_DB_MAX_CONNS", 5, 1, 5)' in main
    assert "bounded to 1–5" in readme
    assert "five pooled PostgreSQL connections plus one dedicated connection" in readme
    assert "Defaults to 5; accepted range 1–5" in guide
    assert "one dedicated connection for the fenced run lock" in guide


def test_private_compose_has_no_host_publication_or_host_mounts():
    compose = (ROOT / "deploy/news-ingestion/compose.ingestion.yml").read_text()
    assert "ports:" not in compose
    assert "volumes:" not in compose
    assert "read_only: true" in compose
    assert "cap_drop:" in compose and "- ALL" in compose
    assert "no-new-privileges:true" in compose
    assert "external: true" in compose
    assert "EXTRACTION_SERVICE_NATIVE_BROWSER: ${EXTRACTION_SERVICE_NATIVE_BROWSER:-false}" in compose
    example = (ROOT / "deploy/news-ingestion/ingestion.env.example").read_text()
    assert "EXTRACTION_SERVICE_NATIVE_BROWSER=false" in example


def test_gate_uses_real_collector_and_trusted_synthetic_origins():
    gate = (ROOT / "scripts/run_go_ingestion_fixture_gate.py").read_text()
    assert "actual_go_collector_tested=True" in gate
    assert '"http://collector:8080"' in gate
    assert "SSL_CERT_FILE" in gate
    assert '"--internal"' in gate
    assert "gate_origin.py" in gate
    assert "source_manager_service_client" not in gate


def test_unpublished_exposed_ports_are_not_host_bindings():
    assert has_host_port_binding({"8000/tcp": None}) is False
    assert has_host_port_binding({"8000/tcp": []}) is False
    assert has_host_port_binding({"8000/tcp": [{"HostIp": "0.0.0.0", "HostPort": "8080"}]}) is True
    assert has_host_port_binding({"8000/tcp": [{"HostIp": "127.0.0.1", "HostPort": ""}]}) is False


def test_selector_learning_fixture_requires_learned_selectors_and_same_snapshot_reparse():
    origin = (ROOT / "tests/fixtures/go-ingestion/gate_origin.py").read_text()
    seed = (ROOT / "tests/fixtures/go-ingestion/gate_seed.py").read_text()
    services = (ROOT / "tests/fixtures/go-ingestion/gate_services.py").read_text()
    probe = (ROOT / "tests/fixtures/go-ingestion/gate_probe.py").read_text()
    gate = (ROOT / "scripts/run_go_ingestion_fixture_gate.py").read_text()
    assert 'class="zx-entry"' in origin and 'class="zx-title"' in origin
    assert '"use_browser": False' in seed and '"wait_for_js": False' in seed
    assert '"article_selectors": ["main"]' in seed
    assert "update(Source).values(enabled=False)" in seed
    assert "enabled_source_count" in probe and 'before.get("enabled_source_count") == 3' in gate
    assert "<article" not in origin and '"article"' not in services
    assert '".zx-entry"' in services and '".zx-title"' in services
    assert "page_fetches" in origin and 'path == "/news"' in origin
    assert "Article.url" in probe and "selector_study_sha256" in probe
    assert 'action == "origin-counters"' in probe
    assert '"learned_selector_reparsed_captured_page"' in gate
    assert 're.fullmatch(r"[0-9a-f]{64}"' in gate
    assert 'origin_counters.get("page_fetches") == 1' in gate
    assert 'counters.get("selector_study", 0) == 1' in gate


def test_gate_exercises_unprocessed_duplicate_metadata_update_as_runtime_role():
    seed = (ROOT / "tests/fixtures/go-ingestion/gate_seed.py").read_text()
    probe = (ROOT / "tests/fixtures/go-ingestion/gate_probe.py").read_text()
    gate = (ROOT / "scripts/run_go_ingestion_fixture_gate.py").read_text()
    assert 'duplicate_url = "http://rss.fixture.test/story/one/1"' in seed
    assert 'content="Pre-existing fixture content"' in seed
    assert "hash_content=None" in seed and "processed=False" in seed
    assert '"hash_content_present"' in probe
    assert 'rss_receipt.get("updated", 0) == 1' in gate
    assert '"runtime_duplicate_article_metadata_update"' in gate


def test_fixture_postgres_runs_unprivileged_and_reports_sanitized_startup_failure():
    gate = (ROOT / "scripts/run_go_ingestion_fixture_gate.py").read_text()
    assert 'args.extend(["--user", "70:70", "--tmpfs"' in gate
    assert "/var/lib/postgresql/data:rw,size=512m,uid=70,gid=70,mode=0700" in gate
    assert '"-v", name, check=False' in gate
    assert '"fixture_postgres_diagnostic"' in gate
    assert '"failure_type"' in gate
    assert sanitize_fixture_log("POSTGRES_PASSWORD=fixture-secret") == ["POSTGRES_PASSWORD=[redacted]"]
    assert sanitize_fixture_log("EXTRACTION_SERVICE_TOKEN: fixture-token") == [
        "EXTRACTION_SERVICE_TOKEN: [redacted]"]


def test_gate_preserves_collector_exec_healthcheck_and_records_one_snapshot():
    gate = (ROOT / "scripts/run_go_ingestion_fixture_gate.py").read_text()
    assert 'memory="192m", health_cmd=' not in gate
    assert 'self.docker("stats", "--no-stream", "--format"' in gate
    assert 'self.docker("inspect", "--format", "{{.RestartCount}}"' in gate
    assert '"one docker stats snapshot immediately before cleanup; not a peak or load test"' in gate
    assert 'actual_go_ingestion_tested=False' in gate
    assert 'actual_go_collector_tested=True' in gate
    assert 'self.capture_timeout_diagnostics(job_id)' in gate
    assert '"fixture_snapshot"' in gate and '"sanitized_logs"' in gate
    assert 'finally:\n            gate.cleanup()' in gate
