"""Catch bootstrap cycles and unusable root-context processor image templates."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def _service(name):
    source = (ROOT / "docker-compose.example.yml").read_text()
    match = re.search(r"^  " + re.escape(name) + r":\n(.*?)(?=^  [a-zA-Z][\w-]*:|\Z)", source, re.M | re.S)
    assert match is not None
    return match.group(1)


def _dependencies(name):
    body = _service(name)
    match = re.search(r"^    depends_on:\n(.*?)(?=^    \S|\Z)", body, re.M | re.S)
    return set(re.findall(r"^      ([\w-]+):", match.group(1), re.M)) if match else set()


def test_migration_precedes_background_and_compose_dependencies_are_acyclic():
    # The one-shot migrate command must finish before the background listeners
    # become healthy, while executor startup must not wait on background.
    assert {"postgres"} <= _dependencies("migrate")
    assert {"migrate", "extractor"} <= _dependencies("executor")
    assert not ({"background", "web"} & _dependencies("executor"))
    assert {"migrate", "executor", "extractor", "postgres", "scheduler-postgres"} <= _dependencies("background")
    assert {"migrate", "executor", "background"} <= _dependencies("web")

    services = ("web", "migrate", "executor", "background", "extractor", "postgres", "scheduler-postgres")
    graph = {service: _dependencies(service) for service in services}
    visiting = set()
    visited = set()

    def visit(service):
        assert service not in visiting, f"Compose dependency cycle through {service}"
        if service in visited:
            return
        visiting.add(service)
        for dependency in graph[service]:
            if dependency in graph:
                visit(dependency)
        visiting.remove(service)
        visited.add(service)

    for service in services:
        visit(service)


def test_processor_image_uses_real_root_context_inputs_and_available_health_tool():
    dockerfile = (ROOT / "go-processing/Dockerfile").read_text()
    for line in dockerfile.splitlines():
        if line.startswith("COPY ") and "--from=" not in line:
            for source in line.split()[1:-1]:
                assert (ROOT / source).exists(), source
    assert "apk add --no-cache ca-certificates" in dockerfile
    assert "wget" in dockerfile and "http://127.0.0.1:8094/healthz" in dockerfile
    assert "USER 65532:65532" in dockerfile


def test_private_processor_template_does_not_publish_host_ports_or_mounts():
    body = (ROOT / "deploy/news-processing/compose.processing.yml").read_text()
    assert "ports:" not in body and "volumes:" not in body
    assert "read_only: true" in body and "no-new-privileges:true" in body
    assert "JOB_EXECUTOR_TOKEN" not in body and "GEMINI_API_KEY" not in body
    assert "ARTICLE_AI_TOKEN" in body
