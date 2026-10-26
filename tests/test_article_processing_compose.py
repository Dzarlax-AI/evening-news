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


def test_web_can_install_schema_before_processor_health_is_required():
    # Processor readiness requires the migration installed by web startup.
    assert "article-processing" not in _dependencies("web")
    assert {"executor", "article-processing"} <= _dependencies("worker")
    assert "worker" not in _dependencies("article-processing")
    assert "web" not in _dependencies("article-processing")


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
