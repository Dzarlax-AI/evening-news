#!/usr/bin/env python3
"""Prepare a private one-env Compose candidate from rendered Compose config.

This deliberately uses the Docker Compose renderer as the YAML/interpolation
implementation. Secret values are read from its JSON output in memory, written
only to a mode-0600 dotenv file, and omitted from audit metadata and diagnostics.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Callable


class PrepareError(Exception):
    """A safe, user-displayable failure without configuration values."""


def _dotenv_quote(value: str) -> str:
    """Quote a Compose dotenv value as a single-quoted literal."""
    if re.search(r"(?<!\\)(?:\\\\)*\\'|(?<!\\)(?:\\\\)*\\$", value):
        raise PrepareError("unsupported_environment_value")
    return "'" + value.replace("'", "\\'") + "'"


def _compose_effective_env_value(value: str) -> str:
    """Undo Compose's $$ source escape for a container environment value."""
    return value.replace("$$", "$")


def _database_role(service: str) -> str | None:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", service).strip("_").upper()
    if normalized in {"APP", "WEB"}:
        return "WEB"
    if normalized in {"EXECUTOR", "MIGRATE"}:
        return "EXECUTOR"
    return None


def prepare_config(rendered: dict[str, Any], project: str) -> tuple[dict[str, Any], str, dict[str, Any]]:
    """Build candidate JSON, dotenv text, and non-secret audit metadata."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", project):
        raise PrepareError("invalid_project_name")
    services = rendered.get("services")
    if not isinstance(services, dict) or not services:
        raise PrepareError("invalid_compose_config")

    # Compose's JSON config output is its normalized source representation:
    # literal dollars already appear as $$ and are preserved as-is when this
    # JSON is loaded again. Do not escape rendered non-environment fields.
    candidate = json.loads(json.dumps(rendered))
    env_lines: list[str] = ["# Generated privately by prepare_runtime_env.py"]
    audit_services: dict[str, Any] = {}
    canonical_values: dict[str, str] = {}
    for service_name, original in services.items():
        if not isinstance(original, dict):
            raise PrepareError("invalid_service_config")
        environment = original.get("environment", {})
        if environment is None:
            environment = {}
        if not isinstance(environment, dict):
            raise PrepareError("unsupported_environment_shape")
        mapped: dict[str, str] = {}
        safe_names: dict[str, str] = {}
        if environment:
            if len(env_lines) > 1:
                env_lines.append("")
            env_lines.append(f"# {service_name} responsibility")
        for key, value in environment.items():
            if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                raise PrepareError("invalid_environment_key")
            if not isinstance(value, str):
                raise PrepareError("unresolved_environment_value")
            canonical = key
            if key == "DATABASE_URL":
                role = _database_role(service_name)
                if role:
                    canonical = f"{role}_DATABASE_URL"
            effective_value = _compose_effective_env_value(value)
            if canonical in canonical_values and canonical_values[canonical] != effective_value:
                raise PrepareError("canonical_key_collision")
            if canonical in safe_names:
                raise PrepareError("canonical_key_collision")
            safe_names[canonical] = key
            canonical_values[canonical] = effective_value
            mapped[key] = "${" + canonical + "?Required by generated runtime environment}"
            if not any(canonical in service_audit["key_mapping"].values() for service_audit in audit_services.values()):
                env_lines.append(f"{canonical}={_dotenv_quote(effective_value)}")
        updated = candidate["services"][service_name]
        updated.pop("env_file", None)
        updated["environment"] = mapped
        audit_services[service_name] = {
            "environment_key_count": len(mapped),
            "key_mapping": {key: canonical for canonical, key in safe_names.items()},
        }

    audit = {
        "format": "evening-news-runtime-env-audit-v1",
        "project": project,
        "service_count": len(services),
        "services": audit_services,
        "env_file_references": {
            service: [
                item["path"] if isinstance(item, dict) else item
                for item in config.get("env_file", [])
            ]
            for service, config in services.items()
            if config.get("env_file")
        },
        "secret_values_written_to_audit": False,
        "secret_hashes_written_to_audit": False,
    }
    return candidate, "\n".join(env_lines) + "\n", audit


def resolve_source_env(compose: Path, source_env: Path | None = None) -> Path | None:
    """Choose an explicit interpolation file or the Compose directory's .env."""
    if source_env is not None:
        resolved = source_env.resolve()
        if not resolved.is_file():
            raise PrepareError("source_interpolation_file_not_found")
        return resolved
    default = compose.parent / ".env"
    return default.resolve() if default.is_file() else None


def render_compose(
    compose: Path,
    project: str,
    run: Callable[..., Any] = subprocess.run,
    source_env: Path | None = None,
) -> dict[str, Any]:
    interpolation_file = resolve_source_env(compose, source_env)
    command = [
        "docker", "compose", "--project-name", project,
        "--env-file", str(interpolation_file) if interpolation_file else os.devnull,
        "-f", str(compose), "config", "--format", "json",
    ]
    clean_env = {key: os.environ[key] for key in ("PATH", "HOME", "DOCKER_CONFIG") if key in os.environ}
    try:
        result = run(command, cwd=str(compose.parent), env=clean_env, text=True, capture_output=True, check=False)
    except OSError as exc:
        raise PrepareError("compose_command_unavailable") from exc
    if result.returncode != 0:
        raise PrepareError("compose_render_failed")
    try:
        decoded = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        raise PrepareError("compose_render_invalid_json") from exc
    if not isinstance(decoded, dict):
        raise PrepareError("invalid_compose_config")
    return decoded


def write_outputs(output_dir: Path, candidate: dict[str, Any], dotenv: str, audit: dict[str, Any]) -> None:
    if output_dir.exists():
        raise PrepareError("output_directory_exists")
    output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    outputs = {
        "compose.candidate.json": json.dumps(candidate, indent=2, ensure_ascii=False) + "\n",
        ".env": dotenv,
        "audit.json": json.dumps(audit, indent=2, ensure_ascii=False) + "\n",
    }
    for filename, contents in outputs.items():
        path = output_dir / filename
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
            stream.write(contents)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compose", required=True, type=Path, help="existing production Compose file")
    parser.add_argument("--output-dir", required=True, type=Path, help="new private output directory")
    parser.add_argument("--project", required=True, help="Compose project name")
    parser.add_argument("--source-env", type=Path, help="interpolation file; defaults to .env beside the Compose file")
    args = parser.parse_args(argv)
    try:
        if not args.compose.is_file():
            raise PrepareError("compose_file_not_found")
        compose_path = args.compose.resolve()
        source_env = resolve_source_env(compose_path, args.source_env)
        rendered = render_compose(compose_path, args.project, source_env=source_env)
        candidate, dotenv, audit = prepare_config(rendered, args.project)
        audit["source_compose"] = str(compose_path)
        audit["source_interpolation_file"] = str(source_env) if source_env else None
        write_outputs(args.output_dir, candidate, dotenv, audit)
    except PrepareError as exc:
        print(f"runtime environment preparation failed ({exc})", file=sys.stderr)
        return 1
    except OSError:
        print("runtime environment preparation failed (output_write_failed)", file=sys.stderr)
        return 1
    print("Runtime environment candidate prepared; private outputs are mode 0600")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
