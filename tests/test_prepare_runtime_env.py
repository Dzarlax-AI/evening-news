from __future__ import annotations

import importlib.util
import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).parents[1] / "scripts" / "prepare_runtime_env.py"
spec = importlib.util.spec_from_file_location("prepare_runtime_env", SCRIPT)
prepare_runtime_env = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(prepare_runtime_env)


def dotenv_unquote(value: str) -> str:
    """Decode this helper's Compose single-quoted dotenv literal format."""
    assert value.startswith("'") and value.endswith("'")
    inner = value[1:-1]
    result: list[str] = []
    index = 0
    while index < len(inner):
        if inner[index:index + 2] == "\\'":
            result.append("'")
            index += 2
        else:
            result.append(inner[index])
            index += 1
    return "".join(result)


def parse_dotenv(text: str) -> dict[str, str]:
    """Read generated single-quoted values, including Compose multiline values."""
    lines = iter(text.splitlines())
    parsed = {}
    for line in lines:
        if not line or line.startswith("#"):
            continue
        key, first = line.split("=", 1)
        parts = [first]
        while not ends_with_unescaped_quote(parts[-1]):
            parts.append(next(lines))
        parsed[key] = dotenv_unquote("\n".join(parts))
    return parsed


def ends_with_unescaped_quote(value: str) -> bool:
    if not value.endswith("'"):
        return False
    backslashes = len(value[:-1]) - len(value[:-1].rstrip("\\"))
    return backslashes % 2 == 0


class PrepareRuntimeEnvTests(unittest.TestCase):
    def fixture(self):
        value = "dollar $; escaped $$; ${LITERAL}; quote ' \"; slash \\\\;\nline two"
        rendered = {
            "name": "news",
            "services": {
                "app": {
                    "image": "news/web:tag",
                    "env_file": [{"path": "/root/news/app.env", "required": True}],
                    "environment": {"DATABASE_URL": "postgres://web", "WEB_BACKEND_TOKEN": value, "EMPTY_PRESENT": ""},
                    "healthcheck": {"test": ["CMD-SHELL", "echo $$HOME ${LITERAL} $VALUE"]},
                    "command": ["sh", "-c", "printf '$$HOME'"],
                },
                "executor": {
                    "image": "news/executor:tag",
                    "env_file": [{"path": "/root/news/executor.env", "required": True}],
                    "environment": {"DATABASE_URL": "postgres://executor", "GEMINI_API_KEY": "key"},
                    "labels": {"literal": "${RENDERED} and $$"},
                },
            },
            "networks": {"default": {"name": "news_default"}},
        }
        return value, rendered

    def test_canonical_keys_are_role_scoped_and_env_files_removed(self):
        _, rendered = self.fixture()
        candidate, dotenv, audit = prepare_runtime_env.prepare_config(rendered, "news")
        self.assertNotIn("env_file", candidate["services"]["app"])
        self.assertNotIn("env_file", candidate["services"]["executor"])
        self.assertEqual(candidate["services"]["app"]["environment"]["DATABASE_URL"], "${WEB_DATABASE_URL?Required by generated runtime environment}")
        self.assertEqual(candidate["services"]["executor"]["environment"]["DATABASE_URL"], "${EXECUTOR_DATABASE_URL?Required by generated runtime environment}")
        self.assertEqual(audit["services"]["app"]["key_mapping"]["WEB_BACKEND_TOKEN"], "WEB_BACKEND_TOKEN")
        self.assertIn("/root/news/app.env", audit["env_file_references"]["app"])
        self.assertNotIn("postgres://", json.dumps(audit))
        self.assertNotIn("postgres://web", json.dumps(audit))
        self.assertNotIn("postgres://executor", json.dumps(audit))
        self.assertIn("EMPTY_PRESENT=''", dotenv)

    def test_dotenv_and_candidate_round_trip_special_values_and_non_env_dollars(self):
        value, rendered = self.fixture()
        candidate, dotenv, _ = prepare_runtime_env.prepare_config(rendered, "news")
        env_values = parse_dotenv(dotenv)
        self.assertEqual(env_values["WEB_BACKEND_TOKEN"], value.replace("$$", "$"))
        self.assertEqual(env_values["EMPTY_PRESENT"], "")

        reparsed = candidate
        for service_name, original_service in rendered["services"].items():
            candidate_service = reparsed["services"][service_name]
            original_environment = original_service["environment"]
            self.assertEqual(set(candidate_service["environment"]), set(original_environment))
            for key, value in original_environment.items():
                reference = candidate_service["environment"][key]
                canonical = reference.removeprefix("${").split("?", 1)[0].removesuffix("}")
                self.assertEqual(env_values[canonical], value.replace("$$", "$"))
            for field in ("healthcheck", "command", "labels"):
                if field in original_service:
                    self.assertEqual(candidate_service[field], original_service[field])

    def test_ambiguous_canonical_collision_fails_closed(self):
        _, rendered = self.fixture()
        rendered["services"]["web"] = {"environment": {"DATABASE_URL": "another"}}
        with self.assertRaises(prepare_runtime_env.PrepareError) as raised:
            prepare_runtime_env.prepare_config(rendered, "news")
        self.assertEqual(str(raised.exception), "canonical_key_collision")

    def test_distinct_names_with_equal_values_remain_distinct(self):
        rendered = {
            "services": {
                "web": {"environment": {"WEB_TOKEN_ALIAS": "same synthetic value"}},
                "executor": {"environment": {"EXECUTOR_TOKEN_ALIAS": "same synthetic value"}},
            }
        }
        candidate, dotenv, _ = prepare_runtime_env.prepare_config(rendered, "news")
        self.assertEqual(candidate["services"]["web"]["environment"]["WEB_TOKEN_ALIAS"], "${WEB_TOKEN_ALIAS?Required by generated runtime environment}")
        self.assertEqual(candidate["services"]["executor"]["environment"]["EXECUTOR_TOKEN_ALIAS"], "${EXECUTOR_TOKEN_ALIAS?Required by generated runtime environment}")
        self.assertIn("WEB_TOKEN_ALIAS='same synthetic value'", dotenv)
        self.assertIn("EXECUTOR_TOKEN_ALIAS='same synthetic value'", dotenv)

    def test_dotenv_rejects_unsupported_backslash_edges(self):
        for value in ("backslash before quote \\' and more", "trailing backslash \\"):
            with self.subTest(value=value):
                with self.assertRaises(prepare_runtime_env.PrepareError) as raised:
                    prepare_runtime_env._dotenv_quote(value)
                self.assertEqual(str(raised.exception), "unsupported_environment_value")

    def test_prepare_does_not_overwrite_and_outputs_are_private(self):
        _, rendered = self.fixture()
        candidate, dotenv, audit = prepare_runtime_env.prepare_config(rendered, "news")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "candidate"
            prepare_runtime_env.write_outputs(output, candidate, dotenv, audit)
            self.assertEqual(output.stat().st_mode & 0o777, 0o700)
            for name in ("compose.candidate.json", ".env", "audit.json"):
                self.assertEqual((output / name).stat().st_mode & 0o777, 0o600)
            with self.assertRaises(prepare_runtime_env.PrepareError) as raised:
                prepare_runtime_env.write_outputs(output, candidate, dotenv, audit)
            self.assertEqual(str(raised.exception), "output_directory_exists")

    def test_compose_render_ignores_ambient_interpolation_variables(self):
        calls = []

        def run(command, **kwargs):
            calls.append((command, kwargs))
            return subprocess.CompletedProcess(command, 0, '{"services": {"web": {}}}', "")

        rendered = prepare_runtime_env.render_compose(Path("/tmp/synthetic-compose.yml"), "news", run=run)
        self.assertIn("services", rendered)
        self.assertEqual(calls[0][1]["env"].get("WEB_DATABASE_URL"), None)
        self.assertIn("--env-file", calls[0][0])

    def test_source_env_selection_and_missing_explicit_file(self):
        with tempfile.TemporaryDirectory() as directory:
            compose = Path(directory) / "compose.yml"
            compose.write_text("services: {}\n", encoding="utf-8")
            default_env = Path(directory) / ".env"
            default_env.write_text("VALUE=source\n", encoding="utf-8")
            self.assertEqual(prepare_runtime_env.resolve_source_env(compose), default_env.resolve())
            alternate = Path(directory) / "alternate.env"
            alternate.write_text("VALUE=alternate\n", encoding="utf-8")
            self.assertEqual(prepare_runtime_env.resolve_source_env(compose, alternate), alternate.resolve())
            with self.assertRaises(prepare_runtime_env.PrepareError) as raised:
                prepare_runtime_env.resolve_source_env(compose, Path(directory) / "missing.env")
            self.assertEqual(str(raised.exception), "source_interpolation_file_not_found")

    def test_cli_audit_records_only_source_interpolation_path(self):
        _, rendered = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            compose = root / "compose.yml"
            compose.write_text("services: {}\n", encoding="utf-8")
            source_env = root / "alternate.env"
            source_env.write_text("SYNTHETIC_VALUE=not-for-audit\n", encoding="utf-8")
            output = root / "candidate"
            stdout = io.StringIO()
            with patch.object(prepare_runtime_env, "render_compose", return_value=rendered):
                with contextlib.redirect_stdout(stdout):
                    result = prepare_runtime_env.main([
                        "--compose", str(compose),
                        "--source-env", str(source_env),
                        "--output-dir", str(output),
                        "--project", "news",
                    ])
            self.assertEqual(result, 0)
            audit_text = (output / "audit.json").read_text(encoding="utf-8")
            audit = json.loads(audit_text)
            self.assertEqual(audit["source_interpolation_file"], str(source_env.resolve()))
            self.assertNotIn("not-for-audit", audit_text)
            self.assertIn("candidate prepared", stdout.getvalue())

    @unittest.skipUnless(shutil.which("docker"), "Docker Compose is unavailable")
    def test_docker_compose_config_round_trip_preserves_effective_values(self):
        values = {
            "DOLLAR": "single $ and source $$ and ${LITERAL}",
            "QUOTES": "single ' and double \" quotes",
            "JSON_VALUE": '{"key":"value with spaces","count":2}',
            "EMPTY_VALUE": "",
            "ORDINARY_MODE": "source-mode",
            "CR": "before\rafter",
            "CRLF": "before\r\nafter",
            "MULTILINE": "line one\nline two",
            "UNICODE": "serbian č ć ž š đ · 日本語 · 📰",
        }
        clean_env = {key: os.environ[key] for key in ("PATH", "HOME", "DOCKER_CONFIG") if key in os.environ}

        def render(compose_file: Path, env_file: Path) -> dict:
            result = subprocess.run(
                ["docker", "compose", "--env-file", str(env_file), "-f", str(compose_file), "config", "--format", "json"],
                env=clean_env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, "synthetic Compose rendering failed")
            return json.loads(result.stdout)

        with tempfile.TemporaryDirectory(prefix="news-env-round-trip-") as directory:
            root = Path(directory)
            source_path = root / "source.yml"
            source_path.write_text(
                "name: news\n"
                "services:\n"
                "  web:\n"
                "    image: ${IMAGE_REPOSITORY:?required}/${IMAGE_NAME}:${IMAGE_TAG:?required}\n"
                "    env_file: [\"${ENV_FILE_DIR:?required}/source.env\"]\n"
                "    environment:\n"
                "      ORDINARY_MODE: ${ORDINARY_MODE:-default-mode}\n"
                "    command: [\"sh\", \"-c\", \"printf '$$HOME $${COMMAND}'\"]\n"
                "    healthcheck:\n"
                "      test: [\"CMD-SHELL\", \"echo $$HOME $${HEALTH}\"]\n"
                "    labels:\n"
                "      literal: \"$${LABEL} and $$\"\n",
                encoding="utf-8",
            )
            source_dotenv = root / ".env"
            source_dotenv.write_text(
                "IMAGE_REPOSITORY=example.invalid\n"
                "IMAGE_NAME=news\n"
                "IMAGE_TAG=source-tag\n"
                f"ENV_FILE_DIR={root}\n"
                "ORDINARY_MODE=source-mode\n",
                encoding="utf-8",
            )
            source_env = root / "source.env"
            source_env.write_text(
                "DOLLAR='single $ and source $$ and ${LITERAL}'\n"
                "QUOTES='single \\' and double \" quotes'\n"
                "JSON_VALUE='{" + '"key":"value with spaces","count":2' + "}'\n"
                "EMPTY_VALUE=''\n"
                "CR='before\rafter'\n"
                "CRLF='before\r\nafter'\n"
                "MULTILINE='line one\nline two'\n"
                "UNICODE='serbian č ć ž š đ · 日本語 · 📰'\n",
                encoding="utf-8",
                newline="",
            )
            baseline = render(source_path, source_dotenv)
            with patch.dict(os.environ, {
                "IMAGE_REPOSITORY": "ambient.invalid",
                "IMAGE_NAME": "ambient",
                "IMAGE_TAG": "ambient-tag",
                "ENV_FILE_DIR": "/ambient/missing",
                "ORDINARY_MODE": "ambient-mode",
            }):
                original = prepare_runtime_env.render_compose(source_path.resolve(), "news")
            self.assertEqual(original, baseline)
            self.assertEqual(original["services"]["web"]["image"], "example.invalid/news:source-tag")
            self.assertEqual(original["services"]["web"]["environment"]["ORDINARY_MODE"], "source-mode")
            self.assertEqual(
                prepare_runtime_env.resolve_source_env(source_path.resolve()),
                source_dotenv.resolve(),
            )
            candidate, dotenv, _ = prepare_runtime_env.prepare_config(original, "news")
            candidate_path = root / "candidate.json"
            candidate_path.write_text(json.dumps(candidate), encoding="utf-8")
            candidate_env = root / "candidate.env"
            candidate_env.write_text(dotenv, encoding="utf-8", newline="")
            actual = render(candidate_path, candidate_env)
            missing_env = root / "missing.env"
            missing_env.write_text(
                "\n".join(line for line in dotenv.splitlines() if not line.startswith("DOLLAR=")) + "\n",
                encoding="utf-8",
                newline="",
            )
            missing = subprocess.run(
                ["docker", "compose", "--env-file", str(missing_env), "-f", str(candidate_path), "config", "--format", "json"],
                env=clean_env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(missing.returncode, 0, "a missing required environment key must fail closed")

        def effective(values: dict[str, str]) -> dict[str, str]:
            return {key: value.replace("$$", "$") for key, value in values.items()}

        expected_env = effective(original["services"]["web"]["environment"])
        actual_env = effective(actual["services"]["web"]["environment"])
        self.assertEqual(expected_env, values)
        self.assertEqual(expected_env, actual_env)
        for field in ("image", "command", "healthcheck", "labels"):
            self.assertEqual(original["services"]["web"][field], actual["services"]["web"][field])


if __name__ == "__main__":
    unittest.main()
