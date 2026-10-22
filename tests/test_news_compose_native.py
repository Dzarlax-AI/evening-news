"""Structural checks for the shared-browser and native-worker Compose profiles."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
COMPOSE_SHARED = ROOT / "deploy/news-extractor/compose.shared-browser.yml"
COMPOSE_NATIVE = ROOT / "deploy/news-extractor/compose.native.yml"


def compose_config(files: list[Path], env: dict[str, str]) -> dict:
    compose = shutil.which("docker")
    if compose is None:
        raise unittest.SkipTest("Docker Compose is unavailable")
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as empty_env:
        empty_env.flush()
        command = [compose, "compose", "--profile", "legacy-browser-rollback",
                   "--env-file", empty_env.name]
        for path in files:
            command.extend(("-f", str(path)))
        command.extend(("config", "--format", "json"))
        result = subprocess.run(command, cwd=ROOT, env={**os.environ, **env},
                                capture_output=True, text=True, check=False)
    if result.returncode:
        # Compose output may include interpolated credentials. Report only that
        # rendering failed; never expose command output from the test failure.
        raise AssertionError("Docker Compose could not render the synthetic merge")
    return json.loads(result.stdout)


class NativeComposeTests(unittest.TestCase):
    def test_shared_browser_replaces_legacy_chrome_attachment(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory) / "infra.yml"
            base.write_text(
                """services:
  chrome:
    image: old/chrome:latest
    command: [--remote-debugging-port=9222]
    ports: ["127.0.0.1:9222:9222"]
    networks: [infra]
    profiles: [legacy-browser-rollback]
    restart: 'no'
    volumes: [browser-control:/browser-control]
    healthcheck:
      test: [CMD, old-healthcheck]
volumes:
  browser-control:
    external: true
networks:
  infra:
    external: true
""",
                encoding="utf-8",
            )
            config = compose_config(
                [base, COMPOSE_SHARED],
                {
                    "NEWS_BROWSER_IMAGE": "example.invalid/browser@sha256:" + "a" * 64,
                    "NEWS_BROWSER_IP": "10.77.0.2",
                    "NEWS_PROXY_IP": "10.77.0.5",
                    "NEWS_APP_IP": "10.77.0.3",
                    "NEWS_WORKER_IP": "10.77.0.4",
                    "NEWS_BROWSER_CONTROLLER_IPS": '{"news_app":"10.77.0.3","worker":"10.77.0.4"}',
                    "NEWS_CONTROL_NETWORK": "news-browser-control",
                    "NEWS_CONTROL_SUBNET": "10.77.0.0/24",
                    "NEWS_BROWSER_CONTROL_VOLUME": "infra_browser-control",
                },
            )

        browser = config["services"]["chrome"]
        self.assertEqual(browser["image"], "example.invalid/browser@sha256:" + "a" * 64)
        self.assertEqual(browser["networks"], {"private-control": {"ipv4_address": "10.77.0.2"}})
        self.assertEqual(browser["entrypoint"], ["python", "/opt/news-browser/entrypoint.py"])
        self.assertEqual(browser["command"], [])
        self.assertFalse(browser.get("ports"))
        self.assertEqual(browser["restart"], "unless-stopped")
        self.assertEqual(browser["stop_grace_period"], "40s")
        self.assertNotIn("profiles", browser)
        self.assertEqual(browser["healthcheck"]["test"], [
            "CMD", "python", "/opt/news-browser/entrypoint.py", "--healthcheck",
        ])
        self.assertEqual(
            [(volume["source"], volume["target"]) for volume in browser["volumes"]],
            [("browser-control", "/browser-control")],
        )
        self.assertEqual(config["volumes"]["browser-control"]["name"], "infra_browser-control")
        self.assertTrue(config["volumes"]["browser-control"]["external"])
        self.assertFalse(any("ports" in service for service in config["services"].values()))
        self.assertTrue(browser["read_only"])
        self.assertIn("/root/.config:rw,nosuid,nodev,size=16m", browser["tmpfs"])
        self.assertIn("/root/.cache:rw,nosuid,nodev,size=16m", browser["tmpfs"])
        self.assertEqual(set(browser["cap_add"]), {"NET_ADMIN", "SETPCAP", "SETUID", "SETGID", "KILL"})

    def test_native_profile_has_private_authenticated_services(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory) / "news.yml"
            base.write_text(
                """services:
  app:
    image: old/app:latest
    environment: {KEEP_EXISTING_SETTING: yes}
    networks: [infra, traefik]
networks:
  infra:
    external: true
  traefik:
    external: true
""",
                encoding="utf-8",
            )
            config = compose_config(
                [base, COMPOSE_NATIVE],
                {
                    "NEWS_APP_IMAGE": "example.invalid/app@sha256:" + "b" * 64,
                    "NEWS_EXTRACTOR_IMAGE": "example.invalid/extractor@sha256:" + "c" * 64,
                    "NEWS_BROWSER_IP": "10.77.0.2",
                    "NEWS_APP_IP": "10.77.0.3",
                    "NEWS_WORKER_IP": "10.77.0.4",
                    "NEWS_PROXY_IP": "10.77.0.5",
                    "NEWS_CONTROL_NETWORK": "news-browser-control",
                    "NEWS_OUTBOUND_NETWORK": "news-extractor-outbound",
                    "NEWS_OUTBOUND_SUBNET": "10.78.0.0/24",
                    "NEWS_BROWSER_CONTROL_VOLUME": "infra_browser-control",
                    "NEWS_PROXY_TOKEN": "placeholder-proxy-token-123",
                    "NEWS_EXTRACTOR_TOKEN": "placeholder-worker-token-456",
                },
            )

        services = config["services"]
        app = services["app"]
        worker = services["extractor"]
        egress = services["egress"]
        self.assertEqual(app["image"], "example.invalid/app@sha256:" + "b" * 64)
        self.assertEqual(app["environment"]["KEEP_EXISTING_SETTING"], "yes")
        self.assertEqual(app["environment"]["EXTRACTION_SERVICE_MODE"], "go")
        self.assertEqual(app["environment"]["EXTRACTION_SERVICE_NATIVE_BROWSER"], "true")
        self.assertEqual(app["environment"]["EXTRACTION_SERVICE_PYTHON_FALLBACK"], "false")
        self.assertIn(
            ("browser-control", "/browser-control"),
            [(volume["source"], volume["target"]) for volume in app["volumes"]],
        )
        self.assertEqual(set(app["networks"]), {"infra", "traefik", "private-control"})
        self.assertEqual(app["networks"]["private-control"]["ipv4_address"], "10.77.0.3")
        self.assertEqual(worker["networks"]["private-control"]["ipv4_address"], "10.77.0.4")
        self.assertEqual(egress["networks"]["private-control"]["ipv4_address"], "10.77.0.5")
        self.assertEqual(set(worker["networks"]), {"private-control", "outbound"})
        self.assertEqual(set(egress["networks"]), {"private-control", "outbound"})
        self.assertNotIn("ports", worker)
        self.assertNotIn("ports", egress)
        self.assertEqual(egress["entrypoint"], ["/extract-egress"])
        self.assertEqual(egress["environment"]["EGRESS_MAX_CONCURRENT"], "4")
        self.assertTrue(worker["read_only"])
        self.assertTrue(egress["read_only"])
        self.assertEqual(worker["stop_grace_period"], "55s")


if __name__ == "__main__":
    unittest.main()
