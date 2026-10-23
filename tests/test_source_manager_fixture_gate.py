import json
from types import SimpleNamespace

import pytest

from scripts import run_source_manager_fixture_gate as gate


def _inspect_result(*, returncode=0, stderr="", architecture="amd64"):
    payload = [{"Id": "sha256:" + "a" * 64,
                "Os": "linux", "Architecture": architecture,
                "RepoDigests": ["alpine@sha256:" + "b" * 64]}]
    return SimpleNamespace(returncode=returncode, stderr=stderr,
                           stdout=json.dumps(payload))


def test_old_docker_inspect_falls_back_on_native_amd64(monkeypatch):
    calls = []
    responses = [
        SimpleNamespace(returncode=125, stderr="unknown flag: --platform", stdout=""),
        _inspect_result(),
    ]

    def fake_run(args, **kwargs):
        calls.append(args)
        return responses.pop(0)

    monkeypatch.setattr(gate.subprocess, "run", fake_run)
    inspected, fallback = gate.inspect_linux_amd64_image("alpine@sha256:" + "b" * 64, "x86_64")

    assert inspected["Architecture"] == "amd64"
    assert fallback is True
    assert len(calls) == 2
    assert "--platform" in calls[0]
    assert "--platform" not in calls[1]


def test_old_docker_inspect_does_not_fallback_on_arm_engine(monkeypatch):
    calls = []

    def fake_run(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=125, stderr="unknown flag: --platform", stdout="")

    monkeypatch.setattr(gate.subprocess, "run", fake_run)
    with pytest.raises(gate.GateError) as error:
        gate.inspect_linux_amd64_image("alpine:3.21", "aarch64")

    assert error.value.code == "platform_inspect_unsupported_on_non_amd64_engine"
    assert len(calls) == 1


def test_native_amd64_fallback_rejects_wrong_architecture(monkeypatch):
    responses = [
        SimpleNamespace(returncode=125, stderr="unknown flag: --platform", stdout=""),
        _inspect_result(architecture="arm64"),
    ]
    monkeypatch.setattr(gate.subprocess, "run", lambda *args, **kwargs: responses.pop(0))

    with pytest.raises(gate.GateError) as error:
        gate.inspect_linux_amd64_image("alpine:3.21", "amd64")

    assert error.value.code == "fixture_image_not_linux_amd64"
