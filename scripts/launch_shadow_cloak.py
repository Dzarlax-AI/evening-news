"""Launch the pinned Cloak browser binary inside an isolated shadow namespace.

This bypasses only cloakserve's external CDP binding. Browser arguments come
from the pinned image's own definitions and remain fixed for this experiment.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import runpy

CLOAKSERVE = "/usr/local/bin/cloakserve"
USER_DATA_DIR = "/tmp/native-shadow-browser"
DEBUG_ARGS = ["--remote-debugging-port=5100", "--remote-debugging-address=127.0.0.1",
              f"--user-data-dir={USER_DATA_DIR}"]


def launch_spec(definitions: dict, spki: str | None = None) -> tuple[Path, list[str]]:
    binary = Path(definitions["ensure_binary"]())
    if not binary.is_file():
        raise RuntimeError("Pinned browser binary is unavailable")
    args = list(definitions["BASE_CHROME_ARGS"])
    args += list(definitions["build_args"](
        stealth_args=True, extra_args=["--fingerprint=57292"],
        timezone=None, locale=None, headless=True))
    if any(arg.startswith(("--remote-debugging-port", "--remote-debugging-address", "--user-data-dir"))
           for arg in args):
        raise RuntimeError("Pinned arguments conflict with isolated CDP binding")
    if spki is not None:
        try:
            if len(base64.b64decode(spki, validate=True)) != 32:
                raise ValueError
        except (ValueError, base64.binascii.Error):
            raise ValueError("SPKI must be a single base64 SHA-256 digest") from None
        args.append(f"--ignore-certificate-errors-spki-list={spki}")
    args += DEBUG_ARGS
    return binary, args


def write_launch_info(info_dir: Path, binary: Path, args: list[str]) -> Path:
    if not info_dir.is_absolute() or info_dir.is_symlink() or not info_dir.is_dir():
        raise RuntimeError("Info directory must be an existing absolute directory")
    digest = hashlib.sha256()
    with binary.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    payload = json.dumps({"version": 1, "binary_sha256": digest.hexdigest(),
                          "binary_path": str(binary), "args": args}, separators=(",", ":")).encode()
    temporary = info_dir / "launch-info.json.tmp"
    final = info_dir / "launch-info.json"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, final)
    finally:
        temporary.unlink(missing_ok=True)
    return final


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spki", help="Bootstrap fixture only: one base64 SHA-256 SPKI digest")
    parser.add_argument("--info-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    os.environ["CLOAKBROWSER_AUTO_UPDATE"] = "false"
    definitions = runpy.run_path(CLOAKSERVE, run_name="shadow_defs")
    binary, chrome_args = launch_spec(definitions, args.spki)
    write_launch_info(args.info_dir, binary, chrome_args)
    os.execv(str(binary), [str(binary), *chrome_args])


if __name__ == "__main__":
    main()
