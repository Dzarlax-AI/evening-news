#!/usr/bin/env python3
"""Check that locally built core runtime images contain only their runtime payloads."""

from __future__ import annotations

import subprocess
import sys
from typing import Callable


IMAGES = {
    "web": "evening-news-web:consolidation-fixture",
    "background": "evening-news-background:consolidation-fixture",
    "executor": "evening-news-executor:consolidation-fixture",
}

CHECKS = {
    "web": (
        "test -x /news-web && test -s /app/go-web/contracts/openapi.json "
        "&& test -d /app/go-web/templates && test -d /app/web/static "
        "&& test ! -e /news-background && ! command -v python >/dev/null 2>&1 "
        "&& ! command -v go >/dev/null 2>&1"
    ),
    "background": (
        "test -x /news-background && test -s /usr/share/zoneinfo/Europe/Belgrade "
        "&& test ! -e /news-web && test ! -d /app "
        "&& ! command -v python >/dev/null 2>&1 && ! command -v go >/dev/null 2>&1"
    ),
    "executor": (
        "test -s /app/news_aggregator/executor.py && command -v pg_dump >/dev/null "
        "&& python -c 'from pathlib import Path; source=Path(\"/app/news_aggregator/executor.py\").read_text(); assert \"class PythonJobExecutor\" not in source' "
        "&& python -c 'import importlib.util,asyncpg,sqlalchemy; assert importlib.util.find_spec(\"pytest\") is None and all(importlib.util.find_spec(name) is None for name in (\"magic\",\"psutil\",\"chardet\",\"dateutil\"))' "
        "&& ! dpkg-query -W -f='${Status}' libmagic1 libmagic1t64 2>/dev/null | grep -q 'install ok installed' "
        "&& test ! -d /app/scripts && test ! -d /app/web "
        "&& test ! -e /news-web && test ! -e /news-background "
        "&& ! command -v go >/dev/null 2>&1"
    ),
}


class InventoryError(Exception):
    """A short, safe inventory failure code."""


def check_images(run: Callable[..., subprocess.CompletedProcess] = subprocess.run) -> None:
    for target, image in IMAGES.items():
        result = run(
            [
                "docker", "run", "--rm", "--network", "none", "--read-only",
                "--entrypoint", "/bin/sh", image, "-ec", CHECKS[target],
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise InventoryError(f"{target}_inventory_failed")


def main() -> int:
    try:
        check_images()
    except InventoryError as exc:
        print(f"core image inventory check failed ({exc})", file=sys.stderr)
        return 1
    print("Core runtime image inventory checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
