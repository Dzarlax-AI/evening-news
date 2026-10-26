#!/usr/bin/env python3
"""Tag and publish the already-tested local core images, then record their digests."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Callable, Sequence


CORE_IMAGES = {
    "web": ("evening-news-web:consolidation-fixture", "dzarlax-ai/rss-web"),
    "background": (
        "evening-news-background:consolidation-fixture",
        "dzarlax-ai/rss-background",
    ),
    "executor": (
        "evening-news-executor:consolidation-fixture",
        "dzarlax-ai/rss-summarizer",
    ),
}
REVISION_RE = re.compile(r"[0-9a-f]{40}")
DIGEST_RE = re.compile(r"\bdigest:\s*(sha256:[0-9a-f]{64})\b")
IMAGE_ID_RE = re.compile(r"sha256:[0-9a-f]{64}")


class ReleaseError(Exception):
    """A bounded release failure that avoids echoing command output."""


def publish_core_images(
    revision: str,
    platform: str,
    output: Path,
    acceptance_dir: Path,
    run: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> dict:
    if not REVISION_RE.fullmatch(revision):
        raise ReleaseError("revision_invalid")
    if platform != "linux/amd64":
        raise ReleaseError("platform_invalid")
    if output.exists():
        raise ReleaseError("manifest_already_exists")

    reports = sorted(acceptance_dir.glob("*.json")) if acceptance_dir.is_dir() else []
    if len(reports) != 1:
        raise ReleaseError("acceptance_report_missing_or_ambiguous")
    try:
        acceptance = json.loads(reports[0].read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raise ReleaseError("acceptance_report_invalid") from None
    if not isinstance(acceptance, dict) or acceptance.get("result") != "passed":
        raise ReleaseError("acceptance_not_passed")
    accepted_ids = acceptance.get("image_ids")
    accepted_revisions = acceptance.get("image_revisions")
    accepted_tags = acceptance.get("image_tags")
    if not all(isinstance(value, dict) for value in (accepted_ids, accepted_revisions, accepted_tags)):
        raise ReleaseError("acceptance_report_invalid")
    for target, (local_ref, _repository) in CORE_IMAGES.items():
        if accepted_tags.get(target) != local_ref:
            raise ReleaseError(f"{target}_acceptance_tag_mismatch")
        if accepted_revisions.get(target) != revision:
            raise ReleaseError(f"{target}_acceptance_revision_mismatch")
        if not isinstance(accepted_ids.get(target), str) or not IMAGE_ID_RE.fullmatch(accepted_ids[target]):
            raise ReleaseError(f"{target}_acceptance_image_id_invalid")

    local_images = {}
    # Validate every loaded candidate before tagging or pushing any of them.
    # This prevents a bad later target from being discovered after an earlier
    # repository has already accepted its immutable SHA tag.
    for target, (local_ref, repository) in CORE_IMAGES.items():
        inspect = run(
            [
                "docker", "image", "inspect",
                '--format={{.Id}} {{.Os}}/{{.Architecture}} {{index .Config.Labels "org.opencontainers.image.revision"}}',
                local_ref,
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        fields = inspect.stdout.strip().split() if inspect.returncode == 0 else []
        if len(fields) != 3 or not IMAGE_ID_RE.fullmatch(fields[0]):
            raise ReleaseError(f"{target}_image_unavailable")
        image_id, image_platform, image_revision = fields
        if image_platform != platform:
            raise ReleaseError(f"{target}_platform_mismatch")
        if image_revision != revision:
            raise ReleaseError(f"{target}_revision_mismatch")
        if accepted_ids.get(target) != image_id:
            raise ReleaseError(f"{target}_acceptance_image_id_mismatch")
        local_images[target] = {
            "local_ref": local_ref,
            "repository": repository,
            "image_id": image_id,
        }

    images = {}
    for target, candidate in local_images.items():
        local_ref = candidate["local_ref"]
        repository = candidate["repository"]
        image_id = candidate["image_id"]
        tag = f"ghcr.io/{repository}:sha-{revision}"

        tagged = run(["docker", "tag", image_id, tag], text=True, capture_output=True, check=False)
        if tagged.returncode != 0:
            raise ReleaseError(f"{target}_tag_failed")

        pushed = run(["docker", "push", tag], text=True, capture_output=True, check=False)
        if pushed.returncode != 0:
            # A registry can accept an earlier target before a later push fails.
            # Leave no release manifest and never try to replace immutable tags.
            raise ReleaseError(f"{target}_push_failed")
        found = DIGEST_RE.findall(pushed.stdout + "\n" + pushed.stderr)
        if not found:
            raise ReleaseError(f"{target}_digest_missing")
        images[target] = {
            "repository": repository,
            "revision": revision,
            "platform": platform,
            "imageID": image_id,
            "digest": found[-1],
        }

    manifest = {
        "schemaVersion": 1,
        "revision": revision,
        "platform": platform,
        "images": images,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output.open("x", encoding="utf-8") as stream:
            json.dump(manifest, stream, indent=2, sort_keys=True)
            stream.write("\n")
    except FileExistsError:
        raise ReleaseError("manifest_already_exists") from None
    return manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--platform", default="linux/amd64")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--acceptance-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        manifest = publish_core_images(args.revision, args.platform, args.output, args.acceptance_dir)
    except ReleaseError as exc:
        print(f"core image release failed ({exc})", file=sys.stderr)
        return 1
    print(f"Recorded three verified core image digests in {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
