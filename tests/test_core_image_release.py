import json
from pathlib import Path
import sys
import tempfile
import unittest
from subprocess import CompletedProcess

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.core_image_release import CORE_IMAGES, ReleaseError, publish_core_images


REVISION = "0123456789abcdef0123456789abcdef01234567"
IMAGE_ID = "sha256:" + "a" * 64
DIGEST = "sha256:" + "b" * 64


class CoreImageReleaseTests(unittest.TestCase):
    def write_acceptance(self, directory):
        report = {
            "result": "passed",
            "image_ids": {target: IMAGE_ID for target in CORE_IMAGES},
            "image_revisions": {target: REVISION for target in CORE_IMAGES},
            "image_tags": {target: values[0] for target, values in CORE_IMAGES.items()},
        }
        (directory / "accepted.json").write_text(json.dumps(report))

    def fake_runner(self, fail=None):
        calls = []

        def run(command, **kwargs):
            calls.append(command)
            joined = " ".join(command)
            target = next((
                name for name, (local_ref, repository) in CORE_IMAGES.items()
                if local_ref in joined or f"ghcr.io/{repository}:" in joined
            ), None)
            if fail == (target, command[1] if len(command) > 1 else None):
                return CompletedProcess(command, 1, "secret details", "private output")
            if command[1:3] == ["image", "inspect"]:
                return CompletedProcess(command, 0, f"{IMAGE_ID} linux/amd64 {REVISION}\n", "")
            if command[1] == "push":
                return CompletedProcess(command, 0, f"pushed digest: {DIGEST}\n", "")
            return CompletedProcess(command, 0, "", "")

        return run, calls

    def test_publishes_the_loaded_images_and_records_each_digest(self):
        runner, calls = self.fake_runner()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "release-images.json"
            acceptance_dir = Path(directory) / "acceptance"
            acceptance_dir.mkdir()
            self.write_acceptance(acceptance_dir)
            manifest = publish_core_images(REVISION, "linux/amd64", output, acceptance_dir, run=runner)
            self.assertEqual(set(manifest["images"]), {"web", "background", "executor"})
            saved = json.loads(output.read_text())
            self.assertEqual(saved, manifest)
            self.assertEqual(manifest["images"]["background"]["repository"], "dzarlax-ai/rss-background")
            self.assertEqual(manifest["images"]["executor"]["repository"], "dzarlax-ai/rss-summarizer")
            self.assertEqual(sum(command[1] == "push" for command in calls), 3)
            self.assertEqual(sum(command[1] == "build" for command in calls), 0)

    def test_late_push_failure_leaves_no_manifest_and_does_not_leak_output(self):
        runner, _ = self.fake_runner(fail=("executor", "push"))
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "release-images.json"
            acceptance_dir = Path(directory) / "acceptance"
            acceptance_dir.mkdir()
            self.write_acceptance(acceptance_dir)
            with self.assertRaises(ReleaseError) as caught:
                publish_core_images(REVISION, "linux/amd64", output, acceptance_dir, run=runner)
            self.assertEqual(str(caught.exception), "executor_push_failed")
            self.assertFalse(output.exists())

    def test_all_local_metadata_is_verified_before_the_first_push(self):
        calls = []

        def wrong_revision(command, **kwargs):
            calls.append(command)
            if command[1:3] == ["image", "inspect"]:
                revision = "f" * 40 if "executor" in " ".join(command) else REVISION
                return CompletedProcess(command, 0, f"{IMAGE_ID} linux/amd64 {revision}\n", "")
            return CompletedProcess(command, 0, f"pushed digest: {DIGEST}\n", "")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "release-images.json"
            acceptance_dir = Path(directory) / "acceptance"
            acceptance_dir.mkdir()
            self.write_acceptance(acceptance_dir)
            with self.assertRaisesRegex(ReleaseError, "executor_revision_mismatch"):
                publish_core_images(REVISION, "linux/amd64", output, acceptance_dir, run=wrong_revision)
            self.assertFalse(any(command[1] == "push" for command in calls))
            self.assertFalse(output.exists())

    def test_rejects_invalid_revision_platform_or_existing_manifest_before_docker(self):
        runner, calls = self.fake_runner()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "release-images.json"
            acceptance_dir = Path(directory) / "acceptance"
            acceptance_dir.mkdir()
            self.write_acceptance(acceptance_dir)
            with self.assertRaisesRegex(ReleaseError, "revision_invalid"):
                publish_core_images("not-a-commit", "linux/amd64", output, acceptance_dir, run=runner)
            with self.assertRaisesRegex(ReleaseError, "platform_invalid"):
                publish_core_images(REVISION, "linux/arm64", output, acceptance_dir, run=runner)
            output.write_text("existing")
            with self.assertRaisesRegex(ReleaseError, "manifest_already_exists"):
                publish_core_images(REVISION, "linux/amd64", output, acceptance_dir, run=runner)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
