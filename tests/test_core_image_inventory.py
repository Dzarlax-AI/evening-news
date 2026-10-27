from pathlib import Path
import sys
import unittest
from subprocess import CompletedProcess

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.check_core_image_inventory import CHECKS, IMAGES, InventoryError, check_images


class CoreImageInventoryTests(unittest.TestCase):
    def test_executor_image_guard_rejects_removed_python_job_runner(self):
        self.assertIn("class PythonJobExecutor", CHECKS["executor"])
        self.assertNotIn("class PythonJobExecutor", (Path(__file__).resolve().parents[1] / "news_aggregator/executor.py").read_text())

    def test_inventory_checks_each_role_specific_local_target(self):
        calls = []

        def run(command, **kwargs):
            calls.append(command)
            return CompletedProcess(command, 0, "", "")

        check_images(run=run)
        self.assertEqual(len(calls), 3)
        for target, image in IMAGES.items():
            call = next(command for command in calls if image in command)
            self.assertEqual(call[1:8], ["run", "--rm", "--network", "none", "--read-only", "--entrypoint", "/bin/sh"])
            self.assertEqual(call[8], image)
            self.assertIn(CHECKS[target], call)

    def test_inventory_failure_is_bounded_and_names_only_target(self):
        def run(command, **kwargs):
            return CompletedProcess(command, 1, "secret payload", "sensitive diagnostic")

        with self.assertRaises(InventoryError) as caught:
            check_images(run=run)
        self.assertEqual(str(caught.exception), "web_inventory_failed")
        self.assertNotIn("sensitive", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
