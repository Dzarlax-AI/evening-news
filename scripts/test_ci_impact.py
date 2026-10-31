import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


HELPER = Path(__file__).with_name("ci-impact.sh").resolve()


class CIImpactHelperTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "repo"
        self.root.mkdir()
        self.git("init", "-q", "--initial-branch=main")
        self.git("config", "user.email", "ci@example.invalid")
        self.git("config", "user.name", "CI fixture")
        (self.root / "go-tools").mkdir()
        (self.root / "base.txt").write_text("base\n")
        self.git("add", ".")
        self.git("commit", "-qm", "base")
        self.base = self.git("rev-parse", "HEAD")

        self.git("switch", "-qc", "feature")
        (self.root / "feature.txt").write_text("feature\n")
        self.git("add", ".")
        self.git("commit", "-qm", "feature")
        self.head = self.git("rev-parse", "HEAD")

        self.git("switch", "-q", "main")
        (self.root / "main.txt").write_text("main\n")
        self.git("add", ".")
        self.git("commit", "-qm", "main advance")
        self.base_advance = self.git("rev-parse", "HEAD")
        self.git("merge", "--no-ff", "-qm", "synthetic PR merge", "feature")
        self.synthetic_merge = self.git("rev-parse", "HEAD")
        self.git("switch", "-q", "feature")

        self.bin = Path(self.temp.name) / "bin"
        self.bin.mkdir()
        fake_go = self.bin / "go"
        fake_go.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, sys\n"
            "with open(os.environ['CAPTURE_ARGS'], 'w') as out:\n"
            "    json.dump(sys.argv[1:], out)\n"
        )
        fake_go.chmod(0o700)
        self.capture = Path(self.temp.name) / "argv.json"

    def git(self, *args):
        result = subprocess.run(
            ["git", "-C", str(self.root), *args],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        )
        return result.stdout.strip()

    def run_helper(self, event_name, payload, github_sha, github_ref):
        event_path = Path(self.temp.name) / "event.json"
        event_path.write_text(json.dumps(payload))
        output = Path(self.temp.name) / "github-output"
        summary = Path(self.temp.name) / "github-summary"
        runner_temp = Path(self.temp.name) / "runner-temp"
        runner_temp.mkdir(exist_ok=True)
        env = os.environ.copy()
        for key in ("CI_IMPACT_HEAD", "CI_IMPACT_BASE", "GITHUB_EVENT_BEFORE"):
            env.pop(key, None)
        env.update(
            {
                "PATH": str(self.bin) + os.pathsep + env.get("PATH", ""),
                "CAPTURE_ARGS": str(self.capture),
                "GITHUB_EVENT_NAME": event_name,
                "GITHUB_EVENT_PATH": str(event_path),
                "GITHUB_SHA": github_sha,
                "GITHUB_REF": github_ref,
                "GITHUB_OUTPUT": str(output),
                "GITHUB_STEP_SUMMARY": str(summary),
                "RUNNER_TEMP": str(runner_temp),
            }
        )
        return subprocess.run(
            [str(HELPER)], cwd=self.root, env=env, text=True, capture_output=True
        )

    def selected_pair(self):
        args = json.loads(self.capture.read_text())
        return args[args.index("--base") + 1], args[args.index("--head") + 1]

    def test_pull_request_uses_head_sha_and_local_merge_base(self):
        self.assertNotEqual(self.head, self.synthetic_merge)
        result = self.run_helper(
            "pull_request",
            {
                "pull_request": {
                    "head": {"sha": self.head},
                    "base": {"sha": self.base_advance},
                }
            },
            github_sha=self.synthetic_merge,
            github_ref="refs/pull/7/merge",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.selected_pair(), (self.base, self.head))

    def test_push_uses_event_before_and_after(self):
        result = self.run_helper(
            "push",
            {"before": self.base, "after": self.base_advance},
            github_sha=self.base_advance,
            github_ref="refs/heads/main",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.selected_pair(), (self.base, self.base_advance))

    def test_main_manual_publish_uses_parent_as_baseline(self):
        result = self.run_helper(
            "workflow_dispatch",
            {"inputs": {"publish": True}},
            github_sha=self.base_advance,
            github_ref="refs/heads/main",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.selected_pair(), (self.base, self.base_advance))

    def test_missing_push_baseline_fails_without_running_planner(self):
        result = self.run_helper(
            "push",
            {"before": "", "after": self.base_advance},
            github_sha=self.base_advance,
            github_ref="refs/heads/main",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("base must be a full commit SHA", result.stderr)
        self.assertFalse(self.capture.exists())


if __name__ == "__main__":
    unittest.main()
